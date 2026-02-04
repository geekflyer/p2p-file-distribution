use common::proto::ShardRequest;
use common::proto::shard_transfer_client::ShardTransferClient;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

use crate::storage::{ShardStorage, compute_crc32c};

const GRPC_MAX_MESSAGE_SIZE: usize = 512 * 1024 * 1024; // 512MB

/// Number of parallel range requests for GCS downloads
/// Default to 2 to avoid OOM on memory-constrained VMs (n2-highcpu-2 has only 2GB RAM)
/// Each parallel download holds ~shard_size/num_ranges in memory before writing to disk
const DEFAULT_GCS_PARALLEL_RANGES: usize = 2;

pub struct Downloader {
    storage: Arc<ShardStorage>,
    bucket_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
    /// Optional rate limit for GCS downloads in bytes per second
    gcs_bandwidth_limit_bps: Option<u64>,
    /// Optional rate limit for P2P downloads in bytes per second
    p2p_bandwidth_limit_bps: Option<u64>,
}

impl Downloader {
    pub fn new(storage: Arc<ShardStorage>) -> Self {
        // Parse bandwidth limits (e.g., "10m" for 10 Mbit/s, "1g" for 1 Gbit/s)
        let gcs_bandwidth_limit_bps = std::env::var("TEST_ONLY_LIMIT_GCS_BANDWIDTH")
            .ok()
            .and_then(|s| parse_bandwidth_limit(&s));

        let p2p_bandwidth_limit_bps = std::env::var("TEST_ONLY_LIMIT_P2P_BANDWIDTH")
            .ok()
            .and_then(|s| parse_bandwidth_limit(&s));

        if let Some(rate) = gcs_bandwidth_limit_bps {
            tracing::info!(
                "TEST MODE: GCS bandwidth limited to {} bytes/sec ({:.1} Mbit/s)",
                rate,
                rate as f64 * 8.0 / 1_000_000.0
            );
        }

        if let Some(rate) = p2p_bandwidth_limit_bps {
            tracing::info!(
                "TEST MODE: P2P bandwidth limited to {} bytes/sec ({:.1} Mbit/s)",
                rate,
                rate as f64 * 8.0 / 1_000_000.0
            );
        }

        Self {
            storage,
            bucket_stores: RwLock::new(HashMap::new()),
            gcs_bandwidth_limit_bps,
            p2p_bandwidth_limit_bps,
        }
    }

    /// Apply rate limiting based on bytes downloaded and elapsed time
    async fn apply_rate_limit(
        &self,
        bytes_downloaded: u64,
        start_time: Instant,
        rate_bps: Option<u64>,
    ) {
        if let Some(rate) = rate_bps {
            let expected_time = bytes_downloaded as f64 / rate as f64;
            let actual_time = start_time.elapsed().as_secs_f64();
            if actual_time < expected_time {
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(
                    expected_time - actual_time,
                ))
                .await;
            }
        }
    }

    async fn get_store(&self, bucket: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
        {
            let stores = self.bucket_stores.read().await;
            if let Some(store) = stores.get(bucket) {
                return Ok(store.clone());
            }
        }

        let store = if let Ok(creds_path) = std::env::var("GCS_SERVICE_ACCOUNT_PATH") {
            GoogleCloudStorageBuilder::new()
                .with_bucket_name(bucket)
                .with_service_account_path(creds_path)
                .build()?
        } else {
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()?
        };

        let store: Arc<dyn ObjectStore> = Arc::new(store);

        {
            let mut stores = self.bucket_stores.write().await;
            stores.insert(bucket.to_string(), store.clone());
        }

        Ok(store)
    }

    /// Download a single shard from GCS
    pub async fn download_shard_from_gcs(
        &self,
        job_id: &str,
        gcs_path: &str,
        shard_id: i32,
    ) -> anyhow::Result<bool> {
        let (bucket, object) = parse_gcs_path(gcs_path)?;

        tracing::info!("Downloading shard {} from GCS: {}", shard_id, gcs_path);

        self.storage.start_partial_shard(job_id, shard_id).await?;

        let mut sha256_hasher = Sha256::new();
        let mut bytes_downloaded: u64 = 0;
        let download_start = Instant::now();

        if let Ok(emulator_url) = std::env::var("STORAGE_EMULATOR_HOST") {
            let url = format!(
                "{}/storage/v1/b/{}/o/{}?alt=media",
                emulator_url,
                bucket,
                urlencoding::encode(object)
            );

            let response = match reqwest::get(&url).await {
                Ok(r) if r.status().is_success() => r,
                Ok(r) => {
                    tracing::warn!(
                        "GCS emulator error for shard {}: HTTP {}",
                        shard_id,
                        r.status()
                    );
                    self.storage.abort_partial(job_id, shard_id).await?;
                    return Ok(false);
                }
                Err(e) => {
                    tracing::warn!("GCS emulator error for shard {}: {}", shard_id, e);
                    self.storage.abort_partial(job_id, shard_id).await?;
                    return Ok(false);
                }
            };

            // Stream the response to apply rate limiting during download
            let mut stream = response.bytes_stream();
            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result?;
                sha256_hasher.update(&chunk);
                self.storage
                    .append_to_partial(job_id, shard_id, &chunk)
                    .await?;
                bytes_downloaded += chunk.len() as u64;
                self.apply_rate_limit(
                    bytes_downloaded,
                    download_start,
                    self.gcs_bandwidth_limit_bps,
                )
                .await;
            }
        } else {
            let store = self.get_store(bucket).await?;
            let path = ObjectPath::from(object);
            let result = store.get(&path).await?;
            let mut stream = result.into_stream();

            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result?;
                sha256_hasher.update(&chunk);
                self.storage
                    .append_to_partial(job_id, shard_id, &chunk)
                    .await?;
                bytes_downloaded += chunk.len() as u64;
                self.apply_rate_limit(
                    bytes_downloaded,
                    download_start,
                    self.gcs_bandwidth_limit_bps,
                )
                .await;
            }
        }

        let computed_sha256 = hex::encode(sha256_hasher.finalize());
        tracing::info!(
            "Shard {} from GCS: SHA256={} ({} bytes)",
            shard_id,
            computed_sha256,
            bytes_downloaded
        );

        self.storage.finalize_shard(job_id, shard_id).await?;
        Ok(true)
    }

    /// Download a single shard from GCS using sequential range requests
    /// Streams directly to disk to avoid OOM on memory-constrained VMs
    /// Still benefits from efficient TCP connection reuse
    pub async fn download_shard_from_gcs_parallel(
        &self,
        job_id: &str,
        gcs_path: &str,
        shard_id: i32,
        shard_size: u64,
    ) -> anyhow::Result<bool> {
        use std::io::SeekFrom;
        use tokio::io::{AsyncSeekExt, AsyncWriteExt, AsyncReadExt};

        let (bucket, object) = parse_gcs_path(gcs_path)?;

        let num_ranges = std::env::var("GCS_PARALLEL_RANGES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_GCS_PARALLEL_RANGES);

        tracing::info!(
            "Downloading shard {} from GCS ({} sequential ranges): {}",
            shard_id, num_ranges, gcs_path
        );

        let download_start = Instant::now();

        let store = self.get_store(bucket).await?;
        let path = ObjectPath::from(object);

        // Calculate ranges
        let ranges = calculate_ranges(shard_size, num_ranges);

        // Create the partial file upfront
        self.storage.start_partial_shard(job_id, shard_id).await?;
        let partial_path = self.storage.get_partial_path(job_id, shard_id);

        // Pre-allocate the file to the expected size
        {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&partial_path)
                .await?;
            file.set_len(shard_size).await?;
        }

        // Download ranges sequentially to limit memory usage
        // Each range is downloaded and immediately written to disk before the next starts
        let mut total_bytes = 0u64;
        for (idx, range) in ranges.into_iter().enumerate() {
            let range_start = range.start as u64;

            // Download the range
            let data = match store.get_range(&path, range.clone()).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(
                        "GCS range {} download error for shard {}: {} - will retry",
                        idx, shard_id, e
                    );
                    self.storage.abort_partial(job_id, shard_id).await?;
                    return Ok(false);
                }
            };

            // Write directly to file at the correct offset
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&partial_path)
                .await?;
            file.seek(SeekFrom::Start(range_start)).await?;
            file.write_all(&data).await?;
            file.flush().await?;

            total_bytes += data.len() as u64;
            tracing::debug!(
                "Shard {} range {}/{} complete ({} bytes)",
                shard_id, idx + 1, num_ranges, data.len()
            );
        }

        // Verify total bytes downloaded
        if total_bytes != shard_size {
            tracing::warn!(
                "Shard {} size mismatch: expected {}, got {} - will retry",
                shard_id, shard_size, total_bytes
            );
            self.storage.abort_partial(job_id, shard_id).await?;
            return Ok(false);
        }

        // Stream file through SHA256 hasher (using small buffer to avoid OOM)
        let mut sha256_hasher = Sha256::new();
        {
            let mut file = tokio::fs::File::open(&partial_path).await?;
            let mut buf = vec![0u8; 256 * 1024]; // 256KB buffer
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                sha256_hasher.update(&buf[..n]);
            }
        }
        let computed_sha256 = hex::encode(sha256_hasher.finalize());

        let download_elapsed = download_start.elapsed();
        let throughput_mbps = (shard_size as f64 * 8.0) / download_elapsed.as_secs_f64() / 1_000_000.0;
        tracing::info!(
            "Shard {} SHA256: {} ({} bytes, {:.1} Mbit/s)",
            shard_id, computed_sha256, shard_size, throughput_mbps
        );

        // Finalize the shard
        self.storage.finalize_shard(job_id, shard_id).await?;

        Ok(true)
    }

    /// Download a single shard from a peer
    pub async fn download_shard_from_peer(
        &self,
        job_id: &str,
        peer_addr: &str,
        shard_id: i32,
    ) -> anyhow::Result<bool> {
        tracing::info!("Downloading shard {} from peer {}", shard_id, peer_addr);

        let addr = format!("http://{}", peer_addr);
        let mut client = match ShardTransferClient::connect(addr).await {
            Ok(c) => c
                .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE),
            Err(e) => {
                tracing::warn!("Failed to connect to peer {}: {}", peer_addr, e);
                return Ok(false);
            }
        };

        let request = ShardRequest {
            job_id: job_id.to_string(),
            shard_id,
            from_piece: 0,
        };

        let mut stream = match client.stream_shard(request).await {
            Ok(response) => response.into_inner(),
            Err(e) => {
                tracing::warn!("Failed to start stream from peer {}: {}", peer_addr, e);
                return Ok(false);
            }
        };

        self.storage.start_partial_shard(job_id, shard_id).await?;

        let mut sha256_hasher = Sha256::new();
        let mut bytes_downloaded: u64 = 0;
        let download_start = Instant::now();

        loop {
            let piece = match stream.message().await {
                Ok(Some(p)) => p,
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!("Peer stream error: {}", e);
                    self.storage.abort_partial(job_id, shard_id).await?;
                    return Ok(false);
                }
            };

            if piece.shard_id != shard_id {
                tracing::warn!(
                    "Received wrong shard {} (expected {})",
                    piece.shard_id,
                    shard_id
                );
                continue;
            }

            // Verify CRC32C
            let actual_crc = compute_crc32c(&piece.data);
            if actual_crc != piece.crc32c {
                tracing::warn!(
                    "CRC32C mismatch: expected {:08x}, got {:08x}",
                    piece.crc32c,
                    actual_crc
                );
                self.storage.abort_partial(job_id, shard_id).await?;
                return Ok(false);
            }

            sha256_hasher.update(&piece.data);
            self.storage
                .append_to_partial(job_id, shard_id, &piece.data)
                .await?;
            bytes_downloaded += piece.data.len() as u64;
            self.apply_rate_limit(
                bytes_downloaded,
                download_start,
                self.p2p_bandwidth_limit_bps,
            )
            .await;
        }

        let computed_sha256 = hex::encode(sha256_hasher.finalize());
        tracing::info!(
            "Shard {} from peer {}: SHA256={} ({} bytes)",
            shard_id,
            peer_addr,
            computed_sha256,
            bytes_downloaded
        );

        self.storage.finalize_shard(job_id, shard_id).await?;
        Ok(true)
    }
}

fn parse_gcs_path(path: &str) -> anyhow::Result<(&str, &str)> {
    let path = path
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow::anyhow!("Invalid GCS path: must start with gs://"))?;

    let slash_pos = path
        .find('/')
        .ok_or_else(|| anyhow::anyhow!("Invalid GCS path: no object path"))?;

    Ok((&path[..slash_pos], &path[slash_pos + 1..]))
}

/// Parse bandwidth limit string like "10m" (10 Mbit/s) or "1g" (1 Gbit/s)
fn parse_bandwidth_limit(s: &str) -> Option<u64> {
    let s = s.trim().to_lowercase();
    let (num_str, multiplier) = if s.ends_with('g') {
        (&s[..s.len() - 1], 1_000_000_000u64 / 8) // Gbit to bytes
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], 1_000_000u64 / 8) // Mbit to bytes
    } else if s.ends_with('k') {
        (&s[..s.len() - 1], 1_000u64 / 8) // Kbit to bytes
    } else {
        // Assume raw bytes per second
        (s.as_str(), 1u64)
    };

    num_str.parse::<u64>().ok().map(|n| n * multiplier)
}

/// Calculate byte ranges for parallel download
fn calculate_ranges(total_size: u64, num_ranges: usize) -> Vec<Range<usize>> {
    let range_size = total_size / num_ranges as u64;
    (0..num_ranges)
        .map(|i| {
            let start = i as u64 * range_size;
            let end = if i == num_ranges - 1 {
                total_size
            } else {
                (i + 1) as u64 * range_size
            };
            (start as usize)..(end as usize)
        })
        .collect()
}
