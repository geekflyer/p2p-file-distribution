use common::proto::file_transfer_client::FileTransferClient;
use common::proto::ShardRequest;
use common::GcsManifest;
use futures::StreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::constants::GRPC_MAX_MESSAGE_SIZE;
use crate::storage::{compute_crc32c, ShardStorage};

/// Number of parallel range requests for GCS downloads (can be overridden via env var)
const DEFAULT_GCS_PARALLEL_RANGES: usize = 8;

/// Type alias for the gRPC client used for peer transfers
pub type PeerClient = FileTransferClient<Channel>;

pub struct Downloader {
    storage: Arc<ShardStorage>,
    /// Cache of bucket -> ObjectStore
    bucket_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
    /// Optional rate limit for GCS downloads in bytes per second
    gcs_bandwidth_limit_bps: Option<u64>,
    /// Optional rate limit for P2P downloads in bytes per second
    p2p_bandwidth_limit_bps: Option<u64>,
}

impl Downloader {
    pub fn new(storage: Arc<ShardStorage>) -> anyhow::Result<Self> {
        // Parse bandwidth limits (e.g., "10m" for 10 Mbit/s, "1g" for 1 Gbit/s)
        let gcs_bandwidth_limit_bps = std::env::var("TEST_ONLY_LIMIT_GCS_BANDWIDTH")
            .ok()
            .and_then(|s| parse_bandwidth_limit(&s));

        let p2p_bandwidth_limit_bps = std::env::var("TEST_ONLY_LIMIT_P2P_BANDWIDTH")
            .ok()
            .and_then(|s| parse_bandwidth_limit(&s));

        if let Some(rate) = gcs_bandwidth_limit_bps {
            tracing::info!("TEST MODE: GCS bandwidth limited to {} bytes/sec ({:.1} Mbit/s)", rate, rate as f64 * 8.0 / 1_000_000.0);
        }

        if let Some(rate) = p2p_bandwidth_limit_bps {
            tracing::info!("TEST MODE: P2P bandwidth limited to {} bytes/sec ({:.1} Mbit/s)", rate, rate as f64 * 8.0 / 1_000_000.0);
        }

        Ok(Self {
            storage,
            bucket_stores: RwLock::new(HashMap::new()),
            gcs_bandwidth_limit_bps,
            p2p_bandwidth_limit_bps,
        })
    }

    /// Get or create an ObjectStore for a bucket (production GCS only)
    async fn get_store(&self, bucket: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
        // Check cache first
        {
            let stores = self.bucket_stores.read().await;
            if let Some(store) = stores.get(bucket) {
                return Ok(store.clone());
            }
        }

        // Create new store for this bucket (for production GCS)
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

        // Cache it
        {
            let mut stores = self.bucket_stores.write().await;
            stores.insert(bucket.to_string(), store.clone());
        }

        Ok(store)
    }

    /// Fetch manifest from GCS
    pub async fn fetch_manifest(&self, gcs_manifest_path: &str) -> anyhow::Result<GcsManifest> {
        // Check for emulator
        if let Ok(emulator_url) = std::env::var("STORAGE_EMULATOR_HOST") {
            let (bucket, object) = parse_gcs_path(gcs_manifest_path)?;
            let url = format!(
                "{}/storage/v1/b/{}/o/{}?alt=media",
                emulator_url,
                bucket,
                urlencoding::encode(object)
            );
            let data = reqwest::get(&url).await?.bytes().await?;
            let manifest: GcsManifest = serde_json::from_slice(&data)?;
            return Ok(manifest);
        }

        let (bucket, object) = parse_gcs_path(gcs_manifest_path)?;
        let store = self.get_store(bucket).await?;
        let path = ObjectPath::from(object);
        let data = store.get(&path).await?.bytes().await?.to_vec();
        let manifest: GcsManifest = serde_json::from_slice(&data)?;
        Ok(manifest)
    }

    /// Connect to a peer for P2P transfers
    pub async fn connect_to_peer(peer_addr: &str) -> anyhow::Result<PeerClient> {
        let addr = format!("http://{}", peer_addr);
        tracing::info!("Connecting to peer at {}", addr);
        let client = FileTransferClient::connect(addr)
            .await?
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);
        Ok(client)
    }

    /// Download a shard from GCS and verify SHA256
    /// shard_path is relative to gcs_base_path (e.g., "shard_0000.bin")
    pub async fn download_shard_from_gcs<F, Fut>(
        &self,
        job_id: Uuid,
        gcs_base_path: &str,
        shard_path: &str,
        shard_id: i32,
        shard_size: u64,
        on_progress: F,
    ) -> anyhow::Result<bool>
    where
        F: Fn(u64, u64, Option<u64>) -> Fut + Send + Sync,
        Fut: Future<Output = ()> + Send,
    {
        let (bucket, object_path) = parse_gcs_path(gcs_base_path)?;
        let shard_object = format!("{}/{}", object_path, shard_path);

        tracing::info!("Downloading shard {} from GCS: gs://{}/{}", shard_id, bucket, shard_object);

        // Start partial file with buffered writer for efficient streaming
        let mut writer = self.storage.start_partial_shard_writer(job_id, shard_id).await?;

        let mut sha256_hasher = Sha256::new();
        let mut bytes_downloaded: u64 = 0;
        let mut throughput_tracker = ThroughputTracker::new();
        let download_start = std::time::Instant::now();

        // For emulator, use direct HTTP streaming; for production, use object_store
        let download_result: Result<(), anyhow::Error> = async {
            if let Ok(emulator_url) = std::env::var("STORAGE_EMULATOR_HOST") {
                let url = format!(
                    "{}/storage/v1/b/{}/o/{}?alt=media",
                    emulator_url,
                    bucket,
                    urlencoding::encode(&shard_object)
                );
                let response = match reqwest::get(&url).await {
                    Ok(r) if r.status().is_success() => r,
                    Ok(r) => {
                        anyhow::bail!("GCS emulator HTTP {}", r.status());
                    }
                    Err(e) => {
                        anyhow::bail!("GCS emulator error: {}", e);
                    }
                };

                // Read body in chunks
                let body = response.bytes().await?;
                for chunk in body.chunks(256 * 1024) {
                    sha256_hasher.update(chunk);
                    writer.write(chunk).await?;
                    bytes_downloaded += chunk.len() as u64;
                    throughput_tracker.add_bytes(chunk.len());
                    on_progress(bytes_downloaded, shard_size, throughput_tracker.get_throughput()).await;
                }
            } else {
                // Production: use object_store streaming
                let store = self.get_store(bucket).await?;
                let path = ObjectPath::from(shard_object.as_str());
                let result = store.get(&path).await?;
                let mut stream = result.into_stream();

                let mut bytes_since_flush: u64 = 0;
                const FLUSH_INTERVAL: u64 = 1 * 1024 * 1024; // Flush every 1MB for piece streaming

                while let Some(chunk_result) = stream.next().await {
                    let chunk = chunk_result?;

                    sha256_hasher.update(&chunk);
                    writer.write(&chunk).await?;
                    bytes_downloaded += chunk.len() as u64;
                    bytes_since_flush += chunk.len() as u64;

                    // Periodic flush to make data visible to downstream peers
                    if bytes_since_flush >= FLUSH_INTERVAL {
                        writer.flush().await?;
                        bytes_since_flush = 0;
                    }

                    // Apply rate limiting if configured
                    if let Some(rate_bps) = self.gcs_bandwidth_limit_bps {
                        let expected_time = bytes_downloaded as f64 / rate_bps as f64;
                        let actual_time = download_start.elapsed().as_secs_f64();
                        if actual_time < expected_time {
                            tokio::time::sleep(tokio::time::Duration::from_secs_f64(expected_time - actual_time)).await;
                        }
                    }

                    throughput_tracker.add_bytes(chunk.len());
                    on_progress(bytes_downloaded, shard_size, throughput_tracker.get_throughput()).await;
                }
            }
            Ok(())
        }.await;

        // Handle download errors
        if let Err(e) = download_result {
            tracing::warn!("GCS download error for shard {}: {} - will retry", shard_id, e);
            self.storage.abort_partial(job_id, shard_id).await?;
            return Ok(false);
        }

        // Flush buffered data before finalizing
        if let Err(e) = writer.flush().await {
            tracing::warn!("Failed to flush shard {}: {} - will retry", shard_id, e);
            self.storage.abort_partial(job_id, shard_id).await?;
            return Ok(false);
        }

        // Verify we got the expected size
        if bytes_downloaded != shard_size {
            tracing::warn!(
                "Shard {} size mismatch: expected {}, got {} - will retry",
                shard_id, shard_size, bytes_downloaded
            );
            self.storage.abort_partial(job_id, shard_id).await?;
            return Ok(false);
        }

        // Verify SHA256 (TODO: compare against manifest)
        let computed_sha256 = hex::encode(sha256_hasher.finalize());
        tracing::info!("Shard {} SHA256: {} ({} bytes)", shard_id, computed_sha256, bytes_downloaded);

        // Finalize the shard
        self.storage.finalize_shard(job_id, shard_id).await?;

        Ok(true)
    }

    /// Download a shard from GCS using parallel range requests
    /// This downloads N ranges in parallel, collects them in memory, verifies SHA256,
    /// then writes to disk. Much faster than single-stream for high-bandwidth connections.
    pub async fn download_shard_from_gcs_parallel<F, Fut>(
        &self,
        job_id: Uuid,
        gcs_base_path: &str,
        shard_path: &str,
        shard_id: i32,
        shard_size: u64,
        on_progress: F,
    ) -> anyhow::Result<bool>
    where
        F: Fn(u64, u64, Option<u64>) -> Fut + Send + Sync,
        Fut: Future<Output = ()> + Send,
    {
        let (bucket, object_path) = parse_gcs_path(gcs_base_path)?;
        let shard_object = format!("{}/{}", object_path, shard_path);

        // Get parallelism from env var or use default
        let num_ranges = std::env::var("GCS_PARALLEL_RANGES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_GCS_PARALLEL_RANGES);

        tracing::info!(
            "Downloading shard {} from GCS (parallel, {} ranges): gs://{}/{}",
            shard_id, num_ranges, bucket, shard_object
        );

        let download_start = std::time::Instant::now();

        // Get the object store
        let store = self.get_store(bucket).await?;
        let path = ObjectPath::from(shard_object.as_str());

        // Calculate ranges
        let ranges = calculate_ranges(shard_size, num_ranges);

        // Track bytes downloaded across all ranges for progress reporting
        let bytes_downloaded = Arc::new(AtomicU64::new(0));

        // Download all ranges in parallel
        let handles: Vec<_> = ranges
            .into_iter()
            .enumerate()
            .map(|(idx, range)| {
                let store = store.clone();
                let path = path.clone();
                let bytes_downloaded = bytes_downloaded.clone();
                let range_len = (range.end - range.start) as u64;
                tokio::spawn(async move {
                    let data = store.get_range(&path, range.clone()).await?;
                    // Update progress when this range completes
                    bytes_downloaded.fetch_add(range_len, Ordering::Relaxed);
                    Ok::<_, anyhow::Error>((idx, data.to_vec()))
                })
            })
            .collect();

        // Collect results in order, reporting progress as each range completes
        let mut range_results: Vec<Option<Vec<u8>>> = vec![None; handles.len()];
        for handle in handles {
            match handle.await {
                Ok(Ok((idx, data))) => {
                    range_results[idx] = Some(data);
                    // Report progress after each range completes
                    let current_bytes = bytes_downloaded.load(Ordering::Relaxed);
                    on_progress(current_bytes, shard_size, None).await;
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        "GCS range download error for shard {}: {} - will retry",
                        shard_id, e
                    );
                    return Ok(false);
                }
                Err(e) => {
                    tracing::warn!(
                        "GCS range download task panic for shard {}: {} - will retry",
                        shard_id, e
                    );
                    return Ok(false);
                }
            }
        }

        // Concatenate all ranges
        let mut all_data = Vec::with_capacity(shard_size as usize);
        for (idx, range_data) in range_results.into_iter().enumerate() {
            match range_data {
                Some(data) => all_data.extend_from_slice(&data),
                None => {
                    tracing::warn!(
                        "Missing range {} for shard {} - will retry",
                        idx, shard_id
                    );
                    return Ok(false);
                }
            }
        }

        // Verify size
        if all_data.len() as u64 != shard_size {
            tracing::warn!(
                "Shard {} size mismatch: expected {}, got {} - will retry",
                shard_id, shard_size, all_data.len()
            );
            return Ok(false);
        }

        // Compute SHA256
        let mut sha256_hasher = Sha256::new();
        sha256_hasher.update(&all_data);
        let computed_sha256 = hex::encode(sha256_hasher.finalize());

        let download_elapsed = download_start.elapsed();
        let throughput_mbps = (shard_size as f64 * 8.0) / download_elapsed.as_secs_f64() / 1_000_000.0;
        tracing::info!(
            "Shard {} SHA256: {} ({} bytes, {:.1} Mbit/s)",
            shard_id, computed_sha256, shard_size, throughput_mbps
        );

        // Write to disk
        let mut writer = self.storage.start_partial_shard_writer(job_id, shard_id).await?;
        if let Err(e) = writer.write(&all_data).await {
            tracing::warn!("Failed to write shard {} data: {} - will retry", shard_id, e);
            self.storage.abort_partial(job_id, shard_id).await?;
            return Ok(false);
        }
        if let Err(e) = writer.flush().await {
            tracing::warn!("Failed to flush shard {}: {} - will retry", shard_id, e);
            self.storage.abort_partial(job_id, shard_id).await?;
            return Ok(false);
        }

        // Finalize the shard
        self.storage.finalize_shard(job_id, shard_id).await?;

        // Report final progress
        on_progress(shard_size, shard_size, Some((shard_size as f64 / download_elapsed.as_secs_f64()) as u64)).await;

        Ok(true)
    }

    /// Download a shard from a peer via gRPC streaming
    /// The client should be created once and reused for multiple shards
    pub async fn download_shard_from_peer<F, Fut>(
        &self,
        client: &mut PeerClient,
        job_id: Uuid,
        shard_id: i32,
        shard_size: u64,
        on_progress: F,
    ) -> anyhow::Result<bool>
    where
        F: Fn(u64, u64, Option<u64>) -> Fut + Send + Sync,
        Fut: Future<Output = ()> + Send,
    {
        let request = ShardRequest {
            job_id: job_id.to_string(),
            from_shard: shard_id,
            from_piece: 0, // Pieces are now just transfer chunks
        };

        let mut stream = match client.stream_shards(request).await {
            Ok(response) => response.into_inner(),
            Err(e) => {
                tracing::warn!("Failed to start stream from peer: {} - will retry", e);
                return Ok(false);
            }
        };

        // DON'T create partial file yet - wait until we receive actual data
        // This prevents downstream peers from seeing an empty partial file
        let mut partial_started = false;

        let mut sha256_hasher = Sha256::new();
        let mut bytes_downloaded: u64 = 0;
        let mut throughput_tracker = ThroughputTracker::new();
        let download_start = std::time::Instant::now();

        loop {
            let chunk_data = match stream.message().await {
                Ok(Some(data)) => data,
                Ok(None) => break, // Stream ended normally
                Err(e) => {
                    tracing::warn!("Peer stream error: {} - will retry", e);
                    if partial_started {
                        self.storage.abort_partial(job_id, shard_id).await?;
                    }
                    return Ok(false);
                }
            };

            // Verify this chunk belongs to our shard
            if chunk_data.shard_id != shard_id {
                tracing::warn!("Received chunk for wrong shard {} (expected {})", chunk_data.shard_id, shard_id);
                continue;
            }

            // Verify CRC32C
            let actual_crc32c = compute_crc32c(&chunk_data.data);
            if actual_crc32c != chunk_data.crc32c {
                tracing::warn!(
                    "CRC32C mismatch for chunk: expected {:08x}, got {:08x} - will retry shard",
                    chunk_data.crc32c,
                    actual_crc32c
                );
                self.storage.abort_partial(job_id, shard_id).await?;
                return Ok(false);
            }

            // Update SHA256
            sha256_hasher.update(&chunk_data.data);

            // Start partial file on first chunk (so downstream peers only see it when we have data)
            if !partial_started {
                self.storage.start_partial_shard(job_id, shard_id).await?;
                partial_started = true;
            }

            // Append to partial file
            if let Err(e) = self.storage.append_to_partial(job_id, shard_id, &chunk_data.data).await {
                tracing::warn!("Failed to write chunk: {} - will retry", e);
                self.storage.abort_partial(job_id, shard_id).await?;
                return Ok(false);
            }

            bytes_downloaded += chunk_data.data.len() as u64;

            // Apply P2P rate limiting if configured
            if let Some(rate_bps) = self.p2p_bandwidth_limit_bps {
                let expected_time = bytes_downloaded as f64 / rate_bps as f64;
                let actual_time = download_start.elapsed().as_secs_f64();
                if actual_time < expected_time {
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(expected_time - actual_time)).await;
                }
            }

            // Track throughput
            throughput_tracker.add_bytes(chunk_data.data.len());

            // Report progress
            on_progress(bytes_downloaded, shard_size, throughput_tracker.get_throughput()).await;

            // Check if we've received the complete shard
            if bytes_downloaded >= shard_size {
                break;
            }
        }

        // Verify we got the expected size
        if bytes_downloaded != shard_size {
            tracing::warn!(
                "Shard {} size mismatch: expected {}, got {} - will retry",
                shard_id, shard_size, bytes_downloaded
            );
            if partial_started {
                self.storage.abort_partial(job_id, shard_id).await?;
            }
            return Ok(false);
        }

        // Verify SHA256 (TODO: compare against manifest)
        let computed_sha256 = hex::encode(sha256_hasher.finalize());
        tracing::info!("Shard {} SHA256: {} ({} bytes from peer)", shard_id, computed_sha256, bytes_downloaded);

        // Finalize the shard
        self.storage.finalize_shard(job_id, shard_id).await?;

        Ok(true)
    }
}

/// Tracks throughput using total bytes and elapsed time
struct ThroughputTracker {
    total_bytes: u64,
    start_time: std::time::Instant,
}

impl ThroughputTracker {
    fn new() -> Self {
        Self {
            total_bytes: 0,
            start_time: std::time::Instant::now(),
        }
    }

    fn add_bytes(&mut self, bytes: usize) {
        self.total_bytes += bytes as u64;
    }

    fn get_throughput(&self) -> Option<u64> {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            Some((self.total_bytes as f64 / elapsed) as u64)
        } else {
            None
        }
    }
}

/// Parse a GCS path into bucket and object path
fn parse_gcs_path(path: &str) -> anyhow::Result<(&str, &str)> {
    let path = path
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow::anyhow!("Invalid GCS path: must start with gs://"))?;

    let slash_pos = path
        .find('/')
        .ok_or_else(|| anyhow::anyhow!("Invalid GCS path: no object path"))?;

    let bucket = &path[..slash_pos];
    let object = &path[slash_pos + 1..];

    Ok((bucket, object))
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
