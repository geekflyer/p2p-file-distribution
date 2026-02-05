use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, Semaphore};
use uuid::Uuid;

use crate::storage::{compute_crc32c, ChunkStorage};

/// Default number of chunks to batch per GCS request
const DEFAULT_GCS_BATCH_CHUNKS: usize = 1;

/// Default number of parallel GCS downloads
const DEFAULT_GCS_PARALLEL_DOWNLOADS: usize = 4;

fn get_gcs_batch_chunks() -> usize {
    std::env::var("GCS_BATCH_CHUNKS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_GCS_BATCH_CHUNKS)
}

fn get_gcs_parallel_downloads() -> usize {
    std::env::var("GCS_PARALLEL_DOWNLOADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_GCS_PARALLEL_DOWNLOADS)
}

pub struct Downloader {
    storage: Arc<ChunkStorage>,
    /// Cache of bucket -> ObjectStore
    bucket_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
    /// Optional rate limit for GCS downloads in bytes per second
    gcs_bandwidth_limit_bps: Option<u64>,
    /// Optional rate limit for P2P downloads in bytes per second
    p2p_bandwidth_limit_bps: Option<u64>,
}

impl Downloader {
    pub fn new(storage: Arc<ChunkStorage>) -> anyhow::Result<Self> {
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

    /// Download chunks from GCS in batches with parallel downloads for efficiency
    /// Uses a channel-based approach: N parallel downloaders -> ordered writer
    pub async fn download_chunks_from_gcs<F, Fut>(
        &self,
        file_id: Uuid,
        gcs_path: &str,
        start_chunk: i32,
        total_chunks: i32,
        chunk_size_bytes: u64,
        total_size: u64,
        on_progress: F,
    ) -> anyhow::Result<bool>
    where
        F: Fn(i32, i32, u64, Option<u64>) -> Fut + Send + Sync,
        Fut: Future<Output = ()> + Send,
    {
        let (bucket, object) = parse_gcs_path(gcs_path)?;

        let gcs_parallel = get_gcs_parallel_downloads();
        let gcs_batch = get_gcs_batch_chunks();

        tracing::info!(
            "Downloading chunks {}-{} from GCS: gs://{}/{} ({} parallel, {} chunks/batch)",
            start_chunk, total_chunks - 1, bucket, object, gcs_parallel, gcs_batch
        );

        let download_start = std::time::Instant::now();

        // Calculate all batches upfront
        let mut batches: Vec<(i32, i32)> = Vec::new();
        let mut batch_idx = start_chunk;
        while batch_idx < total_chunks {
            let batch_end = std::cmp::min(batch_idx + gcs_batch as i32, total_chunks);
            batches.push((batch_idx, batch_end));
            batch_idx = batch_end;
        }

        let total_batches = batches.len();
        let emulator_url = std::env::var("STORAGE_EMULATOR_HOST").ok();

        // Channel for completed batches: (batch_start, data)
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(i32, Vec<u8>)>(gcs_parallel * 2);

        // Semaphore to limit concurrent downloads
        let semaphore = Arc::new(Semaphore::new(gcs_parallel));

        // Spawn download tasks
        for (batch_start, batch_end) in batches.iter().copied() {
            let sem = semaphore.clone();
            let tx = tx.clone();
            let bucket = bucket.to_string();
            let object = object.to_string();
            let emulator = emulator_url.clone();
            let store = if emulator.is_none() {
                Some(self.get_store(&bucket).await?)
            } else {
                None
            };

            let start_byte = batch_start as u64 * chunk_size_bytes;
            let end_byte = if batch_end == total_chunks {
                total_size
            } else {
                batch_end as u64 * chunk_size_bytes
            };

            tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                let result = if let Some(ref emu_url) = emulator {
                    download_range_from_emulator(emu_url, &bucket, &object, start_byte, end_byte).await
                } else {
                    let store = store.unwrap();
                    let path = ObjectPath::from(object.as_str());
                    store
                        .get_range(&path, start_byte as usize..end_byte as usize)
                        .await
                        .map(|b| b.to_vec())
                        .map_err(|e| anyhow::anyhow!("{}", e))
                };

                match result {
                    Ok(data) => {
                        // Send to channel - if receiver is dropped, just exit
                        let _ = tx.send((batch_start, data)).await;
                    }
                    Err(e) => {
                        tracing::error!("Failed to download batch {}: {}", batch_start, e);
                        // Channel will close naturally
                    }
                }
            });
        }

        // Drop our sender so the channel closes when all spawned tasks complete
        drop(tx);

        // Buffer for out-of-order batches
        let mut pending: BTreeMap<i32, Vec<u8>> = BTreeMap::new();
        let mut next_batch_start = start_chunk;
        let mut batches_received = 0;
        let mut total_bytes_downloaded: u64 = 0;

        // Process batches as they arrive, writing in order
        while let Some((batch_start, data)) = rx.recv().await {
            batches_received += 1;

            if batch_start == next_batch_start {
                // This is the next batch we need - write it
                let batch_end = batches.iter()
                    .find(|(s, _)| *s == batch_start)
                    .map(|(_, e)| *e)
                    .unwrap();

                total_bytes_downloaded += self.write_batch(
                    file_id, batch_start, batch_end, total_chunks, chunk_size_bytes, &data,
                    total_bytes_downloaded, &download_start, &on_progress
                ).await?;

                next_batch_start = batch_end;

                // Check if we have subsequent batches buffered
                while let Some(buffered_data) = pending.remove(&next_batch_start) {
                    let batch_end = batches.iter()
                        .find(|(s, _)| *s == next_batch_start)
                        .map(|(_, e)| *e)
                        .unwrap();

                    total_bytes_downloaded += self.write_batch(
                        file_id, next_batch_start, batch_end, total_chunks, chunk_size_bytes, &buffered_data,
                        total_bytes_downloaded, &download_start, &on_progress
                    ).await?;

                    next_batch_start = batch_end;
                }
            } else {
                // Out of order - buffer it
                pending.insert(batch_start, data);
            }

            // Rate limiting (for testing)
            if let Some(rate_bps) = self.gcs_bandwidth_limit_bps {
                let expected_time = total_bytes_downloaded as f64 / rate_bps as f64;
                let actual_time = download_start.elapsed().as_secs_f64();
                if actual_time < expected_time {
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(expected_time - actual_time)).await;
                }
            }
        }

        // Check if we got all batches
        if batches_received != total_batches {
            anyhow::bail!("Only received {}/{} batches", batches_received, total_batches);
        }

        let elapsed = download_start.elapsed();
        let throughput_mbps = (total_bytes_downloaded as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;
        tracing::info!(
            "Downloaded {} bytes in {:.1}s ({:.1} Mbit/s)",
            total_bytes_downloaded, elapsed.as_secs_f64(), throughput_mbps
        );

        Ok(true)
    }

    /// Helper to write a batch to storage and report progress
    /// Also computes and stores CRC32C per chunk for P2P serving
    async fn write_batch<F, Fut>(
        &self,
        file_id: Uuid,
        batch_start: i32,
        batch_end: i32,
        total_chunks: i32,
        chunk_size_bytes: u64,
        data: &[u8],
        bytes_so_far: u64,
        download_start: &std::time::Instant,
        on_progress: &F,
    ) -> anyhow::Result<u64>
    where
        F: Fn(i32, i32, u64, Option<u64>) -> Fut + Send + Sync,
        Fut: Future<Output = ()> + Send,
    {
        let mut bytes_written = 0u64;

        for chunk_id in batch_start..batch_end {
            let offset_in_batch = ((chunk_id - batch_start) as u64 * chunk_size_bytes) as usize;
            let chunk_end_offset = if chunk_id == total_chunks - 1 {
                data.len()
            } else {
                std::cmp::min(offset_in_batch + chunk_size_bytes as usize, data.len())
            };

            let chunk_data = &data[offset_in_batch..chunk_end_offset];

            // Compute CRC32C and store it for P2P serving
            let crc32c = compute_crc32c(chunk_data);
            self.storage.store_chunk_crc32c(file_id, chunk_id, crc32c).await?;

            self.storage.append_chunk(file_id, chunk_data).await?;

            bytes_written += chunk_data.len() as u64;
            let total = bytes_so_far + bytes_written;

            let elapsed = download_start.elapsed().as_secs_f64();
            let throughput = if elapsed > 0.0 {
                Some((total as f64 / elapsed) as u64)
            } else {
                None
            };

            on_progress(chunk_id, total_chunks, total, throughput).await;
        }

        Ok(bytes_written)
    }

    /// Download chunks from a peer via raw TCP with sendfile
    /// Protocol:
    /// - Request: [file_id: 16 bytes][start_chunk: 4 bytes i32 LE]
    /// - Response per chunk: [chunk_id: 4 bytes i32 LE][crc32c: 4 bytes u32 LE][size: 4 bytes u32 LE][data]
    pub async fn download_chunks_from_peer<F, Fut>(
        &self,
        peer_addr: &str,
        file_id: Uuid,
        start_from_chunk: i32,
        total_chunks: i32,
        chunk_size_bytes: u64,
        total_size: u64,
        on_progress: F,
    ) -> anyhow::Result<bool>
    where
        F: Fn(i32, i32, u64, Option<u64>) -> Fut + Send + Sync,
        Fut: Future<Output = ()> + Send,
    {
        tracing::info!("Connecting to peer {} for file {}", peer_addr, file_id);

        let mut stream = match TcpStream::connect(peer_addr).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Failed to connect to peer {}: {} - will retry", peer_addr, e);
                return Ok(false);
            }
        };

        // Send request: [file_id: 16 bytes][start_chunk: 4 bytes i32 LE]
        let mut request = [0u8; 20];
        request[0..16].copy_from_slice(file_id.as_bytes());
        request[16..20].copy_from_slice(&start_from_chunk.to_le_bytes());

        if let Err(e) = stream.write_all(&request).await {
            tracing::warn!("Failed to send request to peer: {} - will retry", e);
            return Ok(false);
        }

        let download_start = std::time::Instant::now();
        let mut total_bytes_downloaded: u64 = 0;
        let mut last_chunk_received = start_from_chunk - 1;

        // Read chunks
        loop {
            // Read header: [chunk_id: 4][crc32c: 4][size: 4] = 12 bytes
            let mut header = [0u8; 12];
            match stream.read_exact(&mut header).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Stream ended - check if we got all chunks
                    break;
                }
                Err(e) => {
                    tracing::warn!("Failed to read header from peer: {} - will retry", e);
                    return Ok(false);
                }
            }

            let chunk_id = i32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let crc32c = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
            let size = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

            // Validate size
            let expected_chunk_size_bytes = if chunk_id == total_chunks - 1 {
                let remainder = total_size % chunk_size_bytes;
                if remainder == 0 { chunk_size_bytes } else { remainder }
            } else {
                chunk_size_bytes
            };

            if size as u64 != expected_chunk_size_bytes {
                tracing::warn!(
                    "Chunk {} size mismatch: expected {}, got {}",
                    chunk_id, expected_chunk_size_bytes, size
                );
                return Ok(false);
            }

            // Read chunk data
            let mut data = vec![0u8; size as usize];
            if let Err(e) = stream.read_exact(&mut data).await {
                tracing::warn!("Failed to read chunk data from peer: {} - will retry", e);
                return Ok(false);
            }

            // Verify CRC32C
            let actual_crc32c = compute_crc32c(&data);
            if actual_crc32c != crc32c {
                tracing::warn!(
                    "CRC32C mismatch for chunk {}: expected {:08x}, got {:08x} - will retry",
                    chunk_id, crc32c, actual_crc32c
                );
                return Ok(false);
            }

            // Store CRC32C for downstream P2P serving
            self.storage.store_chunk_crc32c(file_id, chunk_id, actual_crc32c).await?;

            // Append chunk to storage
            self.storage.append_chunk(file_id, &data).await?;

            total_bytes_downloaded += data.len() as u64;
            last_chunk_received = chunk_id;

            // Rate limiting
            if let Some(rate_bps) = self.p2p_bandwidth_limit_bps {
                let expected_time = total_bytes_downloaded as f64 / rate_bps as f64;
                let actual_time = download_start.elapsed().as_secs_f64();
                if actual_time < expected_time {
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(expected_time - actual_time)).await;
                }
            }

            // Calculate throughput
            let elapsed = download_start.elapsed().as_secs_f64();
            let throughput = if elapsed > 0.0 {
                Some((total_bytes_downloaded as f64 / elapsed) as u64)
            } else {
                None
            };

            on_progress(chunk_id, total_chunks, total_bytes_downloaded, throughput).await;

            // Check if we've received all chunks
            if last_chunk_received >= total_chunks - 1 {
                break;
            }
        }

        // Verify we got all expected chunks
        if last_chunk_received < total_chunks - 1 {
            tracing::warn!(
                "Incomplete download: only received up to chunk {} of {}",
                last_chunk_received, total_chunks - 1
            );
            return Ok(false);
        }

        let elapsed = download_start.elapsed();
        let throughput_mbps = (total_bytes_downloaded as f64 * 8.0) / elapsed.as_secs_f64() / 1_000_000.0;
        tracing::info!(
            "Downloaded {} bytes from peer in {:.1}s ({:.1} Mbit/s)",
            total_bytes_downloaded, elapsed.as_secs_f64(), throughput_mbps
        );

        Ok(true)
    }
}

/// Download a byte range from the GCS emulator
async fn download_range_from_emulator(
    emulator_url: &str,
    bucket: &str,
    object: &str,
    start_byte: u64,
    end_byte: u64,
) -> anyhow::Result<Vec<u8>> {
    let url = format!(
        "{}/storage/v1/b/{}/o/{}?alt=media",
        emulator_url,
        bucket,
        urlencoding::encode(object)
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("Range", format!("bytes={}-{}", start_byte, end_byte - 1))
        .send()
        .await?;

    if !response.status().is_success() && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        anyhow::bail!("GCS emulator HTTP {}", response.status());
    }

    Ok(response.bytes().await?.to_vec())
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
