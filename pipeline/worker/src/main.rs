mod coordinator_client;
mod downloader;
mod storage;
mod tcp_server;

use chrono::Utc;
use common::{DistributionTask, TaskProgress, Upstream};
use nix::sys::statvfs::statvfs;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Get disk stats for the root filesystem (total bytes, used bytes)
fn get_disk_stats(_data_dir: &std::path::Path) -> Option<(u64, u64)> {
    // Get disk stats from root filesystem to represent whole VM disk
    match statvfs("/") {
        Ok(stat) => {
            let block_size = stat.block_size() as u64;
            let total_blocks = stat.blocks() as u64;
            let free_blocks = stat.blocks_available() as u64;
            let total = total_blocks * block_size;
            let used = (total_blocks - free_blocks) * block_size;
            Some((total, used))
        }
        Err(e) => {
            tracing::warn!("Failed to get disk stats: {}", e);
            None
        }
    }
}

use coordinator_client::CoordinatorClient;
use downloader::Downloader;
use storage::ChunkStorage;
use tcp_server::{ChunkMeta, ChunkMetaStore, UploadTracker};

/// Rolling window throughput tracker
/// Tracks cumulative bytes over time and calculates throughput from the window
/// Caches last non-zero throughput to avoid showing 0 during brief idle periods
struct ThroughputTracker {
    /// (timestamp, cumulative_bytes) samples
    samples: VecDeque<(Instant, u64)>,
    /// Window duration in seconds
    window_secs: f64,
    /// Last non-zero throughput (to return during brief idle periods)
    last_nonzero_throughput: Option<u64>,
    /// When the last non-zero throughput was recorded
    last_nonzero_at: Option<Instant>,
}

impl ThroughputTracker {
    fn new(window_secs: f64) -> Self {
        Self {
            samples: VecDeque::new(),
            window_secs,
            last_nonzero_throughput: None,
            last_nonzero_at: None,
        }
    }

    /// Record current cumulative bytes
    fn record(&mut self, bytes: u64) {
        let now = Instant::now();
        self.samples.push_back((now, bytes));
        // Remove samples older than window
        while let Some((ts, _)) = self.samples.front() {
            if now.duration_since(*ts).as_secs_f64() > self.window_secs {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Get current throughput in bytes per second
    /// Returns cached value during brief idle periods to avoid flickering
    fn get_throughput(&mut self) -> Option<u64> {
        if self.samples.len() < 2 {
            return self.last_nonzero_throughput;
        }
        let (oldest_ts, oldest_bytes) = self.samples.front()?;
        let (newest_ts, newest_bytes) = self.samples.back()?;

        let time_delta = newest_ts.duration_since(*oldest_ts).as_secs_f64();
        if time_delta < 0.1 {
            return self.last_nonzero_throughput;
        }

        let bytes_delta = newest_bytes.saturating_sub(*oldest_bytes);
        let throughput = (bytes_delta as f64 / time_delta) as u64;

        if throughput > 0 {
            // Update cached value
            self.last_nonzero_throughput = Some(throughput);
            self.last_nonzero_at = Some(*newest_ts);
            Some(throughput)
        } else if let (Some(cached), Some(cached_at)) = (self.last_nonzero_throughput, self.last_nonzero_at) {
            // Return cached value if it's recent (within 2x window)
            if newest_ts.duration_since(cached_at).as_secs_f64() < self.window_secs * 2.0 {
                Some(cached)
            } else {
                // Cached value is stale, task is truly idle
                None
            }
        } else {
            None
        }
    }
}

struct TaskState {
    /// Last fully completed chunk
    last_chunk_completed: i32,
    completed: bool,
    /// Total bytes downloaded so far
    bytes_downloaded: u64,
    /// Rolling window download throughput tracker
    download_throughput: ThroughputTracker,
    /// Rolling window upload throughput tracker
    upload_throughput: ThroughputTracker,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env.local if present (for local development)
    dotenvy::from_filename(".env.local").ok();

    // Ignore SIGPIPE to prevent silent termination when peer disconnects
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }

    // Set panic hook to log panics
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("PANIC: {}", panic_info);
        if let Some(location) = panic_info.location() {
            eprintln!("  at {}:{}:{}", location.file(), location.line(), location.column());
        }
    }));

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("worker=info".parse()?)
                .add_directive("tower_http=debug".parse()?),
        )
        .init();

    // Get configuration from environment
    let tcp_port = std::env::var("TCP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50051u16);
    let worker_addr = std::env::var("WORKER_ADDR")
        .unwrap_or_else(|_| format!("localhost:{}", tcp_port));
    let worker_id = std::env::var("WORKER_ID")
        .unwrap_or_else(|_| worker_addr.clone());
    let coordinator_url =
        std::env::var("COORDINATOR_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());

    tracing::info!("Starting worker at: {}", worker_addr);
    tracing::info!("Worker ID: {}", worker_id);
    tracing::info!("Coordinator URL: {}", coordinator_url);
    tracing::info!("Data directory: {}", data_dir);

    // Create storage
    let storage = Arc::new(ChunkStorage::new(PathBuf::from(&data_dir)));

    // Create coordinator client
    let coordinator = CoordinatorClient::new(coordinator_url, worker_id.clone(), worker_addr.clone());

    // Create downloader
    let downloader = Arc::new(Downloader::new(storage.clone())?);

    // Track task states
    let task_states: Arc<RwLock<HashMap<Uuid, TaskState>>> = Arc::new(RwLock::new(HashMap::new()));

    // Track cancelled file IDs (to abort in-progress downloads)
    let cancelled_files: Arc<RwLock<HashSet<Uuid>>> = Arc::new(RwLock::new(HashSet::new()));

    // Track files currently being downloaded (to avoid starting duplicate downloads)
    let downloading_files: Arc<RwLock<HashSet<Uuid>>> = Arc::new(RwLock::new(HashSet::new()));

    // Track uploaded bytes per file
    let upload_tracker: UploadTracker = Arc::new(RwLock::new(HashMap::new()));

    // Track chunk metadata for P2P serving
    let chunk_meta: ChunkMetaStore = Arc::new(RwLock::new(HashMap::new()));

    // Spawn TCP server for P2P transfers (uses sendfile for zero-copy)
    let tcp_addr = format!("0.0.0.0:{}", tcp_port).parse()?;
    let tcp_storage = storage.clone();
    let tcp_chunk_meta = chunk_meta.clone();
    let tcp_upload_tracker = upload_tracker.clone();
    tokio::spawn(async move {
        if let Err(e) = tcp_server::start_tcp_server(
            tcp_addr,
            tcp_storage,
            tcp_chunk_meta,
            tcp_upload_tracker,
        ).await {
            tracing::error!("TCP server error: {}", e);
        }
    });

    // Main loop: check-in runs independently of downloads
    let mut poll_interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        poll_interval.tick().await;

        // Collect current progress
        let (progress, disk_total, disk_used) = {
            let mut states = task_states.write().await;
            let uploads = upload_tracker.read().await;

            let progress: Vec<TaskProgress> = states
                .iter_mut()
                .map(|(file_id, state)| {
                    // Use bytes_downloaded for download throughput calculation
                    state.download_throughput.record(state.bytes_downloaded);
                    let download_throughput_bps = state.download_throughput.get_throughput();

                    // Use bytes_uploaded for upload throughput calculation
                    let bytes_uploaded = *uploads.get(file_id).unwrap_or(&0);
                    state.upload_throughput.record(bytes_uploaded);
                    let upload_throughput_bps = state.upload_throughput.get_throughput();

                    TaskProgress {
                        file_id: *file_id,
                        last_chunk_id_completed: state.last_chunk_completed,
                        completed_at: if state.completed { Some(Utc::now()) } else { None },
                        bytes_downloaded: state.bytes_downloaded,
                        bytes_uploaded,
                        download_throughput_bps,
                        upload_throughput_bps,
                    }
                })
                .collect();

            let (disk_total, disk_used) = get_disk_stats(&PathBuf::from(&data_dir))
                .map(|(t, u)| (Some(t), Some(u)))
                .unwrap_or((None, None));

            (progress, disk_total, disk_used)
        };

        // Single check-in call - sends status, receives tasks with upstream
        let response = match coordinator.check_in(progress, disk_total, disk_used).await {
            Ok(response) => response,
            Err(e) => {
                tracing::warn!("Failed to check in: {}", e);
                continue;
            }
        };

        // Handle cancel requests
        if !response.cancel_file_ids.is_empty() {
            let mut cancelled = cancelled_files.write().await;
            for file_id in &response.cancel_file_ids {
                if cancelled.insert(*file_id) {
                    tracing::info!("File {} cancelled, will abort download", file_id);
                }
            }
        }

        // Handle purge requests
        for file_id in &response.purge_file_ids {
            // Remove from task states
            {
                let mut states = task_states.write().await;
                states.remove(file_id);
            }
            // Remove from cancelled set
            {
                let mut cancelled = cancelled_files.write().await;
                cancelled.remove(file_id);
            }
            // Remove from downloading set
            {
                let mut downloading = downloading_files.write().await;
                downloading.remove(file_id);
            }
            // Remove from chunk meta
            {
                let mut meta = chunk_meta.write().await;
                meta.remove(file_id);
            }
            // Delete data from storage
            if let Err(e) = storage.delete_file(*file_id).await {
                tracing::warn!("Failed to delete data for purged file {}: {}", file_id, e);
            }
        }

        // Process tasks (with upstream already computed by coordinator)
        if response.tasks.is_empty() {
            continue;
        }

        let file_ids: Vec<_> = response.tasks.iter().map(|t| t.file_id).collect();
        tracing::debug!("Received {} tasks for files: {:?}", response.tasks.len(), file_ids);

        for task in response.tasks {
            let file_id = task.file_id;

            // Skip cancelled files
            {
                let cancelled = cancelled_files.read().await;
                if cancelled.contains(&file_id) {
                    tracing::debug!("Skipping cancelled file {}", file_id);
                    continue;
                }
            }

            // Skip if already downloading
            {
                let downloading = downloading_files.read().await;
                if downloading.contains(&file_id) {
                    tracing::debug!("Skipping file {} - already downloading", file_id);
                    continue;
                }
            }

            // Check if already completed
            {
                let states = task_states.read().await;
                if let Some(state) = states.get(&file_id) {
                    if state.completed {
                        tracing::debug!("Skipping file {} - already completed", file_id);
                        continue;
                    }
                }
            }

            // Store chunk metadata for P2P serving
            {
                let mut meta = chunk_meta.write().await;
                meta.insert(file_id, ChunkMeta {
                    chunk_size: task.chunk_size,
                    total_size: task.total_file_size_bytes,
                    total_chunks: task.total_chunks,
                });
            }

            // Get current progress - check local storage first, then in-memory state
            let start_from_chunk = {
                let states = task_states.read().await;
                match states.get(&file_id) {
                    Some(s) => s.last_chunk_completed + 1,
                    None => {
                        // On restart, check local storage for completed chunks
                        storage.get_last_completed_chunk(file_id, task.chunk_size).await + 1
                    }
                }
            };

            // Skip if already completed
            if start_from_chunk >= task.total_chunks {
                let mut states = task_states.write().await;
                states.entry(file_id).or_insert(TaskState {
                    last_chunk_completed: task.total_chunks - 1,
                    completed: true,
                    bytes_downloaded: task.total_file_size_bytes,
                    download_throughput: ThroughputTracker::new(5.0),
                    upload_throughput: ThroughputTracker::new(5.0),
                }).completed = true;
                continue;
            }

            // Initialize task state if not exists
            {
                let mut states = task_states.write().await;
                states.entry(file_id).or_insert(TaskState {
                    last_chunk_completed: start_from_chunk - 1,
                    completed: false,
                    bytes_downloaded: (start_from_chunk as u64) * task.chunk_size,
                    download_throughput: ThroughputTracker::new(5.0),
                    upload_throughput: ThroughputTracker::new(5.0),
                });
            }

            // Initialize output file if not already done
            if let Err(e) = storage.initialize(file_id, task.chunk_size).await {
                tracing::error!("Failed to initialize file for {}: {}", file_id, e);
                continue;
            }

            // Mark as downloading
            {
                let mut downloading = downloading_files.write().await;
                downloading.insert(file_id);
            }

            // Spawn download task in background
            let downloader = downloader.clone();
            let storage = storage.clone();
            let task_states = task_states.clone();
            let downloading_files = downloading_files.clone();

            tracing::info!(
                "Starting background download for file {} from chunk {}/{}",
                file_id, start_from_chunk, task.total_chunks
            );

            tokio::spawn(async move {
                run_download(
                    file_id,
                    task,
                    start_from_chunk,
                    downloader,
                    storage,
                    task_states,
                    downloading_files,
                ).await;
            });
        }
    }
}

/// Run a download in the background
async fn run_download(
    file_id: Uuid,
    task: DistributionTask,
    start_from_chunk: i32,
    downloader: Arc<Downloader>,
    storage: Arc<ChunkStorage>,
    task_states: Arc<RwLock<HashMap<Uuid, TaskState>>>,
    downloading_files: Arc<RwLock<HashSet<Uuid>>>,
) {
    // Progress callback - update bytes downloaded and chunk progress
    let task_states_clone = task_states.clone();
    let progress_callback = move |chunk_id: i32, _total_chunks: i32, bytes_downloaded: u64, _throughput_bps: Option<u64>| {
        let states = task_states_clone.clone();
        async move {
            let mut states = states.write().await;
            if let Some(state) = states.get_mut(&file_id) {
                state.bytes_downloaded = bytes_downloaded;
                state.last_chunk_completed = chunk_id;
            }
        }
    };

    // Download chunks based on upstream
    let result = match &task.upstream {
        Upstream::Gcs { gcs_path } => {
            downloader
                .download_chunks_from_gcs(
                    file_id,
                    gcs_path,
                    start_from_chunk,
                    task.total_chunks,
                    task.chunk_size,
                    task.total_file_size_bytes,
                    progress_callback,
                )
                .await
        }
        Upstream::Peer { worker_address, .. } => {
            downloader
                .download_chunks_from_peer(
                    worker_address,
                    file_id,
                    start_from_chunk,
                    task.total_chunks,
                    task.chunk_size,
                    task.total_file_size_bytes,
                    progress_callback,
                )
                .await
        }
    };

    // Remove from downloading set
    {
        let mut downloading = downloading_files.write().await;
        downloading.remove(&file_id);
    }

    match result {
        Ok(true) => {
            // Download completed successfully
            {
                let mut states = task_states.write().await;
                if let Some(state) = states.get_mut(&file_id) {
                    state.last_chunk_completed = task.total_chunks - 1;
                    state.bytes_downloaded = task.total_file_size_bytes;
                }
            }

            // Finalize and verify
            if let Err(e) = storage.finalize(file_id).await {
                tracing::error!("Failed to finalize file {}: {}", file_id, e);
                return;
            }

            // Verify CRC32C
            match storage.verify_crc32c(file_id, &task.crc32c).await {
                Ok(true) => {
                    let mut states = task_states.write().await;
                    if let Some(state) = states.get_mut(&file_id) {
                        state.completed = true;
                    }
                    tracing::info!("Completed and verified file {}", file_id);
                }
                Ok(false) => {
                    tracing::error!("CRC32C verification failed for file {} - will need re-download", file_id);
                }
                Err(e) => {
                    tracing::error!("Failed to verify CRC32C for file {}: {}", file_id, e);
                }
            }
        }
        Ok(false) => {
            // Download failed verification - will retry on next poll
            tracing::warn!("Download verification failed for file {} - will retry", file_id);

            // Update state with what we completed
            let last_completed = storage.get_last_completed_chunk(file_id, task.chunk_size).await;
            let mut states = task_states.write().await;
            if let Some(state) = states.get_mut(&file_id) {
                state.last_chunk_completed = last_completed;
            }
        }
        Err(e) => {
            tracing::error!("Download error for file {}: {} - will retry", file_id, e);
        }
    }
}
