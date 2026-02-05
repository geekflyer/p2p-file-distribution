mod constants;
mod coordinator_client;
mod downloader;
mod file_service;
mod storage;

use chrono::Utc;
use common::TaskProgress;
use nix::sys::statvfs::statvfs;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tonic::transport::Server;
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

use common::UpstreamType;
use coordinator_client::CoordinatorClient;
use downloader::Downloader;
use file_service::{ChunkMeta, ChunkMetaStore, FileService, UploadTracker};
use storage::ChunkStorage;

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
                .add_directive("server=info".parse()?)
                .add_directive("tower_http=debug".parse()?),
        )
        .init();

    // Get configuration from environment
    let grpc_port = std::env::var("GRPC_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50051u16);
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| format!("localhost:{}", grpc_port));
    let coordinator_url =
        std::env::var("COORDINATOR_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());

    tracing::info!("Starting server at: {}", server_addr);
    tracing::info!("Coordinator URL: {}", coordinator_url);
    tracing::info!("Data directory: {}", data_dir);

    // Create storage
    let storage = Arc::new(ChunkStorage::new(PathBuf::from(&data_dir)));

    // Create coordinator client
    let coordinator = CoordinatorClient::new(coordinator_url, server_addr.clone());

    // Create downloader
    let downloader = Arc::new(Downloader::new(storage.clone())?);

    // Track task states
    let task_states: Arc<RwLock<HashMap<Uuid, TaskState>>> = Arc::new(RwLock::new(HashMap::new()));

    // Track cancelled job IDs (to abort in-progress downloads)
    let cancelled_jobs: Arc<RwLock<HashSet<Uuid>>> = Arc::new(RwLock::new(HashSet::new()));

    // Track uploaded bytes per job
    let upload_tracker: UploadTracker = Arc::new(RwLock::new(HashMap::new()));

    // Track chunk metadata for P2P serving
    let chunk_meta: ChunkMetaStore = Arc::new(RwLock::new(HashMap::new()));

    // Spawn gRPC server
    let file_service = FileService::new(storage.clone(), upload_tracker.clone(), chunk_meta.clone());
    let grpc_addr = format!("0.0.0.0:{}", grpc_port).parse()?;
    tokio::spawn(async move {
        tracing::info!("Starting gRPC server on {}", grpc_addr);
        if let Err(e) = Server::builder()
            .add_service(file_service.into_server())
            .serve(grpc_addr)
            .await
        {
            tracing::error!("gRPC server error: {}", e);
        }
    });

    // Spawn heartbeat task
    let heartbeat_states = task_states.clone();
    let heartbeat_storage = storage.clone();
    let heartbeat_upload_tracker = upload_tracker.clone();
    let heartbeat_cancelled = cancelled_jobs.clone();
    let heartbeat_chunk_meta = chunk_meta.clone();
    let heartbeat_data_dir = PathBuf::from(&data_dir);
    let heartbeat_coordinator = CoordinatorClient::new(
        std::env::var("COORDINATOR_URL").unwrap_or_else(|_| "http://localhost:8080".to_string()),
        server_addr.clone(),
    );
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;

            // Need write access to update throughput trackers
            let mut states = heartbeat_states.write().await;
            let uploads = heartbeat_upload_tracker.read().await;
            let progress: Vec<TaskProgress> = states
                .iter_mut()
                .map(|(job_id, state)| {
                    // Use bytes_downloaded for throughput calculation
                    state.download_throughput.record(state.bytes_downloaded);
                    let download_throughput_bps = state.download_throughput.get_throughput();

                    TaskProgress {
                        deployment_job_id: *job_id,
                        last_chunk_id_completed: state.last_chunk_completed,
                        completed_at: if state.completed { Some(Utc::now()) } else { None },
                        bytes_downloaded: state.bytes_downloaded,
                        bytes_uploaded: *uploads.get(job_id).unwrap_or(&0),
                        download_throughput_bps,
                        upload_throughput_bps: None, // TODO: track upload throughput similarly
                    }
                })
                .collect();
            drop(states);
            drop(uploads);

            // Get disk stats
            let (disk_total, disk_used) = get_disk_stats(&heartbeat_data_dir)
                .map(|(t, u)| (Some(t), Some(u)))
                .unwrap_or((None, None));

            match heartbeat_coordinator.heartbeat(progress, disk_total, disk_used).await {
                Ok(response) => {
                    // Handle cancel requests - add to cancelled set so download loop can check
                    if !response.cancel_job_ids.is_empty() {
                        let mut cancelled = heartbeat_cancelled.write().await;
                        for job_id in &response.cancel_job_ids {
                            if cancelled.insert(*job_id) {
                                tracing::info!("Job {} cancelled, will abort download", job_id);
                            }
                        }
                    }

                    // Handle purge requests
                    for job_id in response.purge_job_ids {
                        // Remove from task states
                        {
                            let mut states = heartbeat_states.write().await;
                            states.remove(&job_id);
                        }
                        // Remove from cancelled set
                        {
                            let mut cancelled = heartbeat_cancelled.write().await;
                            cancelled.remove(&job_id);
                        }
                        // Remove from chunk meta
                        {
                            let mut meta = heartbeat_chunk_meta.write().await;
                            meta.remove(&job_id);
                        }
                        // Delete data from storage
                        if let Err(e) = heartbeat_storage.delete_job(job_id).await {
                            tracing::warn!("Failed to delete data for purged job {}: {}", job_id, e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to send heartbeat: {}", e);
                }
            }
        }
    });

    // Main loop: poll for tasks and download
    let mut poll_interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        poll_interval.tick().await;

        // Get tasks from coordinator
        let tasks = match coordinator.get_tasks().await {
            Ok(tasks) => tasks,
            Err(e) => {
                tracing::warn!("Failed to get tasks: {}", e);
                continue;
            }
        };

        if tasks.is_empty() {
            continue;
        }

        let job_ids: Vec<_> = tasks.iter().map(|t| t.deployment_job_id).collect::<Vec<_>>();
        tracing::info!("Received {} tasks for jobs: {:?}", tasks.len(), job_ids);

        // Process each task
        for task in tasks {
            let job_id = task.deployment_job_id;

            // Skip cancelled jobs
            {
                let cancelled = cancelled_jobs.read().await;
                if cancelled.contains(&job_id) {
                    tracing::info!("Skipping cancelled job {}", job_id);
                    continue;
                }
            }

            // Store chunk metadata for P2P serving
            {
                let mut meta = chunk_meta.write().await;
                meta.insert(job_id, ChunkMeta {
                    chunk_size: task.chunk_size,
                    total_size: task.total_size,
                    total_chunks: task.total_chunks,
                });
            }

            // Get upstream assignment for this specific job
            let upstream = match coordinator.get_upstream(Some(job_id)).await {
                Ok(upstream) => upstream,
                Err(e) => {
                    tracing::warn!("Failed to get upstream for job {}: {}", job_id, e);
                    continue;
                }
            };

            // Get current progress - check local storage first, then in-memory state
            let start_from_chunk = {
                let states = task_states.read().await;
                match states.get(&job_id) {
                    Some(s) => s.last_chunk_completed + 1,
                    None => {
                        // On restart, check local storage for completed chunks
                        storage.get_last_completed_chunk(job_id, task.chunk_size).await + 1
                    }
                }
            };

            // Skip if already completed
            if start_from_chunk >= task.total_chunks {
                let mut states = task_states.write().await;
                if let Some(state) = states.get_mut(&job_id) {
                    state.completed = true;
                }
                continue;
            }

            // Initialize task state
            {
                let mut states = task_states.write().await;
                states.entry(job_id).or_insert(TaskState {
                    last_chunk_completed: start_from_chunk - 1,
                    completed: false,
                    bytes_downloaded: 0,
                    download_throughput: ThroughputTracker::new(5.0), // 5 second rolling window
                });
            }

            // Initialize output file if not already done
            if let Err(e) = storage.initialize(job_id, task.chunk_size).await {
                tracing::error!("Failed to initialize file for job {}: {}", job_id, e);
                continue;
            }

            // Progress callback - update bytes downloaded and chunk progress
            let task_states_clone = task_states.clone();
            let progress_callback = move |chunk_id: i32, _total_chunks: i32, bytes_downloaded: u64, _throughput_bps: Option<u64>| {
                let states = task_states_clone.clone();
                async move {
                    let mut states = states.write().await;
                    if let Some(state) = states.get_mut(&job_id) {
                        state.bytes_downloaded = bytes_downloaded;
                        state.last_chunk_completed = chunk_id;
                    }
                }
            };

            // Download chunks
            let result = match upstream.upstream_type {
                UpstreamType::Gcs => {
                    downloader
                        .download_chunks_from_gcs(
                            job_id,
                            &task.gcs_file_path,
                            start_from_chunk,
                            task.total_chunks,
                            task.chunk_size,
                            task.total_size,
                            progress_callback,
                        )
                        .await
                }
                UpstreamType::Peer => {
                    match upstream.upstream_peer_address.as_deref() {
                        Some(addr) => {
                            match Downloader::connect_to_peer(addr).await {
                                Ok(mut client) => {
                                    downloader
                                        .download_chunks_from_peer(
                                            &mut client,
                                            job_id,
                                            start_from_chunk,
                                            task.total_chunks,
                                            task.chunk_size,
                                            task.total_size,
                                            progress_callback,
                                        )
                                        .await
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to connect to peer {}: {} - will retry", addr, e);
                                    continue;
                                }
                            }
                        }
                        None => {
                            tracing::warn!("No peer address for P2P upstream - will retry");
                            continue;
                        }
                    }
                }
            };

            match result {
                Ok(true) => {
                    // Download completed successfully
                    let mut states = task_states.write().await;
                    if let Some(state) = states.get_mut(&job_id) {
                        state.last_chunk_completed = task.total_chunks - 1;
                    }
                    drop(states);

                    // Finalize and verify
                    if let Err(e) = storage.finalize(job_id).await {
                        tracing::error!("Failed to finalize job {}: {}", job_id, e);
                        continue;
                    }

                    // Verify CRC32C
                    match storage.verify_crc32c(job_id, &task.file_crc32c).await {
                        Ok(true) => {
                            let mut states = task_states.write().await;
                            if let Some(state) = states.get_mut(&job_id) {
                                state.completed = true;
                            }
                            tracing::info!("Completed and verified job {}", job_id);
                        }
                        Ok(false) => {
                            tracing::error!("CRC32C verification failed for job {} - will need re-download", job_id);
                            // Don't mark as completed, will retry on next poll
                        }
                        Err(e) => {
                            tracing::error!("Failed to verify CRC32C for job {}: {}", job_id, e);
                        }
                    }
                }
                Ok(false) => {
                    // Download failed verification - will retry on next poll
                    tracing::warn!("Download verification failed for job {} - will retry", job_id);

                    // Update state with what we completed
                    let last_completed = storage.get_last_completed_chunk(job_id, task.chunk_size).await;
                    let mut states = task_states.write().await;
                    if let Some(state) = states.get_mut(&job_id) {
                        state.last_chunk_completed = last_completed;
                    }
                }
                Err(e) => {
                    tracing::error!("Download error for job {}: {} - will retry", job_id, e);
                }
            }
        }
    }
}
