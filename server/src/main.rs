mod constants;
mod coordinator_client;
mod downloader;
mod file_service;
mod storage;

use chrono::Utc;
use common::TaskProgress;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::Server;
use uuid::Uuid;

use common::UpstreamType;
use coordinator_client::CoordinatorClient;
use downloader::Downloader;
use file_service::FileService;
use storage::ShardStorage;

struct TaskState {
    /// Last fully completed shard
    last_shard_completed: i32,
    completed: bool,
    /// Current throughput in bytes per second
    throughput_bps: Option<u64>,
    /// Current shard being downloaded
    current_shard_id: Option<i32>,
    /// Progress within current shard (0-100%)
    current_shard_progress_pct: Option<f32>,
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
    let storage = Arc::new(ShardStorage::new(PathBuf::from(&data_dir)));

    // Create coordinator client
    let coordinator = CoordinatorClient::new(coordinator_url, server_addr.clone());

    // Create downloader
    let downloader = Arc::new(Downloader::new(storage.clone())?);

    // Track task states
    let task_states: Arc<RwLock<HashMap<Uuid, TaskState>>> = Arc::new(RwLock::new(HashMap::new()));

    // Spawn gRPC server
    let file_service = FileService::new(storage.clone());
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
    let heartbeat_coordinator = CoordinatorClient::new(
        std::env::var("COORDINATOR_URL").unwrap_or_else(|_| "http://localhost:8080".to_string()),
        server_addr.clone(),
    );
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;

            let states = heartbeat_states.read().await;
            let progress: Vec<TaskProgress> = states
                .iter()
                .map(|(job_id, state)| TaskProgress {
                    deployment_job_id: *job_id,
                    last_shard_id_completed: state.last_shard_completed,
                    completed_at: if state.completed { Some(Utc::now()) } else { None },
                    throughput_bps: state.throughput_bps,
                    current_shard_id: state.current_shard_id,
                    current_shard_progress_pct: state.current_shard_progress_pct,
                })
                .collect();
            drop(states);

            match heartbeat_coordinator.heartbeat(progress).await {
                Ok(response) => {
                    // Handle purge requests
                    for job_id in response.purge_job_ids {
                        // Remove from task states
                        {
                            let mut states = heartbeat_states.write().await;
                            states.remove(&job_id);
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
    let mut poll_interval = tokio::time::interval(Duration::from_secs(2));

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

        tracing::info!("Got {} tasks", tasks.len());

        // Process each task
        for task in tasks {
            let job_id = task.deployment_job_id;

            // Get upstream assignment for this specific job
            let upstream = match coordinator.get_upstream(Some(job_id)).await {
                Ok(upstream) => upstream,
                Err(e) => {
                    tracing::warn!("Failed to get upstream for job {}: {}", job_id, e);
                    continue;
                }
            };

            // Get current progress - check local storage first, then in-memory state
            let start_from_shard = {
                let states = task_states.read().await;
                match states.get(&job_id) {
                    Some(s) => s.last_shard_completed + 1,
                    None => {
                        // On restart, check local storage for completed shards
                        storage.get_last_completed_shard(job_id).await + 1
                    }
                }
            };

            // Skip if already completed
            if start_from_shard >= task.total_shards {
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
                    last_shard_completed: start_from_shard - 1,
                    completed: false,
                    throughput_bps: None,
                    current_shard_id: None,
                    current_shard_progress_pct: None,
                });
            }

            // Connect to peer once if using P2P upstream
            let mut peer_client = if upstream.upstream_type == UpstreamType::Peer {
                match upstream.upstream_peer_address.as_deref() {
                    Some(addr) => match Downloader::connect_to_peer(addr).await {
                        Ok(client) => Some(client),
                        Err(e) => {
                            tracing::warn!("Failed to connect to peer {}: {} - will retry", addr, e);
                            continue; // Skip this task, retry on next poll
                        }
                    },
                    None => {
                        tracing::warn!("No peer address for P2P upstream - will retry");
                        continue;
                    }
                }
            } else {
                None
            };

            // Fetch manifest to get shard paths
            let manifest = match downloader.fetch_manifest(&task.gcs_manifest_path).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("Failed to fetch manifest for job {}: {} - will retry", job_id, e);
                    continue;
                }
            };

            // Download shards
            for shard_id in start_from_shard..task.total_shards {
                // Update current shard being worked on
                {
                    let mut states = task_states.write().await;
                    if let Some(state) = states.get_mut(&job_id) {
                        state.current_shard_id = Some(shard_id);
                        state.current_shard_progress_pct = Some(0.0);
                    }
                }

                // Progress callback for bytes within this shard
                let task_states_clone = task_states.clone();
                let progress_callback = move |bytes_downloaded: u64, total_bytes: u64, throughput_bps: Option<u64>| {
                    let states = task_states_clone.clone();
                    async move {
                        let mut states = states.write().await;
                        if let Some(state) = states.get_mut(&job_id) {
                            state.throughput_bps = throughput_bps;
                            // Calculate progress within shard as percentage
                            let progress_pct = if total_bytes > 0 {
                                (bytes_downloaded as f32 / total_bytes as f32) * 100.0
                            } else {
                                0.0
                            };
                            state.current_shard_progress_pct = Some(progress_pct);
                        }
                    }
                };

                // Get shard info from manifest
                let shard_info = manifest.shards.get(shard_id as usize);
                let shard_path = shard_info
                    .map(|s| s.path.as_str())
                    .unwrap_or_else(|| {
                        tracing::warn!("Shard {} not found in manifest, using default path", shard_id);
                        ""
                    });
                let shard_size = shard_info.map(|s| s.size).unwrap_or(task.shard_size);

                // Download the shard
                let result = match upstream.upstream_type {
                    UpstreamType::Gcs => {
                        downloader
                            .download_shard_from_gcs(
                                job_id,
                                &task.gcs_file_path,
                                shard_path,
                                shard_id,
                                shard_size,
                                progress_callback,
                            )
                            .await
                    }
                    UpstreamType::Peer => {
                        downloader
                            .download_shard_from_peer(
                                peer_client.as_mut().unwrap(),
                                job_id,
                                shard_id,
                                shard_size,
                                progress_callback,
                            )
                            .await
                    }
                };

                match result {
                    Ok(verified) => {
                        if verified {
                            // Shard completed and verified
                            let mut states = task_states.write().await;
                            if let Some(state) = states.get_mut(&job_id) {
                                state.last_shard_completed = shard_id;
                                if shard_id >= task.total_shards - 1 {
                                    state.completed = true;
                                    state.current_shard_id = None;
                                    state.current_shard_progress_pct = None;
                                    tracing::info!("Completed task for job {}", job_id);
                                }
                            }
                        } else {
                            // Verification failed - need to re-download shard
                            tracing::warn!("Shard {} verification failed for job {}, will retry", shard_id, job_id);
                            break; // Exit shard loop, will retry on next poll
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to download shard {} for job {}: {}", shard_id, job_id, e);
                        break; // Exit shard loop, will retry on next poll
                    }
                }
            }
        }
    }
}
