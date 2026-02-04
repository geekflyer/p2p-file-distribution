mod downloader;
mod scheduler_client;
mod shard_service;
mod storage;

use common::proto::transfer_task::SourceType;
use downloader::Downloader;
use scheduler_client::SchedulerConnection;
use shard_service::ShardService;
use std::path::PathBuf;
use std::sync::Arc;
use storage::ShardStorage;
use tonic::transport::Server as TonicServer;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::from_filename(".env.local").ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("server_mesh=info".parse()?),
        )
        .init();

    let coordinator_url = std::env::var("COORDINATOR_URL")
        .unwrap_or_else(|_| "http://localhost:50050".to_string());
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| format!("127.0.0.1:{}", std::env::var("GRPC_PORT").unwrap_or_else(|_| "50051".to_string())));
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50051);
    let data_dir = PathBuf::from(
        std::env::var("DATA_DIR").unwrap_or_else(|_| "data/server-mesh".to_string()),
    );

    info!("Server address: {}", server_addr);
    info!("Coordinator URL: {}", coordinator_url);
    info!("Data directory: {:?}", data_dir);

    // Create storage
    let storage = Arc::new(ShardStorage::new(data_dir));
    let downloader = Arc::new(Downloader::new(storage.clone()));

    // Start gRPC server for ShardTransfer service
    let shard_service = ShardService::new(storage.clone());
    let grpc_addr = format!("0.0.0.0:{}", grpc_port).parse()?;

    info!("Starting ShardTransfer gRPC service on {}", grpc_addr);

    let grpc_server = TonicServer::builder()
        .add_service(shard_service.into_server())
        .serve(grpc_addr);

    // Spawn gRPC server
    tokio::spawn(async move {
        if let Err(e) = grpc_server.await {
            error!("gRPC server error: {}", e);
        }
    });

    // Give gRPC server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect to scheduler
    let mut scheduler = loop {
        match SchedulerConnection::connect(&coordinator_url, server_addr.clone()).await {
            Ok(conn) => break conn,
            Err(e) => {
                warn!("Failed to connect to scheduler: {}, retrying in 2s", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    };

    info!("Connected to scheduler");

    // Spawn heartbeat task
    let heartbeat_addr = server_addr.clone();
    let heartbeat_url = coordinator_url.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            match SchedulerConnection::connect(&heartbeat_url, heartbeat_addr.clone()).await {
                Ok(mut conn) => {
                    if let Err(e) = conn.heartbeat().await {
                        warn!("Heartbeat failed: {}", e);
                    }
                }
                Err(e) => {
                    warn!("Failed to connect for heartbeat: {}", e);
                }
            }
        }
    });

    // Main work loop
    let mut current_job_id = String::new();
    let mut total_shards = 0i32;

    loop {
        // Get current shard bitmap
        let shards_owned = if !current_job_id.is_empty() && total_shards > 0 {
            storage.get_shards_owned_bitmap(&current_job_id, total_shards).await
        } else {
            Vec::new()
        };

        // Request work
        let task = match scheduler.get_work(&current_job_id, shards_owned).await {
            Ok(Some(task)) => task,
            Ok(None) => {
                // No work available, wait and retry
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => {
                warn!("Failed to get work: {}, reconnecting", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                scheduler = match SchedulerConnection::connect(&coordinator_url, server_addr.clone()).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        warn!("Failed to reconnect: {}", e);
                        continue;
                    }
                };
                continue;
            }
        };

        // Update job context if changed
        if task.job_id != current_job_id {
            info!("New job: {} ({} shards)", task.job_id, task.total_shards);
            current_job_id = task.job_id.clone();
            total_shards = task.total_shards;
        }

        let source_type = SourceType::try_from(task.source_type).unwrap_or(SourceType::Gcs);
        info!(
            "Assigned: shard {} from {:?} ({})",
            task.shard_id,
            source_type,
            if source_type == SourceType::Gcs { &task.gcs_path } else { &task.upstream_server_addr }
        );

        // Execute transfer
        let success = match source_type {
            SourceType::Gcs => {
                // Use parallel download for production GCS when shard_size is known
                let use_parallel = task.shard_size > 0 && std::env::var("STORAGE_EMULATOR_HOST").is_err();
                let result = if use_parallel {
                    downloader.download_shard_from_gcs_parallel(
                        &task.job_id,
                        &task.gcs_path,
                        task.shard_id,
                        task.shard_size,
                    ).await
                } else {
                    downloader.download_shard_from_gcs(&task.job_id, &task.gcs_path, task.shard_id).await
                };
                match result {
                    Ok(success) => success,
                    Err(e) => {
                        error!("GCS download error: {}", e);
                        false
                    }
                }
            }
            SourceType::Peer => {
                match downloader.download_shard_from_peer(&task.job_id, &task.upstream_server_addr, task.shard_id).await {
                    Ok(success) => success,
                    Err(e) => {
                        error!("Peer download error: {}", e);
                        false
                    }
                }
            }
        };

        if success {
            info!("Completed shard {}", task.shard_id);

            // Report completion
            if let Err(e) = scheduler.report_completion(&task.job_id, task.shard_id).await {
                warn!("Failed to report completion: {}", e);
            }

            // Log progress
            let owned = storage.count_owned_shards(&task.job_id).await;
            info!("Progress: {}/{} shards", owned, total_shards);

            if owned >= total_shards {
                info!("Job {} complete!", task.job_id);
            }
        } else {
            warn!("Failed to download shard {}, will retry", task.shard_id);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }
}
