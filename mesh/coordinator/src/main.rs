mod admin_ui;
mod api;
mod db;
mod grpc_service;
mod scheduler;
mod state;

use api::AppState;
use axum::{
    routing::{get, post},
    Router,
};
use common::proto::scheduler_server::SchedulerServer;
use state::{ActiveJob, SchedulerState};
use std::sync::Arc;
use tonic::transport::Server as TonicServer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::from_filename(".env.local").ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("coordinator_mesh=info".parse()?)
                .add_directive("tower_http=debug".parse()?),
        )
        .init();

    let database_url = std::env::var("MESH_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "sqlite:mesh.db?mode=rwc".to_string());
    let http_port: u16 = std::env::var("HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8081);  // Different from pipeline coordinator (8080)
    let grpc_port: u16 = std::env::var("GRPC_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(50050);

    // Create database pool
    info!("Connecting to database...");
    let pool = db::create_pool(&database_url).await?;
    info!("Database connected");

    // Create scheduler state
    let scheduler_state = SchedulerState::new();

    // Check for existing in-progress job or start next pending
    if let Some(job) = db::get_active_job(&pool).await? {
        info!("Resuming active job: {}", job.job_id);
        scheduler::set_active_job(
            &scheduler_state,
            ActiveJob {
                job_id: job.job_id.to_string(),
                total_shards: job.total_shards,
                gcs_base_path: job.gcs_base_path,
            },
        )
        .await;
    }

    // Create shared app state
    let app_state = Arc::new(AppState {
        pool: pool.clone(),
        scheduler_state: scheduler_state.clone(),
    });

    // Spawn scheduler loop
    let sched_state = scheduler_state.clone();
    tokio::spawn(async move {
        scheduler::run_scheduler_loop(sched_state).await;
    });

    // Spawn job queue processor
    let job_state = scheduler_state.clone();
    let job_pool = pool.clone();
    tokio::spawn(async move {
        job_queue_processor(job_pool, job_state).await;
    });

    // Build HTTP router for admin API
    let http_app = Router::new()
        .route("/", get(admin_ui::dashboard))
        .route("/admin/jobs", get(api::list_jobs).post(api::create_job))
        .route("/admin/status", get(api::get_status))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(app_state.clone());

    // Start HTTP server
    let http_addr = format!("0.0.0.0:{}", http_port);
    info!("Starting HTTP admin API on {}", http_addr);
    let http_listener = tokio::net::TcpListener::bind(&http_addr).await?;

    // Start gRPC server
    let grpc_addr = format!("0.0.0.0:{}", grpc_port).parse()?;
    info!("Starting gRPC Scheduler service on {}", grpc_addr);

    let scheduler_service = grpc_service::SchedulerService::new(scheduler_state);

    // Run both servers concurrently
    tokio::select! {
        result = axum::serve(http_listener, http_app) => {
            if let Err(e) = result {
                tracing::error!("HTTP server error: {}", e);
            }
        }
        result = TonicServer::builder()
            .add_service(SchedulerServer::new(scheduler_service))
            .serve(grpc_addr) => {
            if let Err(e) = result {
                tracing::error!("gRPC server error: {}", e);
            }
        }
    }

    Ok(())
}

/// Background task that processes the job queue
async fn job_queue_processor(pool: db::DbPool, state: Arc<SchedulerState>) {
    loop {
        // Check if there's an active job
        let has_active = state.active_job.lock().await.is_some();

        if !has_active {
            // Try to start next pending job
            if let Ok(Some(job)) = db::get_next_pending_job(&pool).await {
                info!("Starting job {} from queue", job.job_id);

                // Mark as in_progress in DB
                if let Err(e) = db::mark_job_in_progress(&pool, job.job_id).await {
                    tracing::error!("Failed to mark job in_progress: {}", e);
                } else {
                    // Set as active job
                    scheduler::set_active_job(
                        &state,
                        ActiveJob {
                            job_id: job.job_id.to_string(),
                            total_shards: job.total_shards,
                            gcs_base_path: job.gcs_base_path,
                        },
                    )
                    .await;
                }
            }
        } else {
            // Check if current job is complete
            if scheduler::check_job_completion(&state).await {
                let job_id = {
                    let active = state.active_job.lock().await;
                    active.as_ref().map(|j| j.job_id.clone())
                };

                if let Some(job_id) = job_id {
                    info!("Job {} completed!", job_id);
                    if let Ok(uuid) = uuid::Uuid::parse_str(&job_id) {
                        if let Err(e) = db::mark_job_completed(&pool, uuid).await {
                            tracing::error!("Failed to mark job completed: {}", e);
                        }
                    }
                    // Clear active job
                    *state.active_job.lock().await = None;
                }
            }
        }

        // Sleep before next check
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
