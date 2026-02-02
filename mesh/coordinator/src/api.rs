use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use common::{CreateJobRequest, CreateJobResponse, GcsManifest};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::sync::Arc;

use crate::db::{self, DbPool};
use crate::state::SchedulerState;

pub struct AppState {
    pub pool: DbPool,
    pub scheduler_state: Arc<SchedulerState>,
}

/// POST /admin/jobs
/// Create a new deployment job (queued)
pub async fn create_job(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateJobRequest>,
) -> impl IntoResponse {
    // Fetch manifest to get shard info
    let manifest = match fetch_manifest(&request.gcs_manifest_path).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Failed to fetch manifest: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to fetch manifest: {}", e),
            )
                .into_response();
        }
    };

    // Derive base path from manifest path
    let gcs_base_path = request
        .gcs_manifest_path
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or(&request.gcs_manifest_path);

    match db::create_job(
        &state.pool,
        &request.gcs_manifest_path,
        gcs_base_path,
        manifest.num_shards,
        manifest.total_size,
        manifest.shard_size,
    )
    .await
    {
        Ok(job_id) => {
            tracing::info!("Created job {} with {} shards", job_id, manifest.num_shards);
            Json(CreateJobResponse {
                deployment_job_id: job_id,
            })
            .into_response()
        }
        Err(e) => {
            tracing::error!("Failed to create job: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// GET /admin/jobs
/// List all jobs
pub async fn list_jobs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match db::get_all_jobs(&state.pool).await {
        Ok(jobs) => Json(jobs).into_response(),
        Err(e) => {
            tracing::error!("Failed to list jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// GET /admin/status
/// Get full scheduler status for dashboard (AJAX refresh)
pub async fn get_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use crate::state::SchedulerState;

    let active_job = state.scheduler_state.active_job.lock().await;
    let server_shards = state.scheduler_state.server_shards.lock().await;
    let pending_tasks = state.scheduler_state.pending_tasks.lock().await;
    let waiting_queue = state.scheduler_state.waiting_for_work.lock().await;
    let shard_availability = state.scheduler_state.shard_availability.lock().await;
    let gcs_in_progress = *state.scheduler_state.gcs_download_in_progress.lock().await;

    let total_shards = active_job.as_ref().map(|j| j.total_shards).unwrap_or(0);

    // Build server data
    let mut servers: Vec<_> = server_shards
        .iter()
        .map(|(addr, bitmap)| {
            let owned_count: i32 = (0..total_shards)
                .filter(|&s| SchedulerState::has_shard(bitmap, s))
                .count() as i32;

            let downloading = pending_tasks
                .iter()
                .find(|(a, _)| *a == addr)
                .map(|(_, t)| t.shard_id);

            let uploading = pending_tasks
                .values()
                .find(|t| t.upstream_server_addr == *addr)
                .map(|t| t.shard_id);

            let is_waiting = waiting_queue.iter().any(|ws| ws.server_address == *addr);

            // Build shard status array
            let shards: Vec<&str> = (0..total_shards)
                .map(|shard_id| {
                    if downloading == Some(shard_id) {
                        "downloading"
                    } else if uploading == Some(shard_id) {
                        "uploading"
                    } else if SchedulerState::has_shard(bitmap, shard_id) {
                        "owned"
                    } else {
                        "missing"
                    }
                })
                .collect();

            serde_json::json!({
                "address": addr,
                "owned_count": owned_count,
                "downloading": downloading,
                "uploading": uploading,
                "is_waiting": is_waiting,
                "shards": shards
            })
        })
        .collect();
    servers.sort_by(|a, b| a["address"].as_str().cmp(&b["address"].as_str()));

    // Build transfers list
    let transfers: Vec<_> = pending_tasks
        .iter()
        .map(|(server, task)| {
            serde_json::json!({
                "server": server,
                "shard_id": task.shard_id,
                "source_type": if task.source_type == 1 { "GCS" } else { "PEER" },
                "upstream": task.upstream_server_addr
            })
        })
        .collect();

    // Build waiting queue
    let waiting: Vec<_> = waiting_queue
        .iter()
        .map(|ws| &ws.server_address)
        .collect();

    // Build shard availability
    let availability: Vec<u32> = (0..total_shards)
        .map(|s| shard_availability.get(&s).copied().unwrap_or(0))
        .collect();

    let status = serde_json::json!({
        "active_job": active_job.as_ref().map(|j| serde_json::json!({
            "job_id": j.job_id,
            "total_shards": j.total_shards,
            "gcs_base_path": j.gcs_base_path
        })),
        "total_shards": total_shards,
        "connected_servers": server_shards.len(),
        "pending_transfers": pending_tasks.len(),
        "gcs_in_progress": gcs_in_progress,
        "servers": servers,
        "transfers": transfers,
        "waiting": waiting,
        "shard_availability": availability
    });

    Json(status).into_response()
}

/// Fetch manifest from GCS
async fn fetch_manifest(gcs_manifest_path: &str) -> anyhow::Result<GcsManifest> {
    let path = gcs_manifest_path
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow::anyhow!("Invalid GCS path"))?;
    let slash_pos = path
        .find('/')
        .ok_or_else(|| anyhow::anyhow!("Invalid GCS path"))?;
    let bucket = &path[..slash_pos];
    let object = &path[slash_pos + 1..];

    if let Ok(emulator_url) = std::env::var("STORAGE_EMULATOR_HOST") {
        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media",
            emulator_url,
            bucket,
            urlencoding::encode(object)
        );
        let data = reqwest::get(&url).await?.bytes().await?.to_vec();
        let manifest: GcsManifest = serde_json::from_slice(&data)?;
        return Ok(manifest);
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

    let object_path = ObjectPath::from(object);
    let data = store.get(&object_path).await?.bytes().await?.to_vec();
    let manifest: GcsManifest = serde_json::from_slice(&data)?;
    Ok(manifest)
}
