use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use common::{
    CreateJobRequest, CreateJobResponse, HeartbeatRequest, HeartbeatResponse,
    JobDetails, TaskProgress, UpstreamAssignment, UpstreamType,
};
use object_store::ObjectStore;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::db::{self, DbPool};

/// Default chunk size: 16MB
const DEFAULT_CHUNK_SIZE: u64 = 16 * 1024 * 1024;

pub struct AppState {
    pub pool: DbPool,
}

// ============ Server API Handlers ============

#[derive(Deserialize)]
pub struct ServerQuery {
    #[serde(rename = "serverAddress")]
    pub server_address: String,
}

/// GET /server/model-deployment-tasks?serverAddress={addr}
/// Returns non-completed tasks for the requesting server
pub async fn get_deployment_tasks(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ServerQuery>,
) -> impl IntoResponse {
    match db::get_server_tasks(&state.pool, &query.server_address).await {
        Ok(tasks) => Json(tasks).into_response(),
        Err(e) => {
            tracing::error!("Failed to get tasks: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct UpstreamQuery {
    #[serde(rename = "jobId")]
    pub job_id: Option<Uuid>,
}

/// GET /server/upstream/{serverAddress}?jobId={jobId}
/// Returns assigned upstream for the server based on job progress topology
pub async fn get_upstream(
    State(state): State<Arc<AppState>>,
    Path(server_address): Path<String>,
    Query(query): Query<UpstreamQuery>,
) -> impl IntoResponse {
    let job_id = match query.job_id {
        Some(id) => id,
        None => {
            return (StatusCode::BAD_REQUEST, "jobId query parameter is required").into_response();
        }
    };

    match db::get_servers_by_job_progress(&state.pool, job_id).await {
        Ok(servers) => {
            // servers is sorted by (progress DESC, address ASC)
            // Find position of requesting server
            let position = servers.iter().position(|(addr, _)| addr == &server_address);

            let assignment = match position {
                Some(0) => {
                    // First server (most progress) gets GCS as upstream
                    UpstreamAssignment {
                        upstream_type: UpstreamType::Gcs,
                        upstream_peer_address: None,
                    }
                }
                Some(n) => {
                    // Other servers get predecessor (more progress) as upstream
                    UpstreamAssignment {
                        upstream_type: UpstreamType::Peer,
                        upstream_peer_address: Some(servers[n - 1].0.clone()),
                    }
                }
                None => {
                    // Server not found in healthy list, default to GCS
                    UpstreamAssignment {
                        upstream_type: UpstreamType::Gcs,
                        upstream_peer_address: None,
                    }
                }
            };

            Json(assignment).into_response()
        }
        Err(e) => {
            tracing::error!("Failed to get upstream for job: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /server/heartbeat
/// Report server health and task progress
pub async fn heartbeat(
    State(state): State<Arc<AppState>>,
    Json(request): Json<HeartbeatRequest>,
) -> impl IntoResponse {
    // Update server heartbeat with disk stats
    if let Err(e) = db::upsert_server(
        &state.pool,
        &request.server_address,
        request.disk_total_bytes,
        request.disk_used_bytes,
    ).await {
        tracing::error!("Failed to update server heartbeat: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    // Update task progress
    for TaskProgress {
        deployment_job_id,
        last_chunk_id_completed,
        completed_at,
        bytes_downloaded,
        bytes_uploaded,
        download_throughput_bps,
        upload_throughput_bps,
    } in request.task_progress
    {
        let completed = completed_at.is_some();
        if let Err(e) = db::update_task_progress(
            &state.pool,
            &request.server_address,
            deployment_job_id,
            last_chunk_id_completed,
            completed,
            bytes_downloaded,
            bytes_uploaded,
            download_throughput_bps,
            upload_throughput_bps,
        )
        .await
        {
            tracing::error!("Failed to update task progress: {}", e);
        }
    }

    // Return list of purged jobs for cleanup and cancelled jobs to abort
    let purge_job_ids = db::get_purged_job_ids(&state.pool)
        .await
        .unwrap_or_default();
    let cancel_job_ids = db::get_cancelled_job_ids(&state.pool)
        .await
        .unwrap_or_default();

    Json(HeartbeatResponse { purge_job_ids, cancel_job_ids }).into_response()
}

// ============ Admin API Handlers ============

/// GET /admin/servers
/// List all registered servers and health
pub async fn list_servers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match db::get_all_servers(&state.pool).await {
        Ok(servers) => Json(servers).into_response(),
        Err(e) => {
            tracing::error!("Failed to list servers: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /admin/jobs
/// Create deployment job
pub async fn create_job(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateJobRequest>,
) -> impl IntoResponse {
    // Get chunk size (default 16MB)
    let chunk_size = request.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);

    // Fetch GCS metadata to get file size and CRC32C
    let (total_size, file_crc32c) = match fetch_gcs_metadata(&request.gcs_file_path).await {
        Ok(meta) => meta,
        Err(e) => {
            tracing::error!("Failed to fetch GCS metadata: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to fetch GCS metadata: {}", e),
            )
                .into_response();
        }
    };

    tracing::info!(
        "Creating job for {} ({} bytes, {} chunks of {} bytes, CRC32C: {})",
        request.gcs_file_path,
        total_size,
        (total_size + chunk_size - 1) / chunk_size,
        chunk_size,
        file_crc32c
    );

    match db::create_job(
        &state.pool,
        &request.gcs_file_path,
        total_size,
        chunk_size,
        &file_crc32c,
    )
    .await
    {
        Ok(job_id) => Json(CreateJobResponse {
            deployment_job_id: job_id,
        })
        .into_response(),
        Err(e) => {
            tracing::error!("Failed to create job: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// GCS object metadata response (for emulator)
#[derive(Deserialize)]
struct GcsObjectMetadata {
    size: String,
    #[serde(default)]
    crc32c: String,
}

/// Fetch GCS file metadata (size and CRC32C)
async fn fetch_gcs_metadata(gcs_file_path: &str) -> anyhow::Result<(u64, String)> {
    // Parse gs://bucket/path
    let path = gcs_file_path
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow::anyhow!("Invalid GCS path"))?;
    let slash_pos = path
        .find('/')
        .ok_or_else(|| anyhow::anyhow!("Invalid GCS path"))?;
    let bucket = &path[..slash_pos];
    let object = &path[slash_pos + 1..];

    // Check for emulator
    if let Ok(emulator_url) = std::env::var("STORAGE_EMULATOR_HOST") {
        // For fake-gcs-server: use metadata endpoint
        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            emulator_url,
            bucket,
            urlencoding::encode(object)
        );
        let response = reqwest::get(&url).await?;
        if !response.status().is_success() {
            anyhow::bail!("GCS emulator returned {}: {}", response.status(), response.text().await?);
        }
        let meta: GcsObjectMetadata = response.json().await?;
        let size = meta.size.parse::<u64>()?;
        // Emulator may not return CRC32C, use empty string as placeholder
        let crc32c = if meta.crc32c.is_empty() { String::new() } else { meta.crc32c };
        return Ok((size, crc32c));
    }

    // Production: use object_store for metadata
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
    let meta = store.head(&object_path).await?;
    let size = meta.size as u64;

    // object_store doesn't expose CRC32C directly, so we need to fetch it via the JSON API
    let crc32c = fetch_crc32c_via_api(bucket, object).await.unwrap_or_default();

    Ok((size, crc32c))
}

/// Fetch CRC32C via Google Cloud Storage JSON API
async fn fetch_crc32c_via_api(bucket: &str, object: &str) -> anyhow::Result<String> {
    // Use the JSON API to get CRC32C
    // This requires authentication, which we get from ADC or service account
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}?fields=crc32c",
        bucket,
        urlencoding::encode(object)
    );

    // Try to get credentials for the API call
    let client = reqwest::Client::new();
    let token = get_gcs_access_token().await?;

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("GCS API returned {}", response.status());
    }

    #[derive(Deserialize)]
    struct Crc32cResponse {
        crc32c: String,
    }

    let resp: Crc32cResponse = response.json().await?;
    Ok(resp.crc32c)
}

/// Get GCS access token from ADC or service account
async fn get_gcs_access_token() -> anyhow::Result<String> {
    // Try service account file first
    if let Ok(creds_path) = std::env::var("GCS_SERVICE_ACCOUNT_PATH") {
        let creds = std::fs::read_to_string(&creds_path)?;
        let creds: serde_json::Value = serde_json::from_str(&creds)?;

        // For service account, we need to create a JWT and exchange it for an access token
        // This is complex, so let's use the gcloud CLI if available
        if let Ok(output) = tokio::process::Command::new("gcloud")
            .args(["auth", "print-access-token"])
            .output()
            .await
        {
            if output.status.success() {
                let token = String::from_utf8(output.stdout)?.trim().to_string();
                return Ok(token);
            }
        }

        // If gcloud fails, try to extract token from service account JSON
        if let Some(token) = creds.get("access_token").and_then(|v| v.as_str()) {
            return Ok(token.to_string());
        }
    }

    // Try Application Default Credentials via gcloud
    let output = tokio::process::Command::new("gcloud")
        .args(["auth", "application-default", "print-access-token"])
        .output()
        .await?;

    if output.status.success() {
        let token = String::from_utf8(output.stdout)?.trim().to_string();
        return Ok(token);
    }

    // Try regular gcloud auth
    let output = tokio::process::Command::new("gcloud")
        .args(["auth", "print-access-token"])
        .output()
        .await?;

    if output.status.success() {
        let token = String::from_utf8(output.stdout)?.trim().to_string();
        return Ok(token);
    }

    anyhow::bail!("Could not get GCS access token")
}

/// GET /admin/jobs
/// List all deployment jobs with status
pub async fn list_jobs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match db::get_all_jobs(&state.pool).await {
        Ok(jobs) => Json(jobs).into_response(),
        Err(e) => {
            tracing::error!("Failed to list jobs: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// GET /admin/jobs/{id}
/// Get job details with per-server progress and upstream assignments
pub async fn get_job_details(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<Uuid>,
) -> impl IntoResponse {
    let job = match db::get_job(&state.pool, job_id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, "Job not found").into_response();
        }
        Err(e) => {
            tracing::error!("Failed to get job: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let mut server_progress = match db::get_job_server_progress(&state.pool, job_id).await {
        Ok(progress) => progress,
        Err(e) => {
            tracing::error!("Failed to get job progress: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Get healthy servers ordered by progress for upstream computation
    let healthy_servers = match db::get_servers_by_job_progress(&state.pool, job_id).await {
        Ok(servers) => servers,
        Err(e) => {
            tracing::error!("Failed to get servers by progress: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Build position lookup map: O(n) instead of O(n²) for large server counts
    let position_map: HashMap<&str, usize> = healthy_servers
        .iter()
        .enumerate()
        .map(|(i, (addr, _))| (addr.as_str(), i))
        .collect();

    // Compute upstream for each server in progress list
    for progress in &mut server_progress {
        progress.upstream = match position_map.get(progress.server_address.as_str()) {
            Some(0) => Some(format!("GCS({})", job.gcs_file_path)),
            Some(&n) => Some(format!("⬆️ Peer({})", healthy_servers[n - 1].0)),
            None => None, // Not in healthy list
        };
    }

    Json(JobDetails {
        job,
        server_progress,
    })
    .into_response()
}

/// POST /admin/jobs/{id}/cancel
/// Cancel a running job (stops servers from working on it)
pub async fn cancel_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<Uuid>,
) -> impl IntoResponse {
    match db::cancel_job(&state.pool, job_id).await {
        Ok(true) => {
            tracing::info!("Job {} cancelled", job_id);
            StatusCode::OK.into_response()
        }
        Ok(false) => (
            StatusCode::BAD_REQUEST,
            "Job not found or not in created/in_progress state",
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Failed to cancel job: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /admin/jobs/{id}/purge
/// Purge a completed, failed, or cancelled job (marks for data deletion on servers)
pub async fn purge_job(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<Uuid>,
) -> impl IntoResponse {
    match db::purge_job(&state.pool, job_id).await {
        Ok(true) => {
            tracing::info!("Job {} marked as purged", job_id);
            StatusCode::OK.into_response()
        }
        Ok(false) => (
            StatusCode::BAD_REQUEST,
            "Job not found or not in completed/failed state",
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Failed to purge job: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
