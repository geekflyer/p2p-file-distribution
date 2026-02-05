use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use common::{
    CheckInRequest, CheckInResponse, CreateDistributionRequest, CreateDistributionResponse,
    DistributionDetails, TaskProgress,
};
use object_store::ObjectStore;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::db::{self, DbPool};

/// Default chunk size: 64MB
const DEFAULT_CHUNK_SIZE: u64 = 64 * 1024 * 1024;

pub struct AppState {
    pub pool: DbPool,
}

// ============ Worker API Handlers ============

/// POST /worker/check-in
/// Combined endpoint: worker sends status + progress, receives tasks with upstream
pub async fn check_in(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CheckInRequest>,
) -> impl IntoResponse {
    // Update worker heartbeat with disk stats
    if let Err(e) = db::upsert_worker(
        &state.pool,
        &request.worker_address,
        request.disk_total_bytes,
        request.disk_used_bytes,
    ).await {
        tracing::error!("Failed to update worker heartbeat: {}", e);
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    // Update task progress
    for TaskProgress {
        file_id,
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
            &request.worker_address,
            file_id,
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

    // Get tasks with upstream computed
    let tasks = match db::build_worker_tasks_with_upstream(&state.pool, &request.worker_address).await {
        Ok(tasks) => tasks,
        Err(e) => {
            tracing::error!("Failed to get worker tasks: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Get list of purged and cancelled file IDs
    let purge_file_ids = db::get_purged_file_ids(&state.pool)
        .await
        .unwrap_or_default();
    let cancel_file_ids = db::get_cancelled_file_ids(&state.pool)
        .await
        .unwrap_or_default();

    Json(CheckInResponse {
        tasks,
        purge_file_ids,
        cancel_file_ids,
    }).into_response()
}

// ============ Admin API Handlers ============

/// GET /admin/workers
/// List all registered workers and health
pub async fn list_workers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match db::get_all_workers(&state.pool).await {
        Ok(workers) => Json(workers).into_response(),
        Err(e) => {
            tracing::error!("Failed to list workers: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /admin/distributions
/// Create file distribution
pub async fn create_distribution(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateDistributionRequest>,
) -> impl IntoResponse {
    // Get chunk size (default 64MB)
    let chunk_size_bytes = request.chunk_size_bytes.unwrap_or(DEFAULT_CHUNK_SIZE);

    // Fetch GCS metadata to get file size and CRC32C
    let (total_size, file_crc32c) = match fetch_gcs_metadata(&request.gcs_path).await {
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
        "Creating distribution for {} ({} bytes, {} chunks of {} bytes, CRC32C: {})",
        request.gcs_path,
        total_size,
        (total_size + chunk_size_bytes - 1) / chunk_size_bytes,
        chunk_size_bytes,
        file_crc32c
    );

    match db::create_distribution(
        &state.pool,
        &request.gcs_path,
        total_size,
        chunk_size_bytes,
        &file_crc32c,
    )
    .await
    {
        Ok(file_id) => Json(CreateDistributionResponse { file_id }).into_response(),
        Err(e) => {
            tracing::error!("Failed to create distribution: {}", e);
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
async fn fetch_gcs_metadata(gcs_path: &str) -> anyhow::Result<(u64, String)> {
    // Parse gs://bucket/path
    let path = gcs_path
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

/// GET /admin/distributions
/// List all file distributions with status
pub async fn list_distributions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match db::get_all_distributions(&state.pool).await {
        Ok(distributions) => Json(distributions).into_response(),
        Err(e) => {
            tracing::error!("Failed to list distributions: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// GET /admin/distributions/{id}
/// Get distribution details with per-worker progress and upstream assignments
pub async fn get_distribution_details(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<Uuid>,
) -> impl IntoResponse {
    let distribution = match db::get_distribution(&state.pool, file_id).await {
        Ok(Some(dist)) => dist,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, "Distribution not found").into_response();
        }
        Err(e) => {
            tracing::error!("Failed to get distribution: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let mut worker_progress = match db::get_distribution_worker_progress(&state.pool, file_id).await {
        Ok(progress) => progress,
        Err(e) => {
            tracing::error!("Failed to get distribution progress: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Get healthy workers ordered by progress for upstream computation
    let healthy_workers = match db::get_workers_by_distribution_progress(&state.pool, file_id).await {
        Ok(workers) => workers,
        Err(e) => {
            tracing::error!("Failed to get workers by progress: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Build position lookup map: O(n) instead of O(n^2) for large worker counts
    let position_map: HashMap<&str, usize> = healthy_workers
        .iter()
        .enumerate()
        .map(|(i, (addr, _))| (addr.as_str(), i))
        .collect();

    // Compute upstream for each worker in progress list
    for progress in &mut worker_progress {
        progress.upstream = match position_map.get(progress.worker_address.as_str()) {
            Some(0) => Some(format!("GCS({})", distribution.gcs_path)),
            Some(&n) => Some(format!("Peer({})", healthy_workers[n - 1].0)),
            None => None, // Not in healthy list
        };
    }

    Json(DistributionDetails {
        distribution,
        worker_progress,
    })
    .into_response()
}

/// POST /admin/distributions/{id}/cancel
/// Cancel a running distribution (stops workers from working on it)
pub async fn cancel_distribution(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<Uuid>,
) -> impl IntoResponse {
    match db::cancel_distribution(&state.pool, file_id).await {
        Ok(true) => {
            tracing::info!("Distribution {} cancelled", file_id);
            StatusCode::OK.into_response()
        }
        Ok(false) => (
            StatusCode::BAD_REQUEST,
            "Distribution not found or not in created/in_progress state",
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Failed to cancel distribution: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /admin/distributions/{id}/purge
/// Purge a completed, failed, or cancelled distribution (marks for data deletion on workers)
pub async fn purge_distribution(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<Uuid>,
) -> impl IntoResponse {
    match db::purge_distribution(&state.pool, file_id).await {
        Ok(true) => {
            tracing::info!("Distribution {} marked as purged", file_id);
            StatusCode::OK.into_response()
        }
        Ok(false) => (
            StatusCode::BAD_REQUEST,
            "Distribution not found or not in completed/failed state",
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Failed to purge distribution: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
