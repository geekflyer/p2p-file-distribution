use chrono::{DateTime, Utc};
use common::{
    DeploymentJob, DeploymentTask, JobStatus, Server, ServerStatus, ServerTaskProgress, TaskStatus,
};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

pub type DbPool = SqlitePool;

/// Seconds without heartbeat before a server is marked unhealthy
const SERVER_STALE_TIMEOUT_SECS: i32 = 15;

/// Create a new database connection pool
pub async fn create_pool(database_url: &str) -> Result<DbPool, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .max_connections(30)
        .connect(database_url)
        .await?;

    // Run migrations
    init_db(&pool).await?;

    Ok(pool)
}

/// Initialize database schema
async fn init_db(pool: &DbPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS servers (
            server_address TEXT PRIMARY KEY,
            last_heartbeat TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'healthy',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS model_deployment_jobs (
            deployment_job_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            gcs_manifest_path TEXT NOT NULL,
            total_shards INTEGER NOT NULL DEFAULT 0,
            total_size INTEGER NOT NULL DEFAULT 0,
            shard_size INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'created',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS server_model_deployment_tasks (
            server_address TEXT NOT NULL,
            deployment_job_id TEXT NOT NULL REFERENCES model_deployment_jobs(deployment_job_id),
            last_shard_id_completed INTEGER NOT NULL DEFAULT -1,
            current_shard_id INTEGER,
            current_shard_progress_pct REAL,
            prev_bytes_downloaded INTEGER,
            prev_bytes_downloaded_at TEXT,
            now_bytes_downloaded INTEGER,
            now_bytes_downloaded_at TEXT,
            prev_bytes_uploaded INTEGER,
            prev_bytes_uploaded_at TEXT,
            now_bytes_uploaded INTEGER,
            now_bytes_uploaded_at TEXT,
            status TEXT NOT NULL DEFAULT 'created',
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (server_address, deployment_job_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_tasks_server_status ON server_model_deployment_tasks(server_address, status)",
    )
    .execute(pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_jobs_status ON model_deployment_jobs(status)")
        .execute(pool)
        .await?;

    Ok(())
}

// ============ Server Operations ============

/// Upsert server with heartbeat
pub async fn upsert_server(pool: &DbPool, server_address: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO servers (server_address, last_heartbeat, status, updated_at)
        VALUES ($1, datetime('now'), 'healthy', datetime('now'))
        ON CONFLICT (server_address) DO UPDATE
        SET last_heartbeat = datetime('now'), status = 'healthy', updated_at = datetime('now')
        "#,
    )
    .bind(server_address)
    .execute(pool)
    .await?;
    Ok(())
}

/// Get all servers
pub async fn get_all_servers(pool: &DbPool) -> Result<Vec<Server>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, String)>(
        "SELECT server_address, last_heartbeat, status FROM servers ORDER BY server_address",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(server_address, last_heartbeat, status)| {
            let last_heartbeat = DateTime::parse_from_rfc3339(&last_heartbeat)
                .or_else(|_| {
                    // Try parsing SQLite datetime format
                    chrono::NaiveDateTime::parse_from_str(&last_heartbeat, "%Y-%m-%d %H:%M:%S")
                        .map(|dt| dt.and_utc().fixed_offset())
                })
                .ok()?
                .with_timezone(&Utc);
            Some(Server {
                server_address,
                last_heartbeat,
                status: status.parse().unwrap_or(ServerStatus::Unhealthy),
            })
        })
        .collect())
}

/// Get healthy servers ordered by progress on a specific job (for topology building)
/// Returns servers sorted by (last_shard_id_completed DESC, server_address ASC)
pub async fn get_servers_by_job_progress(
    pool: &DbPool,
    job_id: Uuid,
) -> Result<Vec<(String, i32)>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, i32)>(
        r#"
        SELECT s.server_address, COALESCE(t.last_shard_id_completed, -1) as progress
        FROM servers s
        LEFT JOIN server_model_deployment_tasks t
            ON s.server_address = t.server_address
            AND t.deployment_job_id = $1
        WHERE s.status = 'healthy'
        ORDER BY progress DESC, s.server_address ASC
        "#,
    )
    .bind(job_id.to_string())
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Mark servers as unhealthy if heartbeat is stale
pub async fn mark_stale_servers_unhealthy(pool: &DbPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(&format!(
        r#"
        UPDATE servers
        SET status = 'unhealthy', updated_at = datetime('now')
        WHERE status = 'healthy' AND datetime(last_heartbeat) < datetime('now', '-{} seconds')
        "#,
        SERVER_STALE_TIMEOUT_SECS
    ))
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

// ============ Job Operations ============

/// Create a new deployment job
pub async fn create_job(
    pool: &DbPool,
    gcs_manifest_path: &str,
    total_shards: i32,
    total_size: u64,
    shard_size: u64,
) -> Result<Uuid, sqlx::Error> {
    let job_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO model_deployment_jobs (deployment_job_id, gcs_manifest_path, total_shards, total_size, shard_size, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, 'created', datetime('now'), datetime('now'))
        "#,
    )
    .bind(job_id.to_string())
    .bind(gcs_manifest_path)
    .bind(total_shards)
    .bind(total_size as i64)
    .bind(shard_size as i64)
    .execute(pool)
    .await?;

    // Create tasks for all healthy servers
    sqlx::query(
        r#"
        INSERT INTO server_model_deployment_tasks (server_address, deployment_job_id, status, updated_at)
        SELECT server_address, $1, 'created', datetime('now')
        FROM servers WHERE status = 'healthy'
        "#,
    )
    .bind(job_id.to_string())
    .execute(pool)
    .await?;

    Ok(job_id)
}

/// Get all deployment jobs
pub async fn get_all_jobs(pool: &DbPool) -> Result<Vec<DeploymentJob>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, String, String, i32, i64, i64, String)>(
        r#"
        SELECT deployment_job_id, created_at, updated_at, gcs_manifest_path, total_shards, total_size, shard_size, status
        FROM model_deployment_jobs
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(
            |(deployment_job_id, created_at, updated_at, gcs_manifest_path, total_shards, total_size, shard_size, status)| {
                let deployment_job_id = Uuid::parse_str(&deployment_job_id).ok()?;
                let created_at = parse_datetime(&created_at)?;
                let updated_at = parse_datetime(&updated_at)?;
                Some(DeploymentJob {
                    deployment_job_id,
                    created_at,
                    updated_at,
                    gcs_manifest_path,
                    total_shards,
                    total_size: total_size as u64,
                    shard_size: shard_size as u64,
                    status: status.parse().unwrap_or(JobStatus::Failed),
                })
            },
        )
        .collect())
}

/// Get a specific job by ID
pub async fn get_job(pool: &DbPool, job_id: Uuid) -> Result<Option<DeploymentJob>, sqlx::Error> {
    let row = sqlx::query_as::<_, (String, String, String, String, i32, i64, i64, String)>(
        r#"
        SELECT deployment_job_id, created_at, updated_at, gcs_manifest_path, total_shards, total_size, shard_size, status
        FROM model_deployment_jobs
        WHERE deployment_job_id = $1
        "#,
    )
    .bind(job_id.to_string())
    .fetch_optional(pool)
    .await?;

    Ok(row.and_then(
        |(deployment_job_id, created_at, updated_at, gcs_manifest_path, total_shards, total_size, shard_size, status)| {
            let deployment_job_id = Uuid::parse_str(&deployment_job_id).ok()?;
            let created_at = parse_datetime(&created_at)?;
            let updated_at = parse_datetime(&updated_at)?;
            Some(DeploymentJob {
                deployment_job_id,
                created_at,
                updated_at,
                gcs_manifest_path,
                total_shards,
                total_size: total_size as u64,
                shard_size: shard_size as u64,
                status: status.parse().unwrap_or(JobStatus::Failed),
            })
        },
    ))
}

/// Get server progress for a job
pub async fn get_job_server_progress(
    pool: &DbPool,
    job_id: Uuid,
) -> Result<Vec<ServerTaskProgress>, sqlx::Error> {
    // Query includes fields for throughput calculation
    let rows = sqlx::query_as::<_, (String, i32, Option<i32>, Option<f64>, Option<i64>, Option<String>, Option<i64>, Option<String>, Option<i64>, Option<String>, Option<i64>, Option<String>, String, String)>(
        r#"
        SELECT t.server_address, t.last_shard_id_completed, t.current_shard_id, t.current_shard_progress_pct,
               t.prev_bytes_downloaded, t.prev_bytes_downloaded_at,
               t.now_bytes_downloaded, t.now_bytes_downloaded_at,
               t.prev_bytes_uploaded, t.prev_bytes_uploaded_at,
               t.now_bytes_uploaded, t.now_bytes_uploaded_at,
               t.status, COALESCE(s.status, 'unhealthy') as server_status
        FROM server_model_deployment_tasks t
        LEFT JOIN servers s ON t.server_address = s.server_address
        WHERE t.deployment_job_id = $1
        ORDER BY t.server_address
        "#,
    )
    .bind(job_id.to_string())
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(server_address, last_shard_id_completed, current_shard_id, current_shard_progress_pct,
               prev_bytes_dl, prev_bytes_dl_at, now_bytes_dl, now_bytes_dl_at,
               prev_bytes_ul, prev_bytes_ul_at, now_bytes_ul, now_bytes_ul_at,
               status, server_status)| {
            // Calculate download throughput
            let download_throughput_bps = calculate_throughput(
                prev_bytes_dl, prev_bytes_dl_at.as_deref(),
                now_bytes_dl, now_bytes_dl_at.as_deref(),
            );
            // Calculate upload throughput
            let upload_throughput_bps = calculate_throughput(
                prev_bytes_ul, prev_bytes_ul_at.as_deref(),
                now_bytes_ul, now_bytes_ul_at.as_deref(),
            );

            ServerTaskProgress {
                server_address,
                last_shard_id_completed,
                status: status.parse().unwrap_or(TaskStatus::Failed),
                server_status: server_status.parse().unwrap_or(ServerStatus::Unhealthy),
                upstream: None,
                download_throughput_bps,
                upload_throughput_bps,
                current_shard_id,
                current_shard_progress_pct: current_shard_progress_pct.map(|p| p as f32),
            }
        })
        .collect())
}

/// Calculate throughput from prev/now byte counts and timestamps
fn calculate_throughput(
    prev_bytes: Option<i64>,
    prev_at: Option<&str>,
    now_bytes: Option<i64>,
    now_at: Option<&str>,
) -> Option<u64> {
    let prev_bytes = prev_bytes?;
    let now_bytes = now_bytes?;
    let prev_at = parse_datetime(prev_at?)?;
    let now_at = parse_datetime(now_at?)?;

    let bytes_delta = now_bytes.saturating_sub(prev_bytes);
    let time_delta_secs = (now_at - prev_at).num_milliseconds() as f64 / 1000.0;

    if time_delta_secs > 0.0 && bytes_delta > 0 {
        Some((bytes_delta as f64 / time_delta_secs) as u64)
    } else {
        None
    }
}

// ============ Task Operations ============

/// Get non-completed tasks for a server
pub async fn get_server_tasks(
    pool: &DbPool,
    server_address: &str,
) -> Result<Vec<DeploymentTask>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, i32, i64)>(
        r#"
        SELECT j.deployment_job_id, j.gcs_manifest_path, j.total_shards, j.shard_size
        FROM server_model_deployment_tasks t
        JOIN model_deployment_jobs j ON t.deployment_job_id = j.deployment_job_id
        WHERE t.server_address = $1 AND t.status NOT IN ('completed', 'cancelled')
        ORDER BY j.created_at
        "#,
    )
    .bind(server_address)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(deployment_job_id, gcs_manifest_path, total_shards, shard_size)| {
            let deployment_job_id = Uuid::parse_str(&deployment_job_id).ok()?;
            Some(DeploymentTask {
                deployment_job_id,
                gcs_manifest_path,
                total_shards,
                shard_size: shard_size as u64,
            })
        })
        .collect())
}

/// Update task progress - only writes last_shard_id_completed when shard completes
/// Always updates current progress for UI display
/// Shifts now_* values to prev_* before updating with new values
pub async fn update_task_progress(
    pool: &DbPool,
    server_address: &str,
    job_id: Uuid,
    last_shard_id_completed: i32,
    current_shard_id: Option<i32>,
    current_shard_progress_pct: Option<f32>,
    completed: bool,
    bytes_downloaded: u64,
    bytes_uploaded: u64,
) -> Result<(), sqlx::Error> {
    let status = if completed { "completed" } else { "in_progress" };

    // Shift now_* to prev_*, then update now_* with new values
    // Only update tasks that are not in a terminal state (cancelled)
    sqlx::query(
        r#"
        UPDATE server_model_deployment_tasks
        SET last_shard_id_completed = $1,
            current_shard_id = $2,
            current_shard_progress_pct = $3,
            status = $4,
            prev_bytes_downloaded = now_bytes_downloaded,
            prev_bytes_downloaded_at = now_bytes_downloaded_at,
            now_bytes_downloaded = $5,
            now_bytes_downloaded_at = datetime('now'),
            prev_bytes_uploaded = now_bytes_uploaded,
            prev_bytes_uploaded_at = now_bytes_uploaded_at,
            now_bytes_uploaded = $6,
            now_bytes_uploaded_at = datetime('now'),
            updated_at = datetime('now')
        WHERE server_address = $7 AND deployment_job_id = $8
          AND status NOT IN ('cancelled')
        "#,
    )
    .bind(last_shard_id_completed)
    .bind(current_shard_id)
    .bind(current_shard_progress_pct.map(|p| p as f64))
    .bind(status)
    .bind(bytes_downloaded as i64)
    .bind(bytes_uploaded as i64)
    .bind(server_address)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;

    // Check if all tasks for this job are completed
    if completed {
        update_job_status_if_complete(pool, job_id).await?;
    } else {
        // Mark job as in_progress if it's still in created state (not cancelled/purged)
        sqlx::query(
            r#"
            UPDATE model_deployment_jobs
            SET status = 'in_progress', updated_at = datetime('now')
            WHERE deployment_job_id = $1 AND status = 'created'
            "#,
        )
        .bind(job_id.to_string())
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Check and update job status if all tasks are complete
async fn update_job_status_if_complete(pool: &DbPool, job_id: Uuid) -> Result<(), sqlx::Error> {
    let (incomplete_count,): (i32,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM server_model_deployment_tasks
        WHERE deployment_job_id = $1 AND status != 'completed'
        "#,
    )
    .bind(job_id.to_string())
    .fetch_one(pool)
    .await?;

    if incomplete_count == 0 {
        // Only transition to completed if not already in a terminal state
        sqlx::query(
            r#"
            UPDATE model_deployment_jobs
            SET status = 'completed', updated_at = datetime('now')
            WHERE deployment_job_id = $1 AND status IN ('created', 'in_progress')
            "#,
        )
        .bind(job_id.to_string())
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Cancel a job (stops servers from working on it)
pub async fn cancel_job(pool: &DbPool, job_id: Uuid) -> Result<bool, sqlx::Error> {
    // Only cancel jobs that are in progress or created
    let result = sqlx::query(
        r#"
        UPDATE model_deployment_jobs
        SET status = 'cancelled', updated_at = datetime('now')
        WHERE deployment_job_id = $1 AND status IN ('created', 'in_progress')
        "#,
    )
    .bind(job_id.to_string())
    .execute(pool)
    .await?;

    if result.rows_affected() > 0 {
        // Also cancel all associated tasks
        sqlx::query(
            r#"
            UPDATE server_model_deployment_tasks
            SET status = 'cancelled', updated_at = datetime('now')
            WHERE deployment_job_id = $1 AND status != 'completed'
            "#,
        )
        .bind(job_id.to_string())
        .execute(pool)
        .await?;

        Ok(true)
    } else {
        Ok(false)
    }
}

/// Purge a job (mark as purged, servers will delete data)
pub async fn purge_job(pool: &DbPool, job_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        UPDATE model_deployment_jobs
        SET status = 'purged', updated_at = datetime('now')
        WHERE deployment_job_id = $1 AND status IN ('completed', 'failed', 'cancelled')
        "#,
    )
    .bind(job_id.to_string())
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Get all purged job IDs (for servers to clean up)
pub async fn get_purged_job_ids(pool: &DbPool) -> Result<Vec<Uuid>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String,)>(
        "SELECT deployment_job_id FROM model_deployment_jobs WHERE status = 'purged'",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(id,)| Uuid::parse_str(&id).ok())
        .collect())
}

/// Get all cancelled job IDs (for servers to abort in-progress downloads)
pub async fn get_cancelled_job_ids(pool: &DbPool) -> Result<Vec<Uuid>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String,)>(
        "SELECT deployment_job_id FROM model_deployment_jobs WHERE status = 'cancelled'",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(id,)| Uuid::parse_str(&id).ok())
        .collect())
}

/// Parse datetime from SQLite format
fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map(|dt| dt.and_utc())
        })
        .ok()
}
