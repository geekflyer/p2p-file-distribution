use chrono::{DateTime, Utc};
use common::{
    DistributionStatus, DistributionTask, FileDistribution, Upstream, Worker, WorkerStatus,
    WorkerTaskProgress, TaskStatus,
};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

pub type DbPool = SqlitePool;

/// Seconds without heartbeat before a worker is marked unhealthy
const WORKER_STALE_TIMEOUT_SECS: i32 = 15;

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
        CREATE TABLE IF NOT EXISTS workers (
            worker_address TEXT PRIMARY KEY,
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
        CREATE TABLE IF NOT EXISTS file_distributions (
            file_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            gcs_path TEXT NOT NULL,
            total_chunks INTEGER NOT NULL DEFAULT 0,
            total_size INTEGER NOT NULL DEFAULT 0,
            chunk_size INTEGER NOT NULL DEFAULT 0,
            file_crc32c TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'created',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS distribution_tasks (
            worker_address TEXT NOT NULL,
            file_id TEXT NOT NULL REFERENCES file_distributions(file_id),
            last_chunk_id_completed INTEGER NOT NULL DEFAULT -1,
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
            PRIMARY KEY (worker_address, file_id)
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_tasks_worker_status ON distribution_tasks(worker_address, status)",
    )
    .execute(pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_distributions_status ON file_distributions(status)")
        .execute(pool)
        .await?;

    // Migration: Add throughput columns (reported by worker, not calculated)
    sqlx::query("ALTER TABLE distribution_tasks ADD COLUMN download_throughput_bps INTEGER")
        .execute(pool)
        .await
        .ok(); // Ignore error if column already exists

    sqlx::query("ALTER TABLE distribution_tasks ADD COLUMN upload_throughput_bps INTEGER")
        .execute(pool)
        .await
        .ok(); // Ignore error if column already exists

    // Migration: Add disk stats columns to workers
    sqlx::query("ALTER TABLE workers ADD COLUMN disk_total_bytes INTEGER")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE workers ADD COLUMN disk_used_bytes INTEGER")
        .execute(pool)
        .await
        .ok();

    Ok(())
}

// ============ Worker Operations ============

/// Upsert worker with heartbeat and disk stats
pub async fn upsert_worker(
    pool: &DbPool,
    worker_address: &str,
    disk_total_bytes: Option<u64>,
    disk_used_bytes: Option<u64>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO workers (worker_address, last_heartbeat, status, updated_at, disk_total_bytes, disk_used_bytes)
        VALUES ($1, datetime('now'), 'healthy', datetime('now'), $2, $3)
        ON CONFLICT (worker_address) DO UPDATE
        SET last_heartbeat = datetime('now'), status = 'healthy', updated_at = datetime('now'),
            disk_total_bytes = $2, disk_used_bytes = $3
        "#,
    )
    .bind(worker_address)
    .bind(disk_total_bytes.map(|v| v as i64))
    .bind(disk_used_bytes.map(|v| v as i64))
    .execute(pool)
    .await?;
    Ok(())
}

/// Get all workers
pub async fn get_all_workers(pool: &DbPool) -> Result<Vec<Worker>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, String, Option<i64>, Option<i64>)>(
        "SELECT worker_address, last_heartbeat, status, disk_total_bytes, disk_used_bytes FROM workers ORDER BY worker_address",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(worker_address, last_heartbeat, status, disk_total, disk_used)| {
            let last_heartbeat = DateTime::parse_from_rfc3339(&last_heartbeat)
                .or_else(|_| {
                    // Try parsing SQLite datetime format
                    chrono::NaiveDateTime::parse_from_str(&last_heartbeat, "%Y-%m-%d %H:%M:%S")
                        .map(|dt| dt.and_utc().fixed_offset())
                })
                .ok()?
                .with_timezone(&Utc);
            Some(Worker {
                worker_address,
                last_heartbeat,
                status: status.parse().unwrap_or(WorkerStatus::Unhealthy),
                disk_total_bytes: disk_total.map(|v| v as u64),
                disk_used_bytes: disk_used.map(|v| v as u64),
            })
        })
        .collect())
}

/// Get healthy workers ordered by progress on a specific distribution (for topology building)
/// Returns workers sorted by (last_chunk_id_completed DESC, worker_address ASC)
pub async fn get_workers_by_distribution_progress(
    pool: &DbPool,
    file_id: Uuid,
) -> Result<Vec<(String, i32)>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, i32)>(
        r#"
        SELECT w.worker_address, COALESCE(t.last_chunk_id_completed, -1) as progress
        FROM workers w
        LEFT JOIN distribution_tasks t
            ON w.worker_address = t.worker_address
            AND t.file_id = $1
        WHERE w.status = 'healthy'
        ORDER BY progress DESC, w.worker_address ASC
        "#,
    )
    .bind(file_id.to_string())
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Mark workers as unhealthy if heartbeat is stale
pub async fn mark_stale_workers_unhealthy(pool: &DbPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(&format!(
        r#"
        UPDATE workers
        SET status = 'unhealthy', updated_at = datetime('now')
        WHERE status = 'healthy' AND datetime(last_heartbeat) < datetime('now', '-{} seconds')
        "#,
        WORKER_STALE_TIMEOUT_SECS
    ))
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

// ============ Distribution Operations ============

/// Create a new file distribution
pub async fn create_distribution(
    pool: &DbPool,
    gcs_path: &str,
    total_size: u64,
    chunk_size: u64,
    file_crc32c: &str,
) -> Result<Uuid, sqlx::Error> {
    let file_id = Uuid::new_v4();
    let total_chunks = ((total_size + chunk_size - 1) / chunk_size) as i32;

    sqlx::query(
        r#"
        INSERT INTO file_distributions (file_id, gcs_path, total_chunks, total_size, chunk_size, file_crc32c, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, 'created', datetime('now'), datetime('now'))
        "#,
    )
    .bind(file_id.to_string())
    .bind(gcs_path)
    .bind(total_chunks)
    .bind(total_size as i64)
    .bind(chunk_size as i64)
    .bind(file_crc32c)
    .execute(pool)
    .await?;

    // Create tasks for all healthy workers
    sqlx::query(
        r#"
        INSERT INTO distribution_tasks (worker_address, file_id, status, updated_at)
        SELECT worker_address, $1, 'created', datetime('now')
        FROM workers WHERE status = 'healthy'
        "#,
    )
    .bind(file_id.to_string())
    .execute(pool)
    .await?;

    Ok(file_id)
}

/// Get all file distributions
pub async fn get_all_distributions(pool: &DbPool) -> Result<Vec<FileDistribution>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, String, String, i32, i64, i64, String, String)>(
        r#"
        SELECT file_id, created_at, updated_at, gcs_path, total_chunks, total_size, chunk_size, COALESCE(file_crc32c, '') as file_crc32c, status
        FROM file_distributions
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(
            |(file_id, created_at, updated_at, gcs_path, total_chunks, total_size, chunk_size, file_crc32c, status)| {
                let file_id = Uuid::parse_str(&file_id).ok()?;
                let created_at = parse_datetime(&created_at)?;
                let updated_at = parse_datetime(&updated_at)?;
                Some(FileDistribution {
                    file_id,
                    created_at,
                    updated_at,
                    gcs_path,
                    total_chunks,
                    total_size: total_size as u64,
                    chunk_size: chunk_size as u64,
                    file_crc32c,
                    status: status.parse().unwrap_or(DistributionStatus::Failed),
                })
            },
        )
        .collect())
}

/// Get a specific distribution by ID
pub async fn get_distribution(pool: &DbPool, file_id: Uuid) -> Result<Option<FileDistribution>, sqlx::Error> {
    let row = sqlx::query_as::<_, (String, String, String, String, i32, i64, i64, String, String)>(
        r#"
        SELECT file_id, created_at, updated_at, gcs_path, total_chunks, total_size, chunk_size, COALESCE(file_crc32c, '') as file_crc32c, status
        FROM file_distributions
        WHERE file_id = $1
        "#,
    )
    .bind(file_id.to_string())
    .fetch_optional(pool)
    .await?;

    Ok(row.and_then(
        |(file_id, created_at, updated_at, gcs_path, total_chunks, total_size, chunk_size, file_crc32c, status)| {
            let file_id = Uuid::parse_str(&file_id).ok()?;
            let created_at = parse_datetime(&created_at)?;
            let updated_at = parse_datetime(&updated_at)?;
            Some(FileDistribution {
                file_id,
                created_at,
                updated_at,
                gcs_path,
                total_chunks,
                total_size: total_size as u64,
                chunk_size: chunk_size as u64,
                file_crc32c,
                status: status.parse().unwrap_or(DistributionStatus::Failed),
            })
        },
    ))
}

/// Get worker progress for a distribution
pub async fn get_distribution_worker_progress(
    pool: &DbPool,
    file_id: Uuid,
) -> Result<Vec<WorkerTaskProgress>, sqlx::Error> {
    // Query returns worker-reported throughput directly (no calculation needed)
    let rows = sqlx::query_as::<_, (String, i32, Option<i64>, Option<i64>, String, String)>(
        r#"
        SELECT t.worker_address, t.last_chunk_id_completed,
               t.download_throughput_bps, t.upload_throughput_bps,
               t.status, COALESCE(w.status, 'unhealthy') as worker_status
        FROM distribution_tasks t
        LEFT JOIN workers w ON t.worker_address = w.worker_address
        WHERE t.file_id = $1
        ORDER BY t.worker_address
        "#,
    )
    .bind(file_id.to_string())
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(worker_address, last_chunk_id_completed,
               download_throughput_bps, upload_throughput_bps, status, worker_status)| {
            WorkerTaskProgress {
                worker_address,
                last_chunk_id_completed,
                status: status.parse().unwrap_or(TaskStatus::Failed),
                worker_status: worker_status.parse().unwrap_or(WorkerStatus::Unhealthy),
                upstream: None,
                download_throughput_bps: download_throughput_bps.map(|v| v as u64),
                upload_throughput_bps: upload_throughput_bps.map(|v| v as u64),
            }
        })
        .collect())
}

// ============ Task Operations ============

/// Get non-completed tasks for a worker (with upstream computed)
pub async fn get_worker_tasks(
    pool: &DbPool,
    worker_address: &str,
) -> Result<Vec<(Uuid, String, i32, u64, u64, String)>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, i32, i64, i64, String)>(
        r#"
        SELECT d.file_id, d.gcs_path, d.total_chunks, d.total_size, d.chunk_size, COALESCE(d.file_crc32c, '') as file_crc32c
        FROM distribution_tasks t
        JOIN file_distributions d ON t.file_id = d.file_id
        WHERE t.worker_address = $1 AND t.status NOT IN ('completed', 'cancelled')
        ORDER BY d.created_at
        "#,
    )
    .bind(worker_address)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(file_id, gcs_path, total_chunks, total_size, chunk_size, file_crc32c)| {
            let file_id = Uuid::parse_str(&file_id).ok()?;
            Some((file_id, gcs_path, total_chunks, total_size as u64, chunk_size as u64, file_crc32c))
        })
        .collect())
}

/// Build tasks with upstream for a worker (used in check-in response)
pub async fn build_worker_tasks_with_upstream(
    pool: &DbPool,
    worker_address: &str,
) -> Result<Vec<DistributionTask>, sqlx::Error> {
    let raw_tasks = get_worker_tasks(pool, worker_address).await?;
    let mut tasks = Vec::with_capacity(raw_tasks.len());

    for (file_id, gcs_path, total_chunks, total_size, chunk_size, file_crc32c) in raw_tasks {
        // Get healthy workers ordered by progress for this distribution
        let workers = get_workers_by_distribution_progress(pool, file_id).await?;

        // Find position of requesting worker
        let position = workers.iter().position(|(addr, _)| addr == worker_address);

        let upstream = match position {
            Some(0) => {
                // First worker (most progress) gets GCS as upstream
                Upstream::Gcs { gcs_path: gcs_path.clone() }
            }
            Some(n) => {
                // Other workers get predecessor (more progress) as upstream
                let peer_addr = workers[n - 1].0.clone();
                Upstream::Peer {
                    worker_id: peer_addr.clone(),
                    worker_address: peer_addr,
                }
            }
            None => {
                // Worker not found in healthy list, default to GCS
                Upstream::Gcs { gcs_path: gcs_path.clone() }
            }
        };

        tasks.push(DistributionTask {
            task_id: format!("{}-{}", file_id, worker_address),
            file_id,
            total_chunks,
            chunk_size,
            total_file_size_bytes: total_size,
            crc32c: file_crc32c,
            upstream,
        });
    }

    Ok(tasks)
}

/// Update task progress - only writes last_chunk_id_completed when chunk completes
/// Stores worker-reported throughput directly (no longer calculated from deltas)
pub async fn update_task_progress(
    pool: &DbPool,
    worker_address: &str,
    file_id: Uuid,
    last_chunk_id_completed: i32,
    completed: bool,
    bytes_downloaded: u64,
    bytes_uploaded: u64,
    download_throughput_bps: Option<u64>,
    upload_throughput_bps: Option<u64>,
) -> Result<(), sqlx::Error> {
    let status = if completed { "completed" } else { "in_progress" };

    // Store worker-reported throughput directly
    // Only update tasks that are not in a terminal state (cancelled)
    sqlx::query(
        r#"
        UPDATE distribution_tasks
        SET last_chunk_id_completed = $1,
            status = $2,
            now_bytes_downloaded = $3,
            now_bytes_uploaded = $4,
            download_throughput_bps = $5,
            upload_throughput_bps = $6,
            updated_at = datetime('now')
        WHERE worker_address = $7 AND file_id = $8
          AND status NOT IN ('cancelled')
        "#,
    )
    .bind(last_chunk_id_completed)
    .bind(status)
    .bind(bytes_downloaded as i64)
    .bind(bytes_uploaded as i64)
    .bind(download_throughput_bps.map(|v| v as i64))
    .bind(upload_throughput_bps.map(|v| v as i64))
    .bind(worker_address)
    .bind(file_id.to_string())
    .execute(pool)
    .await?;

    // Check if all tasks for this distribution are completed
    if completed {
        update_distribution_status_if_complete(pool, file_id).await?;
    } else {
        // Mark distribution as in_progress if it's still in created state (not cancelled/purged)
        sqlx::query(
            r#"
            UPDATE file_distributions
            SET status = 'in_progress', updated_at = datetime('now')
            WHERE file_id = $1 AND status = 'created'
            "#,
        )
        .bind(file_id.to_string())
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Check and update distribution status if all tasks are complete
async fn update_distribution_status_if_complete(pool: &DbPool, file_id: Uuid) -> Result<(), sqlx::Error> {
    let (incomplete_count,): (i32,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM distribution_tasks
        WHERE file_id = $1 AND status != 'completed'
        "#,
    )
    .bind(file_id.to_string())
    .fetch_one(pool)
    .await?;

    if incomplete_count == 0 {
        // Only transition to completed if not already in a terminal state
        sqlx::query(
            r#"
            UPDATE file_distributions
            SET status = 'completed', updated_at = datetime('now')
            WHERE file_id = $1 AND status IN ('created', 'in_progress')
            "#,
        )
        .bind(file_id.to_string())
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Cancel a distribution (stops workers from working on it)
pub async fn cancel_distribution(pool: &DbPool, file_id: Uuid) -> Result<bool, sqlx::Error> {
    // Only cancel distributions that are in progress or created
    let result = sqlx::query(
        r#"
        UPDATE file_distributions
        SET status = 'cancelled', updated_at = datetime('now')
        WHERE file_id = $1 AND status IN ('created', 'in_progress')
        "#,
    )
    .bind(file_id.to_string())
    .execute(pool)
    .await?;

    if result.rows_affected() > 0 {
        // Also cancel all associated tasks
        sqlx::query(
            r#"
            UPDATE distribution_tasks
            SET status = 'cancelled', updated_at = datetime('now')
            WHERE file_id = $1 AND status != 'completed'
            "#,
        )
        .bind(file_id.to_string())
        .execute(pool)
        .await?;

        Ok(true)
    } else {
        Ok(false)
    }
}

/// Purge a distribution (mark as purged, workers will delete data)
pub async fn purge_distribution(pool: &DbPool, file_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        r#"
        UPDATE file_distributions
        SET status = 'purged', updated_at = datetime('now')
        WHERE file_id = $1 AND status IN ('completed', 'failed', 'cancelled')
        "#,
    )
    .bind(file_id.to_string())
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

/// Get all purged file IDs (for workers to clean up)
pub async fn get_purged_file_ids(pool: &DbPool) -> Result<Vec<Uuid>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String,)>(
        "SELECT file_id FROM file_distributions WHERE status = 'purged'",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(id,)| Uuid::parse_str(&id).ok())
        .collect())
}

/// Get all cancelled file IDs (for workers to abort in-progress downloads)
pub async fn get_cancelled_file_ids(pool: &DbPool) -> Result<Vec<Uuid>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String,)>(
        "SELECT file_id FROM file_distributions WHERE status = 'cancelled'",
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
