use chrono::{DateTime, Utc};
use common::{DeploymentJob, JobStatus};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use uuid::Uuid;

pub type DbPool = SqlitePool;

/// Create a new database connection pool
pub async fn create_pool(database_url: &str) -> Result<DbPool, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    init_db(&pool).await?;
    Ok(pool)
}

/// Initialize database schema for mesh coordinator
async fn init_db(pool: &DbPool) -> Result<(), sqlx::Error> {
    // Job queue table - simpler than chain version since we track state in memory
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS mesh_jobs (
            deployment_job_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            gcs_manifest_path TEXT NOT NULL,
            gcs_base_path TEXT NOT NULL,
            total_shards INTEGER NOT NULL DEFAULT 0,
            total_size INTEGER NOT NULL DEFAULT 0,
            shard_size INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_mesh_jobs_status ON mesh_jobs(status)")
        .execute(pool)
        .await?;

    Ok(())
}

/// Create a new job (queued as pending)
pub async fn create_job(
    pool: &DbPool,
    gcs_manifest_path: &str,
    gcs_base_path: &str,
    total_shards: i32,
    total_size: u64,
    shard_size: u64,
) -> Result<Uuid, sqlx::Error> {
    let job_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO mesh_jobs (deployment_job_id, gcs_manifest_path, gcs_base_path, total_shards, total_size, shard_size, status)
        VALUES ($1, $2, $3, $4, $5, $6, 'pending')
        "#,
    )
    .bind(job_id.to_string())
    .bind(gcs_manifest_path)
    .bind(gcs_base_path)
    .bind(total_shards)
    .bind(total_size as i64)
    .bind(shard_size as i64)
    .execute(pool)
    .await?;

    Ok(job_id)
}

/// Get the next pending job (for job queue processing)
pub async fn get_next_pending_job(pool: &DbPool) -> Result<Option<MeshJob>, sqlx::Error> {
    let row = sqlx::query_as::<_, (String, String, String, i32, i64, i64)>(
        r#"
        SELECT deployment_job_id, gcs_manifest_path, gcs_base_path, total_shards, total_size, shard_size
        FROM mesh_jobs
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(job_id, gcs_manifest_path, gcs_base_path, total_shards, total_size, shard_size)| {
        MeshJob {
            job_id: Uuid::parse_str(&job_id).unwrap_or_default(),
            gcs_manifest_path,
            gcs_base_path,
            total_shards,
            total_size: total_size as u64,
            shard_size: shard_size as u64,
        }
    }))
}

/// Get current in_progress job
pub async fn get_active_job(pool: &DbPool) -> Result<Option<MeshJob>, sqlx::Error> {
    let row = sqlx::query_as::<_, (String, String, String, i32, i64, i64)>(
        r#"
        SELECT deployment_job_id, gcs_manifest_path, gcs_base_path, total_shards, total_size, shard_size
        FROM mesh_jobs
        WHERE status = 'in_progress'
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(job_id, gcs_manifest_path, gcs_base_path, total_shards, total_size, shard_size)| {
        MeshJob {
            job_id: Uuid::parse_str(&job_id).unwrap_or_default(),
            gcs_manifest_path,
            gcs_base_path,
            total_shards,
            total_size: total_size as u64,
            shard_size: shard_size as u64,
        }
    }))
}

/// Mark job as in_progress
pub async fn mark_job_in_progress(pool: &DbPool, job_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE mesh_jobs SET status = 'in_progress', updated_at = datetime('now')
        WHERE deployment_job_id = $1 AND status = 'pending'
        "#,
    )
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

/// Mark job as completed
pub async fn mark_job_completed(pool: &DbPool, job_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE mesh_jobs SET status = 'completed', updated_at = datetime('now')
        WHERE deployment_job_id = $1 AND status = 'in_progress'
        "#,
    )
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

/// Get all jobs
pub async fn get_all_jobs(pool: &DbPool) -> Result<Vec<DeploymentJob>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, String, String, String, i32, i64, i64, String)>(
        r#"
        SELECT deployment_job_id, created_at, updated_at, gcs_manifest_path, total_shards, total_size, shard_size, status
        FROM mesh_jobs
        ORDER BY created_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|(job_id, created_at, updated_at, gcs_manifest_path, total_shards, total_size, shard_size, status)| {
            let job_id = Uuid::parse_str(&job_id).ok()?;
            let created_at = parse_datetime(&created_at)?;
            let updated_at = parse_datetime(&updated_at)?;
            Some(DeploymentJob {
                deployment_job_id: job_id,
                created_at,
                updated_at,
                gcs_manifest_path,
                total_shards,
                total_size: total_size as u64,
                shard_size: shard_size as u64,
                status: status.parse().unwrap_or(JobStatus::Failed),
            })
        })
        .collect())
}

#[derive(Clone)]
pub struct MeshJob {
    pub job_id: Uuid,
    pub gcs_manifest_path: String,
    pub gcs_base_path: String,
    pub total_shards: i32,
    pub total_size: u64,
    pub shard_size: u64,
}

fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map(|dt| dt.and_utc())
        })
        .ok()
}
