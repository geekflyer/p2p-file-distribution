use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Worker health status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Healthy,
    Unhealthy,
}

impl std::fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerStatus::Healthy => write!(f, "healthy"),
            WorkerStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

impl std::str::FromStr for WorkerStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "healthy" => Ok(WorkerStatus::Healthy),
            "unhealthy" => Ok(WorkerStatus::Unhealthy),
            _ => Err(format!("Invalid worker status: {}", s)),
        }
    }
}

/// File distribution status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DistributionStatus {
    Created,
    InProgress,
    Completed,
    Failed,
    /// Distribution was cancelled by admin
    Cancelled,
    /// Distribution data has been purged from workers (metadata retained)
    Purged,
}

impl std::fmt::Display for DistributionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributionStatus::Created => write!(f, "created"),
            DistributionStatus::InProgress => write!(f, "in_progress"),
            DistributionStatus::Completed => write!(f, "completed"),
            DistributionStatus::Failed => write!(f, "failed"),
            DistributionStatus::Cancelled => write!(f, "cancelled"),
            DistributionStatus::Purged => write!(f, "purged"),
        }
    }
}

impl std::str::FromStr for DistributionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "created" => Ok(DistributionStatus::Created),
            "in_progress" => Ok(DistributionStatus::InProgress),
            "completed" => Ok(DistributionStatus::Completed),
            "failed" => Ok(DistributionStatus::Failed),
            "cancelled" => Ok(DistributionStatus::Cancelled),
            "purged" => Ok(DistributionStatus::Purged),
            _ => Err(format!("Invalid distribution status: {}", s)),
        }
    }
}

/// Task status (same as distribution status but for individual worker tasks)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Created,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Created => write!(f, "created"),
            TaskStatus::InProgress => write!(f, "in_progress"),
            TaskStatus::Completed => write!(f, "completed"),
            TaskStatus::Failed => write!(f, "failed"),
            TaskStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "created" => Ok(TaskStatus::Created),
            "in_progress" => Ok(TaskStatus::InProgress),
            "completed" => Ok(TaskStatus::Completed),
            "failed" => Ok(TaskStatus::Failed),
            "cancelled" => Ok(TaskStatus::Cancelled),
            _ => Err(format!("Invalid task status: {}", s)),
        }
    }
}

/// Upstream source for a worker (tagged union / oneof pattern)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Upstream {
    Gcs { gcs_path: String },
    Peer { worker_id: String, worker_address: String },
}

/// Worker information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Worker {
    pub worker_address: String,
    pub last_heartbeat: DateTime<Utc>,
    pub status: WorkerStatus,
    /// Total disk space in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_total_bytes: Option<u64>,
    /// Used disk space in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_used_bytes: Option<u64>,
}

/// File distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileDistribution {
    pub file_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub gcs_path: String,
    pub total_chunks: i32,
    pub total_size: u64,
    pub chunk_size: u64,
    pub file_crc32c: String,
    pub status: DistributionStatus,
}

/// Distribution task for a specific worker (returned from check-in)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributionTask {
    pub task_id: String,
    pub file_id: Uuid,
    pub total_chunks: i32,
    pub chunk_size: u64,
    pub total_file_size_bytes: u64,
    pub crc32c: String,
    pub upstream: Upstream,
}

/// Task progress in check-in request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskProgress {
    pub file_id: Uuid,
    /// Last fully completed chunk (persisted to DB)
    pub last_chunk_id_completed: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Cumulative bytes downloaded for this task
    pub bytes_downloaded: u64,
    /// Cumulative bytes uploaded (served to peers) for this task
    pub bytes_uploaded: u64,
    /// Download throughput in bytes per second (calculated by worker over rolling window)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_throughput_bps: Option<u64>,
    /// Upload throughput in bytes per second (calculated by worker over rolling window)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_throughput_bps: Option<u64>,
}

/// Check-in request from worker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckInRequest {
    pub worker_id: String,
    pub worker_address: String,
    pub task_progress: Vec<TaskProgress>,
    /// Total disk space in bytes (data directory filesystem)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_total_bytes: Option<u64>,
    /// Used disk space in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_used_bytes: Option<u64>,
}

/// Check-in response to worker
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckInResponse {
    /// Tasks with upstream already computed
    pub tasks: Vec<DistributionTask>,
    /// File IDs that have been purged and should have their data deleted
    pub purge_file_ids: Vec<Uuid>,
    /// File IDs that have been cancelled and should abort in-progress downloads
    #[serde(default)]
    pub cancel_file_ids: Vec<Uuid>,
}

/// Create distribution request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDistributionRequest {
    /// Path to the file in GCS (e.g., gs://bucket/path/model.bin)
    pub gcs_path: String,
    /// Optional chunk size override (default 64MB)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_size: Option<u64>,
}

/// Create distribution response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDistributionResponse {
    pub file_id: Uuid,
}

/// Distribution with worker progress details
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DistributionDetails {
    pub distribution: FileDistribution,
    pub worker_progress: Vec<WorkerTaskProgress>,
}

/// Per-worker task progress
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerTaskProgress {
    pub worker_address: String,
    /// Last fully completed chunk
    pub last_chunk_id_completed: i32,
    pub status: TaskStatus,
    /// Worker health status
    pub worker_status: WorkerStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    /// Download throughput in bytes per second (calculated by coordinator)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_throughput_bps: Option<u64>,
    /// Upload throughput in bytes per second (calculated by coordinator)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_throughput_bps: Option<u64>,
}
