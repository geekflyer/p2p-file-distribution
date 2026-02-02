use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Server health status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ServerStatus {
    Healthy,
    Unhealthy,
}

impl std::fmt::Display for ServerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerStatus::Healthy => write!(f, "healthy"),
            ServerStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

impl std::str::FromStr for ServerStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "healthy" => Ok(ServerStatus::Healthy),
            "unhealthy" => Ok(ServerStatus::Unhealthy),
            _ => Err(format!("Invalid server status: {}", s)),
        }
    }
}

/// Deployment job status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Created,
    InProgress,
    Completed,
    Failed,
    /// Job was cancelled by admin
    Cancelled,
    /// Job data has been purged from servers (metadata retained)
    Purged,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Created => write!(f, "created"),
            JobStatus::InProgress => write!(f, "in_progress"),
            JobStatus::Completed => write!(f, "completed"),
            JobStatus::Failed => write!(f, "failed"),
            JobStatus::Cancelled => write!(f, "cancelled"),
            JobStatus::Purged => write!(f, "purged"),
        }
    }
}

impl std::str::FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "created" => Ok(JobStatus::Created),
            "in_progress" => Ok(JobStatus::InProgress),
            "completed" => Ok(JobStatus::Completed),
            "failed" => Ok(JobStatus::Failed),
            "cancelled" => Ok(JobStatus::Cancelled),
            "purged" => Ok(JobStatus::Purged),
            _ => Err(format!("Invalid job status: {}", s)),
        }
    }
}

/// Task status (same as job status but for individual server tasks)
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

/// Upstream type for a server
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamType {
    Gcs,
    Peer,
}

/// Server information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Server {
    pub server_address: String,
    pub last_heartbeat: DateTime<Utc>,
    pub status: ServerStatus,
}

/// Deployment job
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentJob {
    pub deployment_job_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub gcs_manifest_path: String,
    pub total_shards: i32,
    pub total_size: u64,
    pub shard_size: u64,
    pub status: JobStatus,
}

/// Deployment task for a specific server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentTask {
    pub deployment_job_id: Uuid,
    pub gcs_manifest_path: String,
    pub total_shards: i32,
    pub shard_size: u64,
}

/// Upstream assignment response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamAssignment {
    pub upstream_type: UpstreamType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_peer_address: Option<String>,
}

/// Task progress in heartbeat
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskProgress {
    pub deployment_job_id: Uuid,
    /// Last fully completed shard (persisted to DB)
    pub last_shard_id_completed: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    /// Current throughput in bytes per second (rolling average)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_bps: Option<u64>,
    /// Current shard being downloaded (for UI display, not persisted)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_shard_id: Option<i32>,
    /// Progress within current shard as percentage 0-100 (for UI display, not persisted)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_shard_progress_pct: Option<f32>,
}

/// Heartbeat request from server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    pub server_address: String,
    pub task_progress: Vec<TaskProgress>,
}

/// Heartbeat response to server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    /// Job IDs that have been purged and should have their data deleted
    pub purge_job_ids: Vec<Uuid>,
}

/// Create job request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateJobRequest {
    /// Path to the manifest file (e.g., gs://bucket/path/model.manifest)
    pub gcs_manifest_path: String,
}

/// Shard info in manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard_id: i32,
    /// Relative path to shard file (relative to manifest location)
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

/// GCS manifest format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsManifest {
    pub total_size: u64,
    pub shard_size: u64,
    pub num_shards: i32,
    pub shards: Vec<ShardInfo>,
}

/// Create job response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateJobResponse {
    pub deployment_job_id: Uuid,
}

/// Job with server progress details
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobDetails {
    pub job: DeploymentJob,
    pub server_progress: Vec<ServerTaskProgress>,
}

/// Per-server task progress
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerTaskProgress {
    pub server_address: String,
    /// Last fully completed shard
    pub last_shard_id_completed: i32,
    pub status: TaskStatus,
    /// Server health status
    pub server_status: ServerStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    /// Current throughput in bytes per second
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_bps: Option<u64>,
    /// Current shard being downloaded (for UI display)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_shard_id: Option<i32>,
    /// Progress within current shard as percentage (for UI display)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_shard_progress_pct: Option<f32>,
}
