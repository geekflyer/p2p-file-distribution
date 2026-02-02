use common::{DeploymentTask, HeartbeatRequest, TaskProgress, UpstreamAssignment};
use reqwest::Client;
use uuid::Uuid;

pub struct CoordinatorClient {
    client: Client,
    base_url: String,
    server_address: String,
}

impl CoordinatorClient {
    pub fn new(base_url: String, server_address: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
            server_address,
        }
    }

    /// Get deployment tasks assigned to this server
    pub async fn get_tasks(&self) -> anyhow::Result<Vec<DeploymentTask>> {
        let url = format!(
            "{}/server/model-deployment-tasks?serverAddress={}",
            self.base_url,
            urlencoding::encode(&self.server_address)
        );

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to get tasks: {}", response.status());
        }

        let tasks: Vec<DeploymentTask> = response.json().await?;
        Ok(tasks)
    }

    /// Get upstream assignment for this server, optionally for a specific job
    pub async fn get_upstream(&self, job_id: Option<Uuid>) -> anyhow::Result<UpstreamAssignment> {
        let mut url = format!(
            "{}/server/upstream/{}",
            self.base_url,
            urlencoding::encode(&self.server_address)
        );

        if let Some(id) = job_id {
            url.push_str(&format!("?jobId={}", id));
        }

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to get upstream: {}", response.status());
        }

        let assignment: UpstreamAssignment = response.json().await?;
        Ok(assignment)
    }

    /// Send heartbeat with task progress
    pub async fn heartbeat(&self, task_progress: Vec<TaskProgress>) -> anyhow::Result<()> {
        let url = format!("{}/server/heartbeat", self.base_url);

        let request = HeartbeatRequest {
            server_address: self.server_address.clone(),
            task_progress,
        };

        let response = self.client.post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to send heartbeat: {}", response.status());
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn server_address(&self) -> &str {
        &self.server_address
    }
}
