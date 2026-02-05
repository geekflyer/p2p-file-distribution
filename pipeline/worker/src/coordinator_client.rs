use common::{CheckInRequest, CheckInResponse, TaskProgress};
use reqwest::Client;

pub struct CoordinatorClient {
    client: Client,
    base_url: String,
    worker_id: String,
    worker_address: String,
}

impl CoordinatorClient {
    pub fn new(base_url: String, worker_id: String, worker_address: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
            worker_id,
            worker_address,
        }
    }

    /// Check in with coordinator - sends status + progress, receives tasks with upstream
    pub async fn check_in(
        &self,
        task_progress: Vec<TaskProgress>,
        disk_total_bytes: Option<u64>,
        disk_used_bytes: Option<u64>,
    ) -> anyhow::Result<CheckInResponse> {
        let url = format!("{}/worker/check-in", self.base_url);

        let request = CheckInRequest {
            worker_id: self.worker_id.clone(),
            worker_address: self.worker_address.clone(),
            task_progress,
            disk_total_bytes,
            disk_used_bytes,
        };

        let response = self.client.post(&url).json(&request).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to check in: {}", response.status());
        }

        let check_in_response: CheckInResponse = response.json().await?;
        Ok(check_in_response)
    }

    #[allow(dead_code)]
    pub fn worker_address(&self) -> &str {
        &self.worker_address
    }
}
