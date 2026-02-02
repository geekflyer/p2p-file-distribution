use common::proto::scheduler_client::SchedulerClient;
use common::proto::{HeartbeatRequest, ServerStatus, TransferTask, TransferTaskComplete};
use tonic::transport::Channel;

pub struct SchedulerConnection {
    client: SchedulerClient<Channel>,
    server_address: String,
}

impl SchedulerConnection {
    pub async fn connect(coordinator_url: &str, server_address: String) -> anyhow::Result<Self> {
        tracing::info!("Connecting to scheduler at {}", coordinator_url);
        let client = SchedulerClient::connect(coordinator_url.to_string()).await?;
        Ok(Self {
            client,
            server_address,
        })
    }

    /// Request work from the scheduler
    /// Returns None if no work is available (all shards complete)
    pub async fn get_work(&mut self, job_id: &str, shards_owned: Vec<u8>) -> anyhow::Result<Option<TransferTask>> {
        let request = ServerStatus {
            server_address: self.server_address.clone(),
            job_id: job_id.to_string(),
            shards_owned,
        };

        match self.client.get_work(request).await {
            Ok(response) => {
                let task = response.into_inner();
                // shard_id == -1 means no work available (all shards complete)
                if task.shard_id < 0 {
                    Ok(None)
                } else {
                    Ok(Some(task))
                }
            }
            Err(e) => {
                // Deadline exceeded is expected when no work available
                if e.code() == tonic::Code::DeadlineExceeded {
                    tracing::debug!("GetWork timed out, retrying");
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Report completion of a transfer task
    pub async fn report_completion(&mut self, job_id: &str, shard_id: i32) -> anyhow::Result<()> {
        let request = TransferTaskComplete {
            server_address: self.server_address.clone(),
            job_id: job_id.to_string(),
            shard_id,
        };

        self.client.report_transfer_task_completion(request).await?;
        Ok(())
    }

    /// Send heartbeat
    pub async fn heartbeat(&mut self) -> anyhow::Result<()> {
        let request = HeartbeatRequest {
            server_address: self.server_address.clone(),
        };

        self.client.heartbeat(request).await?;
        Ok(())
    }

    pub fn server_address(&self) -> &str {
        &self.server_address
    }
}
