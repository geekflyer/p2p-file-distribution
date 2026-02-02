use crate::scheduler;
use crate::state::{SchedulerState, WaitingServer};
use common::proto::{
    scheduler_server::Scheduler, Empty, HeartbeatRequest, HeartbeatResponse, ServerStatus,
    TransferTask, TransferTaskComplete,
};
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

pub struct SchedulerService {
    state: Arc<SchedulerState>,
}

impl SchedulerService {
    pub fn new(state: Arc<SchedulerState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Scheduler for SchedulerService {
    async fn get_work(
        &self,
        request: Request<ServerStatus>,
    ) -> Result<Response<TransferTask>, Status> {
        let server_status = request.into_inner();
        let server_address = server_status.server_address.clone();

        info!(
            "Server {} requesting work (job_id: {}, shards_owned: {} bytes)",
            server_address,
            server_status.job_id,
            server_status.shards_owned.len()
        );

        // Update server's shard bitmap
        {
            let mut server_shards = self.state.server_shards.lock().await;
            server_shards.insert(server_address.clone(), server_status.shards_owned.clone());
        }

        // Note: shard_availability is updated only in on_task_completed to avoid double-counting
        // when servers call get_work multiple times

        // Check if this server already has all shards
        {
            let active_job = self.state.active_job.lock().await;
            if let Some(job) = active_job.as_ref() {
                if SchedulerState::has_all_shards(&server_status.shards_owned, job.total_shards) {
                    debug!("Server {} has all {} shards, no work needed", server_address, job.total_shards);
                    // Return a "no work" response - client should interpret empty task
                    return Ok(Response::new(TransferTask {
                        source_type: 0,
                        job_id: job.job_id.clone(),
                        shard_id: -1, // Indicates no work
                        upstream_server_addr: String::new(),
                        gcs_path: String::new(),
                        total_shards: job.total_shards,
                    }));
                }
            }
        }

        // Create a oneshot channel to receive the task assignment
        let (tx, rx) = oneshot::channel();

        // Add to waiting queue
        {
            let mut queue = self.state.waiting_for_work.lock().await;
            queue.push_back(WaitingServer {
                server_address: server_address.clone(),
                job_id: server_status.job_id,
                shards_owned: server_status.shards_owned,
                response_tx: tx,
            });
        }

        // Notify scheduler to try assigning work
        self.state.schedule_notify.notify_one();

        // Wait for assignment (with timeout)
        match tokio::time::timeout(std::time::Duration::from_secs(60), rx).await {
            Ok(Ok(task)) => Ok(Response::new(task)),
            Ok(Err(_)) => {
                // Channel closed - server was removed from queue
                warn!("Work assignment channel closed for {}", server_address);
                Err(Status::unavailable("Server removed from queue"))
            }
            Err(_) => {
                // Timeout - no work available
                debug!("GetWork timeout for {}, retrying", server_address);
                // Remove from queue if still there
                {
                    let mut queue = self.state.waiting_for_work.lock().await;
                    queue.retain(|ws| ws.server_address != server_address);
                }
                Err(Status::deadline_exceeded("No work available, retry"))
            }
        }
    }

    async fn report_transfer_task_completion(
        &self,
        request: Request<TransferTaskComplete>,
    ) -> Result<Response<Empty>, Status> {
        let completion = request.into_inner();

        scheduler::on_task_completed(
            &self.state,
            &completion.server_address,
            &completion.job_id,
            completion.shard_id,
        )
        .await;

        Ok(Response::new(Empty {}))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let heartbeat = request.into_inner();

        scheduler::on_heartbeat(&self.state, &heartbeat.server_address).await;

        Ok(Response::new(HeartbeatResponse {}))
    }
}
