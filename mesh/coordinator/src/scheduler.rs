use crate::state::{ActiveJob, SchedulerState, WaitingServer};
use common::proto::{transfer_task::SourceType, TransferTask};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Core scheduling logic - attempts to assign work to waiting servers
pub async fn run_scheduler_loop(state: Arc<SchedulerState>) {
    loop {
        // Wait for notification that scheduling should be attempted
        state.schedule_notify.notified().await;

        // Try to assign work
        try_assign_work(&state).await;
    }
}

/// Attempt to assign work to all waiting servers
async fn try_assign_work(state: &Arc<SchedulerState>) {
    let active_job = {
        let guard = state.active_job.lock().await;
        match guard.as_ref() {
            Some(job) => job.clone(),
            None => {
                debug!("No active job, cannot assign work");
                return;
            }
        }
    };

    // Collect all waiting servers to process
    let servers_to_process: Vec<WaitingServer> = {
        let mut queue = state.waiting_for_work.lock().await;
        queue.drain(..).collect()
    };

    if servers_to_process.is_empty() {
        return;
    }

    // Track servers that couldn't be assigned work
    let mut unassigned_servers = Vec::new();

    // Try to assign work to each server
    for waiting_server in servers_to_process {
        match assign_work_to_server(state, &active_job, &waiting_server).await {
            Some(task) => {
                // Successfully assigned work
                info!(
                    "Assigned shard {} to {} (source: {:?})",
                    task.shard_id, waiting_server.server_address, task.source_type
                );

                // Send the task to the waiting server
                if waiting_server.response_tx.send(task).is_err() {
                    warn!(
                        "Failed to send task to server {} - connection dropped",
                        waiting_server.server_address
                    );
                }
            }
            None => {
                // Could not assign work, save for re-queuing
                debug!(
                    "Could not assign work to {}",
                    waiting_server.server_address
                );
                unassigned_servers.push(waiting_server);
            }
        }
    }

    // Re-queue servers that couldn't be assigned work
    if !unassigned_servers.is_empty() {
        let mut queue = state.waiting_for_work.lock().await;
        for server in unassigned_servers {
            queue.push_back(server);
        }
    }
}

/// Try to assign a specific shard transfer to a server
/// Optimized for O(S + P) where S = shards needed, P = peers with target shard
async fn assign_work_to_server(
    state: &Arc<SchedulerState>,
    active_job: &ActiveJob,
    waiting_server: &WaitingServer,
) -> Option<TransferTask> {
    // Step 1: Find the rarest shard this server needs (O(S) where S = total_shards)
    // Only hold shard_availability lock for this step
    let (shard_id, availability) = {
        let shard_availability = state.shard_availability.lock().await;

        let mut rarest_shard: Option<(i32, u32)> = None;
        for sid in 0..active_job.total_shards {
            // Skip shards this server already has
            if SchedulerState::has_shard(&waiting_server.shards_owned, sid) {
                continue;
            }

            let count = shard_availability.get(&sid).copied().unwrap_or(0);

            // Pick the rarest shard (lowest count)
            match &rarest_shard {
                None => rarest_shard = Some((sid, count)),
                Some((_, current_min)) if count < *current_min => {
                    rarest_shard = Some((sid, count));
                }
                _ => {}
            }
        }

        rarest_shard?
    };

    // Step 2: Try to find a peer that has this shard and is not busy uploading
    // Now uses O(1) lookup for servers_uploading instead of O(P) scan of pending_tasks
    if availability > 0 {
        // Find an available peer while holding read locks
        let available_peer: Option<String> = {
            let shard_to_servers = state.shard_to_servers.lock().await;
            let servers_uploading = state.servers_uploading.lock().await;

            // O(1) lookup: get servers that have this shard
            if let Some(peers_with_shard) = shard_to_servers.get(&shard_id) {
                // Iterate only over servers that have this shard (O(P) where P << N)
                let mut found = None;
                for peer_addr in peers_with_shard {
                    // Skip self
                    if peer_addr == &waiting_server.server_address {
                        continue;
                    }

                    // O(1) check if peer is already uploading
                    if servers_uploading.contains(peer_addr) {
                        continue;
                    }

                    // Found a suitable peer!
                    found = Some(peer_addr.clone());
                    break;
                }
                found
            } else {
                None
            }
        };

        // If we found a peer, record the assignment
        if let Some(peer_addr) = available_peer {
            let mut pending_tasks = state.pending_tasks.lock().await;
            let mut servers_uploading = state.servers_uploading.lock().await;

            let task = TransferTask {
                source_type: SourceType::Peer as i32,
                job_id: active_job.job_id.clone(),
                shard_id,
                upstream_server_addr: peer_addr.clone(),
                gcs_path: String::new(),
                total_shards: active_job.total_shards,
            };

            // Record this pending task and mark peer as uploading
            pending_tasks.insert(waiting_server.server_address.clone(), task.clone());
            servers_uploading.insert(peer_addr);

            return Some(task);
        }
    }

    // Step 3: No peer available - try GCS if not already in progress
    let mut gcs_in_progress = state.gcs_download_in_progress.lock().await;
    if !*gcs_in_progress {
        let shard_name = format!("shard_{:04}.bin", shard_id);
        let gcs_path = format!("{}/{}", active_job.gcs_base_path, shard_name);

        let task = TransferTask {
            source_type: SourceType::Gcs as i32,
            job_id: active_job.job_id.clone(),
            shard_id,
            upstream_server_addr: String::new(),
            gcs_path,
            total_shards: active_job.total_shards,
        };

        // Mark GCS as in progress
        *gcs_in_progress = true;
        drop(gcs_in_progress);

        // Record this pending task
        let mut pending_tasks = state.pending_tasks.lock().await;
        pending_tasks.insert(waiting_server.server_address.clone(), task.clone());

        return Some(task);
    }

    // Could not assign work
    None
}

/// Called when a server completes a transfer task
pub async fn on_task_completed(
    state: &Arc<SchedulerState>,
    server_address: &str,
    _job_id: &str,
    shard_id: i32,
) {
    info!(
        "Server {} completed shard {}",
        server_address, shard_id
    );

    // Remove from pending tasks and get the upstream server (if peer transfer)
    let (was_gcs, upstream_server) = {
        let mut pending = state.pending_tasks.lock().await;
        if let Some(task) = pending.remove(server_address) {
            let was_gcs = task.source_type == SourceType::Gcs as i32;
            let upstream = if !task.upstream_server_addr.is_empty() {
                Some(task.upstream_server_addr.clone())
            } else {
                None
            };
            (was_gcs, upstream)
        } else {
            (false, None)
        }
    };

    // If it was a GCS download, clear the flag
    if was_gcs {
        let mut gcs_flag = state.gcs_download_in_progress.lock().await;
        *gcs_flag = false;
    }

    // If it was a peer transfer, remove upstream from uploading set
    if let Some(upstream) = upstream_server {
        let mut uploading = state.servers_uploading.lock().await;
        uploading.remove(&upstream);
    }

    // Update shard availability
    {
        let mut availability = state.shard_availability.lock().await;
        let new_count = availability.entry(shard_id).or_insert(0);
        *new_count += 1;
        debug!(
            "Shard {} availability now {} (server {})",
            shard_id, *new_count, server_address
        );
    }

    // Update server's shard bitmap and shard_to_servers index
    {
        let active_job = state.active_job.lock().await;
        if let Some(job) = active_job.as_ref() {
            // Update bitmap
            {
                let mut server_shards = state.server_shards.lock().await;
                let bitmap = server_shards
                    .entry(server_address.to_string())
                    .or_insert_with(Vec::new);
                SchedulerState::set_shard(bitmap, shard_id, job.total_shards);
            }

            // Update shard_to_servers index (O(1) insert)
            {
                let mut shard_to_servers = state.shard_to_servers.lock().await;
                shard_to_servers
                    .entry(shard_id)
                    .or_insert_with(HashSet::new)
                    .insert(server_address.to_string());
            }
        }
    }

    // Notify scheduler to try assigning more work
    state.schedule_notify.notify_one();
}

/// Called when a server sends a heartbeat
pub async fn on_heartbeat(state: &Arc<SchedulerState>, server_address: &str) {
    let mut heartbeats = state.server_heartbeats.lock().await;
    heartbeats.insert(server_address.to_string(), std::time::Instant::now());
}

/// Set the active job
pub async fn set_active_job(state: &Arc<SchedulerState>, job: ActiveJob) {
    info!(
        "Setting active job: {} with {} shards",
        job.job_id, job.total_shards
    );

    // Clear previous job state
    {
        let mut waiting = state.waiting_for_work.lock().await;
        waiting.clear();
    }
    {
        let mut availability = state.shard_availability.lock().await;
        availability.clear();
    }
    {
        let mut pending = state.pending_tasks.lock().await;
        pending.clear();
    }
    {
        let mut gcs = state.gcs_download_in_progress.lock().await;
        *gcs = false;
    }
    {
        let mut shards = state.server_shards.lock().await;
        shards.clear();
    }
    // Clear optimized indexes
    {
        let mut shard_to_servers = state.shard_to_servers.lock().await;
        shard_to_servers.clear();
    }
    {
        let mut uploading = state.servers_uploading.lock().await;
        uploading.clear();
    }

    // Set new active job
    {
        let mut active = state.active_job.lock().await;
        *active = Some(job);
    }
}

/// Check if all servers have completed the active job
pub async fn check_job_completion(state: &Arc<SchedulerState>) -> bool {
    let active_job = state.active_job.lock().await;
    let Some(job) = active_job.as_ref() else {
        return false;
    };

    let server_shards = state.server_shards.lock().await;
    if server_shards.is_empty() {
        return false;
    }

    // Check if all tracked servers have all shards
    for bitmap in server_shards.values() {
        if !SchedulerState::has_all_shards(bitmap, job.total_shards) {
            return false;
        }
    }

    true
}
