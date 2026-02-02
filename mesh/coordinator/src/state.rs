use common::proto::TransferTask;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, Notify};

/// Represents a server waiting for work assignment
pub struct WaitingServer {
    pub server_address: String,
    pub job_id: String,
    pub shards_owned: Vec<u8>, // Bitmap
    pub response_tx: oneshot::Sender<TransferTask>,
}

/// In-memory scheduler state
pub struct SchedulerState {
    /// FIFO queue of servers waiting for work (blocked on getWork)
    pub waiting_for_work: Mutex<VecDeque<WaitingServer>>,

    /// Count of servers that have each shard: Map<shard_id, count>
    pub shard_availability: Mutex<HashMap<i32, u32>>,

    /// Currently pending transfer tasks: Map<server_address, TransferTask>
    /// Used to track who is busy downloading/uploading
    pub pending_tasks: Mutex<HashMap<String, TransferTask>>,

    /// Whether a GCS download is currently in progress (max 1 globally)
    pub gcs_download_in_progress: Mutex<bool>,

    /// Current active job info
    pub active_job: Mutex<Option<ActiveJob>>,

    /// Notify when scheduling should be attempted (new server waiting or task completed)
    pub schedule_notify: Notify,

    /// Map of server_address -> shards_owned bitmap (updated on each getWork call)
    pub server_shards: Mutex<HashMap<String, Vec<u8>>>,

    /// Last heartbeat time per server
    pub server_heartbeats: Mutex<HashMap<String, std::time::Instant>>,

    // === Optimized indexes for O(1) lookups ===

    /// Inverted index: shard_id -> set of server addresses that have it
    /// Enables O(1) lookup of "who has shard X" instead of scanning all servers
    pub shard_to_servers: Mutex<HashMap<i32, HashSet<String>>>,

    /// Set of servers currently uploading (their address is in pending_tasks as upstream)
    /// Enables O(1) lookup of "is server X uploading" instead of scanning pending_tasks
    pub servers_uploading: Mutex<HashSet<String>>,
}

#[derive(Clone)]
pub struct ActiveJob {
    pub job_id: String,
    pub total_shards: i32,
    pub gcs_base_path: String,
}

impl SchedulerState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            waiting_for_work: Mutex::new(VecDeque::new()),
            shard_availability: Mutex::new(HashMap::new()),
            pending_tasks: Mutex::new(HashMap::new()),
            gcs_download_in_progress: Mutex::new(false),
            active_job: Mutex::new(None),
            schedule_notify: Notify::new(),
            server_shards: Mutex::new(HashMap::new()),
            server_heartbeats: Mutex::new(HashMap::new()),
            shard_to_servers: Mutex::new(HashMap::new()),
            servers_uploading: Mutex::new(HashSet::new()),
        })
    }

    /// Check if a server has a specific shard (from their bitmap)
    pub fn has_shard(bitmap: &[u8], shard_id: i32) -> bool {
        if shard_id < 0 {
            return false;
        }
        let byte_idx = (shard_id / 8) as usize;
        let bit_idx = (shard_id % 8) as u8;
        if byte_idx >= bitmap.len() {
            return false;
        }
        (bitmap[byte_idx] & (1 << bit_idx)) != 0
    }

    /// Set a shard bit in a bitmap
    pub fn set_shard(bitmap: &mut Vec<u8>, shard_id: i32, total_shards: i32) {
        if shard_id < 0 || shard_id >= total_shards {
            return;
        }
        let byte_idx = (shard_id / 8) as usize;
        let bit_idx = (shard_id % 8) as u8;
        // Ensure bitmap is large enough
        let needed_bytes = ((total_shards + 7) / 8) as usize;
        if bitmap.len() < needed_bytes {
            bitmap.resize(needed_bytes, 0);
        }
        bitmap[byte_idx] |= 1 << bit_idx;
    }

    /// Count how many shards a server has from their bitmap
    pub fn count_shards(bitmap: &[u8]) -> u32 {
        bitmap.iter().map(|b| b.count_ones()).sum()
    }

    /// Check if server has all shards
    pub fn has_all_shards(bitmap: &[u8], total_shards: i32) -> bool {
        for shard_id in 0..total_shards {
            if !Self::has_shard(bitmap, shard_id) {
                return false;
            }
        }
        true
    }
}

impl Default for SchedulerState {
    fn default() -> Self {
        Self {
            waiting_for_work: Mutex::new(VecDeque::new()),
            shard_availability: Mutex::new(HashMap::new()),
            pending_tasks: Mutex::new(HashMap::new()),
            gcs_download_in_progress: Mutex::new(false),
            active_job: Mutex::new(None),
            schedule_notify: Notify::new(),
            server_shards: Mutex::new(HashMap::new()),
            server_heartbeats: Mutex::new(HashMap::new()),
            shard_to_servers: Mutex::new(HashMap::new()),
            servers_uploading: Mutex::new(HashSet::new()),
        }
    }
}
