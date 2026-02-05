use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Entry in the chunk cache
struct CacheEntry {
    data: Bytes,
    crc32c: u32,
    /// Order counter for LRU eviction (higher = more recent)
    access_order: u64,
}

/// In-memory cache for recently downloaded chunks
/// Reduces disk reads when streaming to downstream workers
pub struct ChunkCache {
    /// Map of (file_id, chunk_id) -> cached chunk data
    entries: RwLock<HashMap<(Uuid, i32), CacheEntry>>,
    /// Maximum number of chunks to cache
    max_entries: usize,
    /// Counter for LRU ordering
    order_counter: RwLock<u64>,
}

impl ChunkCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_entries,
            order_counter: RwLock::new(0),
        }
    }

    /// Store a chunk in the cache
    /// Evicts least recently used entry if at capacity
    pub async fn put(&self, file_id: Uuid, chunk_id: i32, data: Bytes, crc32c: u32) {
        let mut entries = self.entries.write().await;

        // Get next order counter
        let order = {
            let mut counter = self.order_counter.write().await;
            *counter += 1;
            *counter
        };

        // Check if we need to evict
        if entries.len() >= self.max_entries && !entries.contains_key(&(file_id, chunk_id)) {
            // Find LRU entry
            if let Some((&lru_key, _)) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.access_order)
            {
                entries.remove(&lru_key);
            }
        }

        // Insert or update
        entries.insert(
            (file_id, chunk_id),
            CacheEntry {
                data,
                crc32c,
                access_order: order,
            },
        );
    }

    /// Get a chunk from the cache (returns None if not cached)
    /// Updates access order on hit
    pub async fn get(&self, file_id: Uuid, chunk_id: i32) -> Option<(Bytes, u32)> {
        // Try read lock first
        {
            let entries = self.entries.read().await;
            if !entries.contains_key(&(file_id, chunk_id)) {
                return None;
            }
        }

        // Upgrade to write lock to update access order
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(&(file_id, chunk_id)) {
            let order = {
                let mut counter = self.order_counter.write().await;
                *counter += 1;
                *counter
            };
            entry.access_order = order;
            Some((entry.data.clone(), entry.crc32c))
        } else {
            None
        }
    }

    /// Remove all cached chunks for a file
    pub async fn remove_file(&self, file_id: Uuid) {
        let mut entries = self.entries.write().await;
        entries.retain(|(fid, _), _| *fid != file_id);
    }

    /// Get cache stats (for debugging)
    #[allow(dead_code)]
    pub async fn stats(&self) -> (usize, usize) {
        let entries = self.entries.read().await;
        (entries.len(), self.max_entries)
    }
}

/// Type alias for shared chunk cache
pub type SharedChunkCache = Arc<ChunkCache>;

/// Create a new shared chunk cache
pub fn create_chunk_cache(max_entries: usize) -> SharedChunkCache {
    Arc::new(ChunkCache::new(max_entries))
}

/// Default number of chunks to cache (5 chunks * 64MB = 320MB max)
pub const DEFAULT_CACHE_SIZE: usize = 5;
