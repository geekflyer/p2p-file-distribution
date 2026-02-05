use common::proto::p2p_transfer_server::{P2pTransfer, P2pTransferServer};
use common::proto::{ChunkData, ChunkRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::chunk_cache::SharedChunkCache;
use crate::constants::GRPC_MAX_MESSAGE_SIZE;
use crate::storage::{compute_crc32c, ChunkStorage};

/// How long to wait for new data before checking again
const POLL_INTERVAL_MS: u64 = 50;

/// Maximum wait time for a chunk to become available (30 seconds)
const MAX_WAIT_MS: u64 = 30_000;

/// Tracks cumulative bytes uploaded per file
pub type UploadTracker = Arc<RwLock<HashMap<Uuid, u64>>>;

/// Chunk metadata needed for streaming
pub struct ChunkMeta {
    pub chunk_size: u64,
    pub total_size: u64,
    pub total_chunks: i32,
}

/// Chunk metadata store (set by main when task is received)
pub type ChunkMetaStore = Arc<RwLock<HashMap<Uuid, ChunkMeta>>>;

pub struct FileService {
    storage: Arc<ChunkStorage>,
    chunk_cache: SharedChunkCache,
    upload_tracker: UploadTracker,
    chunk_meta: ChunkMetaStore,
}

impl FileService {
    pub fn new(storage: Arc<ChunkStorage>, chunk_cache: SharedChunkCache, upload_tracker: UploadTracker, chunk_meta: ChunkMetaStore) -> Self {
        Self { storage, chunk_cache, upload_tracker, chunk_meta }
    }

    pub fn into_server(self) -> P2pTransferServer<Self> {
        P2pTransferServer::new(self)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    }
}

#[tonic::async_trait]
impl P2pTransfer for FileService {
    type StreamChunksStream = ReceiverStream<Result<ChunkData, Status>>;

    async fn stream_chunks(
        &self,
        request: Request<ChunkRequest>,
    ) -> Result<Response<Self::StreamChunksStream>, Status> {
        let req = request.into_inner();

        let file_id = Uuid::parse_str(&req.file_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid file ID: {}", e)))?;

        let start_chunk = req.start_from_chunk_id;
        let storage = self.storage.clone();
        let chunk_cache = self.chunk_cache.clone();
        let upload_tracker = self.upload_tracker.clone();
        let chunk_meta = self.chunk_meta.clone();

        let (tx, rx) = mpsc::channel(4); // Small buffer since chunks are large

        tracing::info!("Starting chunk stream for file {} from chunk {}", file_id, start_chunk);

        tokio::spawn(async move {
            // Get chunk metadata
            let meta = {
                let metas = chunk_meta.read().await;
                match metas.get(&file_id) {
                    Some(m) => ChunkMeta {
                        chunk_size: m.chunk_size,
                        total_size: m.total_size,
                        total_chunks: m.total_chunks,
                    },
                    None => {
                        tracing::warn!("No chunk metadata for file {}", file_id);
                        let _ = tx.send(Err(Status::not_found("File metadata not found"))).await;
                        return;
                    }
                }
            };

            let chunk_size = meta.chunk_size;
            let total_size = meta.total_size;
            let total_chunks = meta.total_chunks;

            // Stream chunks starting from requested position
            for chunk_id in start_chunk..total_chunks {
                // Wait for chunk to be available
                let mut waited_ms: u64 = 0;
                while !storage.is_chunk_complete(file_id, chunk_id, chunk_size, total_size).await {
                    if waited_ms >= MAX_WAIT_MS {
                        tracing::warn!(
                            "Chunk {} for file {} not available after {}ms, ending stream",
                            chunk_id, file_id, waited_ms
                        );
                        return;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
                    waited_ms += POLL_INTERVAL_MS;
                }

                // Calculate chunk bounds
                let offset = chunk_id as u64 * chunk_size;
                let this_chunk_size = if chunk_id == total_chunks - 1 {
                    // Last chunk may be smaller
                    let remainder = total_size % chunk_size;
                    if remainder == 0 { chunk_size } else { remainder }
                } else {
                    chunk_size
                };

                // Try cache first, fall back to disk
                let (data, crc32c) = if let Some((cached_data, cached_crc)) = chunk_cache.get(file_id, chunk_id).await {
                    tracing::debug!("Cache hit for chunk {} of file {}", chunk_id, file_id);
                    (cached_data.to_vec(), cached_crc)
                } else {
                    tracing::debug!("Cache miss for chunk {} of file {}, reading from disk", chunk_id, file_id);
                    // Read the chunk from disk
                    let data = match storage.read_range(file_id, offset, this_chunk_size as usize).await {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::error!("Failed to read chunk {} for file {}: {}", chunk_id, file_id, e);
                            let _ = tx.send(Err(Status::internal(format!("Failed to read chunk: {}", e)))).await;
                            return;
                        }
                    };

                    // Compute CRC32C
                    let crc32c = compute_crc32c(&data);
                    (data, crc32c)
                };

                // Send chunk
                let chunk_data = ChunkData {
                    chunk_id,
                    data,
                    crc32c,
                };

                let chunk_len = chunk_data.data.len() as u64;

                if tx.send(Ok(chunk_data)).await.is_err() {
                    // Receiver dropped - client disconnected
                    tracing::info!(
                        "Stream for file {} ended early: client disconnected at chunk {}",
                        file_id, chunk_id
                    );
                    return;
                }

                // Track uploaded bytes
                {
                    let mut tracker = upload_tracker.write().await;
                    *tracker.entry(file_id).or_insert(0) += chunk_len;
                }

                tracing::debug!("Streamed chunk {} ({} bytes) for file {}", chunk_id, chunk_len, file_id);
            }

            tracing::info!(
                "Finished streaming file {} ({} chunks)",
                file_id, total_chunks - start_chunk
            );
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
