use common::proto::file_transfer_server::{FileTransfer, FileTransferServer};
use common::proto::{ChunkData, ChunkRequest};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::constants::GRPC_MAX_MESSAGE_SIZE;
use crate::storage::{compute_crc32c, ChunkStorage};

/// How long to wait for new data before checking again
const POLL_INTERVAL_MS: u64 = 50;

/// Maximum wait time for a chunk to become available (30 seconds)
const MAX_WAIT_MS: u64 = 30_000;

/// Tracks cumulative bytes uploaded per job
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
    upload_tracker: UploadTracker,
    chunk_meta: ChunkMetaStore,
}

impl FileService {
    pub fn new(storage: Arc<ChunkStorage>, upload_tracker: UploadTracker, chunk_meta: ChunkMetaStore) -> Self {
        Self { storage, upload_tracker, chunk_meta }
    }

    pub fn into_server(self) -> FileTransferServer<Self> {
        FileTransferServer::new(self)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    }
}

#[tonic::async_trait]
impl FileTransfer for FileService {
    type StreamChunksStream = ReceiverStream<Result<ChunkData, Status>>;

    async fn stream_chunks(
        &self,
        request: Request<ChunkRequest>,
    ) -> Result<Response<Self::StreamChunksStream>, Status> {
        let req = request.into_inner();

        let job_id = Uuid::parse_str(&req.job_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid job ID: {}", e)))?;

        let start_chunk = req.start_from_chunk_id;
        let storage = self.storage.clone();
        let upload_tracker = self.upload_tracker.clone();
        let chunk_meta = self.chunk_meta.clone();

        let (tx, rx) = mpsc::channel(4); // Small buffer since chunks are large

        tracing::info!("Starting chunk stream for job {} from chunk {}", job_id, start_chunk);

        tokio::spawn(async move {
            // Get chunk metadata
            let meta = {
                let metas = chunk_meta.read().await;
                match metas.get(&job_id) {
                    Some(m) => ChunkMeta {
                        chunk_size: m.chunk_size,
                        total_size: m.total_size,
                        total_chunks: m.total_chunks,
                    },
                    None => {
                        tracing::warn!("No chunk metadata for job {}", job_id);
                        let _ = tx.send(Err(Status::not_found("Job metadata not found"))).await;
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
                while !storage.is_chunk_complete(job_id, chunk_id, chunk_size, total_size).await {
                    if waited_ms >= MAX_WAIT_MS {
                        tracing::warn!(
                            "Chunk {} for job {} not available after {}ms, ending stream",
                            chunk_id, job_id, waited_ms
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

                // Read the chunk
                let data = match storage.read_range(job_id, offset, this_chunk_size as usize).await {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::error!("Failed to read chunk {} for job {}: {}", chunk_id, job_id, e);
                        let _ = tx.send(Err(Status::internal(format!("Failed to read chunk: {}", e)))).await;
                        return;
                    }
                };

                // Compute CRC32C
                let crc32c = compute_crc32c(&data);

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
                        "Stream for job {} ended early: client disconnected at chunk {}",
                        job_id, chunk_id
                    );
                    return;
                }

                // Track uploaded bytes
                {
                    let mut tracker = upload_tracker.write().await;
                    *tracker.entry(job_id).or_insert(0) += chunk_len;
                }

                tracing::debug!("Streamed chunk {} ({} bytes) for job {}", chunk_id, chunk_len, job_id);
            }

            tracing::info!(
                "Finished streaming job {} ({} chunks)",
                job_id, total_chunks - start_chunk
            );
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
