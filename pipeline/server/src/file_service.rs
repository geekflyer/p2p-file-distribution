use common::proto::file_transfer_server::{FileTransfer, FileTransferServer};
use common::proto::{PieceData, ShardRequest};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::constants::GRPC_MAX_MESSAGE_SIZE;
use crate::storage::{compute_crc32c, ShardStorage};

/// Chunk size for streaming (1MB) - balances overhead vs reliability
const STREAM_CHUNK_SIZE: usize = 1 * 1024 * 1024;

/// How long to wait for new data before checking again
const POLL_INTERVAL_MS: u64 = 50;

pub struct FileService {
    storage: Arc<ShardStorage>,
}

impl FileService {
    pub fn new(storage: Arc<ShardStorage>) -> Self {
        Self { storage }
    }

    pub fn into_server(self) -> FileTransferServer<Self> {
        FileTransferServer::new(self)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    }
}

#[tonic::async_trait]
impl FileTransfer for FileService {
    type StreamShardsStream = ReceiverStream<Result<PieceData, Status>>;

    async fn stream_shards(
        &self,
        request: Request<ShardRequest>,
    ) -> Result<Response<Self::StreamShardsStream>, Status> {
        let req = request.into_inner();

        let job_id = Uuid::parse_str(&req.job_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid job ID: {}", e)))?;

        let shard_id = req.from_shard;
        let storage = self.storage.clone();

        let (tx, rx) = mpsc::channel(16);

        tracing::info!("Starting stream for shard {} of job {}", shard_id, job_id);

        tokio::spawn(async move {
            let mut retries = 0;
            const MAX_RETRIES: u32 = 600; // Wait up to 30 seconds (600 * 50ms)

            // Wait for shard to start (either complete or partial)
            while !storage.shard_exists(job_id, shard_id).await
                && !storage.partial_exists(job_id, shard_id).await
            {
                retries += 1;
                if retries >= MAX_RETRIES {
                    tracing::warn!(
                        "Shard {} for job {} not available after {} retries, ending stream",
                        shard_id, job_id, retries
                    );
                    return;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
            }

            // Stream pieces as they become available
            let mut bytes_sent: u64 = 0;
            let mut piece_index: i32 = 0;
            let mut stall_count = 0;
            const MAX_STALL: u32 = 600; // 30 seconds of no new data

            loop {
                // Check if shard is complete (mutable - may be updated below)
                let mut is_complete = storage.shard_exists(job_id, shard_id).await;

                // Get current available size (using metadata, not reading file content)
                // Handle race: partial may have been renamed to complete since last check
                let available_size = if is_complete {
                    match storage.get_shard_size(job_id, shard_id).await {
                        Ok(size) => size,
                        Err(e) => {
                            tracing::error!("Failed to get shard size {}: {}", shard_id, e);
                            let _ = tx.send(Err(Status::internal(format!("Failed to get shard size: {}", e)))).await;
                            return;
                        }
                    }
                } else {
                    // Try to get partial size, but if it fails, the shard might have just been finalized
                    match storage.get_partial_size(job_id, shard_id).await {
                        Ok(size) => size,
                        Err(_) => {
                            // Partial file gone - check if shard is now complete
                            if storage.shard_exists(job_id, shard_id).await {
                                // Shard was just finalized - update flag and get complete size
                                is_complete = true;
                                match storage.get_shard_size(job_id, shard_id).await {
                                    Ok(size) => size,
                                    Err(e) => {
                                        tracing::error!("Failed to get finalized shard size {}: {}", shard_id, e);
                                        let _ = tx.send(Err(Status::internal(format!("Shard finalized but can't read size: {}", e)))).await;
                                        return;
                                    }
                                }
                            } else {
                                // Neither partial nor complete exists - wait for it
                                0
                            }
                        }
                    }
                };

                // Send any new complete pieces
                while bytes_sent + STREAM_CHUNK_SIZE as u64 <= available_size
                    || (is_complete && bytes_sent < available_size)
                {
                    let chunk_size = if is_complete && available_size - bytes_sent < STREAM_CHUNK_SIZE as u64 {
                        // Last chunk of complete shard
                        (available_size - bytes_sent) as usize
                    } else {
                        STREAM_CHUNK_SIZE
                    };

                    let chunk = match storage.read_shard_range(job_id, shard_id, bytes_sent, chunk_size).await {
                        Ok(data) => data,
                        Err(e) => {
                            tracing::error!("Failed to read shard range at {}: {}", bytes_sent, e);
                            let _ = tx.send(Err(Status::internal(format!("Failed to read shard: {}", e)))).await;
                            return;
                        }
                    };

                    if chunk.is_empty() {
                        break;
                    }

                    let crc32c = compute_crc32c(&chunk);

                    // Calculate total pieces (unknown until complete, use -1 as placeholder)
                    let total_pieces = if is_complete {
                        ((available_size as usize + STREAM_CHUNK_SIZE - 1) / STREAM_CHUNK_SIZE) as i32
                    } else {
                        -1 // Unknown until shard is complete
                    };

                    let piece_data = PieceData {
                        shard_id,
                        piece_index,
                        total_pieces,
                        data: chunk,
                        crc32c,
                    };

                    if tx.send(Ok(piece_data)).await.is_err() {
                        // Receiver dropped - client disconnected
                        tracing::info!(
                            "Stream for shard {} ended early: client disconnected at {} bytes ({} pieces sent)",
                            shard_id, bytes_sent, piece_index
                        );
                        return;
                    }

                    bytes_sent += chunk_size as u64;
                    piece_index += 1;
                    stall_count = 0; // Reset stall counter on progress
                }

                // If complete and all sent, we're done
                if is_complete && bytes_sent >= available_size {
                    tracing::info!(
                        "Finished streaming shard {} ({} bytes, {} pieces)",
                        shard_id, bytes_sent, piece_index
                    );
                    return;
                }

                // Wait for more data
                stall_count += 1;
                if stall_count >= MAX_STALL {
                    tracing::warn!(
                        "Shard {} stalled at {} bytes after {} polls, ending stream",
                        shard_id, bytes_sent, stall_count
                    );
                    return;
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
