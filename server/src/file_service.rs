use common::proto::file_transfer_server::{FileTransfer, FileTransferServer};
use common::proto::{PieceData, ShardRequest};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::constants::GRPC_MAX_MESSAGE_SIZE;
use crate::storage::{compute_crc32c, ShardStorage};

/// Chunk size for streaming (256KB)
const STREAM_CHUNK_SIZE: usize = 256 * 1024;

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

        tokio::spawn(async move {
            let mut retries = 0;
            const MAX_RETRIES: u32 = 100; // Wait up to 10 seconds (100 * 100ms)

            // Wait for shard to be available
            while !storage.shard_exists(job_id, shard_id).await {
                retries += 1;
                if retries >= MAX_RETRIES {
                    tracing::debug!(
                        "Shard {} for job {} not available after {} retries, ending stream",
                        shard_id, job_id, retries
                    );
                    return;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }

            // Read the complete shard
            let shard_data = match storage.read_shard(job_id, shard_id).await {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("Failed to read shard {}: {}", shard_id, e);
                    let _ = tx.send(Err(Status::internal(format!("Failed to read shard: {}", e)))).await;
                    return;
                }
            };

            // Stream the shard in chunks
            let mut offset = 0;
            let mut chunk_index = 0;

            while offset < shard_data.len() {
                let end = std::cmp::min(offset + STREAM_CHUNK_SIZE, shard_data.len());
                let chunk = &shard_data[offset..end];
                let crc32c = compute_crc32c(chunk);

                let piece_data = PieceData {
                    shard_id,
                    piece_index: chunk_index,
                    total_pieces: ((shard_data.len() + STREAM_CHUNK_SIZE - 1) / STREAM_CHUNK_SIZE) as i32,
                    data: chunk.to_vec(),
                    crc32c,
                };

                if tx.send(Ok(piece_data)).await.is_err() {
                    // Receiver dropped
                    break;
                }

                offset = end;
                chunk_index += 1;
            }

            tracing::debug!("Finished streaming shard {} ({} bytes, {} chunks)", shard_id, shard_data.len(), chunk_index);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
