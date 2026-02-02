use common::proto::shard_transfer_server::{ShardTransfer, ShardTransferServer};
use common::proto::{PieceData, ShardRequest};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::storage::{compute_crc32c, ShardStorage};

const GRPC_MAX_MESSAGE_SIZE: usize = 512 * 1024 * 1024;
const STREAM_CHUNK_SIZE: usize = 256 * 1024;

pub struct ShardService {
    storage: Arc<ShardStorage>,
}

impl ShardService {
    pub fn new(storage: Arc<ShardStorage>) -> Self {
        Self { storage }
    }

    pub fn into_server(self) -> ShardTransferServer<Self> {
        ShardTransferServer::new(self)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
    }
}

#[tonic::async_trait]
impl ShardTransfer for ShardService {
    type StreamShardStream = ReceiverStream<Result<PieceData, Status>>;

    async fn stream_shard(
        &self,
        request: Request<ShardRequest>,
    ) -> Result<Response<Self::StreamShardStream>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let shard_id = req.shard_id;
        let storage = self.storage.clone();

        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            // Wait for shard to be available (up to 10 seconds)
            let mut retries = 0;
            const MAX_RETRIES: u32 = 100;

            while !storage.shard_exists(&job_id, shard_id).await {
                retries += 1;
                if retries >= MAX_RETRIES {
                    tracing::debug!(
                        "Shard {} for job {} not available after {} retries",
                        shard_id, job_id, retries
                    );
                    return;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }

            // Read the shard
            let shard_data = match storage.read_shard(&job_id, shard_id).await {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("Failed to read shard {}: {}", shard_id, e);
                    let _ = tx.send(Err(Status::internal(format!("Failed to read shard: {}", e)))).await;
                    return;
                }
            };

            // Stream in chunks
            let mut offset = 0;
            let mut piece_index = 0;
            let total_pieces = ((shard_data.len() + STREAM_CHUNK_SIZE - 1) / STREAM_CHUNK_SIZE) as i32;

            while offset < shard_data.len() {
                let end = std::cmp::min(offset + STREAM_CHUNK_SIZE, shard_data.len());
                let chunk = &shard_data[offset..end];
                let crc32c = compute_crc32c(chunk);

                let piece = PieceData {
                    shard_id,
                    piece_index,
                    total_pieces,
                    data: chunk.to_vec(),
                    crc32c,
                };

                if tx.send(Ok(piece)).await.is_err() {
                    break;
                }

                offset = end;
                piece_index += 1;
            }

            tracing::debug!(
                "Finished streaming shard {} ({} bytes, {} chunks)",
                shard_id, shard_data.len(), piece_index
            );
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
