use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::storage::ChunkStorage;

/// Chunk metadata needed for streaming
pub struct ChunkMeta {
    pub chunk_size: u64,
    pub total_size: u64,
    pub total_chunks: i32,
}

/// Chunk metadata store (set by main when task is received)
pub type ChunkMetaStore = Arc<RwLock<HashMap<Uuid, ChunkMeta>>>;

/// Tracks cumulative bytes uploaded per file
pub type UploadTracker = Arc<RwLock<HashMap<Uuid, u64>>>;

/// How long to wait for new data before checking again (ms)
const POLL_INTERVAL_MS: u64 = 50;

/// Maximum wait time for a chunk to become available (30 seconds)
const MAX_WAIT_MS: u64 = 30_000;

/// Start the TCP P2P server
pub async fn start_tcp_server(
    addr: SocketAddr,
    storage: Arc<ChunkStorage>,
    chunk_meta: ChunkMetaStore,
    upload_tracker: UploadTracker,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("TCP P2P server listening on {}", addr);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        let storage = storage.clone();
        let chunk_meta = chunk_meta.clone();
        let upload_tracker = upload_tracker.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, peer_addr, storage, chunk_meta, upload_tracker).await {
                tracing::warn!("Connection from {} error: {}", peer_addr, e);
            }
        });
    }
}

/// Handle a single P2P connection
async fn handle_connection(
    mut socket: TcpStream,
    peer_addr: SocketAddr,
    storage: Arc<ChunkStorage>,
    chunk_meta: ChunkMetaStore,
    upload_tracker: UploadTracker,
) -> anyhow::Result<()> {
    // Read request: [file_id: 16 bytes][start_chunk: 4 bytes i32 LE]
    let mut request_buf = [0u8; 20];
    socket.read_exact(&mut request_buf).await?;

    let file_id = Uuid::from_slice(&request_buf[0..16])?;
    let start_chunk = i32::from_le_bytes([
        request_buf[16],
        request_buf[17],
        request_buf[18],
        request_buf[19],
    ]);

    tracing::info!(
        "P2P request from {} for file {} starting at chunk {}",
        peer_addr, file_id, start_chunk
    );

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
                return Ok(());
            }
        }
    };

    // Convert to std TcpStream for sendfile
    let std_socket = socket.into_std()?;
    std_socket.set_nonblocking(false)?;

    // Stream chunks using sendfile
    stream_chunks_sendfile(
        std_socket,
        file_id,
        start_chunk,
        &meta,
        storage,
        upload_tracker,
    )
    .await
}

/// Stream chunks using sendfile for zero-copy transfer
async fn stream_chunks_sendfile(
    socket: std::net::TcpStream,
    file_id: Uuid,
    start_chunk: i32,
    meta: &ChunkMeta,
    storage: Arc<ChunkStorage>,
    upload_tracker: UploadTracker,
) -> anyhow::Result<()> {
    let chunk_size = meta.chunk_size;
    let total_size = meta.total_size;
    let total_chunks = meta.total_chunks;

    // Wait for first chunk before opening file (file might not exist yet)
    let mut waited_ms: u64 = 0;
    while !storage
        .is_chunk_complete(file_id, start_chunk, chunk_size, total_size)
        .await
    {
        if waited_ms >= MAX_WAIT_MS {
            tracing::warn!(
                "First chunk {} for file {} not available after {}ms, ending stream",
                start_chunk, file_id, waited_ms
            );
            return Ok(());
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
        waited_ms += POLL_INTERVAL_MS;
    }

    // Open the data file once for all chunks (file now exists)
    let (data_file, _data_fd) = storage.open_file_for_sendfile(file_id)?;

    for chunk_id in start_chunk..total_chunks {
        // Wait for chunk to be available (skip first chunk, already waited)
        if chunk_id > start_chunk {
            let mut waited_ms: u64 = 0;
            while !storage
                .is_chunk_complete(file_id, chunk_id, chunk_size, total_size)
                .await
            {
                if waited_ms >= MAX_WAIT_MS {
                    tracing::warn!(
                        "Chunk {} for file {} not available after {}ms, ending stream",
                        chunk_id, file_id, waited_ms
                    );
                    return Ok(());
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
                waited_ms += POLL_INTERVAL_MS;
            }
        }

        // Get chunk info (offset, size, crc32c)
        let chunk_info = match storage
            .get_chunk_info(file_id, chunk_id, chunk_size, total_size, total_chunks)
            .await
        {
            Ok(info) => info,
            Err(e) => {
                tracing::error!("Failed to get chunk info for {} chunk {}: {}", file_id, chunk_id, e);
                return Err(e);
            }
        };

        // Send header: [chunk_id: 4][crc32c: 4][size: 4] = 12 bytes
        let mut header = [0u8; 12];
        header[0..4].copy_from_slice(&chunk_id.to_le_bytes());
        header[4..8].copy_from_slice(&chunk_info.crc32c.to_le_bytes());
        header[8..12].copy_from_slice(&chunk_info.size.to_le_bytes());

        // Use blocking I/O for sendfile (must be in spawn_blocking)
        let offset = chunk_info.offset;
        let size = chunk_info.size as usize;

        // Clone what we need for the blocking task
        let socket_clone = socket.try_clone()?;
        let data_file_clone = data_file.try_clone()?;

        let bytes_sent = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
            // Write header first
            let mut sock = &socket_clone;
            sock.write_all(&header)?;

            // Use sendfile for zero-copy transfer
            #[cfg(target_os = "linux")]
            {
                use nix::sys::sendfile::sendfile;
                use std::os::fd::AsRawFd;
                let mut sent = 0usize;
                let mut file_offset = offset as i64;
                while sent < size {
                    let n = sendfile(
                        socket_clone.as_raw_fd(),
                        data_file_clone.as_raw_fd(),
                        Some(&mut file_offset),
                        size - sent,
                    )?;
                    if n == 0 {
                        anyhow::bail!("sendfile returned 0");
                    }
                    sent += n;
                }
                Ok(sent)
            }

            #[cfg(target_os = "macos")]
            {
                use std::io::{Read, Seek, SeekFrom};
                // macOS sendfile has different semantics, use regular read+write
                let mut file = data_file_clone;
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; size];
                file.read_exact(&mut buf)?;
                sock.write_all(&buf)?;
                Ok(size)
            }

            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            {
                use std::io::{Read, Seek, SeekFrom};
                let mut file = data_file_clone;
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; size];
                file.read_exact(&mut buf)?;
                sock.write_all(&buf)?;
                Ok(size)
            }
        })
        .await??;

        // Track uploaded bytes
        {
            let mut tracker = upload_tracker.write().await;
            *tracker.entry(file_id).or_insert(0) += bytes_sent as u64;
        }

        tracing::debug!(
            "Sent chunk {} ({} bytes, crc32c {:08x}) for file {}",
            chunk_id, bytes_sent, chunk_info.crc32c, file_id
        );
    }

    tracing::info!(
        "Finished streaming file {} ({} chunks)",
        file_id, total_chunks - start_chunk
    );

    Ok(())
}
