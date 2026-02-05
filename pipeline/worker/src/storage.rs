use std::io::SeekFrom;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

pub struct ChunkStorage {
    data_dir: PathBuf,
}

/// Metadata about a chunk for P2P serving
#[derive(Debug, Clone, Copy)]
pub struct ChunkInfo {
    pub offset: u64,
    pub size: u32,
    pub crc32c: u32,
}

impl ChunkStorage {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Get the file directory path
    fn file_dir(&self, file_id: Uuid) -> PathBuf {
        self.data_dir.join(file_id.to_string())
    }

    /// Output file path (partial during download)
    fn output_partial_path(&self, file_id: Uuid) -> PathBuf {
        self.file_dir(file_id).join("output.bin.partial")
    }

    /// Output file path (final)
    fn output_path(&self, file_id: Uuid) -> PathBuf {
        self.file_dir(file_id).join("output.bin")
    }

    /// CRC32C sidecar file path (stores CRC32C per chunk as array of u32 LE)
    fn crc32c_path(&self, file_id: Uuid) -> PathBuf {
        self.file_dir(file_id).join("crc32c.bin")
    }

    /// Ensure file directory exists
    async fn ensure_file_dir(&self, file_id: Uuid) -> anyhow::Result<()> {
        let dir = self.file_dir(file_id);
        fs::create_dir_all(&dir).await?;
        Ok(())
    }

    /// Get file size of output file (partial or final)
    async fn get_file_size(&self, file_id: Uuid) -> u64 {
        let partial_path = self.output_partial_path(file_id);
        let final_path = self.output_path(file_id);

        if let Ok(meta) = fs::metadata(&final_path).await {
            return meta.len();
        }
        if let Ok(meta) = fs::metadata(&partial_path).await {
            return meta.len();
        }
        0
    }

    /// Initialize output file (create empty file, truncate any partial data to chunk boundary)
    pub async fn initialize(&self, file_id: Uuid, chunk_size_bytes: u64) -> anyhow::Result<()> {
        self.ensure_file_dir(file_id).await?;
        let path = self.output_partial_path(file_id);

        if fs::try_exists(&path).await.unwrap_or(false) {
            // File exists - truncate to last complete chunk boundary for crash recovery
            let meta = fs::metadata(&path).await?;
            let current_size = meta.len();
            let complete_size = (current_size / chunk_size_bytes) * chunk_size_bytes;
            if complete_size < current_size {
                tracing::info!(
                    "Truncating partial chunk for file {}: {} -> {} bytes",
                    file_id, current_size, complete_size
                );
                let file = OpenOptions::new().write(true).open(&path).await?;
                file.set_len(complete_size).await?;
            }
        } else {
            // Create new empty file
            File::create(&path).await?;
            tracing::info!("Created output file for {}", file_id);
        }
        Ok(())
    }

    /// Append chunk to output file (sequential writes only)
    pub async fn append_chunk(&self, file_id: Uuid, data: &[u8]) -> anyhow::Result<()> {
        let partial_path = self.output_partial_path(file_id);
        let final_path = self.output_path(file_id);

        let path = if fs::try_exists(&partial_path).await.unwrap_or(false) {
            partial_path
        } else if fs::try_exists(&final_path).await.unwrap_or(false) {
            final_path
        } else {
            anyhow::bail!("No output file exists for {}", file_id);
        };

        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .await?;

        file.write_all(data).await?;
        // Sync to ensure data is persisted
        file.sync_data().await?;

        Ok(())
    }

    /// Check if a chunk is complete (based on file size)
    pub async fn is_chunk_complete(&self, file_id: Uuid, chunk_id: i32, chunk_size_bytes: u64, total_size: u64) -> bool {
        let file_size = self.get_file_size(file_id).await;
        // Calculate where this chunk ends
        let chunk_end = (chunk_id as u64 + 1) * chunk_size_bytes;
        // For the last chunk, the end is total_size (which may be less than chunk boundary)
        let actual_chunk_end = std::cmp::min(chunk_end, total_size);
        file_size >= actual_chunk_end
    }

    /// Get last completed chunk based on file size
    pub async fn get_last_completed_chunk(&self, file_id: Uuid, chunk_size_bytes: u64) -> i32 {
        let file_size = self.get_file_size(file_id).await;
        if file_size == 0 || chunk_size_bytes == 0 {
            return -1;
        }
        (file_size / chunk_size_bytes) as i32 - 1
    }

    /// Finalize (rename partial to final)
    pub async fn finalize(&self, file_id: Uuid) -> anyhow::Result<()> {
        let partial_path = self.output_partial_path(file_id);
        let final_path = self.output_path(file_id);

        if fs::try_exists(&partial_path).await.unwrap_or(false) {
            fs::rename(&partial_path, &final_path).await?;
            tracing::info!("Finalized output file for {}", file_id);
        }

        Ok(())
    }

    /// Verify file CRC32C
    pub async fn verify_crc32c(&self, file_id: Uuid, expected: &str) -> anyhow::Result<bool> {
        if expected.is_empty() {
            // No CRC32C to verify (e.g., emulator doesn't provide it)
            tracing::warn!("No CRC32C to verify for {}", file_id);
            return Ok(true);
        }

        let path = self.output_path(file_id);
        if !fs::try_exists(&path).await.unwrap_or(false) {
            anyhow::bail!("Output file not found for {}", file_id);
        }

        // Read file in chunks and compute CRC32C
        let mut file = File::open(&path).await?;
        let mut hasher = 0u32;
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer

        loop {
            let bytes_read = file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            hasher = crc32c::crc32c_append(hasher, &buffer[..bytes_read]);
        }

        // GCS stores CRC32C as base64-encoded big-endian bytes
        let expected_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            expected,
        )?;

        if expected_bytes.len() != 4 {
            anyhow::bail!("Invalid CRC32C length: {}", expected_bytes.len());
        }

        let expected_crc = u32::from_be_bytes([
            expected_bytes[0],
            expected_bytes[1],
            expected_bytes[2],
            expected_bytes[3],
        ]);

        let verified = hasher == expected_crc;
        if verified {
            tracing::info!("CRC32C verified for {} ({})", file_id, expected);
        } else {
            tracing::error!(
                "CRC32C mismatch for {}: expected {:08x}, got {:08x}",
                file_id, expected_crc, hasher
            );
        }

        Ok(verified)
    }

    /// Delete all data for a file (used when file is purged)
    pub async fn delete_file(&self, file_id: Uuid) -> anyhow::Result<()> {
        let dir = self.file_dir(file_id);
        if dir.exists() {
            fs::remove_dir_all(&dir).await?;
            tracing::info!("Deleted data for {}", file_id);
        }
        Ok(())
    }

    /// Store CRC32C for a chunk (appends to sidecar file)
    pub async fn store_chunk_crc32c(&self, file_id: Uuid, chunk_id: i32, crc32c: u32) -> anyhow::Result<()> {
        let crc_path = self.crc32c_path(file_id);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&crc_path)
            .await?;

        // Seek to position for this chunk (each entry is 4 bytes)
        let offset = (chunk_id as u64) * 4;
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(&crc32c.to_le_bytes()).await?;
        file.sync_data().await?;

        Ok(())
    }

    /// Get CRC32C for a chunk (reads from sidecar file)
    pub async fn get_chunk_crc32c(&self, file_id: Uuid, chunk_id: i32) -> anyhow::Result<u32> {
        let crc_path = self.crc32c_path(file_id);

        let mut file = File::open(&crc_path).await?;
        let offset = (chunk_id as u64) * 4;
        file.seek(SeekFrom::Start(offset)).await?;

        let mut buf = [0u8; 4];
        file.read_exact(&mut buf).await?;

        Ok(u32::from_le_bytes(buf))
    }

    /// Get chunk info for P2P serving (offset, size, crc32c)
    pub async fn get_chunk_info(
        &self,
        file_id: Uuid,
        chunk_id: i32,
        chunk_size_bytes: u64,
        total_size: u64,
        total_chunks: i32,
    ) -> anyhow::Result<ChunkInfo> {
        let offset = chunk_id as u64 * chunk_size_bytes;
        let size = if chunk_id == total_chunks - 1 {
            // Last chunk may be smaller
            let remainder = total_size % chunk_size_bytes;
            if remainder == 0 { chunk_size_bytes as u32 } else { remainder as u32 }
        } else {
            chunk_size_bytes as u32
        };

        let crc32c = self.get_chunk_crc32c(file_id, chunk_id).await?;

        Ok(ChunkInfo { offset, size, crc32c })
    }

    /// Open file and return raw fd for sendfile (blocking operation, call from spawn_blocking)
    pub fn open_file_for_sendfile(&self, file_id: Uuid) -> anyhow::Result<(std::fs::File, i32)> {
        // Try final file first, then partial
        let final_path = self.output_path(file_id);
        let partial_path = self.output_partial_path(file_id);

        let path = if final_path.exists() {
            final_path
        } else if partial_path.exists() {
            partial_path
        } else {
            anyhow::bail!("No output file exists for {}", file_id);
        };

        let file = std::fs::File::open(&path)?;
        let fd = file.as_raw_fd();
        Ok((file, fd))
    }
}

/// Compute CRC32C checksum of data (hardware accelerated on modern CPUs)
pub fn compute_crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}
