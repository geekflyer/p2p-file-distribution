use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

pub struct ChunkStorage {
    data_dir: PathBuf,
}

impl ChunkStorage {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Get the job directory path
    fn job_dir(&self, job_id: Uuid) -> PathBuf {
        self.data_dir.join(job_id.to_string())
    }

    /// Output file path for a job (partial during download)
    fn output_partial_path(&self, job_id: Uuid) -> PathBuf {
        self.job_dir(job_id).join("output.bin.partial")
    }

    /// Output file path for a job (final)
    fn output_path(&self, job_id: Uuid) -> PathBuf {
        self.job_dir(job_id).join("output.bin")
    }

    /// Ensure job directory exists
    async fn ensure_job_dir(&self, job_id: Uuid) -> anyhow::Result<()> {
        let dir = self.job_dir(job_id);
        fs::create_dir_all(&dir).await?;
        Ok(())
    }

    /// Get file size of output file (partial or final)
    async fn get_file_size(&self, job_id: Uuid) -> u64 {
        let partial_path = self.output_partial_path(job_id);
        let final_path = self.output_path(job_id);

        if let Ok(meta) = fs::metadata(&final_path).await {
            return meta.len();
        }
        if let Ok(meta) = fs::metadata(&partial_path).await {
            return meta.len();
        }
        0
    }

    /// Check if the job is initialized (output file exists)
    pub async fn is_initialized(&self, job_id: Uuid) -> bool {
        let partial = self.output_partial_path(job_id);
        let final_path = self.output_path(job_id);
        fs::try_exists(&partial).await.unwrap_or(false) ||
            fs::try_exists(&final_path).await.unwrap_or(false)
    }

    /// Initialize output file (create empty file, truncate any partial data to chunk boundary)
    pub async fn initialize(&self, job_id: Uuid, chunk_size: u64) -> anyhow::Result<()> {
        self.ensure_job_dir(job_id).await?;
        let path = self.output_partial_path(job_id);

        if fs::try_exists(&path).await.unwrap_or(false) {
            // File exists - truncate to last complete chunk boundary for crash recovery
            let meta = fs::metadata(&path).await?;
            let current_size = meta.len();
            let complete_size = (current_size / chunk_size) * chunk_size;
            if complete_size < current_size {
                tracing::info!(
                    "Truncating partial chunk for job {}: {} -> {} bytes",
                    job_id, current_size, complete_size
                );
                let file = OpenOptions::new().write(true).open(&path).await?;
                file.set_len(complete_size).await?;
            }
        } else {
            // Create new empty file
            File::create(&path).await?;
            tracing::info!("Created output file for job {}", job_id);
        }
        Ok(())
    }

    /// Append chunk to output file (sequential writes only)
    pub async fn append_chunk(&self, job_id: Uuid, data: &[u8]) -> anyhow::Result<()> {
        let partial_path = self.output_partial_path(job_id);
        let final_path = self.output_path(job_id);

        let path = if fs::try_exists(&partial_path).await.unwrap_or(false) {
            partial_path
        } else if fs::try_exists(&final_path).await.unwrap_or(false) {
            final_path
        } else {
            anyhow::bail!("No output file exists for job {}", job_id);
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
    pub async fn is_chunk_complete(&self, job_id: Uuid, chunk_id: i32, chunk_size: u64, total_size: u64) -> bool {
        let file_size = self.get_file_size(job_id).await;
        // Calculate where this chunk ends
        let chunk_end = (chunk_id as u64 + 1) * chunk_size;
        // For the last chunk, the end is total_size (which may be less than chunk boundary)
        let actual_chunk_end = std::cmp::min(chunk_end, total_size);
        file_size >= actual_chunk_end
    }

    /// Get last completed chunk based on file size
    pub async fn get_last_completed_chunk(&self, job_id: Uuid, chunk_size: u64) -> i32 {
        let file_size = self.get_file_size(job_id).await;
        if file_size == 0 || chunk_size == 0 {
            return -1;
        }
        (file_size / chunk_size) as i32 - 1
    }

    /// Read range from output file (for P2P serving)
    pub async fn read_range(&self, job_id: Uuid, offset: u64, length: usize) -> anyhow::Result<Vec<u8>> {
        // Try final file first, then partial
        let final_path = self.output_path(job_id);
        let partial_path = self.output_partial_path(job_id);

        let path = if fs::try_exists(&final_path).await.unwrap_or(false) {
            final_path
        } else if fs::try_exists(&partial_path).await.unwrap_or(false) {
            partial_path
        } else {
            anyhow::bail!("No output file exists for job {}", job_id);
        };

        let mut file = File::open(&path).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    /// Finalize (rename partial to final)
    pub async fn finalize(&self, job_id: Uuid) -> anyhow::Result<()> {
        let partial_path = self.output_partial_path(job_id);
        let final_path = self.output_path(job_id);

        if fs::try_exists(&partial_path).await.unwrap_or(false) {
            fs::rename(&partial_path, &final_path).await?;
            tracing::info!("Finalized output file for job {}", job_id);
        }

        Ok(())
    }

    /// Verify file CRC32C
    pub async fn verify_crc32c(&self, job_id: Uuid, expected: &str) -> anyhow::Result<bool> {
        if expected.is_empty() {
            // No CRC32C to verify (e.g., emulator doesn't provide it)
            tracing::warn!("No CRC32C to verify for job {}", job_id);
            return Ok(true);
        }

        let path = self.output_path(job_id);
        if !fs::try_exists(&path).await.unwrap_or(false) {
            anyhow::bail!("Output file not found for job {}", job_id);
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
            tracing::info!("CRC32C verified for job {} ({})", job_id, expected);
        } else {
            tracing::error!(
                "CRC32C mismatch for job {}: expected {:08x}, got {:08x}",
                job_id, expected_crc, hasher
            );
        }

        Ok(verified)
    }

    /// Delete all data for a job (used when job is purged)
    pub async fn delete_job(&self, job_id: Uuid) -> anyhow::Result<()> {
        let dir = self.job_dir(job_id);
        if dir.exists() {
            fs::remove_dir_all(&dir).await?;
            tracing::info!("Deleted data for job {}", job_id);
        }
        Ok(())
    }
}

/// Compute CRC32C checksum of data (hardware accelerated on modern CPUs)
pub fn compute_crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}
