use std::path::PathBuf;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use uuid::Uuid;

/// Buffered writer for streaming shard data efficiently
pub struct ShardWriter {
    writer: BufWriter<File>,
}

impl ShardWriter {
    /// Write a chunk of data
    pub async fn write(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.writer.write_all(data).await?;
        Ok(())
    }

    /// Flush any buffered data (call before finalize)
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}

pub struct ShardStorage {
    data_dir: PathBuf,
}

impl ShardStorage {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Get the path for a complete shard file
    fn shard_path(&self, job_id: Uuid, shard_id: i32) -> PathBuf {
        self.data_dir
            .join(job_id.to_string())
            .join(format!("shard_{:04}.bin", shard_id))
    }

    /// Get the path for a partial (in-progress) shard file
    fn partial_path(&self, job_id: Uuid, shard_id: i32) -> PathBuf {
        self.data_dir
            .join(job_id.to_string())
            .join(format!("shard_{:04}.bin.partial", shard_id))
    }

    /// Get the job directory path
    fn job_dir(&self, job_id: Uuid) -> PathBuf {
        self.data_dir.join(job_id.to_string())
    }

    /// Ensure job directory exists
    pub async fn ensure_job_dir(&self, job_id: Uuid) -> anyhow::Result<()> {
        let dir = self.job_dir(job_id);
        fs::create_dir_all(&dir).await?;
        Ok(())
    }

    /// Start a new partial shard (removes any existing partial file)
    pub async fn start_partial_shard(&self, job_id: Uuid, shard_id: i32) -> anyhow::Result<()> {
        self.ensure_job_dir(job_id).await?;
        let partial_path = self.partial_path(job_id, shard_id);
        // Remove existing partial if any
        let _ = fs::remove_file(&partial_path).await;
        // Create empty file
        File::create(&partial_path).await?;
        Ok(())
    }

    /// Start a new partial shard and return a buffered writer for efficient streaming
    pub async fn start_partial_shard_writer(&self, job_id: Uuid, shard_id: i32) -> anyhow::Result<ShardWriter> {
        self.ensure_job_dir(job_id).await?;
        let partial_path = self.partial_path(job_id, shard_id);
        // Remove existing partial if any
        let _ = fs::remove_file(&partial_path).await;
        // Create file with buffered writer (256KB buffer)
        let file = File::create(&partial_path).await?;
        let writer = BufWriter::with_capacity(256 * 1024, file);
        Ok(ShardWriter { writer })
    }

    /// Append data to a partial shard
    pub async fn append_to_partial(&self, job_id: Uuid, shard_id: i32, data: &[u8]) -> anyhow::Result<()> {
        let partial_path = self.partial_path(job_id, shard_id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&partial_path)
            .await?;
        file.write_all(data).await?;
        // Note: no flush() - OS buffering makes data visible for piece streaming
        // and avoids disk I/O overhead on every chunk
        Ok(())
    }

    /// Finalize a partial shard by renaming it to the complete shard path
    /// Call this after SHA256 verification passes
    pub async fn finalize_shard(&self, job_id: Uuid, shard_id: i32) -> anyhow::Result<()> {
        let partial_path = self.partial_path(job_id, shard_id);
        let shard_path = self.shard_path(job_id, shard_id);
        fs::rename(&partial_path, &shard_path).await?;
        Ok(())
    }

    /// Abort a partial shard download (delete the partial file)
    pub async fn abort_partial(&self, job_id: Uuid, shard_id: i32) -> anyhow::Result<()> {
        let partial_path = self.partial_path(job_id, shard_id);
        let _ = fs::remove_file(&partial_path).await;
        Ok(())
    }

    /// Check if a complete shard exists
    pub async fn shard_exists(&self, job_id: Uuid, shard_id: i32) -> bool {
        let path = self.shard_path(job_id, shard_id);
        fs::try_exists(&path).await.unwrap_or(false)
    }

    /// Read a complete shard from storage
    pub async fn read_shard(&self, job_id: Uuid, shard_id: i32) -> anyhow::Result<Vec<u8>> {
        let path = self.shard_path(job_id, shard_id);
        let data = fs::read(&path).await?;
        Ok(data)
    }

    /// Check if a partial shard exists
    pub async fn partial_exists(&self, job_id: Uuid, shard_id: i32) -> bool {
        let path = self.partial_path(job_id, shard_id);
        fs::try_exists(&path).await.unwrap_or(false)
    }

    /// Get the current size of a partial shard (bytes written so far)
    pub async fn get_partial_size(&self, job_id: Uuid, shard_id: i32) -> anyhow::Result<u64> {
        let path = self.partial_path(job_id, shard_id);
        let metadata = fs::metadata(&path).await?;
        Ok(metadata.len())
    }

    /// Get the size of a complete shard (without reading it into memory)
    pub async fn get_shard_size(&self, job_id: Uuid, shard_id: i32) -> anyhow::Result<u64> {
        let path = self.shard_path(job_id, shard_id);
        let metadata = fs::metadata(&path).await?;
        Ok(metadata.len())
    }

    /// Read a range from a partial or complete shard
    /// Returns the data read (may be less than requested if at end of file)
    /// Handles race condition where partial file is renamed to complete during read
    pub async fn read_shard_range(
        &self,
        job_id: Uuid,
        shard_id: i32,
        offset: u64,
        length: usize,
    ) -> anyhow::Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        // Try complete shard first, then partial, handle race condition
        let shard_path = self.shard_path(job_id, shard_id);
        let partial_path = self.partial_path(job_id, shard_id);

        let file = match File::open(&shard_path).await {
            Ok(f) => f,
            Err(_) => match File::open(&partial_path).await {
                Ok(f) => f,
                Err(_) => {
                    // One more try on complete shard (race: partial was just renamed)
                    File::open(&shard_path).await?
                }
            }
        };

        let mut file = file;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; length];
        let bytes_read = file.read(&mut buffer).await?;
        buffer.truncate(bytes_read);

        Ok(buffer)
    }

    /// Get the last completed shard for a job (-1 if none)
    /// Also cleans up any partial files on startup
    pub async fn get_last_completed_shard(&self, job_id: Uuid) -> i32 {
        let dir = self.job_dir(job_id);
        if !dir.exists() {
            return -1;
        }

        // Clean up any partial files first (incomplete downloads from previous run)
        if let Ok(mut entries) = fs::read_dir(&dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    if name.ends_with(".partial") {
                        let _ = fs::remove_file(entry.path()).await;
                        tracing::info!("Cleaned up partial file: {}", name);
                    }
                }
            }
        }

        // Find the highest completed shard with continuous sequence from 0
        let mut max_complete_shard = -1i32;

        // First pass: find all complete shards
        let mut complete_shards = Vec::new();
        if let Ok(mut entries) = fs::read_dir(&dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    // Parse shard_XXXX.bin format (not .partial)
                    if name.starts_with("shard_") && name.ends_with(".bin") && !name.ends_with(".partial") {
                        if let Some(shard_id) = name.get(6..10).and_then(|s| s.parse::<i32>().ok()) {
                            complete_shards.push(shard_id);
                        }
                    }
                }
            }
        }

        // Sort and find the last continuous shard from 0
        complete_shards.sort();
        for (expected, &actual) in complete_shards.iter().enumerate() {
            if actual != expected as i32 {
                // Gap found, return previous
                break;
            }
            max_complete_shard = actual;
        }

        max_complete_shard
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
