use std::path::PathBuf;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub struct ShardStorage {
    data_dir: PathBuf,
}

impl ShardStorage {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Get the path for a complete shard file
    fn shard_path(&self, job_id: &str, shard_id: i32) -> PathBuf {
        self.data_dir
            .join(job_id)
            .join(format!("shard_{:04}.bin", shard_id))
    }

    /// Get the path for a partial (in-progress) shard file
    fn partial_path(&self, job_id: &str, shard_id: i32) -> PathBuf {
        self.data_dir
            .join(job_id)
            .join(format!("shard_{:04}.bin.partial", shard_id))
    }

    /// Get the path for a partial shard file (public version for parallel downloads)
    pub fn get_partial_path(&self, job_id: &str, shard_id: i32) -> PathBuf {
        self.partial_path(job_id, shard_id)
    }

    /// Get the job directory path
    fn job_dir(&self, job_id: &str) -> PathBuf {
        self.data_dir.join(job_id)
    }

    /// Ensure job directory exists
    pub async fn ensure_job_dir(&self, job_id: &str) -> anyhow::Result<()> {
        let dir = self.job_dir(job_id);
        fs::create_dir_all(&dir).await?;
        Ok(())
    }

    /// Start a new partial shard (removes any existing partial file)
    pub async fn start_partial_shard(&self, job_id: &str, shard_id: i32) -> anyhow::Result<()> {
        self.ensure_job_dir(job_id).await?;
        let partial_path = self.partial_path(job_id, shard_id);
        let _ = fs::remove_file(&partial_path).await;
        File::create(&partial_path).await?;
        Ok(())
    }

    /// Append data to a partial shard
    pub async fn append_to_partial(&self, job_id: &str, shard_id: i32, data: &[u8]) -> anyhow::Result<()> {
        let partial_path = self.partial_path(job_id, shard_id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&partial_path)
            .await?;
        file.write_all(data).await?;
        file.flush().await?;
        Ok(())
    }

    /// Finalize a partial shard by renaming it to the complete shard path
    pub async fn finalize_shard(&self, job_id: &str, shard_id: i32) -> anyhow::Result<()> {
        let partial_path = self.partial_path(job_id, shard_id);
        let shard_path = self.shard_path(job_id, shard_id);
        fs::rename(&partial_path, &shard_path).await?;
        Ok(())
    }

    /// Abort a partial shard download (delete the partial file)
    pub async fn abort_partial(&self, job_id: &str, shard_id: i32) -> anyhow::Result<()> {
        let partial_path = self.partial_path(job_id, shard_id);
        let _ = fs::remove_file(&partial_path).await;
        Ok(())
    }

    /// Check if a complete shard exists
    pub async fn shard_exists(&self, job_id: &str, shard_id: i32) -> bool {
        let path = self.shard_path(job_id, shard_id);
        fs::try_exists(&path).await.unwrap_or(false)
    }

    /// Read a complete shard from storage
    pub async fn read_shard(&self, job_id: &str, shard_id: i32) -> anyhow::Result<Vec<u8>> {
        let path = self.shard_path(job_id, shard_id);
        let data = fs::read(&path).await?;
        Ok(data)
    }

    /// Build a bitmap of all owned shards for a job
    pub async fn get_shards_owned_bitmap(&self, job_id: &str, total_shards: i32) -> Vec<u8> {
        let needed_bytes = ((total_shards + 7) / 8) as usize;
        let mut bitmap = vec![0u8; needed_bytes];

        let dir = self.job_dir(job_id);
        if !dir.exists() {
            return bitmap;
        }

        if let Ok(mut entries) = fs::read_dir(&dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("shard_") && name.ends_with(".bin") && !name.ends_with(".partial") {
                        if let Some(shard_id) = name.get(6..10).and_then(|s| s.parse::<i32>().ok()) {
                            if shard_id < total_shards {
                                let byte_idx = (shard_id / 8) as usize;
                                let bit_idx = (shard_id % 8) as u8;
                                if byte_idx < bitmap.len() {
                                    bitmap[byte_idx] |= 1 << bit_idx;
                                }
                            }
                        }
                    }
                }
            }
        }

        bitmap
    }

    /// Count owned shards for a job
    pub async fn count_owned_shards(&self, job_id: &str) -> i32 {
        let dir = self.job_dir(job_id);
        if !dir.exists() {
            return 0;
        }

        let mut count = 0;
        if let Ok(mut entries) = fs::read_dir(&dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("shard_") && name.ends_with(".bin") && !name.ends_with(".partial") {
                        count += 1;
                    }
                }
            }
        }
        count
    }

    /// Delete all data for a job
    pub async fn delete_job(&self, job_id: &str) -> anyhow::Result<()> {
        let dir = self.job_dir(job_id);
        if dir.exists() {
            fs::remove_dir_all(&dir).await?;
            tracing::info!("Deleted data for job {}", job_id);
        }
        Ok(())
    }
}

/// Compute CRC32C checksum of data
pub fn compute_crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}
