# Chain/Pipeline Architecture

## Overview

A linear cascading distribution system where workers form a chain, each downloading from its predecessor. The first worker downloads from GCS (origin), and each subsequent worker downloads from the one ahead of it.

```
GCS ──→ Worker 1 ──→ Worker 2 ──→ Worker 3 ──→ ... ──→ Worker N
           │              │              │                   │
           ▼              ▼              ▼                   ▼
       [chunks]      [chunks]      [chunks]            [chunks]
```

## Key Characteristics

- **Topology**: Linear chain ordered by worker address
- **Upstream assignment**: Static (worker N always pulls from worker N-1)
- **Parallelism**: Pipeline parallelism - while worker 1 downloads chunk K, worker 2 downloads chunk K-1, etc.
- **Bandwidth utilization**: Each worker uses ~1x download + ~1x upload bandwidth
- **Coordinator role**: Assigns upstream peers at check-in, computes chain ordering
- **On-the-fly chunking**: Large files chunked during download (default 64MB chunks)
- **P2P Transfer**: Raw TCP with Linux `sendfile()` for zero-copy transfer
- **Chunk verification**: CRC32C checksums stored and verified per chunk

## Components

### Coordinator (HTTP REST)

Stateless service that:
- Manages file distributions (create, cancel, purge)
- Tracks worker health via check-ins (healthy if check-in within 5s)
- Assigns upstream peers using simple ordering algorithm
- Serves Admin UI for monitoring

**Key endpoint:**
- `POST /worker/check-in` - Worker sends progress, receives tasks with upstream already computed

**Check-in request:**
```json
{
  "workerId": "localhost:50051",
  "workerAddress": "localhost:50051",
  "taskProgress": [
    {
      "fileId": "uuid",
      "lastChunkIdCompleted": 5,
      "bytesDownloaded": 402653184,
      "bytesUploaded": 134217728
    }
  ],
  "diskTotalBytes": 500000000000,
  "diskUsedBytes": 100000000000
}
```

**Check-in response:**
```json
{
  "tasks": [
    {
      "fileId": "uuid",
      "totalChunks": 16,
      "chunkSize": 67108864,
      "totalFileSizeBytes": 1073741824,
      "crc32c": "base64-encoded",
      "upstream": {
        "type": "gcs",
        "gcsPath": "gs://bucket/file.bin"
      }
    }
  ],
  "cancelFileIds": [],
  "purgeFileIds": []
}
```

**Upstream assignment algorithm:**
```
workers = sort(healthy_workers, by=address)
index = workers.index(requesting_worker)
if index == 0:
    return Upstream::Gcs { gcs_path }
else:
    return Upstream::Peer { worker_id, worker_address: workers[index - 1] }
```

### Worker (HTTP client + TCP server)

Runs on each machine:
- Checks in with coordinator (every 2s)
- Downloads chunks sequentially from assigned upstream (GCS or peer)
- Serves completed chunks to downstream peers via raw TCP with `sendfile()`
- Reports progress in check-in payload

**Download flow:**
```
1. Check in with coordinator, receive tasks with upstream
2. For each task:
   a. Initialize output file (or resume from last chunk)
   b. Download chunks from upstream (GCS HTTP or peer TCP)
   c. Verify chunk CRC32C checksums
   d. Append to output file
   e. Report progress in next check-in
3. When complete, finalize file and verify full-file CRC32C
```

**TCP P2P Protocol:**

Request (20 bytes):
```
[file_id: 16 bytes UUID][start_chunk: 4 bytes i32 LE]
```

Response (streaming, per chunk):
```
[chunk_id: 4 bytes i32 LE][crc32c: 4 bytes u32 LE][size: 4 bytes u32 LE][data: size bytes]
```

The server uses Linux `sendfile()` syscall for zero-copy transfer directly from disk to socket.

### Storage

Local filesystem storage:
- Output files stored as `{data_dir}/{file_id}/output.bin`
- CRC32C checksums stored in `{data_dir}/{file_id}/crc32c.bin` (4 bytes per chunk)
- Crash recovery: truncate to last complete chunk boundary on restart

## Data Flow

### Distribution Creation
```
Admin ──POST /admin/distributions──→ Coordinator
                                         │
                                         ▼
                         Create distribution + tasks for all workers
```

### Chunk Distribution (steady state)
```
Time T:
  Worker 1: downloading chunk 5 from GCS
  Worker 2: downloading chunk 4 from Worker 1
  Worker 3: downloading chunk 3 from Worker 2
  ...

Time T+1:
  Worker 1: downloading chunk 6 from GCS
  Worker 2: downloading chunk 5 from Worker 1
  Worker 3: downloading chunk 4 from Worker 2
  ...
```

## Advantages

1. **Simplicity**: Easy to understand and debug
2. **Predictable**: Static topology, no complex scheduling
3. **Efficient bandwidth**: Each worker only uploads to one peer
4. **Natural pipelining**: Chunks flow through chain in parallel
5. **On-the-fly chunking**: No pre-processing required, works with any file
6. **Zero-copy P2P**: `sendfile()` avoids copying data through userspace

## Disadvantages

1. **Chain bottleneck**: Slowest worker limits entire chain
2. **Single point of failure**: If worker K fails, workers K+1..N stall
3. **Underutilized bandwidth**: Workers only upload to 1 peer (could serve more)
4. **No chunk-level parallelism**: Each worker downloads chunks sequentially
5. **Long tail latency**: Last worker must wait for data to traverse entire chain

## Configuration

**Environment variables:**
- `COORDINATOR_URL` - Coordinator HTTP endpoint
- `WORKER_ADDR` - This worker's address (for registration)
- `TCP_PORT` - Port for TCP P2P service (default: 50051)
- `DATA_DIR` - Local storage directory
- `GCS_PARALLEL_DOWNLOADS` - Number of parallel GCS downloads (default: 4)
- `GCS_BATCH_CHUNKS` - Chunks per GCS request (default: 1, 64MB per chunk)
- `STORAGE_EMULATOR_HOST` - GCS emulator URL (local dev)
- `GCS_SERVICE_ACCOUNT_PATH` - GCS credentials (production)

**Test-only variables:**
- `TEST_ONLY_LIMIT_GCS_BANDWIDTH` - Limit GCS bandwidth (e.g., "10m" for 10 Mbit/s)
- `TEST_ONLY_LIMIT_P2P_BANDWIDTH` - Limit P2P bandwidth (e.g., "5m" for 5 Mbit/s)

## GCP Deployment

**Instance types:**
- Coordinator: `e2-highcpu-2`, 20GB boot disk
- Workers: `n2-standard-8` (32GB RAM), 375GB local NVMe SSD
  - Local SSD mounted at `/mnt/disks/local-ssd`
  - Data stored at `/mnt/disks/local-ssd/data`
  - Note: Local SSD data is ephemeral (lost on VM stop/delete)

**Deployment:**
```bash
cd pipeline
cross build --release --target x86_64-unknown-linux-gnu
gsutil cp target/x86_64-unknown-linux-gnu/release/coordinator gs://bucket/binaries/
gsutil cp target/x86_64-unknown-linux-gnu/release/worker gs://bucket/binaries/

NUM_WORKERS=10 ./scripts/gcp-deploy.sh deploy
```

## Failure Handling

- **Worker failure**: Coordinator marks unhealthy after 5s, remaining workers re-ordered
- **Transfer failure**: Worker retries on next check-in cycle (2s)
- **Checksum mismatch**: Chunk discarded, re-downloaded on retry
- **Coordinator failure**: Workers continue serving existing chunks, retry check-ins
- **Crash recovery**: On restart, workers resume from last complete chunk
