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

### Worker (HTTP client + gRPC server)

Runs on each machine:
- Checks in with coordinator (every 2s)
- Downloads chunks sequentially from assigned upstream (GCS or peer)
- Serves completed chunks to downstream peers via gRPC streaming
- Reports progress in check-in payload

**Download flow:**
```
1. Check in with coordinator, receive tasks with upstream
2. For each task:
   a. Initialize output file (or resume from last chunk)
   b. Download chunks from upstream (GCS HTTP or peer gRPC)
   c. Verify chunk CRC32C checksums
   d. Append to output file
   e. Report progress in next check-in
3. When complete, finalize file and verify full-file CRC32C
```

**gRPC service:**
```protobuf
service P2PTransfer {
  rpc StreamChunks(ChunkRequest) returns (stream ChunkData);
}

message ChunkRequest {
  string file_id = 1;
  int32 start_from_chunk_id = 2;
}

message ChunkData {
  int32 chunk_id = 1;
  bytes data = 2;
  fixed32 crc32c = 3;
}
```

### Storage

Local filesystem storage:
- Output files stored as `{data_dir}/{file_id}/output.bin`
- Partial downloads use `.partial` suffix until finalized
- CRC32C checksums on each chunk transfer
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
- `WORKER_ID` - Optional worker ID (defaults to address)
- `GRPC_PORT` - Port for gRPC peer service (default: 50051)
- `DATA_DIR` - Local storage directory
- `STORAGE_EMULATOR_HOST` - GCS emulator URL (local dev)
- `GCS_SERVICE_ACCOUNT_PATH` - GCS credentials (production)

**Test-only variables:**
- `TEST_ONLY_LIMIT_GCS_BANDWIDTH` - Limit GCS bandwidth (e.g., "10m" for 10 Mbit/s)
- `TEST_ONLY_LIMIT_P2P_BANDWIDTH` - Limit P2P bandwidth (e.g., "5m" for 5 Mbit/s)

## Failure Handling

- **Worker failure**: Coordinator marks unhealthy after 5s, remaining workers re-ordered
- **Transfer failure**: Worker retries on next check-in cycle (2s)
- **Checksum mismatch**: Chunk discarded, re-downloaded on retry
- **Coordinator failure**: Workers continue serving existing chunks, retry check-ins
- **Crash recovery**: On restart, workers resume from last complete chunk
