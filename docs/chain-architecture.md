# Chain/Pipeline Architecture

## Overview

A linear cascading distribution system where servers form a chain, each downloading from its predecessor. The first server downloads from GCS (origin), and each subsequent server downloads from the one ahead of it.

```
GCS ──→ Server 1 ──→ Server 2 ──→ Server 3 ──→ ... ──→ Server N
         │              │              │                    │
         ▼              ▼              ▼                    ▼
      [shards]      [shards]      [shards]            [shards]
```

## Key Characteristics

- **Topology**: Linear chain ordered by server address
- **Upstream assignment**: Static (server N always pulls from server N-1)
- **Parallelism**: Pipeline parallelism - while server 1 downloads shard K, server 2 downloads shard K-1, etc.
- **Bandwidth utilization**: Each server uses ~1x download + ~1x upload bandwidth
- **Coordinator role**: Minimal - just assigns upstream peers based on ordering

## Components

### Coordinator (HTTP REST)

Stateless service that:
- Manages deployment jobs (create, cancel, purge)
- Tracks server health via heartbeats (healthy if heartbeat within 5s)
- Assigns upstream peers using simple ordering algorithm
- Serves Admin UI for monitoring

**Key endpoints:**
- `GET /server/tasks?serverAddress={addr}` - Get pending download tasks
- `GET /server/upstream?serverAddress={addr}&jobId={id}` - Get upstream assignment
- `POST /server/heartbeat` - Report health + progress, receive purge commands

**Upstream assignment algorithm:**
```
servers = sort(healthy_servers, by=address)
index = servers.index(requesting_server)
if index == 0:
    return GCS
else:
    return servers[index - 1]
```

### Server/Worker (HTTP client + gRPC server)

Runs on each machine:
- Polls coordinator for tasks (every 2s)
- Downloads shards sequentially from assigned upstream (GCS or peer)
- Serves completed shards to downstream peers via gRPC streaming
- Reports progress via heartbeat (every 1s)

**Download flow:**
```
1. Poll coordinator for tasks
2. Get upstream assignment for job
3. Fetch manifest from GCS
4. For each shard:
   a. Download from upstream (GCS HTTP or peer gRPC)
   b. Verify SHA256 checksum
   c. Save to local storage
   d. Report progress in next heartbeat
```

**gRPC service:**
```protobuf
service FileTransfer {
  rpc StreamShards(ShardRequest) returns (stream ShardData);
}
```

### Storage

Local filesystem storage:
- Shards stored as `{data_dir}/{job_id}/shard_{id}.bin`
- Partial downloads use `.partial` suffix until verified
- CRC32C checksums on gRPC transfer chunks

## Data Flow

### Job Creation
```
Admin ──POST /admin/jobs──→ Coordinator
                                │
                                ▼
                    Create job + tasks for all servers
```

### Shard Distribution (steady state)
```
Time T:
  Server 1: downloading shard 5 from GCS
  Server 2: downloading shard 4 from Server 1
  Server 3: downloading shard 3 from Server 2
  ...

Time T+1:
  Server 1: downloading shard 6 from GCS
  Server 2: downloading shard 5 from Server 1
  Server 3: downloading shard 4 from Server 2
  ...
```

## Advantages

1. **Simplicity**: Easy to understand and debug
2. **Predictable**: Static topology, no complex scheduling
3. **Efficient bandwidth**: Each server only uploads to one peer
4. **Natural pipelining**: Shards flow through chain in parallel

## Disadvantages

1. **Chain bottleneck**: Slowest server limits entire chain
2. **Single point of failure**: If server K fails, servers K+1..N stall
3. **Underutilized bandwidth**: Servers only upload to 1 peer (could serve more)
4. **No shard-level parallelism**: Each server downloads shards sequentially
5. **Long tail latency**: Last server must wait for data to traverse entire chain

## Configuration

**Environment variables:**
- `COORDINATOR_URL` - Coordinator HTTP endpoint
- `SERVER_ADDR` - This server's address (for registration)
- `GRPC_PORT` - Port for gRPC peer service (default: 50051)
- `DATA_DIR` - Local storage directory
- `STORAGE_EMULATOR_HOST` - GCS emulator URL (local dev)
- `GCS_SERVICE_ACCOUNT_PATH` - GCS credentials (production)

## Failure Handling

- **Server failure**: Coordinator marks unhealthy after 5s, reassigns chain
- **Transfer failure**: Server retries on next poll cycle (2s)
- **Checksum mismatch**: Shard discarded, re-downloaded on retry
- **Coordinator failure**: Servers continue serving existing shards, retry polls
