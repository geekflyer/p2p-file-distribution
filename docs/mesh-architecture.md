This doc describes the high level architecture for the p2p-mesh variant of the file distribution system.

1. These are the main components:
1. A set of servers, which run an agent/client that connects to the coordinator, asking the coordinator "what work should I perform" - like which shard to pull from which peer?. Servers can pull shard files from each other GRPC streaming pull.
1. A Coordinator that keeps track of which server owns which model shards (in memory) and assigns servers jobs to download shards from each other or from GCS.
1. A SQLite db (in production could be postgres) to track deployment jobs.
1. GCS as source for file shards.

## Overview

A centrally coordinated mesh distribution system where a scheduler dynamically assigns peer-to-peer transfers based on shard availability and server load. Unlike the chain topology, any server can download any shard from any peer that has it.

```
                    ┌─────────────┐
                    │ Coordinator │
                    │ (Scheduler) │
                    └──────┬──────┘
                           │ assigns transfers
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
       ┌───────┐       ┌───────┐       ┌───────┐
       │Server1│◄─────►│Server2│◄─────►│Server3│  ...
       └───────┘       └───────┘       └───────┘
           ▲               ▲               ▲
           └───────────────┴───────────────┘
                    mesh transfers
```

## Key Characteristics

- **Topology**: Mesh
- **Upstream assignment**: Dynamic - coordinator runs scheduling loop to figure out which pairs make sense.
- **Parallelism**: Pipeline parallelism - while server 1 downloads shard K, server 2 downloads shard K-1, etc.
- **Bandwidth utilization**: Each server uses ~1x download + ~1x upload bandwidth
- **Coordinator role**: Critical - assigns work. Servers are relatively dumb.
- **Job model**: One job at a time. Jobs are queued and processed sequentially.

## Job Queuing

The scheduler processes **one job at a time**. This simplifies state management:

- Jobs are queued (e.g., in SQLite with status `pending` → `in_progress` → `completed`)
- Scheduler picks the next `pending` job when current job completes
- All servers work on the same active job
- When all servers have all shards for current job → job marked `completed` → next job starts
- `ServerStatus.job_id` always refers to the current active job (or empty on first call)

**Job transitions:** When a new job starts, servers receive a `TransferTask` with the new `job_id`. Server detects the job changed, resets its `shards_owned` bitmap to empty, and starts fresh.

## Components

### Coordinator (HTTP REST for Admin API, GRPC for worker communication)

### 1. Scheduler Protocol

How does the coordinator communicate transfer assignments?
Pull-based (servers long poll for work). Scheduler returns a TransferTask once there is a match.

```
Server calls getWork(ServerStatus) with server_address + shards_owned bitmap
Coordinator responds with TransferTask (peer or GCS source)
```

### 2. Shard Tracking

How does the coordinator know which server has which shards?

**Option B: Servers report inventory** ✅ CHOSEN

- Servers include shard list in `getWork()` call via `shards_owned` bitmap
- More resilient to coordinator restarts
- Coordinator maintains `Map<shard_id, uint32>` counting global shard availability

### 3. Assignment Heuristics

How does the scheduler decide which peer should serve a shard?

**Possible factors:**

- **FIFO fairness**: Prioritize servers waiting longest for `getWork()` (servers wait in in-memory queue for work assignment)
- **Rarest shard first**: Prefer shards with lowest replication count (spreads rare shards faster)
- **1:1 concurrency**: Max 1 upload and 1 download per server

### 4. Transfer Initiation

**Option A: Receiver pulls** ✅ CHOSEN

```
Coordinator tells Server B: "Pull shard from Server A" (via TransferTask with upstream_server_addr)
Server B calls StreamShard() on Server A's ShardTransfer service
```

### 5. Concurrency Model

How many concurrent transfers per server?

- Max concurrent downloads per server: **1** (server can only have 1 transfer task at a time)
- Max concurrent uploads per server: **1** (scheduler only assigns server as upstream if not already seeding)
- Max concurrent GCS downloads globally: **1** (to avoid hammering origin)
- Should these be configurable? Could be, but start simple with 1:1:1

### 6. Failure Handling

What happens when a transfer fails?

- **Transfer timeout**: Pending tasks have a timeout. On expiry, task is removed from `pendingTransferTasks` and server can request new work.
- **Server crash**: Missed heartbeats mark server as dead. Clean up its pending tasks (both as downloader and as upstream seeder).
- **Reassignment**: Failed transfers are implicitly retried - server calls `getWork()` again and scheduler assigns new task (possibly different peer, possibly same shard).

## Proposed Design

### Scheduler Protocol

**Pull-based with long-polling**

Workers call `getWork()` on the Scheduler service. The coordinator holds the connection until work is available (long-poll style).

```protobuf
service Scheduler {
  rpc getWork(ServerStatus) returns (TransferTask);
  rpc reportTransferTaskCompletion(TransferTaskComplete) returns ();
  rpc heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}

message ServerStatus {
  string server_address = 1;      // This server's gRPC address (e.g., "10.0.0.5:50051")
  string job_id = 2;              // Current active job (from last TransferTask, or empty on first call)
  bytes shards_owned = 3;         // BitMap of owned shards for this job
}

message TransferTask {
  enum SourceType { PEER = 0; GCS = 1; }
  SourceType source_type = 1;
  string job_id = 2;
  int32 shard_id = 3;
  string upstream_server_addr = 4;  // Set if source_type == PEER
  string gcs_path = 5;              // Set if source_type == GCS
}

message TransferTaskComplete {
  string server_address = 1;
  string job_id = 2;
  int32 shard_id = 3;
}

message HeartbeatRequest {
  string server_address = 1;
}

message HeartbeatResponse {}
```

### ShardTransfer (Worker-to-Worker transfers)

Each worker runs a ShardTransfer gRPC service for peer transfers. Unlike the chain topology which streams multiple shards continuously (`StreamShards`), the mesh version transfers **one shard at a time** per stream call:

```protobuf
service ShardTransfer {
  // Stream all pieces for a single shard (mesh: one shard per call)
  rpc StreamShard(ShardRequest) returns (stream PieceData);
}

message ShardRequest {
  string job_id = 1;
  int32 shard_id = 2;       // Single shard to transfer
  int32 from_piece = 3;     // Resume from this piece (0 = start)
}

message PieceData {
  int32 shard_id = 1;
  int32 piece_index = 2;
  int32 total_pieces = 3;   // Total pieces in this shard
  bytes data = 4;
  fixed32 crc32c = 5;       // Transfer integrity check
}
```

**Key difference from chain topology:** The chain uses `StreamShards` (plural) which continuously streams all shards starting from a position. The mesh uses `StreamShard` (singular) which streams only the pieces of one specific shard, then completes. This allows the scheduler to make per-shard assignment decisions.

### Assignment Algorithm

Scheduler maintains:

1. **waitingForWorkQueue**: FIFO queue of servers blocked on `getWork()`
2. **shardAvailability**: `Map<shard_id, uint32>` counting how many servers have each shard globally
3. **pendingTransferTasks**: `Map<server_address, TransferTask>` tracking in-flight transfers (used to determine who is busy uploading/downloading)
4. **gcsDownloadInProgress**: `bool` - only 1 server can download from GCS at a time

**Scheduling loop:**

1. Pop the longest-waiting server (Server B) from `waitingForWorkQueue`
2. Find the **rarest shard** that Server B needs (lowest count in `shardAvailability`, not in their `shards_owned` bitmap)
3. If a peer has the shard:
   - Find a Seed Server A that has that shard and is not currently uploading (check `pendingTransferTasks` - A should not appear as `upstream_server_addr` in any pending task)
   - Assign Server B a `TransferTask` with `source_type=PEER` and `upstream_server_addr=A`
4. If no peer has the shard AND `gcsDownloadInProgress == false`:
   - Assign `source_type=GCS` to fetch from origin
   - Set `gcsDownloadInProgress = true`
5. If no peer has the shard AND `gcsDownloadInProgress == true`:
   - Server B waits (stays in queue or re-enqueues)
6. Record the assigned task in `pendingTransferTasks[Server B]`

**Constraints:**

- Max 1 download per server (server can only have 1 active TransferTask)
- Max 1 upload per server (scheduler only assigns a server as upstream if not already seeding)
- Max 1 GCS download globally (to avoid hammering origin)

**Timeouts:**

- Pending transfer tasks have a timeout (e.g., 5 minutes)
- If timeout expires without completion, task is removed from `pendingTransferTasks` and can be reassigned
- Servers send periodic heartbeats; missed heartbeats mark server as dead and clean up its pending tasks

### Server Behavior

1. **Startup**: Connect to Scheduler, report empty `shards_owned` bitmap
2. **Work loop**:
   - Call `getWork(ServerStatus)` with current shard bitmap and server address
   - Block until Scheduler returns a `TransferTask`
   - Execute transfer:
     - If `source_type == GCS`: fetch shard directly from GCS
     - If `source_type == PEER`: call `StreamShard()` on upstream server's ShardTransfer service
   - Reassemble pieces into shard, verify checksums
   - Update local `shards_owned` bitmap
   - Call `reportTransferTaskCompletion()`
   - Repeat
3. **Serve shards**: Run ShardTransfer gRPC service to serve owned shards to peers
4. **Heartbeat**: Send periodic heartbeats to Scheduler (e.g., every 30s)

## Comparison with Chain Architecture

| Aspect                 | Chain               | Mesh                       |
| ---------------------- | ------------------- | -------------------------- |
| Topology               | Linear              | Full mesh                  |
| Scheduling             | Static              | Dynamic                    |
| Coordinator complexity | Simple              | Complex                    |
| Bandwidth utilization  | 1 down + 1 up       | 1 down + 1 up (per server) |
| Failure resilience     | Chain breaks        | Reschedule to other peer   |
| Time to completion     | O(shards + servers) | O(shards) theoretically    |

## Implementation Plan

### Phase 1: Core Protocol

- Define protobuf messages for Scheduler and ShardTransfer services
- Implement `getWork()` / `reportTransferTaskCompletion()` RPCs on coordinator
- Implement `StreamShard()` RPC on workers

### Phase 2: Scheduler State

- Track server registrations and their `shards_owned` bitmaps
- Implement `waitingForWorkQueue` (FIFO)
- Implement `shardAvailability` map
- Implement `pendingTransferTasks` tracking
- Implement `gcsDownloadInProgress` flag

### Phase 3: Assignment Logic

- Scheduling loop that matches waiting servers to available shards/peers
- Rarest-shard-first selection
- GCS fallback when no peer has the shard (max 1 GCS download at a time)
- Timeout handling for pending tasks

### Phase 4: Server Agent

- Work loop: getWork → transfer → reportCompletion
- ShardTransfer service to stream shard pieces to peers
- Bitmap tracking for owned shards
- Heartbeat loop

### Phase 5: Integration & Testing

- End-to-end test with multiple workers
- Failure injection (worker crashes, transfer timeouts)
- Metrics and observability
