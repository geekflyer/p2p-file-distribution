# File Distribution System

Two implementations of large file distribution across server fleets:

## Directory Structure

```
pipeline/           # Chain topology implementation
  coordinator/      # HTTP + job management
  server/           # Downloads from GCS or upstream peer
  common/           # Types + proto
  scripts/          # start-local.sh, gcp-deploy.sh

mesh/               # P2P mesh topology implementation
  coordinator/      # gRPC scheduler + HTTP admin
  server/           # Pull-based work loop
  common/           # Types + proto
  scripts/          # (mesh-specific scripts)

scripts/            # Shared scripts (test data generation)
data/               # Local storage for both
```

## Pipeline (Chain Topology)

Chain of servers where Server N downloads from Server N-1 (or GCS for first server).

```bash
cd pipeline
cargo build --release
./scripts/start-local.sh
```

- Coordinator: http://localhost:8080
- Simple chain assignment based on server order

## Mesh (P2P Topology)

Dynamic shard distribution using rarest-first scheduling.

```bash
cd mesh
cargo build --release
# Start coordinator (port 8081 for HTTP, 50050 for gRPC)
./target/release/coordinator
# Start servers
COORDINATOR_URL=http://localhost:50050 ./target/release/server
```

- Coordinator HTTP: http://localhost:8081
- Coordinator gRPC: localhost:50050
- Rarest-shard-first scheduling, max 1 GCS download globally

## Shared

- **scripts/generate-test-data.sh** - Generate test shards
- **scripts/upload-test-data.sh** - Upload to GCS/emulator
- **data/gcs/** - Fake GCS storage

## Environment Variables

- `STORAGE_EMULATOR_HOST` - GCS emulator URL (e.g., http://localhost:4443)
- `TEST_ONLY_LIMIT_GCS_BANDWIDTH` - e.g., "10m" for 10 Mbit/s
- `TEST_ONLY_LIMIT_P2P_BANDWIDTH` - e.g., "5m" for 5 Mbit/s
