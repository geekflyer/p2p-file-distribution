# Pipeline - Cascading File Distribution System

Rust workspace that distributes large files across server fleets using P2P cascade topology.

## Build & Run

```bash
cargo build --release                    # Build all
./scripts/start-local.sh                 # Local dev (requires fake-gcs-server on :4443)
./scripts/gcp-deploy.sh deploy           # Deploy to GCP
```

## Architecture

- **coordinator/** - HTTP service managing jobs, server assignments, health tracking (SQLite)
- **server/** - Worker that downloads shards from GCS or upstream peers, serves to downstream via gRPC
- **common/** - Shared types and protobuf definitions

## Key Concepts

- Chain topology: Server N downloads from Server N-1 (or GCS for first server)
- Shards: Large files split into chunks, tracked via manifest with SHA256 checksums
- Job lifecycle: created → in_progress → completed/failed/cancelled → purged

## Environment Variables

**Coordinator:** `DATA_DIR`, `HTTP_PORT` (8080)
**Server:** `COORDINATOR_URL`, `SERVER_ADDR`, `GRPC_PORT` (50051), `DATA_DIR`
**GCS:** `STORAGE_EMULATOR_HOST` (local dev), `GCS_SERVICE_ACCOUNT_PATH` (prod)

## Testing

```bash
./scripts/generate-test-data.sh          # Generate test shards in data/gcs/
curl -X POST localhost:8080/admin/jobs -H "Content-Type: application/json" \
  -d '{"gcsFilePath":"gs://bucket/path","gcsManifestPath":"gs://bucket/path.manifest"}'
```
