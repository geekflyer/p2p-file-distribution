# File Distribution System

Large file distribution across server fleets using chain topology with P2P propagation.

## Directory Structure

```
pipeline/           # Chain topology implementation
  coordinator/      # HTTP + job management
  worker/           # Downloads from GCS or upstream peer via raw TCP
  common/           # Shared types
  scripts/          # start-local.sh, gcp-deploy.sh

scripts/            # Shared scripts (test data generation)
data/               # Local storage
```

## Pipeline Architecture

Chain of workers where Worker N downloads from Worker N-1 (or GCS for first worker).

- **Coordinator**: Assigns work, tracks progress, serves admin UI
- **Worker**: Downloads chunks from GCS or peers, serves chunks to downstream peers
- **P2P Transfer**: Raw TCP with Linux `sendfile()` for zero-copy transfer
- **Chunk verification**: CRC32C checksums stored per chunk

```bash
cd pipeline
cargo build --release
./scripts/start-local.sh
```

- Coordinator: http://localhost:8080
- Workers connect to coordinator and receive task assignments
- First worker downloads from GCS, subsequent workers download from upstream peer

## Worker Environment Variables

- `COORDINATOR_URL` - Coordinator HTTP URL (e.g., http://localhost:8080)
- `WORKER_ADDR` - Worker's address for P2P (e.g., 10.0.0.1:50051)
- `TCP_PORT` - Port for P2P TCP server (default: 50051)
- `DATA_DIR` - Directory for storing downloaded files
- `GCS_PARALLEL_DOWNLOADS` - Number of parallel GCS downloads (default: 4)
- `GCS_BATCH_CHUNKS` - Chunks per GCS request (default: 1, each chunk is 64MB)
- `STORAGE_EMULATOR_HOST` - GCS emulator URL (e.g., http://localhost:4443)
- `TEST_ONLY_LIMIT_GCS_BANDWIDTH` - e.g., "10m" for 10 Mbit/s
- `TEST_ONLY_LIMIT_P2P_BANDWIDTH` - e.g., "5m" for 5 Mbit/s

## Rust Conventions

- **Cross-compilation**: Always use `cross` for building Linux binaries from macOS
- **TLS**: Use `rustls` instead of OpenSSL (`native-tls` | `libssl-dev`) to avoid dynamic linking issues
  - For `reqwest`: `default-features = false, features = ["json", "rustls-tls"]`
- **Cross.toml**: Configure pre-build to install required packages (e.g., protobuf-compiler for gRPC)
- **SQLite timestamps**: Use ISO8601 TEXT format (e.g., `datetime('now')` in SQL, `chrono::DateTime` in Rust)

## Code Cleanup

- **Delete dead code immediately**: When refactoring or changing approaches, remove unused functions, struct fields, database columns, and API parameters right away. Don't leave orphaned code "for later".
- **Check for unused imports**: After removing code, clean up any imports that are no longer needed.
- **Database migrations**: When removing columns, add migration queries to drop them (SQLite requires table recreation for column removal, so at minimum stop writing to them).

## GCP Deployment

When deploying to GCP with code changes, always cross-compile for Linux first:

```bash
cd pipeline
cross build --release --target x86_64-unknown-linux-gnu
gsutil cp target/x86_64-unknown-linux-gnu/release/coordinator gs://pipeline-test-christian/binaries/
gsutil cp target/x86_64-unknown-linux-gnu/release/worker gs://pipeline-test-christian/binaries/
```

Then create/update the VMs. The binaries in GCS must be Linux x86_64, not macOS.

### Instance Configuration

- **Coordinator**: `e2-highcpu-2`, 20GB boot disk
- **Workers**: `n2-standard-8` (32GB RAM), 375GB local NVMe SSD for data
  - Local SSD mounted at `/mnt/disks/local-ssd`
  - Data stored at `/mnt/disks/local-ssd/data`
  - Note: Local SSD data is ephemeral (lost on VM stop/delete)

### Deployment Script

```bash
cd pipeline
NUM_WORKERS=10 ./scripts/gcp-deploy.sh deploy
./scripts/gcp-deploy.sh status
./scripts/gcp-deploy.sh tunnel  # SSH tunnel to coordinator UI
./scripts/gcp-deploy.sh cleanup
```
