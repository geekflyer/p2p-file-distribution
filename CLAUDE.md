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
gsutil cp target/x86_64-unknown-linux-gnu/release/server gs://pipeline-test-christian/binaries/
```

Then create/update the VMs. The binaries in GCS must be Linux x86_64, not macOS.
