# File Distribution System

Mini MVP for a system to distribute large files (e.g., ML model weights) across server fleets efficiently via P2P transfers.
Architecture is more detailed under [](/docs).

## Implementations

### Pipeline (Chain Topology)

Servers form a chain where each downloads from its predecessor. Simple, predictable. But random slow workers slow other peers progress equally.

```bash
cd pipeline
cargo build --release
./scripts/start-local.sh
# Admin UI: http://localhost:8080
```

### Mesh (P2P Topology)

Dynamic shard distribution using rarest-first scheduling. Better for dealing with random slow servers (mesh scheduler will implicitly route around them - assign less frequently peers to them.). Scheduler has high lock contention at large N workers (1000++).

```bash
cd mesh
cargo build --release
./target/release/coordinator &
COORDINATOR_URL=http://localhost:50050 ./target/release/server
# Admin UI: http://localhost:8081
```

## Testing

```bash
# Generate test data
./scripts/generate-test-data.sh

# Start fake GCS
docker run -d --name fake-gcs -p 4443:4443 fsouza/fake-gcs-server -scheme http

# Upload test data
./scripts/upload-test-data.sh
```

## GCP Deployment

```bash
cd pipeline
NUM_SERVERS=100 ./scripts/gcp-deploy.sh deploy
```
