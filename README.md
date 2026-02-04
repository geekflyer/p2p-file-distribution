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

**Scheduling Algorithm:**
1. Each server requests work from the coordinator
2. Coordinator collects shards the server needs, sorted by rarity (fewest copies first)
3. **P2P transfers**: Iterate through shards by rarity, try to find an available peer (not currently uploading) that has each shard
4. **GCS downloads**: Only used for shards with 0 copies in the cluster (to bring new shards in)
5. If no work can be assigned (GCS busy, no P2P available), server waits in queue

This ensures:
- Rare shards are prioritized for distribution
- Idle servers can transfer less-rare shards while waiting for GCS
- GCS bandwidth is only used to bring new data into the cluster

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

### Pipeline
```bash
cd pipeline
NUM_SERVERS=3 ./scripts/gcp-deploy.sh deploy
# Access: gcloud compute ssh pipeline-coordinator --zone=us-central1-a -- -L 8080:localhost:8080
```

### Mesh
```bash
cd mesh
NUM_SERVERS=3 ./scripts/gcp-deploy.sh deploy
# Access: gcloud compute ssh mesh-coordinator --zone=us-central1-a -- -L 8081:localhost:8081
```
