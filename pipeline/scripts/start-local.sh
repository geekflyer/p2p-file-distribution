#!/bin/bash
set -e

cd "$(dirname "$0")/.."

# Load environment variables
if [ -f .env.local ]; then
    export $(grep -v '^#' .env.local | xargs)
fi

# Number of workers to start (default: 20)
NUM_WORKERS=${NUM_WORKERS:-20}

echo "Starting local development environment..."

# Check if fake-gcs is running
if ! docker ps --format '{{.Names}}' | grep -q '^fake-gcs$'; then
    echo "Starting fake-gcs-server..."
    if docker ps -a --format '{{.Names}}' | grep -q '^fake-gcs$'; then
        docker start fake-gcs
    else
        mkdir -p data/gcs
        docker run -d --name fake-gcs -p 4443:4443 -v "$(pwd)/data/gcs:/storage" \
            fsouza/fake-gcs-server -scheme http -filesystem-root /storage
    fi
    sleep 2
fi

# Stop any existing processes
pkill -f "target/release/coordinator" 2>/dev/null || true
pkill -f "target/release/worker" 2>/dev/null || true
sleep 1

# Start coordinator
echo "Starting coordinator..."
./target/release/coordinator > /tmp/coordinator.log 2>&1 &
sleep 1

# Start workers
echo "Starting $NUM_WORKERS workers..."
for i in $(seq 1 $NUM_WORKERS); do
    port=$((50050 + i))
    padded=$(printf "%02d" $i)

    WORKER_ADDR="localhost:${port}" \
    GRPC_PORT="${port}" \
    DATA_DIR="./data/workers/worker${padded}" \
    ./target/release/worker > /tmp/worker${i}.log 2>&1 &
done

sleep 2

# Check status
healthy=$(curl -s http://localhost:8080/admin/workers | jq '[.[] | select(.status=="healthy")] | length')
echo ""
echo "Started successfully!"
echo "  - Coordinator: http://localhost:8080"
echo "  - Workers: $healthy healthy"
echo "  - Fake GCS: http://localhost:4443"
echo ""
echo "Bandwidth limits:"
echo "  - GCS: $TEST_ONLY_LIMIT_GCS_BANDWIDTH"
echo "  - P2P: $TEST_ONLY_LIMIT_P2P_BANDWIDTH"
