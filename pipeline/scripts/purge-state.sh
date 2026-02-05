#!/bin/bash
set -e

cd "$(dirname "$0")/.."

echo "Purging all state..."

# Stop running processes
echo "Stopping coordinator and workers..."
pkill -f "target/debug/coordinator" 2>/dev/null || true
pkill -f "target/debug/worker" 2>/dev/null || true
sleep 1

# Remove database
if [ -f "pipeline.db" ]; then
    rm -f pipeline.db
    echo "  Deleted pipeline.db"
fi

# Remove worker data directories
for dir in ./data/workers/worker*; do
    if [ -d "$dir" ]; then
        rm -rf "$dir"
        echo "  Deleted $dir"
    fi
done

echo ""
echo "State purged. Restart coordinator and workers to begin fresh."
