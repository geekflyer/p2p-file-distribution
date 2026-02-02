#!/bin/bash
set -e

cd "$(dirname "$0")/.."

echo "Purging all state..."

# Stop running processes
echo "Stopping coordinator and servers..."
pkill -f "target/debug/coordinator" 2>/dev/null || true
pkill -f "target/debug/server" 2>/dev/null || true
sleep 1

# Remove database
if [ -f "pipeline.db" ]; then
    rm -f pipeline.db
    echo "  Deleted pipeline.db"
fi

# Remove server data directories
for dir in ./data/server*; do
    if [ -d "$dir" ]; then
        rm -rf "$dir"
        echo "  Deleted $dir"
    fi
done

echo ""
echo "State purged. Restart coordinator and servers to begin fresh."
