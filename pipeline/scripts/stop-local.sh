#!/bin/bash

echo "Stopping local development environment..."

pkill -f "target/release/coordinator" 2>/dev/null || true
pkill -f "target/release/server" 2>/dev/null || true

echo "Stopped coordinator and servers."
echo "(fake-gcs-server is still running - use 'docker stop fake-gcs' to stop it)"
