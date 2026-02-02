#!/bin/bash
set -e

# Configuration
BUCKET="test-bucket"
MODEL_PATH="models/test-model"
GCS_URL="${GCS_URL:-http://localhost:4444}"

cd "$(dirname "$0")/.."

DATA_DIR="data/gcs/${BUCKET}/${MODEL_PATH}"
MANIFEST_FILE="data/gcs/${BUCKET}/${MODEL_PATH}.manifest"

if [ ! -d "$DATA_DIR" ]; then
    echo "Error: Test data not found at $DATA_DIR"
    echo "Run ./scripts/generate-test-data.sh first"
    exit 1
fi

# Create bucket (ignore error if exists)
echo "Creating bucket ${BUCKET}..."
curl -s -X POST "${GCS_URL}/storage/v1/b?project=test-project" \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"${BUCKET}\"}" > /dev/null 2>&1 || true

# Upload manifest
echo "Uploading manifest..."
curl -s -X POST "${GCS_URL}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=${MODEL_PATH}.manifest" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@${MANIFEST_FILE}" > /dev/null

# Upload all shard files
echo "Uploading shards..."
for file in "${DATA_DIR}"/shard_*.bin; do
    filename=$(basename "$file")
    object_name="${MODEL_PATH}/${filename}"
    echo -n "  ${filename}... "
    curl -s -X POST "${GCS_URL}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=${object_name}" \
        -H "Content-Type: application/octet-stream" \
        --data-binary "@${file}" > /dev/null
    echo "done"
done

echo ""
echo "Upload complete!"
echo "  Bucket: gs://${BUCKET}"
echo "  Path: ${MODEL_PATH}"
