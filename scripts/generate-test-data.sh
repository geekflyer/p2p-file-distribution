#!/bin/bash
set -e

# Configuration - sizes in KB
TOTAL_SIZE_KB=${1:-512000}    # 500MB default
SHARD_SIZE_KB=${2:-1024}      # 1MB default
BUCKET="test-bucket"
MODEL_PATH="models/test-model"

cd "$(dirname "$0")/.."

OUTPUT_DIR="data/gcs/${BUCKET}/${MODEL_PATH}"
MANIFEST_FILE="data/gcs/${BUCKET}/${MODEL_PATH}.manifest"

# Calculate values
SHARD_SIZE=$((SHARD_SIZE_KB * 1024))
TOTAL_SIZE=$((TOTAL_SIZE_KB * 1024))
# Use ceiling division to handle partial last shard
NUM_SHARDS=$(( (TOTAL_SIZE + SHARD_SIZE - 1) / SHARD_SIZE ))
LAST_SHARD_SIZE=$((TOTAL_SIZE - (NUM_SHARDS - 1) * SHARD_SIZE))

echo "Generating test data..."
echo "  Total size: $((TOTAL_SIZE_KB / 1024))MB (${TOTAL_SIZE} bytes)"
echo "  Shard size: ${SHARD_SIZE_KB}KB (${SHARD_SIZE} bytes)"
echo "  Num shards: ${NUM_SHARDS}"
echo "  Last shard: ${LAST_SHARD_SIZE} bytes"
echo ""

# Clean existing data
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

# Start manifest
cat > "${MANIFEST_FILE}" << EOF
{
  "total_size": ${TOTAL_SIZE},
  "shard_size": ${SHARD_SIZE},
  "num_shards": ${NUM_SHARDS},
  "shards": [
EOF

# Generate shards
for shard_id in $(seq 0 $((NUM_SHARDS - 1))); do
    SHARD_FILE=$(printf "${OUTPUT_DIR}/shard_%04d.bin" $shard_id)

    # Determine shard size (last shard may be smaller)
    if [ $shard_id -eq $((NUM_SHARDS - 1)) ]; then
        THIS_SHARD_SIZE=${LAST_SHARD_SIZE}
        THIS_SHARD_SIZE_KB=$(( (LAST_SHARD_SIZE + 1023) / 1024 ))
        # Generate exact size using head
        dd if=/dev/urandom bs=1 count=${LAST_SHARD_SIZE} of="${SHARD_FILE}" 2>/dev/null
    else
        THIS_SHARD_SIZE=${SHARD_SIZE}
        THIS_SHARD_SIZE_KB=${SHARD_SIZE_KB}
        dd if=/dev/urandom of="${SHARD_FILE}" bs=1024 count=${SHARD_SIZE_KB} 2>/dev/null
    fi

    # Calculate shard SHA256
    SHARD_SHA256=$(shasum -a 256 "${SHARD_FILE}" | cut -d' ' -f1)

    # Add shard to manifest (path is relative to the model directory)
    SHARD_FILENAME=$(printf "shard_%04d.bin" $shard_id)
    if [ $shard_id -gt 0 ]; then
        echo "," >> "${MANIFEST_FILE}"
    fi
    printf "    {\"shard_id\": %d, \"path\": \"%s\", \"size\": %d, \"sha256\": \"%s\"}" \
        $shard_id "$SHARD_FILENAME" $THIS_SHARD_SIZE "$SHARD_SHA256" >> "${MANIFEST_FILE}"

    # Progress
    PROGRESS=$((100 * (shard_id + 1) / NUM_SHARDS))
    echo -ne "\r  Progress: $((shard_id + 1))/${NUM_SHARDS} shards (${PROGRESS}%)"
done

# Close manifest
echo "" >> "${MANIFEST_FILE}"
echo "  ]" >> "${MANIFEST_FILE}"
echo "}" >> "${MANIFEST_FILE}"

echo ""
echo ""
echo "Done! Generated ${NUM_SHARDS} shards ($((TOTAL_SIZE_KB / 1024))MB total)"
echo "  Data: ${OUTPUT_DIR}"
echo "  Manifest: ${MANIFEST_FILE}"
