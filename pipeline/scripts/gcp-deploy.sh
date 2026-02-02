#!/bin/bash
set -euo pipefail

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
BUCKET_NAME="${GCP_BUCKET:-pipeline-test-$(whoami)}"
COORDINATOR_VM="pipeline-coordinator"
SERVER_PREFIX="pipeline-server"
NUM_SERVERS="${NUM_SERVERS:-100}"
TEST_DATA_SIZE_GB="${TEST_DATA_SIZE_GB:-100}"
SHARD_SIZE_MB="${SHARD_SIZE_MB:-1024}"

# VM specs
COORDINATOR_MACHINE_TYPE="e2-medium"
SERVER_MACHINE_TYPE="n2-highcpu-2"  # 2 vCPU, 2GB RAM, ~10 Gbps network

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $1"; }
error() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $1"; exit 1; }

# Check prerequisites
check_prereqs() {
    log "Checking prerequisites..."

    if [[ -z "$PROJECT_ID" ]]; then
        PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
        if [[ -z "$PROJECT_ID" ]]; then
            error "No GCP project set. Run: export GCP_PROJECT_ID=your-project"
        fi
    fi

    log "Using project: $PROJECT_ID"
    log "Using region: $REGION, zone: $ZONE"
    log "Bucket: $BUCKET_NAME"
    log "Servers: $NUM_SERVERS"

    gcloud auth list --filter=status:ACTIVE --format="value(account)" > /dev/null || \
        error "Not authenticated. Run: gcloud auth login"
}

# Create GCS bucket
create_bucket() {
    log "Creating GCS bucket gs://$BUCKET_NAME..."

    if gsutil ls -b "gs://$BUCKET_NAME" &>/dev/null; then
        warn "Bucket already exists, skipping creation"
    else
        gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET_NAME"
    fi
}

# Build binaries for Linux
build_binaries() {
    log "Building binaries for Linux..."

    cd "$(dirname "$0")/.."

    # Use 'cross' for cross-compilation (requires Docker)
    if command -v cross &>/dev/null && docker info &>/dev/null; then
        log "Cross-compiling for Linux using 'cross'..."
        if cross build --release --target x86_64-unknown-linux-gnu; then
            BINARY_DIR="target/x86_64-unknown-linux-gnu/release"
            log "Uploading binaries to GCS..."
            gsutil cp "$BINARY_DIR/coordinator" "gs://$BUCKET_NAME/binaries/"
            gsutil cp "$BINARY_DIR/server" "gs://$BUCKET_NAME/binaries/"
            gsutil cp -r coordinator/static "gs://$BUCKET_NAME/binaries/"
            return 0
        else
            warn "Cross-compilation failed."
        fi
    else
        warn "'cross' or Docker not available."
    fi

    warn "Will build on coordinator VM instead."
    return 1
}

# Upload source code for remote build
upload_source() {
    log "Uploading source code for remote build..."

    cd "$(dirname "$0")/.."

    # Create tarball excluding target dir and data
    tar czf /tmp/pipeline-source.tar.gz \
        --exclude='target' \
        --exclude='data' \
        --exclude='*.db' \
        --exclude='.env.local' \
        .

    gsutil cp /tmp/pipeline-source.tar.gz "gs://$BUCKET_NAME/source/"
    rm /tmp/pipeline-source.tar.gz
}

# Create coordinator VM
create_coordinator() {
    log "Creating coordinator VM..."

    # Check if VM exists
    if gcloud compute instances describe "$COORDINATOR_VM" --zone="$ZONE" &>/dev/null; then
        warn "Coordinator VM already exists"
        return
    fi

    # Startup script for coordinator
    cat > /tmp/coordinator-startup.sh << 'STARTUP_EOF'
#!/bin/bash
set -ex

# Get bucket name from metadata
BUCKET=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/bucket" -H "Metadata-Flavor: Google")

# Try to download pre-built binary first
mkdir -p /opt/pipeline
cd /opt/pipeline

if gsutil cp "gs://$BUCKET/binaries/coordinator" ./coordinator 2>/dev/null; then
    chmod +x ./coordinator
    # Download static files
    mkdir -p /opt/pipeline/static
    gsutil cp -r "gs://$BUCKET/binaries/static/*" /opt/pipeline/static/ 2>/dev/null || true
    echo "Using pre-built coordinator binary"
else
    echo "Building from source..."

    # Install build dependencies
    apt-get update
    apt-get install -y curl build-essential pkg-config libssl-dev protobuf-compiler

    # Install Rust
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    export PATH="/root/.cargo/bin:$PATH"

    gsutil cp "gs://$BUCKET/source/pipeline-source.tar.gz" ./
    tar xzf pipeline-source.tar.gz

    # Build BOTH binaries so servers don't need to compile
    cargo build --release -p coordinator -p server
    cp target/release/coordinator ./coordinator
    cp target/release/server ./server

    # Upload binaries to GCS
    gsutil cp ./coordinator "gs://$BUCKET/binaries/coordinator"
    gsutil cp ./server "gs://$BUCKET/binaries/server"
    echo "Uploaded binaries to GCS"
fi

# Create data directory
mkdir -p /opt/pipeline/data

# Create systemd service
cat > /etc/systemd/system/coordinator.service << 'EOF'
[Unit]
Description=Pipeline Coordinator
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/pipeline
Environment=DATA_DIR=/opt/pipeline/data
Environment=RUST_LOG=info
ExecStart=/opt/pipeline/coordinator
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable coordinator
systemctl start coordinator

echo "Coordinator started!"
STARTUP_EOF

    gcloud compute instances create "$COORDINATOR_VM" \
        --zone="$ZONE" \
        --machine-type="$COORDINATOR_MACHINE_TYPE" \
        --image-family=debian-12 \
        --image-project=debian-cloud \
        --boot-disk-size=20GB \
        --scopes=storage-rw \
        --metadata="bucket=$BUCKET_NAME" \
        --metadata-from-file=startup-script=/tmp/coordinator-startup.sh \
        --tags=pipeline-coordinator

    rm /tmp/coordinator-startup.sh

    log "Coordinator VM created. Waiting for startup..."
}

# Generate test data on a temporary VM
generate_test_data() {
    log "Generating ${TEST_DATA_SIZE_GB}GB test data..."

    local BUILDER_VM="pipeline-builder"
    local TOTAL_BYTES=$((TEST_DATA_SIZE_GB * 1024 * 1024 * 1024))
    local SHARD_BYTES=$((SHARD_SIZE_MB * 1024 * 1024))
    local NUM_SHARDS=$(( (TOTAL_BYTES + SHARD_BYTES - 1) / SHARD_BYTES ))

    log "Will create $NUM_SHARDS shards of ${SHARD_SIZE_MB}MB each"

    # Check if test data already exists
    if gsutil ls "gs://$BUCKET_NAME/testdata/model.manifest" &>/dev/null; then
        warn "Test data already exists in gs://$BUCKET_NAME/testdata/"
        read -p "Regenerate? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            return
        fi
        gsutil -m rm -r "gs://$BUCKET_NAME/testdata/" || true
    fi

    # Create builder VM with SSD for fast generation
    log "Creating builder VM..."

    cat > /tmp/builder-startup.sh << STARTUP_EOF
#!/bin/bash
set -ex

BUCKET="$BUCKET_NAME"
TOTAL_SIZE=$TOTAL_BYTES
SHARD_SIZE=$SHARD_BYTES
NUM_SHARDS=$NUM_SHARDS

apt-get update
apt-get install -y python3 jq

cd /tmp

# Generate shards and manifest
echo "Generating \$NUM_SHARDS shards..."

manifest_shards="["
for i in \$(seq 0 \$((NUM_SHARDS - 1))); do
    shard_file="shard_\$(printf '%04d' \$i).bin"

    # Calculate this shard's size (last shard may be smaller)
    if [ \$i -eq \$((NUM_SHARDS - 1)) ]; then
        this_shard_size=\$((TOTAL_SIZE - i * SHARD_SIZE))
    else
        this_shard_size=\$SHARD_SIZE
    fi

    # Generate random data
    dd if=/dev/urandom of="\$shard_file" bs=1M count=\$((this_shard_size / 1024 / 1024)) iflag=fullblock 2>/dev/null

    # If there's a remainder, append it
    remainder=\$((this_shard_size % (1024 * 1024)))
    if [ \$remainder -gt 0 ]; then
        dd if=/dev/urandom bs=\$remainder count=1 >> "\$shard_file" 2>/dev/null
    fi

    # Calculate SHA256
    sha256=\$(sha256sum "\$shard_file" | cut -d' ' -f1)
    actual_size=\$(stat -c%s "\$shard_file")

    # Upload to GCS
    gsutil cp "\$shard_file" "gs://\$BUCKET/testdata/\$shard_file"
    rm "\$shard_file"

    # Add to manifest
    if [ \$i -gt 0 ]; then
        manifest_shards+=","
    fi
    manifest_shards+="{\"shard_id\": \$i, \"path\": \"\$shard_file\", \"size\": \$actual_size, \"sha256\": \"\$sha256\"}"

    echo "Shard \$i/\$((NUM_SHARDS-1)) uploaded (\$actual_size bytes)"
done
manifest_shards+="]"

# Create manifest
cat > manifest.json << MANIFEST_EOF
{
  "total_size": \$TOTAL_SIZE,
  "shard_size": \$SHARD_SIZE,
  "num_shards": \$NUM_SHARDS,
  "shards": \$manifest_shards
}
MANIFEST_EOF

gsutil cp manifest.json "gs://\$BUCKET/testdata/model.manifest"

echo "DONE" > /tmp/builder-done
gsutil cp /tmp/builder-done "gs://\$BUCKET/builder-done"

echo "Test data generation complete!"
STARTUP_EOF

    gcloud compute instances create "$BUILDER_VM" \
        --zone="$ZONE" \
        --machine-type="n2-standard-8" \
        --image-family=debian-12 \
        --image-project=debian-cloud \
        --boot-disk-size=200GB \
        --boot-disk-type=pd-ssd \
        --scopes=storage-rw \
        --metadata-from-file=startup-script=/tmp/builder-startup.sh

    rm /tmp/builder-startup.sh

    log "Builder VM created. Waiting for data generation..."
    log "This will take a while for ${TEST_DATA_SIZE_GB}GB..."

    # Wait for completion
    while ! gsutil ls "gs://$BUCKET_NAME/builder-done" &>/dev/null; do
        echo -n "."
        sleep 30
    done
    echo ""

    log "Test data generation complete!"

    # Clean up builder VM
    log "Deleting builder VM..."
    gcloud compute instances delete "$BUILDER_VM" --zone="$ZONE" --quiet
    gsutil rm "gs://$BUCKET_NAME/builder-done"
}

# Create server VMs using instance template and managed instance group
create_servers() {
    log "Creating $NUM_SERVERS server VMs..."

    # Get coordinator internal IP
    COORDINATOR_IP=$(gcloud compute instances describe "$COORDINATOR_VM" \
        --zone="$ZONE" \
        --format='get(networkInterfaces[0].networkIP)')

    log "Coordinator IP: $COORDINATOR_IP"

    # Create instance template
    local TEMPLATE_NAME="pipeline-server-template"

    # Delete existing template if it exists
    gcloud compute instance-templates delete "$TEMPLATE_NAME" --quiet 2>/dev/null || true

    cat > /tmp/server-startup.sh << STARTUP_EOF
#!/bin/bash
set -ex

# Get metadata
BUCKET="$BUCKET_NAME"
COORDINATOR_IP="$COORDINATOR_IP"

# Get own IP for server address
MY_IP=\$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip" -H "Metadata-Flavor: Google")

mkdir -p /opt/pipeline
cd /opt/pipeline

# Try to download pre-built binary first
if gsutil cp "gs://\$BUCKET/binaries/server" ./server 2>/dev/null; then
    chmod +x ./server
    echo "Using pre-built binary"
else
    echo "Building from source..."

    # Install build dependencies
    apt-get update
    apt-get install -y curl build-essential pkg-config libssl-dev protobuf-compiler

    # Install Rust
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    export PATH="/root/.cargo/bin:\$PATH"

    gsutil cp "gs://\$BUCKET/source/pipeline-source.tar.gz" ./
    tar xzf pipeline-source.tar.gz
    cargo build --release -p server
    cp target/release/server ./server

    # Upload binary to GCS so other VMs can use it
    gsutil cp ./server "gs://\$BUCKET/binaries/server"
fi

# Create data directory
mkdir -p /opt/pipeline/data

# Create systemd service
cat > /etc/systemd/system/server.service << EOF
[Unit]
Description=Pipeline Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/pipeline
Environment=DATA_DIR=/opt/pipeline/data
Environment=COORDINATOR_URL=http://\$COORDINATOR_IP:8080
Environment=SERVER_ADDR=\$MY_IP:50051
Environment=GRPC_PORT=50051
Environment=RUST_LOG=info
ExecStart=/opt/pipeline/server
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable server
systemctl start server

echo "Server started!"
STARTUP_EOF

    gcloud compute instance-templates create "$TEMPLATE_NAME" \
        --machine-type="$SERVER_MACHINE_TYPE" \
        --image-family=debian-12 \
        --image-project=debian-cloud \
        --boot-disk-size=120GB \
        --boot-disk-type=pd-ssd \
        --scopes=storage-rw \
        --tags=pipeline-server \
        --no-address \
        --metadata-from-file=startup-script=/tmp/server-startup.sh

    rm /tmp/server-startup.sh

    # Create managed instance group
    local MIG_NAME="pipeline-servers"

    # Delete existing MIG if it exists
    gcloud compute instance-groups managed delete "$MIG_NAME" --zone="$ZONE" --quiet 2>/dev/null || true

    gcloud compute instance-groups managed create "$MIG_NAME" \
        --zone="$ZONE" \
        --template="$TEMPLATE_NAME" \
        --size="$NUM_SERVERS"

    log "Server instance group created with $NUM_SERVERS instances"
}

# Create firewall rules
create_firewall_rules() {
    log "Creating firewall rules..."

    # Allow internal traffic for gRPC between servers
    if ! gcloud compute firewall-rules describe pipeline-internal &>/dev/null; then
        gcloud compute firewall-rules create pipeline-internal \
            --direction=INGRESS \
            --priority=1000 \
            --network=default \
            --action=ALLOW \
            --rules=tcp:50051,tcp:8080 \
            --source-tags=pipeline-server,pipeline-coordinator \
            --target-tags=pipeline-server,pipeline-coordinator
    fi

    # Allow SSH via IAP (for VMs without external IPs)
    if ! gcloud compute firewall-rules describe pipeline-iap-ssh &>/dev/null; then
        gcloud compute firewall-rules create pipeline-iap-ssh \
            --direction=INGRESS \
            --priority=1000 \
            --network=default \
            --action=ALLOW \
            --rules=tcp:22 \
            --source-ranges=35.235.240.0/20 \
            --target-tags=pipeline-server,pipeline-coordinator
    fi

    log "Firewall rules configured"
}

# Setup networking for VMs without external IPs
setup_private_networking() {
    log "Setting up private networking..."

    # Enable Private Google Access on default subnet (allows GCS access without external IP)
    gcloud compute networks subnets update default \
        --region="$REGION" \
        --enable-private-ip-google-access 2>/dev/null || true

    # Create Cloud NAT for outbound internet (package downloads during setup)
    if ! gcloud compute routers describe pipeline-router --region="$REGION" &>/dev/null; then
        log "Creating Cloud Router..."
        gcloud compute routers create pipeline-router \
            --region="$REGION" \
            --network=default
    fi

    if ! gcloud compute routers nats describe pipeline-nat --router=pipeline-router --region="$REGION" &>/dev/null; then
        log "Creating Cloud NAT..."
        gcloud compute routers nats create pipeline-nat \
            --router=pipeline-router \
            --region="$REGION" \
            --nat-all-subnet-ip-ranges \
            --auto-allocate-nat-external-ips
    fi

    log "Private networking configured"
}

# Print access instructions
print_access_info() {
    log "Deployment complete!"
    echo ""
    echo "=========================================="
    echo "ACCESS INSTRUCTIONS"
    echo "=========================================="
    echo ""
    echo "1. Wait for VMs to finish startup (check logs):"
    echo "   gcloud compute instances get-serial-port-output $COORDINATOR_VM --zone=$ZONE"
    echo ""
    echo "2. Access Admin UI via SSH tunnel:"
    echo "   gcloud compute ssh $COORDINATOR_VM --zone=$ZONE -- -L 8080:localhost:8080"
    echo "   Then open: http://localhost:8080"
    echo ""
    echo "3. Check coordinator logs:"
    echo "   gcloud compute ssh $COORDINATOR_VM --zone=$ZONE -- journalctl -u coordinator -f"
    echo ""
    echo "4. Check server count in UI or:"
    echo "   curl -s http://localhost:8080/admin/servers | jq length"
    echo ""
    echo "5. Create a deployment job in the UI with:"
    echo "   GCS File Path: gs://$BUCKET_NAME/testdata"
    echo "   Manifest Path: gs://$BUCKET_NAME/testdata/model.manifest"
    echo ""
    echo "=========================================="
}

# Cleanup function
cleanup() {
    log "Cleaning up resources..."

    read -p "Delete all resources? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        return
    fi

    # Delete instance group
    gcloud compute instance-groups managed delete pipeline-servers --zone="$ZONE" --quiet 2>/dev/null || true

    # Delete instance template
    gcloud compute instance-templates delete pipeline-server-template --quiet 2>/dev/null || true

    # Delete coordinator
    gcloud compute instances delete "$COORDINATOR_VM" --zone="$ZONE" --quiet 2>/dev/null || true

    # Delete firewall rules
    gcloud compute firewall-rules delete pipeline-internal --quiet 2>/dev/null || true
    gcloud compute firewall-rules delete pipeline-iap-ssh --quiet 2>/dev/null || true

    # Delete Cloud NAT and router
    gcloud compute routers nats delete pipeline-nat --router=pipeline-router --region="$REGION" --quiet 2>/dev/null || true
    gcloud compute routers delete pipeline-router --region="$REGION" --quiet 2>/dev/null || true

    # Optionally delete bucket
    read -p "Delete GCS bucket gs://$BUCKET_NAME? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        gsutil -m rm -r "gs://$BUCKET_NAME" || true
    fi

    log "Cleanup complete"
}

# Main
main() {
    local cmd="${1:-deploy}"

    case "$cmd" in
        deploy)
            check_prereqs
            create_bucket

            # Try to build locally, fall back to remote build
            if ! build_binaries; then
                upload_source
            fi

            create_firewall_rules
            setup_private_networking
            create_coordinator
            generate_test_data
            create_servers
            print_access_info
            ;;
        cleanup)
            check_prereqs
            cleanup
            ;;
        status)
            check_prereqs
            echo "Coordinator:"
            gcloud compute instances describe "$COORDINATOR_VM" --zone="$ZONE" --format="table(name,status,networkInterfaces[0].networkIP)" 2>/dev/null || echo "  Not found"
            echo ""
            echo "Servers:"
            gcloud compute instance-groups managed list-instances pipeline-servers --zone="$ZONE" 2>/dev/null || echo "  Not found"
            ;;
        tunnel)
            log "Opening SSH tunnel to coordinator..."
            log "Admin UI will be available at http://localhost:8080"
            gcloud compute ssh "$COORDINATOR_VM" --zone="$ZONE" -- -L 8080:localhost:8080
            ;;
        *)
            echo "Usage: $0 {deploy|cleanup|status|tunnel}"
            exit 1
            ;;
    esac
}

main "$@"
