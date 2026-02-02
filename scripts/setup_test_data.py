#!/usr/bin/env python3
"""Generate test data and upload to fake GCS server."""

import hashlib
import json
import os
import requests
from pathlib import Path

FAKE_GCS_URL = "http://localhost:4443"
BUCKET_NAME = "test-bucket"
MODEL_PATH = "models/test-model"
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
TOTAL_SIZE = 1 * 1024 * 1024 * 1024  # 1GB

def create_bucket():
    """Create the test bucket."""
    url = f"{FAKE_GCS_URL}/storage/v1/b"
    data = {"name": BUCKET_NAME}
    resp = requests.post(url, json=data)
    if resp.status_code in (200, 409):  # 409 = already exists
        print(f"Bucket '{BUCKET_NAME}' ready")
    else:
        print(f"Failed to create bucket: {resp.status_code} {resp.text}")
        raise Exception("Failed to create bucket")

def upload_chunk(chunk_id: int, data: bytes, checksum: str):
    """Upload a chunk to fake GCS."""
    object_name = f"{MODEL_PATH}/chunk_{chunk_id:08d}.bin"
    url = f"{FAKE_GCS_URL}/upload/storage/v1/b/{BUCKET_NAME}/o?uploadType=media&name={object_name}"
    resp = requests.post(url, data=data, headers={"Content-Type": "application/octet-stream"})
    if resp.status_code != 200:
        print(f"Failed to upload chunk {chunk_id}: {resp.status_code} {resp.text}")
        raise Exception(f"Failed to upload chunk {chunk_id}")
    return checksum

def generate_and_upload_chunks():
    """Generate random chunks and upload them."""
    num_chunks = TOTAL_SIZE // CHUNK_SIZE
    manifest = {
        "total_size": TOTAL_SIZE,
        "chunk_size": CHUNK_SIZE,
        "num_chunks": num_chunks,
        "chunks": []
    }

    print(f"Generating {num_chunks} chunks of {CHUNK_SIZE // (1024*1024)}MB each...")

    for i in range(num_chunks):
        # Generate random data using os.urandom (fast)
        data = os.urandom(CHUNK_SIZE)

        # Compute checksum
        checksum = hashlib.sha256(data).hexdigest()

        # Upload chunk
        upload_chunk(i, data, checksum)

        manifest["chunks"].append({
            "chunk_id": i,
            "size": len(data),
            "checksum": checksum
        })

        if (i + 1) % 10 == 0:
            print(f"  Uploaded {i + 1}/{num_chunks} chunks...")

    # Upload manifest
    manifest_json = json.dumps(manifest, indent=2)
    manifest_name = f"{MODEL_PATH}.manifest"
    url = f"{FAKE_GCS_URL}/upload/storage/v1/b/{BUCKET_NAME}/o?uploadType=media&name={manifest_name}"
    resp = requests.post(url, data=manifest_json, headers={"Content-Type": "application/json"})
    if resp.status_code != 200:
        print(f"Failed to upload manifest: {resp.status_code} {resp.text}")
        raise Exception("Failed to upload manifest")

    print(f"Uploaded manifest with {num_chunks} chunks")
    print(f"\nGCS path: gs://{BUCKET_NAME}/{MODEL_PATH}")
    print(f"Total chunks: {num_chunks}")

    return num_chunks

if __name__ == "__main__":
    print("Setting up test data on fake GCS server...")
    create_bucket()
    num_chunks = generate_and_upload_chunks()
    print(f"\nDone! Use these values:")
    print(f"  GCS Path: gs://{BUCKET_NAME}/{MODEL_PATH}")
    print(f"  Total Chunks: {num_chunks}")
