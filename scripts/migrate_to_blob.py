#!/usr/bin/env python3
"""
Migration: reorganize blobs from flat structure to month-based structure.

Old path: <org>/<workspace>/<file_type>/<product>/<filename>
New path: <org>/<workspace>/<file_type>/<product>/<month>/<filename>

Usage:
    python3 migrate_to_blob.py          # Dry run
    python3 migrate_to_blob.py --run    # Actually migrate
"""

import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from blob_utils import (
    _get_blob_service_client,
    AZURE_CONTAINER_NAME,
    AZURE_BASE_PATH,
    upload_to_blob,
    get_current_month,
)

FILE_TYPES = ["allocation_file", "paid_file", "working_file"]
PRODUCTS = ["pl", "cc", "al"]
BASE_DIR = "/home/sarvam/axis"
DATA_DIR = os.path.join(BASE_DIR, "data")


def migrate_blobs(dry_run=True, month=None):
    """
    Move blobs from flat paths to month-based paths.
    Works from blob storage directly (no local files needed).
    """
    if month is None:
        month = get_current_month()

    blob_service_client = _get_blob_service_client()
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

    moved = 0
    skipped = 0
    errors = 0

    for file_type in FILE_TYPES:
        for product in PRODUCTS:
            old_prefix = f"{AZURE_BASE_PATH}/{file_type}/{product}/"

            blobs = list(container_client.list_blobs(name_starts_with=old_prefix))

            for blob in blobs:
                old_path = blob.name
                parts = old_path.split("/")
                # Old: org/workspace/file_type/product/filename
                # New: org/workspace/file_type/product/month/filename

                # Skip if already in a month folder (len > 5 means it has the month segment)
                # org/workspace/file_type/product = 4 segments, then month + filename = 6+
                filename = parts[-1]
                if not filename:
                    continue

                # Check if this blob is already under a month subfolder
                # parts = [org, workspace, file_type, product, ...rest]
                # If rest has 2+ parts, it's already in a month folder
                rest = parts[4:]  # everything after product
                if len(rest) >= 2:
                    skipped += 1
                    continue

                # This is a flat blob - needs migration
                new_path = f"{AZURE_BASE_PATH}/{file_type}/{product}/{month}/{filename}"

                if dry_run:
                    print(f"  [DRY RUN] {old_path}")
                    print(f"         -> {new_path}")
                    moved += 1
                else:
                    try:
                        # Copy blob to new location
                        source_blob = blob_service_client.get_blob_client(
                            container=AZURE_CONTAINER_NAME, blob=old_path
                        )
                        dest_blob = blob_service_client.get_blob_client(
                            container=AZURE_CONTAINER_NAME, blob=new_path
                        )

                        dest_blob.start_copy_from_url(source_blob.url)

                        # Wait for copy to complete
                        props = dest_blob.get_blob_properties()
                        while props.copy.status == "pending":
                            import time
                            time.sleep(0.5)
                            props = dest_blob.get_blob_properties()

                        if props.copy.status == "success":
                            # Delete old blob
                            source_blob.delete_blob()
                            print(f"  Moved: {old_path} -> {new_path}")
                            moved += 1
                        else:
                            print(f"  COPY FAILED: {old_path} (status: {props.copy.status})")
                            errors += 1
                    except Exception as e:
                        print(f"  ERROR: {old_path}: {e}")
                        errors += 1

    print(f"\n{'=' * 60}")
    print(f"Month: {month}")
    print(f"Moved: {moved}, Skipped (already migrated): {skipped}, Errors: {errors}")
    if dry_run:
        print("This was a dry run. Use --run to actually migrate.")
    else:
        print("Migration complete.")
    print(f"{'=' * 60}")


def upload_local_files(dry_run=True, month=None):
    """Upload any local data/ files to blob (for initial migration)."""
    if month is None:
        month = get_current_month()

    total_files = 0
    total_size = 0

    for file_type in FILE_TYPES:
        for product in PRODUCTS:
            local_dir = os.path.join(DATA_DIR, file_type, product)
            if not os.path.exists(local_dir):
                continue

            files = [f for f in os.listdir(local_dir) if os.path.isfile(os.path.join(local_dir, f))]
            if not files:
                continue

            print(f"\n--- {file_type}/{product}/{month} ({len(files)} files) ---")

            for filename in sorted(files):
                file_path = os.path.join(local_dir, filename)
                file_size = os.path.getsize(file_path)
                total_size += file_size
                total_files += 1

                if dry_run:
                    print(f"  [DRY RUN] Would upload: {filename} ({file_size:,} bytes)")
                else:
                    result = upload_to_blob(file_path, file_type, product, month=month)
                    if result:
                        print(f"  {filename} ({file_size:,} bytes)")
                    else:
                        print(f"  FAILED: {filename}")

    if total_files > 0:
        print(f"\nTotal: {total_files} files, {total_size / (1024 * 1024):.2f} MB")
    else:
        print("\nNo local files to upload.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate Axis blob storage to month-based structure")
    parser.add_argument("--run", action="store_true", help="Actually execute (default is dry run)")
    parser.add_argument("--month", type=str, default=None, help="Target month folder (default: current IST month)")
    args = parser.parse_args()

    month = args.month or get_current_month()

    print("=" * 60)
    print(f"Axis Blob Storage Migration -> month folders")
    print(f"Target month: {month}")
    print("=" * 60)

    # First, upload any remaining local files
    upload_local_files(dry_run=not args.run, month=month)

    # Then, move existing flat blobs into month folders
    print(f"\n{'=' * 60}")
    print("Reorganizing existing blobs into month folders...")
    print(f"{'=' * 60}")
    migrate_blobs(dry_run=not args.run, month=month)
