#!/usr/bin/env python3
"""
Azure Blob Storage utilities for Axis Bank file sync.

Blob path structure:
  <container>/<org_id>/<workspace_id>/<file_type>/<product>/<month>/<filename>

Examples:
  campaign-inbound/axisbank.com/axisbank-com-defau-3b9581/paid_file/pl/feb/PL_RX_Payment_file_Sarvam_23Feb.xlsx
  campaign-inbound/axisbank.com/axisbank-com-defau-3b9581/working_file/cc/feb/Post_Due_Date_Cards_Status_23-Feb-2026_transformed.csv
  campaign-inbound/axisbank.com/axisbank-com-defau-3b9581/allocation_file/cc/mar/Post_due_date_Cards_Cycle_1.xlsx
"""

import os
import shutil
import tempfile
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, AzureError
from pytz import timezone as pytz_timezone

AZURE_CONNECTION_STRING = os.environ["AZURE_CONNECTION_STRING"]
AZURE_CONTAINER_NAME = "campaign-inbound"

ORG_ID = "axisbank.com"
WORKSPACE_ID = "axisbank-com-defau-3b9581"
AZURE_BASE_PATH = f"{ORG_ID}/{WORKSPACE_ID}"


IST = pytz_timezone("Asia/Kolkata")


def get_current_month():
    """Return lowercase abbreviated month name in IST, e.g. 'feb', 'mar'."""
    return datetime.now(IST).strftime("%b").lower()


def get_previous_month():
    """Return lowercase abbreviated month name for the previous month in IST."""
    now = datetime.now(IST)
    # Go back to first of current month, then subtract 1 day to land in previous month
    first_of_month = now.replace(day=1)
    prev = first_of_month - __import__("datetime").timedelta(days=1)
    return prev.strftime("%b").lower()


def _get_blob_service_client():
    return BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)


def build_blob_path(file_type, product, filename, month=None):
    """
    Build blob path from components.

    Args:
        file_type: "paid_file", "working_file", or "allocation_file"
        product:   "pl", "cc", or "al"
        filename:  the file name (spaces replaced with underscores)
        month:     lowercase month abbreviation (defaults to current IST month)
    """
    if month is None:
        month = get_current_month()
    safe_name = filename.replace(" ", "_")
    return f"{AZURE_BASE_PATH}/{file_type}/{product}/{month}/{safe_name}"


def upload_to_blob(local_path, file_type, product, filename=None, month=None):
    """
    Upload a local file to Azure Blob Storage.

    Args:
        local_path:  path to the local file
        file_type:   "paid_file", "working_file", or "allocation_file"
        product:     "pl", "cc", or "al"
        filename:    override filename (defaults to basename of local_path)
        month:       lowercase month abbreviation (defaults to current IST month)

    Returns:
        blob_path on success, None on failure
    """
    if not os.path.exists(local_path):
        print(f"[blob] File not found: {local_path}")
        return None

    if filename is None:
        filename = os.path.basename(local_path)

    blob_path = build_blob_path(file_type, product, filename, month=month)

    try:
        blob_service_client = _get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_CONTAINER_NAME, blob=blob_path
        )

        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        file_size = os.path.getsize(local_path)
        print(f"[blob] Uploaded: {blob_path} ({file_size:,} bytes)")
        return blob_path

    except AzureError as e:
        print(f"[blob] Upload failed for {blob_path}: {e}")
        return None
    except Exception as e:
        print(f"[blob] Unexpected error uploading {blob_path}: {e}")
        return None


def download_from_blob(file_type, product, filename, local_path=None, month=None):
    """
    Download a file from Azure Blob Storage.

    Args:
        file_type:  "paid_file", "working_file", or "allocation_file"
        product:    "pl", "cc", or "al"
        filename:   the blob filename
        local_path: where to save locally (defaults to data/<file_type>/<product>/<filename>)
        month:      lowercase month abbreviation (defaults to current IST month)

    Returns:
        local_path on success, None on failure
    """
    blob_path = build_blob_path(file_type, product, filename, month=month)

    if local_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        local_path = os.path.join(base_dir, "data", file_type, product, filename)

    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        blob_service_client = _get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_CONTAINER_NAME, blob=blob_path
        )

        if not blob_client.exists():
            print(f"[blob] Not found: {blob_path}")
            return None

        blob_data = blob_client.download_blob()
        with open(local_path, "wb") as f:
            blob_data.readinto(f)

        file_size = os.path.getsize(local_path)
        print(f"[blob] Downloaded: {blob_path} -> {local_path} ({file_size:,} bytes)")
        return local_path

    except ResourceNotFoundError:
        print(f"[blob] Not found: {blob_path}")
        return None
    except AzureError as e:
        print(f"[blob] Download failed for {blob_path}: {e}")
        return None
    except Exception as e:
        print(f"[blob] Unexpected error downloading {blob_path}: {e}")
        return None


def list_blobs(file_type=None, product=None, month=None):
    """
    List blobs under a given prefix.

    Args:
        file_type: optional filter ("paid_file", "working_file", "allocation_file")
        product:   optional filter ("pl", "cc", "al")
        month:     optional month filter (defaults to current IST month when file_type+product given)

    Returns:
        list of blob names
    """
    prefix = AZURE_BASE_PATH
    if file_type:
        prefix = f"{prefix}/{file_type}"
        if product:
            prefix = f"{prefix}/{product}"
            if month is None:
                month = get_current_month()
            prefix = f"{prefix}/{month}"

    try:
        blob_service_client = _get_blob_service_client()
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

        blobs = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            blobs.append(blob.name)

        return blobs

    except AzureError as e:
        print(f"[blob] List failed for prefix {prefix}: {e}")
        return []
    except Exception as e:
        print(f"[blob] Unexpected error listing {prefix}: {e}")
        return []


def delete_blob(file_type, product, filename, month=None):
    """
    Delete a blob from Azure Blob Storage.

    Returns:
        True on success, False on failure
    """
    blob_path = build_blob_path(file_type, product, filename, month=month)

    try:
        blob_service_client = _get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_CONTAINER_NAME, blob=blob_path
        )

        if not blob_client.exists():
            print(f"[blob] Already absent: {blob_path}")
            return True

        blob_client.delete_blob()
        print(f"[blob] Deleted: {blob_path}")
        return True

    except ResourceNotFoundError:
        return True
    except AzureError as e:
        print(f"[blob] Delete failed for {blob_path}: {e}")
        return False


def sync_from_blob(file_type, product, local_dir=None, month=None):
    """
    Download ALL blobs under a file_type/product/month prefix to a local directory.
    Used to pull allocation files or working files from blob before processing.

    Args:
        file_type: "paid_file", "working_file", or "allocation_file"
        product:   "pl", "cc", or "al"
        local_dir: target directory (defaults to a temp directory)
        month:     lowercase month abbreviation (defaults to current IST month)

    Returns:
        (local_dir, file_list) tuple. Caller is responsible for cleanup if using temp dir.
    """
    if month is None:
        month = get_current_month()

    if local_dir is None:
        local_dir = tempfile.mkdtemp(prefix=f"axis_{file_type}_{product}_")

    os.makedirs(local_dir, exist_ok=True)

    prefix = f"{AZURE_BASE_PATH}/{file_type}/{product}/{month}/"

    try:
        blob_service_client = _get_blob_service_client()
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

        downloaded = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            filename = blob.name.split("/")[-1]
            if not filename:
                continue

            local_path = os.path.join(local_dir, filename)
            blob_client = blob_service_client.get_blob_client(
                container=AZURE_CONTAINER_NAME, blob=blob.name
            )
            blob_data = blob_client.download_blob()
            with open(local_path, "wb") as f:
                blob_data.readinto(f)

            downloaded.append(local_path)

        print(f"[blob] Synced {len(downloaded)} file(s) from {file_type}/{product}/{month} to {local_dir}")
        return local_dir, downloaded

    except AzureError as e:
        print(f"[blob] Sync failed for {file_type}/{product}/{month}: {e}")
        return local_dir, []
    except Exception as e:
        print(f"[blob] Unexpected error syncing {file_type}/{product}/{month}: {e}")
        return local_dir, []


def cleanup_local_dir(directory, keep_dir=False):
    """
    Remove all files in a directory. Optionally remove the directory itself.

    Args:
        directory: path to clean
        keep_dir:  if True, remove files but keep the empty directory
    """
    if not os.path.exists(directory):
        return

    if keep_dir:
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
        print(f"[cleanup] Cleared files in {directory}")
    else:
        shutil.rmtree(directory, ignore_errors=True)
        print(f"[cleanup] Removed {directory}")
