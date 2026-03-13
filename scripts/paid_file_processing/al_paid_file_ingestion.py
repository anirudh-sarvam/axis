#!/usr/bin/env python3
"""
AL Paid File SFTP Ingestion Script

Fetches AL payment files (AL RX POC payment file Sarvam DD-Mon.xlsx) from SFTP and processes them
using transform_AL_stop_file.py logic.
"""

import subprocess
import os
import sys
import re
import asyncio
import fcntl
import pandas as pd
import json
import urllib.request
import urllib
import traceback
from datetime import datetime
import pytz

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_THIS_DIR)
_ROOT_DIR = os.path.dirname(_SCRIPTS_DIR)
sys.path.insert(0, _THIS_DIR)
sys.path.insert(0, _SCRIPTS_DIR)
sys.path.insert(0, _ROOT_DIR)

from transform_AL_stop_file import (
    transform_al_stop_file,
    convert_to_al_workflow_format,
    upload_al_working_file,
    AL_WORKFLOW_IDS,
    AL_PAID_FILE_DIR,
    AL_WORKING_FILE_DIR,
)
from blob_utils import upload_to_blob

IST = pytz.timezone("Asia/Kolkata")
BASE_DIR = "/home/sarvam/axis"
LOG_FILE = BASE_DIR + "/logs/al_paid_file_ingestion.log"
AXIS_PROCESSED_LOG = BASE_DIR + "/logs/axis_processed_files.log"
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "C0ADMKT2WDT")
ALERT_TYPE = "AL PAID FILE INGESTION"

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)


def _now():
    """Return current datetime in IST without tzinfo."""
    return datetime.now(IST).replace(tzinfo=None)


def send_slack_alert(
    message,
    file_path=None,
    error_details=None,
    is_success=False,
    alert_type=ALERT_TYPE,
):
    """Post alert to Slack via chat.postMessage."""
    alert_text = f"*{alert_type}*\n{message}"
    if file_path:
        alert_text += f"\nFile: `{file_path}`"
    if error_details:
        alert_text += f"\nError: {error_details}"
    if is_success:
        alert_text += "\n✅ Success"

    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "channel": SLACK_CHANNEL_ID,
        "text": alert_text,
    }

    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 200:
                print(f"[Slack] Unexpected status: {resp.status}")
    except Exception as e:
        print(f"[Slack] Failed to send alert: {e}")


def read_file_dataframe(file_path):
    """Read xlsx/xls/csv into DataFrame."""
    if file_path.endswith((".xlsx", ".xls")):
        return pd.read_excel(file_path)
    return pd.read_csv(file_path, low_memory=False)


def build_al_file_pattern():
    """Build regex for AL paid files (AL RX POC payment file Sarvam)."""
    pattern = r"AL\s+RX\s+POC\s+payment\s+file.*?Sarvam.*\.(xlsx|xls|csv)"
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    return compiled_pattern, pattern


def get_al_downloaded_files():
    """Read set of remote paths from AL log file."""
    if not os.path.exists(LOG_FILE):
        return set()
    try:
        with open(LOG_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except Exception:
        return set()


def build_al_allocation_pattern():
    """Build regex for AL allocation files. Negative lookahead to exclude OOO files."""
    pattern = r"SARVAM(?!.*(?:OOO[_ ]+NPA|No[_ ]*Credit|POOO)).*?Allocation.*\.(xlsx|xls|csv)"
    # Matches: SARVAM-CYCLE-10-ALLOCATION.xlsx but excludes all OOO files
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    return compiled_pattern, pattern


def validate_al_file_format(file_path):
    """
    Validate AL paid file format.
    Similar to PL: checks ACCNO/ACCNO2 and STATUS.
    Returns (is_valid, error_msg, df).
    """
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}", None
    if os.path.getsize(file_path) == 0:
        return False, "File is empty", None

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in (".xlsx", ".xls", ".csv"):
        return False, f"Unsupported format: {ext}", None

    try:
        df = read_file_dataframe(file_path)
    except Exception as e:
        return False, f"Failed to read file: {e}", None

    if df is None or len(df) == 0:
        return False, "File has no data", None

    # Check ACCNO/ACCNO2 columns (case-insensitive)
    col_map = {c.strip().lower(): c for c in df.columns}
    accno_col = None
    for name in ["accno", "accno2"]:
        if name in col_map:
            accno_col = col_map[name]
            break
    if accno_col is None:
        return False, "Missing ACCNO or ACCNO2 column", df

    # Check STATUS column (case-insensitive)
    status_col = None
    for name in ["status"]:
        if name in col_map:
            status_col = col_map[name]
            break
    if status_col is None:
        return False, "Missing STATUS column", df

    # Basic validation
    df = df.copy()
    df[accno_col] = df[accno_col].astype(str).str.strip()
    df = df[df[accno_col].str.len() > 0]
    df = df[df[accno_col] != "nan"]

    if len(df) == 0:
        return False, "No valid ACCNO rows after validation", df

    return True, None, df


def log_to_axis_processed_files(remote_path):
    """Append remote_path to AXIS_PROCESSED_LOG with fcntl locking."""
    try:
        with open(AXIS_PROCESSED_LOG, "a") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.write(remote_path + "\n")
                f.flush()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception as e:
        print(f"[log] Failed to append to {AXIS_PROCESSED_LOG}: {e}")


async def process_al_file(file_path):
    """
    Validate, transform using transform_al_stop_file, convert to workflow format,
    upload to AL workflows, upload original to blob.
    Returns dict with success, resolved, unresolved, total_count.
    """
    is_valid, error_msg, df = validate_al_file_format(file_path)
    if not is_valid:
        send_slack_alert(
            f"Validation failed: {error_msg}",
            file_path=file_path,
            error_details=error_msg,
        )
        return {"success": False, "error": error_msg, "resolved": 0, "unresolved": 0, "total_count": 0}

    try:
        df_transformed = transform_al_stop_file(df)
        if df_transformed is None or len(df_transformed) == 0:
            send_slack_alert("No data after transformation", file_path=file_path)
            return {"success": False, "error": "No data after transformation", "resolved": 0, "unresolved": 0, "total_count": 0}

        df_workflow = convert_to_al_workflow_format(df_transformed)
        if df_workflow is None or len(df_workflow) == 0:
            send_slack_alert("No data after conversion", file_path=file_path)
            return {"success": False, "error": "No data after conversion", "resolved": 0, "unresolved": 0, "total_count": 0}

        resolved = (df_workflow["resolved_paid"] == "true").sum()
        unresolved = (df_workflow["resolved_paid"] == "false").sum()
        total_count = len(df_workflow)

        # Save transformed file and upload to workflows
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        os.makedirs(AL_WORKING_FILE_DIR, exist_ok=True)
        working_path = os.path.join(AL_WORKING_FILE_DIR, f"{base_name}_transformed.csv")
        df_workflow.to_csv(working_path, index=False)

        for workflow_id in AL_WORKFLOW_IDS:
            await upload_al_working_file(working_path, workflow_id)

        # Upload original to blob
        filename = os.path.basename(file_path)
        blob_path = upload_to_blob(file_path, "paid_file", "al", filename=filename)
        if not blob_path:
            send_slack_alert("Blob upload failed", file_path=file_path)

        send_slack_alert(
            f"Processed {total_count} rows (resolved: {resolved}, unresolved: {unresolved})",
            file_path=file_path,
            is_success=True,
        )
        return {"success": True, "resolved": resolved, "unresolved": unresolved, "total_count": total_count}

    except Exception as e:
        tb = traceback.format_exc()
        send_slack_alert(
            f"Processing failed: {e}",
            file_path=file_path,
            error_details=tb,
        )
        return {"success": False, "error": str(e), "resolved": 0, "unresolved": 0, "total_count": 0}


async def main(files_to_process=None):
    """
    Process list of (local_path, remote_name, full_remote_path) tuples.
    For each: validate exists, process, log to axis_processed_files on success.
    More detailed error reporting with processed_files and failed_files lists.
    """
    if files_to_process is None:
        files_to_process = []

    processed_files = []
    failed_files = []

    for item in files_to_process:
        if len(item) < 3:
            continue
        local_path, remote_name, full_remote_path = item[0], item[1], item[2]
        if not os.path.exists(local_path):
            send_slack_alert(f"Local file not found: {local_path}", file_path=full_remote_path)
            failed_files.append((full_remote_path, "Local file not found"))
            continue

        result = await process_al_file(local_path)
        if result.get("success"):
            log_to_axis_processed_files(full_remote_path)
            processed_files.append(full_remote_path)
        else:
            failed_files.append((full_remote_path, result.get("error", "Unknown error")))

    if failed_files:
        send_slack_alert(
            f"Completed with {len(failed_files)} failure(s). Processed: {len(processed_files)}, Failed: {len(failed_files)}",
            error_details="\n".join(f"{p}: {e}" for p, e in failed_files[:5]),
        )


if __name__ == "__main__":
    asyncio.run(main())
