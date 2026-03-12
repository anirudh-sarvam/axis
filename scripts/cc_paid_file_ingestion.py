#!/usr/bin/env python3
"""
CC Paid File SFTP Ingestion Script

Fetches CC payment files (Post Due Date Cards Status) from SFTP and processes them
using transform_cc_stop_file.py logic.
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform_cc_stop_file import (
    convert_to_cc_workflow_format,
    upload_cc_working_file,
    transform_cc_stop_file,
    CC_WORKFLOW_IDS,
    CC_PAID_FILE_DIR,
    CC_WORKING_FILE_DIR,
    CC_ALLOCATION_DIR,
)
from blob_utils import upload_to_blob

IST = pytz.timezone("Asia/Kolkata")
BASE_DIR = "/home/sarvam/axis"
LOG_FILE = BASE_DIR + "/logs/cc_paid_file_ingestion.log"
AXIS_PROCESSED_LOG = BASE_DIR + "/logs/axis_processed_files.log"
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "C0ADMKT2WDT")
ALERT_TYPE = "CC PAID FILE INGESTION"

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


def build_cc_file_pattern():
    """Build regex for CC paid files (Post Due Date Cards Status)."""
    pattern = r"Post\s+D(?:ue|ye)\s+Date\s+Cards\s+Status.*\.(xlsx|xls|csv)"
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    return compiled_pattern, pattern


def build_cc_allocation_pattern():
    """Build regex for CC allocation files."""
    pattern = r"Post\s+due\s+date\s+Cards.*?Allocation.*\.(xlsx|xls|csv)"
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    return compiled_pattern, pattern


def get_cc_downloaded_files():
    """Read set of remote paths from CC log file."""
    if not os.path.exists(LOG_FILE):
        return set()
    try:
        with open(LOG_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except Exception:
        return set()


def validate_cc_file_format(file_path):
    """
    Validate CC paid file format.
    Check for user_identifier column (not ACCNO), check amount columns.
    Uses transform_cc_stop_file for validation.
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

    # CC uses user_identifier / ACCOUNT_NO - not ACCNO
    account_col = None
    for col in ["ACCOUNT_NO", "#ACCOUNT_NO", "Account No", "user_identifier"]:
        if col in df.columns:
            account_col = col
            break
    if account_col is None:
        return False, "Missing user_identifier/ACCOUNT_NO column", df

    # Check for amount columns
    amount_col = None
    for col in ["Amount", "AMOUNT", "amount", "Payment", "PAYMENT", "Payment Received Amount"]:
        if col in df.columns:
            amount_col = col
            break
    if amount_col is None:
        return False, "Missing amount/payment column", df

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


async def process_cc_file(file_path):
    """
    Validate, transform using transform_cc_stop_file, convert to workflow format,
    upload to CC workflows, upload original to blob.
    Returns dict with success, tad_resolved, mad_resolved, total_count.
    """
    is_valid, error_msg, df = validate_cc_file_format(file_path)
    if not is_valid:
        send_slack_alert(
            f"Validation failed: {error_msg}",
            file_path=file_path,
            error_details=error_msg,
        )
        return {"success": False, "error": error_msg, "tad_resolved": 0, "mad_resolved": 0, "total_count": 0}

    try:
        df_transformed = transform_cc_stop_file(df)
        if df_transformed is None or len(df_transformed) == 0:
            send_slack_alert("No data after transformation", file_path=file_path)
            return {"success": False, "error": "No data after transformation", "tad_resolved": 0, "mad_resolved": 0, "total_count": 0}

        df_workflow = convert_to_cc_workflow_format(df_transformed)
        if df_workflow is None or len(df_workflow) == 0:
            send_slack_alert("No data after conversion", file_path=file_path)
            return {"success": False, "error": "No data after conversion", "tad_resolved": 0, "mad_resolved": 0, "total_count": 0}

        tad_resolved = (df_workflow["tad_resolved"] == "true").sum()
        mad_resolved = (df_workflow["mad_resolved"] == "true").sum()
        total_count = len(df_workflow)

        # Save transformed file and upload to workflows
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        os.makedirs(CC_WORKING_FILE_DIR, exist_ok=True)
        working_path = os.path.join(CC_WORKING_FILE_DIR, f"{base_name}_transformed.csv")
        df_workflow.to_csv(working_path, index=False)

        for workflow_id in CC_WORKFLOW_IDS:
            await upload_cc_working_file(working_path, workflow_id)

        # Upload original to blob
        filename = os.path.basename(file_path)
        blob_path = upload_to_blob(file_path, "paid_file", "cc", filename=filename)
        if not blob_path:
            send_slack_alert("Blob upload failed", file_path=file_path)

        send_slack_alert(
            f"Processed {total_count} rows (TAD resolved: {tad_resolved}, MAD resolved: {mad_resolved})",
            file_path=file_path,
            is_success=True,
        )
        return {"success": True, "tad_resolved": tad_resolved, "mad_resolved": mad_resolved, "total_count": total_count}

    except Exception as e:
        tb = traceback.format_exc()
        send_slack_alert(
            f"Processing failed: {e}",
            file_path=file_path,
            error_details=tb,
        )
        return {"success": False, "error": str(e), "tad_resolved": 0, "mad_resolved": 0, "total_count": 0}


async def main(files_to_process=None):
    """
    Process list of (local_path, remote_name, full_remote_path) tuples.
    For each: validate exists, process, log to axis_processed_files on success.
    """
    if files_to_process is None:
        files_to_process = []

    for item in files_to_process:
        if len(item) < 3:
            continue
        local_path, remote_name, full_remote_path = item[0], item[1], item[2]
        if not os.path.exists(local_path):
            send_slack_alert(f"Local file not found: {local_path}", file_path=full_remote_path)
            continue

        result = await process_cc_file(local_path)
        if result.get("success"):
            log_to_axis_processed_files(full_remote_path)


if __name__ == "__main__":
    asyncio.run(main())
