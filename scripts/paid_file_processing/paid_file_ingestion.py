#!/usr/bin/env python3
"""
PL Paid File SFTP Ingestion Script

Fetches PL payment files (PL RX POC payment file Sarvam DD-Mon.xlsx) from SFTP and processes them
using transform_stop_file.py logic.
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
import time as time_module
import traceback
from datetime import datetime
import pytz

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_THIS_DIR)
_ROOT_DIR = os.path.dirname(_SCRIPTS_DIR)
sys.path.insert(0, _THIS_DIR)
sys.path.insert(0, _SCRIPTS_DIR)
sys.path.insert(0, _ROOT_DIR)

from transform_stop_file import (
    transform_stop_file,
    convert_to_workflow_format,
    update_cumulative_working_file,
    upload_working_file,
    WORKFLOW_IDS,
    PL_WORKING_FILE_DIR,
)
from blob_utils import upload_to_blob

IST = pytz.timezone("Asia/Kolkata")
SFTP_HOST = "s2fs.axisbank.com"
SFTP_USER = "sarvam"
SFTP_PASS = os.environ["SFTP_PASS"]
BASE_DIR = "/home/sarvam/axis"
PAID_FILE_DIR = BASE_DIR + "/data/paid_file/pl"
WORKING_FILE_DIR = BASE_DIR + "/data/working_file/pl"
LOG_FILE = BASE_DIR + "/logs/paid_file_ingestion.log"
AXIS_PROCESSED_LOG = BASE_DIR + "/logs/axis_processed_files.log"
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "C0ADMKT2WDT")
PAID_LOG_FILE = LOG_FILE

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)


def _now():
    """Return current datetime in IST without tzinfo."""
    return datetime.now(IST).replace(tzinfo=None)


def send_slack_alert(
    message,
    file_path=None,
    error_details=None,
    is_success=False,
    alert_type="PAID FILE INGESTION",
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


def validate_file_format(file_path):
    """
    Validate PL paid file format.
    Check exists, not empty, format, read df, check ACCNO/ACCNO2 columns, STATUS column,
    transform, validate ACCNO count.
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

    # Basic transform / validation
    df = df.copy()
    df[accno_col] = df[accno_col].astype(str).str.strip()
    df = df[df[accno_col].str.len() > 0]
    df = df[df[accno_col] != "nan"]

    if len(df) == 0:
        return False, "No valid ACCNO rows after validation", df

    return True, None, df


def build_file_pattern():
    """Build regex for PL paid files."""
    pattern = r"(?:PL\s+RX\s+POC\s+payment\s+file.*?Sarvam|Payment\s+File\s+Sarvam\s*-?\s*R30).*\.(xlsx|xls|csv)"
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    return compiled_pattern, pattern


def build_pl_allocation_pattern():
    """Build regex for PL allocation files."""
    pattern = r"(?:(?:PL\s+)?POC\s+RX\s+allocation|PL\s+RX\s+Post\s+due\s+Allocation|^Sarvam\s*-\s*R30).*\.(xlsx|xls|csv)"
    compiled_pattern = re.compile(pattern, re.IGNORECASE)
    return compiled_pattern, pattern


def is_file_modified_today(month_str, date_str, time_or_year):
    """Parse file date from ls output, compare to today IST."""
    try:
        now = _now()
        month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }
        m = month_map.get(month_str[:3], 0)
        if m == 0:
            return False
        d = int(date_str)
        # time_or_year: if contains ':' it's time (same year), else year
        if ":" in str(time_or_year):
            year = now.year
        else:
            year = int(time_or_year)
        file_date = datetime(year, m, d)
        return file_date.date() == now.date()
    except (ValueError, TypeError, KeyError):
        return False


def log_downloaded_file(remote_path):
    """Append remote_path to LOG_FILE."""
    try:
        with open(LOG_FILE, "a") as f:
            f.write(remote_path + "\n")
    except Exception as e:
        print(f"[log] Failed to append to {LOG_FILE}: {e}")


def get_downloaded_files():
    """Read set of remote paths from LOG_FILE."""
    if not os.path.exists(LOG_FILE):
        return set()
    try:
        with open(LOG_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except Exception as e:
        print(f"[log] Failed to read {LOG_FILE}: {e}")
        return set()


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


async def process_paid_file(file_path):
    """
    Validate, transform, convert to workflow format, update cumulative,
    upload to workflows, upload original to blob.
    Returns dict with success, status_counts.
    """
    is_valid, error_msg, df = validate_file_format(file_path)
    if not is_valid:
        send_slack_alert(
            f"Validation failed: {error_msg}",
            file_path=file_path,
            error_details=error_msg,
        )
        return {"success": False, "error": error_msg, "status_counts": {}}

    try:
        df_transformed = transform_stop_file(df)
        if df_transformed is None or len(df_transformed) == 0:
            send_slack_alert("No data after transform", file_path=file_path)
            return {"success": False, "error": "No data after transform", "status_counts": {}}

        df_workflow = convert_to_workflow_format(df_transformed)
        if df_workflow is None or len(df_workflow) == 0:
            send_slack_alert("No data after conversion", file_path=file_path)
            return {"success": False, "error": "No data after conversion", "status_counts": {}}

        working_path = update_cumulative_working_file(df_workflow)
        if not os.path.exists(working_path):
            send_slack_alert("Failed to update cumulative working file", file_path=file_path)
            return {"success": False, "error": "Cumulative update failed", "status_counts": {}}

        for wf_id in WORKFLOW_IDS:
            await upload_working_file(working_path, wf_id)

        # Upload original to blob
        filename = os.path.basename(file_path)
        blob_path = upload_to_blob(file_path, "paid_file", "pl", filename=filename)
        if not blob_path:
            send_slack_alert("Blob upload failed", file_path=file_path)

        status_counts = df_transformed["STATUS"].value_counts().to_dict()
        resolved = status_counts.get("RESOLVED", status_counts.get("Resolved", 0))
        unresolved = len(df_transformed) - resolved
        send_slack_alert(
            f"Processed {len(df_workflow)} rows\n"
            f"• Resolved: {resolved}\n"
            f"• Unresolved: {unresolved}",
            file_path=file_path,
            is_success=True,
        )
        return {"success": True, "status_counts": status_counts}

    except Exception as e:
        tb = traceback.format_exc()
        send_slack_alert(
            f"Processing failed: {e}",
            file_path=file_path,
            error_details=tb,
        )
        return {"success": False, "error": str(e), "status_counts": {}}


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

        result = await process_paid_file(local_path)
        if result.get("success"):
            log_to_axis_processed_files(full_remote_path)


if __name__ == "__main__":
    asyncio.run(main())
