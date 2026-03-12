"""PL Pullback Handler — marks pullback accounts as resolved_paid.

Detects account column automatically, creates a CSV with
user_identifier + resolved_paid=true, uploads to Sarvam API.
"""

import pandas as pd
import os
import sys
import asyncio

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform_stop_file import (
    get_axis_access_token,
    upload_user_context_file,
    get_job_status,
)

PL_WORKFLOW_ID = "axis-workflow-v3-e696d676"


def _find_account_column(df):
    for c in df.columns:
        if "acc" in c.lower() or "account" in c.lower():
            return c
    return df.columns[0]


def process_pullback(local_path, output_dir):
    """Read pullback Excel, produce resolved CSV.
    Returns (df_result, stats_dict).
    """
    df = pd.read_excel(local_path)
    col = _find_account_column(df)

    result = pd.DataFrame()
    result["user_identifier"] = df[col].astype(str).str.strip()
    result["resolved_paid"] = "true"
    result = result.drop_duplicates(subset="user_identifier", keep="last")

    os.makedirs(output_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(local_path))[0]
    csv_path = os.path.join(output_dir, f"{base}_resolved.csv")
    result.to_csv(csv_path, index=False)

    stats = {
        "total_accounts": len(df),
        "unique_resolved": len(result),
        "account_column": col,
    }
    return result, csv_path, stats


async def upload_and_wait(csv_path):
    """Upload resolved CSV to Sarvam API and wait for completion."""
    token = await get_axis_access_token()
    job_id = await upload_user_context_file(csv_path, token, PL_WORKFLOW_ID)
    print(f"Job ID: {job_id}")

    while True:
        await asyncio.sleep(5)
        status_resp = await get_job_status(job_id, token, PL_WORKFLOW_ID)
        status = status_resp.get("status", "unknown")
        print(f"Job status: {status}")
        if status.lower() != "running":
            print(f"Job completed with status: {status}")
            return status_resp
    return None


async def process(local_path, output_dir):
    """Full pipeline: process file + upload to API.
    Returns (success, csv_path, stats).
    """
    result, csv_path, stats = process_pullback(local_path, output_dir)
    print(f"PL pullback: {stats['total_accounts']} accounts, {stats['unique_resolved']} unique resolved")
    resp = await upload_and_wait(csv_path)
    success = resp and resp.get("status", "").upper() == "COMPLETED"
    return success, csv_path, stats
