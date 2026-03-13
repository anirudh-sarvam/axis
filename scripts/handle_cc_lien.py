"""CC Lien Handler — marks all lien-restriction accounts as tad_resolved.

Reads CC lien restriction Excel, marks ALL accounts as tad_resolved=true
(lien has been placed on their account), creates CSV with user_identifier +
tad_resolved=true, uploads to Sarvam API.
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

CC_WORKFLOW_ID = "axis-cc-workflow-96c8a4ac"


def _find_account_column(df):
    col_map = {c.strip().upper().replace(" ", "_"): c for c in df.columns}
    for name in ("#ACCOUNT_NO", "ACCOUNT_NO", "ACC_NO", "CARD_NO", "CARDNO"):
        if name in col_map:
            return col_map[name]
    for c in df.columns:
        if ("card" in c.lower() or "account" in c.lower()) and "no" in c.lower():
            return c
    return df.columns[0]


def process_lien(local_path, output_dir):
    """Read CC lien restriction Excel, mark all accounts as tad_resolved.
    Returns (df_result, csv_path, stats_dict).
    """
    df = pd.read_excel(local_path)
    acc_col = _find_account_column(df)

    result = pd.DataFrame()
    result["user_identifier"] = df[acc_col].astype(str).str.strip()
    result["tad_resolved"] = "true"
    result = result[result["user_identifier"].str.len() > 0]
    result = result[result["user_identifier"] != "nan"]
    result = result.drop_duplicates(subset="user_identifier", keep="last")

    os.makedirs(output_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(local_path))[0]
    csv_path = os.path.join(output_dir, f"{base}_resolved.csv")
    result.to_csv(csv_path, index=False)

    print(f"[CC Lien] All {len(result)} accounts marked tad_resolved (lien restriction)")

    stats = {
        "total_accounts": len(df),
        "unique_resolved": len(result),
        "account_column": acc_col,
    }
    return result, csv_path, stats


async def upload_and_wait(csv_path):
    """Upload resolved CSV to Sarvam API and wait for completion."""
    token = await get_axis_access_token()
    job_id = await upload_user_context_file(csv_path, token, CC_WORKFLOW_ID)
    print(f"Job ID: {job_id}")

    while True:
        await asyncio.sleep(5)
        status_resp = await get_job_status(job_id, token, CC_WORKFLOW_ID)
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
    result, csv_path, stats = process_lien(local_path, output_dir)
    print(f"CC lien: {stats['total_accounts']} total, {stats.get('funds_available', '?')} with funds, {stats['unique_resolved']} unique resolved")
    resp = await upload_and_wait(csv_path)
    success = resp and resp.get("status", "").upper() == "COMPLETED"
    return success, csv_path, stats
