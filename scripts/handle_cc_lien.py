"""CC Lien Handler — marks lien-status accounts as tad_resolved.

Reads CC lien Excel, filters for accounts where SI BALANCE >= Lien Amount
(funds available to cover lien), creates CSV with user_identifier +
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
    for c in df.columns:
        c_up = c.upper().replace(" ", "_")
        if c_up in ("#ACCOUNT_NO", "ACCOUNT_NO", "ACC_NO", "CARD_NO", "CARDNO"):
            return c
        if ("card" in c.lower() or "account" in c.lower()) and "no" in c.lower():
            return c
    return df.columns[0]


def _find_column(df, candidates):
    """Find a column whose name matches one of the candidate substrings."""
    for c in df.columns:
        c_norm = c.upper().replace(" ", "_")
        for cand in candidates:
            if cand in c_norm:
                return c
    return None


def process_lien(local_path, output_dir):
    """Read CC lien Excel, produce resolved CSV.
    Only accounts where SI BALANCE >= Lien Amount are marked resolved.
    Returns (df_result, csv_path, stats_dict).
    """
    df = pd.read_excel(local_path)
    acc_col = _find_account_column(df)
    balance_col = _find_column(df, ["SI_BALANCE", "SI BALANCE", "BALANCE"])
    lien_col = _find_column(df, ["LIEN_AMOUNT", "LIEN AMOUNT", "LIEN_AMT"])

    if balance_col is None or lien_col is None:
        print(f"[CC Lien] Could not find balance/lien columns. Columns: {list(df.columns)}")
        print(f"[CC Lien] balance_col={balance_col}, lien_col={lien_col}")
        filtered = df
    else:
        df[balance_col] = pd.to_numeric(df[balance_col], errors="coerce").fillna(0)
        df[lien_col] = pd.to_numeric(df[lien_col], errors="coerce").fillna(0)
        filtered = df[df[balance_col] >= df[lien_col]]
        print(f"[CC Lien] {balance_col} >= {lien_col}: {len(filtered)} / {len(df)} accounts have funds")

    result = pd.DataFrame()
    result["user_identifier"] = filtered[acc_col].astype(str).str.strip()
    result["tad_resolved"] = "true"
    result = result.drop_duplicates(subset="user_identifier", keep="last")

    os.makedirs(output_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(local_path))[0]
    csv_path = os.path.join(output_dir, f"{base}_resolved.csv")
    result.to_csv(csv_path, index=False)

    stats = {
        "total_accounts": len(df),
        "funds_available": len(filtered),
        "unique_resolved": len(result),
        "account_column": acc_col,
        "balance_column": balance_col or "N/A",
        "lien_column": lien_col or "N/A",
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
