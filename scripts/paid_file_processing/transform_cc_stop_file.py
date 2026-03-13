#!/usr/bin/env python3
"""
CC (Credit Card) Stop File Transformation and Upload

Transforms CC payment files (Post Due Date Cards Status) into CC workflow format
using allocation data for TAD/MAD resolution based on payment amounts.

CC workflow format:
  user_identifier, total_amount_due, min_amount_due, payment_till_date, tad_resolved, mad_resolved
"""

import pandas as pd
import asyncio
import os
import sys
import glob

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_THIS_DIR)
_ROOT_DIR = os.path.dirname(_SCRIPTS_DIR)
sys.path.insert(0, _THIS_DIR)
sys.path.insert(0, _SCRIPTS_DIR)
sys.path.insert(0, _ROOT_DIR)

from api_client import get_axis_access_token, upload_user_context_file, get_job_status
from blob_utils import sync_from_blob, cleanup_local_dir

BASE_URL = "https://apps.sarvam.ai"
ORG_ID = "axisbank.com"
WORKSPACE_ID = "axisbank-com-defau-3b9581"

# CC-specific workflow IDs
CC_WORKFLOW_IDS = [
    "axis-cc-workflow-96c8a4ac",
]

# Directory configuration (matches PL structure under data/)
BASE_DIR = "/home/sarvam/axis"
CC_ALLOCATION_DIR = os.path.join(BASE_DIR, "data", "allocation_file", "cc")
CC_PAID_FILE_DIR = os.path.join(BASE_DIR, "data", "paid_file", "cc")
CC_WORKING_FILE_DIR = os.path.join(BASE_DIR, "data", "working_file", "cc")


def load_allocation_data() -> pd.DataFrame:
    """
    Load all CC allocation files from blob storage (current month only).
    Returns DataFrame with: user_identifier, total_amount_due, min_amount_due
    If multiple files, merges them (last occurrence wins for duplicates).
    """
    # Fetch allocation files from blob (current month only)
    temp_dir, downloaded_files = sync_from_blob("allocation_file", "cc")

    alloc_files = downloaded_files
    if not alloc_files:
        print(f"Warning: No CC allocation files found for current month")
        cleanup_local_dir(temp_dir)
        return pd.DataFrame(columns=["user_identifier", "total_amount_due", "min_amount_due"])

    dfs = []
    for path in alloc_files:
        try:
            if path.endswith((".xlsx", ".xls")):
                df = pd.read_excel(path)
            else:
                df = pd.read_csv(path, low_memory=False)

            # Extract key columns - handle both raw and transformed file formats
            account_col = None
            for col in ["#ACCOUNT_NO", "ACCOUNT_NO", "Account No", "user_identifier"]:
                if col in df.columns:
                    account_col = col
                    break

            if account_col is None:
                print(f"Warning: No account column found in {os.path.basename(path)}, skipping")
                continue

            result = pd.DataFrame()
            result["user_identifier"] = df[account_col].astype(str).str.strip()

            # Total amount due
            # Transformed files have total_amount_due directly; raw files use TOTAL_OUTS
            # Prefer total_amount_due (correct cycle amount) over TOTAL_OUTS (total outstanding)
            if "total_amount_due" in df.columns:
                result["total_amount_due"] = pd.to_numeric(df["total_amount_due"], errors="coerce").fillna(0)
            elif "TOTAL_OUTS" in df.columns:
                result["total_amount_due"] = pd.to_numeric(df["TOTAL_OUTS"], errors="coerce").fillna(0)
            else:
                result["total_amount_due"] = 0

            # Minimum amount due
            # Transformed files have min_amount_due directly; raw files use MIN_AMT_DUE
            if "min_amount_due" in df.columns:
                result["min_amount_due"] = pd.to_numeric(df["min_amount_due"], errors="coerce").fillna(0)
            elif "MIN_AMT_DUE" in df.columns:
                result["min_amount_due"] = pd.to_numeric(df["MIN_AMT_DUE"], errors="coerce").fillna(0)
            else:
                result["min_amount_due"] = 0

            result["total_amount_due"] = result["total_amount_due"].apply(lambda x: int(round(x)))
            result["min_amount_due"] = result["min_amount_due"].apply(lambda x: int(round(x)))

            dfs.append(result)
            print(f"Loaded {len(result)} accounts from allocation: {os.path.basename(path)}")

        except Exception as e:
            print(f"Warning: Failed to load allocation file {os.path.basename(path)}: {e}")

    if not dfs:
        return pd.DataFrame(columns=["user_identifier", "total_amount_due", "min_amount_due"])

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.drop_duplicates(subset="user_identifier", keep="last")
    print(f"Total unique accounts in allocation: {len(combined)}")

    # Cleanup temp allocation files
    cleanup_local_dir(temp_dir)

    return combined


def _find_col(row, candidates):
    """Find the first matching column name from candidates (case-insensitive)."""
    row_keys = {k.strip().lower(): k for k in row.index}
    for c in candidates:
        if c in row.index:
            return c
        lower = c.strip().lower()
        if lower in row_keys:
            return row_keys[lower]
    return None


def get_payment_amount(row):
    """Extract payment amount from CC payment file row."""
    col = _find_col(row, [
        "Amount", "AMOUNT", "amount",
        "Payment", "PAYMENT", "payment",
        "Payment Received Amount", "PAYMENT_RECEIVED_AMOUNT", "payment_received_amount",
    ])
    if col and pd.notna(row[col]):
        try:
            return int(round(float(row[col])))
        except (ValueError, TypeError):
            pass
    return 0


def get_user_identifier(row):
    """Extract account number from CC payment file row."""
    col = _find_col(row, [
        "ACCOUNT_NO", "#ACCOUNT_NO", "Account No", "Account_No",
        "account_no", "#account_no", "account no",
        "user_identifier",
    ])
    if col and pd.notna(row[col]) and str(row[col]).strip():
        return str(row[col]).strip()
    return None


def transform_cc_stop_file(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform CC payment file using allocation data for TAD/MAD resolution.

    Input:  Raw CC payment file DataFrame (ACCOUNT_NO, Amount, ...)
    Output: DataFrame with user_identifier, total_amount_due, min_amount_due,
            payment_till_date, tad_resolved, mad_resolved
    """
    df = df.copy()

    # Extract account and payment from payment file
    df["user_identifier"] = df.apply(get_user_identifier, axis=1)
    df["payment_till_date"] = df.apply(get_payment_amount, axis=1)

    # Drop rows without valid account numbers
    df = df.dropna(subset=["user_identifier"])

    # Capture raw Final Status before dedup (case-insensitive column lookup)
    status_col = None
    for c in df.columns:
        if c.strip().lower() in ("final status", "final_status", "status"):
            status_col = c
            break
    if status_col:
        df["_raw_resolved"] = df[status_col].astype(str).str.strip().str.upper() == "RESOLVED"
    else:
        df["_raw_resolved"] = False

    # Keep only what we need from payment file, deduplicate
    payment_df = df[["user_identifier", "payment_till_date", "_raw_resolved"]].copy()
    payment_df = payment_df.drop_duplicates(subset="user_identifier", keep="last")

    # Load allocation data
    alloc_df = load_allocation_data()

    # Merge: start with payment file accounts, left join allocation data
    merged = payment_df.merge(alloc_df, on="user_identifier", how="left")

    # Fill missing allocation data with 0
    merged["total_amount_due"] = merged["total_amount_due"].fillna(0).astype(int)
    merged["min_amount_due"] = merged["min_amount_due"].fillna(0).astype(int)
    merged["payment_till_date"] = merged["payment_till_date"].fillna(0).astype(int)

    # tad_resolved: payment >= total_amount_due (payment math only, exclusive)
    merged["tad_resolved"] = merged.apply(
        lambda r: "true" if r["total_amount_due"] > 0 and r["payment_till_date"] >= r["total_amount_due"] else "false",
        axis=1
    )

    # mad_resolved: payment >= min_amount_due but < total_amount_due,
    # OR Final Status = Resolved (fallback). Excludes TAD-resolved accounts.
    merged["mad_resolved"] = merged.apply(
        lambda r: "false" if r["total_amount_due"] > 0 and r["payment_till_date"] >= r["total_amount_due"]
        else ("true" if (r["min_amount_due"] > 0 and r["payment_till_date"] >= r["min_amount_due"]) or r.get("_raw_resolved", False) else "false"),
        axis=1
    )

    tad_by_payment = (merged["tad_resolved"] == "true").sum()
    mad_by_payment = ((merged["min_amount_due"] > 0) & (merged["payment_till_date"] >= merged["min_amount_due"])).sum()
    mad_by_status = ((merged["mad_resolved"] == "true") & ~((merged["min_amount_due"] > 0) & (merged["payment_till_date"] >= merged["min_amount_due"]))).sum()
    resolved_by_status = merged["_raw_resolved"].sum()

    merged.drop(columns=["_raw_resolved"], inplace=True)

    # Log stats
    print(f"\nCC transformation stats:")
    print(f"  Total accounts in payment file: {len(merged)}")
    print(f"  Accounts with allocation data: {len(merged[merged['total_amount_due'] > 0])}")
    print(f"  Accounts without allocation data: {len(merged[merged['total_amount_due'] == 0])}")
    print(f"  TAD resolved (payment >= TAD): {tad_by_payment}")
    print(f"  MAD resolved (total): {(merged['mad_resolved'] == 'true').sum()}")
    print(f"    - by payment (payment >= MAD): {mad_by_payment}")
    print(f"    - by Final Status (payment < MAD): {mad_by_status}")
    print(f"  Raw Final Status = Resolved: {resolved_by_status}")

    return merged


def convert_to_cc_workflow_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert to CC workflow upload format.

    Output columns: user_identifier, total_amount_due, min_amount_due,
                    payment_till_date, tad_resolved, mad_resolved
    """
    workflow_df = df[[
        "user_identifier",
        "total_amount_due",
        "min_amount_due",
        "payment_till_date",
        "tad_resolved",
        "mad_resolved"
    ]].copy()

    workflow_df["user_identifier"] = workflow_df["user_identifier"].astype(str).str.strip()
    workflow_df["total_amount_due"] = workflow_df["total_amount_due"].astype(int)
    workflow_df["min_amount_due"] = workflow_df["min_amount_due"].astype(int)
    workflow_df["payment_till_date"] = workflow_df["payment_till_date"].astype(int)

    return workflow_df


async def upload_cc_working_file(working_file_path: str, workflow_id: str) -> None:
    """Upload CC working file to specified workflow."""
    try:
        access_token = await get_axis_access_token()
        job_id = await upload_user_context_file(working_file_path, access_token, workflow_id)
        print(f"Job ID: {job_id}")

        while True:
            await asyncio.sleep(5)
            job_status = await get_job_status(job_id, access_token, workflow_id)
            status = job_status.get("status", "unknown")
            print(f"Job status: {status}")

            if status.lower() != "running":
                print(f"Job completed with status: {status}")
                print(job_status)
                break
    except Exception as e:
        print(f"Error uploading CC file: {e}")
        raise


async def main():
    """Manual execution: transform and upload a CC payment file."""
    filename = "Post Due Date Cards Status 12-Feb-2026 (Sarvam).xlsx"
    input_file_path = os.path.join(CC_PAID_FILE_DIR, filename)

    if not os.path.exists(input_file_path):
        print(f"Error: File not found: {input_file_path}")
        return

    # Read file
    if input_file_path.endswith((".xlsx", ".xls")):
        df = pd.read_excel(input_file_path)
    else:
        df = pd.read_csv(input_file_path, low_memory=False)

    print(f"Loaded {len(df)} rows. Columns: {list(df.columns)}")

    # Transform (merges with allocation data, computes TAD/MAD)
    df_transformed = transform_cc_stop_file(df)
    if df_transformed is None or len(df_transformed) == 0:
        print("Error: No data to process")
        return

    # Convert to workflow format
    df_workflow = convert_to_cc_workflow_format(df_transformed)
    print(f"\nWorkflow format ({len(df_workflow)} rows):")
    print(df_workflow.head(10))

    # Save transformed file
    base_name = os.path.splitext(filename)[0]
    transformed_path = os.path.join(CC_WORKING_FILE_DIR, f"{base_name}_transformed.csv")
    os.makedirs(CC_WORKING_FILE_DIR, exist_ok=True)
    df_workflow.to_csv(transformed_path, index=False)
    print(f"\nSaved: {transformed_path}")

    # Upload to all CC workflows
    for workflow_id in CC_WORKFLOW_IDS:
        print(f"\nUploading to CC workflow: {workflow_id}")
        await upload_cc_working_file(transformed_path, workflow_id)


if __name__ == "__main__":
    asyncio.run(main())
