#!/usr/bin/env python3
"""
PL (Personal Loan) Stop File Transformation and Upload

Transforms PL payment files into workflow format, maintains a cumulative working file
across processing runs, and computes confirmed_paid_amount using allocation data from blob.
"""

import pandas as pd
import asyncio
import os
import sys
import pytz
from datetime import datetime

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_THIS_DIR)
_ROOT_DIR = os.path.dirname(_SCRIPTS_DIR)
sys.path.insert(0, _THIS_DIR)
sys.path.insert(0, _SCRIPTS_DIR)
sys.path.insert(0, _ROOT_DIR)

from api_client import get_axis_access_token, upload_user_context_file, get_job_status
from blob_utils import sync_from_blob, upload_to_blob, download_from_blob, cleanup_local_dir
WORKFLOW_IDS = [
    "axis-workflow-v3-e696d676",
]
BASE_DIR = "/home/sarvam/axis"
PL_WORKING_FILE_DIR = os.path.join(BASE_DIR, "data", "working_file", "pl")


def _find_col(row, candidates):
    """Find the first matching column name from candidates (case-insensitive)."""
    row_keys = {k.strip().lower(): k for k in row.keys()}
    for c in candidates:
        if c in row.keys():
            return c
        lower = c.strip().lower()
        if lower in row_keys:
            return row_keys[lower]
    return None


def get_user_identifier(row):
    """Extract ACCNO2 or ACCNO from row."""
    col = _find_col(row, ["ACCNO2", "ACCNO", "Accno2", "Accno", "accno2", "accno"])
    if col is not None:
        return row[col]
    raise ValueError(
        f"No account number column found. "
        f"Expected 'ACCNO2' or 'ACCNO'. Available: {list(row.keys())}"
    )


def get_status(row):
    """Determine RESOLVED/UNRESOLVED."""
    status_val = ""
    for col in ["Status", "STATUS", "status"]:
        if col in row and pd.notna(row[col]) and str(row[col]).strip():
            status_val = str(row[col]).strip().lower()
            break

    overdue_val = None
    for col in ["Total Overdue", "TOTAL_OVERDUE", "Total_Overdue"]:
        if col in row and pd.notna(row[col]):
            overdue_val = row[col]
            break

    if (
        status_val in ("resolved", "resoled", "r")
        or (
            overdue_val is not None and round(float(overdue_val)) <= 1
        )
        or str(row.get("Campaign till date", "")) == "5th jan"
    ):
        return "RESOLVED"
    else:
        return "UNRESOLVED"


def _parse_date_val(date_val):
    """Parse a date value (Timestamp or string) to YYYY-MM-DD."""
    try:
        if isinstance(date_val, pd.Timestamp):
            return date_val.strftime("%Y-%m-%d")
        elif isinstance(date_val, str):
            for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d-%b-%Y", "%d %b %Y"]:
                try:
                    return datetime.strptime(date_val.strip(), fmt).strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    continue
    except Exception:
        pass
    return None


def get_payment_date(row):
    """Extract PAYMENT_DATE from Res_Date, PM Date, or LAST_PAYMENT_DATE, format as YYYY-MM-DD."""
    for candidates in [
        ["Res_Date", "RES_DATE", "res_date"],
        ["PM Date", "PM_DATE", "pm date", "PM_Date"],
        ["LAST_PAYMENT_DATE", "Last_Payment_Date", "last_payment_date"],
    ]:
        col = _find_col(row, candidates)
        if col and pd.notna(row[col]) and str(row[col]).strip():
            result = _parse_date_val(row[col])
            if result:
                return result

    return ""


def get_total_overdue(row):
    """Extract total overdue amount, return 0 on error."""
    col = _find_col(row, ["Total Overdue", "TOTAL_OVERDUE", "Total_Overdue", "total_overdue", "TOD", "OD"])
    if col and pd.notna(row[col]):
        try:
            return round(float(row[col]))
        except (ValueError, TypeError):
            pass
    return 0


def transform_stop_file(df: pd.DataFrame) -> pd.DataFrame:
    """Transform to standard format: ACCNO, STATUS, PAYMENT_DATE, TOTAL_OVERDUE."""
    df = df.copy()

    df["ACCNO"] = df.apply(get_user_identifier, axis=1)
    df["STATUS"] = df.apply(get_status, axis=1)
    df["PAYMENT_DATE"] = df.apply(get_payment_date, axis=1)
    df.loc[df["STATUS"] == "UNRESOLVED", "PAYMENT_DATE"] = ""
    df["TOTAL_OVERDUE"] = df.apply(get_total_overdue, axis=1)

    output_df = df[["ACCNO", "STATUS", "PAYMENT_DATE", "TOTAL_OVERDUE"]].copy()
    return output_df


def convert_to_workflow_format(df: pd.DataFrame) -> pd.DataFrame:
    """Convert to ACCNO2, resolved_paid, user_identifier, confirmed_paid_amount format."""
    if "TOTAL_OVERDUE" not in df.columns:
        df = transform_stop_file(df)
    allocation_data = load_pl_allocation_data()

    rows = []
    for _, row in df.iterrows():
        uid = str(row["ACCNO"]).strip()
        current_overdue = int(row.get("TOTAL_OVERDUE", 0))
        original_overdue = allocation_data.get(uid, current_overdue)
        confirmed_paid = max(0, original_overdue - current_overdue)

        rows.append({
            "ACCNO2": uid,
            "resolved_paid": "true" if str(row["STATUS"]).upper() == "RESOLVED" else "false",
            "user_identifier": uid,
            "confirmed_paid_amount": confirmed_paid,
        })

    workflow_df = pd.DataFrame(rows)
    workflow_df = workflow_df.drop_duplicates(subset="user_identifier", keep="last")
    workflow_df = workflow_df[["ACCNO2", "resolved_paid", "user_identifier", "confirmed_paid_amount"]]
    return workflow_df


def load_pl_allocation_data() -> dict:
    """Sync allocation files from blob, build {accno: total_overdue} dict."""
    allocation_data = {}
    temp_dir, downloaded_files = sync_from_blob("allocation_file", "pl")

    if not downloaded_files:
        cleanup_local_dir(temp_dir)
        return allocation_data

    for f in downloaded_files:
        try:
            if f.endswith((".xlsx", ".xls")):
                df = pd.read_excel(f)
            else:
                df = pd.read_csv(f, low_memory=False)

            df.columns = df.columns.str.strip()
            accno_col = None
            overdue_col = None
            for col in df.columns:
                if col.upper() in ["ACCNO2", "ACCNO"]:
                    accno_col = col
                if col.upper() in ["TOTAL OVERDUE", "TOTAL_OVERDUE", "OD"]:
                    overdue_col = col

            if accno_col and overdue_col:
                for _, row in df.iterrows():
                    accno = str(row[accno_col]).strip()
                    try:
                        overdue = round(float(row[overdue_col])) if pd.notna(row[overdue_col]) else 0
                    except (ValueError, TypeError):
                        overdue = 0
                    if accno and accno != "nan":
                        allocation_data[accno] = overdue
        except Exception as e:
            print(f"Warning: Failed to load PL allocation {os.path.basename(f)}: {e}")

    cleanup_local_dir(temp_dir)
    return allocation_data


def load_cumulative_working_file() -> pd.DataFrame:
    """Download cumulative from blob."""
    os.makedirs(PL_WORKING_FILE_DIR, exist_ok=True)
    local_path = os.path.join(PL_WORKING_FILE_DIR, "pl_cumulative.csv")
    from blob_utils import get_current_month
    result = download_from_blob("working_file", "pl", "pl_cumulative.csv", local_path=local_path)
    if result and os.path.exists(result):
        df = pd.read_csv(result, low_memory=False)
        expected_cols = ["ACCNO2", "resolved_paid", "user_identifier", "confirmed_paid_amount"]
        return df[df.columns.intersection(expected_cols)]
    return pd.DataFrame(columns=["ACCNO2", "resolved_paid", "user_identifier", "confirmed_paid_amount"])


def update_cumulative_working_file(today_df: pd.DataFrame) -> str:
    """Merge today's data with cumulative. Upload snapshot and cumulative to blob. Returns path to cumulative file."""
    from blob_utils import get_current_month
    cumulative = load_cumulative_working_file()
    merged = pd.concat([cumulative, today_df], ignore_index=True)
    merged = merged.drop_duplicates(subset="user_identifier", keep="last")
    merged = merged[["ACCNO2", "resolved_paid", "user_identifier", "confirmed_paid_amount"]]

    os.makedirs(PL_WORKING_FILE_DIR, exist_ok=True)
    snapshot_path = os.path.join(PL_WORKING_FILE_DIR, f"pl_snapshot_{datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')}.csv")
    cumulative_path = os.path.join(PL_WORKING_FILE_DIR, "pl_cumulative.csv")
    merged.to_csv(cumulative_path, index=False)
    merged.to_csv(snapshot_path, index=False)

    upload_to_blob(snapshot_path, "working_file", "pl", month=get_current_month())
    upload_to_blob(cumulative_path, "working_file", "pl", month=get_current_month())
    return cumulative_path


async def upload_working_file(working_file_path: str, workflow_id: str) -> None:
    """Get token, upload, poll for completion."""
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


async def main():
    """Manual execution entry point for a PL payment file."""
    filename = "PL R30 Payment file Feb-3_SARVAM.xlsx"
    pl_paid_dir = os.path.join(BASE_DIR, "data", "paid_file", "pl")
    input_file_path = os.path.join(pl_paid_dir, filename)

    if not os.path.exists(input_file_path):
        print(f"Error: File not found: {input_file_path}")
        return

    if input_file_path.endswith((".xlsx", ".xls")):
        df = pd.read_excel(input_file_path)
    else:
        df = pd.read_csv(input_file_path, low_memory=False)

    print(df.columns)
    df = transform_stop_file(df)

    if df is None or len(df) == 0:
        print("Error: No data to process")
        return

    print(df.head(10))
    print(df["STATUS"].value_counts())

    df_workflow = convert_to_workflow_format(df)
    transformed_path = update_cumulative_working_file(df_workflow)
    print(f"\nSaved: {transformed_path}")

    for workflow_id in WORKFLOW_IDS:
        print(f"\nUploading to workflow: {workflow_id}")
        await upload_working_file(transformed_path, workflow_id)


if __name__ == "__main__":
    asyncio.run(main())
