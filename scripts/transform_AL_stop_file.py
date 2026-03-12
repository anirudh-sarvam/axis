#!/usr/bin/env python3
"""
AL (Auto Loan) Stop File Transformation and Upload

Transforms AL payment files (Paid File_DD Mon.csv) into workflow format
and uploads to AL workflows. Follows the same pattern as transform_stop_file.py (PL).

Workflow format:
  ACCNO2, resolved_paid, user_identifier, confirmed_paid_amount
"""

import pandas as pd
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform_stop_file import (
    get_axis_access_token,
    upload_user_context_file,
    get_job_status,
)
from blob_utils import sync_from_blob, cleanup_local_dir

BASE_URL = "https://apps.sarvam.ai"
ORG_ID = "axisbank.com"
WORKSPACE_ID = "axisbank-com-defau-3b9581"

AL_WORKFLOW_IDS = [
    "axis-workflow-v3-e696d676",
]

BASE_DIR = "/home/sarvam/axis"
AL_PAID_FILE_DIR = os.path.join(BASE_DIR, "data", "paid_file", "al")
AL_WORKING_FILE_DIR = os.path.join(BASE_DIR, "data", "working_file", "al")


def load_al_allocation_data():
    """Load Total Overdue per ACCNO from AL allocation files (current month only).

    Returns a dict: {accno: total_overdue}
    """
    allocation_data = {}

    temp_dir, downloaded_files = sync_from_blob("allocation_file", "al")
    if not downloaded_files:
        print("Warning: No AL allocation files found for current month")
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
            for col in df.columns:
                if col.upper() in ["ACCNO2", "ACCNO"]:
                    accno_col = col
                    break
            if not accno_col:
                print(f"Warning: No ACCNO column in AL allocation: {os.path.basename(f)}, skipping")
                continue

            overdue_col = None
            for col in df.columns:
                if col.upper() in ["TOTAL OVERDUE", "TOTAL_OVERDUE"]:
                    overdue_col = col
                    break
            if not overdue_col:
                print(f"Warning: No Total Overdue column in AL allocation: {os.path.basename(f)}, skipping")
                continue

            count = 0
            for _, row in df.iterrows():
                accno = str(row[accno_col]).strip()
                try:
                    overdue = round(float(row[overdue_col])) if pd.notna(row[overdue_col]) else 0
                except (ValueError, TypeError):
                    overdue = 0
                if accno and accno != "nan":
                    allocation_data[accno] = overdue
                    count += 1
            print(f"Loaded {count} accounts from AL allocation: {os.path.basename(f)}")
        except Exception as e:
            print(f"Warning: Failed to load AL allocation file {os.path.basename(f)}: {e}")

    print(f"Total unique AL accounts in allocation: {len(allocation_data)}")
    cleanup_local_dir(temp_dir)
    return allocation_data


def get_user_identifier(row):
    """Extract account number from AL payment file row."""
    if "ACCNO2" in row.index and pd.notna(row["ACCNO2"]) and str(row["ACCNO2"]).strip():
        return str(row["ACCNO2"]).strip()
    if "ACCNO" in row.index and pd.notna(row["ACCNO"]) and str(row["ACCNO"]).strip():
        return str(row["ACCNO"]).strip()
    return None


def get_status(row):
    """Determine RESOLVED/UNRESOLVED status from AL payment file."""
    if "STATUS" in row.index and pd.notna(row["STATUS"]):
        status = str(row["STATUS"]).strip().lower()
        if status in ["resolved", "normalization"]:
            return "RESOLVED"

    if "OD" in row.index and pd.notna(row["OD"]):
        try:
            if round(float(row["OD"])) <= 1:
                return "RESOLVED"
        except (ValueError, TypeError):
            pass

    return "UNRESOLVED"


def get_total_overdue(row):
    """Extract total overdue from AL payment file row."""
    if "OD" in row.index and pd.notna(row["OD"]):
        try:
            return int(round(float(row["OD"])))
        except (ValueError, TypeError):
            pass
    return 0


def transform_al_stop_file(df: pd.DataFrame) -> pd.DataFrame:
    """Transform AL payment file to standard format.

    Input:  Raw AL payment file DataFrame (ACCNO, STATUS, OD, Last payment)
    Output: DataFrame with ACCNO, _STATUS, AMOUNT, TOTAL_OVERDUE columns added
    """
    df = df.copy()
    df.columns = df.columns.str.strip()

    df["ACCNO"] = df.apply(get_user_identifier, axis=1)
    df["_STATUS"] = df.apply(get_status, axis=1)
    df["TOTAL_OVERDUE"] = df.apply(get_total_overdue, axis=1)

    df = df.dropna(subset=["ACCNO"])

    print(f"\nAL STATUS distribution after transformation:")
    print(df["_STATUS"].value_counts())

    return df


def convert_to_al_workflow_format(df: pd.DataFrame) -> pd.DataFrame:
    """Convert transformed AL data to workflow upload format.

    Uses allocation data to compute confirmed_paid_amount = max(0, original_overdue - current_overdue).

    Output columns: ACCNO2, resolved_paid, user_identifier, confirmed_paid_amount
    """
    allocation_data = load_al_allocation_data()

    rows = []
    for _, row in df.iterrows():
        uid = str(row["ACCNO"]).strip()
        current_overdue = int(row.get("TOTAL_OVERDUE", 0))
        original_overdue = allocation_data.get(uid, current_overdue)
        confirmed_paid = max(0, original_overdue - current_overdue)

        rows.append({
            "ACCNO2": uid,
            "resolved_paid": "true" if str(row["_STATUS"]).upper() == "RESOLVED" else "false",
            "user_identifier": uid,
            "confirmed_paid_amount": confirmed_paid,
        })

    workflow_df = pd.DataFrame(rows)
    workflow_df = workflow_df.drop_duplicates(subset="user_identifier", keep="last")
    workflow_df = workflow_df[["ACCNO2", "resolved_paid", "user_identifier", "confirmed_paid_amount"]]

    paid_count = len(workflow_df[workflow_df["confirmed_paid_amount"] > 0])
    print(f"AL workflow: {len(workflow_df)} accounts, {paid_count} with payments")

    return workflow_df


async def upload_al_working_file(working_file_path: str, workflow_id: str) -> None:
    """Upload AL working file to specified workflow."""
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
        print(f"Error uploading AL file: {e}")
        raise


async def main():
    """Manual execution: transform and upload an AL payment file."""
    filename = "Paid_File_5_Mar.csv"
    input_file_path = os.path.join(AL_PAID_FILE_DIR, filename)

    if not os.path.exists(input_file_path):
        print(f"Error: File not found: {input_file_path}")
        return

    if input_file_path.endswith((".xlsx", ".xls")):
        df = pd.read_excel(input_file_path)
    else:
        df = pd.read_csv(input_file_path, low_memory=False)

    print(f"Loaded {len(df)} rows. Columns: {list(df.columns)}")

    df_transformed = transform_al_stop_file(df)
    if df_transformed is None or len(df_transformed) == 0:
        print("Error: No data to process")
        return

    df_workflow = convert_to_al_workflow_format(df_transformed)
    print(f"\nWorkflow format ({len(df_workflow)} rows):")
    print(df_workflow.head(10))

    base_name = os.path.splitext(filename)[0]
    transformed_path = os.path.join(AL_WORKING_FILE_DIR, f"{base_name}_transformed.csv")
    os.makedirs(AL_WORKING_FILE_DIR, exist_ok=True)
    df_workflow.to_csv(transformed_path, index=False)
    print(f"\nSaved: {transformed_path}")

    for workflow_id in AL_WORKFLOW_IDS:
        print(f"\nUploading to AL workflow: {workflow_id}")
        await upload_al_working_file(transformed_path, workflow_id)


if __name__ == "__main__":
    asyncio.run(main())
