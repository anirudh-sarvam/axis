#!/usr/bin/env python3
"""
CC Allocation Scheduling Pipeline

Transforms CC allocation files, validates data, creates cohorts via the
scheduling API, schedules campaigns per risk segment/bank, and uploads
transformed files to blob storage.

Triggered by axis_sync.py when a CC allocation file arrives on SFTP.
"""

import os
import sys
import json
import re
import traceback
import urllib.request
import asyncio

import httpx
import pandas as pd
import pytz
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS_DIR = os.path.dirname(_THIS_DIR)
_ROOT_DIR = os.path.dirname(_SCRIPTS_DIR)
sys.path.insert(0, _THIS_DIR)
sys.path.insert(0, _SCRIPTS_DIR)
sys.path.insert(0, _ROOT_DIR)

from transform_cc_allocation_file import (
    transform_cc_allocation_file,
    generate_risk_cohorts,
)
from api_client import get_axis_access_token
from blob_utils import upload_to_blob

IST = pytz.timezone("Asia/Kolkata")
BASE_DIR = "/home/sarvam/axis"
CONFIG_PATH = os.path.join(BASE_DIR, "cohort-mappings", "credit-card-config.json")

SCHEDULING_API_BASE = "https://apps.sarvam.ai/api/scheduling/v3"
ORG_ID = "axisbank.com"
WORKSPACE_ID = "axisbank-com-defau-3b9581"

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "C0AE46B861X")
ALERT_TYPE = "CC ALLOCATION SCHEDULING"


def _load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def send_slack_alert(
    message, file_path=None, error_details=None, is_success=False,
    alert_type=ALERT_TYPE,
):
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
    payload = {"channel": SLACK_CHANNEL_ID, "text": alert_text}

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


def validate_allocation_data(df, file_path=None):
    """
    Validate transformed allocation DataFrame.
    Returns (is_valid, error_messages_list).
    Sends Slack alerts for each error found.
    """
    errors = []

    required = ["user_identifier", "user_mobile_number", "risk_segment"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        for err in errors:
            send_slack_alert(err, file_path=file_path)
        return False, errors

    uid = df["user_identifier"].astype(str).str.strip()
    empty_ids = uid.isin(["", "nan", "None"]) | df["user_identifier"].isna()
    if empty_ids.all():
        errors.append("All user_identifier values are empty/NaN")
    elif empty_ids.sum() > 0:
        pct = empty_ids.sum() / len(df) * 100
        errors.append(
            f"{empty_ids.sum()}/{len(df)} ({pct:.1f}%) rows have "
            f"empty/NaN user_identifier"
        )

    valid_phones = df["user_mobile_number"].notna() & (
        df["user_mobile_number"].astype(str).str.strip() != ""
    )
    if valid_phones.sum() == 0:
        errors.append("No valid phone numbers found in user_mobile_number column")

    valid_risk = df["risk_segment"].notna() & (
        df["risk_segment"].astype(str).str.strip().str.len() > 0
    )
    if valid_risk.sum() == 0:
        errors.append("No valid risk_segment values found")

    if errors:
        for err in errors:
            send_slack_alert(err, file_path=file_path)
        return False, errors

    return True, []


def extract_cycle_date(df):
    """
    Parse CYCLEDATE from the DataFrame, compute end_timestamp as
    cycle_date + 1 month at 19:30 IST, returned as UTC ISO string.
    """
    if "CYCLEDATE" not in df.columns:
        raise ValueError("CYCLEDATE column not found in data")

    cycle_val = None
    for val in df["CYCLEDATE"]:
        if pd.notna(val) and str(val).strip():
            cycle_val = str(val).strip()
            break

    if cycle_val is None:
        raise ValueError("No non-null CYCLEDATE value found")

    cycle_date = None
    for fmt in [
        "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d",
        "%d-%b-%Y", "%d %b %Y",
    ]:
        try:
            cycle_date = datetime.strptime(cycle_val.split()[0], fmt)
            break
        except (ValueError, TypeError):
            continue

    if cycle_date is None:
        try:
            cycle_date = pd.to_datetime(cycle_val).to_pydatetime()
        except Exception:
            raise ValueError(f"Could not parse CYCLEDATE value: {cycle_val}")

    end_date = cycle_date + relativedelta(months=1)
    end_dt_ist = IST.localize(
        end_date.replace(hour=19, minute=30, second=0, microsecond=0)
    )
    end_dt_utc = end_dt_ist.astimezone(pytz.utc)
    return end_dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _build_transformation_json(config):
    """Build the cohort transformation JSON from config (excluding scheduling)."""
    transform = {}
    for key in ("phone_number", "app_overrides", "workflow_variables"):
        if key in config:
            transform[key] = config[key]
    return json.dumps(transform).encode("utf-8")


async def create_cohort(
    access_token, cohort_csv_path, cohort_name, workflow_id, workflow_version,
    config=None,
):
    """POST multipart form to /cohorts endpoint. Returns cohort_id."""
    url = (
        f"{SCHEDULING_API_BASE}/orgs/{ORG_ID}/workspaces/{WORKSPACE_ID}"
        f"/cohorts?workflow_id={workflow_id}&workflow_version={workflow_version}"
    )
    headers = {"Authorization": f"Bearer {access_token}"}

    transformation_bytes = _build_transformation_json(config or {})

    with open(cohort_csv_path, "rb") as f:
        files = {
            "cohort_file": (os.path.basename(cohort_csv_path), f, "text/csv"),
            "cohort_transformation_file": ("transformation.json", transformation_bytes, "application/json"),
        }
        data = {"name": cohort_name}

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                url, headers=headers, data=data, files=files, timeout=120.0,
            )
            if resp.status_code >= 400:
                print(f"[Cohort] Error {resp.status_code}: {resp.text}")
            resp.raise_for_status()
            result = resp.json()

    cohort_id = result.get("cohort_id") or result.get("id")
    print(f"[Cohort] Created: {cohort_name} -> {cohort_id}")
    return cohort_id


async def create_campaign(
    access_token, campaign_name, workflow_id, workflow_version,
    cohort_id, end_ts, allowed_schedule,
):
    """POST JSON to /campaigns endpoint. Returns campaign response dict."""
    url = (
        f"{SCHEDULING_API_BASE}/orgs/{ORG_ID}/workspaces/{WORKSPACE_ID}"
        f"/campaigns"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "name": campaign_name,
        "description": "",
        "app_id": workflow_id,
        "app_version": workflow_version,
        "cohort_id": cohort_id,
        "start_timestamp": ((datetime.now(IST) + timedelta(minutes=5)).astimezone(pytz.utc)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "end_timestamp": end_ts,
        "allowed_schedule": allowed_schedule,
    }

    print(f"[Campaign] Payload: {json.dumps(payload, indent=2)}")
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url, headers=headers, json=payload, timeout=60.0,
        )
        if resp.status_code >= 400:
            print(f"[Campaign] Error {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        result = resp.json()

    campaign_id = (
        result.get("campaign_id") or result.get("id") or result.get("name")
    )
    print(f"[Campaign] Created: {campaign_name} -> {campaign_id}")
    return result


def _enumerate_cohort_files(df, base_output_path):
    """
    After generate_risk_cohorts has written files, enumerate the paths that
    were actually created on disk.
    Returns list of (risk_level, bank, file_path, record_count) tuples.
    """
    base_name = (
        base_output_path.rsplit(".", 1)[0]
        if "." in base_output_path
        else base_output_path
    )
    ext = (
        base_output_path.rsplit(".", 1)[1]
        if "." in base_output_path
        else "csv"
    )

    risk_levels = ["L", "M", "H"]
    has_bank = "bank_name" in df.columns
    banks = sorted(df["bank_name"].unique()) if has_bank else [None]

    results = []
    for risk in risk_levels:
        for bank in banks:
            if bank is not None:
                path = f"{base_name}_{bank}_cohort_{risk}.{ext}"
            else:
                path = f"{base_name}_cohort_{risk}.{ext}"

            if os.path.exists(path) and os.path.getsize(path) > 0:
                count = sum(1 for _ in open(path)) - 1  # minus header
                if count > 0:
                    results.append((risk, bank, path, count))

    return results


RISK_LABEL_MAP = {"L": "LowRisk", "M": "MedRisk", "H": "HighRisk"}


def _build_campaign_name(bank, risk_level, filename):
    """Build campaign name like cc-axis-march-cyc15-HighRisk."""
    bank_str = bank.lower() if bank else "all"
    risk_str = RISK_LABEL_MAP.get(risk_level, risk_level)

    cycle_match = re.search(r"[Cc]ycle\s*(\d+)", filename)
    cycle_str = f"cyc{cycle_match.group(1)}" if cycle_match else "cyc0"

    month_match = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*",
        filename, re.IGNORECASE,
    )
    month_str = month_match.group(0).lower() if month_match else "unknown"

    return f"cc-{bank_str}-{month_str}-{cycle_str}-{risk_str}"


async def process(allocation_file_path, password=None):
    """
    Main orchestrator:
    transform -> validate -> generate cohorts -> create cohort API ->
    schedule campaign API -> blob upload -> Slack summary.
    """
    config = _load_config()
    scheduling = config.get("scheduling", {})
    risk_workflow_map = scheduling.get("risk_workflow_map", {})
    allowed_schedule = scheduling.get("allowed_schedule", {
        "allowed_start_time": "10:00",
        "allowed_end_time": "19:00",
        "allowed_days": [
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        ],
    })

    filename = os.path.basename(allocation_file_path)
    base_name = os.path.splitext(filename)[0]
    output_dir = os.path.join(BASE_DIR, "temp_buffer")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{base_name}_transformed.csv")

    print(f"\n{'='*80}")
    print(f"CC ALLOCATION SCHEDULING: {filename}")
    print(f"{'='*80}")

    # Step 1: Transform
    try:
        df = transform_cc_allocation_file(
            allocation_file_path, output_file, password=password,
        )
    except Exception as e:
        send_slack_alert(
            f"Transform failed: {e}",
            file_path=filename,
            error_details=traceback.format_exc(),
        )
        raise

    # Step 2: Validate
    is_valid, errors = validate_allocation_data(df, file_path=filename)
    if not is_valid:
        raise ValueError(
            f"Validation failed for {filename}: {'; '.join(errors)}"
        )

    # Step 3: Extract cycle date -> compute campaign end timestamp
    try:
        end_timestamp = extract_cycle_date(df)
    except Exception as e:
        send_slack_alert(
            f"Failed to extract CYCLEDATE: {e}", file_path=filename,
        )
        raise

    # Step 4: Generate risk x bank cohort files
    generate_risk_cohorts(df, output_file)
    cohort_files = _enumerate_cohort_files(df, output_file)

    if not cohort_files:
        send_slack_alert(
            "No cohort files generated after splitting", file_path=filename,
        )
        raise ValueError("No cohort files generated")

    print(f"\nFound {len(cohort_files)} cohort files to schedule")

    # Step 5: Authenticate
    access_token = await get_axis_access_token()

    # Step 6: For each cohort file -> create cohort + campaign + blob upload
    scheduled = []
    for risk_level, bank, cohort_path, record_count in cohort_files:
        wf_config = risk_workflow_map.get(risk_level)
        if not wf_config:
            send_slack_alert(
                f"No workflow mapping for risk level '{risk_level}', skipping",
                file_path=filename,
            )
            continue

        workflow_id = wf_config["workflow_id"]
        workflow_version = wf_config["version"]
        cohort_name = os.path.splitext(os.path.basename(cohort_path))[0]
        bank_label = bank.lower() if bank else "all"
        campaign_name = _build_campaign_name(bank, risk_level, filename)

        try:
            cohort_id = await create_cohort(
                access_token, cohort_path, cohort_name,
                workflow_id, workflow_version, config=config,
            )

            await create_campaign(
                access_token, campaign_name, workflow_id, workflow_version,
                cohort_id, end_timestamp, allowed_schedule,
            )

            blob_filename = os.path.basename(cohort_path)
            upload_to_blob(
                cohort_path, "allocation_file", "cc", filename=blob_filename,
            )

            scheduled.append({
                "risk": risk_level,
                "bank": bank_label,
                "records": record_count,
                "cohort_id": cohort_id,
                "campaign": campaign_name,
            })

        except Exception as e:
            print(f"[ERROR] Failed to schedule {bank_label}-{risk_level}: {e}")
            traceback.print_exc()
            send_slack_alert(
                f"Failed to schedule {bank_label}-{risk_level} "
                f"({record_count} records): {e}",
                file_path=filename,
                error_details=traceback.format_exc(),
            )

    # Upload original transformed file to blob
    upload_to_blob(
        output_file, "allocation_file", "cc",
        filename=os.path.basename(output_file),
    )

    # Step 7: Slack summary
    if scheduled:
        lines = [f"Scheduled {len(scheduled)} campaigns from `{filename}`:"]
        for s in scheduled:
            lines.append(
                f"  - {s['bank']}-{s['risk']}: {s['records']} records, "
                f"cohort={s['cohort_id']}, campaign={s['campaign']}"
            )
        send_slack_alert("\n".join(lines), is_success=True)
    else:
        send_slack_alert(
            "No campaigns were scheduled (all cohorts failed)",
            file_path=filename,
        )

    # Cleanup local cohort files
    for _, _, cohort_path, _ in cohort_files:
        try:
            os.remove(cohort_path)
        except OSError:
            pass
    try:
        os.remove(output_file)
    except OSError:
        pass

    return scheduled


async def main():
    """Manual execution entry point for testing."""
    test_file = os.path.join(
        BASE_DIR, "data", "allocation_file",
        "Post due date Cards Cycle 1 Allocation Mar'2026 "
        "(Sarvam)_transformed_axis_cohort_H.csv",
    )

    if not os.path.exists(test_file):
        print(f"Error: Test file not found: {test_file}")
        return

    result = await process(test_file)
    print(f"\nScheduling complete. {len(result)} campaigns scheduled.")


if __name__ == "__main__":
    asyncio.run(main())
