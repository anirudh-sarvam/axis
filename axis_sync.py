#!/usr/bin/env python3
"""
AXIS SFTP Sync

Scans SFTP recursively, classifies files by pattern, and routes them:
  - Paid files (PL/CC/AL) → download + Slack + async ingestion
  - Pullback/Lien files (PL/CC) → download + Slack + blob + resolve via API
  - Allocation files (PL/CC/AL) → download + Slack + blob upload + cleanup
  - OOO files (POOO/NPA/NoCredit) → download + Slack + blob upload + trigger pipeline
  - Regular files → download + Slack + cleanup
"""

import subprocess
import os
import sys
import shutil
import json
import urllib.request
import urllib.parse
import time
import asyncio
import re
import fcntl
import tempfile
from datetime import datetime
import pytz
import importlib.util
from dotenv import load_dotenv

load_dotenv()

from scripts.paid_file_ingestion import (
    build_file_pattern,
    build_pl_allocation_pattern,
    is_file_modified_today, 
    log_downloaded_file,
    get_downloaded_files,
    send_slack_alert,
    PAID_FILE_DIR,
    main as process_paid_files,
)
from scripts.cc_paid_file_ingestion import (
    build_cc_file_pattern,
    build_cc_allocation_pattern,
    get_cc_downloaded_files,
    CC_PAID_FILE_DIR,
    CC_ALLOCATION_DIR,
    main as process_cc_paid_files,
)
from scripts.al_paid_file_ingestion import (
    build_al_file_pattern,
    build_al_allocation_pattern,
    get_al_downloaded_files,
    AL_PAID_FILE_DIR,
    main as process_al_paid_files,
)
from blob_utils import upload_to_blob

# Dynamic import of OOO blob utils
sys.path.insert(0, "/home/sarvam/ooo-automation")
_ooo_blob_spec = importlib.util.spec_from_file_location(
    "ooo_blob_utils", "/home/sarvam/ooo-automation/blob_utils.py"
)
_ooo_blob_mod = importlib.util.module_from_spec(_ooo_blob_spec)
_ooo_blob_spec.loader.exec_module(_ooo_blob_mod)
upload_ooo_allocation = _ooo_blob_mod.upload_ooo_allocation

IST = pytz.timezone("Asia/Kolkata")
SFTP_HOST = "s2fs.axisbank.com"
SFTP_USER = "sarvam"
SFTP_PASS = os.environ["SFTP_PASS"]
BASE_DIR = "/home/sarvam/axis"
LOCAL_BUFFER = BASE_DIR + "/temp_buffer"
LOG_FILE = BASE_DIR + "/logs/axis_processed_files.log"
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "C0ADMKT2WDT")
OOO_BASE_DIR = "/home/sarvam/ooo-automation"
OOO_LOG_DIR = OOO_BASE_DIR + "/logs"

OOO_PIPELINES = {
    "pooo": {
        "local_dir": OOO_BASE_DIR + "/pooo",
        "config_file": f"{OOO_BASE_DIR}/pooo/config.toml",
        "config_section": "filter",
        "pipeline_cmd": "cd " + OOO_BASE_DIR + " && .venv/bin/python pooo/main.py --today",
    },
    "npa": {
        "local_dir": OOO_BASE_DIR + "/npa",
        "config_file": f"{OOO_BASE_DIR}/npa/config.toml",
        "config_section": "npa",
        "pipeline_cmd": "cd " + OOO_BASE_DIR + " && .venv/bin/python npa/main.py --today",
    },
    "no-credit": {
        "local_dir": OOO_BASE_DIR + "/nocredit",
        "config_file": f"{OOO_BASE_DIR}/nocredit/config.toml",
        "config_section": "nocredit",
        "pipeline_cmd": "cd " + OOO_BASE_DIR + " && .venv/bin/python nocredit/main.py --today",
    },
}


def _now():
    """Return datetime.now(IST).replace(tzinfo=None)."""
    return datetime.now(IST).replace(tzinfo=None)


def _load_ooo_canonical_filenames():
    """Read master_file paths from OOO config.toml files to get canonical filenames."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            tomllib = None

    if tomllib is None:
        return

    for ooo_type, cfg in OOO_PIPELINES.items():
        config_path = cfg["config_file"]
        section = cfg["config_section"]
        if not os.path.exists(config_path):
            continue
        try:
            with open(config_path, "rb") as f:
                config = tomllib.load(f)
        except Exception:
            continue
        section_data = config.get(section, {})
        master_file = section_data.get("master_file", "")
        master_file_password = section_data.get("master_file_password", "")
        if master_file:
            canonical_filename = os.path.basename(master_file)
            cfg["canonical_filename"] = canonical_filename
            cfg["master_file_password"] = master_file_password


def filename_contains_today_date(name):
    """Check if filename has today's day number as standalone number."""
    now = _now()
    day_str = str(now.day)
    pattern = r"\b" + re.escape(day_str) + r"\b"
    return bool(re.search(pattern, name))


def append_to_log(filepath, entry):
    """Append entry to log file with fcntl file locking."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "a") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.write(entry + "\n")
                f.flush()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception as e:
        print(f"[log] Failed to append to {filepath}: {e}")


def upload_to_slack(file_path, filename):
    """Upload file to Slack using V2 API (getUploadURLExternal -> upload -> completeUploadExternal)."""
    if not os.path.exists(file_path):
        print(f"[Slack] File not found: {file_path}")
        return False

    file_size = os.path.getsize(file_path)
    if file_size == 0:
        print(f"[Slack] File is empty: {file_path}")
        return False

    # Step 1: getUploadURLExternal
    url1 = "https://slack.com/api/files.getUploadURLExternal"
    data1 = urllib.parse.urlencode({
        "filename": filename,
        "length": file_size,
    }).encode("utf-8")
    req1 = urllib.request.Request(
        url1,
        data=data1,
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req1, timeout=30) as resp1:
            result1 = json.loads(resp1.read().decode())
    except Exception as e:
        print(f"[Slack] getUploadURLExternal failed: {e}")
        return False

    if not result1.get("ok"):
        print(f"[Slack] getUploadURLExternal error: {result1.get('error', 'unknown')}")
        return False

    upload_url = result1.get("upload_url")
    file_id = result1.get("file_id")
    if not upload_url or not file_id:
        print("[Slack] Missing upload_url or file_id")
        return False

    # Step 2: POST file to upload_url
    try:
        with open(file_path, "rb") as f:
            file_data = f.read()
        req2 = urllib.request.Request(
            upload_url,
            data=file_data,
            headers={"Content-Type": "application/octet-stream"},
            method="POST",
        )
        with urllib.request.urlopen(req2, timeout=120) as resp2:
            if resp2.status != 200:
                print(f"[Slack] Upload failed with status {resp2.status}")
                return False
    except Exception as e:
        print(f"[Slack] Upload to URL failed: {e}")
        return False

    # Step 3: completeUploadExternal
    url3 = "https://slack.com/api/files.completeUploadExternal"
    payload3 = {
        "files": [{"id": file_id, "title": filename}],
                "channel_id": SLACK_CHANNEL_ID,
        "initial_comment": "AXIS Bank SFTP Sync",
    }
    req3 = urllib.request.Request(
        url3,
        data=json.dumps(payload3).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
                "Content-Type": "application/json",
            },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req3, timeout=30) as resp3:
            result3 = json.loads(resp3.read().decode())
    except Exception as e:
        print(f"[Slack] completeUploadExternal failed: {e}")
        return False

    if not result3.get("ok"):
        print(f"[Slack] completeUploadExternal error: {result3.get('error', 'unknown')}")
        return False

    print(f"✅ Uploaded to Slack: {filename}")
    return True


def escape_sftp_path(path):
    """Escape special characters for SFTP batch file arguments (paths are double-quoted)."""
    return path.replace("\\", "\\\\").replace('"', '\\"')


def download_file_from_sftp(remote_path, remote_name, local_path, current_dir):
    """Download using sshpass+sftp batch. Returns True/False."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    esc_dir = escape_sftp_path(current_dir)
    esc_name = escape_sftp_path(remote_name)
    esc_local = escape_sftp_path(local_path)
    batch_content = f'cd "{esc_dir}"\nget "{esc_name}" "{esc_local}"\nbye\n'
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as bf:
        bf.write(batch_content)
        batch_path = bf.name
    try:
        result = subprocess.run(
            f"sshpass -p '{SFTP_PASS}' sftp -oBatchMode=no -oStrictHostKeyChecking=no -oHostKeyAlgorithms=+ssh-rsa -b {batch_path} {SFTP_USER}@{SFTP_HOST}",
            shell=True,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=BASE_DIR,
        )
        return result.returncode == 0
    except Exception as e:
        print(f"[SFTP] Download failed for {remote_name}: {e}")
        return False
    finally:
        try:
            os.unlink(batch_path)
        except OSError:
            pass


def run_ooo_pipeline(ooo_type, name):
    """Trigger OOO pipeline with --today in background subprocess. Skip after 6 PM IST."""
    now = _now()
    if now.hour >= 18:
        send_slack_alert(
            f"OOO pipeline {ooo_type} skipped (after 6 PM IST): {name}",
            alert_type="OOO PIPELINE",
        )
        return
    cfg = OOO_PIPELINES.get(ooo_type, {})
    pipeline_cmd = cfg.get("pipeline_cmd", "")
    if not pipeline_cmd:
        return
    try:
        subprocess.Popen(
            pipeline_cmd,
            shell=True,
            cwd=OOO_BASE_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        send_slack_alert(
            f"Failed to start OOO pipeline {ooo_type}: {e}",
            file_path=name,
            alert_type="OOO PIPELINE",
        )


def build_file_rules(patterns, histories):
    """Build ordered list of classification rules.
    Order matters: CC Lien before CC paid, CC Pullback before CC allocation.
    """
    pl_paid_pat = patterns.get("pl_paid") or build_file_pattern()[0]
    cc_paid_pat = patterns.get("cc_paid") or build_cc_file_pattern()[0]
    cc_alloc_pat = patterns.get("cc_alloc") or build_cc_allocation_pattern()[0]
    al_paid_pat = patterns.get("al_paid") or build_al_file_pattern()[0]
    pl_alloc_pat = patterns.get("pl_alloc") or build_pl_allocation_pattern()[0]
    al_alloc_pat = patterns.get("al_alloc") or build_al_allocation_pattern()[0]

    cc_lien_pat = re.compile(
        r"Post\s+D(?:ue|ye)\s+Date\s+Cards\s+Lien\s+Status.*\.(xlsx|xls|csv)", re.IGNORECASE
    )
    cc_pullback_pat = re.compile(
        r"Post\s+D(?:ue|ye)\s+Date\s+(?:Cards?\s+)?Cycle\s+\d+.*(?:Restriction|Pullback)\s+Accounts.*\.(xlsx|xls|csv)",
        re.IGNORECASE,
    )
    pl_pullback_pat = re.compile(
        r"PL\s+RX\s+Exclude\s+referred\s+accounts.*\.(xlsx|xls|csv)", re.IGNORECASE
    )

    rules = [
        {"pattern": cc_lien_pat, "handler": "pullback", "product": "cc", "pullback_type": "cc_lien"},
        {"pattern": cc_pullback_pat, "handler": "pullback", "product": "cc", "pullback_type": "cc_pullback"},
        {"pattern": al_paid_pat, "handler": "collect", "product": "al", "history": histories.get("al", set())},
        {"pattern": pl_paid_pat, "handler": "collect", "product": "pl", "history": histories.get("pl", set())},
        {"pattern": cc_paid_pat, "handler": "collect", "product": "cc", "history": histories.get("cc", set())},
        {"pattern": cc_alloc_pat, "handler": "allocation", "product": "cc"},
        {"pattern": pl_alloc_pat, "handler": "allocation", "product": "pl"},
        {"pattern": al_alloc_pat, "handler": "allocation", "product": "al"},
        {"pattern": pl_pullback_pat, "handler": "pullback", "product": "pl", "pullback_type": "pl_pullback"},
        {
            "pattern": re.compile(r"SARVAM[_ ]+POOO[_ ]+Calling", re.IGNORECASE),
            "handler": "ooo",
            "ooo_type": "pooo",
        },
        {
            "pattern": re.compile(r"SARVAM[_ ]+OOO[_ ]+NPA[_ ]+Allocation", re.IGNORECASE),
            "handler": "ooo",
            "ooo_type": "npa",
        },
        {
            "pattern": re.compile(r"SARVAM[_ ]+No[_ ]+Credit[_ ]+Allocation", re.IGNORECASE),
            "handler": "ooo",
            "ooo_type": "no-credit",
        },
    ]
    return rules


def classify_file(name, full_remote_path, rules, general_history):
    if full_remote_path in general_history:
        return None
    for rule in rules:
        pat = rule.get("pattern")
        if pat is None:
            continue
        if not pat.search(name):
            continue
        if rule.get("handler") == "collect":
            hist = rule.get("history", set())
            if full_remote_path in hist:
                return None
        return rule
    return "regular"


async def process_pullback_files(pullback_files):
    """Process all collected pullback/lien files: resolve via API, upload CSVs to blob, alert Slack."""
    from scripts.handle_pl_pullback import process as process_pl_pullback
    from scripts.handle_cc_pullback import process as process_cc_pullback
    from scripts.handle_cc_lien import process as process_cc_lien

    output_dir = os.path.join(BASE_DIR, "temp_buffer", "pullback_output")

    for local_path, name, full_remote_path, rule in pullback_files:
        product = rule.get("product", "pl")
        pullback_type = rule.get("pullback_type", "pl_pullback")

        try:
            if pullback_type == "pl_pullback":
                success, csv_path, stats = await process_pl_pullback(local_path, output_dir)
            elif pullback_type == "cc_pullback":
                success, csv_path, stats = await process_cc_pullback(local_path, output_dir)
            elif pullback_type == "cc_lien":
                success, csv_path, stats = await process_cc_lien(local_path, output_dir)
            else:
                print(f"[Pullback] Unknown pullback_type: {pullback_type}")
                continue

            if csv_path and os.path.exists(csv_path):
                csv_name = os.path.basename(csv_path)
                upload_to_blob(csv_path, "pullback_file", product, filename=csv_name)

            status_str = "SUCCESS" if success else "COMPLETED (check status)"
            msg = (
                f"*{pullback_type.upper().replace('_', ' ')}*\n"
                f"File: `{name}`\n"
                f"Product: {product.upper()}\n"
                f"Total accounts: {stats.get('total_accounts', '?')}\n"
                f"Unique resolved: {stats.get('unique_resolved', '?')}\n"
                f"API Upload: {status_str}"
            )
            send_slack_alert(msg, alert_type="PULLBACK")

        except Exception as e:
            send_slack_alert(
                f"Pullback processing failed for {name}: {e}",
                alert_type="PULLBACK ERROR",
            )
            print(f"[Pullback] Error processing {name}: {e}")

        finally:
            try:
                os.remove(local_path)
            except OSError:
                pass


def handle_file(rule, name, full_remote_path, current_dir, general_history, collected_files, pullback_collected):
    """Download classified file, route to handler."""
    local_path = os.path.join(LOCAL_BUFFER, name.replace("/", "_"))
    if not download_file_from_sftp(full_remote_path, name, local_path, current_dir):
        return
    if rule.get("handler") == "pullback":
        upload_to_slack(local_path, name)
        blob_path = upload_to_blob(local_path, "pullback_file", rule.get("product", "pl"), filename=name)
        if blob_path:
            print(f"[Pullback] Original file uploaded to blob: {blob_path}")
        pullback_collected.append((local_path, name, full_remote_path, rule))
        append_to_log(LOG_FILE, full_remote_path)
        return
    if rule.get("handler") == "collect":
        upload_to_slack(local_path, name)
        collected_files.append((local_path, name, full_remote_path))
        return
    if rule.get("handler") == "allocation":
        product = rule.get("product", "pl")
        blob_path = upload_to_blob(local_path, "allocation_file", product, filename=name)
        if blob_path:
            upload_to_slack(local_path, name)
        try:
            os.remove(local_path)
        except OSError:
            pass
        append_to_log(LOG_FILE, full_remote_path)
        return
    if rule.get("handler") == "ooo":
        ooo_type = rule.get("ooo_type", "pooo")
        cfg = OOO_PIPELINES.get(ooo_type, {})
        canonical_filename = cfg.get("canonical_filename", name)
        local_dir = cfg.get("local_dir", OOO_BASE_DIR)
        dest_path = os.path.join(local_dir, canonical_filename)
        try:
            shutil.copy2(local_path, dest_path)
        except Exception as e:
            send_slack_alert(f"Failed to copy OOO file: {e}", file_path=name, alert_type="OOO")
            return
        password = cfg.get("master_file_password") or None
        try:
            upload_ooo_allocation(dest_path, ooo_type, filename=canonical_filename, password=password)
        except Exception as e:
            send_slack_alert(f"OOO blob upload failed: {e}", file_path=name, alert_type="OOO")
        upload_to_slack(local_path, name)
        try:
            os.remove(local_path)
        except OSError:
            pass
        append_to_log(LOG_FILE, full_remote_path)
        run_ooo_pipeline(ooo_type, name)
        return


def handle_regular_file(name, full_remote_path, current_dir, general_history):
    """Download -> Slack upload -> cleanup."""
    local_path = os.path.join(LOCAL_BUFFER, name.replace("/", "_"))
    if not download_file_from_sftp(full_remote_path, name, local_path, current_dir):
        return
    upload_to_slack(local_path, name)
    try:
        os.remove(local_path)
    except OSError:
        pass
    append_to_log(LOG_FILE, full_remote_path)


def recursive_scan(current_dir, general_history, rules, collected_files, pullback_collected):
    """List SFTP directory via sshpass, parse ls -lt output, classify each file."""
    lines = []
    for attempt in range(3):
        try:
            esc_dir = escape_sftp_path(current_dir)
            batch_content = f'cd "{esc_dir}"\nls -lt\nbye\n'
            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as bf:
                bf.write(batch_content)
                batch_path = bf.name
            try:
                result = subprocess.run(
                    f"sshpass -p '{SFTP_PASS}' sftp -oBatchMode=no -oStrictHostKeyChecking=no -oHostKeyAlgorithms=+ssh-rsa -b {batch_path} {SFTP_USER}@{SFTP_HOST}",
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    cwd=BASE_DIR,
                )
            finally:
                try:
                    os.unlink(batch_path)
                except OSError:
                    pass
            if result.returncode != 0:
                raise RuntimeError(result.stderr or "sftp failed")
            lines = result.stdout.strip().split("\n")
            break
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2)

    now = _now()
    for line in lines:
        line = line.strip()
        if not line or line.startswith("sftp>") or "total" in line.lower():
            continue
        parts = line.split()
        if len(parts) < 9:
            continue
        perms, links, owner, group, size, month, day, time_or_year = parts[:8]
        name = " ".join(parts[8:])
        if name in (".", ".."):
            continue

        is_directory = perms.startswith("d")
        is_today = is_file_modified_today(month, day, time_or_year)

        if is_directory:
            if is_today:
                subdir = f"{current_dir}/{name}".replace("//", "/")
                try:
                    recursive_scan(subdir, general_history, rules, collected_files, pullback_collected)
                except Exception as e:
                    print(f"[SFTP] Failed to scan subdirectory {subdir}: {e}")
            continue

        full_remote_path = f"{current_dir}/{name}".replace("//", "/")

        if not is_today:
            continue

        rule = classify_file(name, full_remote_path, rules, general_history)
        if rule is None:
            continue
        if rule == "regular":
            handle_regular_file(name, full_remote_path, current_dir, general_history)
        else:
            handle_file(rule, name, full_remote_path, current_dir, general_history, collected_files, pullback_collected)


def load_history():
    """Load set from axis_processed_files.log."""
    if not os.path.exists(LOG_FILE):
        return set()
    try:
        with open(LOG_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except Exception:
        return set()


async def main():
    """Main entry: build patterns, load histories, scan, process collected paid files."""
    lock_fd = open("/tmp/axis_sync.lock", "w")
    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        print("Another axis_sync process is running, exiting")
        sys.exit(0)

    os.makedirs(LOCAL_BUFFER, exist_ok=True)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    patterns = {}
    histories = {
        "pl": get_downloaded_files(),
        "cc": get_cc_downloaded_files(),
        "al": get_al_downloaded_files(),
    }
    rules = build_file_rules(patterns, histories)
    general_history = load_history()
    collected_files = []
    pullback_collected = []

    _load_ooo_canonical_filenames()

    try:
        recursive_scan(".", general_history, rules, collected_files, pullback_collected)
    except Exception as e:
        err_str = str(e)
        if "kex_exchange_identification" not in err_str and "Connection reset" not in err_str:
            send_slack_alert(f"SFTP scan failed: {e}", alert_type="AXIS SYNC")
        raise

    # Process collected paid files
    pl_list = [(lp, rn, frp) for lp, rn, frp in collected_files if build_file_pattern()[0].search(rn)]
    cc_list = [(lp, rn, frp) for lp, rn, frp in collected_files if build_cc_file_pattern()[0].search(rn)]
    al_list = [(lp, rn, frp) for lp, rn, frp in collected_files if build_al_file_pattern()[0].search(rn)]

    if pl_list:
        await process_paid_files(pl_list)
    if cc_list:
        await process_cc_paid_files(cc_list)
    if al_list:
        await process_al_paid_files(al_list)

    if pullback_collected:
        await process_pullback_files(pullback_collected)


if __name__ == "__main__":
    asyncio.run(main())
