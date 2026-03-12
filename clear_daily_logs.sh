#!/usr/bin/env bash
# Clear processed file logs at midnight IST so each day starts fresh.
LOG_DIR="/home/sarvam/axis/logs"

> "$LOG_DIR/axis_processed_files.log"
> "$LOG_DIR/paid_file_ingestion.log"
> "$LOG_DIR/al_paid_file_ingestion.log"
> "$LOG_DIR/cc_paid_file_ingestion.log"
> "$LOG_DIR/axis_cron.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Daily log cleanup complete" >> "$LOG_DIR/axis_cron.log"
