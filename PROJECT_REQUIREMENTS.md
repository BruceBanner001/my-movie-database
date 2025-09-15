<!--
========================================================================
PROJECT_REQUIREMENTS.md
Purpose: Project-wide requirements & mapping for the Excel → JSON automation.
This header block documents provenance and usage instructions.
========================================================================
-->
# 📑 Requirements Report – Excel → JSON Automation (UPDATED)

## Purpose
Automate conversion of a private Excel (Google Drive) into `seriesData.json` for the frontend,
with images, synopsis, duration, update tracking and backups. Includes:
- caching (skip re-fetch when already present),
- chunked runs (limit items per workflow),
- scheduled weekly "betterment" attempts,
- manual updates via an Excel sheet,
- mobile-friendly run reports and old-image retention.

--- 

(remaining content preserved)

## Inputs & Outputs
- Input: Private Excel uploaded to Google Drive (file id written to `EXCEL_FILE_ID.txt`).
- Output: `seriesData.json` (main dataset).
- Supporting artifacts:
  - `images/` (current images)
  - `old-images/` (previous images kept for `KEEP_OLD_IMAGES_DAYS`)
  - `backups/` (changed/deleted objects per run)
  - `reports/` (plain-text run reports: `report_DDMMYYYY_HHMM.txt`)

## Key Files & Environment
- `EXCEL_FILE_ID.txt` — contains Google Drive file id (workflow writes this).
- `GDRIVE_SERVICE_ACCOUNT.json` — service account JSON secret (workflow writes this).
- `create_update_backup_delete.py` — main script (the updated version).
- `seriesData.json` — output JSON.
- Env vars (set in GitHub Actions `env:`):
  - `MAX_PER_RUN` (integer) — 0 or unset = process all; otherwise process only this many shows per run (easy throttle).
  - `SCHEDULED_RUN` (`true`/`false`) — set to `true` for scheduled weekly runs **only**. When `true` the script will attempt to find *better* images/synopses even if values already exist in the JSON.
  - `KEEP_OLD_IMAGES_DAYS` (integer, default 7) — how long to keep old images in `old-images/` before auto-delete.
