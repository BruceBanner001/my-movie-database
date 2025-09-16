# Excel → JSON Automation

This repository automates converting an Excel file stored on Google Drive into `seriesData.json`.
The workflow is designed to **always download the Excel from Google Drive** using a service account (no local fallback).

Key points:
- The GitHub Actions workflow writes two files from secrets:
  - `EXCEL_FILE_ID.txt` — the Google Drive file ID for the Excel workbook.
  - `GDRIVE_SERVICE_ACCOUNT.json` — the service account JSON key.

- The service account email (found in the JSON under `client_email`) **must be granted Viewer access** to the Excel file in Google Drive.

- The script `create_update_backup_delete.py` downloads the sheet to `downloaded-data.xlsx`, processes configured sheets, writes `seriesData.json`, produces reports under `reports/`, and commits changes back to the repository (if any).

Usage (locally for testing):
```bash
python create_update_backup_delete.py
```
(Locally you must still provide `EXCEL_FILE_ID.txt` and `GDRIVE_SERVICE_ACCOUNT.json` in the repo root if you want to run it.)

See `.github/workflows/update.yml` for the CI workflow configuration.
