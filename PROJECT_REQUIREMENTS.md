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

## Inputs & Outputs
- Input: Private Excel uploaded to Google Drive (file id written to `EXCEL_FILE_ID.txt`).
- Output: `seriesData.json` (main dataset).
- Supporting artifacts:
  - `images/` (current images)
  - `old-images/` (previous images kept for `KEEP_OLD_IMAGES_DAYS`)
  - `backups/` (changed/deleted objects per run)
  - `reports/` (plain-text run reports: `report_DDMMYYYY_HHMM.txt`)

---

## Key Files & Environment
- `EXCEL_FILE_ID.txt` — contains Google Drive file id (workflow writes this).
- `GDRIVE_SERVICE_ACCOUNT.json` — service account JSON secret (workflow writes this).
- `create_update_backup_delete_improved.py` — main script (the updated version).
- `seriesData.json` — output JSON.
- Env vars (set in GitHub Actions `env:`):
  - `MAX_PER_RUN` (integer) — 0 or unset = process all; otherwise process only this many shows per run (easy throttle).
  - `SCHEDULED_RUN` (`true`/`false`) — set to `true` for scheduled weekly runs **only**. When `true` the script will attempt to find *better* images/synopses even if values already exist in the JSON.
  - `KEEP_OLD_IMAGES_DAYS` (integer, default 7) — how long to keep old images in `old-images/` before auto-delete.
- Secret names (used by workflow):
  - `GDRIVE_SERVICE_ACCOUNT` (write to `GDRIVE_SERVICE_ACCOUNT.json`)
  - SMTP secrets for notifications if used: `SMTP_USERNAME`, `SMTP_PASSWORD`, `NOTIFY_EMAIL`

---

## Excel → JSON Mapping (fields)
The script maps Excel columns (case-insensitive trimmed column names) to the following JSON schema:

- `no` → `showID` (plus sheet offset: e.g. 1000/2000/3000)
- `series title` → `showName`
- `started date` → `watchStartedOn` (DD-MM-YYYY)
- `finished date` → `watchEndedOn` (DD-MM-YYYY)
- `year` → `releasedYear` (int)
- `total episodes` → `totalEpisodes` (int)
- `original language` → `nativeLanguage` (Capitalized)
- `language` → `watchedLanguage` (Capitalized)
- `comments` → `comments` (cleaned; words capitalized; ends with a dot)
- `ratings` → `ratings` (int, default 0)
- `catagory`/`category` → `genres` (array; split by comma; capitalized)
- `original network` → `network` (array; split by comma)
- `Again Watched Date` columns (all columns after the recognized date column) → `againWatchedDates` (array, DD-MM-YYYY)
- Derived & added fields in JSON:
  - `showImage` — absolute URL to `images/<file>.jpg` (via `GITHUB_PAGES_URL`)
  - `showType` — `"Drama"` (default) or `"Mini Drama"` (sheet-specific)
  - `country` — derived from `nativeLanguage` (e.g., Korean → South Korea, Chinese → China)
  - `updatedOn` — IST date (format `dd MONTH YYYY`) when record last changed
  - `updatedDetails` — short message (≤ 30 chars). For manual updates: `"Updated <Field> Mannually By Owner"`
  - `synopsis` — auto-fetched (cleaned ~300–400 chars where possible)
  - `Duration` — parsed runtime in minutes (int) or `null`
  - `topRatings` — formula: `ratings × len(againWatchedDates) × 100`

---

## New Behaviors (caching, chunking, scheduled improvements)
1. **Caching**:
   - If an object in `seriesData.json` already has `showImage` and/or `synopsis`, the script will **skip** re-downloading or re-scraping for that field **unless** `SCHEDULED_RUN=true`.
   - This drastically reduces runtime for incremental runs.

2. **Chunking / MAX_PER_RUN**:
   - Set `MAX_PER_RUN` env var to limit how many shows from a sheet are processed in one run (e.g., `100`).  
   - If `MAX_PER_RUN` is `0` or unset run will process all shows in the sheet.
   - This lets you break a huge dataset into many safe runs (so you won't hit GitHub Actions 6-hour limit).

3. **Scheduled Weekly Betterment**:
   - When `SCHEDULED_RUN=true` (use only in scheduled workflow), the script will attempt to **find better images & synopsis** even when fields exist.
   - For non-scheduled/manual runs it will avoid unnecessary network work.

---

## Image fetching & improvements
- Image queries are reduced and the search stops at the **first valid** image downloaded.
- Images are resized to **600×900 JPEG** and saved to `images/`.
- When a new image replaces an old one:
  - the old image file (local) is moved to `old-images/`.
  - old files in `old-images/` are removed after `KEEP_OLD_IMAGES_DAYS`.
  - report is updated with `Image Updated` entries showing Old && New (report provides links/paths).
- `GITHUB_PAGES_URL` is used to build absolute image URLs stored in `showImage`.

---

## Preferred site order (by language) — easy to extend
- The code uses a `PREFERRED_SITE_ORDER` map to try particular sites first depending on `nativeLanguage`.
  - Current mapping:
    - `Korean` → [`asianwiki`, `mydramalist`]
    - `Chinese` → [`mydramalist`, `asianwiki`]
- If you want to add other languages or change order, edit the `PREFERRED_SITE_ORDER` dictionary in the script:

PREFERRED_SITE_ORDER = {
  "Korean": ["asianwiki","mydramalist"],
  "Chinese": ["mydramalist","asianwiki"],
  # Add more: "Japanese": ["asianwiki","mydramalist"]
}
