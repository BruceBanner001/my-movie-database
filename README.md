# Excel → JSON Automation

This project automates converting a private Excel file (stored on Google Drive) into a structured JSON database (`seriesData.json`) used by the frontend.  
The workflow always downloads the Excel from Google Drive using a service account — no local fallback.

---

## 📂 Inputs & Outputs

- **Input**: Excel workbook on Google Drive (file id stored in `EXCEL_FILE_ID.txt`).
- **Output**:  
  - `seriesData.json` — main dataset (for frontend).  
  - `images/` — current cover images.  
  - `old-images/` — previous images (moved here when replaced).  
  - `backups/` — JSON snapshots of changed/deleted objects.  
  - `deleted-data/` — archived JSON for deleted objects.  
  - `reports/` — plain-text run reports (`report_DDMMYYYY_HHMM.txt`).  

---

## 🔑 Setup Requirements

- Two files must exist in repo root (workflow writes them from secrets):  
  - `EXCEL_FILE_ID.txt` → Google Drive file ID.  
  - `GDRIVE_SERVICE_ACCOUNT.json` → service account JSON key.  

⚠️ The service account email inside the JSON must be given **Viewer access** to the Google Drive Excel file.

---

## ⚙️ Environment Variables (workflow)

- `GITHUB_PAGES_URL` → URL to your GitHub Pages site (used for image URLs).  
- `MAX_PER_RUN` → max entries per run (0 = all).  
- `MAX_RUN_TIME_MINUTES` → time limit (optional).  
- `SCHEDULED_RUN` → `true` for weekly runs (tries better images/synopsis even if values exist).  
- `KEEP_OLD_IMAGES_DAYS` → how many days to retain files in `old-images/` (default: 7).  
- `DELETED_LIFESPAN_DAYS` → how many days to keep files in `deleted-data/` (default: 30).  
- `DEBUG_FETCH` → set `true` for debug logs.  
- `SYNOPSIS_MAX_LEN` → max synopsis length (default: 1000 chars).  

---

## 📝 Excel → JSON Mapping

- `no` → `showID` (with sheet-based offset).  
- `series title` → `showName`.  
- `started date` / `finished date` → `watchStartedOn` / `watchEndedOn` (DD-MM-YYYY).  
- `year` → `releasedYear`.  
- `total episodes` → `totalEpisodes`.  
- `original language` → `nativeLanguage` (capitalized).  
- `language` → `watchedLanguage`.  
- `comments` → `comments` (cleaned & punctuated).  
- `ratings` → `ratings` (int, default 0).  
- `catagory` / `category` → `genres` (list).  
- `original network` → `network` (list).  
- `Again Watched Date` columns → `againWatchedDates` (array).  

🔧 Derived fields:
- `showImage` — absolute URL to GitHub Pages.  
- `showType` — `"Drama"` or `"Mini Drama"`.  
- `country` — derived from `nativeLanguage`.  
- `updatedOn` — IST date (`DD Month YYYY`).  
- `updatedDetails` — `"First Time Uploaded"` or `"Updated …"`.  
- `synopsis` — auto-fetched and cleaned.  
- `Duration` — runtime (minutes, if found).  
- `topRatings` — formula: `ratings × len(againWatchedDates) × 100`.  

---

## 📧 Email Notifications

### ✅ On Success
Only **one styled HTML email** is sent per workflow run.  
It includes **centered headers** and clean fonts:

- **Secrets Check** (if sensitive files were found).  
- **Report Content**:  
  - **First Time Uploads** →  
    `Series Name → First Time Uploaded`  
  - **Updates** →  
    - `Series Name → Image Updated`  
      → New Image (**hyperlinked**)  
      → Old Image (**hyperlinked**)  
    - `Series Name → Synopsis Updated`  
  - **Deletions** →  
    `ID → ✅ Deleted and archived`  
- **Summary** → Created / Updated / Deleted counts.  

### ❌ On Failure
A single plain email is sent:  
```
Subject: workflow failed
Body: <error message>
```

---

## 📑 Reports (saved in repo)

Even though styled emails are sent, plain-text reports are still written under `reports/` for history:
- `reports/report_*.txt` → detailed run log.  
- `reports/secrets_report_*.txt` → detected secrets.  

---

## 🔄 Retention Rules
- `old-images/` — cleaned after `KEEP_OLD_IMAGES_DAYS` (default 7).  
- `deleted-data/` — cleaned after `DELETED_LIFESPAN_DAYS` (default 30).  
- `reports/` — cleaned after 30 days (on scheduled runs).  

---

## 🖥️ Usage (local testing)

```bash
# Dry run (simulate updates without writing JSON)
python create_update_backup_delete.py --dry-run

# Scheduled run (simulate weekly job with enrichment)
python create_update_backup_delete.py --scheduled
```

Requirements: see [`requirements.txt`](requirements.txt).  

---

## 📌 Workflow Overview

- Workflow downloads Excel from Google Drive.  
- Script processes sheets, updates JSON, moves old images, archives deletions, and writes reports.  
- On success → commits changes + sends single styled email.  
- On failure → sends plain failure email.  
