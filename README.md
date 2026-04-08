# 🎬 My Movie Database - Automation Engine
**Author:** BRUCE  
**Version:** v12.1.2 (Strict Actor & Alias Patch)

A high-performance Python engine designed to synchronize Google Sheets data with a structured JSON database. It automates metadata scraping, poster management, and multi-stage backups.

---

## 🚀 Core Functionality
* **Automated Sync:** Fetches updates from Google Drive via GitHub Actions.
* **Smart Scraping:** Pulls Synopsis, Posters, and Cast from **AsianWiki** & **MyDramaList**.
* **Intelligent Validation:** Uses year-tolerance (±1 year) and strict actor-profile rejection.
* **Archiving:** Generates diff-backups for every data change to prevent data loss.

---

## 📂 Folder Structure
```text
root/
├── create_update_backup_delete.py  # Main Python Script
├── seriesData.json                 # Main Show Database
├── artists.json                    # Global Artist Registry
├── cast.json                       # Role & Character Mapping
├── show-images/                    # Drama & Movie Posters
├── artist-images/                  # Actor & Crew Headshots
├── reports/                        # Daily Execution Logs
├── backups/                        # Incremental Change History
└── backup-meta-data/               # Source URL Snapshots
```

---

## 📝 Data Transformation Example
The engine converts spreadsheet rows into metadata-rich JSON objects.

### 🔹 Input (Spreadsheet Row)
| No | Series Title | Year | Ratings | Category |
| :--- | :--- | :--- | :--- | :--- |
| 1001 | Vincenzo | 2021 | 10 | Comedy, Law, Crime |

### 🔹 Output (seriesData.json)
```json
{
    "showID": 1001,
    "showName": "Vincenzo",
    "releasedYear": 2021,
    "ratings": 10,
    "genres": ["Comedy", "Law", "Crime"],
    "synopsis": "A mafia lawyer from Italy returns to South Korea...",
    "showImage": "1001.jpg",
    "updatedDetails": "First Time Uploaded",
    "updatedOn": "08 April 2026"
}