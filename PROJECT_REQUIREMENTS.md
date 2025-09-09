# 📑 Requirements Report – Excel → JSON Automation

## 1. **Excel → JSON Conversion Rules**
- **Keys & Naming**
  - `no` → `showID` (prefix based on sheet: `1000+` for *Sheet1*, `2000+` for *Sheet2*, `3000+` for *Sheet3*).
  - `Series Title` → `showName` (trim spaces).
  - Add `"showImage"` → cover image URL (fetched from search engines, resized).
  - `Started Date` → `watchStartedOn` (format: `DD-MM-YYYY`).
  - `Finished Date` → `watchEndedOn`.
  - `Year` → `releasedYear`.
  - `Total Episodes` → `totalEpisodes`.
  - Add `"showType"` → `"Drama"` (default), `"Mini Drama"` if from *Mini Drama* sheet.
  - `Original Language` → `nativeLanguage` (trimmed, capitalized).
  - `Language` → `watchedLanguage` (trimmed, capitalized).
  - Add `"country"` → derived:
    - `"Korean"` → `"South Korea"`.
    - `"Chinese"` → `"China"`.
    - else → `null`.
  - `Comments` → `comments` (cleaned, capitalized each word, add `.` at end).
  - `Ratings` → `ratings`.
  - `Catagory`/`Category` → `genres` (array, split by comma, each capitalized).
  - `Original Network` → `network` (array, split by comma).
  - `Again Watched Date` → `againWatchedDates` (array of dates, format: `DD-MM-YYYY`).
  - Add `"updatedOn"` → current IST date (format: `dd MONTH YYYY`).
  - Add `"updatedDetails"`:
    - `"First time Uploaded"` for new objects.
    - Short description (≤30 chars) if key fields change:  
      (`showName`, `showImage`, `releasedYear`, `totalEpisodes`, `comments`, `ratings`, `genres`, `duration`, `synopsis`).
  - Add `"synopsis"` → auto-fetched from MDL, AsianWiki, Wikipedia, etc. (prefer 300–400 chars).
  - Add `"Duration"` → parsed into minutes (if available, otherwise `null`).
  - Add `"topRatings"` → `(ratings × againWatchedDates count × 100)`.

---

## 2. **Images**
- Cover images are fetched automatically using queries like:
  - `<showName> <year> drama cover`
  - `<showName> <year> official poster`
  - `<showName> network poster`
- Sources: DuckDuckGo (ddgs), Bing, Google, fallback to drama sites (*Netflix, Viki, Prime, AsianWiki, MyDramaList*).
- Images resized to **600×900 px**, high quality JPEG.
- Saved inside `images/`.
- JSON stores **absolute URLs** (via `GITHUB_PAGES_URL`) → e.g.:
  ```
  "showImage": "https://brucebanner001.github.io/my-movie-database/images/Crash_Landing_on_You_2019.jpg"
  ```

---

## 3. **Backups**
- When objects change (or are deleted), the old JSON object is saved into a backup file:
  - Location: `/backups/`
  - Name format: `DDMMYYYY_HHMM.json` (timestamp at update run).
- Ensures historical tracking of changes.

---

## 4. **Google Drive Integration**
- Source Excel file is private in Google Drive.
- Accessed via **service account**:
  - Service account JSON stored in GitHub secret → `GDRIVE_SERVICE_ACCOUNT`.
  - File ID set via `EXCEL_FILE_ID`.
- Downloaded as `local-data.xlsx` before processing.

---

## 5. **Automation via GitHub Actions**
- Workflow file: `.github/workflows/update.yml`
- Triggers:
  - **Weekly:** Every **Sunday 12:00 AM IST** (`cron: "30 18 * * 6"`).
  - **Manual:** Can be run anytime from Actions tab.
  - **On Push:** Runs if you update:
    - `create_update_backup_delete.py`
    - `requirements.txt`
    - `.github/workflows/update.yml`
- Steps:
  1. Checkout repo.
  2. Setup Python (3.11).
  3. Install dependencies from `requirements.txt`.
  4. Write service account JSON.
  5. Download Excel file from Google Drive.
  6. Run conversion script → update JSON + images + backups.
  7. Commit & push changes back to repo.

---

## 6. **Notifications**
- **GitHub Summary** → Markdown summary with run details.
- **Email Notifications**:
  - **Failure:** immediate email with ❌ subject.
  - **Success:** only for **weekly scheduled runs** (✅ subject).
- Configurable via Secrets:
  - `SMTP_USERNAME` (e.g. Gmail address).
  - `SMTP_PASSWORD` (App Password).
  - `NOTIFY_EMAIL` (recipient).

---

## 7. **Repo Setup**
- Must contain:
  - `create_update_backup_delete.py` (final script).
  - `requirements.txt` (with Google API + pandas + pillow + ddgs + bs4 + lxml).
  - `.github/workflows/update.yml` (workflow).
- Folders:
  - `images/` (with `.gitkeep` if empty).
  - `backups/` (with `.gitkeep` if empty).
- Generated files:
  - `seriesData.json` (main dataset).
  - Backup JSONs (inside `/backups`).

---

## 8. **Example JSON Output**

### First-time Upload
```json
{
  "showID": 1001,
  "showName": "Crash Landing on You",
  "showImage": "https://brucebanner001.github.io/my-movie-database/images/Crash_Landing_on_You_2019.jpg",
  "watchStartedOn": "07-02-2023",
  "watchEndedOn": "28-03-2023",
  "releasedYear": 2019,
  "totalEpisodes": 16,
  "showType": "Drama",
  "nativeLanguage": "Korean",
  "watchedLanguage": "English",
  "country": "South Korea",
  "comments": "Excellent drama with amazing chemistry.",
  "ratings": 5,
  "genres": ["Romance", "Comedy"],
  "network": ["Netflix", "tvN"],
  "againWatchedDates": ["21-12-2023", "07-05-2024"],
  "updatedOn": "08 September 2025",
  "updatedDetails": "First time Uploaded",
  "synopsis": "A South Korean heiress crash lands in North Korea after a paragliding accident, where she meets an army officer. Their story unfolds amidst political tension and heartfelt romance, blending suspense, comedy, and cultural contrast.",
  "Duration": 60,
  "topRatings": 1000
}
```

### After an Update (e.g., Image & Comments Changed)
```json
{
  "showID": 1001,
  "showName": "Crash Landing on You",
  "showImage": "https://brucebanner001.github.io/my-movie-database/images/Crash_Landing_on_You_2019_v2.jpg",
  "watchStartedOn": "07-02-2023",
  "watchEndedOn": "28-03-2023",
  "releasedYear": 2019,
  "totalEpisodes": 16,
  "showType": "Drama",
  "nativeLanguage": "Korean",
  "watchedLanguage": "English",
  "country": "South Korea",
  "comments": "Updated comment: Still one of my favorite dramas.",
  "ratings": 5,
  "genres": ["Romance", "Comedy"],
  "network": ["Netflix", "tvN"],
  "againWatchedDates": ["21-12-2023", "07-05-2024"],
  "updatedOn": "15 September 2025",
  "updatedDetails": "New Image has updated.",
  "synopsis": "A South Korean heiress crash lands in North Korea after a paragliding accident, where she meets an army officer. Their story unfolds amidst political tension and heartfelt romance, blending suspense, comedy, and cultural contrast.",
  "Duration": 60,
  "topRatings": 1000
}
```

⚡ Difference:
- `showImage` changed to new version.
- `comments` updated.
- `updatedOn` refreshed.
- `updatedDetails` changed from `"First time Uploaded"` to `"New Image has updated."`
