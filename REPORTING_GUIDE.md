# 📊 Understanding Execution Reports
**Author:** BRUCE

Every time the script runs (either on a schedule or manually), it generates a text report in the `reports/` folder.

## 1. Report Status Types
* **PARTIAL_..._REPORT.txt**: This means the script hit the `MAX_FETCHES` limit (usually 50 rows). It saved its progress and will continue in the next run.
* **FINAL_..._REPORT.txt**: This means every row in your Excel sheet is now perfectly synced with your JSON database.

## 2. Reading the Summary (Examples)
The report uses symbols to make it easy to scan:

| Symbol | Meaning | Example |
| :--- | :--- | :--- |
| 🆕 | **Created** | A brand new drama was added to `seriesData.json`. |
| 🔁 | **Updated** | You changed a "Rating" or "Comment" in Excel, and it was synced. |
| 🔍 | **Refetched** | The script found new "Cast" or a "Synopsis" from the web. |
| 🖼️ | **Images** | A new poster was downloaded to `show-images/`. |
| ⚠️ | **Warning** | A piece of data (like "Duration") was missing on the website. |

## 3. The Backup System
* **BACKUP_... .json**: A "diff" file. If you update a rating from 8 to 9, this file stores the "Old" and "New" values so you never lose history.
* **META_... .json**: A technical snapshot. It stores the exact URL (AsianWiki/MDL) used to fetch that specific show's data.

## 4. Deletion Logs
If you use the **"Deleting Records"** sheet, the report will confirm:
* `✅ Deleted`: The entry is gone from the main database and safely archived in `deleted-data/`.