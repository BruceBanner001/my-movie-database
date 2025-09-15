<!--
========================================================================
README.md
Purpose: Usage instructions and workflow overview for the Excel → JSON automation.
========================================================================
-->
# Excel → JSON Automation (Longform)

This project automates updating a JSON database from an Excel sheet.

## Usage

### Manual run
```bash
python create_update_backup_delete.py
```
- For local/manual runs ensure `local-data.xlsx` exists in repo root.

### Scheduled run (weekly)
Configure the workflow to run on schedule. The workflow sets `MAX_RUN_TIME_MINUTES` based on trigger type.

## Reports

- `reports/report_*.txt` → detailed updates.  
- `reports/recommendations_*.txt` → extra values found online.  
- `reports/secrets_report_*.txt` → secrets scan results.  

## Notes
- `seriesData.json` can be an empty array (`[]`) at the start. The script will populate it from Excel.
- Manual update sheet ("manual update") will only apply once there are existing objects with matching showID.



## 🔄 Workflow Schedule & Runtime

The project is automated with **GitHub Actions** (`update.yml`).  
It supports both **manual** and **scheduled** runs:

### 📌 Triggers
- **Manual** → You press the **Run workflow** button in GitHub  
  - Runs immediately  
  - Runtime limit: **180 minutes**  
- **Automatic** → Scheduled every **Sunday at 12:00 UTC**  
  - Runs weekly without manual action  
  - Runtime limit: **240 minutes**

### ⏱️ Runtime Control (`MAX_RUN_TIME_MINUTES`)
- The workflow sets `MAX_RUN_TIME_MINUTES` depending on the trigger:
  - Manual → `180`
  - Scheduled → `240`
- The Python script reads this value and **stops after the time limit**.  
- If the run stops before finishing:
  - Progress is saved in `.progress/progress.json`  
  - Next run resumes where it left off  

### 📝 Example
If a scheduled run takes longer than **240 minutes**:
- The script exits safely with a message:  
  ```
  WORKFLOW CONTINUED... (time limit reached; will resume next run)
  ```
- On the following Sunday run, processing resumes automatically.

### 📂 Outputs
- `seriesData.json` → Updated main dataset  
- `reports/` → Text + HTML run reports  
- `backups/` → JSON backups with timestamped filenames  
- `images/` → Downloaded or updated show images  
