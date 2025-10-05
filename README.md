# Excel → JSON Automation Script

### 🧭 Overview
This project automates the process of synchronizing data between Excel (or YAML) workflows and structured JSON files.  
It intelligently detects updates, preserves important data, creates backups, and generates detailed human-readable reports.

The script is designed for long-term scalability — ideal for managing TV show or media datasets where values (like ratings, comments, or genres) are updated frequently.

---

### ⚙️ Workflow Summary
Each workflow run performs these steps in order:

1. **Read Excel/YAML Source** – Loads sheets and extracts data objects.
2. **Compare Existing JSON** – Checks if an object already exists and identifies differences.
3. **Merge Intelligently** – Updates only changed fields while preserving non-empty data.
4. **Track Changes** – Detects Created, Updated, Deleted, and Skipped items.
5. **Backup & Report** – Backs up modified items and writes a clear summary report.

---

### 🧩 Key Features

#### 🗂 One Backup per Workflow
Only one backup JSON is created per run — containing *only* modified objects.  
Example: `backups/backup_05_October_2025_0812_modified.json`

#### 💤 Skipped Detection
If no fields are changed after comparison, the object appears under **“No Modification, Skipped”** in the report.  
Skipped entries do not trigger a backup.

#### 🧠 Intelligent Merge
Certain fields (like `otherNames`, `genres`) are preserved when incoming values are empty, preventing data loss.

#### 🗒 Detailed Update Reports
Each update includes the list of modified fields (e.g., `Ratings, Comments, Genre Updated`).  
The report also contains counts of created, updated, skipped, and deleted items.

#### ✍️ Manual Updates
Manual edits from Excel/YAML are recognized and labeled clearly as **“Manually Updated by Owner.”**

---

### 🧾 updatedDetails Behavior

| Scenario | Example Output | Description |
|-----------|----------------|--------------|
| New Object | `First time Uploaded` | First appearance of an item. |
| Auto Update | `Ratings, Comments Updated` | Normal detected update in JSON. |
| Manual Update | `Ratings, Genre Manually Updated by Owner` | Modified manually from Excel/YAML. |

This field is automatically filled based on which keys changed during merge.

---

### 🧱 Backup & Report Structure

#### 🔸 Backup
- Location: `backups/`
- Filename pattern: `backup_<date>_modified.json`
- Contains *only previous versions* of updated objects.

#### 🔸 Report
- Location: `reports/`
- Filename pattern: `report_<date>.txt`
- Sections include:
  - **Data Created**
  - **Data Updated**
  - **No Modification, Skipped**
  - **Image Updated**
  - **Deleted**
  - **Summary (Created / Updated / Skipped / Deleted)**

Example summary line:
```
SUMMARY: Created: 1, Updated: 2, Skipped: 3, Deleted: 0
```

---

### 🧩 Preservation Logic

#### PRESERVE_IF_EMPTY
Keeps existing values if the incoming field is empty.  
Used for fields like `otherNames`, so previous data isn’t overwritten with blanks.

#### LIST_PROPERTIES
Ensures specified keys remain lists instead of strings.  
Example: `["genres", "otherNames"]`

---

### 🚀 Scalability & Customization
- Add new list-like fields to `LIST_PROPERTIES` to make merging automatic.
- Add more keys to `PRESERVE_IF_EMPTY` for data safety.
- Modify report format easily from `write_report()`.
- The structure supports adding email notifications or API sync later without breaking logic.

---

© 2025 — JSON/Excel Automation Workflow | Developed for maintainability, scalability, and accuracy.
