
# Excel → JSON Automation (Longform)

This project automates updating a JSON database from an Excel sheet.

## Usage

### Manual run
```bash
python create_update_backup_delete.py --dry-run
```
- Simulates updates without writing JSON.

### Scheduled run (weekly)
```bash
python create_update_backup_delete.py --scheduled
```
- Enables enrichment stubs and recommendations.
- Performs report retention cleanup.

## Workflow

- **Manual trigger**: Emails `[Manual] JSON Update Successful/Failed`.
- **Automatic trigger**: Emails `[Automatic] JSON Update Successful/Failed`.

## Reports

- `reports/report_*.txt` → detailed updates.  
- `reports/recommendations_*.txt` → extra values found online.  
- `reports/secrets_report_*.txt` → secrets scan results.  

## Secrets

Workflow scans with gitleaks.  
If secrets are detected, a separate email is sent with details.

## Retention

Old reports auto-cleaned in scheduled runs.  
Default: 30 days (change via `REPORT_RETENTION_DAYS`).

## Manual Updates

When updated via Excel manually, JSON includes:  
```
"updatedDetails": "Updated Ratings, ShowImage Mannually By Owner"
```

## otherNames

Every object has an `otherNames` property right after `showName`.  
- Defaults to `[]` if no names are found.  
- Parsed from Excel if provided (English first, others later).  
- Extras beyond limit go into recommendations (only scheduled runs).  

## TODO Stubs

- `fetch_other_names(show_name)`  
- `fetch_images(show_name)`  
- `fetch_ratings(show_name)`  

Implement enrichment logic using PREFERRED_SITE_ORDER.


## Report Sections Explained

Each workflow run produces `reports/report_YYYYMMDD_HHMM.txt` and `.html`.  
Below are the sections you will see:

### Added / Updated Records
Lists all new or updated JSON objects processed in this run.

### Deleted Records
Shows IDs that were removed based on the `Deleting Records` sheet.  
Each deleted object is also stored in `deleted-data/DELETED_DD_Month_YYYY_HHMM_<id>.json` for 30 days.

### Exceed Max Length
Lists objects whose synopsis exceeded the configured max length (default: 1000).  
The report shows ID, name, site, and a clickable `Link` to the source.

### Image Cleanup
Summarizes how many images were moved from `images/` to `old-images/`  
and how many were permanently deleted based on the `KEEP_OLD_IMAGES_DAYS` setting.

### Workflow Status
At the end of the report, you will see:
- **WORKFLOW CONTINUED...** if the process hit the time limit and will resume in the next run.
- **WORKFLOW COMPLETED FOR THE SHEET: <name>** when all rows for a sheet are done.
