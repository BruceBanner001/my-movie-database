
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

## TODO Stubs

- `fetch_other_names(show_name)`  
- `fetch_images(show_name)`  
- `fetch_ratings(show_name)`  

Implement enrichment logic using PREFERRED_SITE_ORDER.
