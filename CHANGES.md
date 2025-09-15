# Patch Summary

Files included in this bundle:
- create_update_backup_delete.py  (patched, robust error handling, detailed header)
- update.yml                     (workflow with conditional MAX_RUN_TIME_MINUTES)
- requirements.txt               (annotated)
- PROJECT_REQUIREMENTS.md        (annotated)
- README.md                      (annotated)
- seriesData.json                (empty array)

Main fixes applied:
- Fixed normalize_list_from_csv signature and behavior.
- Added country_from_native helper.
- Fail loudly when local-data.xlsx is missing.
- Exit non-zero when no records processed so CI can detect failures.
- Improved report writing and added failure_reason.txt when appropriate.

MAX_RUN_TIME_MINUTES behavior:
- Set via workflow step that writes to $GITHUB_ENV.
- Manual runs (workflow_dispatch) -> 180 minutes in this bundle's example.
- Scheduled runs (cron) -> 240 minutes in this bundle's example.
