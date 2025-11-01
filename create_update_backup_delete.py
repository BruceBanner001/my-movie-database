# ============================================================
# Script: create_update_backup_delete.py
# Author: [BruceBanner001]
# Description:
#   This script automates the creation, update, and backup process
#   for JSON data objects derived from Excel or YAML workflows.
#
#   Key features:
#   - Checkpoint & Resume logic for long-running jobs.
#   - Separated fetching logic in fetching.py for maintainability.
#   - Robust manual update functionality from a dedicated sheet.
#   - Enforces a consistent 24-property schema for all JSON objects.
#
# ============================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# --------------------------- VERSION & CONFIG ------------------------
SCRIPT_VERSION = "v5.2.0 (False Update & Reporting Fix)"

# --- Master JSON Object Template ---
JSON_OBJECT_TEMPLATE = {
    "showID": None, "showName": None, "otherNames": [], "showImage": None,
    "watchStartedOn": None, "watchEndedOn": None, "releasedYear": 0,
    "releaseDate": None, "totalEpisodes": 0, "showType": None,
    "nativeLanguage": None, "watchedLanguage": None, "country": None,
    "comments": None, "ratings": 0, "genres": [], "network": [],
    "againWatchedDates": [], "updatedOn": None, "updatedDetails": None,
    "synopsis": None, "topRatings": 0, "Duration": None,
    "sitePriorityUsed": {
        "showImage": None, "releaseDate": None, "otherNames": None,
        "duration": None, "synopsis": None
    }
}

# --- Site Priority Configuration ---
SITE_PRIORITY_BY_LANGUAGE = {
    "korean": {"synopsis": "asianwiki", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist"},
    "chinese": {"synopsis": "mydramalist", "image": "mydramalist", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist"},
    "japanese": {"synopsis": "asianwiki", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "asianwiki"},
    "thai": {"synopsis": "mydramalist", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist"},
    "taiwanese": {"synopsis": "mydramalist", "image": "mydramalist", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist"},
    "default": {"synopsis": "mydramalist", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "asianwiki"}
}

# --- Field Name Mapping ---
FIELD_NAME_MAP = {
    "showID": "Show ID", "showName": "Show Name", "otherNames": "Other Names",
    "showImage": "Show Image", "watchStartedOn": "Watch Started On", "watchEndedOn": "Watch Ended On",
    "releasedYear": "Released Year", "releaseDate": "Release Date", "totalEpisodes": "Total Episodes",
    "showType": "Show Type", "nativeLanguage": "Native Language", "watchedLanguage": "Watched Language",
    "country": "Country", "comments": "Comments", "ratings": "Ratings",
    "genres": "Category", "network": "Network", "againWatchedDates": "Again Watched Dates",
    "updatedOn": "Updated On", "updatedDetails": "Updated Details", "synopsis": "Synopsis",
    "topRatings": "Top Ratings", "Duration": "Duration", "sitePriorityUsed": "Site Priority Used"
}

# --- Locked Fields Configuration ---
LOCKED_FIELDS_AFTER_CREATION = {
    'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration',
    'updatedOn', 'updatedDetails', 'sitePriorityUsed', 'topRatings'
}

# ---------------------------- IMPORTS & GLOBALS ----------------------------
import os, re, sys, time, json, io, shutil, traceback, copy
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from PIL import Image
from io import BytesIO
import fetching

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    HAVE_GOOGLE_API = True
except Exception: HAVE_GOOGLE_API = False

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def filename_timestamp(): return now_ist().strftime("%d_%B_%Y_%H%M")
def run_id_timestamp(): return now_ist().strftime("RUN_%Y%m%d_%H%M%S")

JSON_FILE = "seriesData.json"
BACKUP_DIR = "backups"
IMAGES_DIR = "images"
DELETE_IMAGES_DIR = "deleted-images"
DELETED_DATA_DIR = "deleted-data"
REPORTS_DIR = "reports"
BACKUP_META_DIR = "backup-meta-data"
PROGRESS_FILE = os.path.join(REPORTS_DIR, "progress.json")

DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
SCHEDULED_RUN = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
KEEP_OLD_FILES_DAYS = int(os.environ.get("KEEP_OLD_FILES_DAYS", "90") or 90)
GITHUB_PAGES_URL = "https://brucebanner001.github.io/my-movie-database"
SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json"
EXCEL_FILE_ID_TXT = "EXCEL_FILE_ID.txt"
MAX_RUN_TIME_MINUTES = int(os.environ.get("MAX_RUN_TIME_MINUTES", "0") or "0")

def logd(msg):
    if DEBUG_FETCH: print(f"[DEBUG] {msg}")

# ---------------------------- CORE UTILITIES --------------------------------
def human_readable_field(field):
    return FIELD_NAME_MAP.get(field, field)

def safe_filename(name):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())

def ddmmyyyy(val):
    if pd.isna(val): return None
    if isinstance(val, pd.Timestamp): return val.strftime("%d-%m-%Y")
    try:
        dt = pd.to_datetime(str(val).strip(), dayfirst=True, errors="coerce")
        return None if pd.isna(dt) else dt.strftime("%d-%m-%Y")
    except Exception: return None

def normalize_list(cell_value):
    if cell_value is None: return []
    items = [str(x).strip() for x in cell_value] if isinstance(cell_value, (list, tuple)) else [p.strip() for p in str(cell_value).split(',') if p.strip()]
    return sorted([item for item in items if item])

def objects_differ(old, new):
    """Smarter comparison to prevent false positives."""
    keys_to_compare = set(old.keys()) | set(new.keys()) - LOCKED_FIELDS_AFTER_CREATION
    for k in keys_to_compare:
        o_val = old.get(k)
        n_val = new.get(k)

        # Treat None, 0, and empty strings/lists as equivalent for many fields
        if k not in ['showName', 'showID', 'releasedYear']:
            o_is_empty = o_val is None or o_val == "" or o_val == [] or o_val == 0
            n_is_empty = n_val is None or n_val == "" or n_val == [] or n_val == 0
            if o_is_empty and n_is_empty:
                continue

        if isinstance(o_val, list) or isinstance(n_val, list):
            if normalize_list(o_val) != normalize_list(n_val): return True
        elif o_val != n_val:
            try:
                # Handle cases like 10 vs "10"
                if str(o_val or "") != str(n_val or ""): return True
            except (TypeError, ValueError):
                if o_val != n_val: return True
    return False


def download_and_save_image(url, local_path):
    try:
        r = requests.get(url, headers=fetching.HEADERS, stream=True, timeout=15)
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
            with Image.open(r.raw) as img:
                img = img.convert("RGB")
                img.thumbnail((600, 900), Image.LANCZOS)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                img.save(local_path, "JPEG", quality=90)
                return True
    except Exception as e:
        logd(f"Image download failed from {url}: {e}")
    return False

def build_absolute_url(local_path):
    return f"{GITHUB_PAGES_URL.rstrip('/')}/{local_path.replace(os.sep, '/')}"

# ---------------------------- DATA FETCHING ORCHESTRATOR -----------------------------
def fetch_data_based_on_priority(show_name, release_year, show_id, site_priority, run_context):
    fetched_data = {}
    fetch_map = {
        'synopsis': {'asianwiki': fetching.fetch_synopsis_from_asianwiki, 'mydramalist': fetching.fetch_synopsis_from_mydramalist},
        'image': {'asianwiki': fetching.fetch_image_from_asianwiki, 'mydramalist': fetching.fetch_image_from_mydramalist},
        'otherNames': {'asianwiki': fetching.fetch_othernames_from_asianwiki, 'mydramalist': fetching.fetch_othernames_from_mydramalist},
        'Duration': {'asianwiki': fetching.fetch_duration_from_asianwiki, 'mydramalist': fetching.fetch_duration_from_mydramalist},
        'releaseDate': {'asianwiki': fetching.fetch_release_date_from_asianwiki, 'mydramalist': fetching.fetch_release_date_from_mydramalist}
    }
    spu = copy.deepcopy(JSON_OBJECT_TEMPLATE['sitePriorityUsed'])

    for field, site_map in fetch_map.items():
        priority_key = 'duration' if field == 'Duration' else field.lower()
        preferred_site = site_priority.get(priority_key)
        
        if preferred_site in site_map:
            try:
                result, url = (site_map[preferred_site](show_name, release_year, show_id) if field == 'image' else site_map[preferred_site](show_name, release_year))
                if result:
                    fetched_data[field] = result
                    key_map = {'image': 'showImage', 'Duration': 'duration'}
                    spu_key = key_map.get(field, field)
                    spu[spu_key] = preferred_site
                    run_context['temp_fetch_urls'][key_map.get(field, field)] = url
            except Exception as e:
                logd(f"Failed to fetch {field} from {preferred_site}: {e}")

    if 'image' in fetched_data:
        image_url = fetched_data['image']
        local_path = os.path.join(IMAGES_DIR, f"{show_id}.jpg")
        if download_and_save_image(image_url, local_path):
            fetched_data['showImage'] = build_absolute_url(local_path)
            run_context['files_generated']['images'].append(local_path)
        del fetched_data['image']

    fetched_data['sitePriorityUsed'] = spu
    return fetched_data

# ---------------------------- CORE WORKFLOW FUNCTIONS ---------------------------------
def process_deletions(excel_file_like, json_file, run_context):
    report = {}
    try:
        df = pd.read_excel(excel_file_like, sheet_name='Deleting Records')
        if 'id' not in df.columns[0].lower(): return {}, []
    except ValueError: return {}, []
    try:
        with open(json_file, 'r', encoding='utf-8') as f: data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): data = []
    by_id = {int(o['showID']): o for o in data if 'showID' in o}
    ids_to_delete = set(pd.to_numeric(df.iloc[:, 0], errors='coerce').dropna().astype(int))
    deleted_ids = set()
    for sid in ids_to_delete:
        if sid in by_id:
            deleted_obj = by_id.pop(sid)
            deleted_ids.add(sid)
            ts = run_context['start_timestamp'].strftime("%d_%B_%Y_%H%M")
            archive_path = os.path.join(DELETED_DATA_DIR, f"DELETED_{ts}_{sid}.json")
            os.makedirs(DELETED_DATA_DIR, exist_ok=True)
            with open(archive_path, 'w', encoding='utf-8') as f: json.dump(deleted_obj, f, indent=4)
            run_context['files_generated']['deleted_data'].append(archive_path)
            report.setdefault('data_deleted', []).append(f"- {sid} -> {deleted_obj.get('showName')} ({deleted_obj.get('releasedYear')}) -> ✅ Deleted and archived")
            if 'showImage' in deleted_obj and deleted_obj['showImage']:
                img_name = os.path.basename(deleted_obj['showImage'])
                src_path = os.path.join(IMAGES_DIR, img_name)
                if os.path.exists(src_path):
                    img_archive_path = os.path.join(DELETE_IMAGES_DIR, f"DELETED_{ts}_{sid}.jpg")
                    os.makedirs(DELETE_IMAGES_DIR, exist_ok=True)
                    shutil.move(src_path, img_archive_path)
                    run_context['files_generated']['deleted_images'].append(img_archive_path)
    if deleted_ids:
        with open(json_file, 'w', encoding='utf-8') as f: json.dump(list(by_id.values()), f, indent=4)
    return report, list(deleted_ids)

def apply_manual_updates(excel_file_like, by_id, run_context):
    report = {}
    try:
        df = pd.read_excel(excel_file_like, sheet_name='Manual Update', keep_default_na=False)
    except ValueError: return {}
    df.columns = [c.strip().lower() for c in df.columns]
    for _, row in df.iterrows():
        sid = pd.to_numeric(row.get('no'), errors='coerce')
        if pd.isna(sid) or int(sid) not in by_id: continue
        sid = int(sid)
        obj = by_id[sid]
        old_obj = copy.deepcopy(obj)
        changed_fields = {}
        update_map = {'image': 'showImage', 'other names': 'otherNames', 'release date': 'releaseDate', 'synopsis': 'synopsis', 'duration': 'Duration'}
        for col_name, obj_key in update_map.items():
            if col_name in row and row[col_name]:
                value = row[col_name]
                if obj_key == 'showImage':
                    local_path = os.path.join(IMAGES_DIR, f"{sid}.jpg")
                    if download_and_save_image(value, local_path):
                        new_value = build_absolute_url(local_path)
                        if obj.get(obj_key) != new_value:
                            changed_fields[obj_key] = {'old': obj.get(obj_key), 'new': new_value}
                            obj[obj_key] = new_value
                            run_context['files_generated']['images'].append(local_path)
                elif obj_key == 'otherNames':
                    new_value = normalize_list(value)
                    if obj.get(obj_key) != new_value:
                        changed_fields[obj_key] = {'old': obj.get(obj_key), 'new': new_value}
                        obj[obj_key] = new_value
                else:
                    new_value = str(value).strip()
                    if obj.get(obj_key) != new_value:
                        changed_fields[obj_key] = {'old': obj.get(obj_key), 'new': new_value}
                        obj[obj_key] = new_value
        if changed_fields:
            human_readable_changes = [human_readable_field(f) for f in changed_fields]
            obj['updatedDetails'] = f"{', '.join(human_readable_changes)} Updated Manually By Owner"
            obj['updatedOn'] = run_context['start_timestamp'].strftime('%d %B %Y')
            for key in changed_fields:
                spu_key = 'duration' if key == 'Duration' else key
                if spu_key in obj.get('sitePriorityUsed', {}): obj['sitePriorityUsed'][spu_key] = "Manual"
            report.setdefault('updated', []).append({'old': old_obj, 'new': obj})
            create_diff_backup(old_obj, obj, run_context)
    return report

def excel_to_objects(excel_file_like, sheet_name):
    df = pd.read_excel(excel_file_like, sheet_name=sheet_name, keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]
    try:
        again_idx = [i for i, c in enumerate(df.columns) if "again watched" in c][0]
    except IndexError: raise ValueError(f"'Again Watched' column not found in sheet: {sheet_name}")
    COLUMN_MAP = {"no": "showID", "series title": "showName", "started date": "watchStartedOn", "finished date": "watchEndedOn", "year": "releasedYear", "total episodes": "totalEpisodes", "original language": "nativeLanguage", "language": "watchedLanguage", "ratings": "ratings", "catagory": "genres", "category": "genres", "original network": "network", "comments": "comments"}
    base_id_map = {"sheet1": 100, "feb 7 2023 onwards": 1000, "sheet2": 3000}
    base_id = base_id_map.get(sheet_name.lower(), 0)
    all_objects_from_sheet = []
    for _, row in df.iterrows():
        obj = {}
        for col in df.columns[:again_idx]:
            key = COLUMN_MAP.get(col, col.strip())
            val = row[col]
            if key == "showID": obj[key] = base_id + int(val) if val else None
            elif key == "showName": obj[key] = str(val).strip() if val else None
            elif key in ("watchStartedOn", "watchEndedOn"): obj[key] = ddmmyyyy(val)
            elif key in ("releasedYear", "totalEpisodes", "ratings"): obj[key] = int(val) if val else 0
            elif key in ("genres", "network"): obj[key] = normalize_list(val)
            else: obj[key] = str(val).strip() if val else None
        if not obj.get("showID") or not obj.get("showName"): continue
        obj["againWatchedDates"] = [ddmmyyyy(d) for d in row[again_idx:] if ddmmyyyy(d)]
        obj["showType"] = "Mini Drama" if "mini" in sheet_name.lower() else "Drama"
        if obj.get("nativeLanguage", "").lower() in ("korean", "korea"): obj["country"] = "South Korea"
        all_objects_from_sheet.append(obj)
    return all_objects_from_sheet

def save_metadata_backup(new_obj, site_priority, run_context):
    spu = new_obj.get('sitePriorityUsed', {})
    successful_fields = [k for k, v in spu.items() if v]
    if not successful_fields: return
    failed_fields = [k for k, v in spu.items() if not v]
    status = "SUCCESS" if not failed_fields else "PARTIAL_SUCCESS"
    fetched_fields_list = []
    for field in successful_fields:
        field_data = {"field": field, "value": new_obj.get(field), "sourceSite": spu.get(field), "sourceURL": run_context['temp_fetch_urls'].get(field)}
        fetched_fields_list.append(field_data)
    backup_data = {
        "scriptVersion": SCRIPT_VERSION, "runID": run_context['run_id'], "timestamp": run_context['start_timestamp'].strftime("%d %B %Y %I:%M %p (IST)"),
        "showID": new_obj['showID'], "showName": new_obj['showName'], "releasedYear": new_obj.get('releasedYear'),
        "fetchingInputs": {"language": new_obj.get('nativeLanguage', 'default').lower(), "sitePriorityConfiguration": site_priority},
        "summary": {"status": status, "successfulFields": successful_fields, "failedFields": failed_fields},
        "fetchedFields": fetched_fields_list
    }
    backup_path = os.path.join(BACKUP_META_DIR, f"META_{run_context['start_timestamp'].strftime('%d_%B_%Y_%H%M')}_{new_obj['showID']}.json")
    os.makedirs(BACKUP_META_DIR, exist_ok=True)
    with open(backup_path, 'w', encoding='utf-8') as f: json.dump(backup_data, f, indent=4)
    run_context['files_generated']['meta_backups'].append(backup_path)

def create_diff_backup(old_obj, new_obj, run_context):
    changed_fields = {}
    for key, new_val in new_obj.items():
        old_val = old_obj.get(key)
        if isinstance(new_val, list): new_val = normalize_list(new_val)
        if isinstance(old_val, list): old_val = normalize_list(old_val)
        if old_val != new_val: changed_fields[key] = {"old": old_val, "new": new_val}
    if not changed_fields: return
    backup_data = {
        "scriptVersion": SCRIPT_VERSION, "runID": run_context['run_id'], "timestamp": run_context['start_timestamp'].strftime("%d %B %Y %I:%M %p (IST)"),
        "backupType": "partial_diff", "showID": new_obj['showID'], "showName": new_obj['showName'], "releasedYear": new_obj.get('releasedYear'),
        "updatedDetails": new_obj.get('updatedDetails', 'Record Updated'), "changedFields": changed_fields
    }
    backup_path = os.path.join(BACKUP_DIR, f"BACKUP_{run_context['start_timestamp'].strftime('%d_%B_%Y_%H%M')}_{new_obj['showID']}.json")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    with open(backup_path, 'w', encoding='utf-8') as f: json.dump(backup_data, f, indent=4)
    run_context['files_generated']['backups'].append(backup_path)

# ---------------------------- REPORTING --------------------------------------
def write_report(run_context):
    report_path = os.path.join(REPORTS_DIR, f"Report_{run_context['start_timestamp'].strftime('%d_%B_%Y_%H%M')}.txt")
    run_context['files_generated']['reports'].append(report_path)
    report_changes = run_context['report_data']
    end_time = now_ist()
    duration = end_time - run_context['start_timestamp']
    duration_str = f"{duration.seconds // 60} min {duration.seconds % 60} sec"
    lines = [f"✅ Workflow completed successfully", f"🆔 Run ID: {run_context['run_id']}", f"📅 Run Time: {end_time.strftime('%d %B %Y %I:%M %p (IST)')}", f"🕒 Duration: {duration_str}", f"⚙️ Script Version: {SCRIPT_VERSION}", ""]
    sep = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    overall_stats = {'created': 0, 'updated': 0, 'skipped': 0, 'deleted': 0, 'warnings': 0, 'images': 0, 'rows': 0}
    for sheet, changes in sorted(report_changes.items()):
        if not changes: continue
        lines.extend([sep, f"🗂️ === {sheet} — {run_context['start_timestamp'].strftime('%d %B %Y')} ==="])
        lines.append(sep)
        if changes.get('created'):
            lines.append("\n🆕 Data Created:")
            for obj in changes['created']: lines.append(f"- {obj['showID']} - {obj['showName']} ({obj.get('releasedYear')}) -> First Time Uploaded")
        if changes.get('updated'):
            lines.append("\n🔁 Data Updated:")
            for pair in changes['updated']:
                obj = pair['new']
                emoji = "✍️" if "Manually" in obj.get('updatedDetails', '') else "🔁"
                lines.append(f"{emoji} {obj['showID']} - {obj['showName']} ({obj.get('releasedYear')}) -> {obj['updatedDetails']}")
        if changes.get('fetched_data'):
            lines.append("\n🖼️ Fetched Data Updated:")
            for item in changes['fetched_data']: lines.append(item)
        if changes.get('fetch_warnings'):
            lines.append("\n🕳️ Value Not Found:")
            for item in changes['fetch_warnings']: lines.append(item)
            overall_stats['warnings'] += len(changes['fetch_warnings'])
        if changes.get('skipped'):
            lines.append("\n🚫 Unchanged Entries (Skipped):")
            for item in changes['skipped']: lines.append(f"- {item}")
        if changes.get('data_deleted'):
            lines.append("\n❌ Data Deleted:")
            for item in changes['data_deleted']: lines.append(item)
        if sheet not in ["Deleting Records", "Manual Updates"]:
            stats = {k: len(v) for k, v in changes.items()}
            total_rows = stats.get('created', 0) + stats.get('updated', 0) + stats.get('skipped', 0)
            overall_stats.update({k: overall_stats[k] + stats.get(k, 0) for k in ['created', 'updated', 'skipped']})
            overall_stats['images'] += sum(1 for item in changes.get('fetched_data', []) if "Show Image" in item)
            overall_stats['rows'] += total_rows
            lines.extend([f"\n📊 Summary (Sheet: {sheet})", sep, f"🆕 Total Created: {stats.get('created', 0)}", f"🔁 Total Updated: {stats.get('updated', 0)}", f"🖼️ Total Images Updated: {sum(1 for item in changes.get('fetched_data', []) if 'Show Image' in item)}", f"🚫 Total Skipped: {stats.get('skipped', 0)}", f"⚠️ Total Warnings: {len(changes.get('fetch_warnings', []))}", f"  Total Number of Rows: {total_rows}"])
        lines.append("")
    overall_stats['deleted'] = len(run_context['files_generated']['deleted_data'])
    lines.extend([sep, "📊 Overall Summary", sep, f"🆕 Total Created: {overall_stats['created']}", f"🔁 Total Updated: {overall_stats['updated']}", f"🖼️ Total Images Updated: {overall_stats['images']}", f"🚫 Total Skipped: {overall_stats['skipped']}", f"❌ Total Deleted: {overall_stats['deleted']}", f"⚠️ Total Warnings: {overall_stats['warnings']}", f"💾 Backup Files Created: {len(run_context['files_generated']['backups'])}", f"  Grand Total Rows Processed: {overall_stats['rows']}", "", f"💾 Metadata Backups Created: {len(run_context['files_generated']['meta_backups'])}", ""])
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f: lines.append(f"📦 Total Objects in seriesData.json: {len(json.load(f))}")
    except Exception: lines.append("📦 Total Objects in seriesData.json: Unknown")
    lines.extend([sep, "🗂️ Folders Generated:", sep])
    for folder, files in run_context['files_generated'].items():
        if files:
            lines.append(f"{folder}/")
            for file_path in files: lines.append(f"    {os.path.basename(file_path)}")
    lines.extend([sep, "🏁 Workflow finished successfully"])
    with open(report_path, 'w', encoding='utf-8') as f: f.write("\n".join(lines))
    print(f"✅ Final report written -> {report_path}")

# ---------------------------- MAIN WORKFLOW & RUN CONTEXT -----------------------------------
def load_run_context(sheets):
    if os.path.exists(PROGRESS_FILE):
        print("Resuming previous run...")
        with open(PROGRESS_FILE, 'r') as f:
            context = json.load(f)
            context['start_timestamp'] = datetime.fromisoformat(context['start_timestamp'])
            return context
    print("Starting a new run...")
    start_time = now_ist()
    return {
        'run_id': run_id_timestamp(), 'start_timestamp': start_time.isoformat(),
        'initial_setup_done': False, 'sheets_to_process': sheets, 'current_sheet_index': 0,
        'current_row_index': 0, 'report_data': {},
        'files_generated': {'backups': [], 'images': [], 'deleted_data': [], 'deleted_images': [], 'meta_backups': [], 'reports': []}
    }

def save_run_context(context):
    context_to_save = context.copy()
    context_to_save['start_timestamp'] = context['start_timestamp'].isoformat()
    with open(PROGRESS_FILE, 'w') as f: json.dump(context_to_save, f, indent=4)
    print("...Progress saved.")

def main():
    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)):
        print("❌ Missing GDrive credentials."); sys.exit(1)
    try:
        with open(EXCEL_FILE_ID_TXT, 'r') as f: excel_id = f.read().strip()
        if not excel_id: raise ValueError("EXCEL_FILE_ID.txt is empty.")
    except (FileNotFoundError, ValueError) as e:
        print(f"❌ Error with Excel ID file: {e}"); sys.exit(1)
    sheets_to_process = [s.strip() for s in os.environ.get("SHEETS", "Sheet1").split(";") if s.strip()]
    run_context = load_run_context(sheets_to_process)
    run_context['start_timestamp'] = datetime.fromisoformat(run_context['start_timestamp'])
    run_context['temp_fetch_urls'] = {}
    print(f"🚀 Running Script — Version {SCRIPT_VERSION} | Run ID: {run_context['run_id']}")
    excel_bytes = fetch_excel_from_gdrive_bytes(excel_id, SERVICE_ACCOUNT_FILE)
    if not excel_bytes: print("❌ Could not fetch Excel file from Google Drive."); sys.exit(1)
    if not run_context.get('initial_setup_done', False):
        del_report, _ = process_deletions(io.BytesIO(excel_bytes.getvalue()), JSON_FILE, run_context)
        if del_report: run_context.setdefault('report_data', {})['Deleting Records'] = del_report
        try:
            with open(JSON_FILE, 'r') as f: current_objects = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError): current_objects = []
        merged_by_id = {o['showID']: o for o in current_objects if 'showID' in o}
        manual_report = apply_manual_updates(io.BytesIO(excel_bytes.getvalue()), merged_by_id, run_context)
        if manual_report: run_context.setdefault('report_data', {})['Manual Updates'] = manual_report
        run_context['initial_setup_done'] = True
        save_run_context(run_context)
    try:
        with open(JSON_FILE, 'r') as f: current_objects = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): current_objects = []
    merged_by_id = {o['showID']: o for o in current_objects if 'showID' in o}
    time_limit_seconds = (MAX_RUN_TIME_MINUTES * 60) - 120 if MAX_RUN_TIME_MINUTES > 0 else float('inf')
    start_loop_time = time.time()
    for sheet_idx in range(run_context['current_sheet_index'], len(sheets_to_process)):
        sheet_name = sheets_to_process[sheet_idx]
        run_context['current_sheet_index'] = sheet_idx
        all_objects_from_sheet = excel_to_objects(io.BytesIO(excel_bytes.getvalue()), sheet_name)
        for row_idx in range(run_context.get('current_row_index', 0), len(all_objects_from_sheet)):
            if (time.time() - start_loop_time) > time_limit_seconds:
                print("⏳ Time limit approaching. Saving progress and exiting.")
                run_context['current_row_index'] = row_idx
                save_run_context(run_context)
                with open(JSON_FILE, 'w') as f: json.dump(sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0)), f, indent=4)
                sys.exit(0)
            obj_from_excel = all_objects_from_sheet[row_idx]
            sid = obj_from_excel['showID']
            old_obj = merged_by_id.get(sid)
            report = run_context['report_data'].setdefault(sheet_name, {'created': [], 'updated': [], 'skipped': [], 'fetched_data': [], 'fetch_warnings': []})
            if old_obj is None:
                site_priority = SITE_PRIORITY_BY_LANGUAGE.get(obj_from_excel.get('nativeLanguage','').lower(), SITE_PRIORITY_BY_LANGUAGE['default'])
                fetched_data = fetch_data_based_on_priority(obj_from_excel['showName'], obj_from_excel['releasedYear'], sid, site_priority, run_context)
                obj_from_excel.update(fetched_data)
                final_obj = {**copy.deepcopy(JSON_OBJECT_TEMPLATE), **obj_from_excel}
                final_obj['updatedOn'] = run_context['start_timestamp'].strftime('%d %B %Y')
                final_obj['updatedDetails'] = "First Time Uploaded"
                report['created'].append(final_obj)
                merged_by_id[sid] = final_obj
                save_metadata_backup(final_obj, site_priority, run_context)
                missing = [human_readable_field(k) for k, v in final_obj.items() if v is None and k in JSON_OBJECT_TEMPLATE and k != 'showID']
                fetched = [human_readable_field(k) for k, v in final_obj['sitePriorityUsed'].items() if v]
                if fetched: report['fetched_data'].append(f"- {sid} - {final_obj['showName']} ({final_obj.get('releasedYear')}) -> {', '.join(fetched)} Updated")
                if missing: report['fetch_warnings'].append(f"- {sid} - {final_obj['showName']} ({final_obj.get('releasedYear')}) -> ⚠️ Missing: {', '.join(missing)} Not Found")
            elif objects_differ(old_obj, obj_from_excel):
                changes = [human_readable_field(k) for k, v in obj_from_excel.items() if old_obj.get(k) != v and k not in LOCKED_FIELDS_AFTER_CREATION]
                final_obj = {**old_obj, **obj_from_excel}
                final_obj['updatedDetails'] = f"{', '.join(changes)} Updated" if changes else "Record Updated"
                final_obj['updatedOn'] = run_context['start_timestamp'].strftime('%d %B %Y')
                report['updated'].append({'old': old_obj, 'new': final_obj})
                create_diff_backup(old_obj, final_obj, run_context)
                merged_by_id[sid] = final_obj
            else:
                report['skipped'].append(f"- {sid} - {old_obj['showName']} ({old_obj.get('releasedYear')})")
        run_context['current_row_index'] = 0
    print("✅ All sheets processed. Finalizing run.")
    with open(JSON_FILE, 'w', encoding='utf-8') as f: json.dump(sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0)), f, indent=4)
    write_report(run_context)
    if os.path.exists(PROGRESS_FILE): os.remove(PROGRESS_FILE)
    print("\nAll done.")

# ---------------------------- GOOGLE DRIVE API & ENTRYPOINT -------------------
def fetch_excel_from_gdrive_bytes(excel_file_id, service_account_path):
    if not HAVE_GOOGLE_API: return None
    try:
        creds = service_account.Credentials.from_service_account_file(service_account_path, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        try:
            request = drive_service.files().get_media(fileId=excel_file_id)
        except Exception:
            request = drive_service.files().export_media(fileId=excel_file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except Exception as e:
        logd(f"Google Drive fetch failed: {e}\n{traceback.format_exc()}"); return None

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ A fatal, unexpected error occurred: {e}")
        logd(traceback.format_exc())
        os.makedirs(REPORTS_DIR, exist_ok=True)
        with open(os.path.join(REPORTS_DIR, "failure_reason.txt"), "w") as f:
            f.write(str(e) + "\n\n" + traceback.format_exc())
        sys.exit(1)