# ============================================================
# Script: create_update_backup_delete.py
# Author: [BruceBanner001]
# Description:
#   This script automates the creation, update, and backup process
#   for JSON data objects derived from Excel or YAML workflows.
#
#   Key features:
#   - Enforces a consistent 24-property schema for all JSON objects.
#   - Detailed diff-based backups for all updates.
#   - Creates metadata backups for all newly created items.
#   - Field locking to protect fetched data from being overwritten.
#   - Highly detailed and structured run reports.
#
# ============================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# --------------------------- VERSION & CONFIG ------------------------
SCRIPT_VERSION = "v3.2.1 (Fix: Restore Metadata Backups)"

# --- Master JSON Object Template ---
# Ensures every object written to seriesData.json has a consistent structure.
JSON_OBJECT_TEMPLATE = {
    "showID": None,
    "showName": None,
    "otherNames": [],
    "showImage": None,
    "watchStartedOn": None,
    "watchEndedOn": None,
    "releasedYear": 0,
    "releaseDate": None,
    "totalEpisodes": 0,
    "showType": None,
    "nativeLanguage": None,
    "watchedLanguage": None,
    "country": None,
    "comments": None,
    "ratings": 0,
    "genres": [],
    "network": [],
    "againWatchedDates": [],
    "updatedOn": None,
    "updatedDetails": None,
    "synopsis": None,
    "topRatings": 0,
    "Duration": 0,
    "sitePriorityUsed": {
        "showImage": None,
        "releaseDate": None,
        "otherNames": None,
        "duration": None,
        "synopsis": None
    }
}


# --- Site Priority Configuration ---
SITE_PRIORITY_BY_LANGUAGE = {
    "korean": {
        "synopsis": "asianwiki",
        "image": "asianwiki",
        "otherNames": "mydramalist",
        "duration": "mydramalist",
        "releaseDate": "asianwiki"
    },
    "chinese": {
        "synopsis": "mydramalist",
        "image": "mydramalist",
        "otherNames": "mydramalist",
        "duration": "mydramalist",
        "releaseDate": "mydramalist"
    },
    "japanese": {
        "synopsis": "asianwiki",
        "image": "asianwiki",
        "otherNames": "mydramalist",
        "duration": "mydramalist",
        "releaseDate": "asianwiki"
    },
    "thai": {
        "synopsis": "mydramalist",
        "image": "asianwiki",
        "otherNames": "mydramalist",
        "duration": "mydramalist",
        "releaseDate": "mydramalist"
    },
    "taiwanese": {
        "synopsis": "mydramalist",
        "image": "mydramalist",
        "otherNames": "mydramalist",
        "duration": "mydramalist",
        "releaseDate": "mydramalist"
    },
    "default": {
        "synopsis": "mydramalist",
        "image": "asianwiki",
        "otherNames": "mydramalist",
        "duration": "mydramalist",
        "releaseDate": "asianwiki"
    }
}

# --- Field Name Mapping ---
FIELD_NAME_MAP = {
    "showID": "Show ID",
    "showName": "Show Name",
    "otherNames": "Other Names",
    "showImage": "Show Image",
    "watchStartedOn": "Watch Started On",
    "watchEndedOn": "Watch Ended On",
    "releasedYear": "Released Year",
    "releaseDate": "Release Date",
    "totalEpisodes": "Total Episodes",
    "showType": "Show Type",
    "nativeLanguage": "Native Language",
    "watchedLanguage": "Watched Language",
    "country": "Country",
    "comments": "Comments",
    "ratings": "Ratings",
    "genres": "Category",
    "network": "Network",
    "againWatchedDates": "Again Watched Dates",
    "updatedOn": "Updated On",
    "updatedDetails": "Updated Details",
    "synopsis": "Synopsis",
    "topRatings": "Top Ratings",
    "Duration": "Duration",
    "sitePriorityUsed": "Site Priority Used"
}

# --- Locked Fields Configuration ---
LOCKED_FIELDS_AFTER_CREATION = {
    'synopsis',
    'showImage',
    'otherNames',
    'releaseDate',
    'Duration',
    'updatedOn',
    'updatedDetails',
    'sitePriorityUsed',
    'topRatings'
}


# ---------------------------- IMPORTS & GLOBALS ----------------------------
import os
import re
import sys
import time
import json
import io
import shutil
import traceback
import copy
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO

try:
    from ddgs import DDGS
    HAVE_DDGS = True
except Exception:
    HAVE_DDGS = False

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    HAVE_GOOGLE_API = True
except Exception:
    HAVE_GOOGLE_API = False

# --- Timezone & Timestamps ---
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def filename_timestamp(): return now_ist().strftime("%d_%B_%Y_%H%M")
def run_id_timestamp(): return now_ist().strftime("RUN_%Y%m%d_%H%M%S")

# --- Paths & Environment Variables ---
JSON_FILE = "seriesData.json"
BACKUP_DIR = "backups"
IMAGES_DIR = "images"
DELETE_IMAGES_DIR = "deleted-images"
DELETED_DATA_DIR = "deleted-data"
REPORTS_DIR = "reports"
BACKUP_META_DIR = "backup-meta-data"

DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
SCHEDULED_RUN = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
KEEP_OLD_FILES_DAYS = int(os.environ.get("KEEP_OLD_FILES_DAYS", "90") or 90)
GITHUB_PAGES_URL = os.environ.get("GITHUB_PAGES_URL", "").strip() or "https://<your-username>.github.io/my-movie-database"
SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json"
EXCEL_FILE_ID_TXT = "EXCEL_FILE_ID.txt"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

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
    keys_to_compare = set(old.keys()) | set(new.keys()) - LOCKED_FIELDS_AFTER_CREATION
    for k in keys_to_compare:
        o_val = old.get(k)
        n_val = new.get(k)
        if isinstance(o_val, list) and isinstance(n_val, list):
            if normalize_list(o_val) != normalize_list(n_val): return True
        elif o_val != n_val: return True
    return False

# ---------------------------- HTTP & PARSING HELPERS ---------------------------------
def fetch_page(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200: return r.text
        logd(f"Failed to fetch {url}, status code: {r.status_code}")
    except requests.RequestException as e:
        logd(f"Fetch page error for {url}: {e}")
    return None

def ddgs_search(query, type='text', max_results=5):
    if not HAVE_DDGS: return []
    try:
        with DDGS() as dd:
            if type == 'text':
                return list(dd.text(query, max_results=max_results))
            elif type == 'images':
                return [r.get("image") for r in dd.images(query, max_results=max_results) if r.get("image")]
    except Exception as e:
        logd(f"DDGS {type} search error for '{query}': {e}")
    return []

def parse_metadata_from_html(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text(" ", strip=True)
    metadata = {}

    meta_desc = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    if meta_desc and meta_desc.get("content") and len(meta_desc.get("content")) > 50:
        metadata['synopsis'] = meta_desc.get("content").strip()
    
    duration_match = re.search(r'(\b\d{2,3})\s*(min|minutes)\b', full_text, re.I)
    if duration_match: metadata['Duration'] = int(duration_match.group(1))

    release_match = re.search(r'(Release Date|Aired On|Aired)[\s:]*([A-Za-z]+\s+\d{1,2},\s*\d{4})', full_text, re.I)
    if release_match: metadata['releaseDate'] = release_match.group(2).strip()

    other_names_match = re.search(r'Also Known As[:\s]*([^\n\r]+)', full_text, re.I)
    if other_names_match: metadata['otherNames'] = normalize_list(other_names_match.group(1))

    if metadata.get('synopsis'):
        domain = re.sub(r'^https?://(www\.)?', '', base_url).split('/')[0]
        label = 'AsianWiki' if 'asianwiki' in domain else 'MyDramaList' if 'mydramalist' in domain else domain
        metadata['synopsis'] = f"{metadata['synopsis']} (Source: {label})"

    return metadata

def download_and_save_image(url, local_path):
    try:
        r = requests.get(url, headers=HEADERS, stream=True, timeout=15)
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

# ---------------------------- DATA FETCHING ORCHESTRATORS -----------------------------
def fetch_metadata_for_show(show_name, release_year, site_priority):
    query_base = f"{show_name} {release_year}"
    all_data = {}
    
    search_order = list(dict.fromkeys([
        site_priority.get("synopsis"), "mydramalist", "asianwiki"
    ]))

    for site in search_order:
        if not site: continue
        results = ddgs_search(f"{query_base} site:{site}.com", 'text')
        if not results: continue
        
        page_url = results[0].get('href')
        html = fetch_page(page_url)
        if not html: continue

        parsed_data = parse_metadata_from_html(html, page_url)
        for key, value in parsed_data.items():
            if not all_data.get(key) and value:
                all_data[key] = value
                all_data.setdefault('sitePriorityUsed', {})[key] = site
        
        if all(all_data.get(k) for k in ['synopsis', 'Duration', 'releaseDate', 'otherNames']):
            break
    
    return all_data

def fetch_image_for_show(show_name, release_year, show_id, site_priority, run_context):
    query_base = f"{show_name} {release_year} poster"
    
    search_order = list(dict.fromkeys([
        site_priority.get("image"), "asianwiki", "mydramalist"
    ]))

    for site in search_order:
        if not site: continue
        image_urls = ddgs_search(f"{query_base} site:{site}.com", 'images')
        if image_urls:
            local_path = os.path.join(IMAGES_DIR, f"{show_id}.jpg")
            if download_and_save_image(image_urls[0], local_path):
                run_context['files_generated']['images'].append(local_path)
                return build_absolute_url(local_path), site
    
    image_urls = ddgs_search(query_base, 'images')
    if image_urls:
        local_path = os.path.join(IMAGES_DIR, f"{show_id}.jpg")
        if download_and_save_image(image_urls[0], local_path):
            run_context['files_generated']['images'].append(local_path)
            return build_absolute_url(local_path), "ddgs"
            
    return None, None

def build_absolute_url(local_path):
    return f"{GITHUB_PAGES_URL.rstrip('/')}/{local_path.replace(os.sep, '/')}"

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
            
            ts = filename_timestamp()
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
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(list(by_id.values()), f, indent=4)
    
    return report, list(deleted_ids)

def apply_manual_updates(excel_file_like, by_id, run_context):
    report = {}
    try:
        df = pd.read_excel(excel_file_like, sheet_name='Manual Update')
    except ValueError: return {}

    for _, row in df.iterrows():
        sid = pd.to_numeric(row.iloc[0], errors='coerce')
        if pd.isna(sid) or int(sid) not in by_id: continue
        sid = int(sid)
        
        try:
            updates = json.loads(row.iloc[1])
        except (json.JSONDecodeError, TypeError): continue
        
        obj = by_id[sid]
        old_obj = copy.deepcopy(obj)
        changed_fields = {}

        for key, value in updates.items():
            if obj.get(key) != value:
                changed_fields[key] = {'old': obj.get(key), 'new': value}
                obj[key] = value
                if key in ['synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration']:
                    obj.setdefault('sitePriorityUsed', {})[key] = "Manual"

        if changed_fields:
            human_readable_changes = [human_readable_field(f) for f in changed_fields]
            obj['updatedDetails'] = f"{', '.join(human_readable_changes)} Updated Manually By Owner"
            obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            
            report.setdefault('updated', []).append({'old': old_obj, 'new': obj})
            create_diff_backup(old_obj, obj, run_context)

    return report

def excel_to_objects(excel_file_like, sheet_name, existing_by_id, run_context):
    df = pd.read_excel(excel_file_like, sheet_name=sheet_name, keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]
    
    try:
        again_idx = [i for i, c in enumerate(df.columns) if "again watched" in c][0]
    except IndexError:
        raise ValueError(f"'Again Watched' column not found in sheet: {sheet_name}")

    COLUMN_MAP = {
        "no": "showID",
        "series title": "showName",
        "started date": "watchStartedOn", 
        "finished date": "watchEndedOn",
        "year": "releasedYear",
        "total episodes": "totalEpisodes", 
        "original language": "nativeLanguage",
        "language": "watchedLanguage",
        "ratings": "ratings", 
        "catagory": "genres",
        "category": "genres",
        "original network": "network",
        "comments": "comments"
    }
    base_id_map = {"sheet1": 100, "feb 7 2023 onwards": 1000, "sheet2": 3000}
    base_id = base_id_map.get(sheet_name.lower(), 0)

    processed_objects = []
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
        
        sid = obj['showID']
        existing = existing_by_id.get(sid)
        
        if existing is None:
            obj['updatedDetails'] = "First Time Uploaded"
            obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            
            site_priority = SITE_PRIORITY_BY_LANGUAGE.get(obj.get('nativeLanguage','').lower(), SITE_PRIORITY_BY_LANGUAGE['default'])
            
            img_url, img_site = fetch_image_for_show(obj['showName'], obj['releasedYear'], sid, site_priority, run_context)
            if img_url: obj['showImage'] = img_url

            metadata = fetch_metadata_for_show(obj['showName'], obj['releasedYear'], site_priority)
            obj.update(metadata)

            spu = {k:None for k in JSON_OBJECT_TEMPLATE['sitePriorityUsed']}
            if img_site: spu['showImage'] = img_site
            if 'sitePriorityUsed' in metadata: spu.update(metadata['sitePriorityUsed'])
            obj['sitePriorityUsed'] = spu
        else:
            for field in LOCKED_FIELDS_AFTER_CREATION:
                if field in existing: obj[field] = existing[field]
        
        obj["topRatings"] = (obj.get("ratings", 0)) * (len(obj.get("againWatchedDates", [])) + 1) * 100
        
        final_obj = {**copy.deepcopy(JSON_OBJECT_TEMPLATE), **obj}
        processed_objects.append(final_obj)
        
    return processed_objects

def save_metadata_backup(new_obj, run_context):
    """Saves a metadata backup file for a newly created object."""
    fetched_fields = {}
    spu = new_obj.get('sitePriorityUsed', {})
    for key, site in spu.items():
        if site:
            fetched_fields[key] = {"value": new_obj.get(key), "source": site}

    if not fetched_fields: return # Don't create empty backups

    backup_data = {
        "scriptVersion": SCRIPT_VERSION,
        "runID": run_context['run_id'],
        "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"),
        "showID": new_obj['showID'],
        "showName": new_obj['showName'],
        "fetchedFields": fetched_fields
    }
    
    backup_path = os.path.join(BACKUP_META_DIR, f"META_{filename_timestamp()}_{new_obj['showID']}.json")
    os.makedirs(BACKUP_META_DIR, exist_ok=True)
    with open(backup_path, 'w', encoding='utf-8') as f: json.dump(backup_data, f, indent=4)
    run_context['files_generated']['meta_backups'].append(backup_path)

def create_diff_backup(old_obj, new_obj, run_context):
    changed_fields = {}
    for key, new_val in new_obj.items():
        old_val = old_obj.get(key)
        if isinstance(new_val, list): new_val = normalize_list(new_val)
        if isinstance(old_val, list): old_val = normalize_list(old_val)
        if old_val != new_val:
            changed_fields[key] = {"old": old_val, "new": new_val}
            
    if not changed_fields: return

    backup_data = {
        "scriptVersion": SCRIPT_VERSION,
        "runID": run_context['run_id'],
        "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"),
        "backupType": "partial_diff",
        "showID": new_obj['showID'],
        "showName": new_obj['showName'],
        "releasedYear": new_obj.get('releasedYear'),
        "updatedDetails": new_obj.get('updatedDetails', 'Record Updated'),
        "changedFields": changed_fields
    }
    
    backup_path = os.path.join(BACKUP_DIR, f"BACKUP_{filename_timestamp()}_{new_obj['showID']}.json")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    with open(backup_path, 'w', encoding='utf-8') as f: json.dump(backup_data, f, indent=4)
    run_context['files_generated']['backups'].append(backup_path)

# ---------------------------- REPORTING --------------------------------------
def write_report(run_context):
    report_path = run_context['report_file_path']
    report_changes = run_context['report_data']
    
    lines = [
        "✅ Workflow completed successfully",
        f"🆔 Run ID: {run_context['run_id']}",
        f"📅 Run Time: {now_ist().strftime('%d %B %Y %I:%M %p (IST)')}",
        f"🕒 Duration: {run_context['duration_str']}",
        f"⚙️ Script Version: {SCRIPT_VERSION}",
        ""
    ]
    sep = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    overall_stats = {
        'created': 0, 'updated': 0, 'skipped': 0, 'deleted': 0,
        'warnings': 0, 'images': 0, 'rows': 0
    }

    for sheet, changes in report_changes.items():
        if not changes: continue

        lines.extend([sep, f"𗀂 === {sheet} — {now_ist().strftime('%d %B %Y')} ==="])
        lines.append(sep)
        
        if changes.get('created'):
            lines.append("\n🆕 Data Created:")
            for obj in changes['created']:
                lines.append(f"- {obj['showID']} - {obj['showName']} ({obj.get('releasedYear')}) -> First Time Uploaded")

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
            overall_stats['created'] += stats.get('created', 0)
            overall_stats['updated'] += stats.get('updated', 0)
            overall_stats['skipped'] += stats.get('skipped', 0)
            overall_stats['images'] += sum(1 for item in changes.get('fetched_data', []) if "Show Image" in item)
            overall_stats['rows'] += total_rows
            
            lines.extend([
                f"\n📊 Summary (Sheet: {sheet})", sep,
                f"🆕 Total Created: {stats.get('created', 0)}",
                f"🔁 Total Updated: {stats.get('updated', 0)}",
                f"🖼️ Total Images Updated: {sum(1 for item in changes.get('fetched_data', []) if 'Show Image' in item)}",
                f"🚫 Total Skipped: {stats.get('skipped', 0)}",
                f"⚠️ Total Warnings: {len(changes.get('fetch_warnings', []))}",
                f"  Total Number of Rows: {total_rows}"
            ])
        lines.append("")

    overall_stats['deleted'] = len(run_context['files_generated']['deleted_data'])
    lines.extend([
        sep, "📊 Overall Summary", sep,
        f"🆕 Total Created: {overall_stats['created']}",
        f"🔁 Total Updated: {overall_stats['updated']}",
        f"🖼️ Total Images Updated: {overall_stats['images']}",
        f"🚫 Total Skipped: {overall_stats['skipped']}",
        f"❌ Total Deleted: {overall_stats['deleted']}",
        f"⚠️ Total Warnings: {overall_stats['warnings']}",
        f"💾 Backup Files Created: {len(run_context['files_generated']['backups'])}",
        f"  Grand Total Rows Processed: {overall_stats['rows']}",
        "",
        f"💾 Metadata Backups Created: {len(run_context['files_generated']['meta_backups'])}",
        ""
    ])

    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            lines.append(f"📦 Total Objects in seriesData.json: {len(json.load(f))}")
    except Exception: lines.append("📦 Total Objects in seriesData.json: Unknown")
    
    lines.extend([sep, "𗀂 Folders Generated:", sep])
    for folder, files in run_context['files_generated'].items():
        if files:
            lines.append(f"{folder}/")
            for file_path in files:
                lines.append(f"    {os.path.basename(file_path)}")
    lines.extend([sep, "🏁 Workflow finished successfully"])
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))

# ---------------------------- MAIN WORKFLOW -----------------------------------
def main():
    start_time = now_ist()
    run_context = {
        'run_id': run_id_timestamp(),
        'start_time': start_time,
        'report_data': {},
        'files_generated': {
            'backups': [], 'images': [], 'deleted_data': [], 
            'deleted_images': [], 'meta_backups': [], 'reports': []
        }
    }
    
    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)):
        print("❌ Missing GDrive credentials."); sys.exit(1)
        
    try:
        with open(EXCEL_FILE_ID_TXT, 'r') as f: excel_id = f.read().strip()
        if not excel_id: raise ValueError("EXCEL_FILE_ID.txt is empty.")
    except (FileNotFoundError, ValueError) as e:
        print(f"❌ Error with Excel ID file: {e}"); sys.exit(1)

    print(f"🚀 Running Script — Version {SCRIPT_VERSION} | Run ID: {run_context['run_id']}")

    excel_bytes = fetch_excel_from_gdrive_bytes(excel_id, SERVICE_ACCOUNT_FILE)
    if not excel_bytes: print("❌ Could not fetch Excel file from Google Drive."); sys.exit(1)
    
    del_report, _ = process_deletions(io.BytesIO(excel_bytes.getvalue()), JSON_FILE, run_context)
    if del_report: run_context['report_data']['Deleting Records'] = del_report

    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f: current_objects = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): current_objects = []
    
    merged_by_id = {o['showID']: o for o in current_objects if 'showID' in o}
    
    manual_report = apply_manual_updates(io.BytesIO(excel_bytes.getvalue()), merged_by_id, run_context)
    if manual_report: run_context['report_data']['Manual Updates'] = manual_report
    
    sheets_to_process = [s.strip() for s in os.environ.get("SHEETS", "Sheet1").split(";") if s.strip()]
    for sheet in sheets_to_process:
        report = {'created': [], 'updated': [], 'skipped': [], 'fetched_data': [], 'fetch_warnings': []}
        
        try:
            processed_objects = excel_to_objects(io.BytesIO(excel_bytes.getvalue()), sheet, merged_by_id, run_context)
        except Exception as e:
            print(f"❌ FATAL ERROR processing sheet '{sheet}': {e}"); continue

        for new_obj in processed_objects:
            sid = new_obj['showID']
            old_obj = merged_by_id.get(sid)
            
            if old_obj is None:
                report['created'].append(new_obj)
                merged_by_id[sid] = new_obj
                save_metadata_backup(new_obj, run_context)
                
                missing = [human_readable_field(k) for k, v in new_obj.items() if v is None and k in JSON_OBJECT_TEMPLATE]
                fetched = [human_readable_field(k) for k, v in new_obj['sitePriorityUsed'].items() if v]
                
                if fetched: report['fetched_data'].append(f"- {sid} - {new_obj['showName']} -> {', '.join(fetched)} Updated")
                if missing: report['fetch_warnings'].append(f"- {sid} - {new_obj['showName']} -> ⚠️ Missing: {', '.join(missing)} Not Found")

            elif objects_differ(old_obj, new_obj):
                changes = [human_readable_field(k) for k, v in new_obj.items() if old_obj.get(k) != v and k not in LOCKED_FIELDS_AFTER_CREATION]
                new_obj['updatedDetails'] = f"{', '.join(changes)} Updated" if changes else "Record Updated"
                new_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                
                report['updated'].append({'old': old_obj, 'new': new_obj})
                create_diff_backup(old_obj, new_obj, run_context)
                merged_by_id[sid] = new_obj
            else:
                report['skipped'].append(f"{sid} - {old_obj['showName']} ({old_obj.get('releasedYear')})")
        
        if any(report.values()):
            run_context['report_data'][sheet] = report

    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0)), f, indent=4)
        
    end_time = now_ist()
    duration = end_time - start_time
    run_context['duration_str'] = f"{duration.seconds // 60} min {duration.seconds % 60} sec"
    
    report_path = os.path.join(REPORTS_DIR, f"Report_{filename_timestamp()}.txt")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    run_context['report_file_path'] = report_path
    run_context['files_generated']['reports'].append(report_path)
    
    write_report(run_context)
    print(f"✅ Report written -> {report_path}")
    print("\nAll done.")

# ---------------------------- GOOGLE DRIVE API --------------------------------
def fetch_excel_from_gdrive_bytes(excel_file_id, service_account_path):
    if not HAVE_GOOGLE_API:
        print("ℹ️ Google API packages not installed."); return None
    try:
        creds = service_account.Credentials.from_service_account_file(
            service_account_path, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        
        try:
            request = drive_service.files().get_media(fileId=excel_file_id)
        except Exception:
            request = drive_service.files().export_media(
                fileId=excel_file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        return fh
    except Exception as e:
        logd(f"Google Drive fetch failed: {e}\n{traceback.format_exc()}"); return None

# ---------------------------- ENTRYPOINT -----------------------------------
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ A fatal, unexpected error occurred: {e}")
        logd(traceback.format_exc())
        sys.exit(1)