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
#   - Specialized, site-specific data fetchers (AsianWiki, MyDramaList).
#   - Robust data validation to prevent crashes from bad Excel data.
#
# ============================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# --------------------------- VERSION & CONFIG ------------------------
SCRIPT_VERSION = "v4.2.0 (Feat: Robust Data Validation)"

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
    "Duration": None, # Changed from 0 to None (String type)
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
STATE_FILE = "run_state.json"


DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
SCHEDULED_RUN = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
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

def ddmmyyyy(val):
    if pd.isna(val): return None
    if isinstance(val, pd.Timestamp): return val.strftime("%d-%m-%Y")
    try:
        dt = pd.to_datetime(str(val).strip(), dayfirst=True, errors="coerce")
        return None if pd.isna(dt) else dt.strftime("%d-%m-%Y")
    except Exception: return None

def normalize_list(cell_value):
    if cell_value is None: return []
    if isinstance(cell_value, str):
        items = [p.strip() for p in cell_value.split(',') if p.strip()]
    elif isinstance(cell_value, (list, tuple)):
        items = [str(x).strip() for x in cell_value]
    else:
        items = [str(cell_value).strip()]
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
def get_soup_from_search(query):
    if not HAVE_DDGS: return None
    try:
        with DDGS() as dd:
            results = list(dd.text(query, max_results=1))
            if not results:
                logd(f"DDGS search for '{query}' returned no results.")
                return None
            
            page_url = results[0].get('href')
            r = requests.get(page_url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return BeautifulSoup(r.text, "html.parser")
            else:
                logd(f"Failed to fetch {page_url}, status code: {r.status_code}")
    except Exception as e:
        logd(f"Error in get_soup_from_search for '{query}': {e}")
    return None

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
    
def build_absolute_url(local_path):
    return f"{GITHUB_PAGES_URL.rstrip('/')}/{local_path.replace(os.sep, '/')}"

# ---------------------------- 🏮 ASIANWIKI FETCHING BLOCKS ----------------------------

def fetch_synopsis_from_asianwiki(show_name, release_year):
    soup = get_soup_from_search(f"{show_name} {release_year} site:asianwiki.com")
    if not soup: return None
    h2_synopsis = soup.find('h2', string='Synopsis')
    if not h2_synopsis: return None
    content = []
    for sibling in h2_synopsis.find_next_siblings():
        if sibling.name == 'h2': break
        if sibling.name == 'p': content.append(sibling.get_text(strip=True))
    synopsis = " ".join(content)
    return f"{synopsis} (Source: AsianWiki)" if synopsis else None

def fetch_image_from_asianwiki(show_name, release_year, show_id):
    soup = get_soup_from_search(f"{show_name} {release_year} site:asianwiki.com")
    if not soup: return None
    img_tag = soup.select_one('a.image > img')
    if not img_tag or not img_tag.get('src'): return None
    image_url = img_tag['src']
    local_path = os.path.join(IMAGES_DIR, f"{show_id}.jpg")
    if download_and_save_image(image_url, local_path):
        return build_absolute_url(local_path)
    return None

def fetch_othernames_from_asianwiki(show_name, release_year):
    soup = get_soup_from_search(f"{show_name} {release_year} site:asianwiki.com")
    if not soup: return None
    parent_div = soup.find('div', id='mw-content-text')
    if not parent_div: return []
    text_content = parent_div.get_text(" ", strip=True)
    match = re.search(r"Drama:\s*([^(\n\r]+)", text_content)
    return normalize_list(match.group(1).strip()) if match else []

def fetch_duration_from_asianwiki(show_name, release_year):
    return None

def fetch_release_date_from_asianwiki(show_name, release_year):
    soup = get_soup_from_search(f"{show_name} {release_year} site:asianwiki.com")
    if not soup: return None
    text_content = soup.get_text(" ", strip=True)
    match = re.search(r"Release Date:\s*([^\n\r]+)", text_content)
    return match.group(1).strip() if match else None

# ---------------------------- 🌏 MYDRAMALIST FETCHING BLOCKS ---------------------------

def fetch_synopsis_from_mydramalist(show_name, release_year):
    soup = get_soup_from_search(f"{show_name} {release_year} site:mydramalist.com")
    if not soup: return None
    synopsis_div = soup.select_one('.show-synopsis')
    if not synopsis_div: return None
    synopsis = synopsis_div.get_text(strip=True).replace('(Source: MyDramaList)', '').strip()
    return f"{synopsis} (Source: MyDramaList)" if synopsis else None

def fetch_image_from_mydramalist(show_name, release_year, show_id):
    soup = get_soup_from_search(f"{show_name} {release_year} site:mydramalist.com")
    if not soup: return None
    img_tag = soup.select_one('.film-cover img, .cover img')
    if not img_tag or not img_tag.get('src'): return None
    image_url = img_tag['src']
    local_path = os.path.join(IMAGES_DIR, f"{show_id}.jpg")
    if download_and_save_image(image_url, local_path):
        return build_absolute_url(local_path)
    return None

def fetch_othernames_from_mydramalist(show_name, release_year):
    soup = get_soup_from_search(f"{show_name} {release_year} site:mydramalist.com")
    if not soup: return []
    aka_header = soup.find('b', string=re.compile(r'Also Known As:'))
    if not aka_header: return []
    names_text = aka_header.next_sibling
    return normalize_list(names_text) if names_text and isinstance(names_text, str) else []

def fetch_duration_from_mydramalist(show_name, release_year):
    soup = get_soup_from_search(f"{show_name} {release_year} site:mydramalist.com")
    if not soup: return None
    duration_el = soup.find(lambda tag: 'Duration:' in tag.get_text() and tag.name == 'li')
    if not duration_el: return None
    return duration_el.get_text().replace('Duration:', '').strip()

def fetch_release_date_from_mydramalist(show_name, release_year):
    soup = get_soup_from_search(f"{show_name} {release_year} site:mydramalist.com")
    if not soup: return None
    aired_el = soup.find(lambda tag: 'Aired:' in tag.get_text() and tag.name == 'li')
    if not aired_el: return None
    return aired_el.get_text().replace('Aired:', '').strip()

# ---------------------------- DATA FETCHING ORCHESTRATOR -----------------------------
FETCH_MAP = {
    'asianwiki': {
        'synopsis': fetch_synopsis_from_asianwiki, 'image': fetch_image_from_asianwiki,
        'otherNames': fetch_othernames_from_asianwiki, 'duration': fetch_duration_from_asianwiki,
        'releaseDate': fetch_release_date_from_asianwiki,
    },
    'mydramalist': {
        'synopsis': fetch_synopsis_from_mydramalist, 'image': fetch_image_from_mydramalist,
        'otherNames': fetch_othernames_from_mydramalist, 'duration': fetch_duration_from_mydramalist,
        'releaseDate': fetch_release_date_from_mydramalist,
    }
}

def fetch_and_populate_metadata(obj, site_priority, run_context):
    show_name, release_year, show_id = obj['showName'], obj['releasedYear'], obj['showID']
    spu = obj.setdefault('sitePriorityUsed', {})
    fields_to_fetch = ['synopsis', 'image', 'releaseDate', 'duration', 'otherNames']
    
    for field in fields_to_fetch:
        primary_site = site_priority.get(field)
        fallback_site = 'mydramalist' if primary_site == 'asianwiki' else 'asianwiki'
        result, used_site = None, None
        
        if primary_site in FETCH_MAP and field in FETCH_MAP[primary_site]:
            fetch_func = FETCH_MAP[primary_site][field]
            args = (show_name, release_year, show_id) if field == 'image' else (show_name, release_year)
            result = fetch_func(*args)
            if result: used_site = primary_site

        if not result and fallback_site in FETCH_MAP and field in FETCH_MAP[fallback_site]:
            fetch_func = FETCH_MAP[fallback_site][field]
            args = (show_name, release_year, show_id) if field == 'image' else (show_name, release_year)
            result = fetch_func(*args)
            if result: used_site = fallback_site

        if result:
            if field == 'image':
                obj['showImage'] = result
                run_context['files_generated']['images'].append(os.path.join(IMAGES_DIR, f"{show_id}.jpg"))
            else:
                obj[field] = result
            spu[field] = used_site
    return obj

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
        df = pd.read_excel(excel_file_like, sheet_name='Manual Updates', keep_default_na=False)
        df.columns = [c.strip().lower() for c in df.columns]
    except ValueError: return {}
    
    COLUMN_MAP = {"no": "showID", "image": "showImage", "other names": "otherNames",
                  "release date": "releaseDate", "synopsis": "synopsis", "duration": "Duration"}

    for _, row in df.iterrows():
        sid = pd.to_numeric(row.get('no'), errors='coerce')
        if pd.isna(sid) or int(sid) not in by_id: continue
        sid = int(sid)
        obj, old_obj, changed_fields = by_id[sid], copy.deepcopy(by_id[sid]), {}

        for col, key in COLUMN_MAP.items():
            if col in row and row[col]:
                new_value = row[col]
                if key == 'showImage':
                    local_path = os.path.join(IMAGES_DIR, f"{sid}.jpg")
                    if download_and_save_image(new_value, local_path):
                        new_value = build_absolute_url(local_path)
                        run_context['files_generated']['images'].append(local_path)
                    else: continue
                elif key == 'otherNames': new_value = normalize_list(new_value)
                else: new_value = str(new_value).strip()

                if obj.get(key) != new_value:
                    changed_fields[key] = {'old': obj.get(key), 'new': new_value}
                    obj[key] = new_value
                    obj.setdefault('sitePriorityUsed', {})[key] = "Manual"

        if changed_fields:
            human_readable_changes = [human_readable_field(f) for f in changed_fields]
            obj['updatedDetails'] = f"{', '.join(human_readable_changes)} Updated Manually By Owner"
            obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            report.setdefault('updated', []).append({'old': old_obj, 'new': obj})
            create_diff_backup(old_obj, obj, run_context)
    return report

def excel_to_objects(excel_file_like, sheet_name, existing_by_id, run_context):
    try:
        df = pd.read_excel(excel_file_like, sheet_name=sheet_name, keep_default_na=False)
        df.columns = [c.strip().lower() for c in df.columns]
    except Exception as e:
        # This will catch if the sheet itself doesn't exist
        print(f"INFO: Could not read sheet '{sheet_name}': {e}")
        return []

    report = run_context['report_data'].setdefault(sheet_name, {})
    report.setdefault('data_warnings', [])
    
    try:
        again_idx = [i for i, c in enumerate(df.columns) if "again watched" in c][0]
    except IndexError:
        print(f"ERROR: 'Again Watched' column not found in sheet: {sheet_name}. Skipping sheet.")
        return []

    COLUMN_MAP = {"no": "showID", "series title": "showName", "started date": "watchStartedOn", 
                  "finished date": "watchEndedOn", "year": "releasedYear", "total episodes": "totalEpisodes", 
                  "original language": "nativeLanguage", "language": "watchedLanguage", "ratings": "ratings", 
                  "catagory": "genres", "category": "genres", "original network": "network", "comments": "comments"}
    base_id_map = {"sheet1": 100, "feb 7 2023 onwards": 1000, "sheet2": 3000}
    base_id = base_id_map.get(sheet_name.lower(), 0)

    processed_objects = []
    for index, row in df.iterrows():
        obj = {}
        row_num = index + 2
        
        for col in df.columns[:again_idx]:
            key, val = COLUMN_MAP.get(col, col.strip()), row[col]
            
            if key in ("showID", "releasedYear", "totalEpisodes", "ratings"):
                numeric_val = pd.to_numeric(val, errors='coerce')
                if pd.isna(numeric_val):
                    if val and str(val).strip(): # Only warn if there was actual bad data
                        warning_msg = f"- Row {row_num}: Invalid value '{val}' in numeric column '{col}'. Using 0."
                        report['data_warnings'].append(warning_msg)
                    obj[key] = 0
                else:
                    obj[key] = int(numeric_val)
            elif key == "showName": obj[key] = str(val).strip() if val else None
            elif key in ("watchStartedOn", "watchEndedOn"): obj[key] = ddmmyyyy(val)
            elif key in ("genres", "network"): obj[key] = normalize_list(val)
            else: obj[key] = str(val).strip() if val else None

        if 'showID' in obj and obj['showID'] != 0:
            obj['showID'] += base_id

        if not obj.get("showID") or not obj.get("showName"):
            continue

        obj["againWatchedDates"] = [ddmmyyyy(d) for d in row[again_idx:] if ddmmyyyy(d)]
        obj["showType"] = "Mini Drama" if "mini" in sheet_name.lower() else "Drama"
        if obj.get("nativeLanguage", "").lower() in ("korean", "korea"): obj["country"] = "South Korea"
        
        sid = obj['showID']
        existing = existing_by_id.get(sid)
        
        if existing is None:
            obj['updatedDetails'] = "First Time Uploaded"
            obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            site_priority = SITE_PRIORITY_BY_LANGUAGE.get(obj.get('nativeLanguage','').lower(), SITE_PRIORITY_BY_LANGUAGE['default'])
            obj = fetch_and_populate_metadata(obj, site_priority, run_context)
        else:
            for field in LOCKED_FIELDS_AFTER_CREATION:
                if field in existing: obj[field] = existing[field]
        
        obj["topRatings"] = (obj.get("ratings", 0)) * (len(obj.get("againWatchedDates", [])) + 1) * 100
        final_obj = {**copy.deepcopy(JSON_OBJECT_TEMPLATE), **obj}
        processed_objects.append(final_obj)
        
    return processed_objects

def save_metadata_backup(new_obj, run_context):
    fetched_fields, spu = {}, new_obj.get('sitePriorityUsed', {})
    for key, site in spu.items():
        if site: fetched_fields[key] = {"value": new_obj.get(key), "source": site}
    if not fetched_fields: return
    backup_data = {"scriptVersion": SCRIPT_VERSION, "runID": run_context['run_id'],
                   "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"),
                   "showID": new_obj['showID'], "showName": new_obj['showName'],
                   "fetchedFields": fetched_fields}
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
        if old_val != new_val: changed_fields[key] = {"old": old_val, "new": new_val}
    if not changed_fields: return
    backup_data = {"scriptVersion": SCRIPT_VERSION, "runID": run_context['run_id'],
                   "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"),
                   "backupType": "partial_diff", "showID": new_obj['showID'],
                   "showName": new_obj['showName'], "releasedYear": new_obj.get('releasedYear'),
                   "updatedDetails": new_obj.get('updatedDetails', 'Record Updated'),
                   "changedFields": changed_fields}
    backup_path = os.path.join(BACKUP_DIR, f"BACKUP_{filename_timestamp()}_{new_obj['showID']}.json")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    with open(backup_path, 'w', encoding='utf-8') as f: json.dump(backup_data, f, indent=4)
    run_context['files_generated']['backups'].append(backup_path)

# ---------------------------- REPORTING --------------------------------------
def write_report(run_context):
    report_path = run_context['report_file_path']
    report_changes = run_context['report_data']
    
    lines = [f"✅ Workflow completed successfully", f"🆔 Run ID: {run_context['run_id']}",
             f"📅 Run Time: {now_ist().strftime('%d %B %Y %I:%M %p (IST)')}",
             f"🕒 Duration: {run_context['duration_str']}", f"⚙️ Script Version: {SCRIPT_VERSION}", ""]
    sep = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    overall_stats = {'created': 0, 'updated': 0, 'skipped': 0, 'deleted': 0, 'warnings': 0, 'images': 0, 'rows': 0}

    for sheet, changes in report_changes.items():
        if not changes: continue
        display_sheet = sheet.replace("sheet", "Sheet ").title()
        lines.extend([sep, f"🗂️ === {display_sheet} — {now_ist().strftime('%d %B %Y')} ==="])
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
        if changes.get('data_warnings'):
            lines.append("\n⚠️ Data Validation Warnings:")
            for item in changes['data_warnings']: lines.append(item)
            overall_stats['warnings'] += len(changes['data_warnings'])
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
            total = sum(stats.get(k, 0) for k in ['created', 'updated', 'skipped'])
            overall_stats['created'] += stats.get('created', 0)
            overall_stats['updated'] += stats.get('updated', 0)
            overall_stats['skipped'] += stats.get('skipped', 0)
            overall_stats['images'] += sum(1 for item in changes.get('fetched_data', []) if "Show Image" in item)
            overall_stats['rows'] += total
            lines.extend([f"\n📊 Summary (Sheet: {display_sheet})", sep, f"🆕 Total Created: {stats.get('created', 0)}",
                          f"🔁 Total Updated: {stats.get('updated', 0)}", f"🚫 Total Skipped: {stats.get('skipped', 0)}",
                          f"⚠️ Total Warnings: {stats.get('data_warnings', 0) + stats.get('fetch_warnings', 0)}",
                          f"  Total Number of Rows: {total}"])
        lines.append("")

    overall_stats['deleted'] = len(run_context['files_generated']['deleted_data'])
    lines.extend([sep, "📊 Overall Summary", sep, f"🆕 Total Created: {overall_stats['created']}",
                  f"🔁 Total Updated: {overall_stats['updated']}", f"🖼️ Total Images Updated: {overall_stats['images']}",
                  f"🚫 Total Skipped: {overall_stats['skipped']}", f"❌ Total Deleted: {overall_stats['deleted']}",
                  f"⚠️ Total Warnings: {overall_stats['warnings']}",
                  f"💾 Backup Files Created: {len(run_context['files_generated']['backups'])}",
                  f"  Grand Total Rows Processed: {overall_stats['rows']}", "",
                  f"💾 Metadata Backups Created: {len(run_context['files_generated']['meta_backups'])}", ""])
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            lines.append(f"📦 Total Objects in seriesData.json: {len(json.load(f))}")
    except Exception: lines.append("📦 Total Objects in seriesData.json: Unknown")
    lines.extend([sep, "🗂️ Folders Generated:", sep])
    for folder, files in run_context['files_generated'].items():
        if files:
            lines.append(f"{folder}/")
            for file_path in files: lines.append(f"    {os.path.basename(file_path)}")
    lines.extend([sep, "🏁 Workflow finished successfully"])
    with open(report_path, 'w', encoding='utf-8') as f: f.write("\n".join(lines))

# ---------------------------- MAIN WORKFLOW -----------------------------------
def main():
    start_time = now_ist()
    run_context = {
        'run_id': run_id_timestamp(), 'start_time_iso': start_time.isoformat(),
        'report_data': {},
        'files_generated': {'backups': [], 'images': [], 'deleted_data': [], 'deleted_images': [],
                            'meta_backups': [], 'reports': []}
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
        try:
            processed_objects = excel_to_objects(io.BytesIO(excel_bytes.getvalue()), sheet, merged_by_id, run_context)
            for new_obj in processed_objects:
                sid, old_obj = new_obj['showID'], merged_by_id.get(new_obj['showID'])
                if old_obj is None:
                    merged_by_id[sid] = new_obj
                    save_metadata_backup(new_obj, run_context)
                    run_context['report_data'][sheet].setdefault('created', []).append(new_obj)
                    missing = [human_readable_field(k) for k, v in new_obj.items() if (v is None or v == []) and k not in ['comments', 'againWatchedDates']]
                    fetched = [human_readable_field(k) for k, v in new_obj['sitePriorityUsed'].items() if v]
                    if fetched: run_context['report_data'][sheet].setdefault('fetched_data', []).append(f"- {sid} - {new_obj['showName']} -> {', '.join(fetched)} Updated")
                    if missing: run_context['report_data'][sheet].setdefault('fetch_warnings', []).append(f"- {sid} - {new_obj['showName']} -> ⚠️ Missing: {', '.join(missing)} Not Found")
                elif objects_differ(old_obj, new_obj):
                    changes = [human_readable_field(k) for k, v in new_obj.items() if old_obj.get(k) != v and k not in LOCKED_FIELDS_AFTER_CREATION]
                    new_obj['updatedDetails'] = f"{', '.join(changes)} Updated" if changes else "Record Updated"
                    new_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                    merged_by_id[sid] = new_obj
                    create_diff_backup(old_obj, new_obj, run_context)
                    run_context['report_data'][sheet].setdefault('updated', []).append({'old': old_obj, 'new': new_obj})
                else:
                    run_context['report_data'][sheet].setdefault('skipped', []).append(f"{sid} - {old_obj['showName']} ({old_obj.get('releasedYear')})")
        except Exception as e:
            print(f"❌ FATAL ERROR processing sheet '{sheet}': {e}")
            logd(traceback.format_exc())

    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0)), f, indent=4)
        
    end_time = now_ist()
    duration = end_time - datetime.fromisoformat(run_context['start_time_iso'])
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
                fileId=excel_file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
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