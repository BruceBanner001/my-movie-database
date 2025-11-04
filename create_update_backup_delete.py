# ============================================================
# Script: create_update_backup_delete.py
# Author: [BruceBanner001]
# Description:
#   This is the definitive version. It fixes the DDGS crash, removes vague
#   "Updated" messages, and restores correct reporting logic.
#
# Version: v4.7.1 (Definitive Fix for DDGS Crash & Vague Reporting)
# ============================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# --------------------------- VERSION & CONFIG ------------------------
SCRIPT_VERSION = "v4.7.1 (Definitive Fix for DDGS Crash & Vague Reporting)"

JSON_OBJECT_TEMPLATE = {
    "showID": None, "showName": None, "otherNames": [], "showImage": None,
    "watchStartedOn": None, "watchEndedOn": None, "releasedYear": 0,
    "releaseDate": None, "totalEpisodes": 0, "showType": None,
    "nativeLanguage": None, "watchedLanguage": None, "country": None,
    "comments": None, "ratings": 0, "genres": [], "network": [],
    "againWatchedDates": [], "updatedOn": None, "updatedDetails": None,
    "synopsis": None, "topRatings": 0, "Duration": None,
    "sitePriorityUsed": {"showImage": None, "releaseDate": None, "otherNames": None, "duration": None, "synopsis": None}
}

SITE_PRIORITY_BY_LANGUAGE = {
    "korean": { "synopsis": "asianwiki", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist" },
    "chinese": { "synopsis": "mydramalist", "image": "mydramalist", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist" },
    "japanese": { "synopsis": "asianwiki", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "asianwiki" },
    "thai": { "synopsis": "mydramalist", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist" },
    "taiwanese": { "synopsis": "mydramalist", "image": "mydramalist", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist" },
    "default": { "synopsis": "mydramalist", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "asianwiki" }
}

FIELD_NAME_MAP = { "showID": "Show ID", "showName": "Show Name", "otherNames": "Other Names", "showImage": "Show Image", "watchStartedOn": "Watch Started On", "watchEndedOn": "Watch Ended On", "releasedYear": "Released Year", "releaseDate": "Release Date", "totalEpisodes": "Total Episodes", "showType": "Show Type", "nativeLanguage": "Native Language", "watchedLanguage": "Watched Language", "country": "Country", "comments": "Comments", "ratings": "Ratings", "genres": "Category", "network": "Network", "againWatchedDates": "Again Watched Dates", "updatedOn": "Updated On", "updatedDetails": "Updated Details", "synopsis": "Synopsis", "topRatings": "Top Ratings", "Duration": "Duration", "sitePriorityUsed": "Site Priority Used" }
LOCKED_FIELDS_AFTER_CREATION = {'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'updatedOn', 'updatedDetails', 'sitePriorityUsed', 'topRatings'}

# ---------------------------- IMPORTS & GLOBALS ----------------------------
import os, re, sys, json, io, shutil, traceback, copy
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString
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

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def filename_timestamp(): return now_ist().strftime("%d_%B_%Y_%H%M")
def run_id_timestamp(): return now_ist().strftime("RUN_%Y%m%d_%H%M%S")

JSON_FILE, BACKUP_DIR, IMAGES_DIR, DELETE_IMAGES_DIR = "seriesData.json", "backups", "images", "deleted-images"
DELETED_DATA_DIR, REPORTS_DIR, BACKUP_META_DIR = "deleted-data", "reports", "backup-meta-data"
DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
GITHUB_PAGES_URL = os.environ.get("GITHUB_PAGES_URL", "").strip()
SERVICE_ACCOUNT_FILE, EXCEL_FILE_ID_TXT = "GDRIVE_SERVICE_ACCOUNT.json", "EXCEL_FILE_ID.txt"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}

def logd(msg):
    if DEBUG_FETCH: print(f"[DEBUG] {msg}")

def human_readable_field(field): return FIELD_NAME_MAP.get(field, field)
def ddmmyyyy(val):
    if pd.isna(val): return None
    try: dt = pd.to_datetime(str(val).strip(), errors='coerce'); return None if pd.isna(dt) else dt.strftime("%d-%m-%Y")
    except Exception: return None
def normalize_list(val):
    if val is None: return []
    items = [p.strip() for p in str(val).split(',') if p.strip()]
    return sorted([item for item in items if item])
def objects_differ(old, new):
    keys = set(old.keys()) | set(new.keys()) - LOCKED_FIELDS_AFTER_CREATION
    for k in keys:
        # Treat None and empty list as the same for comparison purposes
        old_val = old.get(k) if old.get(k) is not None else []
        new_val = new.get(k) if new.get(k) is not None else []
        if normalize_list(old_val) != normalize_list(new_val):
            return True
    return False

def get_soup_from_search(query):
    logd(f"Searching: {query}")
    if not HAVE_DDGS: logd("DDGS library not available."); return None
    try:
        # [ THE FIX IS HERE ] - Reverted the broken change. DDGS does not take a 'headers' argument here.
        with DDGS() as dd:
            results = list(dd.text(query, max_results=1))
            if not results or not results[0].get('href'): logd("No search results found."); return None
            url = results[0]['href']; logd(f"Found URL: {url}")
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200: return BeautifulSoup(r.text, "html.parser")
            else: logd(f"HTTP Error {r.status_code} for {url}"); return None
    except Exception as e: logd(f"Search/fetch error for '{query}': {e}"); return None

def download_and_save_image(url, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        r = requests.get(url, headers=HEADERS, stream=True, timeout=15)
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
            with Image.open(r.raw) as img:
                img.convert("RGB").resize((600, 900), Image.Resampling.LANCZOS).save(local_path, "JPEG", quality=90)
                logd(f"Image saved to {local_path}"); return True
    except Exception as e: logd(f"Image download failed from {url}: {e}")
    return False
def build_absolute_url(local_path): return f"{GITHUB_PAGES_URL.rstrip('/')}/{local_path.replace(os.sep, '/')}"

def fetch_synopsis_from_asianwiki(s, y):
    soup = get_soup_from_search(f'"{s} {y}" site:asianwiki.com');
    if not soup: return None
    h2 = soup.find('h2', string='Synopsis');
    if not h2: logd("Synopsis heading not found on AsianWiki."); return None
    p_tags = h2.find_next_siblings('p')
    synopsis = " ".join([p.get_text(strip=True) for p in p_tags])
    return f"{synopsis} (Source: AsianWiki)" if synopsis else None
def fetch_image_from_asianwiki(s, y, sid):
    soup = get_soup_from_search(f'"{s} {y}" site:asianwiki.com');
    if not soup: return None
    img = soup.select_one('a.image > img[src]')
    if not img: logd("Image tag not found on AsianWiki."); return None
    if download_and_save_image(img['src'], os.path.join(IMAGES_DIR, f"{sid}.jpg")):
        return build_absolute_url(os.path.join(IMAGES_DIR, f"{sid}.jpg"))
    return None
def fetch_othernames_from_asianwiki(s, y):
    soup = get_soup_from_search(f'"{s} {y}" site:asianwiki.com');
    if not soup: return []
    div = soup.find('div', id='mw-content-text')
    if not div: logd("Main content div not found on AsianWiki."); return []
    text_content = div.get_text(" ", strip=True)
    match = re.search(r"Drama:\s*([^/(\n\r]+)", text_content)
    if match: return normalize_list(match.group(1).strip())
    logd("'Drama:' field not found on AsianWiki.")
    return []
def fetch_duration_from_asianwiki(s, y): return None
def fetch_release_date_from_asianwiki(s, y):
    soup = get_soup_from_search(f'"{s} {y}" site:asianwiki.com');
    if not soup: return None
    for b in soup.find_all('b'):
        if 'Release Date:' in b.get_text():
            release_text = b.next_sibling
            if release_text and isinstance(release_text, NavigableString):
                return release_text.strip()
    logd("'Release Date:' field not found on AsianWiki.")
    return None

def fetch_synopsis_from_mydramalist(s, y):
    soup = get_soup_from_search(f'"{s} {y}" site:mydramalist.com');
    if not soup: return None
    div = soup.select_one('div.show-synopsis, div[itemprop="description"] p')
    if not div: logd("Synopsis element not found on MyDramaList."); return None
    synopsis = div.get_text(strip=True).replace('(Source: MyDramaList)', '').strip()
    return f"{synopsis} (Source: MyDramaList)" if synopsis else None
def fetch_image_from_mydramalist(s, y, sid):
    soup = get_soup_from_search(f'"{s} {y}" site:mydramalist.com');
    if not soup: return None
    img = soup.select_one('.film-cover img[src], .cover img[src]')
    if not img: logd("Image tag not found on MyDramaList."); return None
    if download_and_save_image(img['src'], os.path.join(IMAGES_DIR, f"{sid}.jpg")):
        return build_absolute_url(os.path.join(IMAGES_DIR, f"{sid}.jpg"))
    return None
def fetch_othernames_from_mydramalist(s, y):
    soup = get_soup_from_search(f'"{s} {y}" site:mydramalist.com');
    if not soup: return []
    li = soup.find('li', class_='list-item p-a-0')
    if not li: logd("Container for 'Also Known As' not found on MyDramaList."); return []
    b = li.find('b', string=re.compile(r'Also Known As:'))
    if not b: logd("'Also Known As:' field not found on MyDramaList."); return []
    return normalize_list(b.next_sibling)
def fetch_duration_from_mydramalist(s, y):
    soup = get_soup_from_search(f'"{s} {y}" site:mydramalist.com');
    if not soup: return None
    li = soup.find(lambda t: 'Duration:' in t.get_text() and t.name == 'li');
    if not li: logd("'Duration:' field not found on MyDramaList."); return None
    duration_text = li.get_text().replace('Duration:', '').strip()
    return duration_text.replace("min.", "mins")
def fetch_release_date_from_mydramalist(s, y):
    soup = get_soup_from_search(f'"{s} {y}" site:mydramalist.com');
    if not soup: return None
    li = soup.find(lambda t: 'Aired:' in t.get_text() and t.name == 'li');
    if not li: logd("'Aired:' field not found on MyDramaList."); return None
    return li.get_text().replace('Aired:', '').strip()

FETCH_MAP = {'asianwiki': {'synopsis': fetch_synopsis_from_asianwiki, 'image': fetch_image_from_asianwiki, 'otherNames': fetch_othernames_from_asianwiki, 'duration': fetch_duration_from_asianwiki, 'releaseDate': fetch_release_date_from_asianwiki}, 'mydramalist': {'synopsis': fetch_synopsis_from_mydramalist, 'image': fetch_image_from_mydramalist, 'otherNames': fetch_othernames_from_mydramalist, 'duration': fetch_duration_from_mydramalist, 'releaseDate': fetch_release_date_from_mydramalist}}
def fetch_and_populate_metadata(obj, site_priority, context):
    s_name, s_year, s_id = obj['showName'], obj['releasedYear'], obj['showID']
    spu = obj.setdefault('sitePriorityUsed', {})
    for field in ['synopsis', 'image', 'releaseDate', 'duration', 'otherNames']:
        primary_site, fallback_site = site_priority.get(field), 'mydramalist' if site_priority.get(field) == 'asianwiki' else 'asianwiki'
        result, used_site = None, None
        for site in [primary_site, fallback_site]:
            if site and site in FETCH_MAP and field in FETCH_MAP[site]:
                args = (s_name, s_year, s_id) if field == 'image' else (s_name, s_year)
                result = FETCH_MAP[site][field](*args)
                if result: used_site = site; break
        if result:
            target_key = "Duration" if field == "duration" else field
            obj[target_key] = result
            if field == 'image': context['files_generated']['images'].append(os.path.join(IMAGES_DIR, f"{s_id}.jpg"))
            spu[field] = used_site
    return obj

def process_deletions(excel, json_file, context):
    try: df = pd.read_excel(excel, sheet_name='Deleting Records')
    except ValueError: print("INFO: 'Deleting Records' sheet not found. Skipping deletion step."); return {}, []
    try:
        with open(json_file, 'r', encoding='utf-8') as f: data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): data = []
    by_id = {int(o['showID']): o for o in data if o.get('showID')}
    to_delete = set(pd.to_numeric(df.iloc[:, 0], errors='coerce').dropna().astype(int))
    deleted, report = set(), {}
    for sid in to_delete:
        if sid in by_id:
            obj = by_id.pop(sid); deleted.add(sid); ts = filename_timestamp()
            path = os.path.join(DELETED_DATA_DIR, f"DELETED_{ts}_{sid}.json"); os.makedirs(DELETED_DATA_DIR, exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f: json.dump(obj, f, indent=4)
            context['files_generated']['deleted_data'].append(path)
            report.setdefault('data_deleted', []).append(f"- {sid} -> {obj.get('showName')} ({obj.get('releasedYear')}) -> ✅ Deleted")
            if obj.get('showImage'):
                src = os.path.join(IMAGES_DIR, os.path.basename(obj['showImage']))
                if os.path.exists(src):
                    dest = os.path.join(DELETE_IMAGES_DIR, f"DELETED_{ts}_{sid}.jpg"); os.makedirs(DELETE_IMAGES_DIR, exist_ok=True); shutil.move(src, dest)
                    context['files_generated']['deleted_images'].append(dest)
    if deleted:
        with open(json_file, 'w', encoding='utf-8') as f: json.dump(sorted(list(by_id.values()), key=lambda x: x.get('showID', 0)), f, indent=4)
    return report, list(deleted)

def apply_manual_updates(excel, by_id, context):
    try: df = pd.read_excel(excel, sheet_name='Manual Updates', keep_default_na=False); df.columns = [c.strip().lower() for c in df.columns]
    except ValueError: print("INFO: 'Manual Updates' sheet not found. Skipping."); return {}
    MAP, report = {"no": "showID", "image": "showImage", "other names": "otherNames", "release date": "releaseDate", "synopsis": "synopsis", "duration": "Duration"}, {}
    for _, row in df.iterrows():
        sid = pd.to_numeric(row.get('no'), errors='coerce')
        if pd.isna(sid) or int(sid) not in by_id: continue
        sid = int(sid); obj, old, changed = by_id[sid], copy.deepcopy(by_id[sid]), {}
        for col, key in MAP.items():
            if col in row and row[col]:
                val = row[col]
                if key == 'showImage' and download_and_save_image(val, os.path.join(IMAGES_DIR, f"{sid}.jpg")):
                    val = build_absolute_url(os.path.join(IMAGES_DIR, f"{sid}.jpg")); context['files_generated']['images'].append(os.path.join(IMAGES_DIR, f"{sid}.jpg"))
                elif key == 'otherNames': val = normalize_list(val)
                else: val = str(val).strip()
                if obj.get(key) != val: changed[key] = {'old': obj.get(key), 'new': val}; obj[key] = val; obj.setdefault('sitePriorityUsed', {})[key] = "Manual"
        if changed:
            obj['updatedDetails'] = f"{', '.join([human_readable_field(f) for f in changed])} Updated Manually"; obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            report.setdefault('updated', []).append({'old': old, 'new': obj}); create_diff_backup(old, obj, context)
    return report

def excel_to_objects(excel, sheet, by_id, context):
    try: df = pd.read_excel(excel, sheet_name=sheet, keep_default_na=False); df.columns = [c.strip().lower() for c in df.columns]
    except ValueError: print(f"INFO: Sheet '{sheet}' not found. Skipping."); return []
    report = context['report_data'].setdefault(sheet, {}); report.setdefault('data_warnings', [])
    try: again_idx = [i for i, c in enumerate(df.columns) if "again watched" in c][0]
    except IndexError: print(f"ERROR: 'Again Watched' in '{sheet}' not found. Skipping."); return []
    MAP = {"no": "showID", "series title": "showName", "started date": "watchStartedOn", "finished date": "watchEndedOn", "year": "releasedYear", "total episodes": "totalEpisodes", "original language": "nativeLanguage", "language": "watchedLanguage", "ratings": "ratings", "catagory": "genres", "category": "genres", "original network": "network", "comments": "comments"}
    base_id = {"sheet1": 100, "feb 7 2023 onwards": 1000, "sheet2": 3000}.get(sheet.lower(), 0)
    processed = []
    for index, row in df.iterrows():
        obj, row_num = {}, index + 2
        for col in df.columns[:again_idx]:
            key, val = MAP.get(col, col.strip()), row[col]
            if key in ("showID", "releasedYear", "totalEpisodes", "ratings"):
                num_val = pd.to_numeric(val, errors='coerce')
                if pd.isna(num_val):
                    if val and str(val).strip(): report['data_warnings'].append(f"- Row {row_num}: Invalid value '{val}' in '{col}'. Using 0.")
                    obj[key] = 0
                else: obj[key] = int(num_val)
            else: obj[key] = ddmmyyyy(val) if key in ("watchStartedOn", "watchEndedOn") else normalize_list(val) if key in ("genres", "network") else str(val).strip() if val else None
        if obj.get("showID", 0) != 0: obj['showID'] += base_id
        if not obj.get("showID") or not obj.get("showName"): continue
        obj["againWatchedDates"] = [ddmmyyyy(d) for d in row[again_idx:] if ddmmyyyy(d)]
        obj["showType"] = "Mini Drama" if "mini" in sheet.lower() else "Drama"
        if obj.get("nativeLanguage", "").lower() in ("korean", "korea"): obj["country"] = "South Korea"
        if by_id.get(obj['showID']) is None:
            obj['updatedDetails'] = "First Time Uploaded"; obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            priority = SITE_PRIORITY_BY_LANGUAGE.get(obj.get('nativeLanguage','').lower(), SITE_PRIORITY_BY_LANGUAGE['default'])
            obj = fetch_and_populate_metadata(obj, priority, context)
        else:
            for field in LOCKED_FIELDS_AFTER_CREATION: obj[field] = by_id[obj['showID']].get(field)
        obj["topRatings"] = (obj.get("ratings", 0)) * (len(obj.get("againWatchedDates", [])) + 1) * 100
        processed.append({**copy.deepcopy(JSON_OBJECT_TEMPLATE), **obj})
    return processed

def save_metadata_backup(obj, context):
    fetched = {k: {"value": obj.get(k), "source": s} for k, s in obj.get('sitePriorityUsed', {}).items() if s}
    if not fetched: logd(f"Skipping metadata backup for {obj['showID']}: no new data fetched."); return
    data = {"scriptVersion": SCRIPT_VERSION, "runID": context['run_id'], "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"), "showID": obj['showID'], "showName": obj['showName'], "fetchedFields": fetched}
    path = os.path.join(BACKUP_META_DIR, f"META_{filename_timestamp()}_{obj['showID']}.json"); os.makedirs(BACKUP_META_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
    context['files_generated']['meta_backups'].append(path)

def create_diff_backup(old, new, context):
    changed_fields = {}
    for key, new_val in new.items():
        if key not in LOCKED_FIELDS_AFTER_CREATION:
             old_val = old.get(key)
             if normalize_list(old_val) != normalize_list(new_val):
                 changed_fields[key] = {"old": old_val, "new": new_val}
    if not changed_fields: return
    data = {"scriptVersion": SCRIPT_VERSION, "runID": context['run_id'], "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"), "backupType": "partial_diff", "showID": new['showID'], "showName": new['showName'], "releasedYear": new.get('releasedYear'), "updatedDetails": new.get('updatedDetails', 'Record Updated'), "changedFields": changed_fields}
    path = os.path.join(BACKUP_DIR, f"BACKUP_{filename_timestamp()}_{new['showID']}.json"); os.makedirs(BACKUP_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4)
    context['files_generated']['backups'].append(path)

def write_report(context):
    lines = [f"✅ Workflow completed successfully", f"🆔 Run ID: {context['run_id']}", f"📅 Run Time: {now_ist().strftime('%d %B %Y %I:%M %p (IST)')}", f"🕒 Duration: {context['duration_str']}", f"⚙️ Script Version: {SCRIPT_VERSION}", ""]
    sep, stats = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", {'created': 0, 'updated': 0, 'skipped': 0, 'deleted': 0, 'warnings': 0, 'images': 0, 'rows': 0}
    for sheet, changes in context['report_data'].items():
        if not any(v for k, v in changes.items()): continue
        display_sheet = sheet.replace("sheet", "Sheet ").title(); lines.extend([sep, f"🗂️ === {display_sheet} — {now_ist().strftime('%d %B %Y')} ==="]); lines.append(sep)
        if changes.get('created'): lines.append("\n🆕 Data Created:"); [lines.append(f"- {o['showID']} - {o['showName']} ({o.get('releasedYear')}) -> {o.get('updatedDetails', '')}") for o in changes['created']]
        if changes.get('updated'): lines.append("\n🔁 Data Updated:"); [lines.append(f"✍️ {p['new']['showID']} - {p['new']['showName']} -> {p['new']['updatedDetails']}") for p in changes['updated']]
        if changes.get('data_warnings'): lines.append("\n⚠️ Data Validation Warnings:"); [lines.append(i) for i in changes['data_warnings']]; stats['warnings'] += len(changes['data_warnings'])
        if changes.get('fetched_data'): lines.append("\n🖼️ Fetched Data:"); [lines.append(i) for i in changes['fetched_data']]
        if changes.get('fetch_warnings'): lines.append("\n🕳️ Value Not Found:"); [lines.append(i) for i in changes['fetch_warnings']]; stats['warnings'] += len(changes['fetch_warnings'])
        if changes.get('skipped'): lines.append("\n🚫 Skipped (Unchanged):"); [lines.append(f"- {i}") for i in changes['skipped']]
        if changes.get('data_deleted'): lines.append("\n❌ Data Deleted:"); [lines.append(i) for i in changes['data_deleted']]
        if sheet not in ["Deleting Records", "Manual Updates"]:
            s = {k: len(v) for k, v in changes.items()}; total = sum(s.get(k, 0) for k in ['created', 'updated', 'skipped'])
            stats['created'] += s.get('created', 0); stats['updated'] += s.get('updated', 0); stats['skipped'] += s.get('skipped', 0)
            stats['images'] += sum(1 for i in changes.get('fetched_data', []) if "Image" in i); stats['rows'] += total
            lines.extend([f"\n📊 Summary (Sheet: {display_sheet})", sep, f"🆕 Created: {s.get('created', 0)}", f"🔁 Updated: {s.get('updated', 0)}", f"🚫 Skipped: {s.get('skipped', 0)}", f"⚠️ Warnings: {len(changes.get('data_warnings',[])) + len(changes.get('fetch_warnings',[]))}", f"  Total Rows: {total}"])
        lines.append("")
    stats['deleted'] = len(context['files_generated']['deleted_data'])
    lines.extend([sep, "📊 Overall Summary", sep, f"🆕 Total Created: {stats['created']}", f"🔁 Total Updated: {stats['updated']}", f"🖼️ Total Images Updated: {stats['images']}", f"🚫 Total Skipped: {stats['skipped']}", f"❌ Total Deleted: {stats['deleted']}", f"⚠️ Total Warnings: {stats['warnings']}", f"💾 Backup Files: {len(context['files_generated']['backups'])}", f"  Grand Total Rows: {stats['rows']}", "", f"💾 Metadata Backups: {len(context['files_generated']['meta_backups'])}", ""])
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f: lines.append(f"📦 Total Objects in {JSON_FILE}: {len(json.load(f))}")
    except Exception: lines.append(f"📦 Total Objects in {JSON_FILE}: Unknown")
    lines.extend([sep, "🗂️ Folders Generated:", sep])
    for folder, files in context['files_generated'].items():
        if files: lines.append(f"{folder}/"); [lines.append(f"    {os.path.basename(p)}") for p in files]
    lines.extend([sep, "🏁 Workflow finished successfully"])
    with open(context['report_file_path'], 'w', encoding='utf-8') as f: f.write("\n".join(lines))

def main():
    start_time = now_ist()
    context = {'run_id': run_id_timestamp(), 'start_time_iso': start_time.isoformat(), 'report_data': {}, 'files_generated': {'backups': [], 'images': [], 'deleted_data': [], 'deleted_images': [], 'meta_backups': [], 'reports': []}}
    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)): print("❌ Missing GDrive credentials."); sys.exit(1)
    try:
        with open(EXCEL_FILE_ID_TXT, 'r') as f: excel_id = f.read().strip()
        if not excel_id: raise ValueError("EXCEL_FILE_ID.txt is empty.")
    except Exception as e: print(f"❌ Error with Excel ID file: {e}"); sys.exit(1)
    print(f"🚀 Running Script — Version {SCRIPT_VERSION} | Run ID: {context['run_id']}")
    excel_bytes = fetch_excel_from_gdrive_bytes(excel_id, SERVICE_ACCOUNT_FILE)
    if not excel_bytes: print("❌ Could not fetch Excel file."); sys.exit(1)
    
    del_report, _ = process_deletions(io.BytesIO(excel_bytes.getvalue()), JSON_FILE, context)
    if del_report: context['report_data']['Deleting Records'] = del_report
    
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f: current_objects = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): current_objects = []
    
    merged_by_id = {o['showID']: o for o in current_objects if o.get('showID')}
    manual_report = apply_manual_updates(io.BytesIO(excel_bytes.getvalue()), merged_by_id, context)
    if manual_report: context['report_data']['Manual Updates'] = manual_report
    
    sheets_to_process = [s.strip() for s in os.environ.get("SHEETS", "Sheet1").split(';') if s.strip()]
    
    for sheet in sheets_to_process:
        try:
            report = context['report_data'].setdefault(sheet, {'created': [], 'updated': [], 'skipped': [], 'fetched_data': [], 'fetch_warnings': [], 'data_warnings': []})
            processed_objects = excel_to_objects(io.BytesIO(excel_bytes.getvalue()), sheet, merged_by_id, context)
            
            for new_obj in processed_objects:
                sid, old_obj = new_obj['showID'], merged_by_id.get(new_obj['showID'])
                if old_obj is None:
                    report['created'].append(new_obj)
                    merged_by_id[sid] = new_obj
                    save_metadata_backup(new_obj, context)
                    missing = [human_readable_field(k) for k,v in new_obj.items() if (v is None or v==[]) and k not in ['comments','againWatchedDates']]
                    fetched = [human_readable_field(k) for k,v in new_obj['sitePriorityUsed'].items() if v]
                    if fetched: report['fetched_data'].append(f"- {sid} - {new_obj['showName']} -> Fetched: {', '.join(fetched)}")
                    if missing: report['fetch_warnings'].append(f"- {sid} - {new_obj['showName']} -> ⚠️ Missing: {', '.join(missing)}")
                elif objects_differ(old_obj, new_obj):
                    changes = [human_readable_field(k) for k, v in new_obj.items() if old_obj.get(k) != v and k not in LOCKED_FIELDS_AFTER_CREATION]
                    # [ THE FIX IS HERE ] - Only create an update if there are actual changes.
                    if changes:
                        new_obj['updatedDetails'] = f"{', '.join(changes)} Updated"
                        new_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                        report['updated'].append({'old': old_obj, 'new': new_obj})
                        merged_by_id[sid] = new_obj
                        create_diff_backup(old_obj, new_obj, context)
                    else:
                        # If objects_differ was true but no specific fields changed, it's a skip.
                        report['skipped'].append(f"- {sid} - {old_obj['showName']} ({old_obj.get('releasedYear')})")
                else:
                    report['skipped'].append(f"- {sid} - {old_obj['showName']} ({old_obj.get('releasedYear')})")
        except Exception as e:
            print(f"❌ UNEXPECTED FATAL ERROR processing sheet '{sheet}': {e}")
            logd(traceback.format_exc())
            
    with open(JSON_FILE, 'w', encoding='utf-8') as f: json.dump(sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0)), f, indent=4)
    
    end_time = now_ist(); duration = end_time - datetime.fromisoformat(context['start_time_iso'])
    context['duration_str'] = f"{duration.seconds // 60} min {duration.seconds % 60} sec"
    report_path = os.path.join(REPORTS_DIR, f"Report_{filename_timestamp()}.txt"); os.makedirs(REPORTS_DIR, exist_ok=True)
    context['report_file_path'] = report_path; context['files_generated']['reports'].append(report_path)
    
    write_report(context)
    print(f"✅ Report written -> {report_path}")
    print("\nAll done.")

def fetch_excel_from_gdrive_bytes(file_id, creds_path):
    if not HAVE_GOOGLE_API: print("ℹ️ Google API packages not installed."); return None
    try:
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        service = build('drive', 'v3', credentials=creds)
        try: request = service.files().get_media(fileId=file_id)
        except Exception: request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        fh = io.BytesIO(); downloader = MediaIoBaseDownload(fh, request); done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0); return fh
    except Exception as e: logd(f"Google Drive fetch failed: {e}\n{traceback.format_exc()}"); return None

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ A fatal, unexpected error occurred: {e}")
        logd(traceback.format_exc())
        sys.exit(1)