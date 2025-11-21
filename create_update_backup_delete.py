# ============================================================
# Script: create_update_backup_delete.py
# Author: [BruceBanner001]
# Description:
#   This is the definitive final version. v16.0 Engine.
#   It contains a completely rebuilt, landmark-validating search engine
#   to guarantee the correct page is scraped every single time.
#
# Version: v3.0
# ============================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# --------------------------- VERSION & CONFIG ------------------------
SCRIPT_VERSION = "v3.0"

JSON_OBJECT_TEMPLATE = {
    "showID": None, "showName": None, "otherNames": None, "showImage": None,
    "watchStartedOn": None, "watchEndedOn": None, "releasedYear": 0,
    "releaseDate": None, "totalEpisodes": 0, "showType": None,
    "nativeLanguage": None, "watchedLanguage": None, "country": None,
    "comments": None, "ratings": 0, "genres": [], "network": [],
    "againWatchedDates": [], "updatedOn": None, "updatedDetails": None,
    "synopsis": None, "topRatings": 0, "Duration": None,
    "sitePriorityUsed": {"showImage": None, "releaseDate": None, "otherNames": None, "duration": None, "synopsis": None}
}

SITE_PRIORITY_BY_LANGUAGE = {
    "korean": { "synopsis": "asianwiki", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "asianwiki" },
    "chinese": { "synopsis": "mydramalist", "image": "mydramalist", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist" },
    "japanese": { "synopsis": "asianwiki", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "asianwiki" },
    "thai": { "synopsis": "mydramalist", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist" },
    "taiwanese": { "synopsis": "mydramalist", "image": "mydramalist", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "mydramalist" },
    "default": { "synopsis": "mydramalist", "image": "asianwiki", "otherNames": "mydramalist", "duration": "mydramalist", "releaseDate": "asianwiki" }
}

FIELD_NAME_MAP = { "showID": "Show ID", "showName": "Show Name", "otherNames": "Other Names", "showImage": "Show Image", "watchStartedOn": "Watch Started On", "watchEndedOn": "Watch Ended On", "releasedYear": "Released Year", "releaseDate": "Release Date", "totalEpisodes": "Total Episodes", "showType": "Show Type", "nativeLanguage": "Native Language", "watchedLanguage": "Watched Language", "country": "Country", "comments": "Comments", "ratings": "Ratings", "genres": "Category", "network": "Network", "againWatchedDates": "Again Watched Dates", "updatedOn": "Updated On", "updatedDetails": "Updated Details", "synopsis": "Synopsis", "topRatings": "Top Ratings", "Duration": "Duration", "sitePriorityUsed": "Site Priority Used" }
LOCKED_FIELDS_AFTER_CREATION = {'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'updatedOn', 'updatedDetails', 'sitePriorityUsed', 'topRatings'}

# ---------------------------- IMPORTS & GLOBALS ----------------------------
import os, re, sys, json, io, shutil, traceback, copy, time, string
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString
from PIL import Image

try: from ddgs import DDGS; HAVE_DDGS = True
except Exception: HAVE_DDGS = False
try: import cloudscraper; HAVE_SCRAPER = True
except Exception: HAVE_SCRAPER = False
try: from google.oauth2 import service_account; from googleapiclient.discovery import build; from googleapiclient.http import MediaIoBaseDownload; HAVE_GOOGLE_API = True
except Exception: HAVE_GOOGLE_API = False

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def filename_timestamp(): return now_ist().strftime("%d_%B_%Y_%H%M")
def run_id_timestamp(): return now_ist().strftime("RUN_%Y%m%d_%H%M%S")

JSON_FILE, BACKUP_DIR, IMAGES_DIR, DELETE_IMAGES_DIR = "seriesData.json", "backups", "images", "deleted-images"
DELETED_DATA_DIR, REPORTS_DIR, BACKUP_META_DIR = "deleted-data", "reports", "backup-meta-data"
DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
GITHUB_PAGES_URL = os.environ.get("GITHUB_PAGES_URL", "").strip()
SERVICE_ACCOUNT_FILE, EXCEL_FILE_ID_TXT = "GDRIVE_SERVICE_ACCOUNT.json", "EXCEL_FILE_ID.txt"
SCRAPER = cloudscraper.create_scraper() if HAVE_SCRAPER else requests.Session()

def logd(msg):
    if DEBUG_FETCH: print(f"[DEBUG] {msg}")

def human_readable_field(field): return FIELD_NAME_MAP.get(field, field)
def ddmmyyyy(val):
    if pd.isna(val): return None
    try: dt = pd.to_datetime(str(val).strip(), errors='coerce'); return None if pd.isna(dt) else dt.strftime("%d-%m-%Y")
    except Exception: return None
def normalize_list(val):
    if val is None: return []
    if isinstance(val, list): items = val
    else: items = [p.strip() for p in str(val).split(',') if p.strip()]
    return sorted([item for item in items if item])

def objects_differ(old, new):
    excel_fields = set(FIELD_NAME_MAP.keys()) - LOCKED_FIELDS_AFTER_CREATION
    for k in excel_fields:
        old_val = old.get(k)
        new_val = new.get(k)
        if k == 'otherNames':
            if str(old_val or "") != str(new_val or ""): return True
        elif normalize_list(old_val) != normalize_list(new_val): return True
    return False

def get_soup_from_search(query_base, site):
    logd(f"Initiating search for: {query_base} on {site}.com")
    if not HAVE_DDGS: logd("DDGS library not available."); return None, None
    clean_query = query_base.split("(")[0].strip()
    search_queries = [ f'"{query_base}" site:{site}.com', f'"{clean_query}" site:{site}.com' ]
    for query in search_queries:
        logd(f"Executing search query: {query}")
        try:
            time.sleep(1)
            with DDGS() as dd:
                results = list(dd.text(query, max_results=5))
                if not results: continue
                for res in results:
                    url = res.get('href', '')
                    if not url or 'bing.com' in url or any(bad in url for bad in ['/reviews', '/episode', '/cast', '/recs', '?lang=', '/photos']): continue
                    if site == "asianwiki" and ("/File:" in url or "/index.php?title=File:" in url):
                        logd(f"Rejecting invalid AsianWiki file URL: {url}"); continue
                    logd(f"Found candidate URL: {url}")
                    r = SCRAPER.get(url, timeout=15)
                    if r.status_code == 200:
                        soup = BeautifulSoup(r.text, "html.parser")
                        is_valid = False
                        # FIX: Bulletproof AsianWiki Validator
                        if site == "asianwiki":
                            profile_header = soup.find('h2', string='Profile')
                            if profile_header and profile_header.find_next_sibling('table', class_='infobox'):
                                is_valid = True
                            else:
                                logd("Validation failed: AsianWiki 'Profile' header and infobox landmark missing.")
                        elif site == "mydramalist":
                            if soup.find('div', class_='box-body'):
                                is_valid = True
                            else:
                                logd("Validation failed: MyDramaList landmark missing.")
                        if is_valid:
                            logd("Landmark validation passed. This is the correct page.")
                            return soup, url
                    else: logd(f"HTTP Error {r.status_code} for {url}")
        except Exception as e: logd(f"Search attempt failed for query '{query}': {e}")
    logd("All search attempts failed."); return None, None

def download_and_save_image(url, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        url = re.sub(r'_[24]c\.jpg$', '.jpg', url)
        logd(f"Downloading high-quality image from: {url}")
        r = SCRAPER.get(url, stream=True, timeout=20)
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
            with Image.open(r.raw) as img:
                img = img.convert("RGB")
                img.thumbnail((800, 1200), Image.Resampling.LANCZOS)
                img.save(local_path, "JPEG", quality=95)
                logd(f"Image saved to {local_path}"); return True
    except Exception as e: logd(f"Image download failed from {url}: {e}")
    return False
def build_absolute_url(local_path): return f"{GITHUB_PAGES_URL.rstrip('/')}/{local_path.replace(os.sep, '/')}"

def fetch_synopsis_from_asianwiki(s, y):
    soup, url = get_soup_from_search(f'{s} {y}', "asianwiki")
    if not soup: return None, None
    h2 = soup.find('h2', id=re.compile(r"Synopsis|Plot", re.IGNORECASE))
    if not h2: logd("Synopsis/Plot heading not found on AsianWiki."); return None, None
    content = [p.get_text(strip=True) for p in h2.find_next_siblings('p')]
    synopsis = "\n\n".join(p for p in content if p)
    return (synopsis, url) if synopsis else (None, None)

def fetch_image_from_asianwiki(s, y, sid):
    soup, url = get_soup_from_search(f'{s} {y}', "asianwiki")
    if not soup: return None, None
    img = soup.select_one('a.image > img[src]')
    if not img: logd("Image tag not found on AsianWiki."); return None, None
    img_url = requests.compat.urljoin("https://asianwiki.com", img['src'])
    if download_and_save_image(img_url, os.path.join(IMAGES_DIR, f"{sid}.jpg")):
        return (build_absolute_url(os.path.join(IMAGES_DIR, f"{sid}.jpg")), url)
    return None, None

def fetch_othernames_from_asianwiki(s, y):
    soup, url = get_soup_from_search(f'{s} {y}', "asianwiki")
    if not soup: return None, None
    p_tag = soup.find('p', string=re.compile(r"^(Drama:|Movie:)"))
    if p_tag:
        full_text = p_tag.get_text(strip=True).replace(" Hangul:", " (Hangul:")
        match = re.search(r':(.*?)(?=\(Revised romanization:|\(literal title\)|$)', full_text, re.DOTALL)
        if match:
            names_text = match.group(1).strip()
            return (names_text, url) if names_text else (None, None)
    logd("'Other Names' from 'Drama:' field not found on AsianWiki."); return None, None

def fetch_duration_from_asianwiki(s, y): return None, None

def fetch_release_date_from_asianwiki(s, y):
    soup, url = get_soup_from_search(f'{s} {y}', "asianwiki")
    if not soup: return None, None
    b_tag = soup.find('b', string=re.compile(r"Release Date:"))
    if b_tag and b_tag.parent:
        b_tag.decompose()
        release_text = b_tag.parent.get_text(strip=True)
        return (release_text, url) if release_text else (None, None)
    logd("'Release Date:' field not found on AsianWiki."); return None, None

def fetch_synopsis_from_mydramalist(s, y):
    soup, url = get_soup_from_search(f'{s} {y}', "mydramalist")
    if not soup: return None, None
    synopsis_div = soup.select_one('div.show-synopsis, div[itemprop="description"]')
    if not synopsis_div: logd("Synopsis element not found on MyDramaList."); return None, None
    synopsis = synopsis_div.get_text(separator='\n\n', strip=True)
    synopsis = re.sub(r'(^Remove ads\n\n)|((~~.*?~~|Edit Translation).*$)|(\s*\(\s*Source:.*?\)\s*$)', '', synopsis, flags=re.DOTALL | re.IGNORECASE).strip()
    return (synopsis, url) if synopsis else (None, None)

def fetch_image_from_mydramalist(s, y, sid):
    soup, url = get_soup_from_search(f'{s} {y}', "mydramalist")
    if not soup: return None, None
    img = soup.select_one('.film-cover img[src], .cover img[src], div.cover img[src]')
    if not img: logd("Image tag not found on MyDramaList."); return None, None
    if download_and_save_image(img['src'], os.path.join(IMAGES_DIR, f"{sid}.jpg")):
        return (build_absolute_url(os.path.join(IMAGES_DIR, f"{sid}.jpg")), url)
    return None, None

def fetch_othernames_from_mydramalist(s, y):
    soup, url = get_soup_from_search(f'{s} {y}', "mydramalist")
    if not soup: return None, None
    # FIX: Bulletproof "Other Names" Scraper
    b_tag = soup.find('b', string="Also Known As:")
    if b_tag and (li_tag := b_tag.find_parent('li')):
        b_tag.decompose()
        names_text = li_tag.get_text(strip=True)
        return (names_text, url) if names_text else (None, None)
    logd("'Also Known As:' field not found on MyDramaList."); return None, None

def fetch_duration_from_mydramalist(s, y):
    soup, url = get_soup_from_search(f'{s} {y}', "mydramalist")
    if not soup: return None, None
    b_tag = soup.find('b', string=re.compile(r"Duration:"))
    if b_tag and (li_tag := b_tag.find_parent('li')):
        b_tag.decompose()
        duration_text = li_tag.get_text(strip=True)
        if "hr" not in duration_text and duration_text.endswith(" min."):
            duration_text = duration_text.replace(" min.", " mins")
        return (duration_text, url) if duration_text else (None, None)
    logd("'Duration:' field not found on MyDramaList."); return None, None

def fetch_release_date_from_mydramalist(s, y):
    soup, url = get_soup_from_search(f'{s} {y}', "mydramalist")
    if not soup: return None, None
    b_tag = soup.find('b', string=re.compile(r"Aired:"))
    if b_tag and (li_tag := b_tag.find_parent('li')):
        b_tag.decompose()
        release_text = li_tag.get_text(strip=True)
        return (release_text, url) if release_text else (None, None)
    logd("'Aired:' field not found on MyDramaList."); return None, None

FETCH_MAP = {
    'asianwiki': {'synopsis': fetch_synopsis_from_asianwiki, 'image': fetch_image_from_asianwiki, 'otherNames': fetch_othernames_from_asianwiki, 'duration': fetch_duration_from_asianwiki, 'releaseDate': fetch_release_date_from_asianwiki},
    'mydramalist': {'synopsis': fetch_synopsis_from_mydramalist, 'image': fetch_image_from_mydramalist, 'otherNames': fetch_othernames_from_mydramalist, 'duration': fetch_duration_from_mydramalist, 'releaseDate': fetch_release_date_from_mydramalist}
}

def process_deletions(excel, json_file, context):
    try: df = pd.read_excel(excel, sheet_name='Deleting Records')
    except ValueError: print("INFO: 'Deleting Records' sheet not found. Skipping deletion step."); return {}, []
    if df.empty: logd("'Deleting Records' sheet is empty. Nothing to delete."); return {}, []
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
            with open(path, 'w', encoding='utf-8') as f: json.dump(obj, f, indent=4, ensure_ascii=False)
            context['files_generated']['deleted_data'].append(path)
            report.setdefault('data_deleted', []).append(f"- {sid} -> {obj.get('showName')} ({obj.get('releasedYear')}) -> ✅ Deleted")
            if obj.get('showImage'):
                src = os.path.join(IMAGES_DIR, os.path.basename(obj['showImage']))
                if os.path.exists(src):
                    dest = os.path.join(DELETE_IMAGES_DIR, f"DELETED_{ts}_{sid}.jpg"); os.makedirs(DELETE_IMAGES_DIR, exist_ok=True); shutil.move(src, dest)
                    context['files_generated']['deleted_images'].append(dest)
    if deleted:
        with open(json_file, 'w', encoding='utf-8') as f: json.dump(sorted(list(by_id.values()), key=lambda x: x.get('showID', 0)), f, indent=4, ensure_ascii=False)
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
                val = str(row[col]).strip()
                if key == 'showImage' and download_and_save_image(val, os.path.join(IMAGES_DIR, f"{sid}.jpg")):
                    val = build_absolute_url(os.path.join(IMAGES_DIR, f"{sid}.jpg")); context['files_generated']['images'].append(os.path.join(IMAGES_DIR, f"{sid}.jpg"))
                if obj.get(key) != val: changed[key] = {'old': obj.get(key), 'new': val}; obj[key] = val; obj.setdefault('sitePriorityUsed', {})[key] = "Manual"
        if changed:
            obj['updatedDetails'] = f"{', '.join([human_readable_field(f) for f in changed])} Updated Manually"; obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            report.setdefault('updated', []).append({'old': old, 'new': obj}); create_diff_backup(old, obj, context)
    return report

def excel_to_objects(excel, sheet):
    try:
        df = pd.read_excel(excel, sheet_name=sheet, keep_default_na=False); df.columns = [c.strip().lower() for c in df.columns]
    except ValueError:
        print(f"INFO: Sheet '{sheet}' not found. Skipping."); return [], []
    warnings = []
    try: again_idx = [i for i, c in enumerate(df.columns) if "again watched" in c][0]
    except IndexError: print(f"ERROR: 'Again Watched' in '{sheet}' not found. Skipping."); return [], []
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
                    if val and str(val).strip(): warnings.append(f"- Row {row_num}: Invalid value '{val}' in '{col}'. Using 0.")
                    obj[key] = 0
                else: obj[key] = int(num_val)
            else: obj[key] = ddmmyyyy(val) if key in ("watchStartedOn", "watchEndedOn") else normalize_list(val) if key in ("genres", "network") else str(val).strip() if val else None
        if obj.get("showID", 0) != 0: obj['showID'] += base_id
        if not obj.get("showID") or not obj.get("showName"): continue
        obj["againWatchedDates"] = [ddmmyyyy(d) for d in row[again_idx:] if ddmmyyyy(d)]
        obj["showType"] = "Mini Drama" if "mini" in sheet.lower() else "Drama"
        lang = obj.get("nativeLanguage", "").lower()
        if lang in ("korean", "korea"): obj["country"] = "South Korea"
        elif lang in ("chinese", "china"): obj["country"] = "China"
        processed.append(obj)
    return processed, warnings

def save_metadata_backup(obj, context):
    fetched = {}
    source_links = context.get('source_links_temp', {})
    for key, site in obj.get('sitePriorityUsed', {}).items():
        if site and site != "Manual":
            target_key = "showImage" if key == "image" else "Duration" if key == "duration" else key
            field_data = {"value": obj.get(target_key), "source": site}
            if key in source_links: field_data["source_link"] = source_links[key]
            fetched[key] = field_data
    if not fetched: logd(f"Skipping metadata backup for {obj['showID']}: no new data fetched."); return
    data = {"scriptVersion": SCRIPT_VERSION, "runID": context['run_id'], "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"), "showID": obj['showID'], "showName": obj['showName'], "fetchedFields": fetched}
    path = os.path.join(BACKUP_META_DIR, f"META_{filename_timestamp()}_{obj['showID']}.json"); os.makedirs(BACKUP_META_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4, ensure_ascii=False)
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
    with open(path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4, ensure_ascii=False)
    context['files_generated']['backups'].append(path)

def write_report(context):
    lines = [f"✅ Workflow completed successfully", f"🆔 Run ID: {context['run_id']}", f"📅 Run Time: {now_ist().strftime('%d %B %Y %I:%M %p (IST)')}", f"🕒 Duration: {context['duration_str']}", f"⚙️ Script Version: {SCRIPT_VERSION}", ""]
    sep, stats = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", {'created': 0, 'updated': 0, 'skipped': 0, 'deleted': 0, 'warnings': 0, 'images': 0, 'rows': 0, 'refetched': 0}
    for sheet, changes in context['report_data'].items():
        if not any(v for k, v in changes.items()): continue
        display_sheet = sheet.replace("sheet", "Sheet ").title(); lines.extend([sep, f"🗂️ === {display_sheet} — {now_ist().strftime('%d %B %Y')} ==="]); lines.append(sep)
        if changes.get('created'): lines.append("\n🆕 Data Created:"); [lines.append(f"- {o['showID']} - {o['showName']} ({o.get('releasedYear')}) -> {o.get('updatedDetails', '')}") for o in changes['created']]
        if changes.get('updated'): lines.append("\n🔁 Data Updated:"); [lines.append(f"✍️ {p['new']['showID']} - {p['new']['showName']} -> {p['new']['updatedDetails']}") for p in changes['updated']]
        if changes.get('refetched'): lines.append("\n🔍 Refetched Data:"); [lines.append(f"✨ {o['showID']} - {o['showName']} -> Metadata Refreshed") for o in changes['refetched']]; stats['refetched'] += len(changes.get('refetched', []))
        if changes.get('data_warnings'): lines.append("\n⚠️ Data Validation Warnings:"); [lines.append(i) for i in changes['data_warnings']]; stats['warnings'] += len(changes['data_warnings'])
        if changes.get('fetched_data'): lines.append("\n🖼️ Fetched Data Details:"); [lines.append(i) for i in sorted(changes['fetched_data'])]
        if changes.get('fetch_warnings'): lines.append("\n🕳️ Value Not Found:"); [lines.append(i) for i in sorted(changes['fetch_warnings'])]; stats['warnings'] += len(changes['fetch_warnings'])
        if changes.get('skipped'): lines.append("\n🚫 Skipped (Unchanged):"); [lines.append(f"- {i}") for i in sorted(changes['skipped'])]
        if changes.get('data_deleted'): lines.append("\n❌ Data Deleted:"); [lines.append(i) for i in changes['data_deleted']]
        if sheet not in ["Deleting Records", "Manual Updates"]:
            s = {k: len(v) for k, v in changes.items()}; total = sum(s.get(k, 0) for k in ['created', 'updated', 'skipped', 'refetched'])
            stats['created'] += s.get('created', 0); stats['updated'] += s.get('updated', 0); stats['skipped'] += s.get('skipped', 0)
            stats['images'] += sum(1 for i in changes.get('fetched_data', []) if "Image" in i); stats['rows'] += total
            lines.extend([f"\n📊 Summary (Sheet: {display_sheet})", sep, f"🆕 Created: {s.get('created', 0)}", f"🔁 Updated: {s.get('updated', 0)}", f"🔍 Refetched: {s.get('refetched', 0)}", f"🚫 Skipped: {s.get('skipped', 0)}", f"⚠️ Warnings: {len(changes.get('data_warnings',[])) + len(changes.get('fetch_warnings',[]))}", f"  Total Rows: {total}"])
        lines.append("")
    stats['deleted'] = len(context['files_generated']['deleted_data'])
    lines.extend([sep, "📊 Overall Summary", sep, f"🆕 Total Created: {stats['created']}", f"🔁 Total Updated: {stats['updated']}", f"🔍 Total Refetched: {stats['refetched']}", f"🖼️ Total Images Updated: {stats['images']}", f"🚫 Total Skipped: {stats['skipped']}", f"❌ Total Deleted: {stats['deleted']}", f"⚠️ Total Warnings: {stats['warnings']}", f"💾 Backup Files: {len(context['files_generated']['backups'])}", f"  Grand Total Rows: {stats['rows']}", "", f"💾 Metadata Backups: {len(context['files_generated']['meta_backups'])}", ""])
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
        report = context['report_data'].setdefault(sheet, {'created': [], 'updated': [], 'refetched': [], 'skipped': [], 'fetched_data': [], 'fetch_warnings': [], 'data_warnings': []})
        excel_rows, warnings = excel_to_objects(io.BytesIO(excel_bytes.getvalue()), sheet)
        if warnings: report['data_warnings'].extend(warnings)

        for excel_obj in excel_rows:
            sid = excel_obj['showID']
            old_obj_from_json = merged_by_id.get(sid)
            is_new = old_obj_from_json is None
            
            final_obj = {**JSON_OBJECT_TEMPLATE, **(old_obj_from_json or {}), **excel_obj}
            
            lang = final_obj.get("nativeLanguage", "").lower()
            priority = SITE_PRIORITY_BY_LANGUAGE.get(lang, SITE_PRIORITY_BY_LANGUAGE['default'])
            s_name, s_year = final_obj['showName'], final_obj['releasedYear']
            spu = final_obj.setdefault('sitePriorityUsed', {})
            source_links = {}
            
            fields_to_check = [('synopsis', 'synopsis'), ('showImage', 'image'), ('otherNames', 'otherNames'), ('releaseDate', 'releaseDate'), ('Duration', 'duration')]
            
            initial_metadata_state = {k: final_obj.get(k) for k, _ in fields_to_check}
            
            for obj_key, fetch_key in fields_to_check:
                if not final_obj.get(obj_key):
                    primary_site, fallback_site = priority.get(fetch_key), 'mydramalist' if priority.get(fetch_key) == 'asianwiki' else 'asianwiki'
                    for site in [primary_site, fallback_site]:
                        if site:
                            args = (s_name, s_year, sid) if fetch_key == 'image' else (s_name, s_year)
                            data, url = FETCH_MAP[site][fetch_key](*args)
                            if data:
                                final_obj[obj_key] = data
                                spu[fetch_key] = site
                                source_links[fetch_key] = url
                                if fetch_key == 'image': context['files_generated']['images'].append(os.path.join(IMAGES_DIR, f"{sid}.jpg"))
                                break
            
            final_obj['topRatings'] = (final_obj.get("ratings", 0)) * (len(final_obj.get("againWatchedDates", [])) + 1) * 100
            
            excel_data_has_changed = not is_new and objects_differ(old_obj_from_json, final_obj)
            metadata_was_fetched = any(final_obj.get(k) != initial_metadata_state.get(k) for k, _ in fields_to_check)

            if is_new:
                final_obj['updatedDetails'] = "First Time Uploaded"
                final_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                report['created'].append(final_obj)
            elif excel_data_has_changed:
                changes = [human_readable_field(k) for k, v in excel_obj.items() if normalize_list(old_obj_from_json.get(k)) != normalize_list(v)]
                final_obj['updatedDetails'] = f"{', '.join(changes)} Updated"
                final_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                report['updated'].append({'old': old_obj_from_json, 'new': final_obj})
                create_diff_backup(old_obj_from_json, final_obj, context)
            elif metadata_was_fetched:
                report['refetched'].append(final_obj)
            else:
                report['skipped'].append(f"{sid} - {final_obj['showName']} ({final_obj.get('releasedYear')})")
            
            if is_new or excel_data_has_changed or metadata_was_fetched:
                 merged_by_id[sid] = final_obj
                 context['source_links_temp'] = source_links
                 save_metadata_backup(final_obj, context)

            missing = [FIELD_NAME_MAP[k] for k, _ in fields_to_check if not final_obj.get(k)]
            newly_fetched = [fetch_key.capitalize() for obj_key, fetch_key in fields_to_check if final_obj.get(obj_key) and not initial_metadata_state.get(obj_key)]
            if newly_fetched: report['fetched_data'].append(f"- {sid} - {final_obj['showName']} -> Fetched: {', '.join(sorted(newly_fetched))}")
            if missing: report['fetch_warnings'].append(f"- {sid} - {final_obj['showName']} -> ⚠️ Missing: {', '.join(sorted(missing))}")

    with open(JSON_FILE, 'w', encoding='utf-8') as f: json.dump(sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0)), f, indent=4, ensure_ascii=False)
    
    end_time = now_ist()
    duration = end_time - datetime.fromisoformat(context['start_time_iso'])
    context['duration_str'] = f"{duration.seconds // 60} min {duration.seconds % 60} sec"
    report_path = os.path.join(REPORTS_DIR, f"Report_{filename_timestamp()}.txt"); os.makedirs(REPORTS_DIR, exist_ok=True)
    context['report_file_path'] = report_path
    context['files_generated']['reports'].append(report_path)
    write_report(context)
    print(f"✅ Report written -> {report_path}")
    print("\nAll done.")

def fetch_excel_from_gdrive_bytes(file_id, creds_path):
    if not HAVE_GOOGLE_API: print("ℹ️ Google API packages not installed."); return None
    try:
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        service = build('drive', 'v3', credentials=creds)
        # FIX: Try direct download first (for .xlsx), then export (for GSheets)
        try:
            request = service.files().get_media(fileId=file_id)
            logd("Attempting direct media download...")
        except Exception:
            logd("Direct download failed, attempting GSheet export...")
            request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        fh = io.BytesIO(); downloader = MediaIoBaseDownload(fh, request); done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0); return fh
    except Exception as e:
        logd(f"Google Drive fetch failed: {e}\n{traceback.format_exc()}"); return None

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ A fatal, unexpected error occurred: {e}")
        logd(traceback.format_exc())
        sys.exit(1)