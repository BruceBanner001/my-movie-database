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
    items = [p.strip() for p in str(val).split(',') if p.strip()]
    return sorted([item for item in items if item])
def objects_differ(old, new):
    keys = set(old.keys()) | set(new.keys()) - LOCKED_FIELDS_AFTER_CREATION
    for k in keys:
        old_val = old.get(k) if old.get(k) is not None else []
        new_val = new.get(k) if new.get(k) is not None else []
        if normalize_list(old_val) != normalize_list(new_val): return True
    return False

def get_soup_from_search(query_base, site, soup_cache):
    cache_key = f"{query_base}_{site}"
    if cache_key in soup_cache:
        logd(f"Found soup in cache for {query_base} on {site}.")
        return soup_cache[cache_key]

    logd(f"Initiating search for: {query_base} on {site}.com")
    if not HAVE_DDGS: logd("DDGS library not available."); return None, None

    clean_query = query_base.split("(")[0].strip()
    # FIX: Add .com to the site in the search query
    search_queries = [
        f'"{query_base}" site:{site}.com',
        f'"{clean_query}" site:{site}.com',
        f'"{clean_query} drama" site:{site}.com'
    ]

    for query in search_queries:
        logd(f"Executing search query: {query}")
        try:
            time.sleep(1)
            with DDGS() as dd:
                results = list(dd.text(query, max_results=5))
                if not results: continue

                for res in results:
                    url = res.get('href', '')
                    if not url: continue # FIX: Skip if URL is empty
                    if any(bad in url for bad in ['/reviews', '/episode', '/cast', '/recs', '?lang=', '/photos']): continue

                    logd(f"Found candidate URL: {url}")
                    r = SCRAPER.get(url, timeout=15)
                    if r.status_code == 200:
                        soup = BeautifulSoup(r.text, "html.parser")
                        is_valid = False
                        
                        if site == "asianwiki":
                            infobox = soup.find('table', class_='infobox')
                            if infobox:
                                text = infobox.get_text()
                                if any(kw in text for kw in ['Network:', 'Episodes:', 'Release Date:', 'Runtime:', 'Director:', 'Writer:']):
                                    is_valid = True
                                else:
                                    logd("Validation failed: AsianWiki infobox is missing key drama fields. Rejecting.")
                        elif site == "mydramalist":
                            if soup.find('div', class_='box-body'):
                                is_valid = True
                            else:
                                logd("Validation failed: MyDramaList landmark missing. Rejecting.")

                        if is_valid:
                            logd("Landmark validation passed. This is the correct page.")
                            soup_cache[cache_key] = (soup, url)
                            return soup, url
                    else:
                        logd(f"HTTP Error {r.status_code} for {url}")
        except Exception as e:
            logd(f"Search attempt failed for query '{query}': {e}")
            continue
    
    soup_cache[cache_key] = (None, None)
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

def scrape_synopsis_from_asianwiki(soup):
    h2 = soup.find('h2', id=re.compile(r"Synopsis|Plot", re.IGNORECASE))
    if not h2: logd("Synopsis/Plot heading not found on AsianWiki."); return None
    content = []
    for sibling in h2.find_next_siblings():
        if sibling.name == 'h2': break
        if sibling.name == 'p': content.append(sibling.get_text(strip=True))
    synopsis = "\n\n".join(p for p in content if p)
    return synopsis if synopsis else None

def scrape_image_from_asianwiki(soup, sid):
    img = soup.select_one('a.image > img[src]')
    if not img: logd("Image tag not found on AsianWiki."); return None
    img_url = requests.compat.urljoin("https://asianwiki.com", img['src'])
    if download_and_save_image(img_url, os.path.join(IMAGES_DIR, f"{sid}.jpg")):
        return build_absolute_url(os.path.join(IMAGES_DIR, f"{sid}.jpg"))
    return None

def scrape_othernames_from_asianwiki(soup):
    p_tag = soup.find('p', text=re.compile(r"^(Drama:|Movie:)"))
    if not p_tag:
        b_tag = soup.find('b', text=re.compile(r"^(Drama:|Movie:)"))
        if b_tag: p_tag = b_tag.find_parent('p')
    if p_tag:
        match = re.search(r':(.*?)(?=\(Revised romanization:|\(literal title\))', p_tag.get_text(strip=True), re.DOTALL)
        if match:
            return [name.strip() for name in match.group(1).strip().split('/') if name.strip()]
    logd("'Other Names' from 'Drama:' field not found on AsianWiki."); return None

def scrape_release_date_from_asianwiki(soup):
    b_tag = soup.find('b', text=re.compile(r"Release Date:"))
    if b_tag:
        parent = b_tag.parent
        b_tag.decompose()
        return parent.get_text(strip=True)
    logd("'Release Date:' field not found on AsianWiki."); return None

def scrape_synopsis_from_mydramalist(soup):
    synopsis_div = soup.select_one('div.show-synopsis, div[itemprop="description"]')
    if not synopsis_div: logd("Synopsis element not found on MyDramaList."); return None
    synopsis = synopsis_div.get_text(separator='\n\n', strip=True)
    synopsis = re.sub(r'(^Remove ads\n\n)|((~~.*?~~|Edit Translation).*$)|(\s*\(\s*Source:.*?\)\s*$)', '', synopsis, flags=re.DOTALL | re.IGNORECASE).strip()
    return synopsis if synopsis else None

def scrape_image_from_mydramalist(soup, sid):
    img = soup.select_one('.film-cover img[src], .cover img[src], div.cover img[src]')
    if not img: logd("Image tag not found on MyDramaList."); return None
    if download_and_save_image(img['src'], os.path.join(IMAGES_DIR, f"{sid}.jpg")):
        return build_absolute_url(os.path.join(IMAGES_DIR, f"{sid}.jpg"))
    return None

def scrape_othernames_from_mydramalist(soup):
    li_tag = soup.find('li', class_='list-item', text=re.compile(r"Also Known As:"))
    if not li_tag:
        b_tag = soup.find('b', text=re.compile(r"Also Known As:"))
        if b_tag: li_tag = b_tag.find_parent('li')
    if li_tag:
        if b_tag := li_tag.find('b'): b_tag.decompose()
        return [name.strip() for name in li_tag.get_text(strip=True).split(',') if name.strip()]
    logd("'Also Known As:' field not found on MyDramaList."); return None

def scrape_duration_from_mydramalist(soup):
    b_tag = soup.find('b', text=re.compile(r"Duration:"))
    if b_tag and (li_tag := b_tag.find_parent('li')):
        b_tag.decompose()
        duration_text = li_tag.get_text(strip=True)
        return duration_text.replace(" min.", " mins") if "hr" not in duration_text else duration_text
    logd("'Duration:' field not found on MyDramaList."); return None

def scrape_release_date_from_mydramalist(soup):
    b_tag = soup.find('b', text=re.compile(r"Aired:"))
    if b_tag and (li_tag := b_tag.find_parent('li')):
        b_tag.decompose()
        return li_tag.get_text(strip=True)
    logd("'Aired:' field not found on MyDramaList."); return None

SCRAPE_MAP = {
    'asianwiki': {'synopsis': scrape_synopsis_from_asianwiki, 'image': scrape_image_from_asianwiki, 'otherNames': scrape_othernames_from_asianwiki, 'duration': lambda s, sid=None: None, 'releaseDate': scrape_release_date_from_asianwiki},
    'mydramalist': {'synopsis': scrape_synopsis_from_mydramalist, 'image': scrape_image_from_mydramalist, 'otherNames': scrape_othernames_from_mydramalist, 'duration': scrape_duration_from_mydramalist, 'releaseDate': scrape_release_date_from_mydramalist}
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
    try:
        df = pd.read_excel(excel, sheet_name=sheet, keep_default_na=False); df.columns = [c.strip().lower() for c in df.columns]
    except ValueError: print(f"INFO: Sheet '{sheet}' not found. Skipping."); return []
    report = context['report_data'].setdefault(sheet, {}); report.setdefault('data_warnings', [])
    try: again_idx = [i for i, c in enumerate(df.columns) if "again watched" in c][0]
    except IndexError: print(f"ERROR: 'Again Watched' in '{sheet}' not found. Skipping."); return []
    MAP = {"no": "showID", "series title": "showName", "started date": "watchStartedOn", "finished date": "watchEndedOn", "year": "releasedYear", "total episodes": "totalEpisodes", "original language": "nativeLanguage", "language": "watchedLanguage", "ratings": "ratings", "catagory": "genres", "category": "genres", "original network": "network", "comments": "comments"}
    base_id = {"sheet1": 100, "feb 7 2023 onwards": 1000, "sheet2": 3000}.get(sheet.lower(), 0)
    processed, soup_cache = [], {}

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
        lang = obj.get("nativeLanguage", "").lower()
        if lang in ("korean", "korea"): obj["country"] = "South Korea"
        elif lang in ("chinese", "china"): obj["country"] = "China"
        
        source_links = {}
        priority = SITE_PRIORITY_BY_LANGUAGE.get(lang, SITE_PRIORITY_BY_LANGUAGE['default'])
        s_name, s_year, s_id = obj['showName'], obj['releasedYear'], obj['showID']

        is_new_object = by_id.get(s_id) is None
        if is_new_object:
            obj['updatedDetails'] = "First Time Uploaded"; obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            fields_to_fetch_keys = ['synopsis', 'image', 'releaseDate', 'duration', 'otherNames']
        else:
            existing_object = by_id[s_id]
            for field in LOCKED_FIELDS_AFTER_CREATION: obj[field] = existing_object.get(field)
            key_map = {'synopsis': 'synopsis', 'showImage': 'image', 'releaseDate': 'releaseDate', 'Duration': 'duration', 'otherNames': 'otherNames'}
            fields_to_fetch_keys = [key_map[k] for k, v in obj.items() if k in key_map and not v]

        if fields_to_fetch_keys:
            spu = obj.setdefault('sitePriorityUsed', {})
            for field in fields_to_fetch_keys:
                primary_site, fallback_site = priority.get(field), 'mydramalist' if priority.get(field) == 'asianwiki' else 'asianwiki'
                
                for site in [primary_site, fallback_site]:
                    if not site: continue
                    
                    soup, url = get_soup_from_search(f'{s_name} {s_year}', site, soup_cache)
                    if not soup: continue

                    args = (soup, s_id) if field == 'image' else (soup,)
                    data = SCRAPE_MAP[site][field](*args)
                    
                    if data is not None and data != []:
                        target_key = "showImage" if field == "image" else "Duration" if field == "duration" else field
                        obj[target_key] = data
                        spu[field] = site
                        source_links[field] = url
                        if field == 'image': context['files_generated']['images'].append(os.path.join(IMAGES_DIR, f"{s_id}.jpg"))
                        break
        
        obj["topRatings"] = (obj.get("ratings", 0)) * (len(obj.get("againWatchedDates", [])) + 1) * 100
        final_obj = {**copy.deepcopy(JSON_OBJECT_TEMPLATE), **obj}
        context['source_links_temp'] = source_links
        processed.append(final_obj)
    return processed

def save_metadata_backup(obj, context):
    fetched = {}
    source_links = context.get('source_links_temp', {})
    for key, site in obj.get('sitePriorityUsed', {}).items():
        if site:
            target_key = "showImage" if key == "image" else "Duration" if key == "duration" else key
            field_data = {"value": obj.get(target_key), "source": site}
            if key in source_links:
                field_data["source_link"] = source_links[key]
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

    fetched_display_map = {
        "synopsis": "Synopsis", "image": "Image", "releaseDate": "Release Date",
        "duration": "Duration", "otherNames": "Other Names"
    }

    for sheet in sheets_to_process:
        try:
            report = context['report_data'].setdefault(sheet, {'created': [], 'updated': [], 'skipped': [], 'fetched_data': [], 'fetch_warnings': [], 'data_warnings': []})
            processed_objects = excel_to_objects(io.BytesIO(excel_bytes.getvalue()), sheet, merged_by_id, context)

            for new_obj in processed_objects:
                sid, old_obj = new_obj['showID'], merged_by_id.get(new_obj['showID'])

                # --- Processing Logic (Create, Update, Skip) ---
                if old_obj is None:
                    report['created'].append(new_obj)
                    merged_by_id[sid] = new_obj
                    save_metadata_backup(new_obj, context)
                elif objects_differ(old_obj, new_obj):
                    changes = [human_readable_field(k) for k, v in new_obj.items() if k not in LOCKED_FIELDS_AFTER_CREATION and normalize_list(old_obj.get(k)) != normalize_list(v)]
                    if changes:
                        new_obj['updatedDetails'] = f"{', '.join(changes)} Updated"
                        new_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                        report['updated'].append({'old': old_obj, 'new': new_obj})
                        merged_by_id[sid] = new_obj
                        create_diff_backup(old_obj, new_obj, context)
                    else:
                        report['skipped'].append(f"{sid} - {old_obj['showName']} ({old_obj.get('releasedYear')})")
                else:
                    report['skipped'].append(f"{sid} - {old_obj['showName']} ({old_obj.get('releasedYear')})")

                # FIX: UNIFIED REPORTING LOGIC RUNS FOR EVERY OBJECT
                missing = []
                if not new_obj.get('otherNames'): missing.append('Other Names')
                if not new_obj.get('showImage'): missing.append('Image')
                if not new_obj.get('releaseDate'): missing.append('Release Date')
                if not new_obj.get('synopsis'): missing.append('Synopsis')
                if not new_obj.get('Duration'): missing.append('Duration')

                fetched = sorted([fetched_display_map.get(k, k) for k,v in new_obj.get('sitePriorityUsed', {}).items() if v])
                if fetched: report['fetched_data'].append(f"- {sid} - {new_obj['showName']} -> Fetched: {', '.join(fetched)}")
                if missing: report['fetch_warnings'].append(f"- {sid} - {new_obj['showName']} -> ⚠️ Missing: {', '.join(sorted(missing))}")

        except Exception as e:
            print(f"❌ UNEXPECTED FATAL ERROR processing sheet '{sheet}': {e}")
            logd(traceback.format_exc())

    with open(JSON_FILE, 'w', encoding='utf-8') as f: json.dump(sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0)), f, indent=4, ensure_ascii=False)

    end_time = now_ist()
    duration = end_time - datetime.fromisoformat(context['start_time_iso'])
    context['duration_str'] = f"{duration.seconds // 60} min {duration.seconds % 60} sec"

    report_path = os.path.join(REPORTS_DIR, f"Report_{filename_timestamp()}.txt")
    os.makedirs(REPORTS_DIR, exist_ok=True)
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