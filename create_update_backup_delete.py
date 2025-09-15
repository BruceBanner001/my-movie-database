# ============================================================================
# Script: create_update_backup_delete.py
# Purpose: Automate download of Excel → JSON transformation, image management,
#          backups, and report generation for the "my-movie-database" repo.
#
# Overview:
#   - Downloads a private Excel (via Google Drive service account) or reads a local
#     file named `local-data.xlsx`.
#   - Converts rows in configured sheets into JSON objects and writes `seriesData.json`.
#   - Fetches/resizes cover images into `images/`, moves replaced images into `old-images/`.
#   - Fetches and truncates synopses (caching applies unless SCHEDULED_RUN=true).
#   - Supports manual updates via a sheet named "manual update".
#   - Supports deletions via a sheet named "Deleting Records".
#   - Produces run reports into `reports/` and writes a status JSON at `reports/status.json`.
#
# Key design goals (from PROJECT_REQUIREMENTS.md):
#   - Caching: avoid re-fetching images/synopses unless SCHEDULED_RUN is true.
#   - Chunking: support MAX_PER_RUN to limit items per run.
#   - Time-limited runs: support MAX_RUN_TIME_MINUTES (0 = no limit) and persist progress.
#   - Safety: fail loudly when critical inputs are missing (helps CI surface errors).
#
# ENVIRONMENT VARIABLES (set in GitHub Actions workflow or your shell):
#   - GDRIVE_SERVICE_ACCOUNT : JSON string written to GDRIVE_SERVICE_ACCOUNT.json by workflow (optional).
#   - EXCEL_FILE_ID          : Google Drive file ID (if using remote Excel; workflow writes EXCEL_FILE_ID.txt).
#   - MAX_PER_RUN            : Max number of items per sheet to process this run (0 = all).
#   - MAX_RUN_TIME_MINUTES   : Max run time (0 = no time limit). See README for behavior.
#   - KEEP_OLD_IMAGES_DAYS   : Days to keep files in old-images before deletion (default 7).
#   - GITHUB_PAGES_URL       : URL used to build absolute showImage links (default uses repo gh-pages URL).
#   - SHEETS                 : Semicolon-separated sheet names (e.g. "Sheet1;Manual Update").
#   - SCHEDULED_RUN          : "true"/"false". If true, script will attempt to refresh existing images/synopsis.
#   - DEBUG_FETCH            : "true"/"false" to enable debug logs for fetches.
#
# OUTPUTS / ARTIFACTS:
#   - seriesData.json        : Main JSON produced by the script (array of objects).
#   - images/                : Downloaded/resized cover images (600x900 JPEG).
#   - old-images/            : Previous images moved here on replacement (retention by KEEP_OLD_IMAGES_DAYS).
#   - backups/               : JSON backups for changed objects per sheet run.
#   - deleted-data/          : Individually saved deleted objects (DELETED_<timestamp>_<id>.json).
#   - reports/               : Run reports (report_*.txt and .html) and status.json.
#
# Exit codes:
#   - 0 : Success (>=1 records processed)
#   - 2 : No records processed (treated as failure so CI picks it up)
#   - 3 : Required Excel missing (fatal)
#
# IMPORTANT:
#   - Keep SHEETS env aligned with actual sheet names in Excel.
#   - For manual testing, you may place local-data.xlsx in repo root.
#   - This file includes defensive checks so the CI will fail loudly when misconfigured.
# ============================================================================
import os
import io
import re
import json
import time
import random
import requests
import shutil
import sys
import pandas as pd
from datetime import datetime, timezone, timedelta
from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup
from ddgs import DDGS
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# ---------------------- Basic utilities & config ----------------------
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

def filename_timestamp():
    return now_ist().strftime("%d%m%Y_%H%M%S")

# Paths and defaults (can be overriden via environment)
EXCEL_FILE_ID_TXT = "EXCEL_FILE_ID.txt"
SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json"
LOCAL_EXCEL_FILE = "local-data.xlsx"
JSON_FILE = "seriesData.json"

BACKUP_DIR = "backups"
IMAGES_DIR = "images"
OLD_IMAGES_DIR = "old-images"
DELETED_DATA_DIR = "deleted-data"
REPORTS_DIR = "reports"
PROGRESS_DIR = ".progress"
PROGRESS_FILE = os.path.join(PROGRESS_DIR, "progress.json")
STATUS_JSON = os.path.join(REPORTS_DIR, "status.json")

# Environment-driven configuration with safe defaults
GITHUB_PAGES_URL = os.environ.get("GITHUB_PAGES_URL", "https://brucebanner001.github.io/my-movie-database/")
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "0") or 0)
MAX_RUN_TIME_MINUTES = int(os.environ.get("MAX_RUN_TIME_MINUTES", "0") or 0)
SCHEDULED_RUN = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
DELETED_LIFESPAN_DAYS = int(os.environ.get("DELETED_LIFESPAN_DAYS", "30") or 30)
KEEP_OLD_IMAGES_DAYS = int(os.environ.get("KEEP_OLD_IMAGES_DAYS", "7") or 7)
DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
SYNOPSIS_MAX_LEN = int(os.environ.get("SYNOPSIS_MAX_LEN", "1000") or 1000)

COVER_WIDTH, COVER_HEIGHT = 600, 900
FORCE_REFRESH_IMAGES = False
IMAGE_SEARCH_MAX_PER_QUERY = 6

_sheets_env = os.environ.get("SHEETS", "").strip()
if _sheets_env:
    SHEETS = [s.strip() for s in _sheets_env.split(";") if s.strip()]
else:
    SHEETS = ["Sheet1"]

PREFERRED_SITE_ORDER = {"Korean": ["asianwiki", "mydramalist"], "Chinese": ["mydramalist", "asianwiki"]}

ALLOWED_SYNOP_SITES = ["asianwiki.com", "mydramalist.com", "netflix.com", "viki.com", "primevideo.com", "imdb.com"]

HEADERS = {"User-Agent": "Mozilla/5.0"}

def logd(msg):
    if DEBUG_FETCH:
        print(f"[DEBUG] {msg}")

def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())

def cap_first(s: str):
    return s[:1].upper() + s[1:] if s else s

def words_capitalize(s: str):
    return " ".join(w.capitalize() for w in (s or "").split())

def ddmmyyyy(val):
    if pd.isna(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.strftime("%d-%m-%Y")
    s = str(val).strip()
    if re.match(r"^\d{2}-\d{2}-\d{4}$", s):
        return s
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return None

def load_progress():
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_progress(progress: dict):
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2)

# ---------------------- Robust CSV list normalizer ----------------------
def normalize_list_from_csv(cell_value, cap=False, strip=False):
    """
    Convert a comma-separated cell value into a clean list of strings.

    Params:
      - cell_value: string, list/tuple or None
      - cap: if True capitalizes each item (first letter)
      - strip: if True strips whitespace from items

    Returns:
      - list of cleaned strings (possibly empty)
    """
    if cell_value is None:
        return []
    if isinstance(cell_value, (list, tuple)):
        items = [str(x) for x in cell_value if x is not None and str(x).strip()]
    else:
        s = str(cell_value)
        if not s.strip():
            return []
        items = [p for p in [p.strip() for p in s.split(",")] if p]
    if cap:
        items = [p.capitalize() if p else p for p in items]
    if strip:
        items = [p.strip() for p in items]
    return items

def country_from_native(native):
    """
    Basic mapping from nativeLanguage to a country name.
    Extend this mapping when you need more languages.
    """
    if not native:
        return None
    n = native.strip().lower()
    if n in ("korean", "korea", "korean language"):
        return "South Korea"
    if n in ("chinese", "mandarin", "cantonese"):
        return "China"
    if n in ("japanese","japan"):
        return "Japan"
    if n in ("thai","thai language"):
        return "Thailand"
    if n in ("english","eng"):
        return "United States"
    return None

# ---------------------- Image search & download helpers ----------------------
def try_ddgs_images(query, max_results=IMAGE_SEARCH_MAX_PER_QUERY):
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(query, max_results=max_results))
            return [r.get('image') for r in results if r.get('image')]
    except Exception as e:
        logd(f"DDGS image search error: {e}")
        return []

def download_image_to(url, path):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code == 200 and resp.headers.get('content-type','').startswith('image'):
            img = Image.open(BytesIO(resp.content))
            img = img.convert('RGB').resize((COVER_WIDTH, COVER_HEIGHT), Image.LANCZOS)
            img.save(path, format='JPEG', quality=95)
            return True
    except Exception as e:
        logd(f"Image download failed: {e}")
    return False

def build_absolute_url(local_path: str) -> str:
    local_path = local_path.replace('\\','/')
    return GITHUB_PAGES_URL.rstrip('/') + '/' + local_path.lstrip('/')

def download_cover_image(show_name, year, networks=None, prefer_sites=None, existing_image_url=None, allow_replace=False):
    # If no show_name or year -> cannot create filename; return None
    if not show_name or not year:
        return None
    os.makedirs(IMAGES_DIR, exist_ok=True); os.makedirs(OLD_IMAGES_DIR, exist_ok=True)
    safe_year = str(year) if year else "unknown"
    filename = f"{safe_filename(show_name)}_{safe_year}.jpg"
    local_path = os.path.join(IMAGES_DIR, filename)
    if os.path.exists(local_path) and not FORCE_REFRESH_IMAGES and not allow_replace:
        return build_absolute_url(local_path)
    if existing_image_url and not SCHEDULED_RUN and not allow_replace:
        return existing_image_url
    queries = []
    if prefer_sites:
        for s in prefer_sites:
            if s == 'asianwiki': queries.append(f"{show_name} {year} asianwiki poster")
            if s == 'mydramalist': queries.append(f"{show_name} {year} mydramalist poster")
    queries += [f"{show_name} {year} drama poster", f"{show_name} poster", f"{show_name} {year} poster"]
    for q in queries:
        logd(f"Image query: {q}")
        urls = try_ddgs_images(q) or []
        for url in urls:
            if download_image_to(url, local_path):
                logd(f"Saved image to {local_path}")
                return build_absolute_url(local_path)
        time.sleep(random.uniform(0.6,1.2))
    logd(f"No image found for {show_name}")
    return None

# ---------------------- Synopsis fetch/parsing ----------------------
def ddgs_text(query, max_results=6):
    try:
        with DDGS() as dd:
            return list(dd.text(query, max_results=max_results))
    except Exception as e:
        logd(f"DDGS text error: {e}")
        return []

def pick_best_result(results):
    if not results:
        return None
    for r in results:
        url = r.get('href') or r.get('url') or ''
        if any(site in url for site in ALLOWED_SYNOP_SITES):
            return url
    return results[0].get('href') or results[0].get('url')

def fetch_page(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200: return r.text
    except Exception as e:
        logd(f"Fetch page error: {e}")
    return None

def parse_synopsis_from_html(html, base_url):
    soup = BeautifulSoup(html, 'lxml')
    text = soup.get_text(' ', strip=True)
    duration = None
    try:
        duration = extract_duration_minutes(text)
    except Exception:
        duration = None
    syn = None
    if 'mydramalist.com' in base_url:
        meta = soup.find('meta', attrs={'name':'description'}) or soup.find('meta', attrs={'property':'og:description'})
        if meta and meta.get('content'): syn = meta['content']
    if not syn and 'asianwiki.com' in base_url:
        meta = soup.find('meta', attrs={'name':'description'}) or soup.find('meta', attrs={'property':'og:description'})
        if meta and meta.get('content'): syn = meta['content']
    if not syn and 'wikipedia.org' in base_url:
        p = soup.find('p');
        if p: syn = p.get_text(' ', strip=True)
    if not syn and any(s in base_url for s in ['netflix.com','viki.com','primevideo.com','imdb.com']):
        meta = soup.find('meta', attrs={'name':'description'}) or soup.find('meta', attrs={'property':'og:description'})
        if meta and meta.get('content'): syn = meta['content']
    if not syn:
        lower = text.lower(); i = lower.find('synopsis')
        if i != -1: syn = text[i:i+1500]
    return syn, duration, text

def extract_duration_minutes(text):
    text_l = text.lower()
    m = re.search(r"(\\d+)\\s*h(?:our)?s?\\s*(\\d+)\\s*m(?:in)?", text_l)
    if m: return int(m.group(1))*60 + int(m.group(2))
    m = re.search(r"(\\d+)\\s*h(?:our)?s?", text_l)
    if m: return int(m.group(1))*60
    m = re.search(r"(\\d+)\\s*m(?:in|inute|inutes)\\b", text_l)
    if m: return int(m.group(1))
    m = re.search(r"runtime[^0-9]*?(\\d{1,3})", text_l)
    if m: return int(m.group(1))
    return None

def clean_and_truncate_synopsis(raw_text, max_len=SYNOPSIS_MAX_LEN):
    txt = re.sub(r"\\s+"," ", (raw_text or "")).strip()
    if len(txt) <= max_len: return txt
    truncated = txt[:max_len]
    if '.' in truncated:
        truncated = truncated.rsplit('.',1)[0] + '.'
    return truncated

def fetch_synopsis_and_duration(show_name, year, prefer_sites=None, existing_synopsis=None, allow_replace=False):
    if existing_synopsis and not SCHEDULED_RUN and not allow_replace:
        return existing_synopsis, None, None, 0
    queries = []
    if prefer_sites:
        for s in prefer_sites:
            if s=='mydramalist': queries.append(f"{show_name} {year} site:mydramalist.com synopsis")
            if s=='asianwiki': queries.append(f"{show_name} {year} site:asianwiki.com synopsis")
    fallback = [f"{show_name} {year} drama synopsis site:mydramalist.com", f"{show_name} {year} synopsis site:asianwiki.com", f"{show_name} {year} synopsis site:wikipedia.org"]
    queries += fallback
    for q in queries:
        logd(f"Synopsis query: {q}")
        results = ddgs_text(q, max_results=6)
        if not results: continue
        url = pick_best_result(results)
        if not url: continue
        html = fetch_page(url)
        if not html: continue
        syn, dur, full_text = parse_synopsis_from_html(html, url)
        if syn:
            orig_len = len(syn)
            truncated = clean_and_truncate_synopsis(syn, SYNOPSIS_MAX_LEN)
            return truncated, dur, url, orig_len
        time.sleep(0.4)
    return (existing_synopsis or "Synopsis not available."), None, None, 0

# ---------------------- Excel -> objects (row mapping) ----------------------
COLUMN_MAP = {
    "no": "showID", "series title": "showName", "started date": "watchStartedOn", "finished date": "watchEndedOn",
    "year": "releasedYear", "total episodes": "totalEpisodes", "original language": "nativeLanguage", "language": "watchedLanguage",
    "ratings": "ratings", "catagory": "genres", "category": "genres", "original network": "network", "comments": "comments"
}
CHANGE_TRACK_FIELDS = ["showName","showImage","releasedYear","totalEpisodes","comments","ratings","genres","Duration","synopsis"]

def sheet_base_offset(sheet_name: str) -> int:
    if sheet_name == "Sheet1": return 100
    if sheet_name == "Feb 7 2023 Onwards": return 1000
    if sheet_name == "Sheet2": return 3000
    return 0

def tidy_comment(val):
    if pd.isna(val) or not str(val).strip(): return None
    text = " ".join(str(val).split())
    text = " ".join(w.capitalize() for w in text.split())
    if not text.endswith('.'): text += '.'
    return text

def excel_to_objects(excel_file, sheet_name, existing_by_id, report_changes, start_index=0, max_items=None, time_limit_seconds=None):
    # Read sheet; allow pandas to raise useful exceptions for caller to record
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.columns = [c.strip().lower() for c in df.columns]
    again_idx = None
    for i,c in enumerate(df.columns):
        if "again watched" in c:
            again_idx = i; break
    if again_idx is None:
        raise ValueError(f"'Again Watched Date' columns not found in sheet: {sheet_name}")
    items=[]; processed=0; start_time=time.time(); last_idx=start_index
    total_rows = len(df)
    for idx in range(start_index, total_rows):
        if max_items and processed>=max_items: break
        if time_limit_seconds and (time.time()-start_time)>=time_limit_seconds: break
        row = df.iloc[idx]; obj={}
        try:
            for col in df.columns[:again_idx]:
                key = COLUMN_MAP.get(col, col); val = row[col]
                if key == "showID":
                    base = sheet_base_offset(sheet_name)
                    obj["showID"] = base + int(val) if pd.notna(val) else None
                elif key == "showName":
                    obj["showName"] = " ".join(str(val).split()) if pd.notna(val) else None
                elif key in ("watchStartedOn","watchEndedOn"):
                    obj[key] = ddmmyyyy(val)
                elif key == "releasedYear":
                    obj[key] = int(val) if pd.notna(val) else None
                elif key == "totalEpisodes":
                    obj[key] = int(val) if pd.notna(val) else None
                elif key == "nativeLanguage":
                    obj[key] = cap_first(str(val).strip()) if pd.notna(val) else None
                elif key == "watchedLanguage":
                    obj[key] = cap_first(str(val).strip()) if pd.notna(val) else None
                elif key == "comments":
                    obj[key] = tidy_comment(val)
                elif key == "ratings":
                    try: obj[key] = int(val) if pd.notna(val) else 0
                    except: obj[key] = 0
                elif key == "genres":
                    obj[key] = normalize_list_from_csv(val, cap=True, strip=True)
                elif key == "network":
                    obj[key] = normalize_list_from_csv(val, cap=False, strip=True)
                else:
                    obj[key] = str(val).strip() if pd.notna(val) else None
            obj["showType"] = "Mini Drama" if sheet_name.lower() == "mini drama" else "Drama"
            obj["country"] = country_from_native(obj.get("nativeLanguage"))
            dates = [ddmmyyyy(v) for v in row[again_idx:] if ddmmyyyy(v)]
            obj["againWatchedDates"] = dates
            obj["updatedOn"] = now_ist().strftime("%d %B %Y"); obj["updatedDetails"] = "First time Uploaded"
            r = int(obj.get("ratings") or 0); obj["topRatings"] = r * len(dates) * 100
            obj["Duration"] = None
            obj.setdefault("otherNames", [])
            show_name = obj.get("showName"); released_year = obj.get("releasedYear"); networks = obj.get("network") or []
            existing = existing_by_id.get(obj.get("showID")) if obj.get("showID") is not None else None
            native = obj.get("nativeLanguage"); prefer = PREFERRED_SITE_ORDER.get(native)
            existing_image_url = existing.get("showImage") if existing else None
            allow_replace_image = SCHEDULED_RUN
            new_image_url = None
            if existing_image_url and allow_replace_image:
                new_image_url = download_cover_image(show_name, released_year, networks, prefer_sites=prefer, existing_image_url=existing_image_url, allow_replace=True)
                if new_image_url and new_image_url != existing_image_url:
                    try:
                        old_local = os.path.join(IMAGES_DIR, os.path.basename(existing_image_url))
                        if os.path.exists(old_local):
                            dest = os.path.join(OLD_IMAGES_DIR, os.path.basename(old_local)); shutil.move(old_local, dest)
                    except Exception as e: logd(f"Could not move old image: {e}")
            else:
                if not existing_image_url:
                    new_image_url = download_cover_image(show_name, released_year, networks, prefer_sites=prefer, existing_image_url=None, allow_replace=False)
                else:
                    new_image_url = existing_image_url
            obj["showImage"] = new_image_url
            existing_syn = existing.get("synopsis") if existing else None
            new_syn, dur, syn_url, orig_len = fetch_synopsis_and_duration(show_name, released_year, prefer_sites=prefer, existing_synopsis=existing_syn, allow_replace=False)
            obj["synopsis"] = new_syn
            if dur is not None and dur>0: obj["Duration"] = int(dur)
            elif existing and existing.get("Duration"): obj["Duration"] = existing.get("Duration")
            ordered = {
                "showID":obj.get("showID"),
                "showName":obj.get("showName"),
                "otherNames":obj.get("otherNames",[]),
                "showImage":obj.get("showImage"),
                "watchStartedOn":obj.get("watchStartedOn"),
                "watchEndedOn":obj.get("watchEndedOn"),
                "releasedYear":obj.get("releasedYear"),
                "totalEpisodes":obj.get("totalEpisodes"),
                "showType":obj.get("showType"),
                "nativeLanguage":obj.get("nativeLanguage"),
                "watchedLanguage":obj.get("watchedLanguage"),
                "country":obj.get("country"),
                "comments":obj.get("comments"),
                "ratings":obj.get("ratings"),
                "genres":obj.get("genres"),
                "network":obj.get("network"),
                "againWatchedDates":obj.get("againWatchedDates"),
                "updatedOn":obj.get("updatedOn"),
                "updatedDetails":obj.get("updatedDetails"),
                "synopsis":obj.get("synopsis"),
                "topRatings":obj.get("topRatings"),
                "Duration":obj.get("Duration")
            }
            items.append(ordered); processed += 1; last_idx = idx
            sid = ordered.get("showID")
            if existing is None:
                report_changes.setdefault("created",[]).append(ordered)
            else:
                if existing != ordered:
                    report_changes.setdefault("updated",[]).append({"old":existing,"new":ordered})
            if syn_url and orig_len and orig_len > SYNOPSIS_MAX_LEN:
                report_changes.setdefault('exceed', []).append({"id": sid, "name": ordered.get('showName'), "year": ordered.get('releasedYear'), "site": syn_url.split('/')[2] if syn_url.startswith('http') else syn_url, "url": syn_url, "orig_len": orig_len})
        except Exception as e:
            # Raise to allow caller to record which row failed; row-level failures abort sheet processing
            raise RuntimeError(f"Row {idx} in sheet '{sheet_name}' processing failed: {e}")
    finished = (last_idx >= total_rows - 1) if total_rows>0 else True
    next_index = (last_idx + 1) if processed>0 else start_index
    return items, processed, finished, next_index

# ---------------------- Deletions ----------------------
def process_deletions(excel_file, json_file, report_changes):
    try:
        df = pd.read_excel(excel_file, sheet_name='Deleting Records')
    except Exception:
        return
    if df.shape[1] < 1:
        return
    cols = [str(c).strip().lower() for c in df.columns]
    id_col = None
    for i,c in enumerate(cols):
        if c == 'id' or 'id' in c:
            id_col = df.columns[i]; break
    if id_col is None:
        id_col = df.columns[0]
    if not os.path.exists(json_file):
        return
    with open(json_file, 'r', encoding='utf-8') as f:
        try: data = json.load(f)
        except: data = []
    by_id = {o['showID']: o for o in data if 'showID' in o}
    to_delete = []
    for _, row in df.iterrows():
        val = row[id_col]
        if pd.isna(val): continue
        try: to_delete.append(int(val))
        except: continue
    if not to_delete: return
    os.makedirs(DELETED_DATA_DIR, exist_ok=True)
    deleted_ids = []
    for iid in to_delete:
        if iid in by_id:
            deleted_obj = by_id.pop(iid)
            deleted_ids.append(iid)
            fname = f"DELETED_{now_ist().strftime('%d_%B_%Y_%H%M')}_{iid}.json"
            outpath = os.path.join(DELETED_DATA_DIR, safe_filename(fname))
            with open(outpath, 'w', encoding='utf-8') as of:
                json.dump(deleted_obj, of, indent=4, ensure_ascii=False)
            print(f"✅ Deleted record {iid} -> {outpath}")
    merged = sorted(by_id.values(), key=lambda x: x.get('showID',0))
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(merged, f, indent=4, ensure_ascii=False)
    if deleted_ids:
        report_changes.setdefault('deleted', []).extend(deleted_ids)

def cleanup_deleted_data():
    if not os.path.exists(DELETED_DATA_DIR): return
    cutoff = datetime.now() - timedelta(days=DELETED_LIFESPAN_DAYS)
    for fname in os.listdir(DELETED_DATA_DIR):
        path = os.path.join(DELETED_DATA_DIR, fname)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff: os.remove(path); print(f"🗑️ Removed expired deleted-data file: {path}")
        except Exception as e:
            print(f"⚠️ Could not cleanup deleted-data {path}: {e}")

# ---------------------- Manual updates handler ----------------------
def apply_manual_updates(excel_file: str, json_file: str):
    sheet = 'manual update'
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet)
    except Exception:
        print("ℹ️ No 'manual update' sheet found; skipping manual updates.")
        return

    if df.shape[1] < 2:
        print("Manual update sheet must have at least two columns: showID and dataString")
        return

    if not os.path.exists(json_file):
        print("No JSON file to update")
        return

    with open(json_file, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except Exception:
            data = []

    by_id = {o['showID']: o for o in data if 'showID' in o}
    updated_objs = []

    for _, row in df.iterrows():
        sid = None
        try:
            sid = int(row[0]) if not pd.isna(row[0]) else None
        except Exception:
            continue
        if sid is None or sid not in by_id:
            continue
        raw = row[1]
        if pd.isna(raw) or not str(raw).strip():
            continue
        s = str(raw).strip()
        try:
            if s.startswith('{') and s.endswith('}'):
                upd = json.loads(s)
            else:
                if s.startswith('{') and not s.endswith('}'):
                    s = s + '}'
                if not s.startswith('{'):
                    s2 = '{' + s + '}'
                else:
                    s2 = s
                upd = json.loads(s2)
        except Exception:
            upd = {}
            parts = [p.strip() for p in s.split(',') if p.strip()]
            for part in parts:
                if ':' in part:
                    k, v = part.split(':', 1)
                    upd[k.strip()] = v.strip()
        if not upd:
            continue
        obj = by_id[sid]
        for k, v in upd.items():
            if k.lower() == "ratings":
                try: obj["ratings"] = int(v)
                except: obj["ratings"] = obj.get("ratings",0)
            elif k.lower() in ("releasedyear","year"):
                try: obj["releasedYear"] = int(v)
                except: pass
            else:
                obj[k] = v
        obj['updatedOn'] = now_ist().strftime('%d %B %Y')
        obj['updatedDetails'] = f"Updated {', '.join([words_capitalize(k) for k in upd.keys()])} Mannually By Owner"
        updated_objs.append(obj)

    if updated_objs:
        merged = sorted(by_id.values(), key=lambda x: x.get('showID', 0))
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(merged, f, indent=4, ensure_ascii=False)
        print(f"✅ Applied {len(updated_objs)} manual updates")
    else:
        print("ℹ️ No valid manual updates found/applied.")

# ---------------------- Report writing ----------------------
def write_report(report_changes_by_sheet, report_path):
    lines = []
    exceed_entries = []
    total_created = total_updated = total_deleted = 0
    for sheet, changes in report_changes_by_sheet.items():
        lines.append(f"=== {sheet} — {now_ist().strftime('%d %B %Y')} ===")
        if 'error' in changes:
            lines.append(f"ERROR processing sheet: {changes['error']}")
        created = changes.get('created', [])
        total_created += len(created)
        if created:
            lines.append("\\nData Created:")
            for obj in created: lines.append(f"- {words_capitalize(obj.get('showName','Unknown'))} -> Created")
        updated = changes.get('updated', [])
        total_updated += len(updated)
        if updated:
            lines.append("\\nData Updated:")
            for pair in updated:
                new = pair.get('new'); old = pair.get('old')
                changed_fields = [f for f in CHANGE_TRACK_FIELDS if old.get(f) != new.get(f)]
                fields_text = ", ".join([words_capitalize(f) for f in changed_fields]) if changed_fields else "General"
                lines.append(f"- {words_capitalize(new.get('showName','Unknown'))} -> Updated: {fields_text}")
        images = changes.get('images', [])
        if images:
            lines.append("\\nImage Updated:")
            for itm in images:
                lines.append(f"- {words_capitalize(itm.get('showName','Unknown'))} -> Old && New")
                lines.append(f"  Old: {itm.get('old')}"); lines.append(f"  New: {itm.get('new')}")
        deleted = changes.get('deleted', [])
        total_deleted += len(deleted)
        if deleted:
            lines.append("\\nDeleted Records:")
            for iid in deleted: lines.append(f"- {iid} -> ✅Deleted")
        if changes.get('exceed'):
            exceed_entries.extend(changes.get('exceed'))
        lines.append("\\n")
    lines.insert(0, f"SUMMARY: Created: {total_created}, Updated: {total_updated}, Deleted: {total_deleted}")
    if exceed_entries:
        lines.append(f"=== Exceed Max Length ({SYNOPSIS_MAX_LEN}) ===")
        for e in exceed_entries:
            lines.append(f"{e.get('id')} -> {e.get('name')} ({e.get('year')}) -> {e.get('site')} -> Link: {e.get('url')}")
        lines.append("\\n")
    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f: f.write('\\n'.join(lines))
    try:
        with open(report_path, 'r', encoding='utf-8') as f: txt = f.read()
        html = "<html><body>"
        html += f"<h1>{now_ist().strftime('%d %B %Y_%H.%M')}</h1>"
        if exceed_entries:
            html += f"<h2 style='color:orange'>Exceed Max Length ({SYNOPSIS_MAX_LEN})</h2><ul>"
            for e in exceed_entries:
                html += f"<li>{e.get('id')} → {e.get('name')} ({e.get('year')}) → {e.get('site')} → <a href='{e.get('url')}'>Link</a></li>"
            html += "</ul><hr/>"
        html += "<pre style='font-family: monospace; white-space: pre-wrap;'>"
        html += txt.replace('<','&lt;').replace('>','&gt;')
        html += "</pre></body></html>"
        with open(report_path.replace('.txt','.html'), 'w', encoding='utf-8') as hf: hf.write(html)
    except Exception as e:
        print(f"⚠️ Could not write HTML report: {e}")

# ---------------------- Main updater ----------------------
def update_json_from_excel(excel_file, json_file, sheet_names, max_per_run=0, max_run_time_minutes=0):
    moved = 0
    removed = 0
    processed_total = 0
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as f: old_objects = json.load(f)
        except Exception:
            print(f"⚠️ {json_file} invalid. Starting fresh."); old_objects = []
    else:
        old_objects = []
    old_by_id = {o['showID']: o for o in old_objects if 'showID' in o}
    report_changes_by_sheet = {}
    try:
        process_deletions(excel_file, json_file, report_changes_by_sheet.setdefault('Deleting Records', {}))
    except Exception as e:
        report_changes_by_sheet.setdefault('Deleting Records', {})['error'] = str(e)
    if os.path.exists(json_file):
        try:
            with open(json_file,'r',encoding='utf-8') as f: old_objects = json.load(f)
        except: old_objects = []
    old_by_id = {o['showID']: o for o in old_objects if 'showID' in o}
    merged_by_id = dict(old_by_id)
    progress = load_progress()
    overall_continued = False
    time_limit_seconds = max_run_time_minutes*60 if max_run_time_minutes>0 else None
    any_sheet_processed = False
    for s in sheet_names:
        report_changes = {}
        start_idx = int(progress.get(s,0) or 0)
        logd(f"Processing sheet {s} starting at {start_idx}")
        try:
            items, processed, finished, next_start_idx = excel_to_objects(excel_file, s, merged_by_id, report_changes, start_index=start_idx, max_items=(max_per_run if max_per_run>0 else None), time_limit_seconds=time_limit_seconds)
        except Exception as e:
            err = str(e)
            print(f"⚠️ Error processing {s}: {err}")
            report_changes['error'] = err
            items, processed, finished, next_start_idx = [],0,True,start_idx
        changed_or_deleted = []
        for new_obj in items:
            sid = new_obj.get('showID')
            if sid in merged_by_id:
                old_obj = merged_by_id[sid]
                if old_obj != new_obj:
                    new_obj['updatedOn'] = now_ist().strftime('%d %B %Y'); new_obj['updatedDetails'] = 'Object updated'; changed_or_deleted.append(old_obj); merged_by_id[sid] = new_obj
            else:
                merged_by_id[sid] = new_obj
        if changed_or_deleted:
            os.makedirs(BACKUP_DIR, exist_ok=True)
            backup_name = os.path.join(BACKUP_DIR, f"{filename_timestamp()}_{safe_filename(s)}.json")
            with open(backup_name,'w',encoding='utf-8') as f: json.dump(changed_or_deleted,f,indent=4,ensure_ascii=False)
            print(f"✅ Backup saved → {backup_name}")
        report_changes_by_sheet[s] = report_changes
        if processed>0:
            any_sheet_processed = True
            processed_total += processed
        if not finished:
            progress[s] = next_start_idx; overall_continued = True
        else:
            if s in progress: progress.pop(s,None)
        save_progress(progress)
    merged = sorted(merged_by_id.values(), key=lambda x: x.get('showID',0))
    with open(json_file,'w',encoding='utf-8') as f: json.dump(merged,f,indent=4,ensure_ascii=False)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(REPORTS_DIR, f"report_{filename_timestamp()}.txt")
    write_report(report_changes_by_sheet, report_path)
    print(f"✅ Report written → {report_path}")
    if SCHEDULED_RUN: cleanup_deleted_data()
    cutoff = datetime.now() - timedelta(days=KEEP_OLD_IMAGES_DAYS)
    if os.path.exists(OLD_IMAGES_DIR):
        for fname in os.listdir(OLD_IMAGES_DIR):
            path = os.path.join(OLD_IMAGES_DIR, fname)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(path))
                if mtime < cutoff: os.remove(path)
            except Exception as e: print(f"⚠️ Could not cleanup old image {path}: {e}")
    status = {"continued": overall_continued, "timestamp": now_ist().strftime('%d %B %Y_%H.%M'), "processed_total": processed_total}
    with open(STATUS_JSON, 'w', encoding='utf-8') as sf: json.dump(status, sf, indent=2)
    print(f"✅ Status written → {STATUS_JSON}")
    if processed_total == 0:
        print("❌ No records were processed in this run. Please check: (1) local-data.xlsx exists, (2) sheet names match, (3) there were no row errors. Failing with non-zero exit code.")
        with open(os.path.join(REPORTS_DIR, "failure_reason.txt"), "w", encoding="utf-8") as ff:
            ff.write("No records processed. Check logs and the report.")
        sys.exit(2)
    return moved, removed

def download_from_gdrive(file_id, destination, service_account_file):
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=["https://www.googleapis.com/auth/drive.readonly"])
    service = build("drive","v3", credentials=creds)
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(destination, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status: print(f"⬇️ Downloading Excel: {int(status.progress() * 100)}%")
    print(f"✅ Download complete → {destination}")

if __name__ == '__main__':
    # Try to find excel id and download if secrets + id are provided
    try:
        if os.path.exists(EXCEL_FILE_ID_TXT):
            with open(EXCEL_FILE_ID_TXT, 'r') as f:
                excel_id = f.read().strip()
        else:
            excel_id = None
    except Exception:
        excel_id = None

    try:
        if excel_id and os.path.exists(SERVICE_ACCOUNT_FILE):
            try:
                download_from_gdrive(excel_id, LOCAL_EXCEL_FILE, SERVICE_ACCOUNT_FILE)
            except Exception as e:
                print(f"⚠️ Google Drive fetch failed: {e}")
        else:
            if not os.path.exists(LOCAL_EXCEL_FILE):
                print("❌ Excel not available locally (local-data.xlsx). Aborting. Please ensure EXCEL_FILE_ID.txt + GDRIVE_SERVICE_ACCOUNT.json are present or place local-data.xlsx in repo root.")
                sys.exit(3)
    except Exception as e:
        print(f"⚠️ Google Drive fetch failed: {e}")

    # Apply manual updates (if any)
    try:
        apply_manual_updates(LOCAL_EXCEL_FILE, JSON_FILE)
    except Exception as e:
        logd(f"apply_manual_updates error: {e}")

    # Run update (will exit with code 2 if nothing processed)
    moved, removed = update_json_from_excel(
        LOCAL_EXCEL_FILE,
        JSON_FILE,
        SHEETS,
        max_per_run=MAX_PER_RUN,
        max_run_time_minutes=MAX_RUN_TIME_MINUTES
    )

    print("All done.")
    print(f"Moved: {moved}, Removed: {removed}")
