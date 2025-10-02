
# ============================================================================
# Patched Script: create_update_backup_delete.py
# Purpose: Excel -> JSON automation (patched for enhanced synopsis/image fetching,
#          deletion handling, and single-email composing for CI workflows).
#
# IMPORTANT NOTES FOR MAINTENANCE / EXTENSION
# -------------------------------------------------
# 1) Site Preferences / Language mapping:
#    - The code uses a simple mapping to decide "preferred sites" for fetching synopsis
#      and images based on the show's native language (nativeLanguage field).
#    - To add a new language preference:
#        a) Locate the function excel_to_objects(...) and find the block that sets `prefer`.
#        b) Add a new branch for the language name (use lowercased checks, and include
#           possible variants, e.g. 'korean', 'korea', 'korean language').
#        c) Update fetch_synopsis_and_duration(...) and fetch_and_save_image_for_show(...)
#           if you want to treat the new site specially (site-specific parsing).
#
# 2) Adding a new preferred site parser:
#    - If you want more accurate extraction from a particular site (e.g., 'asianwiki' or 'mydramalist'),
#      add a new branch in parse_synopsis_from_html(...) that checks the domain and applies
#      site-specific DOM selectors (e.g., look for elements with id/class 'synopsis', 'summary',
#      or meta property 'og:description'). Keep a generic fallback for robustness.
#
# 3) Email behavior for CI (GitHub Actions):
#    - This script now composes the full email body in-memory via compose_email_body_from_report(report_path).
#      We purposely **do not** write email_body_*.txt files to disk anymore.
#    - The workflow should either:
#        A) read reports/report_*.txt and send email with that content, or
#        B) run this script and capture the printed email body (stdout) and pass it to the email action.
#    - The subject format required by the owner: "[Manual] Workflow <DD Month YYYY HHMM> Report"
#
# 4) Deletion behavior:
#    - When a showID is deleted via the "Deleting Records" sheet:
#        * the deleted object is saved to deleted-data/DELETED_<timestamp>_<id>.json
#        * the associated image file (if present under images/) is moved to old-images/
#        * a report entry is generated for moved images: 'deleted_images_moved'
#
# 5) Synopsis length:
#    - Controlled by SYNOPSIS_MAX_LEN environment variable (default 1500). Soft truncation attempts to cut at sentence end.
#
# 6) Debugging:
#    - Set DEBUG_FETCH=true in env to print useful debug messages.
#
# ============================================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================================
# Script: create_update_backup_delete.py
# Purpose: Excel -> JSON automation (patched to not require a local Excel file).
#
# Requirements (recommended):
#   pip install pandas requests beautifulsoup4 pillow openpyxl google-api-python-client google-auth-httplib2 google-auth
# Notes:
#   - The script expects two files to be present in the runner environment:
#       EXCEL_FILE_ID.txt         (text file containing the Google Drive file id)
#       GDRIVE_SERVICE_ACCOUNT.json  (service account JSON key)
#   - If google-api-python-client/google-auth packages are not available, script
#     will exit gracefully with instructions (no hard crash).
# ============================================================================

import os
import re
import sys
import time
import json
import copy
import io
import shutil
import traceback
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO

# Try to import DDGS (duckduckgo images) — optional
try:
    from ddgs import DDGS
    HAVE_DDGS = True
except Exception:
    HAVE_DDGS = False

# Try to import Google Drive client libraries — optional but recommended
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload, HttpRequest
    HAVE_GOOGLE_API = True
except Exception:
    HAVE_GOOGLE_API = False

# ---------------------------- Timezone helpers -------------------------------
IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    return datetime.now()(IST)


def filename_timestamp():
    return now_ist().strftime("%d_%B_%Y_%H%M")


# ---------------------------- Paths & Config --------------------------------
# No local Excel file is required; Excel is always read from Google Drive
LOCAL_EXCEL_FILE = None  # Local Excel not required, using GDrive only

JSON_FILE = "seriesData.json"
BACKUP_DIR = "backups"
IMAGES_DIR = "images"
OLD_IMAGES_DIR = "old-images"
DELETED_DATA_DIR = "deleted-data"
REPORTS_DIR = "reports"
PROGRESS_DIR = ".progress"
PROGRESS_FILE = os.path.join(PROGRESS_DIR, "progress.json")
STATUS_JSON = os.path.join(REPORTS_DIR, "status.json")

EXCEL_FILE_ID_TXT = "EXCEL_FILE_ID.txt"
SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json"

GITHUB_PAGES_URL = os.environ.get("GITHUB_PAGES_URL", "").strip() or "https://<your-username>.github.io/my-movie-database"
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "0") or 0)
MAX_RUN_TIME_MINUTES = int(os.environ.get("MAX_RUN_TIME_MINUTES", "0") or 0)
KEEP_OLD_IMAGES_DAYS = int(os.environ.get("KEEP_OLD_IMAGES_DAYS", "7") or 7)
SCHEDULED_RUN = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
SYNOPSIS_MAX_LEN = int(os.environ.get("SYNOPSIS_MAX_LEN", "1000") or 1000)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Bot/1.0)"}


def logd(msg):
    if DEBUG_FETCH:
        print("[DEBUG]", msg)


# ---------------------------- Utilities -------------------------------------
def safe_filename(name):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())


def ddmmyyyy(val):
    if pd.isna(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.strftime("%d-%m-%Y")
    s = str(val).strip()
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
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_progress(progress):
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2)


# ---------------------------- Text cleaning helpers -------------------------
def clean_parenthesis_remove_cjk(s):
    """Remove parentheses that contain CJK (Chinese/Japanese/Korean) text."""
    if not s:
        return s
    return re.sub(r'\([^)]*[\u4e00-\u9fff\u3400-\u4dbf\uac00-\ud7af][^)]*\)', '', s)


def normalize_whitespace_and_sentences(s):
    if not s:
        return s
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'\.([^\s])', r'. \1', s)
    return s


def normalize_list_from_csv(cell_value, cap=False, strip=False):
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


# ---------------------------- Date parsing helpers -------------------------
_MONTHS = {m.lower(): m for m in ["January", "February", "March", "April", "May", "June",
                                  "July", "August", "September", "October", "November", "December"]}
_SHORT_MONTHS = {m[:3].lower(): m for m in _MONTHS}



# ------------------------- Smart merge helpers (ADDED) -------------------------
# Properties that should be merged intelligently rather than bluntly overwritten when the new value is empty/absent.
PRESERVE_IF_EMPTY = {
    "otherNames",
    # add keys here to preserve non-empty existing values when incoming is empty
}

# Properties to treat as lists (ensure lists rather than comma-separated strings)
LIST_PROPERTIES = {
    "otherNames",
    "genres",
    # add list-like keys here
}

def now_iso():
    from datetime import datetime
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def write_json_file_atomic(path, obj):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def backup_object(obj_id, obj, backup_dir=BACKUP_DIR):
    """Write a timestamped backup of obj into backup_dir and return path."""
    try:
        os.makedirs(backup_dir, exist_ok=True)
        ts = datetime.now().now().strftime('%Y%m%dT%H%M%SZ')
        safe_id = str(obj_id).replace(' ', '_').replace('/', '_')
        fname = f"{safe_id}__backup__{ts}.json"
        path = os.path.join(backup_dir, safe_filename(fname))
        write_json_file_atomic(path, obj)
        return path
    except Exception as e:
        print(f"⚠️ Backup failed for {obj_id}: {e}")
        return None

def normalize_list_value(v, delim=','):
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [str(x).strip() for x in v if x is not None and str(x).strip()!='']
    if isinstance(v, str):
        parts = [p.strip() for p in v.split(delim)]
        return [p for p in parts if p!='']
    return [str(v)]

def ensure_list_types(obj, list_props=LIST_PROPERTIES):
    for k in list_props:
        if k in obj and obj[k] is not None:
            obj[k] = normalize_list_value(obj[k])

def format_property_for_message(prop_name):
    if not prop_name:
        return prop_name
    s = prop_name.replace('_', ' ')
    out = []
    for ch in s:
        if ch.isupper() and out and out[-1] != ' ' and out[-1].islower():
            out.append(' ')
        out.append(ch)
    return ''.join(out).strip().title()

def compose_updated_details(changed_props, created_first_time=False):
    if created_first_time:
        return 'First time created'
    changed = [format_property_for_message(p) for p in changed_props if p != 'updatedDetails']
    if not changed:
        return 'Object Updated'
    return f"{', '.join(changed)} Updated"

def smart_merge(existing, incoming, preserve_if_empty=PRESERVE_IF_EMPTY, list_props=LIST_PROPERTIES):
    """Merge incoming into existing intelligently. Returns (merged, changed_keys)."""
    merged = dict(existing)
    incoming_norm = dict(incoming)
    # Normalize lists on both sides
    for lp in list_props:
        if lp in incoming_norm:
            incoming_norm[lp] = normalize_list_value(incoming_norm.get(lp))
        if lp in merged:
            merged[lp] = normalize_list_value(merged.get(lp))
    changed = []
    all_keys = set(list(existing.keys()) + list(incoming_norm.keys()))
    for key in all_keys:
        old_val = existing.get(key)
        new_val = incoming_norm.get(key, None)
        is_new_empty = new_val is None or (isinstance(new_val, str) and new_val.strip()=='') or (isinstance(new_val, (list,tuple)) and len(new_val)==0)
        is_old_empty = old_val is None or (isinstance(old_val, str) and str(old_val).strip()=='') or (isinstance(old_val, (list,tuple)) and len(old_val)==0)
        # Preserve old if instructed
        if key in preserve_if_empty and is_new_empty and (not is_old_empty):
            merged[key] = old_val
            continue
        # If list prop
        if key in list_props:
            if key not in incoming_norm:
                # incoming didn't include key -> keep existing
                continue
            merged[key] = normalize_list_value(new_val)
        else:
            if key in incoming_norm:
                merged[key] = new_val
            else:
                continue
        if old_val != merged.get(key):
            changed.append(key)
    return merged, changed

# ---------------------- End of smart merge helpers ----------------------------


def _normalize_month_name(m):
    mk = m.strip().lower()
    if mk in _MONTHS:
        return _MONTHS[mk]
    if mk in _SHORT_MONTHS:
        return _SHORT_MONTHS[mk]
    return m.capitalize()


def format_date_str(s):
    """Try to find a date in `s` and return it as 'DD Month YYYY'. Returns None if not found."""
    if not s:
        return None
    s = s.strip()
    m = re.search(r'([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})', s)
    if m:
        month = _normalize_month_name(m.group(1))
        day = str(int(m.group(2)))
        year = m.group(3)
        return f"{day} {month} {year}"
    m2 = re.search(r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', s)
    if m2:
        day = str(int(m2.group(1)))
        month = _normalize_month_name(m2.group(2))
        year = m2.group(3)
        return f"{day} {month} {year}"
    return None


def format_date_range(s):
    """Find a date range and format as 'DD Month YYYY - DD Month YYYY' or return single formatted date."""
    if not s:
        return None
    m = re.search(r'([A-Za-z0-9,\s]+?)\s*[\-–]\s*([A-Za-z0-9,\s]+)', s)
    if m:
        d1 = format_date_str(m.group(1))
        d2 = format_date_str(m.group(2))
        if d1 and d2:
            return f"{d1} - {d2}"
    d = format_date_str(s)
    if d:
        return d
    return None


# ---------------------------- Image helpers --------------------------------
def download_image_to(url, path):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
            img = Image.open(BytesIO(r.content))
            img = img.convert("RGB")
            max_w, max_h = 600, 900
            img.thumbnail((max_w, max_h), Image.LANCZOS)
            os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
            img.save(path, format="JPEG", quality=90)
            return True
    except Exception as e:
        logd(f"image download failed: {e}")
    return False


def build_absolute_url(local_path):
    local_path = local_path.replace("\\", "/")
    return GITHUB_PAGES_URL.rstrip("/") + "/" + local_path.lstrip("/")


# ---------------------------- Web search helpers ----------------------------
def try_ddgs_text(query, max_results=6):
    if not HAVE_DDGS:
        return []
    try:
        with DDGS() as dd:
            return list(dd.text(query, max_results=max_results))
    except Exception as e:
        logd(f"DDGS text error: {e}")
        return []


def ddgs_images(query, max_results=6):
    if not HAVE_DDGS:
        return []
    try:
        with DDGS() as dd:
            return [r.get("image") for r in dd.images(query, max_results=max_results) if r.get("image")]
    except Exception as e:
        logd(f"DDGS image error: {e}")
        return []


def fetch_page(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        logd(f"fetch page error: {e}")
    return None


def pick_best_result(results):
    if not results:
        return None
    for r in results:
        url = r.get("href") or r.get("url") or r.get("link") or ""
        if any(site in url for site in ["mydramalist.com", "asianwiki.com", "wikipedia.org"]):
            return url
    return results[0].get("href") or results[0].get("url") or None


# ---------------------------- Parsing synopsis & metadata ------------------
def parse_synopsis_from_html(html, base_url):

    # Detailed parsing behaviour for this function:
    # - Try site-specific extraction first by checking the base_url domain.
    # - For AsianWiki: synopsis often lives under elements labelled 'synopsis' or in paragraphs after a 'Synopsis' header.
    # - For MyDramaList: synopsis is sometimes in meta description or inside a div with class containing 'synopsis' or 'summary'.
    # - Generic fallback: use meta description (og:description) or the first long paragraph (>80 chars).
    # - Always strip extraneous whitespace and remove parenthetical CJK text (these often duplicate otherNames).
            
    """Parse synopsis, duration, and metadata like otherNames and releaseDate."""
    soup = BeautifulSoup(html, "lxml")
    full_text = soup.get_text("\n", strip=True)
    syn_candidates = []
    meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    if meta and meta.get("content") and len(meta.get("content")) > 30:
        syn_candidates.append(meta.get("content").strip())
    for h in soup.find_all(re.compile("^h[1-6]$")):
        txt = h.get_text(" ", strip=True).lower()
        if any(k in txt for k in ("plot", "synopsis", "story", "summary")):
            parts = []
            for sib in h.find_next_siblings():
                if sib.name and re.match(r'^h[1-6]$', sib.name.lower()):
                    break
                if sib.name == 'p':
                    parts.append(sib.get_text(" ", strip=True))
                if sib.name in ('div', 'section'):
                    txt_inner = sib.get_text(" ", strip=True)
                    if txt_inner:
                        parts.append(txt_inner)
                if len(parts) >= 6:
                    break
            if parts:
                syn_candidates.append("\n\n".join(parts))
                break
    if not syn_candidates:
        for p in soup.find_all('p'):
            txt = p.get_text(" ", strip=True)
            if len(txt) > 80:
                syn_candidates.append(txt)
                break
    syn = syn_candidates[0] if syn_candidates else None
    duration = None
    try:
        lower = full_text.lower()
        m = re.search(r'(\b\d{2,3})\s*(?:min|minutes)\b', lower)
        if m:
            duration = int(m.group(1))
        else:
            m2 = re.search(r'runtime[^0-9]*(\d{1,3})', lower)
            if m2:
                duration = int(m2.group(1))
    except Exception:
        duration = None

    metadata = {}
    m = re.search(r'Also\s+Known\s+As[:\s]*([^\n\r]+)', full_text, flags=re.I)
    if m:
        other_raw = m.group(1).strip()
        metadata['otherNames'] = [p.strip() for p in re.split(r',\s*', other_raw) if p.strip()]
    else:
        metadata['otherNames'] = []

    m3 = re.search(r'(Release\s+Date|Aired|Aired on|Original release)[:\s]*([^\n\r]+)', full_text, flags=re.I)
    if m3:
        raw = m3.group(2).strip()
        rfmt = format_date_range(raw)
        if rfmt:
            metadata['releaseDateRaw'] = raw
            metadata['releaseDate'] = rfmt
        else:
            metadata['releaseDateRaw'] = raw
            metadata['releaseDate'] = raw
    else:
        m4 = re.search(r'([A-Za-z]+\s+\d{1,2},\s*\d{4})', full_text)
        if m4:
            metadata['releaseDateRaw'] = m4.group(1).strip()
            metadata['releaseDate'] = format_date_str(metadata['releaseDateRaw'])
        else:
            metadata['releaseDate'] = None

    if syn:
        syn = clean_parenthesis_remove_cjk(syn)
        paragraphs = [normalize_whitespace_and_sentences(p) for p in syn.split('\n\n') if p.strip()]
        syn = '\n\n'.join(paragraphs)
    domain = re.sub(r'^https?://(www\.)?', '', base_url).split('/')[0] if base_url else ''
    label = 'AsianWiki' if 'asianwiki' in domain else ('MyDramaList' if 'mydramalist' in domain else domain)
    syn_with_src = f"{syn} (Source: {label})" if syn else None
    return syn_with_src, duration, full_text, metadata


def ddgs_text(query):
    if HAVE_DDGS:
        return try_ddgs_text(query, max_results=6)
    return []


def fetch_synopsis_and_duration(show_name, year, prefer_sites=None, existing_synopsis=None, allow_replace=False):

    # This function orchestrates searching (via DuckDuckGo text API) and parsing for synopsis.
    # - prefer_sites: a list in order of priority (e.g., ['asianwiki', 'mydramalist']).
    # - existing_synopsis: if present and allow_replace=False (and not a scheduled run), the function returns it to avoid overwriting.
    # - To add a new search provider/site, add corresponding query templates below and consider adding a site-specific parser in parse_synopsis_from_html.
    """Search for synopsis and structured metadata. Uses preferred sites order."""
    if existing_synopsis and not SCHEDULED_RUN and not allow_replace:
        return existing_synopsis, None, None, len(existing_synopsis), {}
    queries = []
    if prefer_sites:
        for s in prefer_sites:
            if s == "asianwiki":
                queries.append(f"{show_name} {year} site:asianwiki.com")
            if s == "mydramalist":
                queries.append(f"{show_name} {year} site:mydramalist.com")
    queries += [
        f"{show_name} {year} drama synopsis",
        f"{show_name} {year} synopsis site:mydramalist.com",
        f"{show_name} {year} synopsis site:asianwiki.com"
    ]
    for q in queries:
        logd(f"Synopsis query: {q}")
        results = ddgs_text(q)
        url = pick_best_result(results) if results else None
        urls_to_try = [url] if url else []
        for u in urls_to_try:
            if not u:
                continue
            html = fetch_page(u)
            if not html:
                continue
            syn, dur, fulltext, metadata = parse_synopsis_from_html(html, u)
            if syn:
                # Normalize and ensure source appended
                syn = syn.strip()
                if '(source:' not in syn.lower():
                    domain = re.sub(r'^https?://(www\\.)?', '', u).split('/')[0] if u else ''
                    label = 'AsianWiki' if 'asianwiki' in domain else ('MyDramaList' if 'mydramalist' in domain else domain)
                    syn = syn + ' (Source: ' + (label or domain) + ')'
                # Soft truncate to SYNOPSIS_MAX_LEN preserving sentence end if possible
                if len(syn) > SYNOPSIS_MAX_LEN:
                    cut = syn[:SYNOPSIS_MAX_LEN]
                    last_period = cut.rfind('.')
                    if last_period > int(SYNOPSIS_MAX_LEN*0.6):
                        syn = cut[:last_period+1]
                    else:
                        syn = cut
                orig_len = len(syn)
                return syn, dur, u, orig_len, metadata
            time.sleep(0.4)
    return (existing_synopsis or "Synopsis not available."), None, None, len(existing_synopsis or ""), {}


# ---------------------------- Excel -> objects mapping ----------------------
COLUMN_MAP = {
    "no": "showID",
    "series title": "showName",
    "title": "showName",
    "started date": "watchStartedOn",
    "finished date": "watchEndedOn",
    "year": "releasedYear",
    "total episodes": "totalEpisodes",
    "released year": "releasedYear",
    "original language": "nativeLanguage",
    "language": "watchedLanguage",
    "ratings": "ratings",
    "catagory": "genres",
    "category": "genres",
    "genres": "genres",
    "geners": "genres",
    "genre": "genres",
    "other names": "otherNames",
    "othernames": "otherNames",
    "otherNames": "otherNames",
    "original network": "network",
    "comments": "comments",
}




def tidy_comment(val):
    if pd.isna(val) or not str(val).strip():
        return None
    text = re.sub(r'\s+', ' ', str(val)).strip()
    if not text.endswith('.'):
        text = text + '.'
    text = re.sub(r'\.([^\s])', r'. \1', text)
    return text


def sheet_base_offset(sheet_name: str) -> int:
    if sheet_name == "Sheet1":
        return 100
    if sheet_name == "Feb 7 2023 Onwards":
        return 1000
    if sheet_name == "Sheet2":
        return 3000
    return 0


def excel_to_objects(excel_file, sheet_name, existing_by_id, report_changes, start_index=0, max_items=None, time_limit_seconds=None,
                     deleted_ids_for_run=None, deleting_not_found_initial=None, deleting_found_in_sheets=None):
    """Read rows from a sheet and transform to ordered objects.

    Notable behaviour:
      - excel_file may be a file path or a file-like object (BytesIO).
    """
    # pandas.read_excel accepts a file-like object or path
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.columns = [c.strip().lower() for c in df.columns]
    again_idx = None
    for i, c in enumerate(df.columns):
        if "again watched" in c:
            again_idx = i
            break
    if again_idx is None:
        raise ValueError(f"'Again Watched Date' columns not found in sheet: {sheet_name}")
    items = []
    processed = 0
    start_time = time.time()
    last_idx = start_index
    total_rows = len(df)
    for idx in range(start_index, total_rows):
        if max_items and processed >= max_items:
            break
        if time_limit_seconds and (time.time() - start_time) >= time_limit_seconds:
            break
        row = df.iloc[idx]
        obj = {}
        try:
            for col in df.columns[:again_idx]:
                key = COLUMN_MAP.get(col, col)
                val = row[col]
                if key == "showID":
                    base = sheet_base_offset(sheet_name)
                    obj["showID"] = base + int(val) if pd.notna(val) else None
                elif key == "showName":
                    raw_name = str(val) if pd.notna(val) else ""
                    clean_name = re.sub(r'\s+', ' ', raw_name).strip()
                    obj["showName"] = clean_name if clean_name else None
                elif key in ("watchStartedOn", "watchEndedOn"):
                    obj[key] = ddmmyyyy(val)
                elif key == "releasedYear":
                    obj[key] = int(val) if pd.notna(val) else None
                elif key == "totalEpisodes":
                    obj[key] = int(val) if pd.notna(val) else None
                elif key == "nativeLanguage":
                    obj[key] = str(val).strip().capitalize() if pd.notna(val) else None
                elif key == "watchedLanguage":
                    obj[key] = str(val).strip().capitalize() if pd.notna(val) else None
                elif key == "comments":
                    obj[key] = tidy_comment(val)
                elif key == "ratings":
                    try:
                        obj[key] = int(val) if pd.notna(val) else 0
                    except Exception:
                        obj[key] = 0
                elif key == "genres":
                    obj[key] = normalize_list_from_csv(val, cap=True, strip=True)
                elif key == "network":
                    obj[key] = normalize_list_from_csv(val, cap=False, strip=True)
                else:
                    obj[key] = str(val).strip() if pd.notna(val) else None
            obj["showType"] = "Mini Drama" if sheet_name.lower() == "mini drama" else "Drama"
            obj["country"] = None
            native = obj.get("nativeLanguage")
            if native:
                n = str(native).strip().lower()
                if n in ("korean", "korea", "korean language"):
                    obj["country"] = "South Korea"
                elif n in ("chinese", "china", "mandarin"):
                    obj["country"] = "China"
                elif n in ("japanese", "japan"):
                    obj["country"] = "Japan"
            dates = [ddmmyyyy(v) for v in row[again_idx:] if ddmmyyyy(v)]
            obj["againWatchedDates"] = dates
            obj["updatedOn"] = now_ist().strftime("%d %B %Y")
            obj["updatedDetails"] = "First time Uploaded"
            r = int(obj.get("ratings") or 0)
            obj["topRatings"] = r * len(dates) * 100
            obj.setdefault("otherNames", [])
            obj["Duration"] = None

            sid = obj.get("showID")
            if deleted_ids_for_run and sid in deleted_ids_for_run:
                report_changes.setdefault('ignored_deleting', []).append(
                    f'{sid} -> Already Deleted as per "Deleting Records" Sheet -> ⚠️ Cannot add to seriesData.json'
                )
                if deleting_not_found_initial and sid in deleting_not_found_initial:
                    deleting_found_in_sheets.add(sid)
                continue

            if deleting_not_found_initial and sid in deleting_not_found_initial:
                deleting_found_in_sheets.add(sid)

            show_name = obj.get("showName")
            released_year = obj.get("releasedYear")
            prefer = None
            if native and native.lower().startswith('korean'):
                prefer = ["asianwiki", "mydramalist"]
            elif native and native.lower().startswith('chinese'):
                prefer = ["mydramalist", "asianwiki"]
            else:
                prefer = ["mydramalist", "asianwiki"]

            existing = existing_by_id.get(obj.get("showID")) if obj.get("showID") is not None else None
            existing_image_url = existing.get("showImage") if existing else None
            new_image_url = existing_image_url or None

            if SCHEDULED_RUN and (existing_image_url is None):
                try:
                    local_image_path, remote_image_url = fetch_and_save_image_for_show(show_name or "", prefer, obj.get("showID"))
                    if local_image_path:
                        new_image_url = build_absolute_url(local_image_path)
                        report_changes.setdefault('images', []).append({'showName': show_name, 'old': existing_image_url, 'new': new_image_url})
                except Exception as e:
                    logd(f"Image fetch failed for {show_name}: {e}")

            obj["showImage"] = new_image_url

            existing_syn = existing.get("synopsis") if existing else None
            new_syn, dur, syn_url, orig_len, metadata = fetch_synopsis_and_duration(show_name or "", released_year or "",
                                                                                 prefer_sites=prefer, existing_synopsis=existing_syn,
                                                                                 allow_replace=False)
            if new_syn and isinstance(new_syn, str):
                new_syn = normalize_whitespace_and_sentences(new_syn)
            obj["synopsis"] = new_syn

            if metadata and metadata.get('otherNames'):
                obj['otherNames'] = metadata.get('otherNames')
            if metadata and metadata.get('releaseDate'):
                obj['releaseDate'] = metadata.get('releaseDate')
            else:
                obj['releaseDate'] = None

            if dur is not None and dur > 0:
                obj['Duration'] = int(dur)
            elif existing and existing.get('Duration'):
                obj['Duration'] = existing.get('Duration')

            ordered = {
                "showID": obj.get("showID"),
                "showName": obj.get("showName"),
                "otherNames": obj.get("otherNames", []),
                "showImage": obj.get("showImage"),
                "watchStartedOn": obj.get("watchStartedOn"),
                "watchEndedOn": obj.get("watchEndedOn"),
                "releasedYear": obj.get("releasedYear"),
                # NEW property: releaseDate (string).
                "releaseDate": obj.get("releaseDate"),
                "totalEpisodes": obj.get("totalEpisodes"),
                "showType": obj.get("showType"),
                "nativeLanguage": obj.get("nativeLanguage"),
                "watchedLanguage": obj.get("watchedLanguage"),
                "country": obj.get("country"),
                "comments": obj.get("comments"),
                "ratings": obj.get("ratings"),
                "genres": obj.get("genres"),
                "network": obj.get("network"),
                "againWatchedDates": obj.get("againWatchedDates"),
                "updatedOn": obj.get("updatedOn"),
                "updatedDetails": obj.get("updatedDetails"),
                "synopsis": obj.get("synopsis"),
                "topRatings": obj.get("topRatings"),
                "Duration": obj.get("Duration")
            }
            items.append(ordered)
            processed += 1
            last_idx = idx
            sid = ordered.get("showID")
            if existing is None:
                report_changes.setdefault("created", []).append(ordered)
            else:
                if existing != ordered:
                    report_changes.setdefault("updated", []).append({"old": existing, "new": ordered})
            if orig_len and orig_len > SYNOPSIS_MAX_LEN:
                report_changes.setdefault('exceed', []).append({"id": sid, "name": ordered.get('showName'),
                                                                "year": ordered.get('releasedYear'),
                                                                "site": syn_url or "", "url": syn_url or "", "orig_len": orig_len})
        except Exception as e:
            raise RuntimeError(f"Row {idx} in sheet '{sheet_name}' processing failed: {e}")
    finished = (last_idx >= total_rows - 1) if total_rows > 0 else True
    next_index = (last_idx + 1) if processed > 0 else start_index
    return items, processed, finished, next_index


# ---------------------------- Deletion processing ---------------------------
def process_deletions(excel_file, json_file, report_changes):
    """Read the 'Deleting Records' sheet and remove any showIDs present in seriesData.json."""
    try:
        df = pd.read_excel(excel_file, sheet_name='Deleting Records')
    except Exception:
        return [], []
    if df.shape[1] < 1:
        return [], []
    cols = [str(c).strip().lower() for c in df.columns]
    id_col = None
    for i, c in enumerate(cols):
        if c == 'id' or 'id' in c:
            id_col = df.columns[i]
            break
    if id_col is None:
        id_col = df.columns[0]
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as jf:
                data = json.load(jf)
        except Exception:
            data = []
    else:
        data = []
    by_id = {int(o['showID']): o for o in data if 'showID' in o and isinstance(o['showID'], int)}
    to_delete = []
    for _, row in df.iterrows():
        val = row[id_col]
        if pd.isna(val):
            continue
        try:
            to_delete.append(int(val))
        except Exception:
            continue
    if not to_delete:
        return [], []
    os.makedirs(DELETED_DATA_DIR, exist_ok=True)
    deleted_ids = []
    not_found_ids = []
    for iid in to_delete:
        if iid in by_id:
            deleted_obj = by_id.pop(iid)
            deleted_ids.append(iid)
            fname = f"DELETED_{now_ist().strftime('%d_%B_%Y_%H%M')}_{iid}.json"
            outpath = os.path.join(DELETED_DATA_DIR, safe_filename(fname))
            try:
                with open(outpath, 'w', encoding='utf-8') as of:
                    json.dump(deleted_obj, of, indent=4, ensure_ascii=False)
                report_changes.setdefault('deleted', []).append(f"{iid} -> ✅ Deleted and archived -> {outpath}")

                # --- Move associated image if exists ---
                try:
                    img_url = deleted_obj.get('showImage') or ""
                    if img_url:
                        candidate = None
                        m = re.search(r'/(images/[^/?#]+)$', img_url)
                        if m:
                            candidate = m.group(1)
                        elif img_url.startswith('images/'):
                            candidate = img_url
                        if candidate:
                            src = os.path.join('.', candidate)
                            if os.path.exists(src):
                                os.makedirs(OLD_IMAGES_DIR, exist_ok=True)
                                dst_name = f"{filename_timestamp()}_{os.path.basename(src)}"
                                dst = os.path.join(OLD_IMAGES_DIR, safe_filename(dst_name))
                                shutil.move(src, dst)
                                report_changes.setdefault('deleted_images_moved', []).append(
                                    f"{iid} -> image moved: {src} -> {dst}"
                                )
                except Exception as e_img:
                    report_changes.setdefault('deleted_images_moved', []).append(f"{iid} -> ⚠️ Image move failed: {e_img}")

            except Exception as e:
                report_changes.setdefault('deleted', []).append(f"{iid} -> ⚠️ Deletion recorded but failed to write archive: {e}")
        else:
            not_found_ids.append(iid)
            report_changes.setdefault('deleted_not_found', []).append(f"-{iid} -> ❌ Not found in seriesData.json")
    merged = sorted(by_id.values(), key=lambda x: x.get('showID', 0))
    try:
        with open(json_file, 'w', encoding='utf-8') as jf:
            json.dump(merged, jf, indent=4, ensure_ascii=False)
        report_changes.setdefault('deleted_summary', []).append(
            f"seriesData.json updated after deletions (deleted {len(deleted_ids)} items)."
        )
    except Exception as e:
        report_changes.setdefault('deleted_summary', []).append(f"⚠️ Failed to write updated {json_file}: {e}")
    return deleted_ids, not_found_ids

def cleanup_deleted_data():
    if not os.path.exists(DELETED_DATA_DIR):
        return
    cutoff = datetime.now()() - timedelta(days=30)
    for fname in os.listdir(DELETED_DATA_DIR):
        path = os.path.join(DELETED_DATA_DIR, fname)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                os.remove(path)
                print(f"🗑️ Removed expired deleted-data file: {path}")
        except Exception as e:
            print(f"⚠️ Could not cleanup deleted-data {path}: {e}")


# ---------------------------- Manual updates --------------------------------
def apply_manual_updates(excel_file: str, json_file: str):
    """Apply ad-hoc JSON-like updates from a 'manual update' sheet.

    Note: in this patched script we do not rely on a local Excel file; the 'excel_file'
    parameter should be a file-like object (BytesIO) or a path-like object if your workflow
    provides a local copy.
    """
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
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
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
                try:
                    obj["ratings"] = int(v)
                except Exception:
                    obj["ratings"] = obj.get("ratings", 0)
            elif k.lower() in ("releasedyear", "year"):
                try:
                    obj["releasedYear"] = int(v)
                except Exception:
                    pass
            else:
                obj[k] = v
        obj['updatedOn'] = now_ist().strftime('%d %B %Y')
        obj['updatedDetails'] = f"Updated {', '.join([k.capitalize() for k in upd.keys()])} Manually By Owner"
        updated_objs.append(obj)
    if updated_objs:
        merged = sorted(by_id.values(), key=lambda x: x.get('showID', 0))
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(merged, f, indent=4, ensure_ascii=False)
        print(f"✅ Applied {len(updated_objs)} manual updates")
    else:
        print("ℹ️ No valid manual updates found/applied.")


# ---------------------------- Image searcher (site-priority) ----------------
def fetch_and_save_image_for_show(show_name, prefer_sites, show_id):

    # Image fetching strategy:
    # - prefer_sites order is used to form targeted queries like '<show_name> <year> site:asianwiki.com'.
    # - Primary extraction: use meta property 'og:image' or first prominent <img> (poster) in the page.
    # - If direct page extraction fails, fall back to DuckDuckGo images (ddgs_images) results.
    # - Saved image naming: prefer '<show_id>.jpg' if show_id present; otherwise a sanitized version of show_name.
    # - If you add new preferred sites, include them in both the query candidates and consider per-site extraction logic.
    """Search for a show image using preferred sites (order matters)."""
    if not show_name:
        return None, None
    candidates = []
    if prefer_sites:
        for s in prefer_sites:
            if s == 'asianwiki':
                candidates.append(f"{show_name} site:asianwiki.com")
            if s == 'mydramalist':
                candidates.append(f"{show_name} site:mydramalist.com")
    candidates += [f"{show_name} poster image", f"{show_name} drama poster"]
    for q in candidates:
        logd(f"Image query: {q}")
        results = ddgs_text(q)
        url = pick_best_result(results) if results else None
        urls_to_try = [url] if url else []
        for u in urls_to_try:
            if not u:
                continue
            html = fetch_page(u)
            if not html:
                continue
            soup = BeautifulSoup(html, 'lxml')
            og = soup.find('meta', property='og:image')
            if og and og.get('content'):
                img_url = og.get('content')
            else:
                img_tag = soup.find('img')
                img_url = img_tag.get('src') if img_tag and img_tag.get('src') else None
            if img_url:
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                if img_url.startswith('/'):
                    base = re.match(r'^(https?://[^/]+)', u)
                    if base:
                        img_url = base.group(1) + img_url
                local_name = f"{show_id}.jpg" if show_id else safe_filename(show_name) + '.jpg'
                local_path = os.path.join(IMAGES_DIR, local_name)
                ok = download_image_to(img_url, local_path)
                if ok:
                    return local_path, img_url
    try:
        imgs = ddgs_images(show_name)
        for img in imgs:
            if img:
                local_name = f"{show_id}.jpg" if show_id else safe_filename(show_name) + '.jpg'
                local_path = os.path.join(IMAGES_DIR, local_name)
                if download_image_to(img, local_path):
                    return local_path, img
    except Exception as e:
        logd(f"ddgs image fallback failed: {e}")
    return None, None


# ---------------------------- Reports --------------------------------------
def write_report(report_changes_by_sheet, report_path, final_not_found_deletions=None):
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
            lines.append("\nData Created:")
            for obj in created:
                lines.append(f"- {obj.get('showName', 'Unknown')} -> Created")
        updated = changes.get('updated', [])
        total_updated += len(updated)
        if updated:
            lines.append("\nData Updated:")
            # Consolidate updates per object
            updates_by_name = {}
            for pair in updated:
                try:
                    oldn = pair.get('old', {})
                    newn = pair.get('new', {})
                    nm = newn.get('showName', oldn.get('showName', 'Unknown'))
                    diff = [k for k in newn.keys() if oldn.get(k) != newn.get(k)]
                    updates_by_name.setdefault(nm, set()).update(diff)
                except Exception:
                    updates_by_name.setdefault(str(pair), set())

            for nm, keys in updates_by_name.items():
                details = compose_updated_details(keys)
                lines.append(f"- {nm} -> {details}")
    if exceed_entries:
        lines.append(f"=== Exceed Max Length ({SYNOPSIS_MAX_LEN}) ===")
        for e in exceed_entries:
            lines.append(f"{e.get('id')} -> {e.get('name')} ({e.get('year')}) -> {e.get('site')} -> Link: {e.get('url')}")
        lines.append("\n")
    if final_not_found_deletions:
        lines.append("=== NOT FOUND (Deleting Records not present in any scanned sheet) ===")
        for iid in final_not_found_deletions:
            lines.append(f"-{iid} -> ❌ Cannot be found in any Sheets.")
        lines.append("\n")
    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
    except Exception as e:
        print(f"⚠️ Could not write TXT report: {e}")


# ---------------------------- Secret scan & email body ---------------------
def scan_for_possible_secrets():
    findings = []
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        try:
            s = open(SERVICE_ACCOUNT_FILE, 'r', encoding='utf-8').read()
            has_private_key = 'private_key' in s
            m = re.search(r'"client_email"\s*:\s*"([^"]+)"', s)
            client_email = m.group(1) if m else None
            findings.append({'file': SERVICE_ACCOUNT_FILE, 'present': True, 'client_email': client_email, 'has_private_key': bool(has_private_key), 'note': 'Service account JSON detected'})
        except Exception as e:
            findings.append({'file': SERVICE_ACCOUNT_FILE, 'present': True, 'note': f'Could not read file safely: {e}'})
    if os.path.exists(EXCEL_FILE_ID_TXT):
        try:
            s = open(EXCEL_FILE_ID_TXT, 'r', encoding='utf-8').read().strip()
            findings.append({'file': EXCEL_FILE_ID_TXT, 'present': True, 'length': len(s), 'note': 'Excel file id present (not shown)'})
        except Exception as e:
            findings.append({'file': EXCEL_FILE_ID_TXT, 'present': True, 'note': f'Could not read file: {e}'})
    for fname in os.listdir('.'):
        lower = fname.lower()
        if any(k in lower for k in ('.env', 'secret', 'credential', 'key', '.pem', '.p12')):
            findings.append({'file': fname, 'present': True, 'note': 'Suspicious filename - check for secrets'})
    return findings



def compose_email_body_from_report(report_path):
    """Compose a single inline email body containing a secrets check followed by the full report text.
    This string is intended to be used by the workflow runner to send a single email per run.
    We purposely do NOT write an `email_body_*.txt` file to disk anymore; the report file `report_*.txt`
    is kept in the repo while the composed email body is used by the runner to send the message.
    """
    body_lines = []
    # First section: secrets check (so recipients immediately see any exposed credentials)
    body_lines.append("SECRETS CHECK:")
    findings = scan_for_possible_secrets()
    if not findings:
        body_lines.append("- No obvious secret files detected in the workspace.")
    else:
        for f in findings:
            line = f"- File: {f.get('file')} — note: {f.get('note')}."
            if f.get('client_email'):
                line += f" client_email: {f.get('client_email')}."
            if f.get('has_private_key'):
                line += " Contains a private_key field (DO NOT share private key material)."
            if f.get('length') is not None:
                line += f" length: {f.get('length')} characters (value not shown)."
            body_lines.append(line)
        body_lines.append("\nIf any of the above files were accidentally committed to your repository: (1) rotate/disable keys, (2) remove the files from the repo (git filter-repo / bfg), (3) re-issue new credentials.")
    body_lines.append("\n--- REPORT CONTENT (pasted below) ---\n")
    body_lines.append(f"Run Report — {now_ist().strftime('%d %B %Y %H:%M')}")
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            body_lines.append(f.read())
    except Exception as e:
        body_lines.append(f"⚠️ Could not read report file for email body: {e}")
    return "\n".join(body_lines)

def fetch_excel_from_gdrive_bytes(excel_file_id, service_account_path):
    """
    Attempt to fetch the file bytes for the given file id using Drive API and service account.
    Returns BytesIO on success or None on failure.
    """
    if not HAVE_GOOGLE_API:
        print("ℹ️ google-api-python-client or google-auth not available in this environment.")
        print("   Install dependencies in your workflow runner: pip install google-api-python-client google-auth-httplib2 google-auth")
        return None
    try:
        scopes = ['https://www.googleapis.com/auth/drive.readonly']
        creds = service_account.Credentials.from_service_account_file(service_account_path, scopes=scopes)
        drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        # First try to download as binary (files.get_media)
        try:
            request = drive_service.files().get_media(fileId=excel_file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logd(f"Download progress: {int(status.progress() * 100)}%")
            fh.seek(0)
            return fh
        except Exception as e_bin:
            logd(f"files().get_media failed ({e_bin}), trying files().export (Sheets export)...")
            try:
                export_mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                request = drive_service.files().export_media(fileId=excel_file_id, mimeType=export_mime)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    logd(f"Export progress: {int(status.progress() * 100)}%")
                fh.seek(0)
                return fh
            except Exception as e_export:
                logd(f"files().export failed: {e_export}")
                return None
    except Exception as e:
        logd(f"Google Drive fetch failed: {e}")
        logd(traceback.format_exc())
        return None


# ---------------------------- Main updater ---------------------------------
def update_json_from_excel(excel_file_like, json_file, sheet_names, max_per_run=0, max_run_time_minutes=0):
    processed_total = 0
    # Load existing JSON (if any)
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                old_objects = json.load(f)
        except Exception:
            print(f"⚠️ {json_file} invalid. Starting fresh.")
            old_objects = []
    else:
        old_objects = []
    old_by_id = {o['showID']: o for o in old_objects if 'showID' in o}
    merged_by_id = dict(old_by_id)
    report_changes_by_sheet = {}

    # 1) Process deletions first and receive lists back.
    try:
        deleted_ids, deleting_not_found_initial = process_deletions(excel_file_like, json_file,
                                                                    report_changes_by_sheet.setdefault('Deleting Records', {}))
    except Exception as e:
        report_changes_by_sheet.setdefault('Deleting Records', {})['error'] = str(e)
        deleted_ids, deleting_not_found_initial = [], []

    # After deletions, reload the JSON file to get the 'current' state for scanning sheets.
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                old_objects = json.load(f)
        except Exception:
            old_objects = []
    old_by_id = {o['showID']: o for o in old_objects if 'showID' in o}
    merged_by_id = dict(old_by_id)
    deleting_found_in_sheets = set()
    progress = load_progress()
    overall_continued = False
    time_limit_seconds = max_run_time_minutes * 60 if max_run_time_minutes > 0 else None
    any_sheet_processed = False
    for s in sheet_names:
        report_changes = {}
        per_sheet_old_objs = []
        start_idx = int(progress.get(s, 0) or 0)
        try:
            items, processed, finished, next_start_idx = excel_to_objects(excel_file_like, s, merged_by_id, report_changes,
                                                                          start_index=start_idx,
                                                                          max_items=(max_per_run if max_per_run > 0 else None),
                                                                          time_limit_seconds=time_limit_seconds,
                                                                          deleted_ids_for_run=set(deleted_ids),
                                                                          deleting_not_found_initial=set(deleting_not_found_initial),
                                                                          deleting_found_in_sheets=deleting_found_in_sheets)
        except Exception as e:
            err = str(e)
            print(f"⚠️ Error processing {s}: {err}")
            report_changes['error'] = err
            items, processed, finished, next_start_idx = [], 0, True, start_idx
        
        for new_obj in items:
                    sid = new_obj.get('showID')
                    if sid in merged_by_id:
                        old_obj = merged_by_id[sid]
                        # perform intelligent merge instead of blind replace
                        ensure_list_types(old_obj, LIST_PROPERTIES)
                        ensure_list_types(new_obj, LIST_PROPERTIES)
                        merged_obj, changed_keys = smart_merge(old_obj, new_obj)
                        if changed_keys:
                            # backup existing object BEFORE writing
                            try:
                                # Backup the PREVIOUS state of the object (deepcopy to avoid later mutation)
                                bpath = backup_object(old_obj.get('showID') or old_obj.get('id') or sid, copy.deepcopy(old_obj), backup_dir=BACKUP_DIR)
                                if bpath:
                                    report_changes.setdefault('backups', []).append(bpath)
                                    # Remember prior object for the per-sheet compact backup
                                    try:
                                        per_sheet_old_objs.append(copy.deepcopy(old_obj))
                                    except Exception:
                                        pass
                            except Exception as e:
                                print(f"⚠️ Backup failed for {sid}: {e}")
                                print(f"⚠️ Backup failed for {sid}: {e}")
                            # Compose granular updatedDetails
                            merged_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                            merged_obj['updatedDetails'] = compose_updated_details(changed_keys, created_first_time=False)
                            merged_by_id[sid] = merged_obj
                            report_changes.setdefault('updated', []).append({'old': old_obj, 'new': merged_obj})
                        else:
                            # No meaningful changes found; keep old object
                            merged_by_id[sid] = old_obj
                            report_changes.setdefault('unchanged', []).append(sid)
                    else:
                        # New object - ensure lists normalized and mark created info
                        ensure_list_types(new_obj, LIST_PROPERTIES)
                        new_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                        new_obj['updatedDetails'] = 'First time created'
                        merged_by_id[sid] = new_obj

                        # ✅ Fix: prevent duplicate "Created" messages
                        if not any(o.get('showID') == sid for o in report_changes.get('created', [])):
                            report_changes.setdefault('created', []).append(new_obj)

        os.makedirs(BACKUP_DIR, exist_ok=True)
        backup_name = os.path.join(BACKUP_DIR, f"{filename_timestamp()}_{safe_filename(s)}.json")
        try:
            if per_sheet_old_objs:
                with open(backup_name, 'w', encoding='utf-8') as bf:
                    json.dump(per_sheet_old_objs, bf, indent=4, ensure_ascii=False)
                print(f"✅ Backup saved → {backup_name} (contains {len(per_sheet_old_objs)} prior objects)")
            else:
                with open(backup_name, 'w', encoding='utf-8') as bf:
                    json.dump({'note': 'no objects changed in this sheet during this run'}, bf, indent=2)
                print(f"ℹ️ No changed objects for sheet '{s}'; wrote note → {backup_name}")
        except Exception as e:
            print(f"⚠️ Could not write backup {backup_name}: {e}")
        report_changes_by_sheet[s] = report_changes
        if processed > 0:
            any_sheet_processed = True
            processed_total += processed
        if not finished:
            progress[s] = next_start_idx
            overall_continued = True
        else:
            if s in progress:
                progress.pop(s, None)
        save_progress(progress)

    still_not_found = set(deleting_not_found_initial or []) - deleting_found_in_sheets

    merged = sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0))
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(merged, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Could not write final {json_file}: {e}")

    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(REPORTS_DIR, f"report_{filename_timestamp()}.txt")
    write_report(report_changes_by_sheet, report_path, final_not_found_deletions=sorted(list(still_not_found)))
    print(f"✅ Report written → {report_path}")

    
    # Compose the email body in-memory; do NOT persist email_body_*.txt to disk (reports only).
    email_body = compose_email_body_from_report(report_path)
    # Print to stdout between markers so CI (GitHub Actions) can capture the email body as a step output.
    try:
        print('\n===EMAIL_BODY_START===')
        print(email_body)
        print('===EMAIL_BODY_END===\n')
    except Exception as e:
        print('⚠️ Failed printing email body to stdout:', e)

    # The workflow runner should use `email_body` to send the message; it is intentionally not written to a file.

    if SCHEDULED_RUN:
        cleanup_deleted_data()
    cutoff = datetime.now()() - timedelta(days=KEEP_OLD_IMAGES_DAYS)
    if os.path.exists(OLD_IMAGES_DIR):
        for fname in os.listdir(OLD_IMAGES_DIR):
            path = os.path.join(OLD_IMAGES_DIR, fname)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(path))
                if mtime < cutoff:
                    os.remove(path)
            except Exception as e:
                print(f"⚠️ Could not cleanup old image {path}: {e}")

    status = {"continued": overall_continued, "timestamp": now_ist().strftime('%d %B %Y_%H.%M'), "processed_total": processed_total}
    try:
        with open(STATUS_JSON, 'w', encoding='utf-8') as sf:
            json.dump(status, sf, indent=2)
    except Exception as e:
        print(f"⚠️ Could not write status json: {e}")

    if processed_total == 0:
        print("⚠️ No records were processed in this run. Please check your Excel file and sheet names.")
        with open(os.path.join(REPORTS_DIR, "failure_reason.txt"), "w", encoding="utf-8") as ff:
            ff.write("No records processed. Check logs and the report.\n")
        # Exit gracefully instead of failing the workflow
        return
    return


# ---------------------------- Entrypoint -----------------------------------
if __name__ == '__main__':
    # Validate presence of GDrive credential files
    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)):
        print("❌ Missing GDrive credentials. Please set EXCEL_FILE_ID.txt and GDRIVE_SERVICE_ACCOUNT.json via GitHub secrets.")
        # exit gracefully: 3 indicates missing credentials
        sys.exit(3)

    try:
        with open(EXCEL_FILE_ID_TXT, 'r', encoding='utf-8') as f:
            excel_id = f.read().strip()
    except Exception:
        excel_id = None

    if not excel_id:
        print("❌ EXCEL_FILE_ID.txt is empty or missing. Aborting gracefully.")
        sys.exit(0)

    # Determine sheet names to process
    _sheets_env = os.environ.get("SHEETS", "").strip()
    if _sheets_env:
        SHEETS = [s.strip() for s in _sheets_env.split(";") if s.strip()]
    else:
        SHEETS = ["Sheet1"]

    # Try to fetch excel bytes from Google Drive
    excel_bytes = fetch_excel_from_gdrive_bytes(excel_id, SERVICE_ACCOUNT_FILE)
    if excel_bytes is None:
        print("❌ Could not fetch Excel file from Google Drive. Exiting gracefully.")
        print("   Ensure the service account JSON and EXCEL_FILE_ID are correct, and required packages are installed.")
        sys.exit(0)

    # pandas can read from a file-like BytesIO for read_excel
    excel_file_like = excel_bytes

    # Apply manual updates if present
    try:
        apply_manual_updates(excel_file_like, JSON_FILE)
    except Exception as e:
        logd(f"apply_manual_updates error: {e}")

    # Run update using Excel bytes from Drive
    try:
        update_json_from_excel(excel_file_like, JSON_FILE, SHEETS, max_per_run=MAX_PER_RUN, max_run_time_minutes=MAX_RUN_TIME_MINUTES)
    except SystemExit:
        # allow sys.exit in update flow to propagate if necessary
        raise
    except Exception as e:
        print(f"❌ Unexpected error during update: {e}")
        logd(traceback.format_exc())
        sys.exit(1)

    print("All done.")