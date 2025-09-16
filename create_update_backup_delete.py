
# ============================================================================
# Script: create_update_backup_delete.py
# Purpose: (Modified) Excel -> JSON automation with:
#   - Robust "Deleting Records" handling (delete at start of run; ignore later rows
#     for IDs deleted in same run; generate detailed reports including 'NOT FOUND').
#   - Email-body generation that pastes the entire run report in the message body
#     and includes a detailed secrets-exposure summary (no secret plaintext dumped).
#   - Improved synopsis + metadata fetcher (preserve paragraph breaks,
#     prefer site order per-language, extract other names, duration, releaseDate).
#   - Image search/download using preferred sites (only on automatic / scheduled runs).
#   - New property 'releaseDate' (string) placed under 'releasedYear' in output order.
#   - Very detailed comments to make future changes (including adding new languages).
# ============================================================================

import os, re, sys, time, json, io, shutil
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO
<<<<<<< HEAD
=======
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# Optional: ddgs for DuckDuckGo search (used if installed); otherwise fallback to requests only
>>>>>>> 458bc47 (Files were updated for local-data.xlsx' not found error)
try:
    from ddgs import DDGS
    HAVE_DDGS = True
except Exception:
    HAVE_DDGS = False

# ---------------------------- Timezone helpers -------------------------------
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

def filename_timestamp():
    return now_ist().strftime("%d_%B_%Y_%H%M")

<<<<<<< HEAD
# ---------------------------- Paths & Config --------------------------------
=======
# Paths and defaults
>>>>>>> 458bc47 (Files were updated for local-data.xlsx' not found error)
LOCAL_EXCEL_FILE = "downloaded-data.xlsx"
JSON_FILE = "seriesData.json"
BACKUP_DIR = "backups"
IMAGES_DIR = "images"
OLD_IMAGES_DIR = "old-images"
DELETED_DATA_DIR = "deleted-data"
REPORTS_DIR = "reports"
PROGRESS_DIR = ".progress"
PROGRESS_FILE = os.path.join(PROGRESS_DIR, "progress.json")
STATUS_JSON = os.path.join(REPORTS_DIR, "status.json")

<<<<<<< HEAD
EXCEL_FILE_ID_TXT = "EXCEL_FILE_ID.txt"
SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json"

=======
# GDrive helper file names
EXCEL_FILE_ID_TXT = "EXCEL_FILE_ID.txt"
SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json"

# Config from env
>>>>>>> 458bc47 (Files were updated for local-data.xlsx' not found error)
GITHUB_PAGES_URL = os.environ.get("GITHUB_PAGES_URL", "").strip() or "https://<your-username>.github.io/my-movie-database"
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "0") or 0)
MAX_RUN_TIME_MINUTES = int(os.environ.get("MAX_RUN_TIME_MINUTES", "0") or 0)
KEEP_OLD_IMAGES_DAYS = int(os.environ.get("KEEP_OLD_IMAGES_DAYS", "7") or 7)
SCHEDULED_RUN = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
SYNOPSIS_MAX_LEN = int(os.environ.get("SYNOPSIS_MAX_LEN", "1000") or 1000)

HEADERS = {"User-Agent":"Mozilla/5.0 (compatible; Bot/1.0)"}

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
    """Remove parentheses that contain CJK (Chinese/Japanese/Korean) text.
    This keeps English parenthetical phrases while removing parentheses that
    primarily contain non-latin scripts which often duplicate 'other names' info.
    """
    if not s: return s
    return re.sub(r'\([^)]*[\u4e00-\u9fff\u3400-\u4dbf\uac00-\ud7af][^)]*\)', '', s)

def normalize_whitespace_and_sentences(s):
    if not s: return s
    s = re.sub(r'\\s+', ' ', s).strip()
    s = re.sub(r'\\.([^\s])', r'. \\1', s)
    # keep paragraph markers as-is (we use \\n in parse stage)
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
_MONTHS = {m.lower(): m for m in ["January","February","March","April","May","June","July","August","September","October","November","December"]}
# Also accept short months like Jan, Feb
_SHORT_MONTHS = {m[:3].lower(): m for m in _MONTHS}

def _normalize_month_name(m):
    mk = m.strip().lower()
    if mk in _MONTHS:
        return _MONTHS[mk]
    if mk in _SHORT_MONTHS:
        return _SHORT_MONTHS[mk]
    # fallback: return capitalized original
    return m.capitalize()

def format_date_str(s):
    """Try to find a date in `s` and return it as 'DD Month YYYY'.
    Supports both 'May 12, 2023' and '12 May 2023' styles. Returns None if not found.
    """
    if not s: return None
    s = s.strip()
    # 1) MonthName Day, Year  -> May 12, 2023
    m = re.search(r'([A-Za-z]+)\\s+(\\d{1,2}),\\s*(\\d{4})', s)
    if m:
        month = _normalize_month_name(m.group(1))
        day = str(int(m.group(2)))
        year = m.group(3)
        return f"{day} {month} {year}"
    # 2) Day MonthName Year -> 12 May 2023
    m2 = re.search(r'(\\d{1,2})\\s+([A-Za-z]+)\\s+(\\d{4})', s)
    if m2:
        day = str(int(m2.group(1))); month = _normalize_month_name(m2.group(2)); year = m2.group(3)
        return f"{day} {month} {year}"
    return None

def format_date_range(s):
    """Try to find a date range in `s`. Return 'DD Month YYYY - DD Month YYYY' or None."""
    if not s: return None
    # capture two dates separated by '-' or '–'
    m = re.search(r'([A-Za-z0-9,\\s]+?)\\s*[\\-–]\\s*([A-Za-z0-9,\\s]+)', s)
    if m:
        d1 = format_date_str(m.group(1))
        d2 = format_date_str(m.group(2))
        if d1 and d2:
            return f"{d1} - {d2}"
    # fallback: single date
    d = format_date_str(s)
    if d: return d
    return None

# ---------------------------- Image helpers --------------------------------
def download_image_to(url, path):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200 and r.headers.get("content-type","").startswith("image"):
            img = Image.open(BytesIO(r.content))
            img = img.convert("RGB")
            # Resize to standard poster-ish size preserving aspect ratio
            max_w, max_h = 600, 900
            img.thumbnail((max_w, max_h), Image.LANCZOS)
            os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
            img.save(path, format="JPEG", quality=90)
            return True
    except Exception as e:
        logd(f"image download failed: {e}")
    return False

def build_absolute_url(local_path):
    local_path = local_path.replace("\\\\","/")
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
    if not results: return None
    for r in results:
        url = r.get("href") or r.get("url") or r.get("link") or ""
        if any(site in url for site in ["mydramalist.com","asianwiki.com","wikipedia.org"]):
            return url
    return results[0].get("href") or results[0].get("url") or None

# ---------------------------- Parsing synopsis & metadata ------------------
def parse_synopsis_from_html(html, base_url):
    """Parse synopsis, duration, and some metadata (other names, release date).
    Returns: synopsis_text (with paragraph breaks kept as \\n\\n), duration_minutes (int|None),
             full_text (entire page text with newlines), metadata(dict).
    metadata can contain: otherNames:list, releaseDateRaw:str, releaseDate:str
    """
    soup = BeautifulSoup(html, "lxml")
    # Keep page text with newlines so regex can find labeled items like 'Also Known As'
    full_text = soup.get_text("\\n", strip=True)
    # Try meta description / og:description first (often concise)
    syn_candidates = []
    meta = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
    if meta and meta.get("content") and len(meta.get("content"))>30:
        syn_candidates.append(meta.get("content").strip())
    # Try to find headings that indicate synopsis/plot and collect following paragraphs
    for h in soup.find_all(re.compile("^h[1-6]$")):
        txt = h.get_text(" ", strip=True).lower()
        if any(k in txt for k in ("plot","synopsis","story","summary")):
            # collect paragraphs until next header or long break
            parts = []
            for sib in h.find_next_siblings():
                if sib.name and re.match(r'^h[1-6]$', sib.name.lower()): break
                if sib.name == 'p':
                    parts.append(sib.get_text(" ", strip=True))
                # sometimes the synopsis is in <div> or <section>
                if sib.name in ('div','section'):
                    txt_inner = sib.get_text(" ", strip=True)
                    if txt_inner: parts.append(txt_inner)
                # protect long loops
                if len(parts)>=6: break
            if parts:
                syn_candidates.append("\\n\\n".join(parts))
                break
    # Fallback: first long paragraph on page
    if not syn_candidates:
        for p in soup.find_all('p'):
            txt = p.get_text(" ", strip=True)
            if len(txt) > 80:
                syn_candidates.append(txt); break
    syn = syn_candidates[0] if syn_candidates else None
    # Extract duration in minutes heuristically from page text
    duration = None
    try:
        lower = full_text.lower()
        # Look for patterns like '42 min' or '42 minutes' or 'Runtime: 42'
        m = re.search(r'(\\b\\d{2,3})\\s*(?:min|minutes)\\b', lower)
        if m: duration = int(m.group(1))
        else:
            m2 = re.search(r'runtime[^0-9]*(\\d{1,3})', lower)
            if m2: duration = int(m2.group(1))
    except Exception:
        duration = None
    # Metadata extraction via regex on full_text
    metadata = {}
    # other names: 'Also Known As: ...' or 'Also known as'
    m = re.search(r'Also\\s+Known\\s+As[:\\s]*([^\\n\\r]+)', full_text, flags=re.I)
    if m:
        other_raw = m.group(1).strip()
        # split by comma; preserve order and return list
        metadata['otherNames'] = [p.strip() for p in re.split(r',\\s*', other_raw) if p.strip()]
    else:
        metadata['otherNames'] = []
    # release date / aired: look for common labels
    m3 = re.search(r'(Release\\s+Date|Aired|Aired on|Original release)[:\\s]*([^\\n\\r]+)', full_text, flags=re.I)
    if m3:
        raw = m3.group(2).strip()
        # raw could be a range. Try to format.
        rfmt = format_date_range(raw)
        if rfmt:
            metadata['releaseDateRaw'] = raw; metadata['releaseDate'] = rfmt
        else:
            metadata['releaseDateRaw'] = raw; metadata['releaseDate'] = raw
    else:
        # Try simple date search
        m4 = re.search(r'([A-Za-z]+\\s+\\d{1,2},\\s*\\d{4})', full_text)
        if m4:
            metadata['releaseDateRaw'] = m4.group(1).strip()
            metadata['releaseDate'] = format_date_str(metadata['releaseDateRaw'])
        else:
            metadata['releaseDate'] = None
    # Clean synopsis: remove CJK parenthetical duplicates and normalize spacing while preserving paragraphs
    if syn:
        # remove CJK-filled parentheses first
        syn = clean_parenthesis_remove_cjk(syn)
        # normalize each paragraph individually
        paragraphs = [normalize_whitespace_and_sentences(p) for p in syn.split('\\n\\n') if p.strip()]
        syn = '\\n\\n'.join(paragraphs)
    # Add source label
    domain = re.sub(r'^https?://(www\\.)?', '', base_url).split('/')[0] if base_url else ''
    label = 'AsianWiki' if 'asianwiki' in domain else ('MyDramaList' if 'mydramalist' in domain else domain)
    if syn:
        syn_with_src = f"{syn} (Source: {label})"
    else:
        syn_with_src = None
    return syn_with_src, duration, full_text, metadata

def ddgs_text(query):
    if HAVE_DDGS:
        return try_ddgs_text(query, max_results=6)
    return []

def fetch_synopsis_and_duration(show_name, year, prefer_sites=None, existing_synopsis=None, allow_replace=False):
    """Search for synopsis and structured metadata. Uses preferred sites order.
    Returns: synopsis_text, duration_minutes, source_url, orig_len, metadata(dict)
    """
    if existing_synopsis and not SCHEDULED_RUN and not allow_replace:
        # If we already have a synopsis and this run is not scheduled (automatic), skip re-fetch
        return existing_synopsis, None, None, len(existing_synopsis), {}
    queries = []
    if prefer_sites:
        for s in prefer_sites:
            if s == "asianwiki": queries.append(f"{show_name} {year} site:asianwiki.com")
            if s == "mydramalist": queries.append(f"{show_name} {year} site:mydramalist.com")
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
        # fallback: try basic guessed urls later
        for u in urls_to_try:
            if not u: continue
            html = fetch_page(u)
            if not html: continue
            syn, dur, fulltext, metadata = parse_synopsis_from_html(html, u)
            if syn:
                orig_len = len(syn)
                return syn, dur, u, orig_len, metadata
        time.sleep(0.4)
    # No remote synopsis found; return fallback
    return (existing_synopsis or "Synopsis not available."), None, None, len(existing_synopsis or ""), {}

# ---------------------------- Excel -> objects mapping ----------------------
COLUMN_MAP = {
    "no": "showID", "series title": "showName", "started date": "watchStartedOn", "finished date": "watchEndedOn",
    "year": "releasedYear", "total episodes": "totalEpisodes", "original language": "nativeLanguage", "language": "watchedLanguage",
    "ratings": "ratings", "catagory": "genres", "category": "genres", "original network": "network", "comments": "comments"
}

def tidy_comment(val):
    if pd.isna(val) or not str(val).strip(): return None
    text = re.sub(r'\\s+', ' ', str(val)).strip()
    if not text.endswith('.'): text = text + '.'
    text = re.sub(r'\\.([^\\s])', r'. \\1', text)
    return text

def sheet_base_offset(sheet_name: str) -> int:
    if sheet_name == "Sheet1": return 100
    if sheet_name == "Feb 7 2023 Onwards": return 1000
    if sheet_name == "Sheet2": return 3000
    return 0

def excel_to_objects(excel_file, sheet_name, existing_by_id, report_changes, start_index=0, max_items=None, time_limit_seconds=None, deleted_ids_for_run=None, deleting_not_found_initial=None, deleting_found_in_sheets=None):
    """Read rows from a sheet and transform to ordered objects.
    - deleted_ids_for_run: set of showIDs that were deleted at the start of this run and MUST be ignored.
    - deleting_not_found_initial: set of IDs that were present in 'Deleting Records' but not in seriesData.json initially.
    - deleting_found_in_sheets: caller-provided set that we will populate with any IDs (from deleting_not_found_initial)
       that we find inside the sheets. This helps later to compute NOT FOUND list.
    """
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
            # Build object from columns up to 'Again Watched' column
            for col in df.columns[:again_idx]:
                key = COLUMN_MAP.get(col, col); val = row[col]
                if key == "showID":
                    base = sheet_base_offset(sheet_name)
                    obj["showID"] = base + int(val) if pd.notna(val) else None
                elif key == "showName":
                    raw_name = str(val) if pd.notna(val) else ""
                    clean_name = re.sub(r'\\s+', ' ', raw_name).strip().lower()
                    obj["showName"] = clean_name if clean_name else None
                elif key in ("watchStartedOn","watchEndedOn"):
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
                    try: obj[key] = int(val) if pd.notna(val) else 0
                    except: obj[key] = 0
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
                if n in ("korean","korea","korean language"): obj["country"] = "South Korea"
                elif n in ("chinese","china","mandarin"): obj["country"] = "China"
                elif n in ("japanese","japan"): obj["country"] = "Japan"
            dates = [ddmmyyyy(v) for v in row[again_idx:] if ddmmyyyy(v)]
            obj["againWatchedDates"] = dates
            obj["updatedOn"] = now_ist().strftime("%d %B %Y"); obj["updatedDetails"] = "First time Uploaded"
            r = int(obj.get("ratings") or 0); obj["topRatings"] = r * len(dates) * 100
            obj.setdefault("otherNames", [])
            obj["Duration"] = None
            # --- Handle deletion-ignore logic ---
            sid = obj.get("showID")
            if deleted_ids_for_run and sid in deleted_ids_for_run:
                # If this showID was deleted at the START of the run (present in 'Deleting Records' and found in seriesData.json),
                # we must NOT re-create or add it during this same run. Instead, add a report entry and skip creating the object.
                report_changes.setdefault('ignored_deleting', []).append(f'{sid} -> Already Deleted as per "Deleting Records" Sheet -> ⚠️ Cannot add to seriesData.json')
                # Also mark as 'seen' if it was one of the initially-not-found deleting IDs (defensive)
                if deleting_not_found_initial and sid in deleting_not_found_initial:
                    deleting_found_in_sheets.add(sid)
                continue
            # If this row corresponds to an ID that was present in 'Deleting Records' but not in JSON initially,
            # track that we've seen it so it won't be listed in final NOT FOUND. We DO NOT block creation in this case.
            if deleting_not_found_initial and sid in deleting_not_found_initial:
                deleting_found_in_sheets.add(sid)
            # --- Decide preference for synopsis/image fetching per-language ---
            show_name = obj.get("showName"); released_year = obj.get("releasedYear")
            prefer = None
            if native and native.lower().startswith('korean'): prefer = ["asianwiki","mydramalist"]
            elif native and native.lower().startswith('chinese'): prefer = ["mydramalist","asianwiki"]
            else:
                # For other languages, the comments below explain how to add preferences:
                # - For Japanese dramas, you might prefer: prefer = ['asianwiki','mydramalist','wikipedia']
                # - Add language checks above and place the most reliable site first.
                prefer = ["mydramalist","asianwiki"]  # default fallbacks
            # --- Image handling: only attempt to fetch a NEW image when this is a scheduled (automatic) run ---
            existing = existing_by_id.get(obj.get("showID")) if obj.get("showID") is not None else None
            existing_image_url = existing.get("showImage") if existing else None
            new_image_url = existing_image_url or None
            # For scheduled runs, try to fetch an image for new objects (or replace missing images)
            if SCHEDULED_RUN and (existing_image_url is None):
                # We'll attempt to find an image from preferred sites and save locally under images/<showID>.jpg
                try:
                    # Attempt to find an image URL and download it; function defined below in file.
                    local_image_path, remote_image_url = fetch_and_save_image_for_show(show_name or "", prefer, obj.get("showID"))
                    if local_image_path:
                        new_image_url = build_absolute_url(local_image_path)
                        report_changes.setdefault('images', []).append({'showName': show_name, 'old': existing_image_url, 'new': new_image_url})
                except Exception as e:
                    logd(f"Image fetch failed for {show_name}: {e}")
            obj["showImage"] = new_image_url
            # --- Synopsis & metadata fetch ---
            existing_syn = existing.get("synopsis") if existing else None
            new_syn, dur, syn_url, orig_len, metadata = fetch_synopsis_and_duration(show_name or "", released_year or "", prefer_sites=prefer, existing_synopsis=existing_syn, allow_replace=False)
            if new_syn and isinstance(new_syn, str):
                new_syn = normalize_whitespace_and_sentences(new_syn)
            obj["synopsis"] = new_syn
            # Duration preference: metadata > fetched dur > existing
            if metadata and metadata.get('otherNames'):
                obj['otherNames'] = metadata.get('otherNames')
            if metadata and metadata.get('releaseDate'):
                obj['releaseDate'] = metadata.get('releaseDate')
            else:
                obj['releaseDate'] = None
            if dur is not None and dur>0: obj['Duration'] = int(dur)
            elif existing and existing.get('Duration'): obj['Duration'] = existing.get('Duration')
            ordered = {
                "showID":obj.get("showID"),
                "showName":obj.get("showName"),
                "otherNames":obj.get("otherNames",[]),
                "showImage":obj.get("showImage"),
                "watchStartedOn":obj.get("watchStartedOn"),
                "watchEndedOn":obj.get("watchEndedOn"),
                "releasedYear":obj.get("releasedYear"),
                # NEW property: releaseDate (string). Placed right after releasedYear as requested.
                "releaseDate": obj.get("releaseDate"),
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
                report_changes.setdefault("created", []).append(ordered)
            else:
                if existing != ordered:
                    report_changes.setdefault("updated", []).append({"old":existing,"new":ordered})
            if orig_len and orig_len > SYNOPSIS_MAX_LEN:
                report_changes.setdefault('exceed', []).append({"id": sid, "name": ordered.get('showName'), "year": ordered.get('releasedYear'), "site": syn_url or "", "url": syn_url or "", "orig_len": orig_len})
        except Exception as e:
            # Keep errors informative and sheet-specific to make debugging easier.
            raise RuntimeError(f"Row {idx} in sheet '{sheet_name}' processing failed: {e}")
    finished = (last_idx >= total_rows - 1) if total_rows>0 else True
    next_index = (last_idx + 1) if processed>0 else start_index
    return items, processed, finished, next_index

# ---------------------------- Deletion processing ---------------------------
def process_deletions(excel_file, json_file, report_changes):
    """Read the 'Deleting Records' sheet and remove any showIDs present in seriesData.json.
    Returns two lists/sets:
        deleted_ids: list of IDs successfully deleted from seriesData.json
        not_found_initial: list of IDs that were present in the Deleting sheet but NOT found in seriesData.json at start.
    report_changes (dict) will be updated with 'deleted' and 'deleted_not_found' lists, matching user's preferred messages.
    """
    try:
        df = pd.read_excel(excel_file, sheet_name='Deleting Records')
    except Exception:
        return [], []
    if df.shape[1] < 1:
        return [], []
    cols = [str(c).strip().lower() for c in df.columns]
    id_col = None
    for i,c in enumerate(cols):
        if c == 'id' or 'id' in c:
            id_col = df.columns[i]; break
    if id_col is None:
        id_col = df.columns[0]
    # Load existing JSON objects (if present)
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
        if pd.isna(val): continue
        try: to_delete.append(int(val))
        except: continue
    if not to_delete: return [], []
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
                # Add friendly message for report matching user's format
                report_changes.setdefault('deleted', []).append(f"{iid} -> ✅ Deleted and archived -> {outpath}")
            except Exception as e:
                report_changes.setdefault('deleted', []).append(f"{iid} -> ⚠️ Deletion recorded but failed to write archive: {e}")
        else:
            # Not found in existing JSON file: we will check sheets later to see if it exists there.
            not_found_ids.append(iid)
            report_changes.setdefault('deleted_not_found', []).append(f"-{iid} -> ❌ Not found in seriesData.json")
    # Write back updated seriesData.json (with deletions removed)
    merged = sorted(by_id.values(), key=lambda x: x.get('showID', 0))
    try:
        with open(json_file, 'w', encoding='utf-8') as jf:
            json.dump(merged, jf, indent=4, ensure_ascii=False)
        report_changes.setdefault('deleted_summary', []).append(f"seriesData.json updated after deletions (deleted {len(deleted_ids)} items)." )
    except Exception as e:
        report_changes.setdefault('deleted_summary', []).append(f"⚠️ Failed to write updated {json_file}: {e}")
    return deleted_ids, not_found_ids

def cleanup_deleted_data():
    if not os.path.exists(DELETED_DATA_DIR): return
    cutoff = datetime.now() - timedelta(days=30)
    for fname in os.listdir(DELETED_DATA_DIR):
        path = os.path.join(DELETED_DATA_DIR, fname)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff: os.remove(path); print(f"🗑️ Removed expired deleted-data file: {path}")
        except Exception as e:
            print(f"⚠️ Could not cleanup deleted-data {path}: {e}")

# ---------------------------- Manual updates --------------------------------
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
                try: obj["ratings"] = int(v)
                except: obj["ratings"] = obj.get("ratings",0)
            elif k.lower() in ("releasedyear","year"):
                try: obj["releasedYear"] = int(v)
                except: pass
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
    """Search for a show image using preferred sites (order matters). If found, download to images/<show_id>.jpg
    Returns: (local_path_relative, remote_url) or (None, None) on failure.
    NOTES / comments for future languages:
      - For Japanese dramas you may prefer to search 'asianwiki' first or a site like 'jdrama' if present.
      - Make sure to extend 'prefer_sites' list above in excel_to_objects accordingly.
    """
    if not show_name: return None, None
    # Try preferred sites first
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
            if not u: continue
            html = fetch_page(u)
            if not html: continue
            soup = BeautifulSoup(html, 'lxml')
            # prefer og:image
            og = soup.find('meta', property='og:image')
            if og and og.get('content'):
                img_url = og.get('content');
            else:
                # fallback: first <img> inside main/article content
                img_tag = soup.find('img')
                img_url = img_tag.get('src') if img_tag and img_tag.get('src') else None
            if img_url:
                # convert to absolute if needed
                if img_url.startswith('//'): img_url = 'https:' + img_url
                if img_url.startswith('/'):
                    base = re.match(r'^(https?://[^/]+)', u)
                    if base: img_url = base.group(1) + img_url
                local_name = f"{show_id}.jpg" if show_id else safe_filename(show_name) + '.jpg'
                local_path = os.path.join(IMAGES_DIR, local_name)
                ok = download_image_to(img_url, local_path)
                if ok:
                    return local_path, img_url
    # Last resort: use ddgs_images
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
    """Generate a TXT run report. Inserts the 'NOT FOUND' section for deletion entries
    that were never found in any scanned sheet.
    """
    lines = []
    exceed_entries = []
    total_created = total_updated = total_deleted = 0
    for sheet, changes in report_changes_by_sheet.items():
        lines.append(f"=== {sheet} — {now_ist().strftime('%d %B %Y')} ===")
        if 'error' in changes:
            lines.append(f"ERROR processing sheet: {changes['error']}")
        created = changes.get('created', []); total_created += len(created)
        if created:
            lines.append("\\nData Created:")
            for obj in created: lines.append(f"- {obj.get('showName','Unknown')} -> Created")
        updated = changes.get('updated', []); total_updated += len(updated)
        if updated:
            lines.append("\\nData Updated:")
            for pair in updated:
                new = pair.get('new'); old = pair.get('old')
                changed_fields = [f for f in ["showName","showImage","releasedYear","totalEpisodes","comments","ratings","genres","Duration","synopsis"] if old.get(f) != new.get(f)]
                fields_text = ", ".join([f.capitalize() for f in changed_fields]) if changed_fields else "General"
                lines.append(f"- {new.get('showName','Unknown')} -> Updated: {fields_text}")
        images = changes.get('images', [])
        if images:
            lines.append("\\nImage Updated:")
            for itm in images:
                lines.append(f"- {itm.get('showName','Unknown')} -> Old && New")
                lines.append(f"  Old: {itm.get('old')}"); lines.append(f"  New: {itm.get('new')}")
        deleted = changes.get('deleted', []); total_deleted += len(deleted)
        if deleted:
            lines.append("\\nDeleted Records:")
            for iid in deleted: lines.append(f"- {iid}")
        deleted_not_found = changes.get('deleted_not_found', [])
        if deleted_not_found:
            lines.append("\\nDeletion notes (IDs not found in seriesData.json initially):")
            for note in deleted_not_found: lines.append(f"- {note}")
        ignored = changes.get('ignored_deleting', [])
        if ignored:
            lines.append("\\nIgnored (present in 'Deleting Records' and already deleted earlier this run):")
            for note in ignored: lines.append(f"- {note}")
        if changes.get('exceed'):
            exceed_entries.extend(changes.get('exceed'))
        lines.append("\\n")
    lines.insert(0, f"SUMMARY: Created: {total_created}, Updated: {total_updated}, Deleted (initially found): {total_deleted}")
    if exceed_entries:
        lines.append(f"=== Exceed Max Length ({SYNOPSIS_MAX_LEN}) ===")
        for e in exceed_entries:
            lines.append(f"{e.get('id')} -> {e.get('name')} ({e.get('year')}) -> {e.get('site')} -> Link: {e.get('url')}")
        lines.append("\\n")
    # Add global NOT FOUND section for deleting IDs that were NOT found in any scanned sheet
    if final_not_found_deletions:
        lines.append("=== NOT FOUND (Deleting Records not present in any scanned sheet) ===")
        for iid in final_not_found_deletions:
            lines.append(f"-{iid} -> ❌ Cannot be found in any Sheets.")
        lines.append("\\n")
    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("\\n".join(lines))
    except Exception as e:
        print(f"⚠️ Could not write TXT report: {e}")


# ---------------------------- Secret scan & email body ---------------------
def scan_for_possible_secrets():
    """Return a dict summarizing likely secret files present in the repository/workspace.
    We DO NOT print secret contents; we only indicate presence and non-sensitive details
    (client_email, file existence, whether a 'private_key' field exists, file size).
    The workflow / operator should rotate keys if they are committed accidentally.
    """
    findings = []
    # Check service account JSON
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        try:
            s = open(SERVICE_ACCOUNT_FILE, 'r', encoding='utf-8').read()
            has_private_key = 'private_key' in s
            m = re.search(r'"client_email"\\s*:\\s*"([^"]+)"', s)
            client_email = m.group(1) if m else None
            findings.append({'file': SERVICE_ACCOUNT_FILE, 'present': True, 'client_email': client_email, 'has_private_key': bool(has_private_key), 'note': 'Service account JSON detected'})
        except Exception as e:
            findings.append({'file': SERVICE_ACCOUNT_FILE, 'present': True, 'note': f'Could not read file safely: {e}'})
    # Check EXCEL_FILE_ID
    if os.path.exists(EXCEL_FILE_ID_TXT):
        try:
            s = open(EXCEL_FILE_ID_TXT, 'r', encoding='utf-8').read().strip()
            findings.append({'file': EXCEL_FILE_ID_TXT, 'present': True, 'length': len(s), 'note': 'Excel file id present (not shown)'})
        except Exception as e:
            findings.append({'file': EXCEL_FILE_ID_TXT, 'present': True, 'note': f'Could not read file: {e}'})
    # Check other suspicious filenames
    for fname in os.listdir('.'):
        lower = fname.lower()
        if any(k in lower for k in ('.env','secret','credential','key','.pem','.p12')):
            findings.append({'file': fname, 'present': True, 'note': 'Suspicious filename - check for secrets'})
    return findings

def compose_email_body_from_report(report_path):
    """Read the report file and create a full email body string containing:
       - human-readable report pasted inline
       - detailed secrets-check summary (no secret plaintext)
       - short remediation steps if secrets appear to be present
    """
    body_lines = []
    body_lines.append(f"Run Report — {now_ist().strftime('%d %B %Y %H:%M')}")
    body_lines.append("\\n--- REPORT CONTENT (pasted below) ---\\n")
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            body_lines.append(f.read())
    except Exception as e:
        body_lines.append(f"⚠️ Could not read report file for email body: {e}")
    body_lines.append("\\n--- SECRETS CHECK ---\\n")
    findings = scan_for_possible_secrets()
    if not findings:
        body_lines.append("No obvious secret files detected in the workspace.")
    else:
        for f in findings:
            line = f"File: {f.get('file')} — note: {f.get('note')}."
            if f.get('client_email'):
                line += f" client_email: {f.get('client_email')}."
            if f.get('has_private_key'):
                line += " Contains a private_key field (DO NOT share private key material)."
            if f.get('length') is not None:
                line += f" length: {f.get('length')} characters (value not shown)."
            body_lines.append(line)
        body_lines.append("\\nIf any of the above files were accidentally committed to your repository, immediately: (1) rotate/disable keys, (2) remove the files from the repo (git filter-repo / bfg), (3) re-issue new credentials.")
    # Return a single string suitable for email body (plain text)
    return "\\n".join(body_lines)

# ---------------------------- Main updater ---------------------------------
def update_json_from_excel(excel_file, json_file, sheet_names, max_per_run=0, max_run_time_minutes=0):
    processed_total = 0
    # Load existing JSON (if any)
    if os.path.exists(json_file):
        try:
            with open(json_file, 'r', encoding='utf-8') as f: old_objects = json.load(f)
        except Exception:
            print(f"⚠️ {json_file} invalid. Starting fresh."); old_objects = []
    else:
        old_objects = []
    old_by_id = {o['showID']: o for o in old_objects if 'showID' in o}
    merged_by_id = dict(old_by_id)
    report_changes_by_sheet = {}
    # 1) Process deletions first and receive lists back.
    try:
        deleted_ids, deleting_not_found_initial = process_deletions(excel_file, json_file, report_changes_by_sheet.setdefault('Deleting Records', {}))
    except Exception as e:
        report_changes_by_sheet.setdefault('Deleting Records', {})['error'] = str(e)
        deleted_ids, deleting_not_found_initial = [], []
    # After deletions, reload the JSON file to get the 'current' state for scanning sheets.
    if os.path.exists(json_file):
        try:
            with open(json_file,'r',encoding='utf-8') as f: old_objects = json.load(f)
        except: old_objects = []
    old_by_id = {o['showID']: o for o in old_objects if 'showID' in o}
    merged_by_id = dict(old_by_id)
    # Track which deleting_not_found_initial are found inside sheets
    deleting_found_in_sheets = set()
    progress = load_progress()
    overall_continued = False
    time_limit_seconds = max_run_time_minutes*60 if max_run_time_minutes>0 else None
    any_sheet_processed = False
    for s in sheet_names:
        report_changes = {}
        start_idx = int(progress.get(s,0) or 0)
        try:
            items, processed, finished, next_start_idx = excel_to_objects(excel_file, s, merged_by_id, report_changes, start_index=start_idx, max_items=(max_per_run if max_per_run>0 else None), time_limit_seconds=time_limit_seconds, deleted_ids_for_run=set(deleted_ids), deleting_not_found_initial=set(deleting_not_found_initial), deleting_found_in_sheets=deleting_found_in_sheets)
        except Exception as e:
            err = str(e)
            print(f"⚠️ Error processing {s}: {err}")
            report_changes['error'] = err
            items, processed, finished, next_start_idx = [],0,True,start_idx
        for new_obj in items:
            sid = new_obj.get('showID')
            if sid in merged_by_id:
                old_obj = merged_by_id[sid]
                if old_obj != new_obj:
                    new_obj['updatedOn'] = now_ist().strftime('%d %B %Y'); new_obj['updatedDetails'] = 'Object updated'; merged_by_id[sid] = new_obj
            else:
                merged_by_id[sid] = new_obj
        if items:
            os.makedirs(BACKUP_DIR, exist_ok=True)
            backup_name = os.path.join(BACKUP_DIR, f"{filename_timestamp()}_{safe_filename(s)}.json")
            try:
                with open(backup_name, 'w', encoding='utf-8') as bf:
                    json.dump(items, bf, indent=4, ensure_ascii=False)
                print(f"✅ Backup saved → {backup_name}")
            except Exception as e:
                print(f"⚠️ Could not write backup {backup_name}: {e}")
        report_changes_by_sheet[s] = report_changes
        if processed>0:
            any_sheet_processed = True
            processed_total += processed
        if not finished:
            progress[s] = next_start_idx; overall_continued = True
        else:
            if s in progress: progress.pop(s,None)
        save_progress(progress)
    # After scanning all sheets, any deletion IDs that were not found initially AND NOT found inside scanned sheets are 'NOT FOUND'
    still_not_found = set(deleting_not_found_initial or []) - deleting_found_in_sheets
    # Write merged seriesData.json back
    merged = sorted(merged_by_id.values(), key=lambda x: x.get('showID',0))
    try:
        with open(json_file,'w',encoding='utf-8') as f: json.dump(merged,f,indent=4,ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Could not write final {json_file}: {e}")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(REPORTS_DIR, f"report_{filename_timestamp()}.txt")
    write_report(report_changes_by_sheet, report_path, final_not_found_deletions=sorted(list(still_not_found)))
    print(f"✅ Report written → {report_path}")
    # Compose an email body file (plain text) that pastes the report content and includes secret checks
    email_body = compose_email_body_from_report(report_path)
    email_path = os.path.join(REPORTS_DIR, f"email_body_{filename_timestamp()}.txt")
    try:
        with open(email_path, 'w', encoding='utf-8') as ef:
            ef.write(email_body)
        print(f"✅ Email body written → {email_path}")
    except Exception as e:
        print(f"⚠️ Could not write email body file: {e}")
    # Housekeeping
    if SCHEDULED_RUN: cleanup_deleted_data()
    cutoff = datetime.now() - timedelta(days=KEEP_OLD_IMAGES_DAYS)
    if os.path.exists(OLD_IMAGES_DIR):
        for fname in os.listdir(OLD_IMAGES_DIR):
            path = os.path.join(OLD_IMAGES_DIR, fname)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(path))
                if mtime < cutoff: os.remove(path)
            except Exception as e:
                print(f"⚠️ Could not cleanup old image {path}: {e}")
    status = {"continued": overall_continued, "timestamp": now_ist().strftime('%d %B %Y_%H.%M'), "processed_total": processed_total}
    try:
        with open(STATUS_JSON, 'w', encoding='utf-8') as sf: json.dump(status, sf, indent=2)
    except Exception as e:
        print(f"⚠️ Could not write status json: {e}")
    if processed_total == 0:
        print("❌ No records were processed in this run. Please check: (1) local-data.xlsx exists, (2) sheet names match, (3) there were no row errors. Failing with non-zero exit code.")
        with open(os.path.join(REPORTS_DIR, "failure_reason.txt"), "w", encoding="utf-8") as ff:
            ff.write("No records processed. Check logs and the report.")
        sys.exit(2)
    return

# ---------------------------- Entrypoint -----------------------------------
if __name__ == '__main__':
<<<<<<< HEAD
    # Environment: script expects the CI workflow to write two files from secrets:
    #  - EXCEL_FILE_ID.txt and GDRIVE_SERVICE_ACCOUNT.json present at runtime.
    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)):
        print("❌ Missing GDrive credentials. Please set EXCEL_FILE_ID.txt and GDRIVE_SERVICE_ACCOUNT.json via GitHub secrets.")
        sys.exit(3)
    # Read Excel file id (used by workflow to download -- not used here directly)
=======
    # Always use Google Drive — do not accept local fallback.
    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)):
        print("❌ Missing GDrive credentials. Please set EXCEL_FILE_ID.txt and GDRIVE_SERVICE_ACCOUNT.json via GitHub secrets.")
        sys.exit(3)

    # Read Excel file ID
>>>>>>> 458bc47 (Files were updated for local-data.xlsx' not found error)
    try:
        with open(EXCEL_FILE_ID_TXT, 'r', encoding='utf-8') as f:
            excel_id = f.read().strip()
    except Exception:
        excel_id = None
<<<<<<< HEAD
    if not excel_id:
        print("❌ EXCEL_FILE_ID.txt is empty or missing. Aborting.")
        sys.exit(3)
    # The workflow is responsible for downloading the Excel to LOCAL_EXCEL_FILE before calling this script.
=======

    if not excel_id:
        print("❌ EXCEL_FILE_ID.txt is empty or missing. Aborting.")
        sys.exit(3)

    # Download from Google Drive to LOCAL_EXCEL_FILE
    try:
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/drive.readonly"])
        service = build("drive", "v3", credentials=creds)
        request = service.files().get_media(fileId=excel_id)
        os.makedirs(os.path.dirname(LOCAL_EXCEL_FILE) or '.', exist_ok=True)
        fh = io.FileIO(LOCAL_EXCEL_FILE, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"⬇️ Downloading Excel: {int(status.progress() * 100)}%")
        fh.close()
        print(f"✅ Download complete → {LOCAL_EXCEL_FILE}")
    except Exception as e:
        print(f"❌ Failed to download Excel from Google Drive: {e}")
        sys.exit(3)

>>>>>>> 458bc47 (Files were updated for local-data.xlsx' not found error)
    # Determine SHEETS from env
    _sheets_env = os.environ.get("SHEETS", "").strip()
    if _sheets_env:
        SHEETS = [s.strip() for s in _sheets_env.split(";") if s.strip()]
    else:
        SHEETS = ["Sheet1"]
<<<<<<< HEAD
=======

>>>>>>> 458bc47 (Files were updated for local-data.xlsx' not found error)
    # Apply manual updates first (if present)
    try:
        apply_manual_updates(LOCAL_EXCEL_FILE, JSON_FILE)
    except Exception as e:
        logd(f"apply_manual_updates error: {e}")

    # Run update
    update_json_from_excel(LOCAL_EXCEL_FILE, JSON_FILE, SHEETS, max_per_run=MAX_PER_RUN, max_run_time_minutes=MAX_RUN_TIME_MINUTES)
    print("All done.")
