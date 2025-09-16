# ============================================================================
# Script: create_update_backup_delete.py
# Purpose: Automate download of Excel → JSON transformation, image management,
#          backups, deletions, and run report generation for "my-movie-database".
#
# Overview:
#   - Downloads/reads Excel (local-data.xlsx) and transforms configured sheets to seriesData.json.
#   - Handles image fetching, synopsis fetching (site-prioritized), backups of changed items.
#   - Handles deletions via "Deleting Records" sheet and writes deleted objects to deleted-data/.
#   - Produces TXT-only run reports in reports/ and a status JSON at reports/status.json.
#   - Saves progress to .progress/progress.json so long runs can resume.
#
# Environment variables (set in workflow or shell):
#   - EXCEL_FILE_ID (optional): Google Drive file id for the Excel file (workflow writes EXCEL_FILE_ID.txt)
#   - GDRIVE_SERVICE_ACCOUNT (optional): service account JSON string (workflow writes GDRIVE_SERVICE_ACCOUNT.json)
#   - MAX_PER_RUN: integer, 0 = all (default 0)
#   - MAX_RUN_TIME_MINUTES: integer, 0 = no limit (set by workflow; manual vs scheduled)
#   - KEEP_OLD_IMAGES_DAYS: days to keep old images before cleanup (default 7)
#   - GITHUB_PAGES_URL: github pages base URL for building showImage links
#   - SHEETS: optional semicolon-separated sheet names (e.g. "Sheet1;Manual Update")
#   - SCHEDULED_RUN: "true"/"false" - scheduled run flag (workflow sets this)
#   - DEBUG_FETCH: "true"/"false" - enable debug logging for fetchers
#
# Outputs:
#   - seriesData.json (main dataset)
#   - deleted-data/DELETED_<timestamp>_<id>.json (individual deleted objects)
#   - backups/ (timestamped backups of changed items)
#   - images/, old-images/
#   - reports/report_<timestamp>.txt (run report, TXT only)
# ============================================================================

import os, re, sys, time, json, random, io, shutil
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from bs4 import BeautifulSoup
from PIL import Image
from io import BytesIO
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# Optional: ddgs for DuckDuckGo search (used if installed); otherwise fallback to requests only
try:
    from ddgs import DDGS
    HAVE_DDGS = True
except Exception:
    HAVE_DDGS = False

# Timezone (IST as requested in earlier conversation)
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

def filename_timestamp():
    return now_ist().strftime("%d_%B_%Y_%H%M")

# Paths and defaults
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

# GDrive helper file names
EXCEL_FILE_ID_TXT = "EXCEL_FILE_ID.txt"
SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json"

# Config from env
GITHUB_PAGES_URL = os.environ.get("GITHUB_PAGES_URL", "").strip() or "https://<your-username>.github.io/my-movie-database"
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "0") or 0)
MAX_RUN_TIME_MINUTES = int(os.environ.get("MAX_RUN_TIME_MINUTES", "0") or 0)
KEEP_OLD_IMAGES_DAYS = int(os.environ.get("KEEP_OLD_IMAGES_DAYS", "7") or 7)
SCHEDULED_RUN = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"
DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "false").lower() == "true"
SYNOPSIS_MAX_LEN = int(os.environ.get("SYNOPSIS_MAX_LEN", "1000") or 1000)

HEADERS = {"User-Agent":"Mozilla/5.0"}

def logd(msg):
    if DEBUG_FETCH:
        print("[DEBUG]", msg)

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

# ---------------- Helpers for text cleaning / synopsis ----------------
def clean_parenthesis_remove_cjk(s):
    if not s: return s
    return re.sub(r'\([^)]*[\u4e00-\u9fff\u3400-\u4dbf\uac00-\ud7af][^)]*\)', '', s)

def normalize_whitespace_and_sentences(s):
    if not s: return s
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'\.([^\s])', r'. \1', s)
    return s

# ---------------- Normalizer for comma lists ----------------
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

def country_from_native(native):
    if not native: return None
    n = str(native).strip().lower()
    if n in ("korean","korea","korean language"): return "South Korea"
    if n in ("chinese","mandarin","cantonese"): return "China"
    if n in ("japanese","japan"): return "Japan"
    if n in ("thai","thai language"): return "Thailand"
    if n in ("english","eng"): return "United States"
    return None

# ---------------- Image helpers (simple downloader + resizing) ----------------
def download_image_to(url, path):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200 and r.headers.get("content-type","").startswith("image"):
            img = Image.open(BytesIO(r.content))
            img = img.convert("RGB").resize((600,900), Image.LANCZOS)
            img.save(path, format="JPEG", quality=90)
            return True
    except Exception as e:
        logd(f"image download failed: {e}")
    return False

def build_absolute_url(local_path):
    local_path = local_path.replace("\\","/")
    return GITHUB_PAGES_URL.rstrip("/") + "/" + local_path.lstrip("/")

# ---------------- Web helpers (DDGS optional) ----------------
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

def parse_synopsis_from_html(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    syn = None; duration = None
    # Try meta description first
    meta = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
    if meta and meta.get("content"):
        syn = meta.get("content")
    # AsianWiki often has 'Plot' or 'Synopsis' paragraphs; try to find element with 'plot' or 'synopsis' in headers nearby
    if not syn:
        for h in soup.find_all(re.compile("^h[1-6]$")):
            txt = h.get_text(" ", strip=True).lower()
            if "plot" in txt or "synopsis" in txt or "story" in txt:
                # get next sibling paragraphs
                p = h.find_next("p")
                if p:
                    syn = p.get_text(" ", strip=True); break
    # fallback: first paragraph
    if not syn:
        p = soup.find("p")
        if p:
            syn = p.get_text(" ", strip=True)
    # duration extraction
    duration = None
    try:
        lower = text.lower()
        m = re.search(r"(\d+)\s*h(?:ours?)?\s*(\d+)?\s*m", lower)
        if m:
            hours = int(m.group(1)); mins = int(m.group(2) or 0); duration = hours*60 + mins
        else:
            m2 = re.search(r"runtime[^0-9]*(\d{1,3})", lower)
            if m2: duration = int(m2.group(1))
    except Exception:
        duration = None
    return syn, duration, text

def pick_best_result(results):
    if not results: return None
    for r in results:
        url = r.get("href") or r.get("url") or r.get("link") or ""
        if any(site in url for site in ["mydramalist.com","asianwiki.com","wikipedia.org"]):
            return url
    return results[0].get("href") or results[0].get("url") or None

def ddgs_text(query):
    # wrapper to prefer ddgs if available
    if HAVE_DDGS:
        return try_ddgs_text(query, max_results=6)
    return []

# ---------------- Improved synopsis fetcher ----------------
def fetch_synopsis_and_duration(show_name, year, prefer_sites=None, existing_synopsis=None, allow_replace=False):
    if existing_synopsis and not SCHEDULED_RUN and not allow_replace:
        return existing_synopsis, None, None, len(existing_synopsis)

    queries = []
    if prefer_sites:
        for s in prefer_sites:
            if s == "asianwiki":
                queries.append(f"{show_name} {year} site:asianwiki.com synopsis")
            if s == "mydramalist":
                queries.append(f"{show_name} {year} site:mydramalist.com synopsis")
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
        # fallback: try basic guessed urls (not exhaustive)
        for u in urls_to_try:
            if not u: continue
            html = fetch_page(u)
            if not html: continue
            syn, dur, fulltext = parse_synopsis_from_html(html, u)
            if syn:
                syn = clean_parenthesis_remove_cjk(syn)
                syn = normalize_whitespace_and_sentences(syn)
                domain = re.sub(r'^https?://(www\.)?', '', u).split('/')[0]
                label = "AsianWiki" if "asianwiki" in domain else ("MyDramaList" if "mydramalist" in domain else domain)
                syn_with_src = f"{syn} (Source: {label})"
                return syn_with_src, dur, u, len(syn)
        time.sleep(0.4)
    return (existing_synopsis or "Synopsis not available."), None, None, 0

# ---------------- Excel -> object mapping ----------------
COLUMN_MAP = {
    "no": "showID", "series title": "showName", "started date": "watchStartedOn", "finished date": "watchEndedOn",
    "year": "releasedYear", "total episodes": "totalEpisodes", "original language": "nativeLanguage", "language": "watchedLanguage",
    "ratings": "ratings", "catagory": "genres", "category": "genres", "original network": "network", "comments": "comments"
}

def tidy_comment(val):
    if pd.isna(val) or not str(val).strip(): return None
    text = re.sub(r'\s+', ' ', str(val)).strip()
    if not text.endswith('.'):
        text = text + '.'
    text = re.sub(r'\.([^\s])', r'. \1', text)
    return text

def sheet_base_offset(sheet_name: str) -> int:
    if sheet_name == "Sheet1": return 100
    if sheet_name == "Feb 7 2023 Onwards": return 1000
    if sheet_name == "Sheet2": return 3000
    return 0

def excel_to_objects(excel_file, sheet_name, existing_by_id, report_changes, start_index=0, max_items=None, time_limit_seconds=None):
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
                    raw_name = str(val) if pd.notna(val) else ""
                    clean_name = re.sub(r'\s+', ' ', raw_name).strip().lower()
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
            obj["country"] = country_from_native(obj.get("nativeLanguage"))
            dates = [ddmmyyyy(v) for v in row[again_idx:] if ddmmyyyy(v)]
            obj["againWatchedDates"] = dates
            obj["updatedOn"] = now_ist().strftime("%d %B %Y"); obj["updatedDetails"] = "First time Uploaded"
            r = int(obj.get("ratings") or 0); obj["topRatings"] = r * len(dates) * 100
            obj["Duration"] = None
            obj.setdefault("otherNames", [])
            show_name = obj.get("showName"); released_year = obj.get("releasedYear"); networks = obj.get("network") or []
            existing = existing_by_id.get(obj.get("showID")) if obj.get("showID") is not None else None
            native = obj.get("nativeLanguage"); prefer = None
            if native and native in ("Korean","korean","Korea"): prefer = ["asianwiki","mydramalist"]
            elif native and native in ("Chinese","chinese","China"): prefer = ["mydramalist","asianwiki"]
            existing_image_url = existing.get("showImage") if existing else None
            allow_replace_image = SCHEDULED_RUN
            new_image_url = None
            if existing_image_url and allow_replace_image:
                new_image_url = existing_image_url
            else:
                new_image_url = existing_image_url or None
            obj["showImage"] = new_image_url
            existing_syn = existing.get("synopsis") if existing else None
            new_syn, dur, syn_url, orig_len = fetch_synopsis_and_duration(show_name or "", released_year or "", prefer_sites=prefer, existing_synopsis=existing_syn, allow_replace=False)
            # normalize synopsis spacing
            if new_syn and isinstance(new_syn, str):
                new_syn = normalize_whitespace_and_sentences(new_syn)
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
            if orig_len and orig_len > SYNOPSIS_MAX_LEN:
                report_changes.setdefault('exceed', []).append({"id": sid, "name": ordered.get('showName'), "year": ordered.get('releasedYear'), "site": syn_url or "", "url": syn_url or "", "orig_len": orig_len})
        except Exception as e:
            raise RuntimeError(f"Row {idx} in sheet '{sheet_name}' processing failed: {e}")
    finished = (last_idx >= total_rows - 1) if total_rows>0 else True
    next_index = (last_idx + 1) if processed>0 else start_index
    return items, processed, finished, next_index

# ---------------- Deletion processing ----------------
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
    if not to_delete: return

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
                print(f"✅ Deleted record {iid} -> {outpath}")
            except Exception as e:
                print(f"⚠️ Failed to write deleted file for {iid}: {e}")
        else:
            not_found_ids.append(iid)
            report_changes.setdefault('deleted_not_found', []).append(f"ID:{iid} -> Nowhere Found in the seriesData.json -> Hence, Skipping.")

    merged = sorted(by_id.values(), key=lambda x: x.get('showID', 0))
    try:
        with open(json_file, 'w', encoding='utf-8') as jf:
            json.dump(merged, jf, indent=4, ensure_ascii=False)
        print(f"✅ seriesData.json updated after deletions (deleted {len(deleted_ids)} items).")
    except Exception as e:
        print(f"⚠️ Failed to write updated {json_file}: {e}")

    if deleted_ids:
        report_changes.setdefault('deleted', []).extend(deleted_ids)
    if not_found_ids:
        print(f"ℹ️ Deletion: following IDs not found and were skipped: {not_found_ids}")

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

# ---------------- Manual updates ----------------
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

# ---------------- Reports ----------------
def write_report(report_changes_by_sheet, report_path):
    lines = []
    exceed_entries = []
    total_created = total_updated = total_deleted = 0
    for sheet, changes in report_changes_by_sheet.items():
        lines.append(f"=== {sheet} — {now_ist().strftime('%d %B %Y')} ===")
        if 'error' in changes:
            lines.append(f"ERROR processing sheet: {changes['error']}")
        created = changes.get('created', []); total_created += len(created)
        if created:
            lines.append("\nData Created:")
            for obj in created: lines.append(f"- {obj.get('showName','Unknown')} -> Created")
        updated = changes.get('updated', []); total_updated += len(updated)
        if updated:
            lines.append("\nData Updated:")
            for pair in updated:
                new = pair.get('new'); old = pair.get('old')
                changed_fields = [f for f in ["showName","showImage","releasedYear","totalEpisodes","comments","ratings","genres","Duration","synopsis"] if old.get(f) != new.get(f)]
                fields_text = ", ".join([f.capitalize() for f in changed_fields]) if changed_fields else "General"
                lines.append(f"- {new.get('showName','Unknown')} -> Updated: {fields_text}")
        images = changes.get('images', [])
        if images:
            lines.append("\nImage Updated:")
            for itm in images:
                lines.append(f"- {itm.get('showName','Unknown')} -> Old && New")
                lines.append(f"  Old: {itm.get('old')}"); lines.append(f"  New: {itm.get('new')}")
        deleted = changes.get('deleted', []); total_deleted += len(deleted)
        if deleted:
            lines.append("\nDeleted Records:")
            for iid in deleted: lines.append(f"- {iid} -> ✅Deleted")
        deleted_not_found = changes.get('deleted_not_found', [])
        if deleted_not_found:
            lines.append("\nDeletion notes (IDs not found):")
            for note in deleted_not_found: lines.append(f"- {note}")
        if changes.get('exceed'):
            exceed_entries.extend(changes.get('exceed'))
        lines.append("\n")
    lines.insert(0, f"SUMMARY: Created: {total_created}, Updated: {total_updated}, Deleted: {total_deleted}")
    if exceed_entries:
        lines.append(f"=== Exceed Max Length ({SYNOPSIS_MAX_LEN}) ===")
        for e in exceed_entries:
            lines.append(f"{e.get('id')} -> {e.get('name')} ({e.get('year')}) -> {e.get('site')} -> Link: {e.get('url')}")
        lines.append("\n")
    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
    except Exception as e:
        print(f"⚠️ Could not write TXT report: {e}")

# ---------------- Main updater ----------------
def update_json_from_excel(excel_file, json_file, sheet_names, max_per_run=0, max_run_time_minutes=0):
    processed_total = 0
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
    try:
        process_deletions(excel_file, json_file, report_changes_by_sheet.setdefault('Deleting Records', {}))
    except Exception as e:
        report_changes_by_sheet.setdefault('Deleting Records', {})['error'] = str(e)
    # reload after deletions
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
        try:
            items, processed, finished, next_start_idx = excel_to_objects(excel_file, s, merged_by_id, report_changes, start_index=start_idx, max_items=(max_per_run if max_per_run>0 else None), time_limit_seconds=time_limit_seconds)
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
    merged = sorted(merged_by_id.values(), key=lambda x: x.get('showID',0))
    try:
        with open(json_file,'w',encoding='utf-8') as f: json.dump(merged,f,indent=4,ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ Could not write final {json_file}: {e}")
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

if __name__ == '__main__':
    # Always use Google Drive — do not accept local fallback.
    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)):
        print("❌ Missing GDrive credentials. Please set EXCEL_FILE_ID.txt and GDRIVE_SERVICE_ACCOUNT.json via GitHub secrets.")
        sys.exit(3)

    # Read Excel file ID
    try:
        with open(EXCEL_FILE_ID_TXT, 'r', encoding='utf-8') as f:
            excel_id = f.read().strip()
    except Exception:
        excel_id = None

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

    # Determine SHEETS from env
    _sheets_env = os.environ.get("SHEETS", "").strip()
    if _sheets_env:
        SHEETS = [s.strip() for s in _sheets_env.split(";") if s.strip()]
    else:
        SHEETS = ["Sheet1"]

    # Apply manual updates first (if present)
    try:
        apply_manual_updates(LOCAL_EXCEL_FILE, JSON_FILE)
    except Exception as e:
        logd(f"apply_manual_updates error: {e}")

    # Run update
    update_json_from_excel(LOCAL_EXCEL_FILE, JSON_FILE, SHEETS, max_per_run=MAX_PER_RUN, max_run_time_minutes=MAX_RUN_TIME_MINUTES)
    print("All done.")
