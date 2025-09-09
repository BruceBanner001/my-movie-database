# ============================================================
# File: create_update_backup_delete.py
# Repo: my-movie-database
# Author: BruceBanner001
#
# What it does
# - Downloads your private Excel from Google Drive (Service Account)
# - Converts each sheet into JSON objects per your mapping rules
# - Downloads/resizes cover images (600x900) and stores them in /images
# - Builds absolute GitHub Pages URLs for images
# - Scrapes high-quality synopsis + duration (mins)
# - Tracks updatedOn (IST) + updatedDetails (<= 30 chars)
# - Backs up changed/deleted objects to backups/DDMMYYYY_HHMM.json
# - Keeps order of JSON fields exactly as requested
# ============================================================

import os
import io
import re
import json
import time
import math
import random
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup
from ddgs import DDGS  # pip install ddgs

# === Google Drive (private) ===
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# -----------------------------
# Config — change as needed
# -----------------------------
# Google Drive
EXCEL_FILE_ID = "YOUR_GOOGLE_DRIVE_FILE_ID"      # <-- REPLACE
SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json"  # <-- REPLACE

# Local temp Excel
LOCAL_EXCEL_FILE = "local-data.xlsx"

# Target JSON
JSON_FILE = "seriesData.json"

# Backups and images
BACKUP_DIR = "backups"
IMAGES_DIR = "images"

# GitHub Pages absolute base (so your app can load images)
GITHUB_PAGES_URL = "https://brucebanner001.github.io/my-movie-database/"

# Fixed cover size (uniform, high quality)
COVER_WIDTH, COVER_HEIGHT = 600, 900
FORCE_REFRESH_IMAGES = False  # set True to re-download images always

# Sheets to process (you can edit this list any time)
SHEETS = ["Sheet1", "Sheet2", "Mini Drama"]  # include any that exist


# ============================================================
# Google Drive helper
# ============================================================
def download_from_gdrive(file_id: str, destination: str, service_account_file: str):
    """Download private Excel file from Google Drive using a Service Account."""
    creds = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    service = build("drive", "v3", credentials=creds)
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(destination, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"⬇️ Downloading Excel: {int(status.progress() * 100)}%")
    print(f"✅ Download complete → {destination}")


# ============================================================
# Utilities
# ============================================================
IST = timezone(timedelta(hours=5, minutes=30))  # Asia/Kolkata

def today_ist_long():
    return datetime.now(IST).strftime("%d %B %Y")  # dd MONTH YYYY

def timestamp_filename():
    return datetime.now(IST).strftime("%d%m%Y_%H%M")

def ddmmyyyy(val):
    """Convert a date-like value to DD-MM-YYYY string (or None)."""
    if pd.isna(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.strftime("%d-%m-%Y")
    # try parse common string
    s = str(val).strip()
    # already dd-mm-yyyy?
    if re.match(r"^\d{2}-\d{2}-\d{4}$", s):
        return s
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return None

def safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", (name or "").strip())

def cap_first(s: str) -> str:
    return s[:1].upper() + s[1:] if s else s

def words_capitalize(s: str) -> str:
    return " ".join(w.capitalize() for w in (s or "").split())

def normalize_list_from_csv(val, cap=True, strip=True):
    """Split comma-separated string into list, normalize capitalization."""
    if pd.isna(val) or not str(val).strip():
        return []
    parts = [p.strip() for p in str(val).split(",")]
    if strip:
        parts = [p for p in parts if p]
    if cap:
        parts = [p[:1].upper() + p[1:] if p else p for p in parts]
    return parts

def country_from_native(lang: str):
    if not lang:
        return None
    lang = lang.strip().lower()
    if lang == "korean":
        return "South Korea"
    if lang == "chinese":
        return "China"
    return None


# ============================================================
# Image Search / Download
# ============================================================
HEADERS = {"User-Agent": "Mozilla/5.0"}

def try_ddgs_images(query, max_results=6):
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(query, max_results=max_results))
            return [r.get("image") for r in results if r.get("image")]
    except Exception as e:
        print(f"⚠️ DDGS image search error: {e}")
        return []

def try_bing_images(query, count=6):
    url = f"https://www.bing.com/images/search?q={requests.utils.quote(query)}&form=HDRSC2"
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        return [p.split('"')[0] for p in r.text.split('"murl":"')[1:count+1]]
    except Exception as e:
        print(f"⚠️ Bing image search error: {e}")
        return []

def try_google_images(query, count=6):
    url = f"https://www.google.com/search?tbm=isch&q={requests.utils.quote(query)}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        return [p.split('"')[1] for p in r.text.split('"ou"')[1:count+1]]
    except Exception as e:
        print(f"⚠️ Google image search error: {e}")
        return []

def resize_and_save_jpg(content: bytes, path: str) -> bool:
    try:
        img = Image.open(BytesIO(content))
        img = img.convert("RGB").resize((COVER_WIDTH, COVER_HEIGHT), Image.LANCZOS)
        img.save(path, format="JPEG", quality=95)
        return True
    except Exception as e:
        print(f"⚠️ Image resize/save failed for {path}: {e}")
        return False

def download_image_to(content_url: str, path: str) -> bool:
    try:
        resp = requests.get(content_url, headers=HEADERS, timeout=12)
        if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("image"):
            return resize_and_save_jpg(resp.content, path)
    except Exception as e:
        print(f"⚠️ Image download failed: {e}")
    return False

def build_absolute_url(local_path: str) -> str:
    # local_path like "images/Name_2021.jpg" → absolute GH Pages URL
    local_path = local_path.replace("\\", "/")
    return GITHUB_PAGES_URL.rstrip("/") + "/" + local_path.lstrip("/")

def search_cover_image(show_name: str, year, networks: list):
    """Try multiple queries and sources, including platform hints and drama sites."""
    core = [
        f"{show_name} {year} drama cover",
        f"{show_name} {year} drama poster",
        f"{show_name} {year} tv series poster",
        f"{show_name} {year} official poster",
        f"{show_name} drama poster",
        f"{show_name} {year} mydramalist poster",
        f"{show_name} {year} asianwiki poster",
        f"{show_name} {year} netflix poster",
        f"{show_name} {year} viki poster",
        f"{show_name} {year} prime video poster",
    ]
    net_queries = [f"{show_name} {year} {net} poster" for net in (networks or [])]
    return core + net_queries

def download_cover_image(show_name: str, year, networks=None) -> str | None:
    if not show_name or not year:
        return None
    os.makedirs(IMAGES_DIR, exist_ok=True)
    filename = f"{safe_filename(show_name)}_{year}.jpg"
    local_path = os.path.join(IMAGES_DIR, filename)

    if os.path.exists(local_path) and not FORCE_REFRESH_IMAGES:
        return build_absolute_url(local_path)

    queries = search_cover_image(show_name, year, networks)

    for q in queries:
        print(f"🔍 Searching image: {q}")
        urls = try_ddgs_images(q) or try_bing_images(q) or try_google_images(q)
        for url in urls:
            if download_image_to(url, local_path):
                print(f"✅ Image saved → {local_path}")
                return build_absolute_url(local_path)
        time.sleep(random.uniform(1.2, 2.5))  # avoid throttling

    print(f"❌ Could not find image for {show_name} ({year})")
    return None


# ============================================================
# Synopsis + Duration (mins) scraping
# ============================================================
ALLOWED_SYNOP_SITES = [
    "mydramalist.com",
    "asianwiki.com",
    "wikipedia.org",
    "netflix.com",
    "viki.com",
    "primevideo.com",
    "imdb.com",
]

def ddgs_text(query, max_results=8):
    try:
        with DDGS() as dd:
            return list(dd.text(query, max_results=max_results))
    except Exception as e:
        print(f"⚠️ DDGS text error: {e}")
        return []

def pick_best_result(results):
    """Prefer whitelisted domains and reasonable titles/snippets."""
    for r in results:
        url = r.get("href") or r.get("url") or ""
        if any(site in url for site in ALLOWED_SYNOP_SITES):
            return url
    # fallback to first
    return results[0].get("href") or results[0].get("url") if results else None

def extract_duration_minutes(text: str) -> int | None:
    # Try patterns like "60 min", "1 hr 10 min", "70 minutes", etc.
    text_l = text.lower()
    # 1) X hr Y min or Xh Ym
    m = re.search(r"(\d+)\s*h(?:our)?s?\s*(\d+)\s*m(?:in)?", text_l)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    # 2) X hr(s)
    m = re.search(r"(\d+)\s*h(?:our)?s?", text_l)
    if m:
        return int(m.group(1)) * 60
    # 3) X min / minutes
    m = re.search(r"(\d+)\s*m(?:in|inute|inutes)\b", text_l)
    if m:
        return int(m.group(1))
    # 4) Runtime: 70
    m = re.search(r"runtime[^0-9]*?(\d{1,3})\s*(?:m|min|minutes)?", text_l)
    if m:
        return int(m.group(1))
    return None

def clean_synopsis(text: str) -> str:
    # Remove excessive whitespace, keep ~300–400 chars (flexible).
    txt = re.sub(r"\s+", " ", (text or "")).strip()
    if len(txt) <= 420:
        return txt
    # try cut at sentence end near ~380
    cut = min(len(txt), 450)
    slice_ = txt[:cut]
    # find last period before cut
    p = slice_.rfind(".")
    if 300 <= p <= 420:
        return slice_[:p+1]
    return txt[:420].rstrip() + ("." if not txt[:420].endswith(".") else "")

def fetch_page(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=14)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"⚠️ Fetch page error: {e}")
    return None

def parse_synopsis_from_html(html: str, base_url: str) -> tuple[str | None, int | None]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    # Duration
    duration = extract_duration_minutes(text)

    # Try site-specific synopsis areas
    syn = None

    # MyDramaList
    if "mydramalist.com" in base_url:
        # Often in <div class="show-synopsis"> or meta desc
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if meta and meta.get("content"):
            syn = meta["content"]

    # AsianWiki
    if not syn and "asianwiki.com" in base_url:
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if meta and meta.get("content"):
            syn = meta["content"]

    # Wikipedia
    if not syn and "wikipedia.org" in base_url:
        p = soup.find("p")
        if p:
            syn = p.get_text(" ", strip=True)

    # Netflix / Viki / PrimeVideo / IMDb → try meta description
    if not syn and any(s in base_url for s in ["netflix.com", "viki.com", "primevideo.com", "imdb.com"]):
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if meta and meta.get("content"):
            syn = meta["content"]

    # Generic fallback: look for "Synopsis"
    if not syn:
        lower = text.lower()
        i = lower.find("synopsis")
        if i != -1:
            syn = text[i:i+600]

    return (syn, duration)

def fetch_synopsis_and_duration(show_name: str, year) -> tuple[str, int | None]:
    if not show_name:
        return ("Synopsis not available.", None)

    queries = [
        f"{show_name} {year} drama synopsis",
        f"{show_name} {year} synopsis site:mydramalist.com",
        f"{show_name} {year} synopsis site:asianwiki.com",
        f"{show_name} {year} synopsis site:wikipedia.org",
        f"{show_name} {year} synopsis site:netflix.com",
        f"{show_name} {year} synopsis site:viki.com",
        f"{show_name} {year} synopsis site:primevideo.com",
        f"{show_name} {year} synopsis site:imdb.com",
    ]

    for q in queries:
        results = ddgs_text(q, max_results=8)
        if not results:
            continue
        url = pick_best_result(results)
        if not url:
            continue
        html = fetch_page(url)
        if not html:
            continue
        syn, dur = parse_synopsis_from_html(html, url)
        if syn:
            return (clean_synopsis(syn), dur)

    return ("Synopsis not available.", None)


# ============================================================
# Excel → JSON (per your rules)
# ============================================================
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
    "catagory": "genres",   # Accept both spellings
    "category": "genres",
    "original network": "network",
    "comments": "comments",
}

CHANGE_TRACK_FIELDS = [
    "showName",
    "showImage",
    "releasedYear",
    "totalEpisodes",
    "comments",
    "ratings",
    "genres",
    "Duration",
    "synopsis",
]

def sheet_base_offset(sheet_name: str) -> int:
    if sheet_name == "Sheet1":
        return 1000
    if sheet_name == "Sheet2":
        return 2000
    if sheet_name == "Sheet3":
        return 3000
    return 0

def tidy_comment(val) -> str | None:
    if pd.isna(val) or not str(val).strip():
        return None
    text = " ".join(str(val).split())
    # Capitalize each word (as per earlier requirement)
    text = " ".join(w.capitalize() for w in text.split())
    if not text.endswith("."):
        text += "."
    return text

def excel_to_objects(excel_file: str, sheet_name: str):
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.columns = [c.strip().lower() for c in df.columns]

    # Find "Again Watched Date" start index
    again_idx = None
    for i, c in enumerate(df.columns):
        if "again watched" in c:
            again_idx = i
            break
    if again_idx is None:
        raise ValueError(f"'Again Watched Date' columns not found in sheet: {sheet_name}")

    items = []
    for _, row in df.iterrows():
        obj = {}
        # Map fixed columns up to again_idx
        for col in df.columns[:again_idx]:
            key = COLUMN_MAP.get(col, col)
            val = row[col]

            if key == "showID":
                base = sheet_base_offset(sheet_name)
                obj["showID"] = base + int(val) if pd.notna(val) else None

            elif key == "showName":
                obj["showName"] = " ".join(str(val).split()) if pd.notna(val) else None

            elif key in ("watchStartedOn", "watchEndedOn"):
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
                try:
                    obj[key] = int(val) if pd.notna(val) else 0
                except Exception:
                    obj[key] = 0

            elif key == "genres":
                obj[key] = normalize_list_from_csv(val, cap=True, strip=True)

            elif key == "network":
                # NOTE: per your latest rule → network as array
                obj[key] = normalize_list_from_csv(val, cap=False, strip=True)

            else:
                # Unmapped columns (kept if needed)
                obj[key] = str(val).strip() if pd.notna(val) else None

        # Derived fields
        obj["showType"] = "Mini Drama" if sheet_name.lower() == "mini drama" else "Drama"
        obj["country"] = country_from_native(obj.get("nativeLanguage"))

        # Again watched dates (G..end)
        dates = []
        for v in row[again_idx:]:
            d = ddmmyyyy(v)
            if d:
                dates.append(d)
        obj["againWatchedDates"] = dates

        # updatedOn (IST) + initial updatedDetails
        obj["updatedOn"] = today_ist_long()
        obj["updatedDetails"] = "First time Uploaded"

        # topRatings = ratings * len(againWatchedDates) * 100
        r = int(obj.get("ratings") or 0)
        obj["topRatings"] = r * len(dates) * 100

        # Duration → null for now; will try to fill from synopsis scraping
        obj["Duration"] = None

        # Placeholder for showImage (download after we know networks)
        show_name = obj.get("showName")
        released_year = obj.get("releasedYear")
        networks = obj.get("network") or []
        obj["showImage"] = download_cover_image(show_name, released_year, networks)

        # Synopsis + duration extraction
        synopsis, duration = fetch_synopsis_and_duration(show_name, released_year)
        obj["synopsis"] = synopsis
        if duration is not None and duration > 0:
            obj["Duration"] = int(duration)

        # Final order as requested
        ordered = {
            "showID": obj.get("showID"),
            "showName": obj.get("showName"),
            "showImage": obj.get("showImage"),
            "watchStartedOn": obj.get("watchStartedOn"),
            "watchEndedOn": obj.get("watchEndedOn"),
            "releasedYear": obj.get("releasedYear"),
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
            "Duration": obj.get("Duration"),
        }

        items.append(ordered)

    return items


# ============================================================
# Update logic (create/update/backup)
# ============================================================
def concise_update_message(old_obj, new_obj):
    """
    Return a concise <= 30-char message describing what changed
    among CHANGE_TRACK_FIELDS. Prioritize single-field messages;
    otherwise say 'Multiple fields updated'.
    """
    changed = []
    for field in CHANGE_TRACK_FIELDS:
        if old_obj.get(field) != new_obj.get(field):
            changed.append(field)

    if not changed:
        return None

    # Single field → more specific
    field = changed[0]
    mapping = {
        "showName": "Show name updated",
        "showImage": "New image updated",
        "releasedYear": "Year updated",
        "totalEpisodes": "Episodes updated",
        "comments": "Comments updated",
        "ratings": "Ratings updated",
        "genres": "Genres updated",
        "Duration": "Duration updated",
        "synopsis": "Synopsis updated",
    }
    msg = mapping.get(field, "Object updated")
    return msg[:30]

def update_json_from_excel(excel_file: str, json_file: str, sheet_names: list[str]):
    # Build new set
    new_objects = []
    available = pd.ExcelFile(excel_file).sheet_names
    for s in sheet_names:
        if s not in available:
            print(f"⚠️ Skipping missing sheet: {s}")
            continue
        new_objects.extend(excel_to_objects(excel_file, s))

    # Load old
    if os.path.exists(json_file):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                old_objects = json.loads(content) if content else []
        except Exception:
            print(f"⚠️ {json_file} invalid. Starting fresh.")
            old_objects = []
    else:
        old_objects = []

    old_by_id = {o["showID"]: o for o in old_objects if "showID" in o}
    new_by_id = {o["showID"]: o for o in new_objects if "showID" in o}

    changed_or_deleted = []

    # Update or add
    for sid, new_obj in new_by_id.items():
        if sid in old_by_id:
            old_obj = old_by_id[sid]
            if old_obj != new_obj:
                # mark updatedOn + updatedDetails
                new_obj["updatedOn"] = today_ist_long()
                msg = concise_update_message(old_obj, new_obj) or "Object updated"
                new_obj["updatedDetails"] = msg[:30]
                changed_or_deleted.append(old_obj)  # move old to backup
                old_by_id[sid] = new_obj  # replace
        else:
            # First time
            old_by_id[sid] = new_obj

    # Removed entries
    for sid, old_obj in list(old_by_id.items()):
        if sid not in new_by_id:
            changed_or_deleted.append(old_obj)
            del old_by_id[sid]

    # Write merged current
    merged = sorted(old_by_id.values(), key=lambda x: x.get("showID", 0))
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=4, ensure_ascii=False)

    # Write backup if needed
    if changed_or_deleted:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        backup_name = os.path.join(BACKUP_DIR, f"{timestamp_filename()}.json")
        with open(backup_name, "w", encoding="utf-8") as f:
            json.dump(changed_or_deleted, f, indent=4, ensure_ascii=False)
        print(f"✅ Updated JSON. Backup saved → {backup_name}")
    else:
        if not old_objects:
            print(f"✅ Created JSON → {json_file}")
        else:
            print("✅ No changes detected")


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    # 1) Fetch latest Excel from Google Drive (private)
    try:
        download_from_gdrive(EXCEL_FILE_ID, LOCAL_EXCEL_FILE, SERVICE_ACCOUNT_FILE)
    except Exception as e:
        print(f"⚠️ Google Drive fetch failed: {e}")
        if not os.path.exists(LOCAL_EXCEL_FILE):
            raise FileNotFoundError("❌ No Excel file available locally or from Drive!")

    # 2) Run updater against the downloaded file
    update_json_from_excel(LOCAL_EXCEL_FILE, JSON_FILE, SHEETS)
