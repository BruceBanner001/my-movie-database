# ============================================================
# File: create_update_backup_delete_improved.py
# Repo: my-movie-database
# Author: Adapted for user (auto-generated modifications)
# Purpose: Improved version implementing caching, chunking, preferred-site search order,
# reduced image queries, weekly-only "betterment" of existing data, email-friendly
# run reports, old-image retention and manual JSON updates via "manual update" sheet.
# IMPORTANT: Update environment variables in your GitHub Actions workflow as described
# in the README section at the bottom of this file.
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
import shutil

# Google Drive
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# -----------------------------
# Config — change as needed
# -----------------------------

# Read Excel File ID from a file written by GitHub Actions
with open("EXCEL_FILE_ID.txt", "r") as f:
    EXCEL_FILE_ID = f.read().strip()

SERVICE_ACCOUNT_FILE = "GDRIVE_SERVICE_ACCOUNT.json" # Stored as Secret Key in GitHub Actions.

# Local temp Excel
LOCAL_EXCEL_FILE = "local-data.xlsx"

# Target JSON
JSON_FILE = "seriesData.json"

# Backups and images
BACKUP_DIR = "backups"
IMAGES_DIR = "images"
OLD_IMAGES_DIR = "old-images"

# Report file (plain text / mobile-friendly)
REPORTS_DIR = "reports"

# GitHub Pages absolute base (so your app can load images)
GITHUB_PAGES_URL = "https://brucebanner001.github.io/my-movie-database/"

# Fixed cover size (uniform, high quality)
COVER_WIDTH, COVER_HEIGHT = 600, 900
FORCE_REFRESH_IMAGES = False  # set True to re-download images always (unless overridden)

# Sheets to process (you can edit this list any time)
SHEETS = ["Sheet1"]  # include any that exist
# SHEETS = ["Feb 7 2023 Onwards"]  # include any that exist

# Maximum number of shows to process per run (set to desired value in workflow env or here)
# Example: set MAX_PER_RUN = 100 to process only 100 shows in a single workflow run.
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "0"))  # 0 means no limit (process all)

# If this run was triggered by schedule (weekly automatic workflow), set env var SCHEDULED_RUN=true
# Workflow should set SCHEDULED_RUN=true for scheduled runs. For manual runs leave it unset or false.
SCHEDULED_RUN = os.environ.get("SCHEDULED_RUN", "false").lower() == "true"

# If set to true, when an image is updated move old image to old-images and keep for 7 days before cleanup
KEEP_OLD_IMAGES_DAYS = 7

# Reduce aggressive searching
IMAGE_SEARCH_MAX_PER_QUERY = 6  # we keep small and stop after first successful image

# Preferred search order mapping by nativeLanguage (can add more languages here)
# The values are lists of site keys which you can map to actual search functions/queries below.
PREFERRED_SITE_ORDER = {
    "Korean": ["asianwiki", "mydramalist"],
    "Chinese": ["mydramalist", "asianwiki"],
}

# Sites allowed for synopsis searching (fallback order will be used after preferred sites fail)
ALLOWED_SYNOP_SITES = [
    "mydramalist.com",
    "asianwiki.com",
    "wikipedia.org",
    "netflix.com",
    "viki.com",
    "primevideo.com",
    "imdb.com",
]

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
# Image Search / Download (reduced + stop early)
# ============================================================
HEADERS = {"User-Agent": "Mozilla/5.0"}


def try_ddgs_images(query, max_results=IMAGE_SEARCH_MAX_PER_QUERY):
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(query, max_results=max_results))
            return [r.get("image") for r in results if r.get("image")]
    except Exception as e:
        print(f"⚠️ DDGS image search error: {e}")
        return []


def try_bing_images(query, count=IMAGE_SEARCH_MAX_PER_QUERY):
    url = f"https://www.bing.com/images/search?q={requests.utils.quote(query)}&form=HDRSC2"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        return [p.split('"')[0] for p in r.text.split('"murl":"')[1:count+1]]
    except Exception as e:
        print(f"⚠️ Bing image search error: {e}")
        return []


def try_google_images(query, count=IMAGE_SEARCH_MAX_PER_QUERY):
    url = f"https://www.google.com/search?tbm=isch&q={requests.utils.quote(query)}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
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
    local_path = local_path.replace("\\", "/")
    return GITHUB_PAGES_URL.rstrip("/") + "/" + local_path.lstrip("/")


def search_cover_image_queries(show_name: str, year, networks: list):
    core = [
        f"{show_name} {year} drama poster",
        f"{show_name} {year} official poster",
        f"{show_name} drama poster",
        f"{show_name} {year} poster",
        f"{show_name} poster",
    ]
    net_queries = [f"{show_name} {year} {net} poster" for net in (networks or [])]
    return core + net_queries


def download_cover_image(show_name: str, year, networks=None, prefer_sites: list | None = None,
                         existing_image_url: str | None = None, allow_replace=False) -> str | None:
    """
    Download an image and return absolute GH Pages URL.
    If existing_image_url provided and allow_replace is False -> skip downloading.
    prefer_sites: list of site keys like ['asianwiki','mydramalist'] which we will try first by crafting queries
    """
    if not show_name or not year:
        return None
    os.makedirs(IMAGES_DIR, exist_ok=True)
    os.makedirs(OLD_IMAGES_DIR, exist_ok=True)

    filename = f"{safe_filename(show_name)}_{year}.jpg"
    local_path = os.path.join(IMAGES_DIR, filename)

    # If already exists locally and we are not forcing refresh and not allowed to replace, skip
    if os.path.exists(local_path) and not FORCE_REFRESH_IMAGES and not allow_replace:
        return build_absolute_url(local_path)

    # If existing_image_url exists and this is not a scheduled run, skip looking for a "better" image
    if existing_image_url and not SCHEDULED_RUN and not allow_replace:
        return existing_image_url

    # Build queries list with preferred site hints first
    queries = []
    if prefer_sites:
        for s in prefer_sites:
            if s == 'asianwiki':
                queries.append(f"{show_name} {year} asianwiki poster")
            if s == 'mydramalist':
                queries.append(f"{show_name} {year} mydramalist poster")
            # add more mappings here as needed

    queries += search_cover_image_queries(show_name, year, networks)

    # Try each source set in priority; stop after first successful image
    for q in queries:
        print(f"🔍 Searching image: {q}")
        urls = try_ddgs_images(q) or try_bing_images(q) or try_google_images(q)
        for url in urls:
            if download_image_to(url, local_path):
                print(f"✅ Image saved → {local_path}")
                return build_absolute_url(local_path)
        time.sleep(random.uniform(0.8, 1.5))  # small sleep to avoid throttling

    print(f"❌ Could not find image for {show_name} ({year})")
    return None


# ============================================================
# Synopsis + Duration (mins) scraping (keep but prefer specific sites first)
# ============================================================

def ddgs_text(query, max_results=6):
    try:
        with DDGS() as dd:
            return list(dd.text(query, max_results=max_results))
    except Exception as e:
        print(f"⚠️ DDGS text error: {e}")
        return []


def pick_best_result(results):
    for r in results:
        url = r.get("href") or r.get("url") or ""
        if any(site in url for site in ALLOWED_SYNOP_SITES):
            return url
    return results[0].get("href") or results[0].get("url") if results else None


def extract_duration_minutes(text: str) -> int | None:
    text_l = text.lower()
    m = re.search(r"(\d+)\s*h(?:our)?s?\s*(\d+)\s*m(?:in)?", text_l)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    m = re.search(r"(\d+)\s*h(?:our)?s?", text_l)
    if m:
        return int(m.group(1)) * 60
    m = re.search(r"(\d+)\s*m(?:in|inute|inutes)\b", text_l)
    if m:
        return int(m.group(1))
    m = re.search(r"runtime[^0-9]*?(\d{1,3})\s*(?:m|min|minutes)?", text_l)
    if m:
        return int(m.group(1))
    return None


def clean_synopsis(text: str) -> str:
    txt = re.sub(r"\s+", " ", (text or "")).strip()
    if len(txt) <= 420:
        return txt
    cut = min(len(txt), 450)
    slice_ = txt[:cut]
    p = slice_.rfind(".")
    if 300 <= p <= 420:
        return slice_[:p+1]
    return txt[:420].rstrip() + ("." if not txt[:420].endswith(".") else "")


def fetch_page(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"⚠️ Fetch page error: {e}")
    return None


def parse_synopsis_from_html(html: str, base_url: str) -> tuple[str | None, int | None]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    duration = extract_duration_minutes(text)
    syn = None
    if "mydramalist.com" in base_url:
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if meta and meta.get("content"):
            syn = meta["content"]
    if not syn and "asianwiki.com" in base_url:
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if meta and meta.get("content"):
            syn = meta["content"]
    if not syn and "wikipedia.org" in base_url:
        p = soup.find("p")
        if p:
            syn = p.get_text(" ", strip=True)
    if not syn and any(s in base_url for s in ["netflix.com", "viki.com", "primevideo.com", "imdb.com"]):
        meta = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
        if meta and meta.get("content"):
            syn = meta["content"]
    if not syn:
        lower = text.lower()
        i = lower.find("synopsis")
        if i != -1:
            syn = text[i:i+600]
    return (syn, duration)


def fetch_synopsis_and_duration(show_name: str, year, prefer_sites: list | None = None,
                                existing_synopsis: str | None = None, allow_replace=False) -> tuple[str, int | None]:
    """
    If existing_synopsis present and not SCHEDULED_RUN and replace not allowed -> skip scraping.
    prefer_sites: list like ['asianwiki','mydramalist'] to be searched first.
    """
    if not show_name:
        return ("Synopsis not available.", None)

    if existing_synopsis and not SCHEDULED_RUN and not allow_replace:
        return (existing_synopsis, None)

    # Build prioritized queries
    queries = []
    if prefer_sites:
        for s in prefer_sites:
            if s == 'mydramalist':
                queries.append(f"{show_name} {year} site:mydramalist.com synopsis")
            if s == 'asianwiki':
                queries.append(f"{show_name} {year} site:asianwiki.com synopsis")

    # Fallback queries
    fallback = [
        f"{show_name} {year} drama synopsis site:mydramalist.com",
        f"{show_name} {year} synopsis site:asianwiki.com",
        f"{show_name} {year} synopsis site:wikipedia.org",
        f"{show_name} {year} synopsis site:netflix.com",
        f"{show_name} {year} synopsis site:viki.com",
        f"{show_name} {year} synopsis site:primevideo.com",
        f"{show_name} {year} synopsis site:imdb.com",
    ]

    queries += fallback

    for q in queries:
        results = ddgs_text(q, max_results=6)
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
        time.sleep(0.5)

    return ("Synopsis not available.", None)


# ============================================================
# Excel → JSON (per your rules) with caching + chunking + manual updates
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
    "catagory": "genres",
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
    text = " ".join(w.capitalize() for w in text.split())
    if not text.endswith("."):
        text += "."
    return text


def excel_to_objects(excel_file: str, sheet_name: str, existing_by_id: dict, report_changes: dict, max_items: int | None = None):
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
    processed = 0
    for _, row in df.iterrows():
        if max_items and processed >= max_items:
            break
        obj = {}
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
                obj[key] = normalize_list_from_csv(val, cap=False, strip=True)

            else:
                obj[key] = str(val).strip() if pd.notna(val) else None

        obj["showType"] = "Mini Drama" if sheet_name.lower() == "mini drama" else "Drama"
        obj["country"] = country_from_native(obj.get("nativeLanguage"))

        # Again watched dates
        dates = []
        for v in row[again_idx:]:
            d = ddmmyyyy(v)
            if d:
                dates.append(d)
        obj["againWatchedDates"] = dates

        obj["updatedOn"] = today_ist_long()
        obj["updatedDetails"] = "First time Uploaded"

        r = int(obj.get("ratings") or 0)
        obj["topRatings"] = r * len(dates) * 100

        obj["Duration"] = None

        # Show image and synopsis behavior with caching rules
        show_name = obj.get("showName")
        released_year = obj.get("releasedYear")
        networks = obj.get("network") or []

        existing = None
        if obj.get("showID") in existing_by_id:
            existing = existing_by_id[obj.get("showID")]

        # Decide preferred site order based on nativeLanguage
        prefer = None
        native = obj.get("nativeLanguage")
        if native and native in PREFERRED_SITE_ORDER:
            prefer = PREFERRED_SITE_ORDER[native]

        # IMAGE: if existing has showImage and not scheduled run -> skip; else try to find
        existing_image_url = existing.get("showImage") if existing else None
        allow_replace_image = SCHEDULED_RUN  # only replace on scheduled runs (weekly)

        # If existing image exists and we are replacing, move old to old-images
        new_image_url = None
        if existing_image_url and allow_replace_image:
            # try to find better image: pass allow_replace=True so download_cover_image will replace
            new_image_url = download_cover_image(show_name, released_year, networks, prefer_sites=prefer,
                                                 existing_image_url=existing_image_url, allow_replace=True)
            if new_image_url and new_image_url != existing_image_url:
                # move old image file to old-images
                try:
                    old_local = os.path.join(IMAGES_DIR, os.path.basename(existing_image_url))
                    if os.path.exists(old_local):
                        dest = os.path.join(OLD_IMAGES_DIR, os.path.basename(old_local))
                        shutil.move(old_local, dest)
                except Exception as e:
                    print(f"⚠️ Could not move old image: {e}")
        else:
            # either no existing image or not scheduled run -> get only if none exists
            if not existing_image_url:
                new_image_url = download_cover_image(show_name, released_year, networks, prefer_sites=prefer,
                                                     existing_image_url=None, allow_replace=False)
            else:
                new_image_url = existing_image_url

        obj["showImage"] = new_image_url

        # SYNOPSIS: similar rules
        existing_syn = existing.get("synopsis") if existing else None
        allow_replace_syn = SCHEDULED_RUN
        if existing_syn and allow_replace_syn:
            new_syn, dur = fetch_synopsis_and_duration(show_name, released_year, prefer_sites=prefer,
                                                       existing_synopsis=existing_syn, allow_replace=True)
        else:
            # if no existing synopsis -> fetch. If exists and not scheduled -> keep existing
            new_syn, dur = fetch_synopsis_and_duration(show_name, released_year, prefer_sites=prefer,
                                                       existing_synopsis=existing_syn, allow_replace=False)

        obj["synopsis"] = new_syn
        if dur is not None and dur > 0:
            obj["Duration"] = int(dur)
        elif existing and existing.get("Duration"):
            obj["Duration"] = existing.get("Duration")

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
        processed += 1

        # Track created vs updated for report
        sid = ordered.get("showID")
        if existing is None:
            report_changes.setdefault("created", []).append(ordered)
        else:
            if existing != ordered:
                # determine what changed and push to updated
                report_changes.setdefault("updated", []).append({"old": existing, "new": ordered})

    return items


# ============================================================
# Update logic (create/update/backup) with chunking and report generation
# ============================================================

def concise_update_message(old_obj, new_obj):
    changed = []
    for field in CHANGE_TRACK_FIELDS:
        if old_obj.get(field) != new_obj.get(field):
            changed.append(field)
    if not changed:
        return None
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


def cleanup_old_images():
    # delete files older than KEEP_OLD_IMAGES_DAYS in OLD_IMAGES_DIR
    if not os.path.exists(OLD_IMAGES_DIR):
        return
    cutoff = datetime.now() - timedelta(days=KEEP_OLD_IMAGES_DAYS)
    for fname in os.listdir(OLD_IMAGES_DIR):
        path = os.path.join(OLD_IMAGES_DIR, fname)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                os.remove(path)
        except Exception as e:
            print(f"⚠️ Could not cleanup old image {path}: {e}")


def write_report(report_changes_by_sheet: dict, report_path: str):
    # Mobile-friendly plain text report. Group by sheet name and date.
    lines = []
    for sheet, changes in report_changes_by_sheet.items():
        lines.append(f"=== {sheet} — {today_ist_long()} ===")
        # Data Created
        created = changes.get('created', [])
        if created:
            lines.append("\nData Created:")
            for obj in created:
                # Capitalize first letters of field-names, but do not include actual values per user request
                lines.append(f"- {words_capitalize(obj.get('showName','Unknown'))} -> Created")
        # Data Updated
        updated = changes.get('updated', [])
        if updated:
            lines.append("\nData Updated:")
            for pair in updated:
                new = pair.get('new')
                old = pair.get('old')
                changed_fields = [f for f in CHANGE_TRACK_FIELDS if old.get(f) != new.get(f)]
                fields_text = ", ".join([words_capitalize(f) for f in changed_fields]) if changed_fields else "General"
                lines.append(f"- {words_capitalize(new.get('showName','Unknown'))} -> Updated: {fields_text}")
        # Image Updated
        images = changes.get('images', [])
        if images:
            lines.append("\nImage Updated:")
            for itm in images:
                lines.append(f"- {words_capitalize(itm.get('showName','Unknown'))} -> Old && New")
                lines.append(f"  Old: {itm.get('old')}")
                lines.append(f"  New: {itm.get('new')}")
        lines.append("\n")

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))


def update_json_from_excel(excel_file: str, json_file: str, sheet_names: list[str], max_per_run: int = 0):
    # Load old objects
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

    # iterate sheets one by one (user asked to process one sheet per workflow is possible by setting SHEETS accordingly)
    merged_by_id = dict(old_by_id)  # will be updated
    report_changes_by_sheet = {}

    for s in sheet_names:
        print(f"Processing sheet: {s}")
        report_changes = {}
        new_items = excel_to_objects(excel_file, s, old_by_id, report_changes, max_items=max_per_run if max_per_run > 0 else None)

        # Merge new items into merged_by_id and create backups for changed/deleted
        changed_or_deleted = []
        for new_obj in new_items:
            sid = new_obj.get('showID')
            if sid in merged_by_id:
                old_obj = merged_by_id[sid]
                if old_obj != new_obj:
                    new_obj['updatedOn'] = today_ist_long()
                    msg = concise_update_message(old_obj, new_obj) or "Object updated"
                    new_obj['updatedDetails'] = msg[:30]
                    changed_or_deleted.append(old_obj)
                    merged_by_id[sid] = new_obj
            else:
                merged_by_id[sid] = new_obj

        # Detect removals in this sheet (if you want to remove entries that existed previously and are not in new sheet)
        # For safety, we won't auto-delete across sheets in this version unless explicitly desired.

        # Collect report changes
        report_changes_by_sheet[s] = report_changes

        # Write backup for this sheet run
        if changed_or_deleted:
            os.makedirs(BACKUP_DIR, exist_ok=True)
            backup_name = os.path.join(BACKUP_DIR, f"{timestamp_filename()}_{safe_filename(s)}.json")
            with open(backup_name, "w", encoding="utf-8") as f:
                json.dump(changed_or_deleted, f, indent=4, ensure_ascii=False)
            print(f"✅ Backup saved → {backup_name}")

    # Write merged current
    merged = sorted(merged_by_id.values(), key=lambda x: x.get("showID", 0))
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=4, ensure_ascii=False)

    # Report
    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_path = os.path.join(REPORTS_DIR, f"report_{timestamp_filename()}.txt")
    write_report(report_changes_by_sheet, report_path)
    print(f"✅ Report written → {report_path}")

    # Cleanup old images older than KEEP_OLD_IMAGES_DAYS
    cleanup_old_images()


# ============================================================
# Manual update: read sheet 'manual update' with columns: showID | dataString
# dataString example: "\"synopsis\":\"my new synopsis\"" OR "\"watchedLanguage\":\"Tamil\""
# Data string should be a small JSON-like fragment (without outer braces) or a full JSON object.
# Example cell value: {"synopsis":"My new synopsis","showImage":"images/new.jpg"}
# ============================================================

def apply_manual_updates(excel_file: str, json_file: str):
    sheet = 'manual update'
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet)
    except Exception:
        print(f"No '{sheet}' sheet found; skipping manual updates.")
        return

    if df.shape[1] < 2:
        print("Manual update sheet must have at least two columns: showID and dataString")
        return

    # Load current JSON
    if not os.path.exists(json_file):
        print("No JSON file to update")
        return
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    by_id = {o['showID']: o for o in data}

    updated = []
    for _, row in df.iterrows():
        sid = int(row[0]) if not pd.isna(row[0]) else None
        if sid is None or sid not in by_id:
            continue
        raw = row[1]
        if pd.isna(raw) or not str(raw).strip():
            continue
        s = str(raw).strip()
        # If the user entered a JSON object, parse it; else try to convert fragment to JSON
        try:
            if s.startswith('{'):
                upd = json.loads(s)
            else:
                # wrap in braces
                upd = json.loads('{' + s + '}')
        except Exception as e:
            print(f"Could not parse manual update for {sid}: {e}")
            continue

        obj = by_id[sid]
        for k, v in upd.items():
            obj[k] = v
        obj['updatedOn'] = today_ist_long()
        obj['updatedDetails'] = f"Updated {', '.join([words_capitalize(k) for k in upd.keys()])} Mannually By Owner"
        updated.append(obj)

    if updated:
        # write back
        merged = sorted(by_id.values(), key=lambda x: x.get('showID', 0))
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(merged, f, indent=4, ensure_ascii=False)
        print(f"✅ Applied {len(updated)} manual updates")


# ============================================================
# Google Drive helper (unchanged)
# ============================================================

def download_from_gdrive(file_id: str, destination: str, service_account_file: str):
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
# Main
# ============================================================
if __name__ == "__main__":
    # 1) Fetch latest Excel from Google Drive (private)
    try:
        download_from_gdrive(EXCEL_FILE_ID, LOCAL_EXCEL_FILE, SERVICE_ACCOUNT_FILE)
    except Exception as e:
        print(f"⚠️ Google Drive fetch failed: {e}")
        if not os.path.exists(LOCAL_EXCEL_FILE):
            import json
            # === DEBUG: Print which secrets are being used ===
            try:
                with open("EXCEL_FILE_ID.txt", "r") as f:
                    excel_file_id = f.read().strip()
                print(f"🔎 Using EXCEL_FILE_ID: {excel_file_id}")
            except Exception as e:
                print(f"❌ Could not read EXCEL_FILE_ID.txt: {e}")
                excel_file_id = None

            try:
                with open("GDRIVE_SERVICE_ACCOUNT.json", "r") as f:
                    sa_data = json.load(f)
                    client_email = sa_data.get("client_email", "UNKNOWN")
                print(f"🔎 Using Service Account: {client_email}")
            except Exception as e:
                print(f"❌ Could not read GDRIVE_SERVICE_ACCOUNT.json: {e}")
                client_email = None

            # === Existing failure raise ===
            raise FileNotFoundError(
                f"❌ No Excel file available locally or from Drive!\n"
                f"   - Excel File ID: {excel_file_id}\n"
                f"   - Service Account: {client_email}\n"
                f"👉 Make sure the Excel file is shared with the Service Account above."
            )


    # 2) Apply manual updates first (if any)
    apply_manual_updates(LOCAL_EXCEL_FILE, JSON_FILE)

    # 3) Run updater against the downloaded file
    # Determine per-run max
    per_run = MAX_PER_RUN if MAX_PER_RUN > 0 else 0
    update_json_from_excel(LOCAL_EXCEL_FILE, JSON_FILE, SHEETS, max_per_run=per_run)

    print("All done.")

# ============================================================
# README NOTES (important for workflow integration)
# ============================================================
# 1) To run limited chunk per workflow set environment variable MAX_PER_RUN (e.g. 100) in your GitHub Actions
#    job's `env:` section. If 0 or unset -> process all rows.
# 2) For weekly scheduled runs (where you want the script to attempt to find "better" images/synopses even when
#    values already exist), set environment variable SCHEDULED_RUN=true in the workflow for scheduled triggers.
# 3) The workflow should upload the service account JSON into GDRIVE_SERVICE_ACCOUNT.json and write the Excel
#    file id into EXCEL_FILE_ID.txt before running this script (as before).
# 4) The script writes a plain text report into ./reports/report_DDMMYYYY_HHMM.txt which you can attach to
#    email notifications. The report groups changes by sheet name and shows created/updated/image-updated entries
#    in a mobile-friendly format.
# 5) Old images moved to ./old-images are automatically cleaned up after KEEP_OLD_IMAGES_DAYS on each run.
# 6) If you want to add more language preferences, update the PREFERRED_SITE_ORDER dict near the top.
# ============================================================
