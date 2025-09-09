"""
====================================================
 Script: create_update_backup_delete.py
 Repo:   my-movie-database
====================================================

Reads drama/movie data from one or more Excel sheets and generates/updates
a JSON database with:
 - Field renames and normalization (dates, arrays, casing, trimming)
 - showID prefix per sheet (Sheet1→+1000, Sheet2→+2000, Sheet3→+3000)
 - showType (or "Mini Drama" if sheet is named exactly "Mini Drama")
 - country inference from nativeLanguage
 - comments cleanup (title-cased; trailing period)
 - againWatchedDates (G..end) as ["DD-MM-YYYY", ...]
 - topRatings = ratings * len(againWatchedDates) * 100
 - updatedOn (IST, "dd Month YYYY")
 - updatedDetails (first upload vs concise change note <= 30 chars)
 - cover image search (DDG → Bing → Google; with network/site hints)
   resized to 600x900, saved to /images, and stored as ABSOLUTE URL
 - synopsis scraping (Wikipedia, MDL, AsianWiki, Netflix/Viki/Prime) with
   smart fallback; normalized to ~300–450 chars; flexible if story needs more/less
 - duration parsing (minutes as integer)
 - backups: any changed/removed OLD objects are moved to one timestamped file
   in /backups per run (DDMMYYYY_HHMM.json)

Requires: pandas, openpyxl, requests, pillow, ddgs, beautifulsoup4, lxml
(see requirements.txt)
"""

from __future__ import annotations

import os
import re
import json
import time
import random
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Tuple, Optional, Dict

import pandas as pd
import requests
from PIL import Image
from bs4 import BeautifulSoup
from ddgs import DDGS

# =====================================================
# CONFIG
# =====================================================

# Images
COVER_WIDTH = 600
COVER_HEIGHT = 900
FORCE_REFRESH_IMAGES = False
IMAGES_DIR = "images"
BACKUPS_DIR = "backups"

# Timezone & formatting
IST = ZoneInfo("Asia/Kolkata")

# Where your site is published (ABSOLUTE URL BASE)
# This is used to generate absolute URLs for showImage in JSON
GITHUB_PAGES_URL = "https://brucebanner001.github.io/my-movie-database/"

# HTTP defaults
DEF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
}

# Preferred sites for synopsis
TARGET_TEXT_SITES = [
    "wikipedia.org", "asianwiki.com", "mydramalist.com",
    "netflix.com", "viki.com", "primevideo.com", "amazon.com"
]

# For image search hints
PLATFORM_HINTS = ["Netflix", "Viki", "Prime Video", "Amazon Prime", "AsianWiki", "MyDramaList"]


# =====================================================
# UTILITIES
# =====================================================

def ensure_dirs() -> None:
    os.makedirs(IMAGES_DIR, exist_ok=True)
    os.makedirs(BACKUPS_DIR, exist_ok=True)
    # Keep folders in git
    for d in (IMAGES_DIR, BACKUPS_DIR):
        kp = os.path.join(d, ".gitkeep")
        if not os.path.exists(kp):
            with open(kp, "a", encoding="utf-8"):
                pass

def format_date(val) -> Optional[str]:
    """Return DD-MM-YYYY or None."""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, pd.Timestamp):
        return val.strftime("%d-%m-%Y")
    try:
        dt = pd.to_datetime(str(val), dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return None

def ist_today_str() -> str:
    return datetime.now(IST).strftime("%d %B %Y")

def timestamp_fname() -> str:
    return datetime.now(IST).strftime("%d%m%Y_%H%M")

def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip()

def cap_each_word(s: str) -> str:
    return " ".join([w.capitalize() for w in str(s).split()])

def safe_filename(name: str) -> str:
    name = clean_spaces(str(name).lower())
    name = name.replace(" ", "_").replace("/", "_")
    return re.sub(r"[^a-z0-9_\-]+", "", name)

def to_minutes(text: str) -> Optional[int]:
    """Parse durations like '1h 10m', '70 min', '70 minutes' → minutes (int)."""
    s = text.lower()
    # 1h 10m, 2h, 1hr 30min
    m = re.search(r"(\d+)\s*h(?:our|r)?(?:\s*(\d+)\s*m(?:in|inute)?s?)?", s)
    if m:
        h = int(m.group(1))
        mm = int(m.group(2)) if m.group(2) else 0
        return h * 60 + mm
    # 70 min / minutes
    m = re.search(r"(\d+)\s*m(?:in|inute)?s?", s)
    if m:
        return int(m.group(1))
    return None


# =====================================================
# IMAGE HELPERS
# =====================================================

def resize_and_save_jpeg(content: bytes, path: str) -> bool:
    try:
        img = Image.open(BytesIO(content)).convert("RGB")
        img = img.resize((COVER_WIDTH, COVER_HEIGHT), Image.LANCZOS)
        img.save(path, "JPEG", quality=93, optimize=True, progressive=True)
        return True
    except Exception as e:
        print(f"⚠️ Image resize/save failed {path}: {e}")
        return False

def try_download(url: str, path: str) -> bool:
    try:
        r = requests.get(url, headers=DEF_HEADERS, timeout=25, stream=True)
        if r.status_code == 200 and "image" in r.headers.get("Content-Type", ""):
            return resize_and_save_jpeg(r.content, path)
    except Exception as e:
        print(f"⚠️ Download failed {url}: {e}")
    return False

def ddg_image_urls(query: str, max_results: int = 12) -> List[str]:
    try:
        with DDGS() as ddgs:
            results = list(ddgs.images(query, max_results=max_results))
            return [r["image"] for r in results if "image" in r]
    except Exception as e:
        print(f"⚠️ DuckDuckGo error: {e}")
        return []

def scrape_bing_image_urls(query: str, max_results: int = 10) -> List[str]:
    url = f"https://www.bing.com/images/search?q={requests.utils.quote(query)}&form=HDRSC2"
    try:
        resp = requests.get(url, headers=DEF_HEADERS, timeout=20)
        resp.raise_for_status()
        urls = []
        for part in resp.text.split('"murl":"')[1:max_results+1]:
            u = part.split('"')[0]
            urls.append(u)
        return urls
    except Exception as e:
        print(f"⚠️ Bing scrape error: {e}")
        return []

def scrape_google_image_urls(query: str, max_results: int = 10) -> List[str]:
    url = f"https://www.google.com/search?tbm=isch&q={requests.utils.quote(query)}"
    try:
        resp = requests.get(url, headers=DEF_HEADERS, timeout=20)
        resp.raise_for_status()
        urls = []
        # crude parse for image original urls
        for part in resp.text.split('"ou"')[1:max_results+1]:
            u = part.split('"')[1]
            urls.append(u)
        return urls
    except Exception as e:
        print(f"⚠️ Google scrape error: {e}")
        return []

def download_cover_image(show_name: str, year: Optional[int], networks: List[str]) -> Optional[str]:
    """
    Try multiple queries and sources; falls back to including platform/network hints.
    Saves to images/<safe_show>_<year>.jpg and returns LOCAL PATH or None.
    """
    ensure_dirs()

    y = f"{year}" if year else ""
    base = f"{safe_filename(show_name)}_{y}".strip("_")
    fpath = os.path.join(IMAGES_DIR, f"{base}.jpg")

    if os.path.exists(fpath) and not FORCE_REFRESH_IMAGES:
        return fpath

    queries = [
        f"{show_name} {y} drama official poster",
        f"{show_name} {y} tv series cover",
        f"{show_name} {y} poster",
        f"{show_name} drama poster",
    ]

    # Add network/platform hints
    hints = list(networks or [])
    for p in PLATFORM_HINTS:
        hints.append(p)
    hints = [h for h in hints if h and h.strip()]

    for h in hints:
        queries.extend([
            f"{show_name} {y} {h} cover",
            f"{show_name} {y} {h} poster",
            f"{show_name} {y} {h} official poster",
        ])

    # Attempt search → download pipeline
    for q in queries:
        print(f"🔍 Image search: {q}")
        for source in (ddg_image_urls, scrape_bing_image_urls, scrape_google_image_urls):
            try_urls = source(q, 10)
            for u in try_urls:
                if try_download(u, fpath):
                    print(f"✅ Image saved: {fpath}")
                    return fpath
        time.sleep(random.uniform(1.2, 2.4))

    print(f"❌ Could not find image for {show_name} ({y})")
    return None


# =====================================================
# SYNOPSIS HELPERS
# =====================================================

def ddg_text_results(query: str, max_results: int = 10) -> List[Dict]:
    try:
        with DDGS() as ddgs:
            return list(ddgs.text(query, max_results=max_results))
    except Exception as e:
        print(f"⚠️ DDG text error: {e}")
        return []

def fetch_url(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=DEF_HEADERS, timeout=25)
        if r.status_code == 200 and r.text:
            return r.text
    except Exception as e:
        print(f"⚠️ Fetch failed: {url} → {e}")
    return None

def extract_synopsis_generic(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    # Try meta description
    m = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    if m and m.get("content"):
        return clean_spaces(m["content"])

    # Fallback: first 2–4 paragraphs with decent length
    paras = [clean_spaces(p.get_text(" ")) for p in soup.find_all("p")]
    paras = [p for p in paras if len(p) > 60]
    if paras:
        return " ".join(paras[:3])
    return None

def extract_synopsis_mdl(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    sec = soup.select_one(".show-synopsis, .synopsis, .col-12 .text-justify, .about .text")
    if sec:
        return clean_spaces(sec.get_text(" "))
    return extract_synopsis_generic(html)

def extract_synopsis_asianwiki(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    # AsianWiki often uses #mw-content-text p's at the top
    content = soup.select_one("#mw-content-text") or soup
    paras = [clean_spaces(p.get_text(" ")) for p in content.find_all("p")]
    paras = [p for p in paras if len(p) > 60]
    if paras:
        return " ".join(paras[:3])
    return extract_synopsis_generic(html)

def extract_synopsis_wikipedia(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    content = soup.select_one("#mw-content-text") or soup
    paras = [clean_spaces(p.get_text(" ")) for p in content.find_all("p", recursive=True)]
    paras = [p for p in paras if len(p) > 60]
    if paras:
        return " ".join(paras[:3])
    return extract_synopsis_generic(html)

def try_extract_duration(html: str) -> Optional[int]:
    text = " ".join(BeautifulSoup(html, "lxml").stripped_strings)
    return to_minutes(text)

def normalize_synopsis(s: str) -> str:
    """Target ~300–450 chars; allow flex if story needs more/less."""
    s = clean_spaces(s)
    # If already within nice range, return
    if 260 <= len(s) <= 520:
        return s
    # Otherwise trim by sentences up to ~520 chars
    sentences = re.split(r"(?<=[.!?])\s+", s)
    out = ""
    for sent in sentences:
        if not out:
            out = sent
        else:
            if len(out) + 1 + len(sent) > 520:
                break
            out = f"{out} {sent}"
    # If still too long, hard trim with ellipsis
    if len(out) > 540:
        out = out[:540].rsplit(" ", 1)[0] + "…"
    return out

def fetch_synopsis_and_duration(show_name: str, year: Optional[int], networks: List[str]) -> Tuple[Optional[str], Optional[int]]:
    """
    Search preferred text sites; return (synopsis, duration_minutes).
    """
    queries = [
        f"{show_name} {year} synopsis",
        f"{show_name} {year} plot",
        f"{show_name} synopsis",
        f"{show_name} drama synopsis",
    ]
    for n in (networks or []):
        queries.append(f"{show_name} {year} {n} synopsis")

    best_syn = None
    best_dur = None

    for q in queries:
        print(f"🔎 Synopsis search: {q}")
        results = ddg_text_results(q, 10)
        # Prioritize target domains
        sorted_results = sorted(
            results,
            key=lambda r: (
                0 if any(s in r.get("url", "") for s in TARGET_TEXT_SITES) else 1
            )
        )
        for r in sorted_results:
            url = r.get("url") or ""
            if not url:
                continue
            html = fetch_url(url)
            if not html:
                continue

            # Extract synopsis with site-specific logic
            syn = None
            if "mydramalist.com" in url:
                syn = extract_synopsis_mdl(html)
            elif "asianwiki.com" in url:
                syn = extract_synopsis_asianwiki(html)
            elif "wikipedia.org" in url:
                syn = extract_synopsis_wikipedia(html)
            else:
                syn = extract_synopsis_generic(html)

            if syn and not best_syn:
                best_syn = normalize_synopsis(syn)

            # Try to parse duration
            if not best_dur:
                d = try_extract_duration(html)
                if d:
                    best_dur = d

            # Early exit if both found
            if best_syn and best_dur:
                break

        if best_syn and best_dur:
            break

        time.sleep(random.uniform(1.0, 2.0))

    return best_syn, best_dur


# =====================================================
# EXCEL → OBJECTS (FIELD MAPPING & RULES)
# =====================================================

def map_show_id(no_val, sheet_name: str) -> Optional[int]:
    try:
        base = int(no_val)
    except Exception:
        return None
    name = (sheet_name or "").strip().lower()
    if name == "sheet1":
        return 1000 + base
    if name == "sheet2":
        return 2000 + base
    if name == "sheet3":
        return 3000 + base
    return base

def infer_country(native_language: Optional[str]) -> Optional[str]:
    if not native_language:
        return None
    s = native_language.strip().lower()
    if s == "korean":
        return "South Korea"
    if s == "chinese":
        return "China"
    return None

def normalize_comments(val) -> Optional[str]:
    if pd.isna(val) or str(val).strip() == "":
        return None
    t = clean_spaces(str(val))
    t = cap_each_word(t)
    if not t.endswith("."):
        t += "."
    return t

def split_array(val, capitalize: bool = False) -> List[str]:
    if pd.isna(val):
        return []
    parts = [p.strip() for p in str(val).split(",")]
    parts = [p for p in parts if p]
    if capitalize:
        parts = [p[:1].upper() + p[1:].lower() if p else p for p in parts]
    return parts

def find_again_start(df_columns: List[str]) -> int:
    for i, c in enumerate(df_columns):
        if "again" in c.lower() and "watch" in c.lower():
            return i
    # fallback: if there is a "Again Watched Date" style column with 'again'
    for i, c in enumerate(df_columns):
        if "again" in c.lower():
            return i
    raise ValueError("No 'Again Watched' column detected")

def excel_to_objects(excel_file: str, sheet_name: str) -> List[dict]:
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    cols = [str(c).strip() for c in df.columns]
    df.columns = cols

    again_idx = find_again_start(cols)

    out: List[dict] = []
    for _, row in df.iterrows():
        obj: Dict = {}

        # 1. showID (sheet-based)
        obj["showID"] = map_show_id(row.get("No"), sheet_name)

        # 2. showName
        obj["showName"] = clean_spaces(row.get("Series Title", ""))

        # 3. showImage → filled after download (absolute URL)
        obj["showImage"] = None

        # 4–5. watch dates
        obj["watchStartedOn"] = format_date(row.get("Started Date"))
        obj["watchEndedOn"] = format_date(row.get("Finished Date"))

        # 6–7. year, episodes
        obj["releasedYear"] = int(row.get("Year")) if pd.notna(row.get("Year")) else None
        obj["totalEpisodes"] = int(row.get("Total Episodes")) if pd.notna(row.get("Total Episodes")) else None

        # 8. showType
        obj["showType"] = "Mini Drama" if sheet_name.strip().lower() == "mini drama" else "Drama"

        # 9–10. languages (capitalized)
        native_lang = row.get("Original Language")
        watched_lang = row.get("Language")
        obj["nativeLanguage"] = clean_spaces(native_lang).capitalize() if pd.notna(native_lang) else None
        obj["watchedLanguage"] = clean_spaces(watched_lang).capitalize() if pd.notna(watched_lang) else None

        # 11. country
        obj["country"] = infer_country(obj["nativeLanguage"])

        # 12. comments
        obj["comments"] = normalize_comments(row.get("Comments"))

        # 13. ratings
        try:
            obj["ratings"] = int(row.get("Ratings")) if pd.notna(row.get("Ratings")) else 0
        except Exception:
            obj["ratings"] = 0

        # 14. genres (array, capitalized)
        obj["genres"] = split_array(row.get("Catagory", row.get("Category")), capitalize=True)

        # 15. network (array)
        obj["network"] = split_array(row.get("Original Network"), capitalize=False)

        # 16. againWatchedDates (G..end)
        dates = []
        for v in row[again_idx:]:
            d = format_date(v)
            if d:
                dates.append(d)
        obj["againWatchedDates"] = dates

        # 17. updatedOn (IST)
        obj["updatedOn"] = ist_today_str()

        # 18. updatedDetails → default here; will revise in update step if changed
        obj["updatedDetails"] = "First time Uploaded"

        # 19. synopsis & duration (attempt scraping)
        syn, dur = fetch_synopsis_and_duration(obj["showName"], obj["releasedYear"], obj["network"])
        obj["synopsis"] = syn or None
        obj["Duration"] = dur if isinstance(dur, int) else None

        # topRatings
        obj["topRatings"] = (obj["ratings"] or 0) * len(obj["againWatchedDates"]) * 100

        # Download cover image last (uses networks as hints) → ABSOLUTE URL
        img_path = download_cover_image(obj.get("showName") or "", obj.get("releasedYear"), obj.get("network") or [])
        if img_path:
            rel_path = os.path.relpath(img_path, ".").replace("\\", "/")
            obj["showImage"] = GITHUB_PAGES_URL + rel_path
        else:
            obj["showImage"] = None

        # Order keys as requested
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

        out.append(ordered)

    return out


# =====================================================
# CHANGE TRACKING / BACKUPS
# =====================================================

SIGNIFICANT_FIELDS = [
    "showName", "showImage", "releasedYear", "totalEpisodes",
    "comments", "ratings", "genres", "Duration", "synopsis"
]

def summarize_change(old: dict, new: dict) -> str:
    """Return a concise <=30 char message for the first significant change."""
    for f, msg in [
        ("showName", "Title updated"),
        ("showImage", "Image updated"),
        ("releasedYear", "Year updated"),
        ("totalEpisodes", "Episodes updated"),
        ("comments", "Comments updated"),
        ("ratings", "Ratings updated"),
        ("genres", "Genres updated"),
        ("Duration", "Duration updated"),
        ("synopsis", "Synopsis updated"),
    ]:
        if old.get(f) != new.get(f):
            return msg
    return "Record updated"

def update_json_from_excel(excel_file: str, json_file: str, sheet_names: List[str]) -> None:
    ensure_dirs()

    # Build NEW data from all sheets
    new_data: List[dict] = []
    available_sheets = pd.ExcelFile(excel_file).sheet_names
    for sheet in sheet_names:
        if sheet not in available_sheets:
            print(f"⚠️ Skipping missing sheet: {sheet}")
            continue
        new_data.extend(excel_to_objects(excel_file, sheet))

    # Load OLD data if exists
    if os.path.exists(json_file):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                old_data = json.loads(content) if content else []
        except (json.JSONDecodeError, ValueError):
            print(f"⚠️ Warning: {json_file} invalid. Starting fresh.")
            old_data = []
    else:
        old_data = []

    old_dict = {item["showID"]: item for item in old_data if item.get("showID") is not None}
    new_dict = {item["showID"]: item for item in new_data if item.get("showID") is not None}

    changed_objects = []

    # Update/insert
    for sid, new_obj in new_dict.items():
        if sid in old_dict:
            old_obj = old_dict[sid]
            if old_obj != new_obj:
                # Backup old before replacing
                changed_objects.append(old_obj)
                # Update 'updatedOn' & 'updatedDetails' concisely
                new_obj["updatedOn"] = ist_today_str()
                note = summarize_change(old_obj, new_obj)
                # Keep within 30 chars
                new_obj["updatedDetails"] = note[:30]
                old_dict[sid] = new_obj
        else:
            # New record
            old_dict[sid] = new_obj
            # first time note already set in creation

    # Deletions: if missing in NEW, backup old and remove
    for sid in list(old_dict.keys()):
        if sid not in new_dict:
            changed_objects.append(old_dict[sid])
            del old_dict[sid]

    # Final merge sorted by showID
    merged_data = sorted(old_dict.values(), key=lambda x: (x.get("showID") or 0))

    # Write JSON
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=4, ensure_ascii=False)

    # Backup changed (if any) in one timestamped file
    if changed_objects:
        ts = timestamp_fname()
        backup_path = os.path.join(BACKUPS_DIR, f"{ts}.json")
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(changed_objects, f, indent=4, ensure_ascii=False)
        print(f"✅ JSON updated. Old/Deleted moved to {backup_path}")
    else:
        if not old_data:
            print(f"✅ JSON created at {json_file}")
        else:
            print("✅ No changes detected")


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    # Change the excel file name if needed (e.g., "local-data.xlsx")
    EXCEL_FILE = "local-data.xlsx"
    JSON_FILE = "seriesData.json"
    SHEETS = ["Sheet1", "Sheet2", "Mini Drama"]  # include any that exist

    update_json_from_excel(EXCEL_FILE, JSON_FILE, SHEETS)
