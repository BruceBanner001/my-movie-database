# ============================================================
# Script: create_update_backup_delete.py
# Author: [BruceBanner001]
# Description:
#   v16.0 Engine.
#   - Professional-grade multi-file database system.
#   - Intelligent scraping and caching.
#   - Full support for ENGLISH dramas via IMDb.
#   - FIX: Robust Cast & Synopsis extraction (AsianWiki/MDL).
#
# Version: v5.8 (Data Integrity Fix)
# ============================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# --------------------------- VERSION & CONFIG ------------------------
SCRIPT_VERSION = "v5.8"

JSON_OBJECT_TEMPLATE = {
    "showID": None, "showName": None, "otherNames": [], "showImage": None,
    "watchStartedOn": None, "watchEndedOn": None, "releasedYear": 0,
    "releaseDate": None, "totalEpisodes": 0, "showType": None,
    "nativeLanguage": None, "watchedLanguage": None, "country": None,
    "comments": None, "ratings": 0, "genres": [], "network": [],
    "againWatchedDates": [], "updatedOn": None, "updatedDetails": None,
    "synopsis": None, "topRatings": 0, "Duration": None,
    "director": [], "tags": [], "cast": [], "extendedCastInfo": {},
    "sitePriorityUsed": {"showImage": None, "releaseDate": None, "otherNames": None, "Duration": None, "synopsis": None, "director": None, "tags": None, "cast": None}
}

SITE_PRIORITY_BY_LANGUAGE = {
    "korean": { "synopsis": "asianwiki", "showImage": "asianwiki", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "asianwiki", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist" },
    "chinese": { "synopsis": "mydramalist", "showImage": "mydramalist", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "mydramalist", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist" },
    "japanese": { "synopsis": "asianwiki", "showImage": "asianwiki", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "asianwiki", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist" },
    "thai": { "synopsis": "mydramalist", "showImage": "mydramalist", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "mydramalist", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist" },
    "taiwanese": { "synopsis": "mydramalist", "showImage": "mydramalist", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "mydramalist", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist" },
    "english": { "synopsis": "imdb", "showImage": "imdb", "otherNames": "imdb", "Duration": "imdb", "releaseDate": "imdb", "director": "imdb", "tags": "imdb", "cast": "imdb" },
    "default": { "synopsis": "mydramalist", "showImage": "asianwiki", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "asianwiki", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist" }
}

FIELD_NAME_MAP = { "showID": "Show ID", "showName": "Show Name", "otherNames": "Other Names", "showImage": "Show Image", "watchStartedOn": "Watch Started On", "watchEndedOn": "Watch Ended On", "releasedYear": "Released Year", "releaseDate": "Release Date", "totalEpisodes": "Total Episodes", "showType": "Show Type", "nativeLanguage": "Native Language", "watchedLanguage": "Watched Language", "country": "Country", "comments": "Comments", "ratings": "Ratings", "genres": "Category", "network": "Network", "againWatchedDates": "Again Watched Dates", "updatedOn": "Updated On", "updatedDetails": "Updated Details", "synopsis": "Synopsis", "topRatings": "Top Ratings", "Duration": "Duration", "director": "Director", "tags": "Tags", "cast": "Cast", "extendedCastInfo": "Extended Cast Info", "sitePriorityUsed": "Site Priority Used" }
LOCKED_FIELDS_AFTER_CREATION = {'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'director', 'tags', 'cast', 'extendedCastInfo', 'updatedOn', 'updatedDetails', 'sitePriorityUsed', 'topRatings'}

# ---------------------------- IMPORTS & GLOBALS ----------------------------
import os, re, sys, json, io, shutil, traceback, copy, time
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import pandas as pd
import requests
from bs4 import BeautifulSoup

DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "true").lower() == "true" 

# --- ROBUST IMPORT FOR DDGS ---
HAVE_DDGS = False
DDGS_ERROR = None
try: 
    from ddgs import DDGS
    HAVE_DDGS = True
except ImportError as e:
    DDGS_ERROR = str(e)
    try:
        from duckduckgo_search import DDGS
        HAVE_DDGS = True
    except ImportError:
        HAVE_DDGS = False

try: import cloudscraper; HAVE_SCRAPER = True
except Exception: HAVE_SCRAPER = False
try: from PIL import Image; HAVE_PIL = True
except Exception: HAVE_PIL = False
try: from google.oauth2 import service_account; from googleapiclient.discovery import build; from googleapiclient.http import MediaIoBaseDownload; HAVE_GOOGLE_API = True
except Exception: HAVE_GOOGLE_API = False

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def filename_timestamp(): return now_ist().strftime("%d_%B_%Y_%H%M")
def run_id_timestamp(): return now_ist().strftime("RUN_%Y%m%d_%H%M%S")

# --- FILE & FOLDER STRUCTURE ---
SERIES_JSON_FILE = "seriesData.json"
ARTISTS_JSON_FILE = "artists.json"
EXTENDED_CAST_JSON_FILE = "extendedCast.json"
ARTIST_LOOKUP_FILE = "artists_lookup.json"

BACKUP_DIR, SHOW_IMAGES_DIR, ARTIST_IMAGES_DIR, DELETE_IMAGES_DIR = "backups", "show-images", "artist-images", "deleted-images"
DELETED_DATA_DIR, REPORTS_DIR, BACKUP_META_DIR = "deleted-data", "reports", "backup-meta-data"
ARCHIVED_BACKUPS_DIR, ARCHIVED_META_DIR = "archived-backups", "archived-backup-meta-data"

SERVICE_ACCOUNT_FILE, EXCEL_FILE_ID_TXT = "GDRIVE_SERVICE_ACCOUNT.json", "EXCEL_FILE_ID.txt"
SCRAPER = cloudscraper.create_scraper() if HAVE_SCRAPER else requests.Session()
SCRAPER.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})

LANG_TO_COUNTRY_MAP = {"korean": "South Korea", "chinese": "China", "japanese": "Japan", "thai": "Thailand", "taiwanese": "Taiwan", "english": "USA"}

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
        if normalize_list(old.get(k)) != normalize_list(new.get(k)): return True
    return False

def _clean_other_names(names_list):
    if not names_list: return []
    unique_names = []
    seen = set()
    for name in names_list:
        clean = name.strip()
        if len(clean) < 2: continue
        if clean.lower() not in seen:
            seen.add(clean.lower())
            unique_names.append(clean)
    return unique_names

def _validate_page_title(soup, expected_name, site):
    try:
        page_title = ""
        if site == "asianwiki":
            h1 = soup.find('h1', class_='firstHeading') or soup.find('h1')
            if h1: page_title = h1.get_text(strip=True)
        elif site == "mydramalist":
            h1 = soup.find('h1', class_='film-title') or soup.find('h1')
            if h1: page_title = h1.get_text(strip=True)
        elif site == "imdb":
            h1 = soup.find('h1')
            if h1: page_title = h1.get_text(strip=True)

        if not page_title: return True
        t1 = re.sub(r'\(\d{4}\)', '', page_title).lower().strip()
        t2 = re.sub(r'\(\d{4}\)', '', expected_name).lower().strip()
        if site == "imdb" and t2 in t1: return True

        ratio = SequenceMatcher(None, t1, t2).ratio()
        if ratio < 0.4 and t2 not in t1 and t1 not in t2:
            logd(f"Title Validation FAILED: Page Title '{page_title}' vs Expected '{expected_name}' (Ratio: {ratio:.2f})")
            return False
        logd(f"Title Validation PASSED: '{page_title}' matches '{expected_name}'")
        return True
    except Exception as e:
        logd(f"Title validation error: {e}")
        return True

def _scrape_country(soup, site):
    try:
        if site == 'asianwiki':
            tag = soup.find('b', string='Country:')
            if tag and tag.parent: return tag.parent.get_text(strip=True).replace('Country:', '').strip()
        elif site == 'mydramalist':
            tag = soup.find('b', string='Country:')
            if tag and tag.parent: return tag.parent.get_text(strip=True).replace('Country:', '').strip()
    except Exception: pass
    return None

def get_soup_from_search(show_name, show_year, site, language, soup_cache):
    cache_key = f"{show_name}_{show_year}_{site}_{language}"
    if cache_key in soup_cache:
        data = soup_cache[cache_key]
        if data[0] is None:
            logd(f"Cache hit: No results found previously for '{show_name}' on {site}.com")
        else:
            logd(f"Found soup in cache for '{show_name}' on {site}.com")
        return data

    expected_country = LANG_TO_COUNTRY_MAP.get(language.lower())
    logd(f"Initiating search for: '{show_name} ({show_year})' on {site}.com (Expected Country: {expected_country})")
    
    if not HAVE_DDGS:
        logd(f"DDGS library not available. Import Error: {DDGS_ERROR}")
        return None, None

    search_queries = [ f'"{show_name}" {show_year} {language} site:{site}.com', f'"{show_name}" {show_year} site:{site}.com', f'"{show_name}" site:{site}.com' ]
    if site == "imdb":
        search_queries = [f'"{show_name}" {show_year} site:imdb.com', f'"{show_name}" TV Series {show_year} site:imdb.com']

    for query in search_queries:
        logd(f"Executing search query: {query}")
        try:
            time.sleep(2.0)
            with DDGS() as dd:
                results = list(dd.text(query, max_results=5))
                if not results: continue

                for res in results:
                    url = res.get('href', '')
                    if not url or 'bing.com' in url or any(bad in url for bad in ['/reviews', '/episode', '/cast', '/recs', '?lang=', '/photos', '/video', '/trivia']): continue
                    if site == "asianwiki" and ("/File:" in url or "/index.php?title=File:" in url): logd(f"Rejecting invalid AsianWiki file URL: {url}"); continue
                    if site == "imdb" and "/title/tt" not in url: continue 

                    logd(f"Found candidate URL: {url}")
                    r = SCRAPER.get(url, timeout=15)
                    if r.status_code == 200:
                        soup = BeautifulSoup(r.text, "html.parser")
                        is_valid_landmark = False
                        if site == "asianwiki":
                            if soup.find(id='Profile'): is_valid_landmark = True
                        elif site == "mydramalist":
                            if soup.find('div', class_='box-body'): is_valid_landmark = True
                        elif site == "imdb":
                            if soup.find('h1') or soup.find(attrs={"type": "application/ld+json"}): is_valid_landmark = True
                        
                        if is_valid_landmark:
                            if expected_country and site != 'imdb':
                                scraped_country = _scrape_country(soup, site)
                                if scraped_country and expected_country in scraped_country: logd(f"Country validation passed.")
                                else: logd(f"Country validation FAILED. Rejecting page."); continue
                            
                            if not _validate_page_title(soup, show_name, site): continue

                            soup_cache[cache_key] = (soup, url)
                            return soup, url
                    else: logd(f"HTTP Error {r.status_code} for {url}")
        except Exception as e: logd(f"Search attempt failed for query '{query}': {e}")

    soup_cache[cache_key] = (None, None)
    return None, None

def download_and_save_image(url, local_path, is_artist=False):
    if not HAVE_PIL: logd("Pillow library not installed."); return False
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        url = re.sub(r'_[24]c\.jpg$', '.jpg', url) if not is_artist else url
        logd(f"Downloading image from: {url}")
        r = SCRAPER.get(url, stream=True, timeout=20)
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
            with Image.open(r.raw) as img:
                img = img.convert("RGB")
                size = (400, 600) if is_artist else (800, 1200)
                img.thumbnail(size, Image.LANCZOS)
                img.save(local_path, "JPEG", quality=90)
                logd(f"Image saved to {local_path}"); return True
    except Exception as e: logd(f"Image download failed: {e}")
    return False

# --- ASIANWIKI SCRAPERS ---
def _scrape_synopsis_from_asianwiki(soup, **kwargs):
    try:
        # Improved regex to catch "Plot Synopsis" or "Synopsis" more aggressively
        headers = soup.find_all(['h2', 'h3'], string=re.compile(r"(Plot|Synopsis)", re.IGNORECASE))
        target_header = None
        for h in headers:
            if h.name == 'h2': target_header = h; break 
        if not target_header and headers: target_header = headers[0] 
        
        if not target_header:
            elm = soup.find(id=re.compile(r"(Synopsis|Plot)", re.IGNORECASE))
            if elm: target_header = elm

        if not target_header: return None
        
        content = []
        for sibling in target_header.next_siblings:
            if sibling.name == 'h2': break 
            
            text = ""
            if sibling.name in ['p', 'div']:
                text = sibling.get_text(strip=True)
            elif isinstance(sibling, str):
                text = sibling.strip()
            
            # Robust: Capture text if it's long enough, skipping ads/spaces
            if text and len(text) > 50: 
                content.append(text)
                
        return "\n\n".join(content) if content else None
    except Exception as e: logd(f"AsianWiki Synopsis Error: {e}"); return None

def _scrape_image_from_asianwiki(soup, **kwargs):
    try:
        img = soup.select_one('a.image > img[src]')
        if not img: return None
        img_url = requests.compat.urljoin("https://asianwiki.com", img['src'])
        image_path = os.path.join(SHOW_IMAGES_DIR, f"{kwargs['sid']}.jpg")
        if download_and_save_image(img_url, image_path):
            return os.path.basename(image_path)
    except Exception: return None

def _scrape_othernames_from_asianwiki(soup, **kwargs):
    try:
        p_tag = soup.find('p', string=re.compile(r"^(Drama:|Movie:)"))
        if p_tag:
            full_text = p_tag.get_text(strip=True).replace(" Hangul:", " (Hangul:")
            match = re.search(r':(.*?)(?=\(Revised romanization:|\(literal title\)|$)', full_text, re.DOTALL)
            if match:
                names_text = match.group(1).strip()
                raw_names = [name.strip() for name in names_text.split('/') if name.strip()]
                filtered = [name for name in raw_names if name.lower() != kwargs['show_name'].lower()]
                return _clean_other_names(filtered)
        return None
    except Exception: return None

def _scrape_release_date_from_asianwiki(soup, **kwargs):
    try:
        b_tag = soup.find('b', string=re.compile(r"Release Date:"))
        if b_tag and (parent := b_tag.parent):
            b_tag.decompose(); return parent.get_text(strip=True)
        return None
    except Exception: return None

# --- MYDRAMALIST SCRAPERS ---
def _scrape_synopsis_from_mydramalist(soup, **kwargs):
    try:
        synopsis_div = soup.select_one('div.show-synopsis, div[itemprop="description"]')
        if not synopsis_div: return None
        paragraphs = []
        for element in synopsis_div.find_all(['p', 'br'], recursive=False):
            if element.name == 'br':
                if paragraphs and paragraphs[-1] != "": paragraphs.append("")
            else:
                text = element.get_text(strip=True)
                if text: paragraphs.append(text)
        if not paragraphs:
            text = synopsis_div.get_text(separator='\n', strip=True)
            paragraphs = [line.strip() for line in text.split('\n') if line.strip()]
        synopsis = "\n\n".join(paragraphs)
        patterns_to_remove = [ r'\s*\(Source:.*?\)\s*$', r'\s*Source:.*$', r'~~.*?~~', r'\s*Edit Translation\s*$', r'\s*(Additional Cast Members|Native title|Also Known As):.*$', r'^\s*Remove ads\s*' ]
        cleaned_synopsis = synopsis
        for pattern in patterns_to_remove:
            cleaned_synopsis = re.sub(pattern, '', cleaned_synopsis, flags=re.IGNORECASE | re.DOTALL).strip()
        return cleaned_synopsis if cleaned_synopsis else None
    except Exception as e: logd(f"MDL Synopsis Error: {e}"); return None

def _scrape_image_from_mydramalist(soup, **kwargs):
    try:
        img = soup.select_one('.film-cover img, .cover img')
        if not img: return None
        url = img.get('src') or img.get('data-src') or img.get('data-original')
        if not url: return None
        image_path = os.path.join(SHOW_IMAGES_DIR, f"{kwargs['sid']}.jpg")
        if download_and_save_image(url, image_path):
            return os.path.basename(image_path)
    except Exception: return None

def _scrape_othernames_from_mydramalist(soup, **kwargs):
    try:
        b_tag = soup.find('b', string="Also Known As:")
        if b_tag and (li_tag := b_tag.find_parent('li')):
            b_tag.decompose()
            names_text = li_tag.get_text(strip=True)
            raw_names = [name.strip() for name in names_text.split(',') if name.strip()]
            filtered = [name for name in raw_names if name.lower() != kwargs['show_name'].lower()]
            return _clean_other_names(filtered)
        return None
    except Exception: return None

def _scrape_duration_from_mydramalist(soup, **kwargs):
    try:
        b_tag = soup.find('b', string=re.compile(r"Duration:"))
        if b_tag and (li_tag := b_tag.find_parent('li')):
            b_tag.decompose()
            duration_text = li_tag.get_text(strip=True)
            return duration_text.replace(" min.", " mins") if "hr" not in duration_text else duration_text
        return None
    except Exception: return None

def _scrape_release_date_from_mydramalist(soup, **kwargs):
    try:
        b_tag = soup.find('b', string=re.compile(r"Aired:"))
        if b_tag and (li_tag := b_tag.find_parent('li')):
            b_tag.decompose(); return li_tag.get_text(strip=True)
        return None
    except Exception: return None

def _scrape_director_from_mydramalist(soup, **kwargs):
    try:
        b_tag = soup.find('b', string="Director:")
        if b_tag and (li_tag := b_tag.find_parent('li')):
            b_tag.decompose()
            names_text = li_tag.get_text(strip=True)
            return [name.strip() for name in names_text.split(',') if name.strip()]
        return None
    except Exception: return None

def _scrape_tags_from_mydramalist(soup, **kwargs):
    try:
        tags_li = soup.select_one('li.show-tags')
        if tags_li:
            return [a.get_text(strip=True) for a in tags_li.find_all('a') if "(Vote tags)" not in a.get_text()]
        return None
    except Exception: return None

def _scrape_cast_from_mydramalist(soup, **kwargs):
    try:
        full_cast_raw = []
        cast_box = soup.find('h3', string=re.compile(r'Cast & Credits', re.IGNORECASE))
        cast_container = None
        if cast_box:
            box = cast_box.find_parent('div', class_='box')
            if box: cast_container = box
        if not cast_container: cast_container = soup
        
        cast_items = cast_container.select('li.list-item')
        if not cast_items: cast_items = cast_container.select('div.cast-list div.col-xs-8, div.cast-list div.col-sm-6')
        if not cast_items: cast_items = cast_container.select('.p-a-0 li')
        
        if not cast_items: return None

        main_role_count = 0
        for i, item in enumerate(cast_items):
            try:
                artist_name = None
                artist_link = None
                anchors = item.select('a')
                for a in anchors:
                    if '/people/' in a.get('href', '') and a.get_text(strip=True):
                        artist_name = a.get_text(strip=True)
                        artist_link = a['href']
                        break 
                
                if not artist_name: continue 
                id_match = re.search(r'/people/(\d+)-', artist_link)
                if not id_match: continue
                artist_id = id_match.group(1)
                
                img_tag = item.select_one('img')
                artist_image_url = None
                if img_tag:
                    artist_image_url = img_tag.get('src') or img_tag.get('data-src') or img_tag.get('data-original')
                    if artist_image_url and ('avatar' in artist_image_url or 'default' in artist_image_url):
                        artist_image_url = None
                
                role = "Support Role"
                character_name = "Unknown"
                small_text = item.select_one('small')
                if small_text:
                    text_content = small_text.get_text(strip=True).lower()
                    # RELAXED MATCHING
                    if "main" in text_content: role = "Main Role"
                    elif "guest" in text_content: role = "Guest Role"
                    elif "support" in text_content: role = "Support Role"
                    
                    char_text = re.sub(r'\s*(Main Role|Support Role|Guest Role)\s*', '', small_text.get_text(strip=True), flags=re.IGNORECASE).strip()
                    if char_text: character_name = char_text
                
                if role == "Main Role": main_role_count += 1
                
                full_cast_raw.append({"artistID": artist_id, "artistName": artist_name, "artistImageURL": artist_image_url, "characterName": character_name, "role": role})
            except Exception: continue
        
        if not full_cast_raw: return None
        
        # FAILSAFE: If no main roles detected but we have actors, assume first 6 are Main
        if main_role_count == 0 and len(full_cast_raw) > 0:
            logd("No Main Roles detected via text. Applying fallback: Top 6 actors set to Main Role.")
            for i in range(min(6, len(full_cast_raw))):
                full_cast_raw[i]['role'] = "Main Role"

        if 'context' in kwargs and 'source_links_temp' in kwargs['context']:
            kwargs['context']['source_links_temp']['raw_cast'] = full_cast_raw
        return full_cast_raw
    except Exception as e: logd(f"Cast Scrape Error: {e}"); return None

# --- IMDB SCRAPERS ---
def _get_imdb_json_ld(soup):
    try:
        script = soup.find('script', type='application/ld+json')
        if script: return json.loads(script.string)
    except Exception: pass
    return None

def _scrape_synopsis_from_imdb(soup, **kwargs):
    try:
        data = _get_imdb_json_ld(soup)
        if data and 'description' in data: return data['description']
        desc = soup.find(attrs={"data-testid": "plot-xl"})
        if desc: return desc.get_text(strip=True)
    except Exception: pass
    return None

def _scrape_image_from_imdb(soup, **kwargs):
    try:
        data = _get_imdb_json_ld(soup)
        if data and 'image' in data:
            url = data['image']
            image_path = os.path.join(SHOW_IMAGES_DIR, f"{kwargs['sid']}.jpg")
            if download_and_save_image(url, image_path):
                return os.path.basename(image_path)
    except Exception: pass
    return None

def _scrape_release_date_from_imdb(soup, **kwargs):
    try:
        data = _get_imdb_json_ld(soup)
        if data and 'datePublished' in data: return data['datePublished']
    except Exception: pass
    return None

def _scrape_director_from_imdb(soup, **kwargs):
    try:
        data = _get_imdb_json_ld(soup)
        if data and 'director' in data:
            directors = data['director']
            if isinstance(directors, list): return [d['name'] for d in directors if 'name' in d]
            elif 'name' in directors: return [directors['name']]
    except Exception: pass
    return None

def _scrape_cast_from_imdb(soup, **kwargs):
    try:
        full_cast_raw = []
        data = _get_imdb_json_ld(soup)
        
        if data and 'actor' in data:
            for actor in data['actor']:
                if 'name' not in actor: continue
                artist_id = str(hash(actor['name']))[-8:] 
                if 'url' in actor:
                    match = re.search(r'/name/(nm\d+)', actor['url'])
                    if match: artist_id = match.group(1)
                
                full_cast_raw.append({
                    "artistID": artist_id,
                    "artistName": actor['name'],
                    "artistImageURL": None,
                    "characterName": "Unknown",
                    "role": "Main Role"
                })
        
        if not full_cast_raw:
            cards = soup.select('div[data-testid="title-cast-item"]')
            for card in cards:
                try:
                    a_tag = card.select_one('a[data-testid="title-cast-item__actor"]')
                    if not a_tag: continue
                    name = a_tag.get_text(strip=True)
                    url = a_tag['href']
                    artist_id = re.search(r'(nm\d+)', url).group(1)
                    img_tag = card.select_one('img')
                    img_url = img_tag['src'] if img_tag else None
                    char_div = card.select_one('a[data-testid="cast-item-characters-link"]')
                    char_name = char_div.get_text(strip=True) if char_div else "Unknown"
                    
                    full_cast_raw.append({
                        "artistID": artist_id,
                        "artistName": name,
                        "artistImageURL": img_url,
                        "characterName": char_name,
                        "role": "Main Role"
                    })
                except Exception: continue

        if full_cast_raw:
             if 'context' in kwargs and 'source_links_temp' in kwargs['context']:
                kwargs['context']['source_links_temp']['raw_cast'] = full_cast_raw
             return full_cast_raw
             
    except Exception: pass
    return None

def _scrape_tags_from_imdb(soup, **kwargs):
    try:
        data = _get_imdb_json_ld(soup)
        if data and 'genre' in data:
            return data['genre'] if isinstance(data['genre'], list) else [data['genre']]
    except Exception: pass
    return None

SCRAPE_MAP = {
    'asianwiki': {'synopsis': _scrape_synopsis_from_asianwiki, 'showImage': _scrape_image_from_asianwiki, 'otherNames': _scrape_othernames_from_asianwiki, 'Duration': lambda **kwargs: None, 'releaseDate': _scrape_release_date_from_asianwiki, 'director': lambda **kwargs: None, 'tags': lambda **kwargs: None, 'cast': lambda **kwargs: None},
    'mydramalist': {'synopsis': _scrape_synopsis_from_mydramalist, 'showImage': _scrape_image_from_mydramalist, 'otherNames': _scrape_othernames_from_mydramalist, 'Duration': _scrape_duration_from_mydramalist, 'releaseDate': _scrape_release_date_from_mydramalist, 'director': _scrape_director_from_mydramalist, 'tags': _scrape_tags_from_mydramalist, 'cast': _scrape_cast_from_mydramalist},
    'imdb': {'synopsis': _scrape_synopsis_from_imdb, 'showImage': _scrape_image_from_imdb, 'otherNames': lambda **kwargs: None, 'Duration': lambda **kwargs: None, 'releaseDate': _scrape_release_date_from_imdb, 'director': _scrape_director_from_imdb, 'tags': _scrape_tags_from_imdb, 'cast': _scrape_cast_from_imdb}
}

def fetch_and_populate_metadata(obj, context, artists_db):
    s_id, s_name, s_year, lang = obj['showID'], obj['showName'], obj['releasedYear'], obj.get("nativeLanguage", "")
    priority = SITE_PRIORITY_BY_LANGUAGE.get(lang.lower(), SITE_PRIORITY_BY_LANGUAGE['default'])
    spu = obj.setdefault('sitePriorityUsed', {})
    
    context['source_links_temp'] = {}
    
    soup_cache = {}
    fields_to_check = [ 'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'director', 'tags', 'cast' ]
    
    for field in fields_to_check:
        if not obj.get(field):
            site_to_use = priority.get(field)
            if not site_to_use: continue
            
            search_terms = [s_name, re.sub(r'\s*\(?Season\s*\d+\)?', '', s_name, flags=re.IGNORECASE).strip()]
            soup, url = None, None
            for term in set(search_terms):
                soup, url = get_soup_from_search(term, s_year, site_to_use, lang, soup_cache)
                if soup: break
            
            if soup:
                scrape_args = {'soup': soup, 'sid': s_id, 'show_name': s_name, 'context': context, 'artists_db': artists_db}
                data = SCRAPE_MAP[site_to_use][field](**scrape_args)
                if data:
                    obj[field] = data
                    spu[field] = site_to_use 
                    context['source_links_temp'][field] = url
                            
    return obj

def process_deletions(excel, context):
    try: df = pd.read_excel(excel, sheet_name='Deleting Records')
    except ValueError: print("INFO: 'Deleting Records' sheet not found. Skipping deletion step."); return
    if df.empty: logd("'Deleting Records' sheet is empty. Nothing to delete."); return
    
    series_data = load_json_file(SERIES_JSON_FILE)
    extended_cast_data = load_json_file(EXTENDED_CAST_JSON_FILE)
    
    series_by_id = {int(o['showID']): o for o in series_data if o.get('showID')}
    to_delete = set(pd.to_numeric(df.iloc[:, 0], errors='coerce').dropna().astype(int))
    deleted_count = 0
    
    for sid in to_delete:
        sid_str = str(sid)
        if sid in series_by_id:
            show_obj = series_by_id.pop(sid)
            extended_cast_obj = extended_cast_data.pop(sid_str, None)
            
            ts = filename_timestamp()
            archive_bundle = { "deletedOn": ts, "showData": show_obj }
            if extended_cast_obj: archive_bundle["extendedCastData"] = extended_cast_obj

            path = os.path.join(DELETED_DATA_DIR, f"DELETED_{ts}_{sid}.json"); os.makedirs(DELETED_DATA_DIR, exist_ok=True)
            save_json_file(path, archive_bundle)
            context['files_generated']['deleted_data'].append(path)
            context['report_data'].setdefault('Deleting Records', {}).setdefault('data_deleted', []).append(f"- {sid} -> {show_obj.get('showName')} ({show_obj.get('releasedYear')}) -> ✅ Deleted")
            
            if show_obj.get('showImage'):
                img_name = os.path.basename(show_obj['showImage'])
                src = os.path.join(SHOW_IMAGES_DIR, img_name)
                if os.path.exists(src):
                    dest = os.path.join(DELETE_IMAGES_DIR, f"DELETED_{ts}_{sid}.jpg"); os.makedirs(DELETE_IMAGES_DIR, exist_ok=True); shutil.move(src, dest)
                    context['files_generated']['deleted_images'].append(dest)
            
            for d in [BACKUP_DIR, BACKUP_META_DIR]:
                for f in os.listdir(d) if os.path.exists(d) else []:
                    if f.endswith(f"_{sid}.json"):
                        archive_dir = os.path.join(ARCHIVED_BACKUPS_DIR if d == BACKUP_DIR else ARCHIVED_META_DIR, sid_str); os.makedirs(archive_dir, exist_ok=True)
                        src_path = os.path.join(d, f); dest_path = os.path.join(archive_dir, f); shutil.move(src_path, dest_path)
                        context['files_generated']['archived_backups' if d == BACKUP_DIR else 'archived_meta_backups'].append(dest_path)
            deleted_count += 1

    if deleted_count > 0:
        save_json_file(SERIES_JSON_FILE, sorted(list(series_by_id.values()), key=lambda x: x.get('showID', 0)))
        save_json_file(EXTENDED_CAST_JSON_FILE, extended_cast_data)

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
                image_path = os.path.join(SHOW_IMAGES_DIR, f"{sid}.jpg")
                if key == 'showImage' and download_and_save_image(val, image_path):
                    val = os.path.basename(image_path); context['files_generated']['show_images'].append(image_path)
                elif key == 'otherNames': val = normalize_list(val)
                else: val = str(val).strip()
                if obj.get(key) != val: changed[key] = {'old': obj.get(key), 'new': val}; obj[key] = val; obj.setdefault('sitePriorityUsed', {})[key] = "Manual"
        if changed:
            obj['updatedDetails'] = f"{', '.join([human_readable_field(f) for f in changed])} Updated Manually"; obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            report.setdefault('updated', []).append({'old': old, 'new': obj}); create_diff_backup(old, obj, context)
    return report

def excel_to_objects(excel, sheet):
    try: df = pd.read_excel(excel, sheet_name=sheet, keep_default_na=False); df.columns = [c.strip().lower() for c in df.columns]
    except ValueError: print(f"INFO: Sheet '{sheet}' not found. Skipping."); return [], []
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
        obj["nativeLanguage"] = obj.get("nativeLanguage", "").strip().capitalize()
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
            value = source_links.get('raw_cast') if key == 'cast' else obj.get(key)
            field_data = {"value": value, "source": site}
            if key in source_links: field_data["source_link"] = source_links[key]
            fetched[key] = field_data
    
    data = {"scriptVersion": SCRIPT_VERSION, "runID": context['run_id'], "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"), "showID": obj['showID'], "showName": obj['showName']}
    if fetched: data["fetchedFields"] = fetched
    if context.get('new_artists_added'): data["newArtistsAdded"] = context.get('new_artists_added')
    
    if not fetched and not context.get('new_artists_added'): return

    path = os.path.join(BACKUP_META_DIR, f"META_{filename_timestamp()}_{obj['showID']}.json"); os.makedirs(BACKUP_META_DIR, exist_ok=True)
    save_json_file(path, data)
    context['files_generated']['meta_backups'].append(path)

def create_diff_backup(old, new, context):
    changed_fields = {}
    for key, new_val in new.items():
        if key not in LOCKED_FIELDS_AFTER_CREATION and normalize_list(old.get(key)) != normalize_list(new_val):
            changed_fields[key] = {"old": old.get(key), "new": new_val}
    if not changed_fields: return
    data = {"scriptVersion": SCRIPT_VERSION, "runID": context['run_id'], "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"), "backupType": "partial_diff", "showID": new['showID'], "showName": new['showName'], "releasedYear": new.get('releasedYear'), "updatedDetails": new.get('updatedDetails', 'Record Updated'), "changedFields": changed_fields}
    path = os.path.join(BACKUP_DIR, f"BACKUP_{filename_timestamp()}_{new['showID']}.json"); os.makedirs(BACKUP_DIR, exist_ok=True)
    save_json_file(path, data)
    context['files_generated']['backups'].append(path)

def write_report(context):
    lines = [f"✅ Workflow completed successfully", f"🆔 Run ID: {context['run_id']}", f"📅 Run Time: {now_ist().strftime('%d %B %Y %I:%M %p (IST)')}", f"🕒 Duration: {context['duration_str']}", f"⚙️ Script Version: {SCRIPT_VERSION}", ""]
    sep, stats = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", {'created': 0, 'updated': 0, 'skipped': 0, 'deleted': 0, 'warnings': 0, 'show_images': 0, 'artist_images': 0, 'rows': 0, 'refetched': 0, 'archived': 0, 'artist_img_warn': 0}
    for sheet, changes in context['report_data'].items():
        if not any(v for k, v in changes.items()): continue
        display_sheet = sheet.replace("sheet", "Sheet ").title(); lines.extend([sep, f"🗂️ === {display_sheet} — {now_ist().strftime('%d %B %Y')} ==="]); lines.append(sep)
        if changes.get('created'): lines.append("\n🆕 Data Created:"); [lines.append(f"- {o['showID']} - {o['showName']} ({o.get('releasedYear')}) -> {o.get('updatedDetails', '')}") for o in changes['created']]
        if changes.get('updated'): lines.append("\n🔁 Data Updated:"); [lines.append(f"✍️ {p['new']['showID']} - {p['new']['showName']} -> {p['new']['updatedDetails']}") for p in changes['updated']]
        if changes.get('refetched'): lines.append("\n🔍 Refetched Data:"); [lines.append(f"✨ {o['id']} - {o['name']} -> Fetched: {', '.join(o['fields'])}") for o in changes['refetched']]
        if changes.get('data_warnings'): lines.append("\n⚠️ Data Validation Warnings:"); [lines.append(i) for i in changes['data_warnings']]
        if changes.get('fetched_data'): lines.append("\n🖼️ Fetched Data Details:"); [lines.append(i) for i in sorted(changes['fetched_data'])]
        if changes.get('fetch_warnings'): lines.append("\n🕳️ Value Not Found:"); [lines.append(i) for i in sorted(changes['fetch_warnings'])]
        if changes.get('artist_image_warnings'): lines.append("\n🧑‍🎨 Artist Image Warnings:"); [lines.append(i) for i in sorted(changes['artist_image_warnings'])]
        if changes.get('skipped'): lines.append("\n🚫 Skipped (Unchanged):"); [lines.append(f"- {i}") for i in sorted(changes['skipped'])]
        if changes.get('data_deleted'): lines.append("\n❌ Data Deleted:"); [lines.append(i) for i in changes['data_deleted']]
        
        if sheet not in ["Deleting Records", "Manual Updates"]:
            s = {k: len(v) for k, v in changes.items() if isinstance(v, list)}; total = sum(s.get(k, 0) for k in ['created', 'updated', 'skipped', 'refetched'])
            stats['created'] += s.get('created', 0); stats['updated'] += s.get('updated', 0); stats['skipped'] += s.get('skipped', 0); stats['refetched'] += s.get('refetched', 0)
            stats['show_images'] += sum(1 for i in changes.get('fetched_data', []) if "Show Image" in i); stats['rows'] += total
            stats['warnings'] += len(changes.get('data_warnings', [])) + len(changes.get('fetch_warnings', [])) + len(changes.get('artist_image_warnings', []))
            stats['artist_img_warn'] += len(changes.get('artist_image_warnings', []))
            lines.extend([f"\n📊 Summary (Sheet: {display_sheet})", sep, f"🆕 Created: {s.get('created', 0)}", f"🔁 Updated: {s.get('updated', 0)}", f"🔍 Refetched: {s.get('refetched', 0)}", f"🚫 Skipped: {s.get('skipped', 0)}", f"⚠️ Warnings: {len(changes.get('data_warnings',[])) + len(changes.get('fetch_warnings',[])) + len(changes.get('artist_image_warnings', []))}", f"  Total Rows: {total}"])
        lines.append("")

    stats['deleted'] = len(context['files_generated']['deleted_data']); stats['artist_images'] = len(context['files_generated']['artist_images'])
    stats['archived'] = len(context['files_generated']['archived_backups']) + len(context['files_generated']['archived_meta_backups'])
    lines.extend([sep, "📊 Overall Summary", sep, f"🆕 Total Created: {stats['created']}", f"🔁 Total Updated: {stats['updated']}", f"🔍 Total Refetched: {stats['refetched']}", f"🖼️ Show Images Updated: {stats['show_images']}", f"🧑‍🎨 New Artist Images Added: {stats['artist_images']}", f"🚫 Total Skipped: {stats['skipped']}", f"❌ Total Deleted: {stats['deleted']}", f"🗄️ Total Archived Backups: {stats['archived']}", f"⚠️ Total Warnings: {stats['warnings']}", f"💾 Backup Files: {len(context['files_generated']['backups'])}", f"  Grand Total Rows: {stats['rows']}", "", f"💾 Metadata Backups: {len(context['files_generated']['meta_backups'])}", ""])
    for file in [SERIES_JSON_FILE, ARTISTS_JSON_FILE, EXTENDED_CAST_JSON_FILE, ARTIST_LOOKUP_FILE]:
        try:
            with open(file, 'r', encoding='utf-8') as f: lines.append(f"📦 Total Objects in {file}: {len(json.load(f))}")
        except Exception: lines.append(f"📦 Total Objects in {file}: 0")
    lines.extend([sep, "🗂️ Folders Generated:", sep])
    for folder, files in context['files_generated'].items():
        if files: lines.append(f"{folder}/"); [lines.append(f"    {os.path.basename(p)}") for p in files]
    lines.extend([sep, "🏁 Workflow finished successfully"])
    with open(context['report_file_path'], 'w', encoding='utf-8') as f: f.write("\n".join(lines))

def process_and_distribute_cast(full_cast, artists_db, context):
    main_cast, support_cast, guest_cast = [], [], []
    context['new_artists_added'] = []
    
    if not full_cast: return [], {}, {}

    for artist in full_cast:
        artist_id = artist['artistID']
        if artist_id not in artists_db:
            image_path = os.path.join(ARTIST_IMAGES_DIR, f"{artist_id}.jpg")
            image_downloaded = artist['artistImageURL'] and download_and_save_image(artist['artistImageURL'], image_path, is_artist=True)
            
            if image_downloaded:
                artists_db[artist_id] = {"artistName": artist['artistName'], "artistImage": os.path.basename(image_path)}
                context['files_generated']['artist_images'].append(image_path)
            else:
                artists_db[artist_id] = {"artistName": artist['artistName'], "artistImage": None}
            
            context['new_artists_added'].append({"artistID": artist_id, "artistName": artist['artistName'], "imageDownloaded": image_downloaded})
        
        cast_member = {"artistID": artist_id, "characterName": artist['characterName'], "role": artist['role']}
        if artist['role'] == 'Main Role': main_cast.append(cast_member)
        elif artist['role'] == 'Support Role': support_cast.append(cast_member)
        elif artist['role'] == 'Guest Role': guest_cast.append(cast_member)

    extended_cast = {}
    if support_cast: extended_cast['supportRoles'] = support_cast
    if guest_cast: extended_cast['guestRoles'] = guest_cast
    
    extended_info = { "hasSupportRoles": bool(support_cast), "supportRoleCount": len(support_cast), "hasGuestRoles": bool(guest_cast), "guestRoleCount": len(guest_cast) }
    
    return main_cast, extended_cast, extended_info

def fetch_excel_from_gdrive_bytes(file_id, creds_path):
    if not HAVE_GOOGLE_API: print("ℹ️ Google API packages not installed."); return None
    try:
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        service = build('drive', 'v3', credentials=creds)
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

def load_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): return {} if file_path in [ARTISTS_JSON_FILE, EXTENDED_CAST_JSON_FILE] else []

def save_json_file(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4, ensure_ascii=False)

def main():
    start_time = now_ist()
    context = {'run_id': run_id_timestamp(), 'start_time_iso': start_time.isoformat(), 'report_data': {}, 'current_sheet': None,
               'files_generated': {'backups': [], 'show_images': [], 'artist_images': [], 'deleted_data': [], 'deleted_images': [], 'meta_backups': [], 'reports': [], 'archived_backups': [], 'archived_meta_backups': []}}
    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)): print("❌ Missing GDrive credentials."); sys.exit(1)
    try:
        with open(EXCEL_FILE_ID_TXT, 'r') as f: excel_id = f.read().strip()
    except Exception as e: print(f"❌ Error with Excel ID file: {e}"); sys.exit(1)
    
    print(f"🚀 Running Script — Version {SCRIPT_VERSION} | Run ID: {context['run_id']}")
    excel_bytes = fetch_excel_from_gdrive_bytes(excel_id, SERVICE_ACCOUNT_FILE)
    if not excel_bytes: print("❌ Could not fetch Excel file."); sys.exit(1)

    process_deletions(io.BytesIO(excel_bytes.getvalue()), context)

    series_data = load_json_file(SERIES_JSON_FILE)
    artists_data = load_json_file(ARTISTS_JSON_FILE)
    extended_cast_data = load_json_file(EXTENDED_CAST_JSON_FILE)
    merged_by_id = {o['showID']: o for o in series_data if o.get('showID')}
    
    manual_report = apply_manual_updates(io.BytesIO(excel_bytes.getvalue()), merged_by_id, context)
    if manual_report: context['report_data']['Manual Updates'] = manual_report

    sheets_to_process = [s.strip() for s in os.environ.get("SHEETS", "Sheet1").split(';') if s.strip()]
    for sheet in sheets_to_process:
        context['current_sheet'] = sheet
        report = context['report_data'].setdefault(sheet, {})
        excel_rows, warnings = excel_to_objects(io.BytesIO(excel_bytes.getvalue()), sheet)
        if warnings: report.setdefault('data_warnings', []).extend(warnings)

        for excel_obj in excel_rows:
            sid = excel_obj['showID']
            old_obj_from_json = merged_by_id.get(sid)
            is_new = old_obj_from_json is None
            
            final_obj = {**JSON_OBJECT_TEMPLATE, **(old_obj_from_json or {}), **excel_obj}
            initial_metadata_state = {k: final_obj.get(k) for k in ['synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'director', 'tags', 'cast']}
            context['new_artists_added'] = [] 
            
            final_obj = fetch_and_populate_metadata(final_obj, context, artists_data)
            
            if 'cast' in final_obj and isinstance(final_obj['cast'], list):
                main_cast, extended_cast, extended_info = process_and_distribute_cast(final_obj['cast'], artists_data, context)
                final_obj['cast'] = main_cast
                final_obj['extendedCastInfo'] = extended_info
                if extended_cast: extended_cast_data[str(sid)] = extended_cast
            
            final_obj['topRatings'] = (final_obj.get("ratings", 0)) * (len(final_obj.get("againWatchedDates", [])) + 1) * 100
            
            excel_data_has_changed = not is_new and objects_differ(old_obj_from_json, excel_obj)
            metadata_was_fetched = any(final_obj.get(k) != v for k, v in initial_metadata_state.items())
            
            key_map = {'synopsis': 'Synopsis', 'showImage': 'Show Image', 'otherNames': 'Other Names', 'releaseDate': 'Release Date', 'Duration': 'Duration', 'director': 'Director', 'tags': 'Tags', 'cast': 'Cast'}
            newly_fetched_fields = sorted([key_map[k] for k, v in initial_metadata_state.items() if not v and (isinstance(final_obj.get(k), list) and final_obj.get(k) or isinstance(final_obj.get(k), str) and final_obj.get(k))])

            if is_new:
                final_obj['updatedDetails'] = "First Time Uploaded"; final_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                report.setdefault('created', []).append(final_obj)
                if newly_fetched_fields: report.setdefault('fetched_data', []).append(f"- {sid} - {final_obj['showName']} -> Fetched: {', '.join(newly_fetched_fields)}")
            elif excel_data_has_changed:
                changes = [human_readable_field(k) for k, v in excel_obj.items() if normalize_list(old_obj_from_json.get(k)) != normalize_list(v)]
                final_obj['updatedDetails'] = f"{', '.join(changes)} Updated"; final_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                report.setdefault('updated', []).append({'old': old_obj_from_json, 'new': final_obj}); create_diff_backup(old_obj_from_json, final_obj, context)
            elif metadata_was_fetched:
                report.setdefault('refetched', []).append({'id': sid, 'name': final_obj['showName'], 'fields': newly_fetched_fields})
            else:
                report.setdefault('skipped', []).append(f"{sid} - {final_obj['showName']} ({final_obj.get('releasedYear')})")
            
            if is_new or excel_data_has_changed or metadata_was_fetched:
                 merged_by_id[sid] = final_obj
                 save_metadata_backup(final_obj, context)

            missing_fields = {'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'director', 'tags', 'cast'}
            missing = [human_readable_field(k) for k, v in final_obj.items() if k in missing_fields and not v]
            if missing: report.setdefault('fetch_warnings', []).append(f"- {sid} - {final_obj['showName']} -> ⚠️ Missing: {', '.join(sorted(missing))}")

    save_json_file(SERIES_JSON_FILE, sorted(merged_by_id.values(), key=lambda x: x.get('showID', 0)))
    save_json_file(ARTISTS_JSON_FILE, artists_data)
    save_json_file(EXTENDED_CAST_JSON_FILE, extended_cast_data)

    # --- CREATE ARTIST LOOKUP FILE ---
    artist_lookup_list = [{"artistID": k, "artistName": v['artistName']} for k, v in artists_data.items()]
    save_json_file(ARTIST_LOOKUP_FILE, sorted(artist_lookup_list, key=lambda x: x['artistName']))
    
    end_time = now_ist()
    duration = end_time - datetime.fromisoformat(context['start_time_iso'])
    context['duration_str'] = f"{duration.seconds // 60} min {duration.seconds % 60} sec"
    report_path = os.path.join(REPORTS_DIR, f"Report_{filename_timestamp()}.txt"); os.makedirs(REPORTS_DIR, exist_ok=True)
    context['report_file_path'] = report_path
    context['files_generated']['reports'].append(report_path)
    write_report(context)
    print(f"✅ Report written -> {report_path}")
    print("\nAll done.")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ A fatal, unexpected error occurred: {e}")
        logd(traceback.format_exc())
        sys.exit(1)