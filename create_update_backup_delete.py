# ============================================================
# Script: create_update_backup_delete.py
# Author:[BruceBanner001]
# Version: v10.4 (BULLETPROOF RELAY-RACE EDITION)
# ============================================================

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# --------------------------- VERSION & CONFIG ------------------------
SCRIPT_VERSION = "v10.4"

JSON_OBJECT_TEMPLATE = {
    "showID": None, "showName": None, "otherNames":[], "showImage": None,
    "watchStartedOn": None, "watchEndedOn": None, "releasedYear": 0,
    "releaseDate": None, "totalEpisodes": 0, "showType": None,
    "nativeLanguage": None, "watchedLanguage": None, "country": None,
    "comments": None, "ratings": 0, "genres":[], "network":[],
    "againWatchedDates":[], "updatedOn": None, "updatedDetails": None,
    "synopsis": None, "topRatings": 0, "Duration": None,
    "director":[], "tags":[], "cast": {}, "airedOn":[],
    "sitePriorityUsed": {"showImage": None, "releaseDate": None, "otherNames": None, "Duration": None, "synopsis": None, "director": None, "tags": None, "cast": None, "network": None, "airedOn": None}
}

SITE_PRIORITY_BY_LANGUAGE = {
    "korean": { "synopsis": "asianwiki", "showImage": "asianwiki", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "asianwiki", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist", "network": "mydramalist", "airedOn": "mydramalist" },
    "chinese": { "synopsis": "mydramalist", "showImage": "mydramalist", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "mydramalist", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist", "network": "mydramalist", "airedOn": "mydramalist" },
    "japanese": { "synopsis": "asianwiki", "showImage": "asianwiki", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "asianwiki", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist", "network": "mydramalist", "airedOn": "mydramalist" },
    "thai": { "synopsis": "mydramalist", "showImage": "mydramalist", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "mydramalist", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist", "network": "mydramalist", "airedOn": "mydramalist" },
    "taiwanese": { "synopsis": "mydramalist", "showImage": "mydramalist", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "mydramalist", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist", "network": "mydramalist", "airedOn": "mydramalist" },
    "filipino": { "synopsis": "mydramalist", "showImage": "mydramalist", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "mydramalist", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist", "network": "mydramalist", "airedOn": "mydramalist" },
    "english": { "synopsis": "imdb", "showImage": "imdb", "otherNames": "imdb", "Duration": "imdb", "releaseDate": "imdb", "director": "imdb", "tags": "imdb", "cast": "imdb", "network": "imdb", "airedOn": "imdb" },
    "default": { "synopsis": "mydramalist", "showImage": "asianwiki", "otherNames": "mydramalist", "Duration": "mydramalist", "releaseDate": "asianwiki", "director": "mydramalist", "tags": "mydramalist", "cast": "mydramalist", "network": "mydramalist", "airedOn": "mydramalist" }
}

FIELD_NAME_MAP = { "showID": "Show ID", "showName": "Show Name", "otherNames": "Other Names", "showImage": "Show Image", "watchStartedOn": "Watch Started On", "watchEndedOn": "Watch Ended On", "releasedYear": "Released Year", "releaseDate": "Release Date", "totalEpisodes": "Total Episodes", "showType": "Show Type", "nativeLanguage": "Native Language", "watchedLanguage": "Watched Language", "country": "Country", "comments": "Comments", "ratings": "Ratings", "genres": "Category", "network": "Network", "againWatchedDates": "Again Watched Dates", "updatedOn": "Updated On", "updatedDetails": "Updated Details", "synopsis": "Synopsis", "topRatings": "Top Ratings", "Duration": "Duration", "director": "Director", "tags": "Tags", "cast": "Cast", "airedOn": "Aired On", "sitePriorityUsed": "Site Priority Used" }
LOCKED_FIELDS_AFTER_CREATION = {'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'director', 'tags', 'cast', 'updatedOn', 'updatedDetails', 'sitePriorityUsed', 'topRatings', 'network', 'airedOn'}

# ---------------------------- IMPORTS & GLOBALS ----------------------------
import os, re, sys, json, io, shutil, traceback, copy, time, hashlib
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
import pandas as pd
import requests
from bs4 import BeautifulSoup

DEBUG_FETCH = os.environ.get("DEBUG_FETCH", "true").lower() == "true" 

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

try: 
    from PIL import Image, ImageFile 
    ImageFile.LOAD_TRUNCATED_IMAGES = True 
    HAVE_PIL = True
except Exception: HAVE_PIL = False

try: from google.oauth2 import service_account; from googleapiclient.discovery import build; from googleapiclient.http import MediaIoBaseDownload; HAVE_GOOGLE_API = True
except Exception: HAVE_GOOGLE_API = False

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist(): return datetime.now(IST)
def filename_timestamp(): return now_ist().strftime("%d_%B_%Y_%H%M")
def run_id_timestamp(): return now_ist().strftime("RUN_%Y%m%d_%H%M%S")

SERIES_JSON_FILE = "seriesData.json"
ARTISTS_JSON_FILE = "artists.json"
CAST_JSON_FILE = "cast.json"
ARTIST_LOOKUP_FILE = "artists_lookup.json"
BATCH_STATE_FILE = "BATCH_REPORT_DATA.json"

BACKUP_DIR, SHOW_IMAGES_DIR, ARTIST_IMAGES_DIR, DELETE_IMAGES_DIR = "backups", "show-images", "artist-images", "deleted-images"
DELETED_DATA_DIR, REPORTS_DIR, BACKUP_META_DIR = "deleted-data", "reports", "backup-meta-data"
ARCHIVED_BACKUPS_DIR, ARCHIVED_META_DIR = "archived-backups", "archived-backup-meta-data"

SERVICE_ACCOUNT_FILE, EXCEL_FILE_ID_TXT = "GDRIVE_SERVICE_ACCOUNT.json", "EXCEL_FILE_ID.txt"
SCRAPER = cloudscraper.create_scraper() if HAVE_SCRAPER else requests.Session()

SCRAPER.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cookie': 'lc-main=en_US'
})

LANG_TO_COUNTRY_MAP = {"korean": "South Korea", "chinese": "China", "japanese": "Japan", "thai": "Thailand", "taiwanese": "Taiwan", "filipino": "Philippines", "english": "USA"}

def logd(msg):
    if DEBUG_FETCH: print(f"[DEBUG] {msg}")

def human_readable_field(field): return FIELD_NAME_MAP.get(field, field)
def ddmmyyyy(val):
    if pd.isna(val): return None
    try: dt = pd.to_datetime(str(val).strip(), errors='coerce'); return None if pd.isna(dt) else dt.strftime("%d-%m-%Y")
    except Exception: return None
def normalize_list(val):
    if val is None: return[]
    if isinstance(val, dict): return val
    if isinstance(val, list): items = val
    else: items =[p.strip() for p in str(val).split(',') if p.strip()]
    return sorted([item for item in items if item])

def objects_differ(old, new):
    excel_fields = set(FIELD_NAME_MAP.keys()) - LOCKED_FIELDS_AFTER_CREATION
    for k in excel_fields:
        if normalize_list(old.get(k)) != normalize_list(new.get(k)): return True
    return False

def _clean_other_names(names_list):
    if not names_list: return[]
    unique_names =[]
    seen = set()
    for name in names_list:
        clean = name.strip()
        if len(clean) < 2: continue
        if clean.lower() not in seen:
            seen.add(clean.lower())
            unique_names.append(clean)
    return unique_names

# ---------------------------- BATCH STATE LOGIC ----------------------------

def merge_batch_state(context):
    if not os.path.exists(BATCH_STATE_FILE): return
    try:
        with open(BATCH_STATE_FILE, 'r', encoding='utf-8') as f:
            batch_state = json.load(f)
        for sheet, data in batch_state.get('report_data', {}).items():
            if sheet not in context['report_data']: context['report_data'][sheet] = {}
            for key, lst in data.items():
                if key not in context['report_data'][sheet]: context['report_data'][sheet][key] =[]
                context['report_data'][sheet][key].extend(lst)
        for category, lst in batch_state.get('files_generated', {}).items():
            if category not in context['files_generated']: context['files_generated'][category] =[]
            context['files_generated'][category].extend(lst)
            
        context['cumulative_time_seconds'] = batch_state.get('cumulative_time_seconds', 0)
        context['global_start_time'] = batch_state.get('global_start_time')
        context['batch_run_count'] = batch_state.get('batch_run_count', 1)
    except Exception as e: logd(f"Failed to load batch state: {e}")

def save_batch_state(context, current_run_seconds):
    state = {
        'global_start_time': context['global_start_time'],
        'batch_run_count': context['batch_run_count'] + 1,
        'report_data': context['report_data'], 
        'files_generated': context['files_generated'],
        'cumulative_time_seconds': context['cumulative_time_seconds'] + current_run_seconds
    }
    with open(BATCH_STATE_FILE, 'w', encoding='utf-8') as f: json.dump(state, f, indent=4, ensure_ascii=False)

# ---------------------------- SCRAPER ENGINE ----------------------------

def _get_imdb_json_ld(soup):
    try:
        script = soup.find('script', type='application/ld+json')
        if script:
            data = json.loads(script.string)
            if isinstance(data, list):
                for item in data:
                    if item.get('@type') in['Movie', 'TVSeries', 'TVEpisode', 'TVMiniSeries']: return item
                return data[0] if data else None
            return data
    except Exception: pass
    return None

def _validate_page_title(soup, expected_name, site, url):
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
        
        def extract_season(text):
            m = re.search(r'\b(?:Season|Part|S)\s*(\d+)\b', text, re.IGNORECASE)
            if m: return int(m.group(1))
            m2 = re.search(r'\s+(\d+)$', re.sub(r'\(\d{4}\)', '', text).strip())
            if m2 and int(m2.group(1)) < 20: 
                return int(m2.group(1))
            return None

        page_s = extract_season(page_title)
        exp_s = extract_season(expected_name)
        
        if page_s is None:
            m_url = re.search(r'(?:season|part)[-_]*(\d+)', url, re.IGNORECASE)
            if m_url: page_s = int(m_url.group(1))
            
        exp_s = exp_s if exp_s is not None else 1
        
        if page_s is not None and exp_s != page_s:
            logd(f"Title Validation FAILED: Season mismatch. Expected S{exp_s}, Page has S{page_s} ({page_title})")
            return False
            
        if exp_s > 1 and page_s is None:
            if site != "imdb":
                base_expected = re.sub(r'\b(?:Season|Part|S)\s*\d+\b|\s+\d+$', '', expected_name, flags=re.IGNORECASE).strip().lower()
                base_page = re.sub(r'\(.*?\)', '', page_title).lower().strip()
                if base_expected in base_page or base_page in base_expected:
                    logd(f"Title Validation FAILED: Expected S{exp_s}, but found base S1 ('{page_title}')")
                    return False

        t1 = re.sub(r'\(\d{4}\)', '', page_title).lower().strip()
        t2 = re.sub(r'\(\d{4}\)', '', expected_name).lower().strip()
        
        t1_core = re.sub(r'\b(?:season|part|s)\s*\d+\b|\s+\d+$', '', t1).strip()
        t2_core = re.sub(r'\b(?:season|part|s)\s*\d+\b|\s+\d+$', '', t2).strip()

        if site == "imdb" and (t2 in t1 or t2_core in t1_core): return True

        ratio = SequenceMatcher(None, t1_core, t2_core).ratio()
        if ratio < 0.4 and t2_core not in t1_core and t1_core not in t2_core:
            logd(f"Title Validation FAILED: Page Title '{page_title}' vs Expected '{expected_name}' (Ratio: {ratio:.2f})")
            return False
        return True
    except Exception as e: return True

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

def get_soup_from_search(search_term, expected_name, show_year, site, language, show_type, soup_cache):
    cache_key = f"{expected_name}_{search_term}_{show_year}_{site}_{language}_{show_type}"
    if cache_key in soup_cache: return soup_cache[cache_key]

    expected_country = LANG_TO_COUNTRY_MAP.get(language.lower())
    if not HAVE_DDGS: return None, None

    search_queries =[]
    clean_name = re.sub(r'\b(?:Season|Part|S)\s*\d+\b|\s+\d+$', '', search_term, flags=re.IGNORECASE).strip()

    if site == "imdb":
        entity_hint = "TV Series" if "Drama" in show_type else "Movie"
        search_queries =[f'"{search_term}" {entity_hint} site:imdb.com/title/', f'"{search_term}" {show_year} site:imdb.com/title/']
        if clean_name != search_term:
            search_queries.extend([f'"{clean_name}" {entity_hint} site:imdb.com/title/', f'"{clean_name}" {show_year} site:imdb.com/title/'])
    else:
        search_queries =[ f'"{search_term}" {show_year} {language} site:{site}.com', f'"{search_term}" {show_year} site:{site}.com', f'"{search_term}" site:{site}.com' ]
        if clean_name != search_term:
            search_queries.extend([ f'"{clean_name}" {show_year} {language} site:{site}.com', f'"{clean_name}" {show_year} site:{site}.com', f'"{clean_name}" site:{site}.com' ])

    for query in search_queries:
        results = None
        for attempt in range(3):
            try:
                time.sleep(2.0 + attempt * 2.0)
                with DDGS() as dd: results = list(dd.text(query, max_results=5))
                break 
            except Exception as e: pass
                
        if not results: continue

        for res in results:
            url = res.get('href', '')
            if not url or 'bing.com' in url or any(bad in url for bad in['/reviews', '/recs', '?lang=', '/photos', '/video', '/trivia']): continue
            if site == "asianwiki" and ("/File:" in url or "/index.php?title=File:" in url): continue
            
            if site == "imdb":
                tt_match = re.search(r'/title/(tt\d+)', url)
                if not tt_match: continue
                url = f"https://www.imdb.com/title/{tt_match.group(1)}/"

            r = SCRAPER.get(url, timeout=15)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                
                if site == "imdb":
                    data = _get_imdb_json_ld(soup)
                    if data:
                        imdb_type = data.get('@type', '')
                        if imdb_type == 'TVEpisode': continue
                        if show_type in['Drama', 'Mini Drama'] and imdb_type == 'Movie': continue
                        if show_type == 'Movie' and imdb_type in['TVSeries', 'TVMiniSeries']: continue
                
                is_valid_landmark = False
                if site == "asianwiki" and soup.find(id='Profile'): is_valid_landmark = True
                elif site == "mydramalist" and soup.find('div', class_='box-body'): is_valid_landmark = True
                elif site == "imdb" and soup.find('h1'): is_valid_landmark = True
                
                if is_valid_landmark:
                    if expected_country and site != 'imdb':
                        scraped_country = _scrape_country(soup, site)
                        if scraped_country and expected_country not in scraped_country: continue
                    
                    if not _validate_page_title(soup, expected_name, site, url): continue

                    soup_cache[cache_key] = (soup, url)
                    return soup, url

    soup_cache[cache_key] = (None, None)
    return None, None

def download_and_save_image(url, local_path, is_artist=False):
    if not HAVE_PIL or not url: return False
    
    dummy_keywords =['default', 'nopicture', 'no-poster', 'avatar', 'blank', 'null', 'data:image']
    if any(kw in url.lower() for kw in dummy_keywords):
        return False

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        url = re.sub(r'_[24]c\.jpg$', '.jpg', url) if not is_artist else url
        r = SCRAPER.get(url, stream=True, timeout=20)
        
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("image"):
            with Image.open(r.raw) as img:
                img = img.convert("RGB")
                size = (400, 600) if is_artist else (800, 1200)
                img.thumbnail(size, Image.LANCZOS)
                img.save(local_path, "JPEG", quality=90)
                return True
    except Exception as e: 
        logd(f"Failed to download image from {url}: {e}")
    return False

def _extract_mdl_list_item(soup, label_regex):
    b_tag = soup.find('b', string=re.compile(label_regex, re.IGNORECASE))
    if b_tag:
        for parent_tag in b_tag.find_parents(['li', 'div', 'p']):
            full_text = parent_tag.get_text(" ", strip=True)
            b_text = b_tag.get_text(" ", strip=True)
            text = full_text.replace(b_text, "").strip()
            text = re.sub(r'^[:\s]+', '', text).strip()
            if text:
                return text, parent_tag
    return None, None

def _extract_aw_list_item(soup, label_regex):
    b_tag = soup.find('b', string=re.compile(label_regex, re.IGNORECASE))
    if b_tag:
        for parent in b_tag.find_parents(['li', 'div', 'p', 'td', 'tr']):
            full_text = parent.get_text(" ", strip=True)
            b_text = b_tag.get_text(" ", strip=True)
            text = full_text.replace(b_text, "").strip()
            text = re.sub(r'^[:\s]+', '', text).strip()
            if text:
                return text
    return None

# --- ASIANWIKI SCRAPERS ---
def _scrape_synopsis_from_asianwiki(soup, **kwargs):
    try:
        target_element = soup.find(id=re.compile(r"(Plot|Synopsis)", re.IGNORECASE))
        if not target_element:
            for tag in soup.find_all(['h2', 'h3', 'h4', 'b', 'strong']):
                if re.search(r"^(Plot|Synopsis)", tag.get_text(strip=True), re.IGNORECASE):
                    target_element = tag
                    break
                    
        if not target_element: return None
        if target_element.name not in['h2', 'h3']:
            parent = target_element.find_parent(['h2', 'h3'])
            if parent: target_element = parent
            
        content =[]
        for sibling in target_element.next_siblings:
            if getattr(sibling, 'name', None) in['h2', 'h3', 'h4']: break 
            text = sibling.get_text(strip=True) if hasattr(sibling, 'get_text') else str(sibling).strip()
            if getattr(sibling, 'name', None) in['script', 'style', 'table']: continue
            if text and len(text) >= 3: 
                content.append(text)
        
        synopsis = "\n\n".join(content) if content else None
        if synopsis: synopsis = re.sub(r'[\s\(\-\[\]\,]+$', '', synopsis).strip()
        return synopsis
    except Exception: return None

def _scrape_image_from_asianwiki(soup, **kwargs):
    try:
        meta_img = soup.find('meta', property='og:image')
        url = meta_img['content'] if meta_img and 'content' in meta_img.attrs else None
        if not url or "default" in url.lower():
            img = soup.select_one('a.image > img[src], .infobox img[src], .thumbinner img[src]')
            if img: url = requests.compat.urljoin("https://asianwiki.com", img['src'])
                
        if not url: return None
        image_path = os.path.join(SHOW_IMAGES_DIR, f"{kwargs['sid']}.jpg")
        if download_and_save_image(url, image_path): return os.path.basename(image_path)
    except Exception: return None

def _scrape_othernames_from_asianwiki(soup, **kwargs):
    try:
        names = []
        target_keywords =['also known as', 'romaji', 'pinyin', 'literal title', 'chinese title', 'japanese title', 'hangul']
        for b_tag in soup.find_all('b'):
            text = b_tag.get_text(strip=True).lower()
            if any(keyword in text for keyword in target_keywords):
                val = ""
                for parent in b_tag.find_parents(['li', 'div', 'p', 'td']):
                    full_text = parent.get_text(" ", strip=True)
                    val = full_text.replace(b_tag.get_text(strip=True), "").replace(':', '').strip()
                    if val:
                        break
                if val and val.lower() != kwargs.get('show_name', '').lower():
                    raw_names = re.split(r'[/,]', val)
                    names.extend([n.strip() for n in raw_names if n.strip() and len(n.strip()) > 1])
        return _clean_other_names(names) if names else None
    except Exception: pass
    return None

def _scrape_release_date_from_asianwiki(soup, **kwargs):
    try:
        text = _extract_aw_list_item(soup, r"^\s*Release Date.*")
        if text: return text
    except Exception: pass
    return None

def _scrape_network_from_asianwiki(soup, **kwargs):
    try:
        text = _extract_aw_list_item(soup, r"^\s*Network.*")
        if text: return[n.strip() for n in text.split(',') if n.strip()]
    except Exception: pass
    return None

def _scrape_director_from_asianwiki(soup, **kwargs):
    try:
        text = _extract_aw_list_item(soup, r"^\s*Director.*")
        if text: return[n.strip() for n in text.split(',') if n.strip()]
    except Exception: pass
    return None

# --- MYDRAMALIST SCRAPERS ---
def _scrape_synopsis_from_mydramalist(soup, **kwargs):
    try:
        synopsis_div = soup.select_one('.show-synopsis, [itemprop="description"]')
        if not synopsis_div: return None
        text = synopsis_div.get_text(separator='\n', strip=True)
        paragraphs =[line.strip() for line in text.split('\n') if line.strip()]
        synopsis = "\n\n".join(paragraphs)
        patterns_to_remove =[ r'\s*\(Source:.*?\)\s*$', r'\s*Source:.*$', r'~~.*', r'\s*Edit Translation\s*$', r'\s*(Additional Cast Members|Native title|Also Known As):.*$', r'^\s*Remove ads\s*' ]
        for pattern in patterns_to_remove: synopsis = re.sub(pattern, '', synopsis, flags=re.IGNORECASE | re.DOTALL).strip()
        if synopsis: synopsis = re.sub(r'[\s\(\-\[\]\,]+$', '', synopsis).strip()
        return synopsis if synopsis else None
    except Exception as e: return None

def _scrape_image_from_mydramalist(soup, **kwargs):
    try:
        meta_img = soup.find('meta', property='og:image')
        url = meta_img['content'] if meta_img and 'content' in meta_img.attrs else None
        if not url or "default" in url.lower():
            img = soup.select_one('.film-cover img, .cover img')
            if img: url = img.get('src') or img.get('data-src') or img.get('data-original')
        if not url: return None
        image_path = os.path.join(SHOW_IMAGES_DIR, f"{kwargs['sid']}.jpg")
        if download_and_save_image(url, image_path): return os.path.basename(image_path)
    except Exception: return None

def _scrape_othernames_from_mydramalist(soup, **kwargs):
    try:
        text, _ = _extract_mdl_list_item(soup, r"^\s*Also Known As.*")
        if text:
            raw_names =[name.strip() for name in text.split(',') if name.strip()]
            filtered =[name for name in raw_names if name.lower() != kwargs.get('show_name', '').lower()]
            return _clean_other_names(filtered)
    except Exception: pass
    return None

def _scrape_duration_from_mydramalist(soup, **kwargs):
    try:
        text, _ = _extract_mdl_list_item(soup, r"^\s*Duration.*")
        if text: return text.replace(" min.", " mins") if "hr" not in text else text
    except Exception: pass
    return None

def _scrape_release_date_from_mydramalist(soup, **kwargs):
    try:
        text, _ = _extract_mdl_list_item(soup, r"^\s*Aired[\s:]*$")
        if text: return text
    except Exception: pass
    return None

def _scrape_director_from_mydramalist(soup, **kwargs):
    try:
        text, _ = _extract_mdl_list_item(soup, r"^\s*Director.*")
        if text: return[name.strip() for name in text.split(',') if name.strip()]
    except Exception: pass
    return None

def _scrape_tags_from_mydramalist(soup, **kwargs):
    try:
        tags_li = soup.select_one('li.show-tags')
        if tags_li: return[a.get_text(strip=True) for a in tags_li.find_all('a') if "(Vote tags)" not in a.get_text()]
    except Exception: return None

def _scrape_network_from_mydramalist(soup, **kwargs):
    try:
        text, parent_tag = _extract_mdl_list_item(soup, r"^\s*Original Network.*")
        if parent_tag:
            nets =[a.get_text(strip=True) for a in parent_tag.find_all('a')]
            if nets: return nets
            return[n.strip() for n in text.split(',') if n.strip()]
    except Exception: pass
    return None

def _scrape_airedon_from_mydramalist(soup, **kwargs):
    try:
        text, _ = _extract_mdl_list_item(soup, r"^\s*Aired On.*")
        if text: return[day.strip() for day in text.split(',') if day.strip()]
    except Exception: pass
    return None

def _scrape_cast_from_mydramalist(soup, **kwargs):
    try:
        full_cast_raw =[]
        seen_ids = set() 
        url = kwargs.get('url', '')
        target_soup = soup
        
        if url:
            base_url = url.split('#')[0].split('?')[0].rstrip('/')
            cast_url = base_url if base_url.endswith('/cast') else base_url + '/cast'
            try:
                time.sleep(4.0) 
                headers = { "Referer": url, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8" }
                r = SCRAPER.get(cast_url, headers=headers, timeout=20)
                if r.status_code == 200 and '/people/' in r.text:
                    cast_soup = BeautifulSoup(r.text, "html.parser")
                    if cast_soup.select('a[href*="/people/"]'): target_soup = cast_soup
            except Exception as e: 
                logd(f"Failed to fetch MDL /cast page: {e}")

        items = target_soup.select('li.list-item, div.cast-list div.col-xs-8, div.cast-list div.col-sm-6, .p-a-0 li, .crew-list div.col-xs-8, .box-body div[class*="col-sm-"], .box-body div[class*="col-md-"], .box-body div.list-item')
        if not items:
            for a in target_soup.select('a[href*="/people/"]'):
                parent = a.find_parent(['li', 'div'], class_=re.compile(r'\b(list-item|col-(?:sm|md|lg)-\d+|row)\b'))
                if parent and parent not in items: items.append(parent)

        if not items: return None

        main_role_count = 0
        for item in items:
            try:
                artist_name, artist_link = None, None
                for a in item.select('a[href*="/people/"]'):
                    text = a.get_text(strip=True)
                    if text:
                        artist_name = text; artist_link = a['href']
                        break 
                
                if not artist_name: continue 
                id_match = re.search(r'/people/(\d+)', artist_link)
                if not id_match: continue
                artist_id = id_match.group(1)
                
                if artist_id in seen_ids: continue
                seen_ids.add(artist_id)
                
                img_tag = item.select_one('img')
                artist_image_url = img_tag.get('src') or img_tag.get('data-src') or img_tag.get('data-original') if img_tag else None
                if artist_image_url and ('avatar' in artist_image_url or 'default' in artist_image_url): artist_image_url = None

                role_texts =[]
                elements = list(item.select('.text-muted, .text-sm, small, .role'))
                nxt = item.find_next_sibling('div')
                if nxt and any('col' in str(c).lower() or 'right' in str(c).lower() or 'role' in str(c).lower() for c in nxt.get('class',[])):
                    sib_elements = list(nxt.select('.text-muted, .text-sm, small, .role'))
                    if sib_elements: elements.extend(sib_elements)
                    else:
                        t = nxt.get_text(" ", strip=True)
                        if t and t != artist_name: role_texts.append(t)

                for e in elements:
                    t = e.get_text(" ", strip=True)
                    if t and t != artist_name and t not in role_texts: role_texts.append(t)

                if not role_texts:
                    for div in item.find_all('div'):
                        if 'col-xs-4' in div.get('class',[]) or 'text-center' in div.get('class',[]): continue
                        if not div.find('a'):
                            t = div.get_text(" ", strip=True)
                            if t and t != artist_name and t not in role_texts: role_texts.append(t)

                is_crew = False
                header_text, raw_header_text = "", ""
                prev_header = item.find_previous(['h2', 'h3', 'h4', 'h5'])
                if prev_header:
                    raw_header_text = prev_header.get_text(" ", strip=True)
                    header_text = raw_header_text.lower()
                
                crew_keywords =['crew', 'director', 'writer', 'screenwriter', 'producer', 'production', 'music', 'composer', 'art', 'editing', 'editor', 'cinematograph', 'original', 'staff', 'lighting', 'ost', 'sound', 'action', 'martial']
                cast_keywords =['cast', 'main', 'support', 'guest', 'cameo', 'bit part', 'actor', 'actress']
                
                if any(cast_kw in header_text for cast_kw in cast_keywords): is_crew = False
                elif any(kw in header_text for kw in crew_keywords): is_crew = True
                
                combined_text = " ".join(role_texts).lower()
                if re.search(r'\b(director|writer|screenwriter|producer|composer|cinematographer|editor|music|crew|staff|art|lighting|original|ost|sound|action|martial)\b', combined_text): is_crew = True
                if re.search(r'\b(main role|main cast|support role|supporting cast|guest role|guest cast|cameo|bit part)\b', combined_text): is_crew = False

                if is_crew:
                    character_name = None  
                    final_role = " ".join(role_texts).strip()
                    if not final_role and raw_header_text and header_text not in['cast', 'crew', 'cast & crew', 'cast and crew']: final_role = raw_header_text
                    if not final_role: final_role = "Crew"
                    final_role = re.sub(r'^[,:\-\s]+|[,:\-\s]+$', '', final_role).strip().title()
                    if len(final_role) > 50: final_role = final_role[:50]
                else:
                    character_name = "Unknown"
                    final_role = "Support Role"
                    if not role_texts and raw_header_text:
                        if re.search(r'\b(main)\b', header_text): final_role = 'Main Role'
                        elif re.search(r'\b(guest|cameo|bit part)\b', header_text): final_role = 'Guest Role'
                            
                    for txt in role_texts:
                        txt_lower = txt.lower()
                        if re.search(r'\b(main role|main cast)\b', txt_lower): final_role = 'Main Role'
                        elif re.search(r'\b(support role|supporting cast)\b', txt_lower): final_role = 'Support Role'
                        elif re.search(r'\b(guest role|guest cast|cameo|bit part)\b', txt_lower): final_role = 'Guest Role'
                        
                        clean_char = re.sub(r'\b(main role|main cast|support role|supporting cast|guest role|guest cast|cameo|bit part)\b', '', txt, flags=re.IGNORECASE)
                        clean_char = re.sub(r'^[,:\-\s]+|[,:\-\s]+$', '', clean_char).strip()
                        if clean_char and clean_char.lower() not in['role', 'cast', 'unknown', artist_name.lower()]: character_name = clean_char
                            
                    if final_role == 'Main Role': main_role_count += 1
                    
                full_cast_raw.append({
                    "artistID": artist_id, "artistName": artist_name, "artistImageURL": artist_image_url,
                    "characterName": character_name, "role": final_role
                })
            except Exception as e: 
                logd(f"Error parsing actor item: {e}")
                continue
        
        if not full_cast_raw: return None
        if main_role_count == 0 and len(full_cast_raw) > 0:
            promoted = 0
            for i in range(len(full_cast_raw)):
                if full_cast_raw[i]['role'] in['Support Role', 'Guest Role', 'Unknown']:
                    full_cast_raw[i]['role'] = "Main Role"; promoted += 1
                    if promoted >= 6: break

        if 'context' in kwargs and 'source_links_temp' in kwargs['context']: 
            kwargs['context']['source_links_temp']['raw_cast'] = full_cast_raw
        
        return full_cast_raw
    except Exception as e: 
        logd(f"Fatal error in _scrape_cast_from_mydramalist: {e}")
        return None

# --- IMDB SCRAPERS ---
def _scrape_synopsis_from_imdb(soup, **kwargs):
    try:
        data = _get_imdb_json_ld(soup)
        synopsis = None
        if data and 'description' in data: 
            synopsis = data['description']
        else:
            desc = soup.select_one('[data-testid^="plot"]')
            if desc: synopsis = desc.get_text(strip=True)
            
        if synopsis: synopsis = re.sub(r'[\s\(\-\[\]\,]+$', '', synopsis).strip()
        return synopsis
    except Exception: pass
    return None

def _scrape_image_from_imdb(soup, **kwargs):
    try:
        target_soup = soup
        m = re.search(r'\b(?:Season|Part)\s*(\d+)\b', kwargs.get('show_name', ''), re.IGNORECASE)
        if m:
            season_num = m.group(1)
            season_url = kwargs['url'].rstrip('/') + f"/episodes/?season={season_num}"
            r = SCRAPER.get(season_url, timeout=15)
            if r.status_code == 200:
                target_soup = BeautifulSoup(r.text, "html.parser")
        
        meta_img = target_soup.find('meta', property='og:image')
        if meta_img and 'content' in meta_img.attrs:
            url = meta_img['content']
            if "title_hero_default" not in url and "imdb_fb_logo" not in url:
                image_path = os.path.join(SHOW_IMAGES_DIR, f"{kwargs['sid']}.jpg")
                if download_and_save_image(url, image_path): return os.path.basename(image_path)

        data = _get_imdb_json_ld(soup)
        if data and 'image' in data:
            url = data['image']
            image_path = os.path.join(SHOW_IMAGES_DIR, f"{kwargs['sid']}.jpg")
            if download_and_save_image(url, image_path): return os.path.basename(image_path)
    except Exception: pass
    return None

def _scrape_release_date_from_imdb(soup, **kwargs):
    try:
        m = re.search(r'\b(?:Season|Part)\s*(\d+)\b', kwargs.get('show_name', ''), re.IGNORECASE)
        if m:
            season_num = m.group(1)
            season_url = kwargs['url'].rstrip('/') + f"/episodes/?season={season_num}"
            r = SCRAPER.get(season_url, timeout=15)
            if r.status_code == 200:
                season_soup = BeautifulSoup(r.text, "html.parser")
                script = season_soup.find('script', type='application/ld+json')
                if script:
                    data = json.loads(script.string)
                    if isinstance(data, list): data = data[0]
                    if data.get('@type') == 'ItemList' and 'itemListElement' in data:
                        elements = data['itemListElement']
                        if len(elements) > 0:
                            first_ep = elements[0].get('item', {})
                            if 'datePublished' in first_ep: return first_ep['datePublished']

        data = _get_imdb_json_ld(soup)
        if data and 'datePublished' in data: return data['datePublished']
    except Exception: pass
    return None

def _scrape_duration_from_imdb(soup, **kwargs):
    try:
        data = _get_imdb_json_ld(soup)
        if data and 'duration' in data:
            dur = data['duration']
            match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', dur.upper())
            if match:
                h = int(match.group(1)) if match.group(1) else 0
                m = int(match.group(2)) if match.group(2) else 0
                total_mins = (h * 60) + m
                if total_mins > 0: return f"{total_mins} mins"
                
        runtime_tag = soup.find('li', attrs={'data-testid': 'title-techspec_runtime'})
        if runtime_tag and (div := runtime_tag.find('div')):
            text = div.get_text(strip=True).lower()
            m = re.search(r'(\d+)\s*m', text)
            if m: return f"{m.group(1)} mins"
    except Exception: pass
    return None

def _scrape_othernames_from_imdb(soup, **kwargs):
    try:
        aka_tag = soup.find('li', attrs={'data-testid': 'title-details-akas'})
        if aka_tag and (div := aka_tag.find('div')):
            text_content = div.get_text(separator=', ', strip=True)
            names =[n.strip() for n in text_content.split(',') if n.strip()]
            return _clean_other_names(names)
    except Exception: pass
    return None

def _scrape_director_from_imdb(soup, **kwargs):
    try:
        dirs =[]
        data = _get_imdb_json_ld(soup)
        if data:
            for key in['director', 'creator']:
                if key in data:
                    entities = data[key]
                    if not isinstance(entities, list): entities = [entities]
                    for e in entities:
                        if e.get('@type') == 'Person' and 'name' in e: dirs.append(e['name'])
        if dirs: return list(dict.fromkeys(dirs))
    except Exception: pass
    return None

def _scrape_tags_from_imdb(soup, **kwargs):
    try:
        data = _get_imdb_json_ld(soup)
        if data and 'genre' in data: return data['genre'] if isinstance(data['genre'], list) else[data['genre']]
    except Exception: pass
    return None

def _scrape_network_from_imdb(soup, **kwargs):
    return None

def _scrape_cast_from_imdb(soup, **kwargs):
    try:
        full_cast_raw =[]
        seen_ids = set()
        cards = soup.select('div[data-testid="title-cast-item"]')
        
        for idx, card in enumerate(cards[:20]): 
            try:
                a_tag = card.select_one('a[data-testid="title-cast-item__actor"]')
                if not a_tag: continue
                name = a_tag.get_text(strip=True)
                url = a_tag['href']
                artist_id = re.search(r'(nm\d+)', url).group(1)
                
                seen_ids.add(artist_id)
                img_tag = card.select_one('img')
                img_url = img_tag['src'] if img_tag else None
                
                char_span = card.select_one('.cast-item-characters-link, .title-cast-item__characters')
                char_name = char_span.get_text(" ", strip=True) if char_span else "Unknown"
                
                char_name = re.sub(r'\s*\d+\s*episodes?.*', '', char_name, flags=re.IGNORECASE).strip()
                char_name = re.sub(r'\s*\d{4}\s*-\s*\d{4}.*', '', char_name).strip()
                if not char_name: char_name = "Unknown"
                
                role = "Main Role"
                if idx >= 6: role = "Support Role"
                if idx >= 15: role = "Guest Role"
                
                full_cast_raw.append({
                    "artistID": artist_id, "artistName": name, "artistImageURL": img_url,
                    "characterName": char_name, "role": role 
                })
            except Exception: continue

        data = _get_imdb_json_ld(soup)
        if data:
            for role_key in['director', 'creator']:
                if role_key in data:
                    entities = data[role_key] if isinstance(data[role_key], list) else[data[role_key]]
                    for e in entities:
                        if e.get('@type') == 'Person' and 'name' in e:
                            artist_name = e['name']
                            a_id = None
                            
                            if 'url' in e:
                                match = re.search(r'(nm\d+)', e['url'])
                                if match: a_id = match.group(1)
                                
                            if not a_id:
                                html_link = soup.find('a', string=re.compile(f"^{re.escape(artist_name)}$", re.IGNORECASE), href=re.compile(r'/name/nm\d+'))
                                if html_link:
                                    a_id = re.search(r'(nm\d+)', html_link['href']).group(1)
                                    
                            if not a_id:
                                a_id = "unk_" + hashlib.md5(artist_name.lower().encode('utf-8')).hexdigest()[:8]

                            if a_id not in seen_ids:
                                seen_ids.add(a_id)
                                full_cast_raw.append({
                                    "artistID": a_id, "artistName": artist_name, "artistImageURL": None,
                                    "characterName": None, "role": role_key.title()
                                })

        if full_cast_raw:
             if 'context' in kwargs and 'source_links_temp' in kwargs['context']:
                kwargs['context']['source_links_temp']['raw_cast'] = full_cast_raw
             return full_cast_raw
    except Exception: pass
    return None

SCRAPE_MAP = {
    'asianwiki': {'synopsis': _scrape_synopsis_from_asianwiki, 'showImage': _scrape_image_from_asianwiki, 'otherNames': _scrape_othernames_from_asianwiki, 'Duration': lambda **kwargs: None, 'releaseDate': _scrape_release_date_from_asianwiki, 'director': _scrape_director_from_asianwiki, 'tags': lambda **kwargs: None, 'cast': lambda **kwargs: None, 'network': _scrape_network_from_asianwiki, 'airedOn': lambda **kwargs: None},
    'mydramalist': {'synopsis': _scrape_synopsis_from_mydramalist, 'showImage': _scrape_image_from_mydramalist, 'otherNames': _scrape_othernames_from_mydramalist, 'Duration': _scrape_duration_from_mydramalist, 'releaseDate': _scrape_release_date_from_mydramalist, 'director': _scrape_director_from_mydramalist, 'tags': _scrape_tags_from_mydramalist, 'cast': _scrape_cast_from_mydramalist, 'network': _scrape_network_from_mydramalist, 'airedOn': _scrape_airedon_from_mydramalist},
    'imdb': {'synopsis': _scrape_synopsis_from_imdb, 'showImage': _scrape_image_from_imdb, 'otherNames': _scrape_othernames_from_imdb, 'Duration': _scrape_duration_from_imdb, 'releaseDate': _scrape_release_date_from_imdb, 'director': _scrape_director_from_imdb, 'tags': _scrape_tags_from_imdb, 'cast': _scrape_cast_from_imdb, 'network': _scrape_network_from_imdb, 'airedOn': lambda **kwargs: None}
}

FALLBACK_ORDER = {
    'asianwiki': ['asianwiki', 'mydramalist'],
    'mydramalist':['mydramalist'],
    'imdb': ['imdb']
}

def fetch_and_populate_metadata(obj, context, artists_db):
    s_id, s_name, s_year, lang = obj['showID'], obj['showName'], obj['releasedYear'], obj.get("nativeLanguage", "")
    priority = SITE_PRIORITY_BY_LANGUAGE.get(lang.lower(), SITE_PRIORITY_BY_LANGUAGE['default'])
    spu = obj.setdefault('sitePriorityUsed', {})
    show_type = obj.get('showType', 'Drama')
    
    context['source_links_temp'] = {}
    soup_cache = {}
    fields_to_check =[ 'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'director', 'tags', 'cast', 'network', 'airedOn' ]
    
    for field in fields_to_check:
        if show_type == 'Movie' and field in['airedOn', 'network']:
            continue
            
        should_fetch = not obj.get(field) or field == 'network' 
        
        if should_fetch:
            initial_site = priority.get(field)
            if not initial_site: continue
            
            sites_to_try = FALLBACK_ORDER.get(initial_site, [initial_site])
            
            for current_site in sites_to_try:
                if current_site == 'imdb' and field == 'airedOn':
                    continue
                
                search_terms =[s_name, re.sub(r'\b(?:Season|Part|S)\s*\d+\b|\s+\d+$', '', s_name, flags=re.IGNORECASE).strip()]
                soup, url = None, None
                
                ordered_terms =[]
                for term in search_terms:
                    if term not in ordered_terms:
                        ordered_terms.append(term)
                        
                for term in ordered_terms:
                    soup, url = get_soup_from_search(term, s_name, s_year, current_site, lang, show_type, soup_cache)
                    if soup: break
                
                if soup:
                    scrape_args = {'soup': soup, 'url': url, 'sid': s_id, 'show_name': s_name, 'context': context, 'artists_db': artists_db}
                    data = SCRAPE_MAP[current_site][field](**scrape_args)
                    
                    if data:
                        if field == 'network':
                            existing = normalize_list(obj.get('network'))
                            new_data = normalize_list(data)
                            merged =[]
                            seen = set()
                            for n in existing + new_data:
                                if n.lower() not in seen:
                                    merged.append(n)
                                    seen.add(n.lower())
                            if merged != existing:
                                obj['network'] = merged
                                spu[field] = f"{initial_site} (Fallback: {current_site})" if current_site != initial_site else current_site
                                context['source_links_temp'][field] = url
                                break
                        else:
                            obj[field] = data
                            spu[field] = f"{initial_site} (Fallback: {current_site})" if current_site != initial_site else current_site
                            context['source_links_temp'][field] = url
                            break # Found data, escape fallback loop!
                            
    return obj

def process_deletions(xl, context):
    try:
        target = next((s for s in xl.sheet_names if s.strip().lower() == 'deleting records'), None)
        if not target: return
        df = pd.read_excel(xl, sheet_name=target)
    except Exception: return
    if df.empty: return
    
    series_data = load_json_file(SERIES_JSON_FILE)
    cast_data = load_json_file(CAST_JSON_FILE)
    
    series_by_id = {}
    for o in series_data:
        if o.get('showID'):
            try: series_by_id[int(o['showID'])] = o
            except ValueError: pass
            
    to_delete = set(pd.to_numeric(df.iloc[:, 0], errors='coerce').dropna().astype(int))
    deleted_count = 0
    
    for sid in to_delete:
        sid_str = str(sid)
        if sid in series_by_id:
            show_obj = series_by_id.pop(sid)
            cast_obj = cast_data.pop(sid_str, None)
            
            ts = context['file_ts']
            archive_bundle = { "deletedOn": ts, "showData": show_obj }
            if cast_obj: archive_bundle["castData"] = cast_obj

            path = os.path.join(DELETED_DATA_DIR, f"DELETED_{ts}_{sid}.json"); os.makedirs(DELETED_DATA_DIR, exist_ok=True)
            save_json_file(path, archive_bundle)
            context['files_generated']['deleted_data'].append(path)
            context['report_data'].setdefault('Deleting Records', {}).setdefault('data_deleted',[]).append(f"- {sid} -> {show_obj.get('showName')} ({show_obj.get('releasedYear')}) -> ✅ Deleted")
            
            if show_obj.get('showImage'):
                img_name = os.path.basename(show_obj['showImage'])
                src = os.path.join(SHOW_IMAGES_DIR, img_name)
                if os.path.exists(src):
                    dest = os.path.join(DELETE_IMAGES_DIR, f"DELETED_{ts}_{sid}.jpg"); os.makedirs(DELETE_IMAGES_DIR, exist_ok=True); shutil.move(src, dest)
                    context['files_generated']['deleted_images'].append(dest)
            
            for d in[BACKUP_DIR, BACKUP_META_DIR]:
                for f in os.listdir(d) if os.path.exists(d) else[]:
                    src_path = os.path.join(d, f)
                    if f.endswith(f"_{sid}.json") and os.path.isfile(src_path):
                        archive_dir = os.path.join(ARCHIVED_BACKUPS_DIR if d == BACKUP_DIR else ARCHIVED_META_DIR, sid_str); os.makedirs(archive_dir, exist_ok=True)
                        dest_path = os.path.join(archive_dir, f); shutil.move(src_path, dest_path)
                        context['files_generated']['archived_backups' if d == BACKUP_DIR else 'archived_meta_backups'].append(dest_path)
            deleted_count += 1

    if deleted_count > 0:
        save_json_file(SERIES_JSON_FILE, sorted(list(series_by_id.values()), key=lambda x: int(x.get('showID') or 0)))
        save_json_file(CAST_JSON_FILE, cast_data)

def apply_manual_updates(xl, by_id, context):
    try:
        target = next((s for s in xl.sheet_names if s.strip().lower() == 'manual updates'), None)
        if not target: return {}
        df = pd.read_excel(xl, sheet_name=target, keep_default_na=False).replace({float('nan'): None, pd.NA: None})
        df.columns =[c.strip().lower() for c in df.columns]
    except Exception: return {}
    
    MAP, report = {"image": "showImage", "other names": "otherNames", "release date": "releaseDate", "synopsis": "synopsis", "duration": "Duration", "aired on": "airedOn"}, {}
    for _, row in df.iterrows():
        sid = pd.to_numeric(row.get('no'), errors='coerce')
        if pd.isna(sid) or int(sid) not in by_id: continue
        sid = int(sid); obj, old, changed = by_id[sid], copy.deepcopy(by_id[sid]), {}
        
        for col, key in MAP.items():
            if col in row and str(row[col]).strip() and str(row[col]).strip().lower() != 'nan':
                val = row[col]
                image_downloaded = False
                
                if key == 'showImage':
                    image_path = os.path.join(SHOW_IMAGES_DIR, f"{sid}.jpg")
                    if download_and_save_image(val, image_path):
                        val = os.path.basename(image_path)
                        context['files_generated']['show_images'].append(image_path)
                        image_downloaded = True
                    else: continue
                elif key == 'otherNames': 
                    val = normalize_list(val)
                elif key == 'airedOn':
                    val =[v.strip() for v in str(val).split(',')] if val else[]
                else: 
                    val = str(val).strip()
                
                if obj.get(key) != val or image_downloaded:
                    old_val = obj.get(key)
                    if image_downloaded and old_val == val:
                        changed[key] = {'old': f"{old_val} (Old Image)", 'new': f"{val} (New Image Replaced)"}
                    else:
                        changed[key] = {'old': old_val, 'new': val}
                    obj[key] = val
                    obj.setdefault('sitePriorityUsed', {})[key] = "Manual"
                    
        if changed:
            obj['updatedDetails'] = f"{', '.join([human_readable_field(f) for f in changed])} Updated Manually"
            obj['updatedOn'] = now_ist().strftime('%d %B %Y')
            report.setdefault('updated',[]).append({'old': old, 'new': obj})
            create_diff_backup(old, obj, context, explicit_changes=changed)
            save_metadata_backup(obj, context)
            
    return report

def excel_to_objects(xl, sheet):
    try:
        target = next((s for s in xl.sheet_names if s.strip().lower() == sheet.strip().lower()), None)
        if not target: return[],[]
        df = pd.read_excel(xl, sheet_name=target, keep_default_na=False)
        df.columns =[c.strip().lower() for c in df.columns]
    except Exception: return [],[]
    
    warnings =[]
    try: 
        again_idx =[i for i, c in enumerate(df.columns) if "again watched" in c][0]
    except IndexError: 
        again_idx = len(df.columns) 
        
    MAP = {"no": "showID", "series title": "showName", "started date": "watchStartedOn", "finished date": "watchEndedOn", "year": "releasedYear", "total episodes": "totalEpisodes", "original language": "nativeLanguage", "language": "watchedLanguage", "ratings": "ratings", "catagory": "genres", "category": "genres", "original network": "network", "comments": "comments"}
    base_id = {"sheet1": 100, "feb 7 2023 onwards": 1000, "sheet2": 3000}.get(sheet.lower(), 0)
    processed =[]
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
        obj["againWatchedDates"] =[ddmmyyyy(d) for d in row[again_idx:] if ddmmyyyy(d)]
        
        sheet_lower = sheet.lower()
        obj["showType"] = "Movie" if "movie" in sheet_lower else "Mini Drama" if "mini" in sheet_lower else "Drama"
        
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
        if site:
            if key == 'cast':
                if site == "Manual" or not source_links.get('raw_cast'):
                    value = obj.get('cast')
                else:
                    value = source_links.get('raw_cast')
            else:
                value = obj.get(key)
                
            field_data = {"value": value, "source": site}
            if key in source_links: field_data["source_link"] = source_links[key]
            fetched[key] = field_data
    
    data = {"scriptVersion": SCRIPT_VERSION, "runID": context['run_id'], "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"), "showID": obj['showID'], "showName": obj['showName']}
    if fetched: data["fetchedFields"] = fetched
    if context.get('new_artists_added'): data["newArtistsAdded"] = context.get('new_artists_added')
    data["sitePriorityUsed"] = obj.get("sitePriorityUsed", {})
    if not fetched and not context.get('new_artists_added'): return

    path = os.path.join(BACKUP_META_DIR, f"META_{context['file_ts']}_{obj['showID']}.json"); os.makedirs(BACKUP_META_DIR, exist_ok=True)
    save_json_file(path, data)
    context['files_generated']['meta_backups'].append(path)

def create_diff_backup(old, new, context, explicit_changes=None):
    if explicit_changes is not None:
        changed_fields = explicit_changes
    else:
        changed_fields = {}
        for key, new_val in new.items():
            if key not in LOCKED_FIELDS_AFTER_CREATION and normalize_list(old.get(key)) != normalize_list(new_val):
                changed_fields[key] = {"old": old.get(key), "new": new_val}
                
    if not changed_fields: return
    
    data = {"scriptVersion": SCRIPT_VERSION, "runID": context['run_id'], "timestamp": now_ist().strftime("%d %B %Y %I:%M %p (IST)"), "backupType": "partial_diff", "showID": new['showID'], "showName": new['showName'], "releasedYear": new.get('releasedYear'), "updatedDetails": new.get('updatedDetails', 'Record Updated'), "changedFields": changed_fields}
    path = os.path.join(BACKUP_DIR, f"BACKUP_{context['file_ts']}_{new['showID']}.json"); os.makedirs(BACKUP_DIR, exist_ok=True)
    save_json_file(path, data)
    context['files_generated']['backups'].append(path)

# ---------------------------- UPDATED write_report ----------------------------

def write_report(context, current_run_seconds):
    
    total_seconds = int(context['cumulative_time_seconds'] + current_run_seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    if hours > 0:
        runtime_str = f"{hours} Hour{'s' if hours > 1 else ''} {minutes} Minute{'s' if minutes != 1 else ''} {seconds} Second{'s' if seconds != 1 else ''}"
    elif minutes > 0:
        runtime_str = f"{minutes} Minute{'s' if minutes != 1 else ''} {seconds} Second{'s' if seconds != 1 else ''}"
    else:
        runtime_str = f"{seconds} Second{'s' if seconds != 1 else ''}"

    if os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch':
        trigger_type = "Manual"
    else:
        trigger_type = "Automatic"
        
    end_time_ist = now_ist().strftime("%d %B %Y - %I:%M:%S %p")
    
    if context.get('paused'):
        status_msg = "✅ Workflow Batch completed successfully"
        batch_msg = "⏳ Batch Processing in Progress..."
    else:
        status_msg = "✅ Workflow Batch completed successfully"
        batch_msg = "🏁 Final Batch Completed"

    lines = [
        status_msg,
        batch_msg,
        "══════════════════════════════════════════════════════",
        "📊 My Movie Database – Excel to JSON Workflow Report",
        "══════════════════════════════════════════════════════",
        "",
        f"🚀 Workflow Type : {trigger_type}",
        f"🔁 RUN           : {os.environ.get('GITHUB_RUN_NUMBER', 'Local')}",
        f"⏰ Start Time    : {context['global_start_time']}",
        f"⏰ End Time      : {end_time_ist}",
        f"⏱️ Runtime       : {runtime_str}",
        f"⚙️ Max Process   : {os.environ.get('MAX_FETCHES', '50')} Row Per Run",
        f"🔄 Total Batches : {context.get('batch_run_count', 1)} Run{'s' if context.get('batch_run_count', 1) != 1 else ''}",
        ""
    ]

    sep, stats = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", {'created': 0, 'updated': 0, 'skipped': 0, 'deleted': 0, 'warnings': 0, 'show_images': 0, 'artist_images': 0, 'rows': 0, 'refetched': 0, 'archived': 0, 'artist_img_warn': 0}
    
    for sheet, changes in context['report_data'].items():
        if not any(v for k, v in changes.items()): continue
        display_sheet = sheet.replace("sheet", "Sheet ").title(); lines.extend([sep, f"🗂️ === {display_sheet} ==="]); lines.append(sep)
        if changes.get('created'): lines.append("\n🆕 Data Created:");[lines.append(f"- {o['showID']} - {o['showName']} ({o.get('releasedYear')}) -> {o.get('updatedDetails', '')}") for o in changes['created']]
        if changes.get('updated'): lines.append("\n🔁 Data Updated:");[lines.append(f"✍️ {p['new']['showID']} - {p['new']['showName']} -> {p['new']['updatedDetails']}") for p in changes['updated']]
        if changes.get('refetched'): lines.append("\n🔍 Refetched Data:");[lines.append(f"✨ {o['id']} - {o['name']} -> Fetched: {', '.join(o['fields'])}") for o in changes['refetched']]
        if changes.get('data_warnings'): lines.append("\n⚠️ Data Validation Warnings:");[lines.append(i) for i in changes['data_warnings']]
        if changes.get('fetched_data'): lines.append("\n🖼️ Fetched Data Details:");[lines.append(i) for i in sorted(changes['fetched_data'])]
        if changes.get('fetch_warnings'): lines.append("\n🕳️ Value Not Found:");[lines.append(i) for i in sorted(changes['fetch_warnings'])]
        if changes.get('artist_image_warnings'): lines.append("\n🧑‍🎨 Artist Image Warnings:");[lines.append(i) for i in sorted(changes['artist_image_warnings'])]
        if changes.get('skipped'): lines.append("\n🚫 Skipped (Unchanged):");[lines.append(f"- {i}") for i in sorted(changes['skipped'])]
        if changes.get('data_deleted'): lines.append("\n❌ Data Deleted:");[lines.append(i) for i in changes['data_deleted']]
        
        if sheet not in["Deleting Records", "Manual Updates"]:
            s = {k: len(v) for k, v in changes.items() if isinstance(v, list)}; total = sum(s.get(k, 0) for k in['created', 'updated', 'skipped', 'refetched'])
            stats['created'] += s.get('created', 0); stats['updated'] += s.get('updated', 0); stats['skipped'] += s.get('skipped', 0); stats['refetched'] += s.get('refetched', 0)
            stats['show_images'] += sum(1 for i in changes.get('fetched_data',[]) if "Show Image" in i); stats['rows'] += total
            stats['warnings'] += len(changes.get('data_warnings',[])) + len(changes.get('fetch_warnings',[])) + len(changes.get('artist_image_warnings',[]))
            lines.extend([f"\n📊 Summary (Sheet: {display_sheet})", sep, f"🆕 Created: {s.get('created', 0)}", f"🔁 Updated: {s.get('updated', 0)}", f"🔍 Refetched: {s.get('refetched', 0)}", f"🚫 Skipped: {s.get('skipped', 0)}", f"⚠️ Warnings: {len(changes.get('data_warnings',[])) + len(changes.get('fetch_warnings',[])) + len(changes.get('artist_image_warnings',[]))}", f"  Total Rows: {total}"])
        lines.append("")

    stats['deleted'] = len(context['files_generated'].get('deleted_data',[])); stats['artist_images'] = len(context['files_generated'].get('artist_images',[]))
    stats['archived'] = len(context['files_generated'].get('archived_backups',[])) + len(context['files_generated'].get('archived_meta_backups',[]))
    
    lines.extend([sep, "📊 Cumulative Batch Summary" if not context.get('paused') else "📊 Overall Summary", sep, f"🆕 Total Created: {stats['created']}", f"🔁 Total Updated: {stats['updated']}", f"🔍 Total Refetched: {stats['refetched']}", f"🖼️ Show Images Updated: {stats['show_images']}", f"🧑‍🎨 New Artist Images Added: {stats['artist_images']}", f"🚫 Total Skipped: {stats['skipped']}", f"❌ Total Deleted: {stats['deleted']}", f"🗄️ Total Archived Backups: {stats['archived']}", f"⚠️ Total Warnings: {stats['warnings']}", f"💾 Backup Files: {len(context['files_generated'].get('backups',[]))}", f"  Grand Total Rows Processed: {stats['rows']}", "", f"💾 Metadata Backups: {len(context['files_generated'].get('meta_backups',[]))}", ""])
    
    for file in[SERIES_JSON_FILE, ARTISTS_JSON_FILE, CAST_JSON_FILE, ARTIST_LOOKUP_FILE]:
        try:
            with open(file, 'r', encoding='utf-8') as f: lines.append(f"📦 Total Objects in {file}: {len(json.load(f))}")
        except Exception: lines.append(f"📦 Total Objects in {file}: 0")
        
    try:
        show_img_count = len([f for f in os.listdir(SHOW_IMAGES_DIR) if f.lower().endswith('.jpg')])
        lines.append(f"🖼️ Total images in {SHOW_IMAGES_DIR}: {show_img_count}")
    except Exception: lines.append(f"🖼️ Total images in {SHOW_IMAGES_DIR}: 0")

    try:
        artist_img_count = len([f for f in os.listdir(ARTIST_IMAGES_DIR) if f.lower().endswith('.jpg')])
        lines.append(f"🧑‍🎨 Total images in {ARTIST_IMAGES_DIR}: {artist_img_count}")
    except Exception: lines.append(f"🧑‍🎨 Total images in {ARTIST_IMAGES_DIR}: 0")
        
    lines.extend([sep, "🗂️ Folders Generated:", sep])
    for folder, files in context['files_generated'].items():
        if files: 
            total_files = len(files)
            lines.append(f"📁 {folder}/ (Total: {total_files} files)")
            if total_files <= 5:
                for p in files: lines.append(f"    📄 {os.path.basename(p)}")
            else:
                for p in files[:3]: lines.append(f"    📄 {os.path.basename(p)}")
                lines.append(f"    ... and {total_files - 3} more files.")
            lines.append("")
            
    if context.get('paused'):
        lines.extend([sep, "⚠️ BATCH LIMIT REACHED: The script paused safely.", "GitHub Actions will trigger next run automatically.", sep])
    else:
        lines.extend([sep, "🏁 Workflow finished successfully"])
        
    with open(context['report_file_path'], 'w', encoding='utf-8') as f: f.write("\n".join(lines))

def process_and_distribute_cast(full_cast, artists_db, context):
    main_cast, support_cast, guest_cast = [],[],[]
    crew_cast, other_crew_cast =[],[]
    context['new_artists_added'] =[]
    
    if not full_cast: return {}, {}

    known_crew_roles =['director', 'writer', 'screenwriter', 'composer', 'producer', 'creator', 'executive', 'editor', 'cinematographer', 'music', 'art']

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
            
            context['new_artists_added'].append({"artistID": artist_id, "artistName": artist['artistName'], "imageDownloaded": bool(image_downloaded)})
        
        role = artist['role']
        char_name = artist.get('characterName') 
        
        role_lower = role.lower()
        if 'main' in role_lower and ('role' in role_lower or 'cast' in role_lower): role = 'Main Role'
        elif 'support' in role_lower: role = 'Support Role'
        elif 'guest' in role_lower or 'cameo' in role_lower or 'bit part' in role_lower: role = 'Guest Role'
        else: role = role.title()
        
        cast_member = {"artistID": artist_id, "characterName": char_name, "role": role}
        
        if role == 'Main Role': main_cast.append(cast_member)
        elif role == 'Support Role': support_cast.append(cast_member)
        elif role == 'Guest Role': guest_cast.append(cast_member)
        else:
            if any(re.search(rf'\b{kcr}\b', role.lower()) for kcr in known_crew_roles):
                crew_cast.append(cast_member)
            else:
                other_crew_cast.append(cast_member)

    full_cast_dict = {}
    if main_cast: full_cast_dict['mainRoles'] = main_cast
    if support_cast: full_cast_dict['supportRoles'] = support_cast
    if guest_cast: full_cast_dict['guestRoles'] = guest_cast
    if crew_cast: full_cast_dict['crew'] = crew_cast
    if other_crew_cast: full_cast_dict['otherCrewMembers'] = other_crew_cast
    
    cast_summary = {}
    if main_cast: cast_summary["Main Role"] = len(main_cast)
    if support_cast: cast_summary["Support Role"] = len(support_cast)
    if guest_cast: cast_summary["Guest Role"] = len(guest_cast)
    
    for c in crew_cast:
        r = c['role']
        cast_summary[r] = cast_summary.get(r, 0) + 1
        
    for c in other_crew_cast:
        cast_summary['otherCrewMembers'] = cast_summary.get('otherCrewMembers', 0) + 1
    
    return cast_summary, full_cast_dict

def fetch_excel_from_gdrive_bytes(file_id, creds_path):
    if not HAVE_GOOGLE_API: return None
    try:
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        service = build('drive', 'v3', credentials=creds)
        try: request = service.files().get_media(fileId=file_id)
        except Exception: request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        fh = io.BytesIO(); downloader = MediaIoBaseDownload(fh, request); done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0); return fh
    except Exception as e: return None

def load_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f: return json.load(f)
    except FileNotFoundError:
        return {} if file_path in[ARTISTS_JSON_FILE, CAST_JSON_FILE] else[]
    except json.JSONDecodeError as e:
        print(f"\n❌ CRITICAL ERROR: {file_path} is corrupted!")
        sys.exit(1)

def save_json_file(file_path, data):
    temp_path = file_path + ".tmp"
    with open(temp_path, 'w', encoding='utf-8') as f: json.dump(data, f, indent=4, ensure_ascii=False)
    os.replace(temp_path, file_path)

# ---------------------------- UPDATED MAIN ENGINE ----------------------------

def main():
    
    MAX_FETCHES = int(os.environ.get("MAX_FETCHES", "50"))
    total_heavy_fetches = 0
    limit_reached = False

    run_start_time = now_ist()
    
    context = {
        'run_id': run_id_timestamp(), 'file_ts': filename_timestamp(),
        'report_data': {}, 'files_generated': {'backups':[], 'show_images':[], 'artist_images':[], 'deleted_data':[], 'deleted_images':[], 'meta_backups':[], 'reports':[], 'archived_backups':[], 'archived_meta_backups':[]},
        'cumulative_time_seconds': 0, 'global_start_time': run_start_time.strftime("%d %B %Y - %I:%M:%S %p"),
        'batch_run_count': 1, 'paused': False
    }
    
    merge_batch_state(context)

    if not (os.path.exists(EXCEL_FILE_ID_TXT) and os.path.exists(SERVICE_ACCOUNT_FILE)): sys.exit(1)
    with open(EXCEL_FILE_ID_TXT, 'r') as f: excel_id = f.read().strip()
    
    excel_bytes = fetch_excel_from_gdrive_bytes(excel_id, SERVICE_ACCOUNT_FILE)
    if not excel_bytes: sys.exit(1)

    xl = pd.ExcelFile(io.BytesIO(excel_bytes.getvalue()))

    process_deletions(xl, context)

    series_data = load_json_file(SERIES_JSON_FILE)
    artists_data = load_json_file(ARTISTS_JSON_FILE)
    cast_data = load_json_file(CAST_JSON_FILE)
    
    merged_by_id = {}
    for o in series_data:
        if o.get('showID'):
            try:
                sid_int = int(o['showID'])
                o['showID'] = sid_int
                merged_by_id[sid_int] = o
            except ValueError: pass
    
    manual_report = apply_manual_updates(xl, merged_by_id, context)
    if manual_report: context['report_data']['Manual Updates'] = manual_report

    sheets_to_process = [s.strip() for s in os.environ.get("SHEETS", "Sheet1").split(';') if s.strip()]
    
    for sheet in sheets_to_process:
        if limit_reached: break
        
        context['current_sheet'] = sheet
        report = context['report_data'].setdefault(sheet, {})
        excel_rows, warnings = excel_to_objects(xl, sheet)
        if warnings: report.setdefault('data_warnings', []).extend(warnings)

        for excel_obj in excel_rows:
            
            sid = excel_obj['showID']
            old_obj_from_json = merged_by_id.get(sid)
            is_new = old_obj_from_json is None
            
            # --- SCENARIO 3: CHECK IF ROW NEEDS WORK ---
            excel_data_has_changed = not is_new and objects_differ(old_obj_from_json, excel_obj)
            
            if is_new or excel_data_has_changed:
                
                # --- SCENARIO 3: CHECK LIMIT BEFORE STARTING WORK ---
                if MAX_FETCHES > 0 and total_heavy_fetches >= MAX_FETCHES:
                    limit_reached = True
                    context['paused'] = True
                    break
                
                total_heavy_fetches += 1
                
                base_template = copy.deepcopy(JSON_OBJECT_TEMPLATE)
                old_data = copy.deepcopy(old_obj_from_json) if old_obj_from_json else {}
                
                final_obj = {**base_template, **old_data, **excel_obj}
                
                for k in LOCKED_FIELDS_AFTER_CREATION:
                    if k in old_data and (old_data[k] or isinstance(old_data[k], (list, dict))):
                        if k == 'network': final_obj[k] = normalize_list(list(dict.fromkeys(normalize_list(old_data[k]) + normalize_list(excel_obj.get(k)))))
                        elif k == 'otherNames': final_obj[k] = _clean_other_names(normalize_list(old_data[k]) + normalize_list(excel_obj.get(k)))
                        else: final_obj[k] = old_data[k]
                
                final_obj['sitePriorityUsed'] = copy.deepcopy(final_obj.get('sitePriorityUsed') or JSON_OBJECT_TEMPLATE['sitePriorityUsed'])
                initial_metadata_state = {k: final_obj.get(k) for k in['synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'director', 'tags', 'cast', 'network', 'airedOn']}
                context['new_artists_added'] =[] 
                
                final_obj = fetch_and_populate_metadata(final_obj, context, artists_data)
                
                if 'cast' in final_obj and isinstance(final_obj['cast'], list):
                    cast_summary, full_cast_dict = process_and_distribute_cast(final_obj['cast'], artists_data, context)
                    final_obj['cast'] = cast_summary
                    if full_cast_dict: cast_data[str(sid)] = full_cast_dict
                
                final_obj.pop('extendedCastInfo', None)
                final_obj['topRatings'] = (final_obj.get("ratings", 0)) * (len(final_obj.get("againWatchedDates",[])) + 1) * 100
                
                metadata_was_fetched = any(final_obj.get(k) != v for k, v in initial_metadata_state.items())
                
                key_map = {'synopsis': 'Synopsis', 'showImage': 'Show Image', 'otherNames': 'Other Names', 'releaseDate': 'Release Date', 'Duration': 'Duration', 'director': 'Director', 'tags': 'Tags', 'cast': 'Cast', 'network': 'Network', 'airedOn': 'Aired On'}
                newly_fetched_fields = sorted([
                    key_map[k] for k, v in initial_metadata_state.items() 
                    if not v and bool(final_obj.get(k))
                ])

                if is_new:
                    final_obj['updatedDetails'] = "First Time Uploaded"
                    final_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                    report.setdefault('created',[]).append(final_obj)
                    if newly_fetched_fields: report.setdefault('fetched_data',[]).append(f"- {sid} - {final_obj['showName']} -> Fetched: {', '.join(newly_fetched_fields)}")
                else:
                    if excel_data_has_changed:
                        changes =[human_readable_field(k) for k, v in excel_obj.items() if normalize_list(old_obj_from_json.get(k)) != normalize_list(v) and k not in LOCKED_FIELDS_AFTER_CREATION]
                        final_obj['updatedDetails'] = f"{', '.join(changes)} Updated"
                        final_obj['updatedOn'] = now_ist().strftime('%d %B %Y')
                        report.setdefault('updated',[]).append({'old': old_obj_from_json, 'new': final_obj})
                        create_diff_backup(old_obj_from_json, final_obj, context)
                    
                    if metadata_was_fetched:
                        report.setdefault('refetched',[]).append({'id': sid, 'name': final_obj['showName'], 'fields': newly_fetched_fields})
                
                merged_by_id[sid] = final_obj
                save_metadata_backup(final_obj, context)

                missing_fields = {'synopsis', 'showImage', 'otherNames', 'releaseDate', 'Duration', 'director', 'tags', 'cast'}
                if final_obj.get('showType') != 'Movie':
                    missing_fields.update({'airedOn', 'network'})
                    
                missing =[human_readable_field(k) for k, v in final_obj.items() if k in missing_fields and not v]
                if missing: report.setdefault('fetch_warnings',[]).append(f"- {sid} - {final_obj['showName']} -> ⚠️ Missing: {', '.join(sorted(missing))}")
                
            else:
                report.setdefault('skipped',[]).append(f"{sid} - {excel_obj['showName']}")

    # Finalize
    duration = (now_ist() - run_start_time).total_seconds()
    
    if limit_reached:
        save_batch_state(context, current_run_seconds=duration)
        with open("RESUME_FLAG.txt", "w") as rf: rf.write("CONTINUE")
    else:
        if os.path.exists(BATCH_STATE_FILE): os.remove(BATCH_STATE_FILE)

    save_json_file(SERIES_JSON_FILE, sorted(merged_by_id.values(), key=lambda x: int(x.get('showID') or 0)))
    save_json_file(ARTISTS_JSON_FILE, artists_data)
    save_json_file(CAST_JSON_FILE, cast_data)

    artist_lookup_list =[{"artistID": k, "artistName": v['artistName']} for k, v in artists_data.items()]
    save_json_file(ARTIST_LOOKUP_FILE, sorted(artist_lookup_list, key=lambda x: x['artistName']))
    
    report_path = os.path.join(REPORTS_DIR, f"{filename_timestamp()}_REPORT.txt")
    os.makedirs(REPORTS_DIR, exist_ok=True)
    context['report_file_path'] = report_path
    context['files_generated']['reports'].append(report_path)
    
    write_report(context, current_run_seconds=duration)
    print(f"✅ Report written -> {report_path}")

if __name__ == '__main__':
    try: main()
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        sys.exit(1)