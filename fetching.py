# fetching.py

import re
from bs4 import BeautifulSoup
import requests

try:
    from ddgs import DDGS
    HAVE_DDGS = True
except ImportError:
    HAVE_DDGS = False

# --- CONFIGURATION ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# --- HELPER FUNCTIONS ---

def _ddgs_search(query, type='text', max_results=3):
    if not HAVE_DDGS: return []
    try:
        with DDGS() as ddgs:
            if type == 'text':
                return list(ddgs.text(query, max_results=max_results))
            elif type == 'images':
                return [r.get("image") for r in ddgs.images(query, max_results=max_results) if r.get("image")]
    except Exception:
        return []

def _get_page_html(site, show_name, release_year):
    query = f"{show_name} {release_year} site:{site}.com"
    results = _ddgs_search(query, type='text')
    
    for result in results:
        page_url = result.get('href')
        if not page_url: continue
        try:
            response = requests.get(page_url, headers=HEADERS, timeout=15)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                title = soup.title.string.lower() if soup.title else ''
                if show_name.lower() in title and str(release_year) in title:
                    return response.text, page_url
        except requests.RequestException:
            continue
    return None, None

def _find_detail_by_label(soup, label_regex):
    element = soup.find(string=re.compile(label_regex, re.I))
    if element:
        # Navigate up to a common parent (like <li> or <div>) and get all its text
        parent = element.find_parent(['li', 'div'])
        if parent:
            # Clean the text by removing the label itself
            return re.sub(label_regex, '', parent.get_text(" ", strip=True), flags=re.I).strip()
    return None

def _parse_asianwiki_page(html):
    soup = BeautifulSoup(html, 'lxml')
    data = {}

    # Image
    image_tag = soup.select_one("td.ent-img img")
    if image_tag and image_tag.get('src'):
        data['image'] = 'https://asianwiki.com' + image_tag['src']

    # Synopsis
    plot_header = soup.find('h2', string='Plot Synopsis')
    if plot_header:
        synopsis_p = plot_header.find_next_sibling('p')
        if synopsis_p: data['synopsis'] = synopsis_p.get_text(strip=True)

    # Details from the profile section
    profile_text = soup.select_one('div.profile_container').get_text(" ", strip=True) if soup.select_one('div.profile_container') else ''
    
    release_match = re.search(r'Release Date:\s*([^|]+)', profile_text)
    if release_match: data['releaseDate'] = release_match.group(1).strip()
    
    duration_match = re.search(r'Runtime:\s*([^|]+)', profile_text)
    if duration_match: data['duration'] = duration_match.group(1).strip() + " mins."

    return data

def _parse_mydramalist_page(html):
    soup = BeautifulSoup(html, 'lxml')
    data = {}

    og_image = soup.find('meta', property='og:image')
    if og_image and og_image.get('content'): data['image'] = og_image['content']

    synopsis_div = soup.find('div', class_='show-synopsis')
    if synopsis_div:
        synopsis_text = re.sub(r'\s*\(\s*Source:.*?\)\s*$', '', synopsis_div.get_text()).strip()
        data['synopsis'] = synopsis_text
    
    data['otherNames'] = _find_detail_by_label(soup, r'Also Known As:')
    data['releaseDate'] = _find_detail_by_label(soup, r'Aired:')
    duration_text = _find_detail_by_label(soup, r'Duration:')
    if duration_text: data['duration'] = duration_text.replace("min.", "mins.")

    return data

# ============================================================
# 🏮 ASIANWIKI FETCHING BLOCKS
# ============================================================

def fetch_synopsis_from_asianwiki(show_name, release_year):
    html, url = _get_page_html('asianwiki', show_name, release_year)
    return (_parse_asianwiki_page(html).get('synopsis'), url) if html else (None, None)

def fetch_image_from_asianwiki(show_name, release_year, show_id):
    html, url = _get_page_html('asianwiki', show_name, release_year)
    return (_parse_asianwiki_page(html).get('image'), url) if html else (None, None)

def fetch_othernames_from_asianwiki(show_name, release_year):
    html, url = _get_page_html('asianwiki', show_name, release_year)
    return (_parse_asianwiki_page(html).get('otherNames'), url) if html else (None, None)

def fetch_duration_from_asianwiki(show_name, release_year):
    html, url = _get_page_html('asianwiki', show_name, release_year)
    return (_parse_asianwiki_page(html).get('duration'), url) if html else (None, None)

def fetch_release_date_from_asianwiki(show_name, release_year):
    html, url = _get_page_html('asianwiki', show_name, release_year)
    return (_parse_asianwiki_page(html).get('releaseDate'), url) if html else (None, None)

# ============================================================
# 🌏 MYDRAMALIST FETCHING BLOCKS
# ============================================================

def fetch_synopsis_from_mydramalist(show_name, release_year):
    html, url = _get_page_html('mydramalist', show_name, release_year)
    return (_parse_mydramalist_page(html).get('synopsis'), url) if html else (None, None)

def fetch_image_from_mydramalist(show_name, release_year, show_id):
    html, url = _get_page_html('mydramalist', show_name, release_year)
    return (_parse_mydramalist_page(html).get('image'), url) if html else (None, None)

def fetch_othernames_from_mydramalist(show_name, release_year):
    html, url = _get_page_html('mydramalist', show_name, release_year)
    return (_parse_mydramalist_page(html).get('otherNames'), url) if html else (None, None)

def fetch_duration_from_mydramalist(show_name, release_year):
    html, url = _get_page_html('mydramalist', show_name, release_year)
    return (_parse_mydramalist_page(html).get('duration'), url) if html else (None, None)

def fetch_release_date_from_mydramalist(show_name, release_year):
    html, url = _get_page_html('mydramalist', show_name, release_year)
    return (_parse_mydramalist_page(html).get('releaseDate'), url) if html else (None, None)