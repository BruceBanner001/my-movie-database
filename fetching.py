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

def _ddgs_search(query):
    """Performs a DuckDuckGo search and returns the first result URL."""
    if not HAVE_DDGS:
        return None
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=1))
            return results[0]['href'] if results else None
    except Exception:
        return None

def _get_page_html(site, show_name, release_year):
    """Finds the correct page on a site and returns its HTML content and URL."""
    query = f"{show_name} {release_year} site:{site}.com"
    page_url = _ddgs_search(query)
    if not page_url:
        return None, None
    
    try:
        response = requests.get(page_url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            return response.text, page_url
    except requests.RequestException:
        return None, None
    return None, None

def _parse_asianwiki_page(html):
    """Extracts all relevant data from an AsianWiki HTML page."""
    soup = BeautifulSoup(html, 'lxml')
    data = {}

    general_info = soup.select_one('div.pi-panel-content')
    if general_info:
        # Other Names
        other_names_div = general_info.find('div', {'data-source': 'title'})
        if other_names_div:
            cleaned = re.sub(r'^Drama:\s*', '', other_names_div.get_text(strip=True))
            data['otherNames'] = cleaned

        # Release Date
        release_date_div = general_info.find('div', {'data-source': 'airing_date'})
        if release_date_div:
            data['releaseDate'] = release_date_div.get_text(strip=True)

    # Synopsis - more robustly find the 'Plot' or 'Synopsis' header
    plot_header = soup.find('span', id=re.compile(r'Plot|Synopsis', re.I))
    if plot_header:
        synopsis_content = []
        # Find the parent h2 and iterate through its next siblings
        for sibling in plot_header.find_parent('h2').find_next_siblings():
            if sibling.name == 'h2': # Stop at the next header
                break
            if sibling.name == 'p':
                synopsis_content.append(sibling.get_text(strip=True))
        if synopsis_content:
            data['synopsis'] = ' '.join(synopsis_content)

    # Image
    image_tag = soup.find('a', class_='image-thumbnail').find('img') if soup.find('a', class_='image-thumbnail') else None
    if image_tag and image_tag.get('src'):
        # Clean up URL to get the base image
        data['image'] = image_tag['src'].split('/revision/')[0]

    return data

def _parse_mydramalist_page(html):
    """Extracts all relevant data from a MyDramaList HTML page."""
    soup = BeautifulSoup(html, 'lxml')
    data = {}

    # Image (from meta tag is most reliable)
    og_image = soup.find('meta', property='og:image')
    if og_image and og_image.get('content'):
        data['image'] = og_image['content']
    
    # Synopsis
    synopsis_div = soup.find('div', class_='show-synopsis')
    if synopsis_div:
        # Remove the "(Source: ...)" part if it exists at the end of the text
        synopsis_text = re.sub(r'\s*\(\s*Source:.*?\)\s*$', '', synopsis_div.get_text()).strip()
        data['synopsis'] = synopsis_text
        
    # Details List for Other Names, Release Date, Duration
    details_list = soup.find_all('li', class_='list-item')
    for item in details_list:
        header = item.find('b', class_='inline-block')
        if not header: continue
        
        header_text = header.get_text(strip=True)
        # Get the text of the parent li and remove the header text to get the value
        value_text = item.get_text(strip=True).replace(header_text, '').strip()

        if "Also Known As:" in header_text:
            data['otherNames'] = value_text
        elif "Aired:" in header_text:
            data['releaseDate'] = value_text
        elif "Duration:" in header_text:
            data['duration'] = value_text.replace("min.", "mins.")
    
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