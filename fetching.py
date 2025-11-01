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
    """Performs a DuckDuckGo search and returns results."""
    if not HAVE_DDGS:
        return []
    try:
        with DDGS() as ddgs:
            if type == 'text':
                return list(ddgs.text(query, max_results=max_results))
            elif type == 'images':
                return [r.get("image") for r in ddgs.images(query, max_results=max_results) if r.get("image")]
    except Exception:
        return []

def _get_page_html(site, show_name, release_year):
    """
    Finds the correct page on a site by checking multiple search results
    and returns its HTML content and URL.
    """
    query = f"{show_name} {release_year} site:{site}.com"
    results = _ddgs_search(query, type='text')
    
    for result in results:
        page_url = result.get('href')
        if not page_url:
            continue
        try:
            response = requests.get(page_url, headers=HEADERS, timeout=15)
            if response.status_code == 200:
                # Basic check to see if the page is relevant
                if show_name.lower().split()[0] in response.text.lower():
                    return response.text, page_url
        except requests.RequestException:
            continue
    return None, None

def _parse_asianwiki_page(html):
    """Extracts data from AsianWiki using robust text-based searching."""
    soup = BeautifulSoup(html, 'lxml')
    data = {}
    
    # Image
    image_tag = soup.select_one("a.image-thumbnail img")
    if image_tag and image_tag.get('src'):
        data['image'] = image_tag['src'].split('/revision/')[0]

    # Synopsis
    plot_header = soup.find(['h2', 'h3'], id=re.compile(r'Plot|Synopsis', re.I))
    if plot_header:
        synopsis_content = []
        for sibling in plot_header.find_next_siblings():
            if sibling.name in ['h2', 'h3']: break
            if sibling.name == 'p': synopsis_content.append(sibling.get_text(strip=True))
        if synopsis_content: data['synopsis'] = ' '.join(synopsis_content)

    # Use regex on text content for other details for maximum robustness
    text_content = soup.get_text(" ", strip=True)
    
    other_names_match = re.search(r'English title\)\s*/\s*(.*?)\s*\(literal title\)', text_content)
    if other_names_match:
        data['otherNames'] = other_names_match.group(1).strip()
    
    release_date_match = re.search(r'Release Date:\s*([A-Za-z0-9,\s\-]+)', text_content)
    if release_date_match:
        data['releaseDate'] = release_date_match.group(1).strip()
        
    return data

def _parse_mydramalist_page(html):
    """Extracts data from MyDramaList using robust text-based searching."""
    soup = BeautifulSoup(html, 'lxml')
    data = {}

    og_image = soup.find('meta', property='og:image')
    if og_image and og_image.get('content'):
        data['image'] = og_image['content']

    synopsis_div = soup.find('div', class_='show-synopsis')
    if synopsis_div:
        synopsis_text = re.sub(r'\s*\(\s*Source:.*?\)\s*$', '', synopsis_div.get_text()).strip()
        data['synopsis'] = synopsis_text
    
    # Search the entire page for text labels
    page_text = soup.get_text()
    
    # Duration
    duration_match = re.search(r'Duration:\s*([\w\s.]+)', page_text)
    if duration_match:
        data['duration'] = duration_match.group(1).replace("min.", "mins.").strip()

    # Release Date
    release_date_match = re.search(r'Aired:\s*([A-Za-z]{3}\s\d{1,2},\s\d{4})', page_text)
    if release_date_match:
        data['releaseDate'] = release_date_match.group(1).strip()
    
    # Other Names
    other_names_match = re.search(r'Also Known As:\s*([\w\s,]+)', page_text)
    if other_names_match:
        data['otherNames'] = other_names_match.group(1).strip()

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