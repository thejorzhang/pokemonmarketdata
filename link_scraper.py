# tcgplayer_sealed_catalogue_scraper.py
# Scrapes all sealed Pokémon products from TCGplayer search pages
# and saves them to products.csv

import requests
from bs4 import BeautifulSoup
import csv
import time
import random

# --- CONFIG ---
BASE_URL = "https://www.tcgplayer.com/search/pokemon/product?productLineName=pokemon&page={page}&view=grid&ProductTypeName=Sealed+Products"
OUTPUT_CSV = "products.csv"
MAX_PAGES = 108  # adjust based on total pages

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Dest": "document"
}

# --- FUNCTIONS ---

def fetch_search_page(page_num):
    url = BASE_URL.format(page=page_num)
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.text

def parse_search_page(html):
    """Extract product names and URLs from a search results page"""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    products = []

    # Each product is an <a> with class 'product-card__image--0' (or similar)
    for a_tag in soup.find_all("a", href=True):
        # Only pick links that match the product pattern
        if "/product/" in a_tag["href"]:
            url = "https://www.tcgplayer.com" + a_tag["href"]
            name_tag = a_tag.find("span", class_="product-card__title")
            if name_tag:
                name = name_tag.text.strip()
                products.append((name, url))

    return products

# --- MAIN EXECUTION ---

all_products = []

for page_num in range(1, MAX_PAGES + 1):
    try:
        print(f"Fetching page {page_num}")
        html = fetch_search_page(page_num)
        page_products = parse_search_page(html)
        if not page_products:
            print("No products found, assuming end of catalogue.")
            break
        all_products.extend(page_products)
        # Random sleep to reduce chance of blocking
        time.sleep(10 + random.random())
    except Exception as e:
        print(f"Error on page {page_num}: {e}")
        break

# Save to CSV
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["name", "url"])
    writer.writerows(all_products)

print(f"Saved {len(all_products)} products to {OUTPUT_CSV}")