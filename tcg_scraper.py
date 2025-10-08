
# tcg_scraper_basic.py
# Simple TCGplayer scraper that logs product data into sealed_market.db

from random import random
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import time

# --- CONFIG ---
PRODUCTS = [
    {
        "name": "Crown Zenith Elite Trainer Box",
        "url": "https://www.tcgplayer.com/product/487730/pokemon-crown-zenith-elite-trainer-box",  # example URL
        "source": "TCGplayer",
    }
]

DB_PATH = "sealed_market.db"

# --- FUNCTIONS ---

def fetch_page(url):
    """Fetch HTML from the product URL."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MarketTrackerBot/1.0)"
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.text


def parse_tcgplayer(html):
    """Extract listing count and lowest price from HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Example parsing (depends on page structure)
    # Listing count (rough proxy: number of seller listings visible)
    listings = soup.find_all("li", class_="listing-item")
    listing_count = len(listings)

    # Lowest price — find the first price element on page
    price_tag = soup.find("span", class_="price-point__data")
    lowest_price = None
    if price_tag:
        # Remove $ and convert to float
        price_text = price_tag.text.strip().replace("$", "")
        try:
            lowest_price = float(price_text)
        except ValueError:
            lowest_price = None

    return listing_count, lowest_price


def log_to_db(product_name, listing_count, lowest_price, source):
    """Insert scraped data into SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Find or create product ID
    c.execute("SELECT id FROM products WHERE name = ?", (product_name,))
    result = c.fetchone()
    if result:
        product_id = result[0]
    else:
        c.execute("INSERT INTO products (name, url) VALUES (?, ?)", (product_name, ""))
        product_id = c.lastrowid

    # Insert snapshot
    c.execute("""
        INSERT INTO listings (product_id, timestamp, listing_count, lowest_price, source)
        VALUES (?, ?, ?, ?, ?)
    """, (product_id, datetime.utcnow().isoformat(), listing_count, lowest_price, source))

    conn.commit()
    conn.close()
    print(f"Logged data for {product_name}: listings={listing_count}, price={lowest_price}")


# --- MAIN EXECUTION ---

for product in PRODUCTS:
    try:
        html = fetch_page(product["url"])
        listing_count, lowest_price = parse_tcgplayer(html)
        log_to_db(product["name"], listing_count, lowest_price, product["source"])
        time.sleep(20) + random.random()
    except Exception as e:
        print(f"Error scraping {product['name']}: {e}")
