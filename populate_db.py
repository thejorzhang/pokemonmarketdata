"""Populate sealed_market.db with listing snapshots from product links.

Usage examples:
    python3 populate_db.py --limit 5
    python3 populate_db.py --csv products.csv --db sealed_market.db --limit 10

Behavior:
- Reads CSV with header (name,url)
- For each product, fetches page HTML (requests). On failure saves diagnostic HTML.
- Parses a rough listing count and lowest price and inserts a snapshot into listings table.
"""
import csv
import sqlite3
import requests
from bs4 import BeautifulSoup
import argparse
import time
import random
import os
import json
import re
from datetime import datetime


DEFAULT_CSV = "products.csv"
DEFAULT_DB = "sealed_market.db"


 
try:
    # Optional selenium support for JS-rendered product pages
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False
def fetch_page(url, headers, timeout=12):
    try:
        print(f"[DEBUG] fetch_page: GET {url}")
        resp = requests.get(url, headers=headers, timeout=timeout)
        print(f"[DEBUG] fetch_page: Status {resp.status_code}, Length {len(resp.text)}")
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"[DEBUG] fetch_page: Exception {type(e).__name__}: {e}")
        return None


def make_driver(headless=True):
    """Create a Chrome WebDriver with modest anti-detection flags.

    Returns a webdriver.Chrome instance or raises.
    """
    opts = Options()
    if headless:
        # Use the newer headless mode when available
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,900")
    opts.add_argument("--log-level=3")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    return driver


def selenium_fetch_page(url, driver, wait_selector=None, timeout=12):
    """Render the URL in Selenium and return page_source.

    If wait_selector is provided, wait up to `timeout` seconds for it.
    Returns page_source or None on failure.
    """
    try:
        print(f"[DEBUG] selenium_fetch_page: GET {url}")
        driver.get(url)
        print(f"[DEBUG] selenium_fetch_page: Page loaded")
    except Exception as e:
        print(f"[DEBUG] selenium_fetch_page: driver.get() failed: {e}")
        return None

    if wait_selector:
        try:
            print(f"[DEBUG] selenium_fetch_page: Waiting for selector '{wait_selector}'")
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
            )
            print(f"[DEBUG] selenium_fetch_page: Selector found")
        except TimeoutException:
            print(f"[DEBUG] selenium_fetch_page: Selector timeout after {timeout}s")
            return driver.page_source

    # short sleep to let additional async load happen (kept small)
    time.sleep(1.0)
    page_source = driver.page_source
    print(f"[DEBUG] selenium_fetch_page: Got page_source, length {len(page_source)}")
    return page_source


def parse_tcgplayer(html):
    """Return dict with extracted fields: listing_count, lowest_price, market_price, listed_median, current_quantity, current_sellers, set_name.
    This uses simple heuristics and may need tweaks for page changes.
    """
    soup = BeautifulSoup(html, "html.parser")
    listing_count = None
    lowest_price = None
    market_price = None
    listed_median = None
    current_quantity = None
    current_sellers = None
    set_name = None

    # Extract set name from span[data-testid="lblProductDetailsSetName"]
    try:
        set_span = soup.select_one('span[data-testid="lblProductDetailsSetName"]')
        if set_span:
            set_name = set_span.get_text(strip=True)
    except Exception:
        pass

    # 1) Try JSON-LD <script type="application/ld+json"> — often contains offers.price
    try:
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                payload = json.loads(s.string or "{}")
            except Exception:
                txt = (s.string or "").strip()
                m = re.search(r"(\{.*\})", txt, re.S)
                if m:
                    try:
                        payload = json.loads(m.group(1))
                    except Exception:
                        payload = None
                else:
                    payload = None

            if not payload:
                continue

            offers = payload.get("offers") if isinstance(payload, dict) else None
            if offers:
                if isinstance(offers, list) and offers:
                    offer = offers[0]
                else:
                    offer = offers

                price = None
                if isinstance(offer, dict):
                    price = offer.get("price") or offer.get("priceCurrency")
                if price:
                    try:
                        lowest_price = float(str(price).replace(",", ""))
                    except Exception:
                        lowest_price = None

            desc = None
            if isinstance(payload, dict):
                desc = payload.get("description") or payload.get("name")
            if desc and listing_count is None:
                m = re.search(r"(\d{1,6})\s+listings", desc, re.I)
                if m:
                    try:
                        listing_count = int(m.group(1))
                    except Exception:
                        listing_count = None

            if listing_count is not None and lowest_price is not None:
                break
    except Exception:
        pass

    # 2) Try og:description meta tag for listing count fallback
    if listing_count is None:
        try:
            og = soup.find("meta", property="og:description")
            if og and og.get("content"):
                m = re.search(r"(\d{1,6})\s+listings", og.get("content"), re.I)
                if m:
                    listing_count = int(m.group(1))
        except Exception:
            pass

    # 3) Price guide section for market price, listed median, quantity, sellers
    try:
        # Market Price (span.price-points__upper__price under Market Price header)
        market_price_el = soup.select_one(".price-points__upper__header__title, .price-points__upper__price")
        if market_price_el and "Market Price" in market_price_el.get_text():
            price_el = market_price_el.find_next(class_="price-points__upper__price")
            if price_el:
                txt = price_el.get_text(strip=True).replace("$", "").replace(",", "")
                try:
                    market_price = float(txt)
                except Exception:
                    pass
        # Fallback: direct select
        if market_price is None:
            mp = soup.select_one(".price-points__upper__price")
            if mp:
                txt = mp.get_text(strip=True).replace("$", "").replace(",", "")
                try:
                    market_price = float(txt)
                except Exception:
                    pass

        # Listed Median (span.text: 'Listed Median:', then .price-points__lower__price)
        for row in soup.select(".price-points__lower tr"):
            label = row.find("span", class_="text")
            if label and "Listed Median" in label.get_text():
                val = row.find("span", class_="price-points__lower__price")
                if val:
                    txt = val.get_text(strip=True).replace("$", "").replace(",", "")
                    try:
                        listed_median = float(txt)
                    except Exception:
                        pass
            if label and "Current Quantity" in label.get_text():
                val = row.find("span", class_="price-points__lower__price")
                if val:
                    txt = val.get_text(strip=True).replace(",", "")
                    try:
                        current_quantity = int(txt)
                    except Exception:
                        pass
            if label and "Current Sellers" in label.get_text():
                val = row.find_all("span", class_="price-points__lower__price")
                if val and len(val) > 1:
                    txt = val[1].get_text(strip=True).replace(",", "")
                    try:
                        current_sellers = int(txt)
                    except Exception:
                        pass
                elif val:
                    txt = val[0].get_text(strip=True).replace(",", "")
                    try:
                        current_sellers = int(txt)
                    except Exception:
                        pass
    except Exception:
        pass

    # 4) Price fallbacks — look for visible price elements (lowest price)
    if lowest_price is None:
        try:
            price_candidates = soup.select("span.price-point__data, span.price, div.price, span[itemprop=price]")
            prices = []
            for p in price_candidates:
                txt = p.get_text(strip=True).replace("$", "").replace(",", "")
                try:
                    val = float(txt)
                    prices.append(val)
                except Exception:
                    m = re.search(r"(\d+[.,]?\d*)", txt)
                    if m:
                        try:
                            prices.append(float(m.group(1).replace(",", "")))
                        except Exception:
                            continue
            if prices:
                lowest_price = min(prices)
        except Exception:
            pass

    # 5) Final fallback: try to parse any visible text for "N listings"
    if listing_count is None:
        try:
            body_text = soup.get_text(separator=" ", strip=True)
            m = re.search(r"(\d{1,6})\s+listings", body_text, re.I)
            if m:
                listing_count = int(m.group(1))
        except Exception:
            pass

    return {
        "listing_count": listing_count,
        "lowest_price": lowest_price,
        "market_price": market_price,
        "listed_median": listed_median,
        "current_quantity": current_quantity,
        "current_sellers": current_sellers,
        "set_name": set_name,
    }


def ensure_product(conn, name, url):
    c = conn.cursor()
    # Try to find by URL first (prefer unique)
    if url:
        c.execute("SELECT id FROM products WHERE url = ?", (url,))
        row = c.fetchone()
        if row:
            return row[0]

    # Otherwise try by exact name
    c.execute("SELECT id FROM products WHERE name = ?", (name,))
    row = c.fetchone()
    if row:
        return row[0]

    # Insert new product
    c.execute("INSERT INTO products (name, url) VALUES (?, ?)", (name, url))
    conn.commit()
    return c.lastrowid


def insert_snapshot(conn, product_id, listing_count, lowest_price, market_price=None, listed_median=None, current_quantity=None, current_sellers=None, set_name=None, source="TCGplayer"):
    c = conn.cursor()
    try:
        c.execute(
            """
            INSERT INTO listings (
                product_id, timestamp, listing_count, lowest_price, median_price, market_price, current_quantity, current_sellers, set_name, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                product_id,
                datetime.utcnow().isoformat(),
                listing_count,
                lowest_price,
                listed_median,
                market_price,
                current_quantity,
                current_sellers,
                set_name,
                source,
            ),
        )
        conn.commit()
    except Exception as e:
        print(f"[DEBUG] DB insertion error: {e}")
        raise


def save_diagnostic(html, out_dir, prefix):
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    fname = os.path.join(out_dir, f"{prefix}_{ts}.html")
    try:
        with open(fname, "w", encoding="utf-8") as fh:
            fh.write(html)
        return fname
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=DEFAULT_CSV, help="CSV file with name,url columns")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB file path")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of products to process (0 = all)")
    parser.add_argument("--delay-min", type=float, default=2.0, help="Minimum delay between requests")
    parser.add_argument("--delay-max", type=float, default=5.0, help="Maximum delay between requests")
    parser.add_argument("--diagnostics-dir", default="diagnostics", help="Directory to save failed HTML pages")
    parser.add_argument("--selenium", action="store_true", help="Use Selenium to render JS product pages (slow but robust)")
    parser.add_argument("--headless", action="store_true", help="When using --selenium, run Chrome headless")
    args = parser.parse_args()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Open DB
    conn = sqlite3.connect(args.db)

    driver = None
    if args.selenium:
        if not SELENIUM_AVAILABLE:
            print("Selenium not available in this environment. Install selenium and chromedriver.")
            return
        try:
            print("Starting Selenium Chrome (headless=%s) ..." % args.headless)
            driver = make_driver(headless=args.headless)
        except Exception as e:
            print("Failed to start Selenium driver:", e)
            return

    count_processed = 0
    count_failed = 0

    with open(args.csv, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for i, row in enumerate(reader, start=1):
            if args.limit and count_processed >= args.limit:
                break

            name = (row.get("name") or row.get("title") or "").strip()
            url = (row.get("url") or row.get("link") or "").strip()
            print(f"\n[{i}] Processing: {name}")
            print(f"    URL: {url}")

            if not url:
                print("  -> No URL, skipping")
                continue

            # Fetch HTML
            html = fetch_page(url, headers)
            print(f"    Fetch result: {'Got HTML (%d bytes)' % len(html) if html else 'None'}")
            
            # If requests failed, try Selenium if enabled
            if not html and args.selenium:
                print("    -> Requests failed, trying Selenium...")
                try:
                    html = selenium_fetch_page(url, driver, wait_selector="li.listing-item, span.price-point__data", timeout=8)
                    if html:
                        print(f"    Selenium result: Got HTML (%d bytes)" % len(html))
                    else:
                        print("    Selenium result: None")
                except Exception as e:
                    print("    -> Selenium fetch failed:", e)
            
            if not html:
                print("  -> Failed to fetch page, saving diagnostic and continuing")
                count_failed += 1
                save_diagnostic("", args.diagnostics_dir, f"fetchfail_{i}")
                time.sleep(random.uniform(args.delay_min, args.delay_max))
                continue

            # Parse
            print(f"    Parsing HTML...")
            parsed = parse_tcgplayer(html)
            
            # If parser got no data and Selenium is available, retry with Selenium (page was likely JS-rendered)
            if (parsed.get('listing_count') is None and 
                parsed.get('market_price') is None and 
                parsed.get('listed_median') is None and 
                args.selenium and driver):
                print("    -> Parser found no data (likely JS-rendered). Retrying with Selenium...")
                try:
                    html_selenium = selenium_fetch_page(url, driver, wait_selector=".price-points__upper__price, .price-points__lower", timeout=10)
                    if html_selenium:
                        print(f"    Selenium retry: Got HTML (%d bytes), re-parsing..." % len(html_selenium))
                        parsed = parse_tcgplayer(html_selenium)
                except Exception as e:
                    print(f"    -> Selenium retry failed: {e}")
            
            # Insert into DB
            try:
                product_id = ensure_product(conn, name, url)
                insert_snapshot(
                    conn,
                    product_id,
                    parsed.get('listing_count'),
                    parsed.get('lowest_price'),
                    market_price=parsed.get('market_price'),
                    listed_median=parsed.get('listed_median'),
                    current_quantity=parsed.get('current_quantity'),
                    current_sellers=parsed.get('current_sellers'),
                    set_name=parsed.get('set_name'),
                )
                print(f"  -> Logged: listings={parsed.get('listing_count')}, lowest_price={parsed.get('lowest_price')}, market=${parsed.get('market_price')}, median=${parsed.get('listed_median')}, qty={parsed.get('current_quantity')}, sellers={parsed.get('current_sellers')}, set={parsed.get('set_name')}")
                count_processed += 1
            except Exception as e:
                print(f"  -> DB error: {e}")
                save_diagnostic(html, args.diagnostics_dir, f"dberror_{i}")
                count_failed += 1

            # politeness delay
            time.sleep(random.uniform(args.delay_min, args.delay_max))

    conn.close()

    print(f"Done. Processed: {count_processed}, Failed: {count_failed}")


if __name__ == "__main__":
    main()
