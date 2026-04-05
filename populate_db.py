"""Populate sealed_market.db with listing snapshots from product links.

Usage examples:
    python3 populate_db.py --limit 5
    python3 populate_db.py --csv products.csv --db sealed_market.db --limit 10 --selenium

Behavior:
- Reads CSV with header (name,url)
- For each product, fetches page HTML (requests, then Selenium if needed)
- Parses listing count, prices, quantities, sellers, set name
- Inserts snapshot into listings table
"""
import csv
import requests
from bs4 import BeautifulSoup
import argparse
import time
import random
import os
import json
import re
import hashlib
from datetime import datetime
import sys

from db import (
    configure_connection as configure_db_connection,
    connect_database,
    get_dialect,
    id_column_sql,
    insert_row_returning_id,
    resolve_database_target,
    sql_placeholder_list,
    table_columns as db_table_columns,
)


DEFAULT_CSV = "products.csv"
DEFAULT_DB = "sealed_market.db"
RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}
NON_RETRYABLE_HTTP_STATUSES = {400, 401, 403, 404, 410}
DEFAULT_COMMIT_EVERY = 25
DEBUG = False


def debug_log(message):
    if DEBUG:
        print(message, flush=True)


def print_progress(current, total, processed, failed, status=""):
    """Print formatted progress bar."""
    if total == 0:
        pct = 0
    else:
        pct = int((current / total) * 100)
    bar_len = 30
    filled = int((pct / 100) * bar_len)
    bar = "█" * filled + "░" * (bar_len - filled)
    status_msg = f" | {status}" if status else ""
    print(f"\r[{bar}] {pct}% ({current}/{total}) | Processed: {processed} | Failed: {failed}{status_msg}", end="", flush=True)


def table_columns(conn, table_name):
    return db_table_columns(conn, table_name)


def configure_connection(conn):
    configure_db_connection(conn)


def ensure_runtime_schema(conn):
    """Backfill schema on existing DBs without requiring manual migration."""
    dialect = get_dialect(conn)
    pk = id_column_sql(dialect)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            {pk},
            name TEXT NOT NULL,
            url TEXT,
            release_date TEXT,
            sku_code TEXT
        )
        """.format(pk=pk)
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS listings (
            {pk},
            product_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            snapshot_date TEXT,
            listing_count INTEGER,
            lowest_price REAL,
            lowest_shipping REAL,
            lowest_total_price REAL,
            median_price REAL,
            market_price REAL,
            current_quantity INTEGER,
            current_sellers INTEGER,
            set_name TEXT,
            condition TEXT,
            source TEXT,
            run_id INTEGER,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        """.format(pk=pk)
    )
    listings_cols = table_columns(conn, "listings")
    if "snapshot_date" not in listings_cols:
        c.execute("ALTER TABLE listings ADD COLUMN snapshot_date TEXT")
        c.execute("UPDATE listings SET snapshot_date = substr(timestamp, 1, 10) WHERE snapshot_date IS NULL")
    if "run_id" not in listings_cols:
        c.execute("ALTER TABLE listings ADD COLUMN run_id INTEGER")
    if "lowest_shipping" not in listings_cols:
        c.execute("ALTER TABLE listings ADD COLUMN lowest_shipping REAL")
    if "lowest_total_price" not in listings_cols:
        c.execute("ALTER TABLE listings ADD COLUMN lowest_total_price REAL")

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_runs (
            {pk},
            source TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,
            csv_path TEXT,
            args_json TEXT,
            attempted_count INTEGER DEFAULT 0,
            processed_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            parse_failed_count INTEGER DEFAULT 0
        )
        """.format(pk=pk)
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_failures (
            {pk},
            run_id INTEGER NOT NULL,
            product_name TEXT,
            url TEXT,
            stage TEXT NOT NULL,
            reason TEXT NOT NULL,
            http_status INTEGER,
            attempts INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES scrape_runs (id)
        )
        """.format(pk=pk)
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            {pk},
            product_id INTEGER NOT NULL,
            sale_date TEXT NOT NULL,
            condition_raw TEXT,
            variant TEXT,
            language TEXT,
            quantity INTEGER,
            purchase_price REAL,
            shipping_price REAL,
            listing_type TEXT,
            title TEXT,
            custom_listing_key TEXT,
            custom_listing_id TEXT,
            source TEXT NOT NULL,
            sale_fingerprint TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        """.format(pk=pk)
    )
    sales_cols = table_columns(conn, "sales")
    if "shipping_price" not in sales_cols:
        c.execute("ALTER TABLE sales ADD COLUMN shipping_price REAL")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS card_sales (
            {pk},
            card_product_id INTEGER NOT NULL,
            sale_date TEXT NOT NULL,
            condition_raw TEXT,
            variant TEXT,
            language TEXT,
            quantity INTEGER,
            purchase_price REAL,
            shipping_price REAL,
            listing_type TEXT,
            title TEXT,
            custom_listing_key TEXT,
            custom_listing_id TEXT,
            source TEXT NOT NULL,
            sale_fingerprint TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (card_product_id) REFERENCES card_products (id)
        )
        """.format(pk=pk)
    )
    card_sales_cols = table_columns(conn, "card_sales")
    if "shipping_price" not in card_sales_cols:
        c.execute("ALTER TABLE card_sales ADD COLUMN shipping_price REAL")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS product_details (
            product_id INTEGER PRIMARY KEY,
            tcgplayer_product_id INTEGER,
            source_url TEXT,
            url_slug TEXT,
            raw_title TEXT,
            set_name TEXT,
            product_line TEXT,
            product_type TEXT,
            product_subtype TEXT,
            release_date TEXT,
            source TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS sets (
            {pk},
            name TEXT NOT NULL,
            category_slug TEXT,
            product_line TEXT,
            source TEXT NOT NULL,
            set_type TEXT,
            release_date TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """.format(pk=pk)
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS card_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_id INTEGER,
            tcgplayer_product_id INTEGER,
            name TEXT NOT NULL,
            url TEXT,
            category_slug TEXT,
            product_line TEXT,
            set_name TEXT,
            source TEXT NOT NULL,
            discovered_at TEXT NOT NULL,
            FOREIGN KEY (set_id) REFERENCES sets (id)
        )
        """
    )
    card_product_cols = table_columns(conn, "card_products")
    if "set_id" not in card_product_cols:
        c.execute("ALTER TABLE card_products ADD COLUMN set_id INTEGER")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS card_details (
            card_product_id INTEGER PRIMARY KEY,
            tcgplayer_product_id INTEGER,
            source_url TEXT,
            raw_title TEXT,
            set_name TEXT,
            card_number TEXT,
            rarity TEXT,
            finish TEXT,
            language TEXT,
            supertype TEXT,
            subtype TEXT,
            release_date TEXT,
            source TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (card_product_id) REFERENCES card_products (id)
        )
        """
    )
    c.execute(
        """
        DELETE FROM listings
        WHERE id IN (
            SELECT older.id
            FROM listings AS older
            JOIN listings AS newer
              ON older.product_id = newer.product_id
             AND COALESCE(older.source, '') = COALESCE(newer.source, '')
             AND older.snapshot_date = newer.snapshot_date
             AND older.snapshot_date IS NOT NULL
             AND (
                 older.timestamp < newer.timestamp
                 OR (older.timestamp = newer.timestamp AND older.id < newer.id)
             )
        )
        """
    )
    c.execute("DROP INDEX IF EXISTS idx_listings_product_source_run")
    c.execute("CREATE INDEX IF NOT EXISTS idx_listings_run_id ON listings (run_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_listings_product_source_snapshot_date ON listings (product_id, source, snapshot_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_scrape_failures_run ON scrape_failures (run_id, stage, reason)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sales_product_sale_date ON sales (product_id, sale_date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_sales_product_sale_date ON card_sales (card_product_id, sale_date)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sets_name_product_line_unique ON sets (name, product_line)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sets_product_line_name ON sets (product_line, name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_product_details_tcgplayer_product_id ON product_details (tcgplayer_product_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_products_tcgplayer_product_id ON card_products (tcgplayer_product_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_products_set_id ON card_products (set_id)")
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_card_products_url_unique
        ON card_products (url)
        WHERE url IS NOT NULL AND url != ''
        """
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_details_tcgplayer_product_id ON card_details (tcgplayer_product_id)")
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_products_url_unique
        ON products (url)
        WHERE url IS NOT NULL AND url != ''
        """
    )
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_product_source_snapshot_unique
        ON listings (product_id, source, snapshot_date)
        WHERE snapshot_date IS NOT NULL
        """
    )
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_product_fingerprint_unique
        ON sales (product_id, sale_fingerprint)
        """
    )
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_card_sales_product_fingerprint_unique
        ON card_sales (card_product_id, sale_fingerprint)
        """
    )
    conn.commit()


def start_scrape_run(conn, source, csv_path, args_dict):
    c = conn.cursor()
    if get_dialect(conn) == "postgres":
        c.execute(
            """
            INSERT INTO scrape_runs (source, started_at, status, csv_path, args_json)
            VALUES (%s, %s, 'running', %s, %s)
            RETURNING id
            """,
            (
                source,
                datetime.utcnow().isoformat(),
                csv_path,
                json.dumps(args_dict, sort_keys=True),
            ),
        )
        run_id = c.fetchone()[0]
    else:
        c.execute(
            """
            INSERT INTO scrape_runs (source, started_at, status, csv_path, args_json)
            VALUES (?, ?, 'running', ?, ?)
            """,
            (
                source,
                datetime.utcnow().isoformat(),
                csv_path,
                json.dumps(args_dict, sort_keys=True),
            ),
        )
        run_id = c.lastrowid
    conn.commit()
    return run_id


def finalize_scrape_run(conn, run_id, status, attempted, processed, failed, parse_failed):
    c = conn.cursor()
    ph = "%s" if get_dialect(conn) == "postgres" else "?"
    c.execute(
        """
        UPDATE scrape_runs
        SET ended_at = {ph}, status = {ph}, attempted_count = {ph}, processed_count = {ph}, failed_count = {ph}, parse_failed_count = {ph}
        WHERE id = {ph}
        """.format(ph=ph),
        (datetime.utcnow().isoformat(), status, attempted, processed, failed, parse_failed, run_id),
    )
    conn.commit()


def record_failure(conn, run_id, product_name, url, stage, reason, http_status=None, attempts=None):
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO scrape_failures (run_id, product_name, url, stage, reason, http_status, attempts, created_at)
        VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        """.format(ph="%s" if get_dialect(conn) == "postgres" else "?"),
        (
            run_id,
            product_name,
            url,
            stage,
            reason,
            http_status,
            attempts,
            datetime.utcnow().isoformat(),
        ),
    )


def has_minimum_parse_data(parsed):
    return any(
        parsed.get(k) is not None
        for k in ("lowest_price", "market_price", "listing_count")
    )

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


def is_driver_alive(driver):
    if not driver:
        return False
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def summarize_exception(exc):
    text = str(exc).strip()
    if not text:
        return type(exc).__name__
    return text.splitlines()[0][:220]


def parse_money(text):
    if text is None:
        return None
    cleaned = str(text).strip().replace("$", "").replace(",", "")
    if not cleaned:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", cleaned)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def parse_integer(text):
    if text is None:
        return None
    cleaned = str(text).strip().replace(",", "")
    if not cleaned:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", cleaned)
    if not m:
        return None
    try:
        return int(float(m.group(1)))
    except Exception:
        return None


def page_looks_like_tcgplayer_shell(driver):
    """Detect TCGplayer's generic shell/challenge page before product data hydrates."""
    try:
        title = (driver.title or "").strip()
    except Exception:
        title = ""

    if title == "Your Trusted Marketplace for Collectible Trading Card Games - TCGplayer":
        return True

    try:
        source = driver.page_source or ""
    except Exception:
        source = ""

    if len(source) < 50000 and "product-details__listings" not in source and "price-points__upper__price" not in source:
        return True
    return False


def fetch_page_with_retries(session, url, headers, timeout=12, max_retries=3, base_backoff=1.25):
    """Return (html, status_code, attempts, reason)."""
    attempts = 0
    last_reason = "unknown_error"
    last_status = None

    while attempts < max_retries:
        attempts += 1
        try:
            debug_log(f"[DEBUG] fetch_page: GET {url} (attempt {attempts}/{max_retries})")
            resp = session.get(url, headers=headers, timeout=timeout)
            last_status = resp.status_code
            debug_log(f"[DEBUG] fetch_page: Status {resp.status_code}, Length {len(resp.text)}")

            if resp.status_code in NON_RETRYABLE_HTTP_STATUSES:
                return None, resp.status_code, attempts, f"http_{resp.status_code}_non_retryable"

            if resp.status_code in RETRYABLE_HTTP_STATUSES:
                last_reason = f"http_{resp.status_code}_retryable"
            elif 200 <= resp.status_code < 300:
                return resp.text, resp.status_code, attempts, "ok"
            else:
                last_reason = f"http_{resp.status_code}"

        except requests.exceptions.Timeout:
            last_reason = "timeout"
        except requests.exceptions.ConnectionError as e:
            msg = str(e).lower()
            if "nameresolutionerror" in msg or "failed to resolve" in msg:
                return None, None, attempts, "dns_resolution_error"
            last_reason = "connection_error"
        except Exception as e:
            last_reason = f"exception_{type(e).__name__}"

        if attempts < max_retries:
            sleep_s = (base_backoff ** attempts) + random.uniform(0, 0.5)
            debug_log(f"[DEBUG] fetch_page: retrying after {sleep_s:.2f}s ({last_reason})")
            time.sleep(sleep_s)

    return None, last_status, attempts, last_reason


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


def selenium_fetch_page(url, driver, wait_selector=None, timeout=12, shell_grace_period=20, shell_poll_interval=2.0):
    """Render the URL in Selenium and return page_source.

    If wait_selector is provided, wait up to `timeout` seconds for it.
    Returns page_source or None on failure.
    """
    try:
        debug_log(f"[DEBUG] selenium_fetch_page: GET {url}")
        driver.get(url)
        debug_log(f"[DEBUG] selenium_fetch_page: Page loaded")
    except Exception as e:
        msg = str(e).lower()
        debug_log(f"[DEBUG] selenium_fetch_page: driver.get() failed: {summarize_exception(e)}")
        if "invalid session id" in msg or "chrome not reachable" in msg:
            raise RuntimeError("selenium_session_dead")
        return None

    if wait_selector:
        try:
            debug_log(f"[DEBUG] selenium_fetch_page: Waiting for selector '{wait_selector}'")
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
            )
            debug_log(f"[DEBUG] selenium_fetch_page: Selector found")
        except TimeoutException:
            debug_log(f"[DEBUG] selenium_fetch_page: Selector timeout after {timeout}s")
            if shell_grace_period > 0 and page_looks_like_tcgplayer_shell(driver):
                if not DEBUG:
                    print(f"\nStill waiting for TCGplayer to render {url}", flush=True)
                elapsed = 0.0
                while elapsed < shell_grace_period:
                    time.sleep(shell_poll_interval)
                    elapsed += shell_poll_interval
                    try:
                        if driver.find_elements(By.CSS_SELECTOR, wait_selector):
                            debug_log(f"[DEBUG] selenium_fetch_page: Selector found during shell grace period after {elapsed:.1f}s")
                            break
                    except Exception:
                        pass
                    if not page_looks_like_tcgplayer_shell(driver):
                        debug_log(f"[DEBUG] selenium_fetch_page: Shell page resolved after {elapsed:.1f}s")
                        break
                else:
                    debug_log(f"[DEBUG] selenium_fetch_page: Shell grace period expired after {shell_grace_period}s")
            return driver.page_source

    # short sleep to let additional async load happen (kept small)
    time.sleep(1.0)
    page_source = driver.page_source
    debug_log(f"[DEBUG] selenium_fetch_page: Got page_source, length {len(page_source)}")
    return page_source


def parse_tcgplayer(html):
    """Return dict with extracted fields: listing_count, lowest_price, lowest_shipping, lowest_total_price, market_price, listed_median, current_quantity, current_sellers, set_name.
    This uses simple heuristics and may need tweaks for page changes.
    """
    soup = BeautifulSoup(html, "html.parser")
    listing_count = None
    lowest_price = None
    lowest_shipping = None
    lowest_total_price = None
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
                    # priceCurrency is not a numeric fallback; use only price fields
                    price = offer.get("price") or offer.get("lowPrice") or offer.get("highPrice")
                    shipping_details = offer.get("shippingDetails")
                    if isinstance(shipping_details, list) and shipping_details:
                        shipping_details = shipping_details[0]
                    if isinstance(shipping_details, dict):
                        shipping_rate = shipping_details.get("shippingRate")
                        if isinstance(shipping_rate, dict):
                            lowest_shipping = parse_money(
                                shipping_rate.get("value") or shipping_rate.get("price")
                            )
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
                market_price = parse_money(mp.get_text(strip=True))

        # Listed Median (span.text: 'Listed Median:', then .price-points__lower__price)
        for row in soup.select(".price-points__lower tr"):
            labels = [label.get_text(" ", strip=True) for label in row.select("span.text")]
            if not labels:
                continue
            row_text = " ".join(labels).lower()
            value_spans = row.select("span.price-points__lower__price")
            value_texts = [val.get_text(strip=True) for val in value_spans]

            if "listed median" in row_text and value_texts:
                parsed = parse_money(value_texts[0])
                if parsed is not None:
                    listed_median = parsed

            if "current quantity" in row_text and value_texts:
                parsed = parse_integer(value_texts[0])
                if parsed is not None:
                    current_quantity = parsed

            if "current sellers" in row_text and value_texts:
                parsed = parse_integer(value_texts[-1])
                if parsed is not None:
                    current_sellers = parsed
    except Exception:
        pass

    # 4) Price fallbacks — look for visible price elements (lowest price)
    if lowest_price is None:
        try:
            price_candidates = soup.select("span.price-point__data, span.price, div.price, span[itemprop=price]")
            prices = []
            for p in price_candidates:
                txt = p.get_text(strip=True).replace("$", "").replace(",", "")
                val = parse_money(txt)
                if val is not None:
                    prices.append(val)
            if prices:
                lowest_price = min(prices)
        except Exception:
            pass

    if lowest_shipping is None:
        try:
            shipping_el = soup.select_one(".spotlight__shipping")
            if shipping_el:
                lowest_shipping = parse_money(shipping_el.get_text(" ", strip=True))
        except Exception:
            pass

    if lowest_price is not None:
        shipping_value = lowest_shipping or 0.0
        lowest_total_price = round(lowest_price + shipping_value, 2)

    # 5) Final fallback: try to parse any visible text for counts and listings.
    try:
        body_text = soup.get_text(separator=" ", strip=True)
        if listing_count is None:
            m = re.search(r"(\d{1,6})\s+listings", body_text, re.I)
            if m:
                listing_count = int(m.group(1))
        if current_quantity is None:
            m = re.search(r"Current Quantity\s*:?\s*(\d{1,6})", body_text, re.I)
            if m:
                current_quantity = int(m.group(1))
        if current_sellers is None:
            m = re.search(r"Current Sellers\s*:?\s*(\d{1,6})", body_text, re.I)
            if m:
                current_sellers = int(m.group(1))
    except Exception:
        pass

    return {
        "listing_count": listing_count,
        "lowest_price": lowest_price,
        "lowest_shipping": lowest_shipping,
        "lowest_total_price": lowest_total_price,
        "market_price": market_price,
        "listed_median": listed_median,
        "current_quantity": current_quantity,
        "current_sellers": current_sellers,
        "set_name": set_name,
    }


def load_product_cache(conn):
    c = conn.cursor()
    c.execute("SELECT id, name, url FROM products")
    cache = {"by_url": {}, "by_name": {}}
    for product_id, name, url in c.fetchall():
        if url:
            cache["by_url"][url] = product_id
        if name:
            cache["by_name"][name] = product_id
    return cache


def resolve_snapshot_date(explicit_value=""):
    if explicit_value:
        return explicit_value
    return datetime.now().astimezone().date().isoformat()


def filter_rows_for_shard(rows, shard_index=0, shard_count=1):
    if shard_count <= 1:
        return rows
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index_out_of_range")

    filtered = []
    for row in rows:
        key = (row.get("url") or row.get("link") or row.get("name") or row.get("title") or "").strip()
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
        bucket = int(digest[:12], 16) % shard_count
        if bucket == shard_index:
            filtered.append(row)
    return filtered


def ensure_product(conn, cache, name, url):
    if url and url in cache["by_url"]:
        return cache["by_url"][url]
    if name and name in cache["by_name"]:
        return cache["by_name"][name]

    product_id = insert_row_returning_id(conn, "products", ["name", "url"], (name, url))
    if url:
        cache["by_url"][url] = product_id
    if name:
        cache["by_name"][name] = product_id
    return product_id


def insert_snapshot(conn, product_id, listing_count, lowest_price, lowest_shipping=None, lowest_total_price=None, market_price=None, listed_median=None, current_quantity=None, current_sellers=None, set_name=None, source="TCGplayer", run_id=None, snapshot_timestamp=None, snapshot_date=None):
    c = conn.cursor()
    effective_timestamp = snapshot_timestamp or datetime.utcnow().isoformat()
    effective_snapshot_date = snapshot_date or resolve_snapshot_date()
    ph = "%s" if get_dialect(conn) == "postgres" else "?"
    try:
        c.execute(
            """
            SELECT id
            FROM listings
            WHERE product_id = {ph} AND source = {ph} AND snapshot_date = {ph}
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """.format(ph=ph),
            (product_id, source, effective_snapshot_date),
        )
        existing = c.fetchone()
        if existing:
            c.execute(
                """
                UPDATE listings
                SET timestamp = {ph},
                    listing_count = {ph},
                    lowest_price = {ph},
                    lowest_shipping = {ph},
                    lowest_total_price = {ph},
                    median_price = {ph},
                    market_price = {ph},
                    current_quantity = {ph},
                    current_sellers = {ph},
                    set_name = {ph},
                    run_id = {ph}
                WHERE id = {ph}
                """.format(ph=ph),
                (
                    effective_timestamp,
                    listing_count,
                    lowest_price,
                    lowest_shipping,
                    lowest_total_price,
                    listed_median,
                    market_price,
                    current_quantity,
                    current_sellers,
                    set_name,
                    run_id,
                    existing[0],
                ),
            )
            return True

        c.execute(
            """
            INSERT INTO listings (
                product_id, timestamp, snapshot_date, listing_count, lowest_price, lowest_shipping, lowest_total_price, median_price, market_price, current_quantity, current_sellers, set_name, source, run_id
            ) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
            """.format(ph=ph),
            (
                product_id,
                effective_timestamp,
                effective_snapshot_date,
                listing_count,
                lowest_price,
                lowest_shipping,
                lowest_total_price,
                listed_median,
                market_price,
                current_quantity,
                current_sellers,
                set_name,
                source,
                run_id,
            ),
        )
    except Exception as e:
        debug_log(f"[DEBUG] DB insertion error: {e}")
        raise
    return True


def mark_stale_runs(conn, source):
    c = conn.cursor()
    ph = "%s" if get_dialect(conn) == "postgres" else "?"
    c.execute(
        """
        UPDATE scrape_runs
        SET ended_at = {ph}, status = 'abandoned'
        WHERE source = {ph} AND status = 'running'
        """.format(ph=ph),
        (datetime.utcnow().isoformat(), source),
    )
    conn.commit()
    return c.rowcount


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
    parser.add_argument("--selenium", action="store_true", help="Compatibility flag; Selenium is enabled by default when available")
    parser.add_argument("--no-selenium", action="store_true", help="Disable Selenium fallback and use requests only")
    parser.add_argument("--headless", action="store_true", help="When using --selenium, run Chrome headless")
    parser.add_argument("--source", default="TCGplayer", help="Snapshot source label (e.g., TCGplayer, eBay)")
    parser.add_argument("--request-timeout", type=float, default=12.0, help="HTTP timeout per request in seconds")
    parser.add_argument("--max-retries", type=int, default=3, help="Max HTTP retry attempts per URL")
    parser.add_argument("--retry-backoff", type=float, default=1.25, help="Exponential backoff base between retries")
    parser.add_argument("--max-selenium-restarts", type=int, default=2, help="How many times to restart Selenium when session dies")
    parser.add_argument("--commit-every", type=int, default=DEFAULT_COMMIT_EVERY, help="Commit SQLite writes every N attempts")
    parser.add_argument("--snapshot-date", default="", help="Store this run under a specific YYYY-MM-DD snapshot date")
    parser.add_argument("--debug", action="store_true", help="Show detailed fetch and Selenium debug logs")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index for parallel batch workers")
    parser.add_argument("--shard-count", type=int, default=1, help="Total shard count for parallel batch workers")
    args = parser.parse_args()

    global DEBUG
    DEBUG = args.debug

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Open DB
    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    stale_runs = mark_stale_runs(conn, args.source)
    if stale_runs:
        print(f"[INFO] Marked {stale_runs} stale '{args.source}' scrape run(s) as abandoned")
    run_id = start_scrape_run(
        conn,
        source=args.source,
        csv_path=args.csv,
        args_dict=vars(args),
    )
    product_cache = load_product_cache(conn)
    session = requests.Session()
    session.headers.update(headers)
    snapshot_date = resolve_snapshot_date(args.snapshot_date)

    driver = None
    selenium_enabled = SELENIUM_AVAILABLE and not args.no_selenium
    selenium_restart_count = 0

    count_processed = 0
    count_failed = 0
    count_attempted = 0
    count_parse_failed = 0
    count_written = 0

    run_status = "completed"
    try:
        if selenium_enabled:
            try:
                print("Starting Selenium Chrome (headless=%s) ..." % args.headless, flush=True)
                driver = make_driver(headless=args.headless)
            except Exception as e:
                print(f"Failed to start Selenium driver: {e}", flush=True)
                selenium_enabled = False

        with open(args.csv, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        rows = filter_rows_for_shard(rows, shard_index=args.shard_index, shard_count=args.shard_count)
        total_rows = len(rows)
        
        print(f"\n🚀 Starting scrape: {total_rows} products, Selenium={'ON' if selenium_enabled else 'OFF'}", flush=True)
        print(f"Snapshot date: {snapshot_date}", flush=True)
        print(f"Limit: {args.limit if args.limit else 'None (all)'}\n", flush=True)

        for i, row in enumerate(rows, start=1):
            if args.limit and count_attempted >= args.limit:
                break

            name = (row.get("name") or row.get("title") or "").strip()
            url = (row.get("url") or row.get("link") or "").strip()
            
            print_progress(i, total_rows, count_processed, count_failed, f"Processing: {name[:40]}")
            if not DEBUG:
                print(f"\n[{i}/{total_rows}] {name or '(unnamed product)'}", flush=True)
            if not url:
                count_attempted += 1
                count_failed += 1
                record_failure(conn, run_id, name, url, "input", "missing_url", attempts=0)
                if args.commit_every > 0 and count_attempted % args.commit_every == 0:
                    conn.commit()
                continue

            # Fetch HTML
            html, status_code, attempts_used, fetch_reason = fetch_page_with_retries(
                session,
                url,
                headers,
                timeout=args.request_timeout,
                max_retries=args.max_retries,
                base_backoff=args.retry_backoff,
            )
            # If requests failed, try Selenium if enabled
            if not html and selenium_enabled:
                try:
                    if not is_driver_alive(driver):
                        raise RuntimeError("selenium_session_dead")
                    html = selenium_fetch_page(url, driver, wait_selector="li.listing-item, span.price-point__data", timeout=8)
                    if html:
                        fetch_reason = "selenium_fallback_ok"
                except RuntimeError:
                    if selenium_restart_count < args.max_selenium_restarts:
                        selenium_restart_count += 1
                        debug_log(f"[DEBUG] Selenium session dead. Restarting driver ({selenium_restart_count}/{args.max_selenium_restarts})")
                        try:
                            if driver:
                                driver.quit()
                        except Exception:
                            pass
                        try:
                            driver = make_driver(headless=args.headless)
                            html = selenium_fetch_page(url, driver, wait_selector="li.listing-item, span.price-point__data", timeout=8)
                            if html:
                                fetch_reason = "selenium_fallback_ok_after_restart"
                        except Exception as restart_exc:
                            debug_log(f"[DEBUG] Selenium restart failed: {summarize_exception(restart_exc)}")
                    else:
                        selenium_enabled = False
                        debug_log("[DEBUG] Selenium disabled for remainder of run (max restarts reached)")
                except Exception as selenium_exc:
                    debug_log(f"[DEBUG] Selenium fallback error: {summarize_exception(selenium_exc)}")
            
            if not html:
                count_attempted += 1
                count_failed += 1
                save_diagnostic("", args.diagnostics_dir, f"fetchfail_{i}")
                record_failure(
                    conn,
                    run_id,
                    name,
                    url,
                    "fetch",
                    fetch_reason,
                    http_status=status_code,
                    attempts=attempts_used,
                )
                if args.commit_every > 0 and count_attempted % args.commit_every == 0:
                    conn.commit()
                time.sleep(random.uniform(args.delay_min, args.delay_max))
                continue

            # Parse
            parsed = parse_tcgplayer(html)
            
            # If parser got no data and Selenium is available, retry with Selenium (page was likely JS-rendered)
            if (parsed.get('listing_count') is None and 
                parsed.get('market_price') is None and 
                parsed.get('listed_median') is None and 
                selenium_enabled):
                try:
                    if not is_driver_alive(driver):
                        raise RuntimeError("selenium_session_dead")
                    html_selenium = selenium_fetch_page(url, driver, wait_selector=".price-points__upper__price, .price-points__lower", timeout=10)
                    if html_selenium:
                        parsed = parse_tcgplayer(html_selenium)
                except RuntimeError:
                    if selenium_restart_count < args.max_selenium_restarts:
                        selenium_restart_count += 1
                        debug_log(f"[DEBUG] Selenium session dead during parse retry. Restarting ({selenium_restart_count}/{args.max_selenium_restarts})")
                        try:
                            if driver:
                                driver.quit()
                        except Exception:
                            pass
                        try:
                            driver = make_driver(headless=args.headless)
                            html_selenium = selenium_fetch_page(url, driver, wait_selector=".price-points__upper__price, .price-points__lower", timeout=10)
                            if html_selenium:
                                parsed = parse_tcgplayer(html_selenium)
                        except Exception as restart_exc:
                            debug_log(f"[DEBUG] Selenium restart failed: {summarize_exception(restart_exc)}")
                    else:
                        selenium_enabled = False
                        debug_log("[DEBUG] Selenium disabled for remainder of run (max restarts reached)")
                except Exception as selenium_exc:
                    debug_log(f"[DEBUG] Selenium parse-retry error: {summarize_exception(selenium_exc)}")

            if not has_minimum_parse_data(parsed):
                count_attempted += 1
                count_failed += 1
                count_parse_failed += 1
                save_diagnostic(html, args.diagnostics_dir, f"parsefail_{i}")
                record_failure(
                    conn,
                    run_id,
                    name,
                    url,
                    "parse",
                    "missing_required_fields",
                    http_status=status_code,
                    attempts=attempts_used,
                )
                if args.commit_every > 0 and count_attempted % args.commit_every == 0:
                    conn.commit()
                time.sleep(random.uniform(args.delay_min, args.delay_max))
                continue
            
            # Insert into DB
            try:
                snapshot_timestamp = datetime.utcnow().isoformat()
                product_id = ensure_product(conn, product_cache, name, url)
                wrote_snapshot = insert_snapshot(
                    conn,
                    product_id,
                    parsed.get('listing_count'),
                    parsed.get('lowest_price'),
                    lowest_shipping=parsed.get('lowest_shipping'),
                    lowest_total_price=parsed.get('lowest_total_price'),
                    market_price=parsed.get('market_price'),
                    listed_median=parsed.get('listed_median'),
                    current_quantity=parsed.get('current_quantity'),
                    current_sellers=parsed.get('current_sellers'),
                    set_name=parsed.get('set_name'),
                    source=args.source,
                    run_id=run_id,
                    snapshot_timestamp=snapshot_timestamp,
                    snapshot_date=snapshot_date,
                )
                count_attempted += 1
                count_processed += 1
                if wrote_snapshot:
                    count_written += 1
                if not DEBUG:
                    print(
                        f"[{i}/{total_rows}] saved listing_count={parsed.get('listing_count')} lowest={parsed.get('lowest_price')} market={parsed.get('market_price')}",
                        flush=True,
                    )
            except Exception as e:
                count_attempted += 1
                save_diagnostic(html, args.diagnostics_dir, f"dberror_{i}")
                count_failed += 1
                record_failure(
                    conn,
                    run_id,
                    name,
                    url,
                    "db",
                    f"insert_error_{type(e).__name__}",
                    http_status=status_code,
                    attempts=attempts_used,
                )
                if not DEBUG:
                    print(f"[{i}/{total_rows}] failed to save {name}", flush=True)

            if args.commit_every > 0 and count_attempted % args.commit_every == 0:
                conn.commit()

            # politeness delay
            time.sleep(random.uniform(args.delay_min, args.delay_max))
        conn.commit()
    except KeyboardInterrupt:
        run_status = "interrupted"
        conn.commit()
        raise
    except Exception:
        run_status = "failed"
        conn.commit()
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        try:
            session.close()
        except Exception:
            pass
        finalize_scrape_run(
            conn,
            run_id=run_id,
            status=run_status,
            attempted=count_attempted,
            processed=count_processed,
            failed=count_failed,
            parse_failed=count_parse_failed,
        )
        conn.close()

    print(f"\n\n✅ Complete!")
    print(f"   Run ID: {run_id}")
    print(f"   Processed: {count_processed}")
    print(f"   Failed: {count_failed}")
    print(f"   Parse Failed: {count_parse_failed}")
    print(f"   Written: {count_written}")
    print(f"   Success Rate: {(count_processed/(count_processed+count_failed)*100):.1f}%" if count_processed+count_failed > 0 else "")
    print(f"   DB: {args.db}\n")

    if run_status == "interrupted":
        return 130
    if run_status == "failed":
        return 1
    if count_processed == 0 and count_failed > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
