"""Refresh product-level metadata for rows missing in product_details.

This script is intentionally separate from link discovery and price scraping.
It reads from the existing products table and enriches only rows that do not
already exist in product_details.
"""

import argparse
import re
import time
from datetime import datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from db import (
    configure_connection as configure_db_connection,
    connect_database,
    resolve_database_target,
    sql_placeholder_list,
)
from populate_db import (
    ensure_runtime_schema,
    fetch_page_with_retries,
    is_driver_alive,
    make_driver,
    selenium_fetch_page,
)


DEFAULT_DB = "sealed_market.db"
DEFAULT_SOURCE = "TCGplayer Product Details"


def extract_tcgplayer_product_id(url):
    match = re.search(r"/product/(\d+)", url or "")
    if not match:
        return None
    return int(match.group(1))


def extract_url_slug(url):
    path = urlparse(url or "").path.strip("/")
    parts = path.split("/")
    if len(parts) >= 3 and parts[0] == "product":
        return parts[2]
    return ""


def normalize_spaces(text):
    return re.sub(r"\s+", " ", (text or "").strip())


def classify_product_type(name, url_slug=""):
    haystack = f"{name or ''} {url_slug or ''}".lower()
    patterns = [
        ("elite_trainer_box", r"\belite trainer box\b|\betb\b"),
        ("booster_box", r"\bbooster box\b"),
        ("booster_bundle", r"\bbooster bundle\b"),
        ("sleeved_booster_pack", r"\bsleeved booster pack\b"),
        ("booster_pack", r"\bbooster pack\b"),
        ("three_pack_blister", r"\b3 pack blister\b|\bthree pack blister\b"),
        ("blister", r"\bblister\b"),
        ("premium_collection", r"\bpremium collection\b"),
        ("ultra_premium_collection", r"\bultra premium collection\b"),
        ("figure_collection", r"\bfigure collection\b"),
        ("pin_collection", r"\bpin collection\b"),
        ("poster_collection", r"\bposter collection\b"),
        ("collection_box", r"\bcollection box\b"),
        ("collection", r"\bcollection\b"),
        ("mini_tin", r"\bmini tin\b"),
        ("tin", r"\btin\b"),
        ("build_and_battle", r"\bbuild and battle\b"),
        ("deck", r"\bdeck\b"),
        ("promo_pack", r"\bpromo pack\b"),
        ("prize_pack", r"\bprize pack\b"),
        ("box_set", r"\bbox set\b"),
        ("bundle", r"\bbundle\b"),
        ("pack", r"\bpack\b"),
    ]
    for product_type, pattern in patterns:
        if re.search(pattern, haystack):
            return product_type
    return "other"


def classify_product_subtype(name, url_slug=""):
    haystack = f"{name or ''} {url_slug or ''}".lower()
    subtype_patterns = [
        "mega_charizard",
        "booster_pack",
        "booster_bundle",
        "elite_trainer_box",
        "premium_collection",
        "poster_collection",
        "pin_collection",
        "mini_tin",
        "prize_pack",
    ]
    for subtype in subtype_patterns:
        if subtype.replace("_", " ") in haystack:
            return subtype
    return None


def parse_release_date(text):
    text = normalize_spaces(text)
    if not text:
        return None
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", text)
    if match:
        return match.group(1)
    match = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+20\d{2}\b",
        text,
    )
    return match.group(0) if match else None


def parse_product_details(html, fallback_name="", source_url=""):
    soup = BeautifulSoup(html or "", "html.parser")
    set_name = None
    raw_title = None
    release_date = None

    h1 = soup.find("h1")
    if h1:
        raw_title = normalize_spaces(h1.get_text(" ", strip=True))

    set_el = soup.select_one('span[data-testid="lblProductDetailsSetName"]')
    if set_el:
        set_name = normalize_spaces(set_el.get_text(" ", strip=True))

    body_text = soup.get_text(" ", strip=True)
    release_date = parse_release_date(body_text)

    name = raw_title or fallback_name
    url_slug = extract_url_slug(source_url)
    return {
        "tcgplayer_product_id": extract_tcgplayer_product_id(source_url),
        "source_url": source_url,
        "url_slug": url_slug,
        "raw_title": raw_title or fallback_name,
        "set_name": set_name,
        "product_line": "Pokemon",
        "product_type": classify_product_type(name, url_slug),
        "product_subtype": classify_product_subtype(name, url_slug),
        "release_date": release_date,
    }


def load_missing_products(conn, limit=0):
    query = """
        SELECT p.id, p.name, p.url
        FROM products p
        LEFT JOIN product_details d ON d.product_id = p.id
        WHERE d.product_id IS NULL
          AND p.url IS NOT NULL
          AND p.url != ''
        ORDER BY p.id
    """
    if limit and limit > 0:
        query += f" LIMIT {int(limit)}"
    return conn.execute(query).fetchall()


def filter_products_for_shard(rows, shard_index=0, shard_count=1):
    if shard_count <= 1:
        return rows
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index_out_of_range")
    filtered = []
    for row in rows:
        product_id = int(row[0])
        if product_id % shard_count == shard_index:
            filtered.append(row)
    return filtered


def upsert_product_details(conn, product_id, details, source=DEFAULT_SOURCE):
    placeholders = sql_placeholder_list(conn, 12)
    conn.execute(
        """
        INSERT INTO product_details (
            product_id,
            tcgplayer_product_id,
            source_url,
            url_slug,
            raw_title,
            set_name,
            product_line,
            product_type,
            product_subtype,
            release_date,
            source,
            scraped_at
        ) VALUES ({placeholders})
        ON CONFLICT(product_id) DO UPDATE SET
            tcgplayer_product_id = excluded.tcgplayer_product_id,
            source_url = excluded.source_url,
            url_slug = excluded.url_slug,
            raw_title = excluded.raw_title,
            set_name = excluded.set_name,
            product_line = excluded.product_line,
            product_type = excluded.product_type,
            product_subtype = excluded.product_subtype,
            release_date = excluded.release_date,
            source = excluded.source,
            scraped_at = excluded.scraped_at
        """.format(placeholders=placeholders),
        (
            product_id,
            details.get("tcgplayer_product_id"),
            details.get("source_url"),
            details.get("url_slug"),
            details.get("raw_title"),
            details.get("set_name"),
            details.get("product_line"),
            details.get("product_type"),
            details.get("product_subtype"),
            details.get("release_date"),
            source,
            datetime.utcnow().isoformat(),
        ),
    )


def main():
    parser = argparse.ArgumentParser(description="Fill missing product_details rows from product pages")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--request-timeout", type=float, default=12.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-backoff", type=float, default=1.25)
    parser.add_argument("--no-selenium", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--delay-min", type=float, default=0.5)
    parser.add_argument("--delay-max", type=float, default=1.5)
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index for parallel batch workers")
    parser.add_argument("--shard-count", type=int, default=1, help="Total shard count for parallel batch workers")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_db_connection(conn)
    ensure_runtime_schema(conn)

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }
    session.headers.update(headers)

    driver = None
    selenium_enabled = not args.no_selenium
    if selenium_enabled:
        try:
            print(f"Starting Selenium Chrome (headless={args.headless}) ...", flush=True)
            driver = make_driver(headless=args.headless)
        except Exception as exc:
            print(f"Failed to start Selenium for product details: {exc}", flush=True)
            selenium_enabled = False

    rows = load_missing_products(conn, limit=args.limit)
    rows = filter_products_for_shard(rows, shard_index=args.shard_index, shard_count=args.shard_count)
    print(f"Refreshing product details for {len(rows)} product(s)", flush=True)

    processed = 0
    for product_id, name, url in rows:
        html, _, _, _ = fetch_page_with_retries(
            session,
            url,
            headers,
            timeout=args.request_timeout,
            max_retries=args.max_retries,
            base_backoff=args.retry_backoff,
        )
        if (not html or 'lblProductDetailsSetName' not in html) and selenium_enabled:
            try:
                if not is_driver_alive(driver):
                    driver = make_driver(headless=args.headless)
                html = selenium_fetch_page(
                    url,
                    driver,
                    wait_selector='span[data-testid="lblProductDetailsSetName"], .price-points__upper__price',
                    timeout=10,
                    shell_grace_period=20,
                )
            except Exception:
                pass

        details = parse_product_details(html or "", fallback_name=name, source_url=url)
        upsert_product_details(conn, product_id, details, source=args.source)
        processed += 1
        print(
            f"[{processed}/{len(rows)}] {name} -> type={details.get('product_type')} set={details.get('set_name') or '-'} release={details.get('release_date') or '-'}",
            flush=True,
        )
        conn.commit()
        time.sleep(max(0.0, args.delay_min))

    conn.close()
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
