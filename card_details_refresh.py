"""Refresh missing card_details rows from card_products product pages."""

import argparse
import re
import time
import random
from datetime import datetime

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
DEFAULT_SOURCE = "TCGplayer Card Details"


def extract_tcgplayer_product_id(url):
    match = re.search(r"/product/(\d+)", url or "")
    if not match:
        return None
    return int(match.group(1))


def normalize_spaces(text):
    return re.sub(r"\s+", " ", (text or "").strip())


def extract_labeled_value(soup, label_text):
    label_lower = label_text.lower()
    for text in soup.stripped_strings:
        normalized = normalize_spaces(text)
        lower = normalized.lower()
        if lower.startswith(f"{label_lower}:"):
            return normalize_spaces(normalized.split(":", 1)[1]) or None
        if lower.startswith(f"{label_lower} -"):
            return normalize_spaces(normalized.split("-", 1)[1]) or None
    return None


def infer_finish(text):
    haystack = (text or "").lower()
    if "reverse holo" in haystack or "reverse-holo" in haystack:
        return "reverse_holofoil"
    if "holofoil" in haystack or "holo foil" in haystack or re.search(r"\bholo\b", haystack):
        return "holofoil"
    if "1st edition" in haystack:
        return "first_edition"
    return None


def infer_language(text):
    haystack = (text or "").lower()
    if "japanese" in haystack:
        return "Japanese"
    if "korean" in haystack:
        return "Korean"
    if "german" in haystack:
        return "German"
    if "french" in haystack:
        return "French"
    if "spanish" in haystack:
        return "Spanish"
    return "English"


def infer_supertype(title):
    haystack = (title or "").lower()
    if " energy " in f" {haystack} ":
        return "Energy"
    if " trainer " in f" {haystack} ":
        return "Trainer"
    return "Pokemon"


def parse_card_details(html, fallback_name="", source_url="", fallback_set_name=None):
    soup = BeautifulSoup(html or "", "html.parser")
    h1 = soup.find("h1")
    raw_title = normalize_spaces(h1.get_text(" ", strip=True)) if h1 else fallback_name
    body_text = normalize_spaces(soup.get_text(" ", strip=True))
    combined_text = normalize_spaces(f"{raw_title} {body_text}")

    set_name = None
    set_el = soup.select_one('span[data-testid="lblProductDetailsSetName"]')
    if set_el:
        set_name = normalize_spaces(set_el.get_text(" ", strip=True))
    if not set_name:
        set_name = fallback_set_name

    card_number = extract_labeled_value(soup, "Number")
    rarity = extract_labeled_value(soup, "Rarity")
    release_date = extract_labeled_value(soup, "Release Date")
    finish = infer_finish(combined_text)
    language = infer_language(combined_text)
    supertype = infer_supertype(raw_title)

    return {
        "tcgplayer_product_id": extract_tcgplayer_product_id(source_url),
        "source_url": source_url,
        "raw_title": raw_title,
        "set_name": set_name,
        "card_number": card_number,
        "rarity": rarity,
        "finish": finish,
        "language": language,
        "supertype": supertype,
        "subtype": None,
        "release_date": release_date,
    }


def load_missing_card_products(conn, limit=0):
    query = """
        SELECT p.id, p.name, p.url, p.set_name
        FROM card_products p
        LEFT JOIN card_details d ON d.card_product_id = p.id
        WHERE d.card_product_id IS NULL
          AND p.url IS NOT NULL
          AND p.url != ''
        ORDER BY p.id
    """
    if limit and limit > 0:
        query += f" LIMIT {int(limit)}"
    return conn.execute(query).fetchall()


def filter_card_products_for_shard(rows, shard_index=0, shard_count=1):
    if shard_count <= 1:
        return rows
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index_out_of_range")
    return [row for row in rows if int(row[0]) % shard_count == shard_index]


def upsert_card_details(conn, card_product_id, details, source=DEFAULT_SOURCE):
    placeholders = sql_placeholder_list(conn, 14)
    conn.execute(
        """
        INSERT INTO card_details (
            card_product_id,
            tcgplayer_product_id,
            source_url,
            raw_title,
            set_name,
            card_number,
            rarity,
            finish,
            language,
            supertype,
            subtype,
            release_date,
            source,
            scraped_at
        ) VALUES ({placeholders})
        ON CONFLICT(card_product_id) DO UPDATE SET
            tcgplayer_product_id = excluded.tcgplayer_product_id,
            source_url = excluded.source_url,
            raw_title = excluded.raw_title,
            set_name = excluded.set_name,
            card_number = excluded.card_number,
            rarity = excluded.rarity,
            finish = excluded.finish,
            language = excluded.language,
            supertype = excluded.supertype,
            subtype = excluded.subtype,
            release_date = excluded.release_date,
            source = excluded.source,
            scraped_at = excluded.scraped_at
        """.format(placeholders=placeholders),
        (
            card_product_id,
            details.get("tcgplayer_product_id"),
            details.get("source_url"),
            details.get("raw_title"),
            details.get("set_name"),
            details.get("card_number"),
            details.get("rarity"),
            details.get("finish"),
            details.get("language"),
            details.get("supertype"),
            details.get("subtype"),
            details.get("release_date"),
            source,
            datetime.utcnow().isoformat(),
        ),
    )


def main():
    parser = argparse.ArgumentParser(description="Fill missing card_details rows from card product pages")
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
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_db_connection(conn)
    ensure_runtime_schema(conn)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    driver = None
    selenium_enabled = not args.no_selenium
    if selenium_enabled:
        try:
            print(f"Starting Selenium Chrome (headless={args.headless}) ...", flush=True)
            driver = make_driver(headless=args.headless)
        except Exception as exc:
            print(f"Warning: Selenium unavailable, falling back to requests only ({exc})", flush=True)
            selenium_enabled = False

    rows = load_missing_card_products(conn, limit=args.limit)
    rows = filter_card_products_for_shard(rows, shard_index=args.shard_index, shard_count=args.shard_count)
    print(
        f"Refreshing card details for {len(rows)} product(s), shard={args.shard_index + 1}/{args.shard_count}",
        flush=True,
    )

    processed = 0
    failed = 0
    for card_product_id, name, url, set_name in rows:
        try:
            html, meta = fetch_page_with_retries(
                url,
                session=session,
                timeout=args.request_timeout,
                max_retries=args.max_retries,
                retry_backoff=args.retry_backoff,
            )
            if (not html or len(html) < 5000) and selenium_enabled:
                if not is_driver_alive(driver):
                    driver = make_driver(headless=args.headless)
                html = selenium_fetch_page(url, driver)
                meta = {"status_code": 200, "attempts": meta.get("attempts", 1), "reason": "selenium_fallback"}

            details = parse_card_details(
                html,
                fallback_name=name,
                source_url=url,
                fallback_set_name=set_name,
            )
            upsert_card_details(conn, card_product_id, details, source=args.source)
            processed += 1
            if processed % 25 == 0:
                conn.commit()
            print(
                f"[{processed + failed}/{len(rows)}] card-details product={details.get('tcgplayer_product_id')} "
                f"rarity={details.get('rarity') or '-'} finish={details.get('finish') or '-'}",
                flush=True,
            )
        except Exception as exc:
            failed += 1
            print(f"[{processed + failed}/{len(rows)}] card-details failed url={url} error={exc}", flush=True)

        if args.delay_max > 0:
            time.sleep(random.uniform(max(args.delay_min, 0.0), max(args.delay_max, args.delay_min, 0.0)))

    conn.commit()
    conn.close()
    if driver:
        try:
            driver.quit()
        except Exception:
            pass

    print(
        {
            "products_considered": len(rows),
            "products_processed": processed,
            "products_failed": failed,
        },
        flush=True,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
