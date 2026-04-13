"""Load or refresh card catalog rows into ``card_products``.

This keeps the cards track separate from the sealed flow so we can expand card
processing without destabilizing the sealed pipeline.
"""

import argparse
import csv
import re
from datetime import datetime

from db import configure_connection, connect_database, get_dialect, resolve_database_target
from link_scraper import scrape_pages, DEFAULT_MAX_PAGES
from populate_db import ensure_runtime_schema


def extract_tcgplayer_product_id(url):
    match = re.search(r"/product/(\d+)", url or "")
    if not match:
        return None
    return int(match.group(1))


def upsert_card_product(
    conn,
    name,
    url,
    category_slug="pokemon",
    product_line="pokemon",
    set_name=None,
    source="TCGplayer Cards",
    refresh_token=None,
    scrape_date=None,
):
    cur = conn.cursor()
    dialect = get_dialect(conn)
    tcgplayer_product_id = extract_tcgplayer_product_id(url)
    discovered_at = refresh_token or datetime.utcnow().isoformat()
    if dialect == "postgres":
        cur.execute(
            """
            INSERT INTO card_products (
                tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at, first_seen_at, last_seen_at, catalog_active
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
            ON CONFLICT (url) DO UPDATE SET
                tcgplayer_product_id = EXCLUDED.tcgplayer_product_id,
                name = EXCLUDED.name,
                category_slug = EXCLUDED.category_slug,
                product_line = EXCLUDED.product_line,
                set_name = EXCLUDED.set_name,
                source = EXCLUDED.source,
                last_seen_at = EXCLUDED.last_seen_at,
                catalog_active = 1
            """,
            (tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at, discovered_at, discovered_at),
        )
        return

    existing = cur.execute("SELECT id FROM card_products WHERE url = ?", (url,)).fetchone()
    if existing:
        cur.execute(
            """
            UPDATE card_products
            SET tcgplayer_product_id = ?,
                name = ?,
                category_slug = ?,
                product_line = ?,
                set_name = ?,
                source = ?,
                last_seen_at = ?,
                catalog_active = 1,
                catalog_scrape_date = ?
            WHERE id = ?
            """,
            (tcgplayer_product_id, name, category_slug, product_line, set_name, source, discovered_at, scrape_date, existing[0]),
        )
        return

    cur.execute(
        """
        INSERT INTO card_products (
            tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at, first_seen_at, last_seen_at, catalog_active, catalog_scrape_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at, discovered_at, discovered_at, scrape_date),
    )


def load_existing_card_products(conn):
    rows = conn.execute(
        """
        SELECT name, url
        FROM card_products
        WHERE url IS NOT NULL
          AND url != ''
          AND url LIKE '%tcgplayer.com/product/%'
        ORDER BY id
        """
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def merge_products(*product_lists):
    merged = []
    positions = {}
    for product_list in product_lists:
        for name, url in product_list:
            if not url:
                continue
            name = (name or "").strip() or "(unknown)"
            if url in positions:
                idx = positions[url]
                existing_name, existing_url = merged[idx]
                if existing_name == "(unknown)" and name != "(unknown)":
                    merged[idx] = (name, existing_url)
                continue
            positions[url] = len(merged)
            merged.append((name, url))
    return merged


def refresh_card_catalog(conn, live_products, category_slug="pokemon", product_line_name="pokemon", source="TCGplayer Cards", mode="fresh"):
    existing_products = load_existing_card_products(conn)
    if mode == "newest":
        final_products = merge_products(existing_products, live_products)
    else:
        final_products = merge_products(live_products)

    if mode in {"fresh", "reconcile"}:
        conn.execute(
            """
            UPDATE card_products
            SET catalog_active = 0
            WHERE url IS NOT NULL
              AND url != ''
              AND url LIKE '%tcgplayer.com/product/%'
            """
        )

    for name, url in final_products:
        upsert_card_product(
            conn,
            name=name,
            url=url,
            category_slug=category_slug,
            product_line=product_line_name,
            source=source,
        )
    conn.commit()

    if mode == "reconcile":
        existing_urls = {url for _, url in existing_products}
        live_urls = {url for _, url in live_products}
        print(
            {
                "mode": mode,
                "live_products": len(live_products),
                "tracked_products": len(final_products),
                "added": sum(1 for _, url in live_products if url not in existing_urls),
                "removed": sum(1 for _, url in existing_products if url not in live_urls),
            },
            flush=True,
        )
        return

    print(
        {
            "mode": mode,
            "live_products": len(live_products),
            "tracked_products": len(final_products),
        },
        flush=True,
    )


def apply_card_catalog_refresh(
    conn,
    live_products,
    refresh_token,
    scrape_date,
    category_slug="pokemon",
    product_line_name="pokemon",
    source="TCGplayer Cards",
):
    for name, url in merge_products(live_products):
        upsert_card_product(
            conn,
            name=name,
            url=url,
            category_slug=category_slug,
            product_line=product_line_name,
            source=source,
            refresh_token=refresh_token,
            scrape_date=scrape_date,
        )
    conn.commit()


def finalize_card_catalog_refresh(conn, refresh_token, mode="fresh"):
    existing_products = load_existing_card_products(conn)
    existing_urls = {url for _, url in existing_products}
    current_products = [
        (row[0], row[1])
        for row in conn.execute(
            """
            SELECT name, url
            FROM card_products
            WHERE url IS NOT NULL
              AND url != ''
              AND url LIKE '%tcgplayer.com/product/%'
              AND last_seen_at = ?
            ORDER BY id
            """,
            (refresh_token,),
        ).fetchall()
    ]
    current_urls = {url for _, url in current_products}
    if mode in {"fresh", "reconcile"}:
        conn.execute(
            """
            UPDATE card_products
            SET catalog_active = 0
            WHERE url IS NOT NULL
              AND url != ''
              AND url LIKE '%tcgplayer.com/product/%'
              AND COALESCE(last_seen_at, '') != ?
            """,
            (refresh_token,),
        )
        conn.commit()

    payload = {
        "mode": mode,
        "live_products": len(current_products),
        "tracked_products": len(current_products),
    }
    if mode == "reconcile":
        payload["added"] = sum(1 for _, url in current_products if url not in existing_urls)
        payload["removed"] = sum(1 for _, url in existing_products if url not in current_urls)
    print(payload, flush=True)


def main():
    parser = argparse.ArgumentParser(description="Refresh card catalog rows into card_products")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--csv", default="", help="Optional CSV with name,url columns for card catalog")
    parser.add_argument("--category-slug", default="pokemon")
    parser.add_argument("--product-line-name", default="pokemon")
    parser.add_argument("--source", default="TCGplayer Cards")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--scrape", action="store_true", help="Scrape the live catalog directly into card_products instead of loading from a CSV")
    parser.add_argument("--mode", choices=["fresh", "newest", "reconcile"], default="fresh")
    parser.add_argument("--pages", type=int, default=5)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--scrape-date", default="")
    parser.add_argument("--wait-time", type=int, default=35)
    parser.add_argument("--page-load-timeout", type=int, default=40)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--product-type-name", default="Cards")
    parser.add_argument("--refresh-token", default="")
    parser.add_argument("--finalize-only", action="store_true")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    args = parser.parse_args()
    if args.mode in {"fresh", "reconcile"}:
        if args.wait_time == 35:
            args.wait_time = 60
        if args.page_load_timeout == 40:
            args.page_load_timeout = 70

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    refresh_token = args.refresh_token.strip() or datetime.utcnow().isoformat()
    scrape_date = args.scrape_date.strip() or datetime.utcnow().date().isoformat()

    if args.finalize_only:
        finalize_card_catalog_refresh(conn, refresh_token, mode=args.mode)
        conn.close()
        return 0

    if args.scrape:
        pages = DEFAULT_MAX_PAGES if args.all else args.pages
        live_products = scrape_pages(
            pages,
            None,
            headless=args.headless,
            stop_on_empty=False,
            mode="fresh",
            wait_time=args.wait_time,
            page_load_timeout=args.page_load_timeout,
            retries=args.retries,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
            category_slug=args.category_slug,
            product_line_name=args.product_line_name,
            product_type_name=args.product_type_name,
        )
        apply_card_catalog_refresh(
            conn,
            live_products,
            refresh_token=refresh_token,
            scrape_date=scrape_date,
            category_slug=args.category_slug,
            product_line_name=args.product_line_name,
            source=args.source,
        )
        if args.shard_count <= 1:
            finalize_card_catalog_refresh(conn, refresh_token, mode=args.mode)
        conn.close()
        return 0

    if not args.csv:
        raise SystemExit("--csv is required unless --scrape is used")

    inserted = 0
    with open(args.csv, newline="", encoding="utf-8") as fh:
        for index, row in enumerate(csv.DictReader(fh), start=1):
            if args.limit and inserted >= args.limit:
                break
            name = (row.get("name") or row.get("title") or "").strip()
            url = (row.get("url") or row.get("link") or "").strip()
            set_name = (row.get("set_name") or row.get("set") or "").strip() or None
            if not name or not url:
                continue
            upsert_card_product(
                conn,
                name=name,
                url=url,
                category_slug=args.category_slug,
                product_line=args.product_line_name,
                set_name=set_name,
                source=args.source,
                scrape_date=scrape_date,
            )
            inserted += 1
            if inserted % 100 == 0:
                conn.commit()
                print(f"Loaded {inserted} card product(s)...", flush=True)

    conn.commit()
    conn.close()
    print(f"Loaded {inserted} card product(s) into card_products", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
