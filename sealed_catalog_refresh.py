"""Refresh sealed catalog rows directly into the database."""

import argparse
from datetime import datetime

from db import configure_connection, connect_database, resolve_database_target
from link_scraper import DEFAULT_MAX_PAGES, scrape_pages
from populate_db import ensure_runtime_schema


def load_existing_products(conn):
    rows = conn.execute(
        """
        SELECT name, url
        FROM products
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


def upsert_sealed_product(conn, name, url, refresh_token, scrape_date):
    existing = conn.execute("SELECT id FROM products WHERE url = ?", (url,)).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE products
            SET name = ?, last_seen_at = ?, catalog_active = 1, catalog_scrape_date = ?
            WHERE id = ?
            """,
            (name, refresh_token, scrape_date, int(existing[0])),
        )
        return

    conn.execute(
        """
        INSERT INTO products (
            name, url, first_seen_at, last_seen_at, catalog_active, catalog_scrape_date
        ) VALUES (?, ?, ?, ?, 1, ?)
        """,
        (name, url, refresh_token, refresh_token, scrape_date),
    )


def apply_catalog_refresh(conn, live_products, refresh_token, scrape_date):
    for name, url in live_products:
        upsert_sealed_product(conn, name, url, refresh_token, scrape_date)
    conn.commit()


def finalize_catalog_refresh(conn, refresh_token, mode="fresh"):
    existing_products = load_existing_products(conn)
    existing_urls = {url for _, url in existing_products}

    current_products = [
        (row[0], row[1])
        for row in conn.execute(
            """
            SELECT name, url
            FROM products
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
            UPDATE products
            SET catalog_active = 0
            WHERE url IS NOT NULL
              AND url != ''
              AND url LIKE '%tcgplayer.com/product/%'
              AND COALESCE(last_seen_at, '') != ?
            """
            ,
            (refresh_token,),
        )
    conn.commit()

    if mode == "reconcile":
        added = sum(1 for _, url in current_products if url not in existing_urls)
        removed = sum(1 for _, url in existing_products if url not in current_urls)
        print(
            {
                "mode": mode,
                "live_products": len(current_products),
                "tracked_products": len(current_products),
                "added": added,
                "removed": removed,
            },
            flush=True,
        )
        return

    print(
        {
            "mode": mode,
            "live_products": len(current_products),
            "tracked_products": len(current_products),
        },
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description="Refresh sealed catalog directly into products")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--mode", choices=["fresh", "newest", "reconcile"], default="fresh")
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--scrape-date", default="")
    parser.add_argument("--wait-time", type=int, default=35)
    parser.add_argument("--page-load-timeout", type=int, default=40)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--refresh-token", default="")
    parser.add_argument("--finalize-only", action="store_true")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--category-slug", default="pokemon")
    parser.add_argument("--product-line-name", default="pokemon")
    parser.add_argument("--product-type-name", default="Sealed Products")
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
        finalize_catalog_refresh(conn, refresh_token, mode=args.mode)
        conn.close()
        return 0

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
    apply_catalog_refresh(conn, merge_products(live_products), refresh_token, scrape_date)
    if args.shard_count <= 1:
        finalize_catalog_refresh(conn, refresh_token, mode=args.mode)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
