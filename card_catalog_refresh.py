"""Load or refresh card catalog rows into ``card_products``.

This keeps the cards track separate from the sealed flow so we can expand card
processing without destabilizing the sealed pipeline.
"""

import argparse
import csv
import re
from datetime import datetime

from db import configure_connection, connect_database, get_dialect, resolve_database_target
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
):
    cur = conn.cursor()
    dialect = get_dialect(conn)
    tcgplayer_product_id = extract_tcgplayer_product_id(url)
    discovered_at = datetime.utcnow().isoformat()
    if dialect == "postgres":
        cur.execute(
            """
            INSERT INTO card_products (
                tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                tcgplayer_product_id = EXCLUDED.tcgplayer_product_id,
                name = EXCLUDED.name,
                category_slug = EXCLUDED.category_slug,
                product_line = EXCLUDED.product_line,
                set_name = EXCLUDED.set_name,
                source = EXCLUDED.source
            """,
            (tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at),
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
                source = ?
            WHERE id = ?
            """,
            (tcgplayer_product_id, name, category_slug, product_line, set_name, source, existing[0]),
        )
        return

    cur.execute(
        """
        INSERT INTO card_products (
            tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at),
    )


def main():
    parser = argparse.ArgumentParser(description="Refresh card catalog rows into card_products")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--csv", required=True, help="CSV with name,url columns for card catalog")
    parser.add_argument("--category-slug", default="pokemon")
    parser.add_argument("--product-line-name", default="pokemon")
    parser.add_argument("--source", default="TCGplayer Cards")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)

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
