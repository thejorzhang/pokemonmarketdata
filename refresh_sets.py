"""Refresh the canonical sets table from current sealed and card metadata."""

import argparse
from datetime import datetime

from db import configure_connection, connect_database, resolve_database_target
from populate_db import ensure_runtime_schema


def normalize_set_name(value):
    return " ".join((value or "").strip().split())


def upsert_set(conn, name, category_slug, product_line, source, set_type, release_date):
    now = datetime.utcnow().isoformat()
    existing = conn.execute(
        """
        SELECT id
        FROM sets
        WHERE name = ? AND COALESCE(product_line, '') = COALESCE(?, '')
        """,
        (name, product_line),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE sets
            SET category_slug = ?,
                source = ?,
                set_type = COALESCE(?, set_type),
                release_date = COALESCE(?, release_date),
                last_seen_at = ?
            WHERE id = ?
            """,
            (category_slug, source, set_type, release_date, now, int(existing[0])),
        )
        return int(existing[0])

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sets (
            name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (name, category_slug, product_line, source, set_type, release_date, now, now),
    )
    return int(cur.lastrowid)


def refresh_sets(conn):
    sets_seen = 0
    sealed_products_linked = 0
    card_products_linked = 0

    sealed_rows = conn.execute(
        """
        SELECT DISTINCT set_name, COALESCE(product_line, 'pokemon') AS product_line, release_date
        FROM product_details
        WHERE set_name IS NOT NULL AND set_name != ''
        ORDER BY set_name
        """
    ).fetchall()
    for set_name, product_line, release_date in sealed_rows:
        normalized = normalize_set_name(set_name)
        if not normalized:
            continue
        upsert_set(conn, normalized, "pokemon", product_line, "TCGplayer Product Details", "sealed", release_date)
        sets_seen += 1

    sealed_products = conn.execute(
        """
        SELECT product_id, set_name, COALESCE(product_line, 'pokemon') AS product_line
        FROM product_details
        WHERE set_name IS NOT NULL
          AND set_name != ''
        """
    ).fetchall()
    for product_id, set_name, product_line in sealed_products:
        normalized = normalize_set_name(set_name)
        if not normalized:
            continue
        set_row = conn.execute(
            "SELECT id FROM sets WHERE name = ? AND COALESCE(product_line, '') = COALESCE(?, '')",
            (normalized, product_line),
        ).fetchone()
        if not set_row:
            continue
        conn.execute("UPDATE product_details SET set_id = ? WHERE product_id = ?", (int(set_row[0]), int(product_id)))
        sealed_products_linked += 1

    card_rows = conn.execute(
        """
        SELECT DISTINCT
            COALESCE(d.set_name, p.set_name) AS set_name,
            COALESCE(p.product_line, 'pokemon') AS product_line,
            d.release_date
        FROM card_products p
        LEFT JOIN card_details d ON d.card_product_id = p.id
        WHERE COALESCE(d.set_name, p.set_name) IS NOT NULL
          AND COALESCE(d.set_name, p.set_name) != ''
        ORDER BY COALESCE(d.set_name, p.set_name)
        """
    ).fetchall()
    for set_name, product_line, release_date in card_rows:
        normalized = normalize_set_name(set_name)
        if not normalized:
            continue
        upsert_set(conn, normalized, "pokemon", product_line, "TCGplayer Cards", "cards", release_date)
        sets_seen += 1

    card_products = conn.execute(
        """
        SELECT p.id, COALESCE(d.set_name, p.set_name) AS set_name, COALESCE(p.product_line, 'pokemon') AS product_line
        FROM card_products p
        LEFT JOIN card_details d ON d.card_product_id = p.id
        WHERE COALESCE(d.set_name, p.set_name) IS NOT NULL
          AND COALESCE(d.set_name, p.set_name) != ''
        """
    ).fetchall()
    for card_product_id, set_name, product_line in card_products:
        normalized = normalize_set_name(set_name)
        if not normalized:
            continue
        set_row = conn.execute(
            "SELECT id FROM sets WHERE name = ? AND COALESCE(product_line, '') = COALESCE(?, '')",
            (normalized, product_line),
        ).fetchone()
        if not set_row:
            continue
        conn.execute("UPDATE card_products SET set_id = ? WHERE id = ?", (int(set_row[0]), int(card_product_id)))
        card_products_linked += 1

    conn.commit()
    return {
        "sets_seen": sets_seen,
        "sealed_products_linked": sealed_products_linked,
        "card_products_linked": card_products_linked,
    }


def main():
    parser = argparse.ArgumentParser(description="Refresh sets table from current metadata")
    parser.add_argument("--db", default="sealed_market.db")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    result = refresh_sets(conn)
    conn.close()
    print(result, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
