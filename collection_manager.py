"""Collection portfolio helpers built on top of the existing market database."""

from __future__ import annotations

import argparse
import json
from datetime import datetime

from db import (
    configure_connection,
    connect_database,
    get_dialect,
    id_column_sql,
    insert_row_returning_id,
    resolve_database_target,
    table_exists,
    table_columns,
)


DEFAULT_COLLECTION_NAME = "My Collection"


def utcnow_iso():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def placeholder(conn):
    return "%s" if get_dialect(conn) == "postgres" else "?"


def ensure_collection_schema(conn):
    dialect = get_dialect(conn)
    pk = id_column_sql(dialect)
    c = conn.cursor()
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS collections (
            {pk},
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS collection_items (
            {pk},
            collection_id INTEGER NOT NULL,
            target_kind TEXT NOT NULL,
            tracked_product_id INTEGER NOT NULL,
            tcgplayer_product_id INTEGER,
            quantity REAL NOT NULL DEFAULT 1,
            unit_cost REAL,
            acquired_at TEXT,
            notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (collection_id) REFERENCES collections (id)
        )
        """
    )

    item_cols = table_columns(conn, "collection_items")
    if "tcgplayer_product_id" not in item_cols:
        c.execute("ALTER TABLE collection_items ADD COLUMN tcgplayer_product_id INTEGER")
    if "quantity" not in item_cols:
        c.execute("ALTER TABLE collection_items ADD COLUMN quantity REAL NOT NULL DEFAULT 1")
    if "unit_cost" not in item_cols:
        c.execute("ALTER TABLE collection_items ADD COLUMN unit_cost REAL")
    if "acquired_at" not in item_cols:
        c.execute("ALTER TABLE collection_items ADD COLUMN acquired_at TEXT")
    if "notes" not in item_cols:
        c.execute("ALTER TABLE collection_items ADD COLUMN notes TEXT")
    if "created_at" not in item_cols:
        c.execute("ALTER TABLE collection_items ADD COLUMN created_at TEXT")
        c.execute("UPDATE collection_items SET created_at = ? WHERE created_at IS NULL", (utcnow_iso(),))
    if "updated_at" not in item_cols:
        c.execute("ALTER TABLE collection_items ADD COLUMN updated_at TEXT")
        c.execute("UPDATE collection_items SET updated_at = ? WHERE updated_at IS NULL", (utcnow_iso(),))

    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_collections_name_unique ON collections (name)")
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_collection_items_unique
        ON collection_items (collection_id, target_kind, tracked_product_id)
        """
    )
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_collection_items_collection
        ON collection_items (collection_id, target_kind, updated_at)
        """
    )
    conn.commit()


def ensure_collection(conn, name=DEFAULT_COLLECTION_NAME):
    ensure_collection_schema(conn)
    c = conn.cursor()
    token = placeholder(conn)
    c.execute(f"SELECT id FROM collections WHERE name = {token}", (name,))
    row = c.fetchone()
    now = utcnow_iso()
    if row:
        c.execute(f"UPDATE collections SET updated_at = {token} WHERE id = {token}", (now, row[0]))
        conn.commit()
        return row[0]

    collection_id = insert_row_returning_id(
        conn,
        "collections",
        ("name", "created_at", "updated_at"),
        (name, now, now),
    )
    conn.commit()
    return collection_id


def _extract_tcgplayer_id_from_url(product_url):
    if not product_url:
        return None
    parts = str(product_url).split("/product/")
    if len(parts) < 2:
        return None
    tail = parts[1].split("/", 1)[0]
    try:
        return int(tail)
    except Exception:
        return None


def resolve_tracked_product(conn, target_kind, tracked_product_id=0, tcgplayer_product_id=0, product_url=""):
    target_kind = (target_kind or "sealed").strip().lower()
    c = conn.cursor()
    token = placeholder(conn)

    if target_kind == "cards":
        if tracked_product_id:
            c.execute(
                f"""
                SELECT id, tcgplayer_product_id, name, url
                FROM card_products
                WHERE id = {token}
                """,
                (int(tracked_product_id),),
            )
            row = c.fetchone()
            if row:
                return {
                    "target_kind": "cards",
                    "tracked_product_id": row[0],
                    "tcgplayer_product_id": row[1],
                    "name": row[2],
                    "url": row[3],
                }
        if tcgplayer_product_id:
            c.execute(
                f"""
                SELECT id, tcgplayer_product_id, name, url
                FROM card_products
                WHERE tcgplayer_product_id = {token}
                """,
                (int(tcgplayer_product_id),),
            )
            row = c.fetchone()
            if row:
                return {
                    "target_kind": "cards",
                    "tracked_product_id": row[0],
                    "tcgplayer_product_id": row[1],
                    "name": row[2],
                    "url": row[3],
                }
        if product_url:
            c.execute(
                f"""
                SELECT id, tcgplayer_product_id, name, url
                FROM card_products
                WHERE url = {token}
                """,
                (product_url,),
            )
            row = c.fetchone()
            if row:
                return {
                    "target_kind": "cards",
                    "tracked_product_id": row[0],
                    "tcgplayer_product_id": row[1],
                    "name": row[2],
                    "url": row[3],
                }
    else:
        has_product_details = table_exists(conn, "product_details")
        if tracked_product_id:
            if has_product_details:
                c.execute(
                    f"""
                    SELECT p.id, d.tcgplayer_product_id, p.name, p.url
                    FROM products p
                    LEFT JOIN product_details d ON d.product_id = p.id
                    WHERE p.id = {token}
                    """,
                    (int(tracked_product_id),),
                )
            else:
                c.execute(
                    f"""
                    SELECT p.id, NULL AS tcgplayer_product_id, p.name, p.url
                    FROM products p
                    WHERE p.id = {token}
                    """,
                    (int(tracked_product_id),),
                )
            row = c.fetchone()
            if row:
                return {
                    "target_kind": "sealed",
                    "tracked_product_id": row[0],
                    "tcgplayer_product_id": row[1],
                    "name": row[2],
                    "url": row[3],
                }
        if tcgplayer_product_id and has_product_details:
            c.execute(
                f"""
                SELECT p.id, d.tcgplayer_product_id, p.name, p.url
                FROM products p
                JOIN product_details d ON d.product_id = p.id
                WHERE d.tcgplayer_product_id = {token}
                """,
                (int(tcgplayer_product_id),),
            )
            row = c.fetchone()
            if row:
                return {
                    "target_kind": "sealed",
                    "tracked_product_id": row[0],
                    "tcgplayer_product_id": row[1],
                    "name": row[2],
                    "url": row[3],
                }
        if product_url:
            if has_product_details:
                c.execute(
                    f"""
                    SELECT p.id, d.tcgplayer_product_id, p.name, p.url
                    FROM products p
                    LEFT JOIN product_details d ON d.product_id = p.id
                    WHERE p.url = {token}
                       OR d.source_url = {token}
                    """,
                    (product_url, product_url),
                )
            else:
                c.execute(
                    f"""
                    SELECT p.id, NULL AS tcgplayer_product_id, p.name, p.url
                    FROM products p
                    WHERE p.url = {token}
                    """,
                    (product_url,),
                )
            row = c.fetchone()
            if row:
                return {
                    "target_kind": "sealed",
                    "tracked_product_id": row[0],
                    "tcgplayer_product_id": row[1],
                    "name": row[2],
                    "url": row[3],
                }

    if not tcgplayer_product_id and product_url:
        tcgplayer_product_id = _extract_tcgplayer_id_from_url(product_url) or 0
        if tcgplayer_product_id:
            return resolve_tracked_product(
                conn,
                target_kind,
                tracked_product_id=tracked_product_id,
                tcgplayer_product_id=tcgplayer_product_id,
                product_url="",
            )

    raise ValueError(f"Could not resolve {target_kind} product from the provided identifier.")


def _weighted_cost(existing_qty, existing_unit_cost, add_qty, add_unit_cost):
    if add_unit_cost is None:
        return existing_unit_cost
    if existing_unit_cost is None or existing_qty <= 0:
        return add_unit_cost
    total_cost = (existing_qty * existing_unit_cost) + (add_qty * add_unit_cost)
    total_qty = existing_qty + add_qty
    if total_qty <= 0:
        return add_unit_cost
    return total_cost / total_qty


def add_collection_item(
    conn,
    collection_name=DEFAULT_COLLECTION_NAME,
    target_kind="sealed",
    tracked_product_id=0,
    tcgplayer_product_id=0,
    product_url="",
    quantity=1.0,
    unit_cost=None,
    acquired_at="",
    notes="",
):
    ensure_collection_schema(conn)
    if quantity <= 0:
        raise ValueError("Quantity must be greater than 0.")

    collection_id = ensure_collection(conn, collection_name)
    resolved = resolve_tracked_product(
        conn,
        target_kind,
        tracked_product_id=tracked_product_id,
        tcgplayer_product_id=tcgplayer_product_id,
        product_url=product_url,
    )
    c = conn.cursor()
    token = placeholder(conn)
    c.execute(
        f"""
        SELECT id, quantity, unit_cost
        FROM collection_items
        WHERE collection_id = {token}
          AND target_kind = {token}
          AND tracked_product_id = {token}
        """,
        (collection_id, resolved["target_kind"], resolved["tracked_product_id"]),
    )
    row = c.fetchone()
    now = utcnow_iso()
    quantity = float(quantity)
    unit_cost_value = None if unit_cost in (None, "") else float(unit_cost)
    if row:
        item_id, existing_qty, existing_unit_cost = row
        next_qty = float(existing_qty or 0) + quantity
        next_unit_cost = _weighted_cost(float(existing_qty or 0), existing_unit_cost, quantity, unit_cost_value)
        c.execute(
            f"""
            UPDATE collection_items
            SET quantity = {token},
                unit_cost = {token},
                tcgplayer_product_id = {token},
                acquired_at = COALESCE({token}, acquired_at),
                notes = CASE WHEN {token} != '' THEN {token} ELSE notes END,
                updated_at = {token}
            WHERE id = {token}
            """,
            (
                next_qty,
                next_unit_cost,
                resolved["tcgplayer_product_id"],
                acquired_at or None,
                notes or "",
                notes or "",
                now,
                item_id,
            ),
        )
    else:
        insert_row_returning_id(
            conn,
            "collection_items",
            (
                "collection_id",
                "target_kind",
                "tracked_product_id",
                "tcgplayer_product_id",
                "quantity",
                "unit_cost",
                "acquired_at",
                "notes",
                "created_at",
                "updated_at",
            ),
            (
                collection_id,
                resolved["target_kind"],
                resolved["tracked_product_id"],
                resolved["tcgplayer_product_id"],
                quantity,
                unit_cost_value,
                acquired_at or None,
                notes or None,
                now,
                now,
            ),
        )
    conn.commit()
    return resolved


def _latest_sealed_points(conn, tracked_product_id):
    rows = conn.execute(
        """
        SELECT
            COALESCE(snapshot_date, substr(timestamp, 1, 10)) AS point_date,
            market_price,
            lowest_total_price,
            lowest_price
        FROM listings
        WHERE product_id = ?
        ORDER BY COALESCE(snapshot_date, substr(timestamp, 1, 10)) DESC, timestamp DESC, id DESC
        LIMIT 12
        """,
        (tracked_product_id,),
    ).fetchall()
    points = []
    for point_date, market_price, lowest_total_price, lowest_price in rows:
        for source, price in (
            ("listing_market", market_price),
            ("listing_lowest_total", lowest_total_price),
            ("listing_lowest", lowest_price),
        ):
            if price is not None:
                points.append((point_date, float(price), source))
                break
    if points:
        return points

    history_rows = conn.execute(
        """
        SELECT
            bucket_start_date,
            market_price,
            avg_sale_price_with_shipping,
            avg_sale_price,
            low_sale_price_with_shipping,
            low_sale_price
        FROM price_history
        WHERE product_id = ?
        ORDER BY bucket_start_date DESC, scraped_at DESC
        LIMIT 12
        """,
        (tracked_product_id,),
    ).fetchall()
    for point_date, market_price, avg_ship, avg_sale, low_ship, low_sale in history_rows:
        for source, price in (
            ("price_history_market", market_price),
            ("price_history_avg_sale_ship", avg_ship),
            ("price_history_avg_sale", avg_sale),
            ("price_history_low_ship", low_ship),
            ("price_history_low", low_sale),
        ):
            if price is not None:
                points.append((point_date, float(price), source))
                break
    if points:
        return points

    sales_rows = conn.execute(
        """
        SELECT
            sale_date,
            COALESCE(purchase_price, 0) + COALESCE(shipping_price, 0)
        FROM sales
        WHERE product_id = ?
        ORDER BY sale_date DESC, id DESC
        LIMIT 12
        """,
        (tracked_product_id,),
    ).fetchall()
    return [(sale_date, float(total), "recent_sale") for sale_date, total in sales_rows if total is not None]


def _latest_card_points(conn, tracked_product_id):
    rows = conn.execute(
        """
        SELECT
            bucket_start_date,
            market_price,
            avg_sale_price_with_shipping,
            avg_sale_price,
            low_sale_price_with_shipping,
            low_sale_price
        FROM card_price_history
        WHERE card_product_id = ?
        ORDER BY bucket_start_date DESC, scraped_at DESC
        LIMIT 20
        """,
        (tracked_product_id,),
    ).fetchall()
    points = []
    for point_date, market_price, avg_ship, avg_sale, low_ship, low_sale in rows:
        for source, price in (
            ("card_history_market", market_price),
            ("card_history_avg_sale_ship", avg_ship),
            ("card_history_avg_sale", avg_sale),
            ("card_history_low_ship", low_ship),
            ("card_history_low", low_sale),
        ):
            if price is not None:
                points.append((point_date, float(price), source))
                break
    if points:
        return points

    sales_rows = conn.execute(
        """
        SELECT
            sale_date,
            COALESCE(purchase_price, 0) + COALESCE(shipping_price, 0)
        FROM card_sales
        WHERE card_product_id = ?
        ORDER BY sale_date DESC, id DESC
        LIMIT 20
        """,
        (tracked_product_id,),
    ).fetchall()
    return [(sale_date, float(total), "recent_card_sale") for sale_date, total in sales_rows if total is not None]


def _valuation_snapshot(conn, target_kind, tracked_product_id):
    points = (
        _latest_card_points(conn, tracked_product_id)
        if target_kind == "cards"
        else _latest_sealed_points(conn, tracked_product_id)
    )
    latest = points[0] if points else None
    previous = points[1] if len(points) > 1 else None
    latest_value = latest[1] if latest else None
    previous_value = previous[1] if previous else None
    latest_date = latest[0] if latest else None
    source = latest[2] if latest else None
    change_pct = None
    if latest_value not in (None, 0) and previous_value not in (None, 0):
        change_pct = ((latest_value - previous_value) / previous_value) * 100.0
    return {
        "latest_value": latest_value,
        "latest_date": latest_date,
        "previous_value": previous_value,
        "change_pct": change_pct,
        "price_source": source,
    }


def fetch_collection_items(conn, collection_name=DEFAULT_COLLECTION_NAME, limit=100):
    ensure_collection_schema(conn)
    c = conn.cursor()
    token = placeholder(conn)
    c.execute(f"SELECT id FROM collections WHERE name = {token}", (collection_name,))
    row = c.fetchone()
    if not row:
        return []
    collection_id = row[0]
    c.execute(
        f"""
        SELECT
            ci.id,
            ci.target_kind,
            ci.tracked_product_id,
            ci.tcgplayer_product_id,
            ci.quantity,
            ci.unit_cost,
            ci.acquired_at,
            ci.notes,
            COALESCE(p.name, cp.name) AS display_name,
            COALESCE(p.url, cp.url) AS display_url,
            COALESCE(pd.set_name, cp.set_name, cd.set_name) AS set_name,
            COALESCE(pd.release_date, p.release_date, cd.release_date) AS release_date
        FROM collection_items ci
        LEFT JOIN products p
          ON ci.target_kind = 'sealed'
         AND p.id = ci.tracked_product_id
        LEFT JOIN product_details pd
          ON ci.target_kind = 'sealed'
         AND pd.product_id = p.id
        LEFT JOIN card_products cp
          ON ci.target_kind = 'cards'
         AND cp.id = ci.tracked_product_id
        LEFT JOIN card_details cd
          ON ci.target_kind = 'cards'
         AND cd.card_product_id = cp.id
        WHERE ci.collection_id = {token}
        ORDER BY ci.updated_at DESC, ci.id DESC
        LIMIT {token}
        """,
        (collection_id, int(limit)),
    )
    items = []
    for row in c.fetchall():
        (
            item_id,
            target_kind,
            tracked_product_id,
            tcgplayer_product_id,
            quantity,
            unit_cost,
            acquired_at,
            notes,
            display_name,
            display_url,
            set_name,
            release_date,
        ) = row
        valuation = _valuation_snapshot(conn, target_kind, tracked_product_id)
        current_unit_value = valuation["latest_value"]
        current_value = current_unit_value * quantity if current_unit_value is not None else None
        cost_basis = unit_cost * quantity if unit_cost is not None else None
        unrealized_pnl = current_value - cost_basis if current_value is not None and cost_basis is not None else None
        items.append(
            {
                "item_id": item_id,
                "target_kind": target_kind,
                "tracked_product_id": tracked_product_id,
                "tcgplayer_product_id": tcgplayer_product_id,
                "name": display_name,
                "url": display_url,
                "set_name": set_name,
                "release_date": release_date,
                "quantity": float(quantity or 0),
                "unit_cost": unit_cost,
                "acquired_at": acquired_at,
                "notes": notes,
                "current_unit_value": current_unit_value,
                "current_value": current_value,
                "cost_basis": cost_basis,
                "unrealized_pnl": unrealized_pnl,
                "change_pct": valuation["change_pct"],
                "valuation_date": valuation["latest_date"],
                "price_source": valuation["price_source"],
            }
        )
    return items


def fetch_collection_summary(conn, collection_name=DEFAULT_COLLECTION_NAME, limit=100):
    items = fetch_collection_items(conn, collection_name=collection_name, limit=limit)
    item_count = len(items)
    total_units = sum(item["quantity"] for item in items)
    estimated_value = sum(item["current_value"] for item in items if item["current_value"] is not None)
    cost_basis_known = [item["cost_basis"] for item in items if item["cost_basis"] is not None]
    cost_basis = sum(cost_basis_known) if cost_basis_known else None
    unrealized_pnl = (
        estimated_value - cost_basis
        if cost_basis is not None and estimated_value is not None
        else None
    )
    unrealized_pct = (
        (unrealized_pnl / cost_basis) * 100.0
        if cost_basis not in (None, 0) and unrealized_pnl is not None
        else None
    )
    movers = sorted(
        [item for item in items if item["change_pct"] is not None],
        key=lambda item: abs(item["change_pct"]),
        reverse=True,
    )
    return {
        "collection_name": collection_name,
        "item_count": item_count,
        "total_units": total_units,
        "estimated_value": estimated_value,
        "cost_basis": cost_basis,
        "unrealized_pnl": unrealized_pnl,
        "unrealized_pct": unrealized_pct,
        "items": items,
        "top_movers": movers[:10],
    }


def _fmt_money(value):
    if value is None:
        return "-"
    return f"${value:,.2f}"


def _fmt_pct(value):
    if value is None:
        return "-"
    return f"{value:.2f}%"


def print_collection_summary(summary):
    print(json.dumps(
        {
            "collection_name": summary["collection_name"],
            "item_count": summary["item_count"],
            "total_units": summary["total_units"],
            "estimated_value": summary["estimated_value"],
            "cost_basis": summary["cost_basis"],
            "unrealized_pnl": summary["unrealized_pnl"],
            "unrealized_pct": summary["unrealized_pct"],
            "top_movers": [
                {
                    "name": item["name"],
                    "change_pct": item["change_pct"],
                    "current_value": item["current_value"],
                }
                for item in summary["top_movers"]
            ],
        },
        indent=2,
        sort_keys=True,
    ))


def main():
    parser = argparse.ArgumentParser(description="Manage local collection holdings")
    parser.add_argument("--db", default="sealed_market.db")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add")
    add_parser.add_argument("--collection", default=DEFAULT_COLLECTION_NAME)
    add_parser.add_argument("--target-kind", default="sealed", choices=["sealed", "cards"])
    add_parser.add_argument("--tracked-product-id", type=int, default=0)
    add_parser.add_argument("--tcgplayer-product-id", type=int, default=0)
    add_parser.add_argument("--product-url", default="")
    add_parser.add_argument("--quantity", type=float, default=1.0)
    add_parser.add_argument("--unit-cost", type=float, default=None)
    add_parser.add_argument("--acquired-at", default="")
    add_parser.add_argument("--notes", default="")

    summary_parser = subparsers.add_parser("summary")
    summary_parser.add_argument("--collection", default=DEFAULT_COLLECTION_NAME)
    summary_parser.add_argument("--limit", type=int, default=100)

    args = parser.parse_args()
    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    try:
        if args.command == "add":
            resolved = add_collection_item(
                conn,
                collection_name=args.collection,
                target_kind=args.target_kind,
                tracked_product_id=args.tracked_product_id,
                tcgplayer_product_id=args.tcgplayer_product_id,
                product_url=args.product_url,
                quantity=args.quantity,
                unit_cost=args.unit_cost,
                acquired_at=args.acquired_at,
                notes=args.notes,
            )
            print(
                json.dumps(
                    {
                        "collection": args.collection,
                        "target_kind": resolved["target_kind"],
                        "tracked_product_id": resolved["tracked_product_id"],
                        "tcgplayer_product_id": resolved["tcgplayer_product_id"],
                        "name": resolved["name"],
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        elif args.command == "summary":
            summary = fetch_collection_summary(conn, collection_name=args.collection, limit=args.limit)
            print_collection_summary(summary)
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
