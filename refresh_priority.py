"""Compute refresh priority tiers for sealed and card products."""

import argparse
from datetime import datetime, timedelta

from db import configure_connection, connect_database, resolve_database_target
from populate_db import ensure_runtime_schema


def utcnow():
    return datetime.utcnow()


def iso(value):
    return value.isoformat() if value else None


def classify_priority(score, sales_7d, sales_30d):
    if sales_7d >= 5 or score >= 80:
        return "hot", 24
    if sales_30d >= 5 or score >= 30:
        return "warm", 72
    if sales_30d >= 1 or score > 0:
        return "cold", 168
    return "dormant", 720


def latest_listing_map(conn):
    rows = conn.execute(
        """
        SELECT l.product_id, MAX(l.timestamp), MAX(COALESCE(l.listing_count, 0))
        FROM listings l
        GROUP BY l.product_id
        """
    ).fetchall()
    return {int(product_id): (last_snapshot_at, int(listing_count or 0)) for product_id, last_snapshot_at, listing_count in rows}


def sales_stats_map(conn, table_name, fk_column):
    now = utcnow()
    cutoff_30 = (now - timedelta(days=30)).date().isoformat()
    cutoff_7 = (now - timedelta(days=7)).date().isoformat()
    rows = conn.execute(
        f"""
        SELECT
            {fk_column},
            SUM(CASE WHEN sale_date >= ? THEN 1 ELSE 0 END) AS sales_30d,
            SUM(CASE WHEN sale_date >= ? THEN 1 ELSE 0 END) AS sales_7d,
            MAX(sale_date) AS last_sale_at
        FROM {table_name}
        GROUP BY {fk_column}
        """,
        (cutoff_30, cutoff_7),
    ).fetchall()
    return {
        int(target_id): {
            "sales_30d": int(sales_30d or 0),
            "sales_7d": int(sales_7d or 0),
            "last_sale_at": last_sale_at,
        }
        for target_id, sales_30d, sales_7d, last_sale_at in rows
    }


def upsert_priority_row(conn, row):
    existing = conn.execute(
        "SELECT id FROM refresh_priority WHERE target_kind = ? AND target_id = ?",
        (row["target_kind"], row["target_id"]),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE refresh_priority
            SET set_id = ?, set_name = ?, activity_score = ?, priority_tier = ?,
                refresh_interval_hours = ?, sales_7d = ?, sales_30d = ?,
                last_sale_at = ?, last_snapshot_at = ?, next_refresh_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                row["set_id"],
                row["set_name"],
                row["activity_score"],
                row["priority_tier"],
                row["refresh_interval_hours"],
                row["sales_7d"],
                row["sales_30d"],
                row["last_sale_at"],
                row["last_snapshot_at"],
                row["next_refresh_at"],
                row["updated_at"],
                int(existing[0]),
            ),
        )
        return

    conn.execute(
        """
        INSERT INTO refresh_priority (
            target_kind, target_id, set_id, set_name, activity_score, priority_tier,
            refresh_interval_hours, sales_7d, sales_30d, last_sale_at, last_snapshot_at,
            next_refresh_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["target_kind"],
            row["target_id"],
            row["set_id"],
            row["set_name"],
            row["activity_score"],
            row["priority_tier"],
            row["refresh_interval_hours"],
            row["sales_7d"],
            row["sales_30d"],
            row["last_sale_at"],
            row["last_snapshot_at"],
            row["next_refresh_at"],
            row["updated_at"],
        ),
    )


def refresh_priority(conn):
    now = utcnow()
    listing_stats = latest_listing_map(conn)
    sealed_sales = sales_stats_map(conn, "sales", "product_id")
    card_sales = sales_stats_map(conn, "card_sales", "card_product_id")
    updated = 0

    sealed_rows = conn.execute(
        """
        SELECT p.id, p.name, d.set_id, d.set_name
        FROM products p
        LEFT JOIN product_details d ON d.product_id = p.id
        """
    ).fetchall()
    for target_id, name, set_id, set_name in sealed_rows:
        sales = sealed_sales.get(int(target_id), {})
        last_snapshot_at, listing_count = listing_stats.get(int(target_id), (None, 0))
        score = sales.get("sales_7d", 0) * 20 + sales.get("sales_30d", 0) * 5 + min(listing_count, 50)
        tier, interval_hours = classify_priority(score, sales.get("sales_7d", 0), sales.get("sales_30d", 0))
        next_refresh = now if not last_snapshot_at else datetime.fromisoformat(last_snapshot_at) + timedelta(hours=interval_hours)
        upsert_priority_row(
            conn,
            {
                "target_kind": "sealed",
                "target_id": int(target_id),
                "set_id": set_id,
                "set_name": set_name,
                "activity_score": float(score),
                "priority_tier": tier,
                "refresh_interval_hours": interval_hours,
                "sales_7d": sales.get("sales_7d", 0),
                "sales_30d": sales.get("sales_30d", 0),
                "last_sale_at": sales.get("last_sale_at"),
                "last_snapshot_at": last_snapshot_at,
                "next_refresh_at": iso(next_refresh),
                "updated_at": iso(now),
            },
        )
        updated += 1

    card_rows = conn.execute(
        """
        SELECT p.id, p.name, p.set_id, COALESCE(d.set_name, p.set_name) AS set_name
        FROM card_products p
        LEFT JOIN card_details d ON d.card_product_id = p.id
        """
    ).fetchall()
    for target_id, name, set_id, set_name in card_rows:
        sales = card_sales.get(int(target_id), {})
        score = sales.get("sales_7d", 0) * 20 + sales.get("sales_30d", 0) * 5
        tier, interval_hours = classify_priority(score, sales.get("sales_7d", 0), sales.get("sales_30d", 0))
        next_refresh = now + timedelta(hours=interval_hours)
        upsert_priority_row(
            conn,
            {
                "target_kind": "cards",
                "target_id": int(target_id),
                "set_id": set_id,
                "set_name": set_name,
                "activity_score": float(score),
                "priority_tier": tier,
                "refresh_interval_hours": interval_hours,
                "sales_7d": sales.get("sales_7d", 0),
                "sales_30d": sales.get("sales_30d", 0),
                "last_sale_at": sales.get("last_sale_at"),
                "last_snapshot_at": None,
                "next_refresh_at": iso(next_refresh),
                "updated_at": iso(now),
            },
        )
        updated += 1

    conn.commit()
    return {"updated_targets": updated}


def main():
    parser = argparse.ArgumentParser(description="Refresh activity-based scrape priority tiers")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--target-kind", choices=["sealed", "cards", "all"], default="all")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    if args.target_kind == "all":
        result = refresh_priority(conn)
    else:
        all_result = refresh_priority(conn)
        result = {"updated_targets": all_result["updated_targets"], "target_kind": args.target_kind}
    conn.close()
    print(result, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
