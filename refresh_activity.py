"""Refresh scrape activity and prioritization tiers for sealed and card products."""

import argparse
from datetime import datetime, timedelta

from db import configure_connection, connect_database, resolve_database_target
from populate_db import ensure_runtime_schema


def parse_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def iso(value):
    return value.isoformat() if value else None


def classify_priority(recent_sales_30d, last_snapshot_at):
    recent_sales_30d = int(recent_sales_30d or 0)
    if recent_sales_30d >= 10:
        return "hot", 24, float(recent_sales_30d) * 10.0 + 5.0
    if recent_sales_30d >= 1:
        return "warm", 72, float(recent_sales_30d) * 10.0 + 2.0
    if last_snapshot_at:
        return "cold", 168, 2.0
    return "dormant", 720, 0.5


def upsert_activity(conn, row):
    conn.execute(
        """
        INSERT INTO scrape_activity (
            target_kind, target_id, set_id, priority_tier, priority_score,
            recent_sales_30d, last_sale_at, last_snapshot_at, next_due_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(target_kind, target_id) DO UPDATE SET
            set_id = excluded.set_id,
            priority_tier = excluded.priority_tier,
            priority_score = excluded.priority_score,
            recent_sales_30d = excluded.recent_sales_30d,
            last_sale_at = excluded.last_sale_at,
            last_snapshot_at = excluded.last_snapshot_at,
            next_due_at = excluded.next_due_at,
            updated_at = excluded.updated_at
        """,
        (
            row["target_kind"],
            row["target_id"],
            row.get("set_id"),
            row["priority_tier"],
            row["priority_score"],
            row["recent_sales_30d"],
            row.get("last_sale_at"),
            row.get("last_snapshot_at"),
            row["next_due_at"],
            row["updated_at"],
        ),
    )


def build_sealed_activity(conn, now):
    rows = conn.execute(
        """
        SELECT
            p.id,
            d.set_id,
            (
                SELECT MAX(l.timestamp)
                FROM listings l
                WHERE l.product_id = p.id
            ) AS last_snapshot_at,
            (
                SELECT COUNT(*)
                FROM sales s
                WHERE s.product_id = p.id
                  AND s.sale_date >= date('now', '-30 day')
            ) AS recent_sales_30d,
            (
                SELECT MAX(s.sale_date)
                FROM sales s
                WHERE s.product_id = p.id
            ) AS last_sale_at
        FROM products p
        LEFT JOIN product_details d ON d.product_id = p.id
        WHERE p.url IS NOT NULL AND p.url != ''
        """
    ).fetchall()
    built = []
    for product_id, set_id, last_snapshot_at, recent_sales_30d, last_sale_at in rows:
        tier, interval_hours, score = classify_priority(recent_sales_30d, last_snapshot_at)
        last_snapshot_dt = parse_iso(last_snapshot_at)
        next_due = now if not last_snapshot_dt else (last_snapshot_dt + timedelta(hours=interval_hours))
        built.append(
            {
                "target_kind": "sealed",
                "target_id": int(product_id),
                "set_id": set_id,
                "priority_tier": tier,
                "priority_score": score,
                "recent_sales_30d": int(recent_sales_30d or 0),
                "last_sale_at": last_sale_at,
                "last_snapshot_at": last_snapshot_at,
                "next_due_at": iso(next_due),
                "updated_at": iso(now),
            }
        )
    return built


def build_card_activity(conn, now):
    rows = conn.execute(
        """
        SELECT
            p.id,
            p.set_id,
            (
                SELECT COUNT(*)
                FROM card_sales s
                WHERE s.card_product_id = p.id
                  AND s.sale_date >= date('now', '-30 day')
            ) AS recent_sales_30d,
            (
                SELECT MAX(s.sale_date)
                FROM card_sales s
                WHERE s.card_product_id = p.id
            ) AS last_sale_at
        FROM card_products p
        WHERE p.url IS NOT NULL AND p.url != ''
        """
    ).fetchall()
    built = []
    for product_id, set_id, recent_sales_30d, last_sale_at in rows:
        tier, interval_hours, score = classify_priority(recent_sales_30d, None)
        last_sale_dt = parse_iso(last_sale_at)
        next_due = now if not last_sale_dt else (last_sale_dt + timedelta(hours=interval_hours))
        built.append(
            {
                "target_kind": "cards",
                "target_id": int(product_id),
                "set_id": set_id,
                "priority_tier": tier,
                "priority_score": score,
                "recent_sales_30d": int(recent_sales_30d or 0),
                "last_sale_at": last_sale_at,
                "last_snapshot_at": None,
                "next_due_at": iso(next_due),
                "updated_at": iso(now),
            }
        )
    return built


def refresh_activity(conn):
    now = datetime.utcnow()
    rows = build_sealed_activity(conn, now) + build_card_activity(conn, now)
    for row in rows:
        upsert_activity(conn, row)
    conn.commit()
    return {
        "targets": len(rows),
        "hot": sum(1 for row in rows if row["priority_tier"] == "hot"),
        "warm": sum(1 for row in rows if row["priority_tier"] == "warm"),
        "cold": sum(1 for row in rows if row["priority_tier"] == "cold"),
        "dormant": sum(1 for row in rows if row["priority_tier"] == "dormant"),
    }


def main():
    parser = argparse.ArgumentParser(description="Refresh scrape activity priorities")
    parser.add_argument("--db", default="sealed_market.db")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    result = refresh_activity(conn)
    conn.close()
    print(result, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
