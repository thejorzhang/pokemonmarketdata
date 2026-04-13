"""Build materialized set-level MVP stats from the current database."""

import argparse
import json
from datetime import datetime, timedelta

from db import (
    configure_connection,
    connect_database,
    get_dialect,
    resolve_database_target,
    sql_placeholder_list,
)
from populate_db import ensure_runtime_schema
from refresh_sets import refresh_sets


def utcnow():
    return datetime.utcnow()


def iso_date_days_ago(days):
    return (utcnow().date() - timedelta(days=days)).isoformat()


def placeholder(conn):
    return "%s" if get_dialect(conn) == "postgres" else "?"


def fetch_sets(conn):
    return conn.execute(
        """
        SELECT id, name, category_slug, product_line, set_type, release_date, first_seen_at, last_seen_at
        FROM sets
        ORDER BY name
        """
    ).fetchall()


def fetch_scalar(conn, query, params=()):
    row = conn.execute(query, tuple(params)).fetchone()
    return row[0] if row else None


def max_date(*values):
    cleaned = [value for value in values if value]
    return max(cleaned) if cleaned else None


def build_rows(conn, active_days=30):
    cutoff = iso_date_days_ago(active_days)
    rows = []

    for set_id, set_name, category_slug, product_line, set_type, set_release_date, first_seen_at, last_seen_at in fetch_sets(conn):
        sealed_product_count = int(
            fetch_scalar(conn, "SELECT COUNT(DISTINCT product_id) FROM product_details WHERE set_id = ?", (set_id,)) or 0
        )
        sealed_detail_count = sealed_product_count
        sealed_listing_count = int(
            fetch_scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM listings l
                JOIN product_details d ON d.product_id = l.product_id
                WHERE d.set_id = ?
                """,
                (set_id,),
            )
            or 0
        )
        sealed_sale_count = int(
            fetch_scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM sales s
                JOIN product_details d ON d.product_id = s.product_id
                WHERE d.set_id = ?
                """,
                (set_id,),
            )
            or 0
        )
        sealed_products_with_sales = int(
            fetch_scalar(
                conn,
                """
                SELECT COUNT(DISTINCT s.product_id)
                FROM sales s
                JOIN product_details d ON d.product_id = s.product_id
                WHERE d.set_id = ?
                """,
                (set_id,),
            )
            or 0
        )
        sealed_last_listing_at = fetch_scalar(
            conn,
            """
            SELECT MAX(l.timestamp)
            FROM listings l
            JOIN product_details d ON d.product_id = l.product_id
            WHERE d.set_id = ?
            """,
            (set_id,),
        )
        sealed_last_sale_at = fetch_scalar(
            conn,
            """
            SELECT MAX(s.sale_date)
            FROM sales s
            JOIN product_details d ON d.product_id = s.product_id
            WHERE d.set_id = ?
            """,
            (set_id,),
        )

        card_product_count = int(
            fetch_scalar(
                conn,
                """
                SELECT COUNT(DISTINCT p.id)
                FROM card_products p
                LEFT JOIN card_details d ON d.card_product_id = p.id
                WHERE p.set_id = ? OR COALESCE(p.set_name, '') = ? OR COALESCE(d.set_name, '') = ?
                """,
                (set_id, set_name, set_name),
            )
            or 0
        )
        card_detail_count = int(
            fetch_scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM card_details d
                JOIN card_products p ON p.id = d.card_product_id
                WHERE p.set_id = ? OR COALESCE(p.set_name, '') = ? OR COALESCE(d.set_name, '') = ?
                """,
                (set_id, set_name, set_name),
            )
            or 0
        )
        card_sale_count = int(
            fetch_scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM card_sales s
                JOIN card_products p ON p.id = s.card_product_id
                WHERE p.set_id = ? OR COALESCE(p.set_name, '') = ?
                """,
                (set_id, set_name),
            )
            or 0
        )
        card_products_with_sales = int(
            fetch_scalar(
                conn,
                """
                SELECT COUNT(DISTINCT s.card_product_id)
                FROM card_sales s
                JOIN card_products p ON p.id = s.card_product_id
                WHERE p.set_id = ? OR COALESCE(p.set_name, '') = ?
                """,
                (set_id, set_name),
            )
            or 0
        )
        card_last_sale_at = fetch_scalar(
            conn,
            """
            SELECT MAX(s.sale_date)
            FROM card_sales s
            JOIN card_products p ON p.id = s.card_product_id
            WHERE p.set_id = ? OR COALESCE(p.set_name, '') = ?
            """,
            (set_id, set_name),
        )

        priority_row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_targets,
                SUM(CASE WHEN priority_tier = 'hot' THEN 1 ELSE 0 END) AS hot_count,
                SUM(CASE WHEN priority_tier = 'warm' THEN 1 ELSE 0 END) AS warm_count,
                SUM(CASE WHEN priority_tier = 'cold' THEN 1 ELSE 0 END) AS cold_count,
                SUM(CASE WHEN priority_tier = 'dormant' THEN 1 ELSE 0 END) AS dormant_count,
                AVG(activity_score) AS avg_score,
                MAX(activity_score) AS max_score
            FROM refresh_priority
            WHERE COALESCE(set_id, 0) = {placeholder(conn)} OR COALESCE(set_name, '') = {placeholder(conn)}
            """,
            (int(set_id), set_name),
        ).fetchone()
        priority_target_count = int(priority_row[0] or 0)
        priority_hot_count = int(priority_row[1] or 0)
        priority_warm_count = int(priority_row[2] or 0)
        priority_cold_count = int(priority_row[3] or 0)
        priority_dormant_count = int(priority_row[4] or 0)
        priority_avg_score = float(priority_row[5]) if priority_row and priority_row[5] is not None else None
        priority_max_score = float(priority_row[6]) if priority_row and priority_row[6] is not None else None

        total_product_count = sealed_product_count + card_product_count
        total_detail_count = sealed_detail_count + card_detail_count
        total_sale_count = sealed_sale_count + card_sale_count
        total_products_with_sales = sealed_products_with_sales + card_products_with_sales
        total_last_sale_at = max_date(sealed_last_sale_at, card_last_sale_at)
        detail_coverage_pct = (total_detail_count / total_product_count * 100.0) if total_product_count else None
        sales_coverage_pct = (total_products_with_sales / total_product_count * 100.0) if total_product_count else None
        refreshed_at = utcnow().isoformat()

        payload = {
            "set_id": int(set_id),
            "set_name": set_name,
            "category_slug": category_slug,
            "product_line": product_line,
            "set_type": set_type,
            "release_date": max_date(set_release_date),
            "first_seen_at": first_seen_at,
            "last_seen_at": last_seen_at,
            "sealed_product_count": sealed_product_count,
            "sealed_detail_count": sealed_detail_count,
            "sealed_listing_count": sealed_listing_count,
            "sealed_sale_count": sealed_sale_count,
            "sealed_products_with_sales": sealed_products_with_sales,
            "sealed_last_listing_at": sealed_last_listing_at,
            "sealed_last_sale_at": sealed_last_sale_at,
            "card_product_count": card_product_count,
            "card_detail_count": card_detail_count,
            "card_sale_count": card_sale_count,
            "card_products_with_sales": card_products_with_sales,
            "card_last_sale_at": card_last_sale_at,
            "priority_target_count": priority_target_count,
            "priority_hot_count": priority_hot_count,
            "priority_warm_count": priority_warm_count,
            "priority_cold_count": priority_cold_count,
            "priority_dormant_count": priority_dormant_count,
            "priority_avg_score": priority_avg_score,
            "priority_max_score": priority_max_score,
            "total_product_count": total_product_count,
            "total_detail_count": total_detail_count,
            "total_sale_count": total_sale_count,
            "total_products_with_sales": total_products_with_sales,
            "total_last_sale_at": total_last_sale_at,
            "detail_coverage_pct": detail_coverage_pct,
            "sales_coverage_pct": sales_coverage_pct,
            "refreshed_at": refreshed_at,
        }
        payload["summary_json"] = json.dumps(payload, sort_keys=True, default=str)
        rows.append(payload)

    return rows


def write_rows(conn, rows):
    columns = [
        "set_id",
        "set_name",
        "category_slug",
        "product_line",
        "set_type",
        "release_date",
        "first_seen_at",
        "last_seen_at",
        "sealed_product_count",
        "sealed_detail_count",
        "sealed_listing_count",
        "sealed_sale_count",
        "sealed_products_with_sales",
        "sealed_last_listing_at",
        "sealed_last_sale_at",
        "card_product_count",
        "card_detail_count",
        "card_sale_count",
        "card_products_with_sales",
        "card_last_sale_at",
        "priority_target_count",
        "priority_hot_count",
        "priority_warm_count",
        "priority_cold_count",
        "priority_dormant_count",
        "priority_avg_score",
        "priority_max_score",
        "total_product_count",
        "total_detail_count",
        "total_sale_count",
        "total_products_with_sales",
        "total_last_sale_at",
        "detail_coverage_pct",
        "sales_coverage_pct",
        "refreshed_at",
        "summary_json",
    ]
    ph = sql_placeholder_list(conn, len(columns))
    update_sql = ", ".join(f"{col} = excluded.{col}" for col in columns if col != "set_id")
    sql = f"""
        INSERT INTO set_stats ({', '.join(columns)})
        VALUES ({ph})
        ON CONFLICT(set_id) DO UPDATE SET {update_sql}
    """
    conn.execute("DELETE FROM set_stats")
    for row in rows:
        conn.execute(sql, tuple(row[col] for col in columns))
    conn.commit()


def build_set_stats(conn, active_days=30):
    refresh_sets(conn)
    rows = build_rows(conn, active_days=active_days)
    write_rows(conn, rows)
    return {"sets_written": len(rows)}


def main():
    parser = argparse.ArgumentParser(description="Build per-set derived stats for the MVP dashboard")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--active-days", type=int, default=30)
    parser.add_argument("--refresh-sets", dest="refresh_sets", action="store_true", help="Refresh the canonical sets table first")
    parser.add_argument("--skip-refresh-sets", dest="refresh_sets", action="store_false", help="Skip refreshing the canonical sets table first")
    parser.set_defaults(refresh_sets=True)
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    result = build_set_stats(conn, active_days=args.active_days)
    conn.close()
    print(result, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
