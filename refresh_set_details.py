"""Materialize set-level detail rows from canonical sets and derived stats."""

import argparse
import json

from build_set_stats import build_set_stats
from db import configure_connection, connect_database, resolve_database_target, sql_placeholder_list
from populate_db import ensure_runtime_schema


def build_set_detail_rows(conn):
    rows = conn.execute(
        """
        SELECT
            s.id,
            s.name,
            s.category_slug,
            s.product_line,
            s.set_type,
            COALESCE(ss.release_date, s.release_date) AS release_date,
            COALESCE(ss.first_seen_at, s.first_seen_at) AS first_seen_at,
            COALESCE(ss.last_seen_at, s.last_seen_at) AS last_seen_at,
            COALESCE(ss.sealed_product_count, 0) AS sealed_product_count,
            COALESCE(ss.card_product_count, 0) AS card_product_count,
            COALESCE(ss.total_product_count, 0) AS total_product_count,
            COALESCE(ss.total_sale_count, 0) AS total_sale_count,
            ss.detail_coverage_pct,
            ss.sales_coverage_pct,
            ss.sealed_last_listing_at,
            ss.total_last_sale_at,
            COALESCE(ss.refreshed_at, s.last_seen_at) AS refreshed_at
        FROM sets s
        LEFT JOIN set_stats ss ON ss.set_id = s.id
        ORDER BY s.name
        """
    ).fetchall()

    payload_rows = []
    for row in rows:
        payload = {
            "set_id": int(row[0]),
            "set_name": row[1],
            "category_slug": row[2],
            "product_line": row[3],
            "set_type": row[4],
            "release_date": row[5],
            "first_seen_at": row[6],
            "last_seen_at": row[7],
            "sealed_product_count": int(row[8] or 0),
            "card_product_count": int(row[9] or 0),
            "total_product_count": int(row[10] or 0),
            "total_sale_count": int(row[11] or 0),
            "detail_coverage_pct": row[12],
            "sales_coverage_pct": row[13],
            "sealed_last_listing_at": row[14],
            "total_last_sale_at": row[15],
            "refreshed_at": row[16],
        }
        payload["summary_json"] = json.dumps(payload, sort_keys=True, default=str)
        payload_rows.append(payload)
    return payload_rows


def write_set_detail_rows(conn, rows):
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
        "card_product_count",
        "total_product_count",
        "total_sale_count",
        "detail_coverage_pct",
        "sales_coverage_pct",
        "sealed_last_listing_at",
        "total_last_sale_at",
        "refreshed_at",
        "summary_json",
    ]
    placeholders = sql_placeholder_list(conn, len(columns))
    updates = ", ".join(f"{column} = excluded.{column}" for column in columns if column != "set_id")
    sql = f"""
        INSERT INTO set_details ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT(set_id) DO UPDATE SET {updates}
    """
    conn.execute("DELETE FROM set_details")
    for row in rows:
        conn.execute(sql, tuple(row[column] for column in columns))
    conn.commit()


def refresh_set_details(conn):
    build_set_stats(conn)
    rows = build_set_detail_rows(conn)
    write_set_detail_rows(conn, rows)
    return {"set_details_written": len(rows)}


def main():
    parser = argparse.ArgumentParser(description="Populate set_details from current sets and set_stats")
    parser.add_argument("--db", default="sealed_market.db")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    result = refresh_set_details(conn)
    conn.close()
    print(result, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
