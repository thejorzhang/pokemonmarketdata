"""Plan set-aware refresh work from refresh_priority."""

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone

from db import configure_connection, connect_database, resolve_database_target
from populate_db import ensure_runtime_schema


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def load_due_rows(conn, target_kind, tier="", set_name="", limit=0):
    params = [target_kind, utcnow_iso()]
    query = """
        SELECT target_id, COALESCE(set_name, '(unknown)') AS set_name, activity_score, priority_tier
        FROM refresh_priority
        WHERE target_kind = ?
          AND (next_refresh_at IS NULL OR next_refresh_at <= ?)
    """
    if tier:
        query += " AND priority_tier = ?"
        params.append(tier)
    if set_name:
        query += " AND set_name = ?"
        params.append(set_name)
    query += " ORDER BY activity_score DESC, set_name ASC, target_id ASC"
    if limit and limit > 0:
        query += f" LIMIT {int(limit)}"
    return conn.execute(query, params).fetchall()


def build_worker_plan(rows, workers):
    buckets = [{"worker": index + 1, "score": 0.0, "items": []} for index in range(workers)]
    grouped = defaultdict(list)
    for target_id, set_name, score, tier in rows:
        grouped[set_name].append((target_id, float(score or 0), tier))

    set_groups = []
    for set_name, items in grouped.items():
        total_score = sum(item[1] for item in items)
        set_groups.append((set_name, total_score, items))
    set_groups.sort(key=lambda item: (-item[1], item[0]))

    for set_name, total_score, items in set_groups:
        bucket = min(buckets, key=lambda entry: (entry["score"], entry["worker"]))
        bucket["items"].append(
            {
                "set_name": set_name,
                "count": len(items),
                "score": round(total_score, 2),
                "target_ids": [item[0] for item in items],
            }
        )
        bucket["score"] += total_score
    return buckets


def export_sealed_csv(conn, rows, out_path):
    if not out_path:
        return 0
    target_ids = [int(row[0]) for row in rows]
    if not target_ids:
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["name", "url"])
        return 0
    placeholders = ", ".join(["?"] * len(target_ids))
    query = f"SELECT name, url FROM products WHERE id IN ({placeholders}) ORDER BY name, id"
    product_rows = conn.execute(query, target_ids).fetchall()
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["name", "url"])
        writer.writerows(product_rows)
    return len(product_rows)


def main():
    parser = argparse.ArgumentParser(description="Plan due refresh work by set and priority")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--target-kind", choices=["sealed", "cards"], default="sealed")
    parser.add_argument("--tier", default="")
    parser.add_argument("--set-name", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--out-csv", default="", help="For sealed targets, optionally export the due rows to a CSV")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    rows = load_due_rows(conn, args.target_kind, tier=args.tier, set_name=args.set_name, limit=args.limit)
    plan = build_worker_plan(rows, max(1, int(args.workers)))
    exported = 0
    if args.target_kind == "sealed" and args.out_csv:
        exported = export_sealed_csv(conn, rows, args.out_csv)
    conn.close()
    print(
        json.dumps(
            {
                "target_kind": args.target_kind,
                "due_items": len(rows),
                "exported_rows": exported,
                "workers": plan,
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
