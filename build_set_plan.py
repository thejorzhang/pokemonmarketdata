"""Build a set-aware worker plan from due refresh-priority rows."""

import argparse
import csv
import json
from pathlib import Path

from db import configure_connection, connect_database, resolve_database_target
from plan_refresh import build_worker_plan, load_due_rows
from populate_db import ensure_runtime_schema


def export_worker_csvs(conn, plan, out_dir, target_kind="sealed"):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    exported = []
    if target_kind != "sealed":
        return exported

    for worker in plan:
        target_ids = []
        for item in worker["items"]:
            target_ids.extend(item["target_ids"])
        target_ids = [int(target_id) for target_id in target_ids]
        worker_csv = out_path / f"worker_{worker['worker']}.csv"
        with worker_csv.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["name", "url"])
            if target_ids:
                placeholders = ", ".join(["?"] * len(target_ids))
                query = f"SELECT name, url FROM products WHERE id IN ({placeholders}) ORDER BY name, id"
                for row in conn.execute(query, target_ids).fetchall():
                    writer.writerow(row)
        exported.append(str(worker_csv))
    return exported


def main():
    parser = argparse.ArgumentParser(description="Build a set-aware worker plan from due refresh rows")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--target-kind", choices=["sealed", "cards"], default="sealed")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--tier", default="")
    parser.add_argument("--set-name", default="")
    parser.add_argument("--all", action="store_true", help="Ignore due time and include all rows in refresh_priority")
    parser.add_argument("--out-dir", default="", help="Optionally export one CSV per worker for sealed runs")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)

    rows = load_due_rows(conn, args.target_kind, tier=args.tier, set_name=args.set_name, limit=args.limit)
    if args.all:
        params = [args.target_kind]
        query = """
            SELECT target_id, COALESCE(set_name, '(unknown)') AS set_name, activity_score, priority_tier
            FROM refresh_priority
            WHERE target_kind = ?
        """
        if args.tier:
            query += " AND priority_tier = ?"
            params.append(args.tier)
        if args.set_name:
            query += " AND set_name = ?"
            params.append(args.set_name)
        query += " ORDER BY activity_score DESC, set_name ASC, target_id ASC"
        if args.limit and args.limit > 0:
            query += f" LIMIT {int(args.limit)}"
        rows = conn.execute(query, params).fetchall()

    plan = build_worker_plan(rows, max(1, int(args.workers)))
    exported = export_worker_csvs(conn, plan, args.out_dir, target_kind=args.target_kind) if args.out_dir else []
    conn.close()

    print(
        json.dumps(
            {
                "target_kind": args.target_kind,
                "item_count": len(rows),
                "exported_csvs": exported,
                "workers": plan,
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
