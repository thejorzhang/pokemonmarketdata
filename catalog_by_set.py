"""Refresh catalog links by iterating known sets instead of one global search."""

import argparse
import csv
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

from batch_workers import merge_products, read_catalog_csv, write_catalog_csv
from db import configure_connection, connect_database, resolve_database_target
from populate_db import ensure_runtime_schema


ROOT = Path(__file__).resolve().parent


def load_sets(conn, set_type="", shard_index=0, shard_count=1, limit=0):
    rows = conn.execute(
        """
        SELECT id, name, product_line
        FROM sets
        WHERE (? = '' OR set_type = ?)
        ORDER BY name
        """,
        (set_type, set_type),
    ).fetchall()
    filtered = []
    for index, row in enumerate(rows):
        if shard_count > 1 and (index % shard_count) != shard_index:
            continue
        filtered.append(row)
    if limit and limit > 0:
        filtered = filtered[: int(limit)]
    return filtered


def run_set_catalog(args, set_name, temp_out):
    command = [
        sys.executable,
        "link_scraper.py",
        "--out",
        str(temp_out),
        "--mode",
        "fresh",
        "--category-slug",
        args.category_slug,
        "--product-line-name",
        args.product_line_name,
        "--product-type-name",
        args.product_type_name,
        "--query",
        set_name,
        "--wait-time",
        str(args.wait_time),
        "--page-load-timeout",
        str(args.page_load_timeout),
        "--retries",
        str(args.retries),
    ]
    if args.all:
        command.append("--all")
    else:
        command.extend(["--pages", str(args.pages)])
    if args.sort:
        command.extend(["--sort", args.sort])
    if args.headless:
        command.append("--headless")
    print(f"[set-catalog] {shlex.join(command)}", flush=True)
    return subprocess.call(command, cwd=str(ROOT))


def main():
    parser = argparse.ArgumentParser(description="Refresh catalog by iterating the known sets table")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--out", default="products_by_set.csv")
    parser.add_argument("--mode", choices=["fresh", "newest", "reconcile"], default="fresh")
    parser.add_argument("--set-type", choices=["", "sealed", "cards"], default="cards")
    parser.add_argument("--category-slug", default="pokemon")
    parser.add_argument("--product-line-name", default="pokemon")
    parser.add_argument("--product-type-name", default="Cards")
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--wait-time", type=int, default=20)
    parser.add_argument("--page-load-timeout", type=int, default=25)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--sort", default="")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--limit-sets", type=int, default=0)
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    set_rows = load_sets(
        conn,
        set_type=args.set_type,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        limit=args.limit_sets,
    )
    conn.close()

    existing_products = read_catalog_csv(args.out) if args.mode in {"newest", "reconcile"} else []
    with tempfile.TemporaryDirectory(prefix="catalog_sets_") as tempdir:
        merged = []
        for set_id, set_name, _product_line in set_rows:
            temp_out = Path(tempdir) / f"set_{set_id}.csv"
            returncode = run_set_catalog(args, set_name, temp_out)
            if returncode != 0:
                raise SystemExit(returncode)
            merged = merge_products(merged, read_catalog_csv(temp_out))

    if args.mode == "newest":
        final_products = merge_products(existing_products, merged)
    else:
        final_products = merged
    write_catalog_csv(args.out, final_products)
    print(
        {
            "sets_processed": len(set_rows),
            "products_written": len(final_products),
            "mode": args.mode,
            "set_type": args.set_type or "all",
        },
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
