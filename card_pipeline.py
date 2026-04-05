"""Run the Pokemon cards expansion pipeline end to end.

This keeps the cards track moving as one operator action:
1. crawl card catalog links
2. load them into card_products
3. enrich missing card_details
"""

import argparse
import shlex
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run_step(command):
    print(f"$ {shlex.join(command)}", flush=True)
    result = subprocess.run(command, cwd=str(ROOT))
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Run the card expansion pipeline")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--csv", default="pokemon_cards.csv")
    parser.add_argument("--category-slug", default="pokemon")
    parser.add_argument("--product-line-name", default="pokemon")
    parser.add_argument("--product-type-name", default="Cards")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--mode", choices=["fresh", "newest", "reconcile"], default="fresh")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--pages", type=int, default=5)
    parser.add_argument("--details-source", default="TCGplayer Card Details")
    parser.add_argument("--catalog-source", default="TCGplayer Cards")
    args = parser.parse_args()

    catalog_command = [
        sys.executable,
        "batch_workers.py",
        "catalog",
        "--out",
        args.csv,
        "--mode",
        args.mode,
        "--category-slug",
        args.category_slug,
        "--product-line-name",
        args.product_line_name,
        "--product-type-name",
        args.product_type_name,
        "--workers",
        str(args.workers),
        "--wait-time",
        "20",
        "--page-load-timeout",
        "25",
        "--retries",
        "1",
    ]
    if args.all:
        catalog_command.append("--all")
    else:
        catalog_command.extend(["--pages", str(args.pages)])
    if args.headless:
        catalog_command.append("--headless")

    load_command = [
        sys.executable,
        "card_catalog_refresh.py",
        "--db",
        args.db,
        "--csv",
        args.csv,
        "--category-slug",
        args.category_slug,
        "--product-line-name",
        args.product_line_name,
        "--source",
        args.catalog_source,
    ]

    details_command = [
        sys.executable,
        "batch_workers.py",
        "card-details",
        "--db",
        args.db,
        "--source",
        args.details_source,
        "--workers",
        str(args.workers),
        "--delay-min",
        "0.5",
        "--delay-max",
        "1.5",
    ]
    if args.headless:
        details_command.append("--headless")

    run_step(catalog_command)
    run_step(load_command)
    run_step(details_command)
    print("Card pipeline completed successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
