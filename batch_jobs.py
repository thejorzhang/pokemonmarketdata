"""Fan out sealed-catalog jobs across batch workers.

This is a thin orchestration layer. It does not implement scraping itself;
instead it launches the existing worker scripts with shard arguments and, for
catalog discovery, merges the per-shard CSV outputs back into one file.
"""

import argparse
import csv
import math
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def log(message):
    print(message, flush=True)


def build_worker_command(job_type, base_args, shard_index, shard_count):
    scripts = {
        "scrape": "populate_db.py",
        "product_details": "product_details_refresh.py",
    }
    if job_type not in scripts:
        raise ValueError(f"unsupported_job_type:{job_type}")
    command = [sys.executable, scripts[job_type], *base_args]
    command.extend(["--shard-index", str(shard_index), "--shard-count", str(shard_count)])
    return command


def read_catalog_csv(path):
    products = []
    seen_urls = set()
    if not Path(path).exists():
        return products
    with open(path, "r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = (row.get("url") or "").strip()
            name = (row.get("name") or "").strip()
            if not url or url in seen_urls:
                continue
            products.append((name, url))
            seen_urls.add(url)
    return products


def merge_products(*product_lists):
    merged = []
    positions = {}
    for product_list in product_lists:
        for name, url in product_list:
            if not url:
                continue
            name = (name or "").strip() or "(unknown)"
            if url in positions:
                idx = positions[url]
                existing_name, existing_url = merged[idx]
                if existing_name == "(unknown)" and name != "(unknown)":
                    merged[idx] = (name, existing_url)
                continue
            positions[url] = len(merged)
            merged.append((name, url))
    return merged


def write_catalog_csv(path, products):
    path = Path(path)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["name", "url"])
        writer.writerows(products)


def stream_process_output(label, process):
    assert process.stdout is not None
    for line in process.stdout:
        print(f"[{label}] {line}", end="", flush=True)


def launch_worker(label, command):
    log(f"Launching {label}: {' '.join(command)}")
    process = subprocess.Popen(
        command,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    thread = threading.Thread(target=stream_process_output, args=(label, process), daemon=True)
    thread.start()
    return process, thread


def wait_for_workers(processes):
    exit_codes = []
    for process, _ in processes:
        exit_codes.append(process.wait())
    for _, thread in processes:
        thread.join(timeout=1.0)
    return exit_codes


def run_scrape(args):
    workers = max(1, int(args.workers))
    if workers == 1:
        command = [
            sys.executable,
            "populate_db.py",
            "--db",
            args.db,
            "--csv",
            args.csv,
            "--source",
            args.source,
            "--snapshot-date",
            args.snapshot_date,
            "--limit",
            str(int(args.limit)),
            "--commit-every",
            str(int(args.commit_every)),
            "--delay-min",
            str(args.delay_min),
            "--delay-max",
            str(args.delay_max),
            "--shard-index",
            "0",
            "--shard-count",
            "1",
        ]
        if args.no_selenium:
            command.append("--no-selenium")
        if args.headless:
            command.append("--headless")
        subprocess.run(command, cwd=str(ROOT), check=True)
        return 0

    per_worker_limit = int(math.ceil(args.limit / workers)) if int(args.limit) > 0 else 0
    processes = []
    for shard_index in range(workers):
        command = [
            sys.executable,
            "populate_db.py",
            "--db",
            args.db,
            "--csv",
            args.csv,
            "--source",
            args.source,
            "--snapshot-date",
            args.snapshot_date,
            "--limit",
            str(per_worker_limit),
            "--commit-every",
            str(int(args.commit_every)),
            "--delay-min",
            str(args.delay_min),
            "--delay-max",
            str(args.delay_max),
            "--shard-index",
            str(shard_index),
            "--shard-count",
            str(workers),
        ]
        if args.no_selenium:
            command.append("--no-selenium")
        if args.headless:
            command.append("--headless")
        processes.append(launch_worker(f"scrape:{shard_index + 1}/{workers}", command))
    exit_codes = wait_for_workers(processes)
    if any(code != 0 for code in exit_codes):
        raise SystemExit(max(exit_codes))
    return 0


def run_product_details(args):
    workers = max(1, int(args.workers))
    if workers == 1:
        command = [
            sys.executable,
            "product_details_refresh.py",
            "--db",
            args.db,
            "--source",
            args.source,
            "--limit",
            str(int(args.limit)),
            "--delay-min",
            str(args.delay_min),
            "--delay-max",
            str(args.delay_max),
            "--shard-index",
            "0",
            "--shard-count",
            "1",
        ]
        if not args.selenium:
            command.append("--no-selenium")
        if args.headless:
            command.append("--headless")
        subprocess.run(command, cwd=str(ROOT), check=True)
        return 0

    per_worker_limit = int(math.ceil(args.limit / workers)) if int(args.limit) > 0 else 0
    processes = []
    for shard_index in range(workers):
        command = [
            sys.executable,
            "product_details_refresh.py",
            "--db",
            args.db,
            "--source",
            args.source,
            "--limit",
            str(per_worker_limit),
            "--delay-min",
            str(args.delay_min),
            "--delay-max",
            str(args.delay_max),
            "--shard-index",
            str(shard_index),
            "--shard-count",
            str(workers),
        ]
        if not args.selenium:
            command.append("--no-selenium")
        if args.headless:
            command.append("--headless")
        processes.append(launch_worker(f"details:{shard_index + 1}/{workers}", command))
    exit_codes = wait_for_workers(processes)
    if any(code != 0 for code in exit_codes):
        raise SystemExit(max(exit_codes))
    return 0


def run_catalog(args):
    workers = max(1, int(args.workers))
    mode = args.mode
    output_path = Path(args.out)

    if workers == 1:
        command = [
            sys.executable,
            "link_scraper.py",
            "--out",
            args.out,
            "--mode",
            mode,
            "--category-slug",
            args.category_slug,
            "--product-line-name",
            args.product_line_name,
            "--product-type-name",
            args.product_type_name,
            "--wait-time",
            str(int(args.wait_time)),
            "--page-load-timeout",
            str(int(args.page_load_timeout)),
            "--retries",
            str(int(args.retries)),
        ]
        if args.all_pages:
            command.append("--all")
        else:
            command.extend(["--pages", str(int(args.pages))])
        if args.headless:
            command.append("--headless")
        subprocess.run(command, cwd=str(ROOT), check=True)
        return 0

    existing_products = read_catalog_csv(output_path)
    tempdir = tempfile.TemporaryDirectory(prefix="tcgplayer_catalog_")
    shard_paths = []
    processes = []

    worker_mode = "fresh"
    for shard_index in range(workers):
        shard_out = Path(tempdir.name) / f"catalog_shard_{shard_index}.csv"
        shard_paths.append(shard_out)
        command = [
            sys.executable,
            "link_scraper.py",
            "--out",
            str(shard_out),
            "--mode",
            worker_mode,
            "--category-slug",
            args.category_slug,
            "--product-line-name",
            args.product_line_name,
            "--product-type-name",
            args.product_type_name,
            "--wait-time",
            str(int(args.wait_time)),
            "--page-load-timeout",
            str(int(args.page_load_timeout)),
            "--retries",
            str(int(args.retries)),
            "--shard-index",
            str(shard_index),
            "--shard-count",
            str(workers),
        ]
        if args.all_pages:
            command.append("--all")
        else:
            command.extend(["--pages", str(int(args.pages))])
        if args.headless:
            command.append("--headless")
        processes.append(launch_worker(f"catalog:{shard_index + 1}/{workers}", command))

    exit_codes = wait_for_workers(processes)
    if any(code != 0 for code in exit_codes):
        tempdir.cleanup()
        raise SystemExit(max(exit_codes))

    live_products = merge_products(*(read_catalog_csv(path) for path in shard_paths))
    if mode == "newest":
        final_products = merge_products(existing_products, live_products)
        log(f"Newest refresh merged {len(live_products)} live product(s) into {len(existing_products)} existing product(s).")
    elif mode == "reconcile":
        existing_urls = {url for _, url in existing_products}
        live_urls = {url for _, url in live_products}
        added = [product for product in live_products if product[1] not in existing_urls]
        removed = [product for product in existing_products if product[1] not in live_urls]
        log(f"Reconcile summary: {len(added)} added, {len(removed)} removed, {len(live_products)} current live products")
        final_products = live_products
    else:
        final_products = live_products

    write_catalog_csv(output_path, final_products)
    log(f"Saved {len(final_products)} products to {output_path}")
    tempdir.cleanup()
    return 0


def main():
    parser = argparse.ArgumentParser(description="Fan out sealed-catalog jobs across workers")
    subparsers = parser.add_subparsers(dest="job_type", required=True)

    scrape = subparsers.add_parser("scrape")
    scrape.add_argument("--db", default="sealed_market.db")
    scrape.add_argument("--csv", default="products.csv")
    scrape.add_argument("--source", default="TCGplayer")
    scrape.add_argument("--snapshot-date", default="")
    scrape.add_argument("--limit", type=int, default=0)
    scrape.add_argument("--commit-every", type=int, default=25)
    scrape.add_argument("--delay-min", type=float, default=2.0)
    scrape.add_argument("--delay-max", type=float, default=5.0)
    scrape.add_argument("--no-selenium", action="store_true")
    scrape.add_argument("--headless", action="store_true")
    scrape.add_argument("--workers", type=int, default=1)

    catalog = subparsers.add_parser("catalog")
    catalog.add_argument("--out", default="products.csv")
    catalog.add_argument("--mode", choices=["fresh", "newest", "reconcile"], default="fresh")
    catalog.add_argument("--pages", type=int, default=3)
    catalog.add_argument("--all-pages", action="store_true")
    catalog.add_argument("--category-slug", default="pokemon")
    catalog.add_argument("--product-line-name", default="pokemon")
    catalog.add_argument("--product-type-name", default="Sealed Products")
    catalog.add_argument("--wait-time", type=int, default=20)
    catalog.add_argument("--page-load-timeout", type=int, default=25)
    catalog.add_argument("--retries", type=int, default=1)
    catalog.add_argument("--headless", action="store_true")
    catalog.add_argument("--workers", type=int, default=1)

    details = subparsers.add_parser("product-details")
    details.add_argument("--db", default="sealed_market.db")
    details.add_argument("--source", default="TCGplayer Product Details")
    details.add_argument("--limit", type=int, default=0)
    details.add_argument("--delay-min", type=float, default=0.5)
    details.add_argument("--delay-max", type=float, default=1.5)
    details.add_argument("--no-selenium", action="store_true")
    details.add_argument("--headless", action="store_true")
    details.add_argument("--workers", type=int, default=1)

    args = parser.parse_args()

    if args.job_type == "scrape":
        return run_scrape(args)
    if args.job_type == "catalog":
        return run_catalog(args)
    if args.job_type == "product-details":
        return run_product_details(args)
    raise SystemExit(f"Unsupported job type: {args.job_type}")


if __name__ == "__main__":
    raise SystemExit(main())
