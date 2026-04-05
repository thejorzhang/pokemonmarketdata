"""Launch batch worker shards for scraping, catalog, and enrichment jobs."""

import argparse
import csv
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
import tempfile
from pathlib import Path

from db import is_sqlite_target, resolve_database_target

ROOT = Path(__file__).resolve().parent


def read_catalog_csv(path):
    products = []
    seen_urls = set()
    path = Path(path)
    if not path.exists():
        return products
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = (row.get("url") or "").strip()
            name = (row.get("name") or "").strip()
            if not url or url in seen_urls:
                continue
            products.append((name, url))
            seen_urls.add(url)
    return products


def write_catalog_csv(path, products):
    path = Path(path)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["name", "url"])
        writer.writerows(products)


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


def build_scrape_worker_command(args, shard_index, shard_count):
    command = [
        "python3",
        "populate_db.py",
        "--db",
        args.db,
        "--source",
        args.source,
        "--snapshot-date",
        args.snapshot_date,
        "--limit",
        "0",
        "--commit-every",
        str(args.commit_every),
        "--delay-min",
        str(args.delay_min),
        "--delay-max",
        str(args.delay_max),
        "--request-timeout",
        str(args.request_timeout),
        "--max-retries",
        str(args.max_retries),
        "--retry-backoff",
        str(args.retry_backoff),
        "--shard-index",
        str(shard_index),
        "--shard-count",
        str(shard_count),
    ]
    if getattr(args, "csv", ""):
        command.extend(["--csv", args.csv])
    if getattr(args, "set_id", 0):
        command.extend(["--set-id", str(int(args.set_id))])
    elif getattr(args, "set_name", ""):
        command.extend(["--set-name", args.set_name])
    if args.diagnostics_dir:
        command.extend(["--diagnostics-dir", args.diagnostics_dir])
    if args.no_selenium:
        command.append("--no-selenium")
    if args.headless:
        command.append("--headless")
    if args.debug:
        command.append("--debug")
    return command


def build_product_details_worker_command(args, shard_index, shard_count):
    command = [
        "python3",
        "product_details_refresh.py",
        "--db",
        args.db,
        "--source",
        args.source,
        "--request-timeout",
        str(args.request_timeout),
        "--max-retries",
        str(args.max_retries),
        "--retry-backoff",
        str(args.retry_backoff),
        "--delay-min",
        str(args.delay_min),
        "--delay-max",
        str(args.delay_max),
        "--shard-index",
        str(shard_index),
        "--shard-count",
        str(shard_count),
    ]
    if getattr(args, "set_id", 0):
        command.extend(["--set-id", str(int(args.set_id))])
    elif getattr(args, "set_name", ""):
        command.extend(["--set-name", args.set_name])
    if args.no_selenium:
        command.append("--no-selenium")
    if args.headless:
        command.append("--headless")
    return command


def build_card_details_worker_command(args, shard_index, shard_count):
    command = [
        "python3",
        "card_details_refresh.py",
        "--db",
        args.db,
        "--source",
        args.source,
        "--request-timeout",
        str(args.request_timeout),
        "--max-retries",
        str(args.max_retries),
        "--retry-backoff",
        str(args.retry_backoff),
        "--delay-min",
        str(args.delay_min),
        "--delay-max",
        str(args.delay_max),
        "--shard-index",
        str(shard_index),
        "--shard-count",
        str(shard_count),
    ]
    if getattr(args, "set_id", 0):
        command.extend(["--set-id", str(int(args.set_id))])
    elif getattr(args, "set_name", ""):
        command.extend(["--set-name", args.set_name])
    if args.no_selenium:
        command.append("--no-selenium")
    if args.headless:
        command.append("--headless")
    return command


def build_catalog_worker_command(args, shard_index, shard_count, output_path):
    command = [
        "python3",
        "link_scraper.py",
        "--out",
        str(output_path),
        "--mode",
        "fresh",
        "--category-slug",
        args.category_slug,
        "--product-line-name",
        args.product_line_name,
        "--product-type-name",
        args.product_type_name,
        "--wait-time",
        str(args.wait_time),
        "--page-load-timeout",
        str(args.page_load_timeout),
        "--retries",
        str(args.retries),
        "--shard-index",
        str(shard_index),
        "--shard-count",
        str(shard_count),
    ]
    if args.all:
        command.append("--all")
    else:
        command.extend(["--pages", str(args.pages)])
    if args.headless:
        command.append("--headless")
    return command


def build_sales_worker_command(args, shard_index, shard_count):
    command = [
        "python3",
        "sales_ingester.py",
        "--db",
        args.db,
        "--source",
        args.source,
        "--target-kind",
        getattr(args, "target_kind", "sealed"),
        "--limit",
        str(args.limit),
        "--commit-every",
        str(args.commit_every),
        "--shard-index",
        str(shard_index),
        "--shard-count",
        str(shard_count),
    ]
    if getattr(args, "set_id", 0):
        command.extend(["--set-id", str(int(args.set_id))])
    elif getattr(args, "set_name", ""):
        command.extend(["--set-name", args.set_name])
    if args.product_id:
        command.extend(["--product-id", str(args.product_id)])
    if args.product_url:
        command.extend(["--product-url", args.product_url])
    if args.snapshot_file:
        command.extend(["--snapshot-file", args.snapshot_file])
    if args.all_dates:
        command.append("--all-dates")
    elif args.sale_date:
        command.extend(["--sale-date", args.sale_date])
    if args.no_browser_fallback:
        command.append("--no-browser-fallback")
    if args.headless:
        command.append("--headless")
    return command


def plan_worker_commands(task, args, workers):
    if workers < 1:
        raise ValueError("workers_must_be_positive")

    builder = {
        "scrape": build_scrape_worker_command,
        "product-details": build_product_details_worker_command,
        "card-details": build_card_details_worker_command,
        "sales": build_sales_worker_command,
    }.get(task)
    if not builder:
        raise ValueError(f"unsupported_task:{task}")

    return [
        builder(args, shard_index=index, shard_count=workers)
        for index in range(workers)
    ]


def run_catalog_batch(args):
    workers = max(1, int(args.workers))
    output_path = Path(args.out)

    if workers == 1:
        command = [
            "python3",
            "link_scraper.py",
            "--out",
            args.out,
            "--mode",
            args.mode,
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
        if args.headless:
            command.append("--headless")
        if args.dry_run:
            print(f"worker 1/1: {shlex.join(command)}")
            return 0
        return subprocess.call(command, cwd=str(ROOT))

    existing_products = read_catalog_csv(output_path)
    with tempfile.TemporaryDirectory(prefix="catalog_batch_") as tempdir:
        shard_outputs = []
        commands = []
        for index in range(workers):
            shard_path = Path(tempdir) / f"catalog_shard_{index}.csv"
            shard_outputs.append(shard_path)
            commands.append(build_catalog_worker_command(args, index, workers, shard_path))

        if args.dry_run:
            for index, command in enumerate(commands, start=1):
                print(f"worker {index}/{len(commands)}: {shlex.join(command)}")
            return 0

        exit_code = run_worker_group(commands)
        if exit_code != 0:
            return exit_code

        live_products = merge_products(*(read_catalog_csv(path) for path in shard_outputs))
        if args.mode == "newest":
            final_products = merge_products(existing_products, live_products)
            print(
                f"[batch] newest merged {len(live_products)} live product(s) into {len(existing_products)} existing product(s)",
                flush=True,
            )
        elif args.mode == "reconcile":
            existing_urls = {url for _, url in existing_products}
            live_urls = {url for _, url in live_products}
            added = [product for product in live_products if product[1] not in existing_urls]
            removed = [product for product in existing_products if product[1] not in live_urls]
            print(
                f"[batch] reconcile summary: {len(added)} added, {len(removed)} removed, {len(live_products)} current live products",
                flush=True,
            )
            final_products = live_products
        else:
            final_products = live_products

        write_catalog_csv(output_path, final_products)
        print(f"[batch] saved {len(final_products)} products to {output_path}", flush=True)
        return 0


def _prefix_output(label, process):
    assert process.stdout is not None
    for line in process.stdout:
        print(f"[{label}] {line}", end="", flush=True)


def _terminate_processes(processes):
    for process in processes:
        if process.poll() is None:
            try:
                process.terminate()
            except Exception:
                pass
    time.sleep(1)
    for process in processes:
        if process.poll() is None:
            try:
                process.kill()
            except Exception:
                pass


def run_worker_group(commands):
    processes = []
    threads = []
    stop_requested = threading.Event()

    def handle_signal(signum, frame):
        if stop_requested.is_set():
            return
        stop_requested.set()
        print("[batch] Stop requested. Terminating workers...", flush=True)
        _terminate_processes(processes)

    previous_int = signal.signal(signal.SIGINT, handle_signal)
    previous_term = signal.signal(signal.SIGTERM, handle_signal)
    try:
        for index, command in enumerate(commands, start=1):
            label = f"worker {index}/{len(commands)}"
            print(f"[batch] starting {label}: {shlex.join(command)}", flush=True)
            process = subprocess.Popen(
                command,
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            processes.append(process)
            thread = threading.Thread(target=_prefix_output, args=(label, process), daemon=True)
            thread.start()
            threads.append(thread)

        returncodes = []
        for index, process in enumerate(processes, start=1):
            returncode = process.wait()
            returncodes.append(returncode)
            print(f"[batch] worker {index}/{len(processes)} exited {returncode}", flush=True)

        for thread in threads:
            thread.join(timeout=1.0)

        return max(returncodes) if any(code != 0 for code in returncodes) else 0
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)


def make_parser():
    parser = argparse.ArgumentParser(description="Run batch workers for TCGplayer scraping tasks")
    subparsers = parser.add_subparsers(dest="task", required=True)

    scrape = subparsers.add_parser("scrape", help="Run batched listing snapshots")
    scrape.add_argument("--db", default="sealed_market.db")
    scrape.add_argument("--csv", default="")
    scrape.add_argument("--source", default="TCGplayer")
    scrape.add_argument("--snapshot-date", default="")
    scrape.add_argument("--commit-every", type=int, default=25)
    scrape.add_argument("--delay-min", type=float, default=2.0)
    scrape.add_argument("--delay-max", type=float, default=5.0)
    scrape.add_argument("--request-timeout", type=float, default=12.0)
    scrape.add_argument("--max-retries", type=int, default=3)
    scrape.add_argument("--retry-backoff", type=float, default=1.25)
    scrape.add_argument("--diagnostics-dir", default="diagnostics")
    scrape.add_argument("--no-selenium", action="store_true")
    scrape.add_argument("--headless", action="store_true")
    scrape.add_argument("--debug", action="store_true")
    scrape.add_argument("--set-id", type=int, default=0)
    scrape.add_argument("--set-name", default="")
    scrape.add_argument("--workers", type=int, default=4)
    scrape.add_argument("--dry-run", action="store_true")

    details = subparsers.add_parser("product-details", help="Run batched product metadata enrichment")
    details.add_argument("--db", default="sealed_market.db")
    details.add_argument("--source", default="TCGplayer Product Details")
    details.add_argument("--delay-min", type=float, default=0.5)
    details.add_argument("--delay-max", type=float, default=1.5)
    details.add_argument("--request-timeout", type=float, default=12.0)
    details.add_argument("--max-retries", type=int, default=3)
    details.add_argument("--retry-backoff", type=float, default=1.25)
    details.add_argument("--no-selenium", action="store_true")
    details.add_argument("--headless", action="store_true")
    details.add_argument("--set-id", type=int, default=0)
    details.add_argument("--set-name", default="")
    details.add_argument("--workers", type=int, default=4)
    details.add_argument("--dry-run", action="store_true")

    card_details = subparsers.add_parser("card-details", help="Run batched card metadata enrichment")
    card_details.add_argument("--db", default="sealed_market.db")
    card_details.add_argument("--source", default="TCGplayer Card Details")
    card_details.add_argument("--delay-min", type=float, default=0.5)
    card_details.add_argument("--delay-max", type=float, default=1.5)
    card_details.add_argument("--request-timeout", type=float, default=12.0)
    card_details.add_argument("--max-retries", type=int, default=3)
    card_details.add_argument("--retry-backoff", type=float, default=1.25)
    card_details.add_argument("--no-selenium", action="store_true")
    card_details.add_argument("--headless", action="store_true")
    card_details.add_argument("--set-id", type=int, default=0)
    card_details.add_argument("--set-name", default="")
    card_details.add_argument("--workers", type=int, default=4)
    card_details.add_argument("--dry-run", action="store_true")

    catalog = subparsers.add_parser("catalog", help="Run batched sealed catalog refresh")
    catalog.add_argument("--out", default="products.csv")
    catalog.add_argument("--mode", choices=["fresh", "newest", "reconcile"], default="fresh")
    catalog.add_argument("--pages", type=int, default=3)
    catalog.add_argument("--all", action="store_true")
    catalog.add_argument("--wait-time", type=int, default=20)
    catalog.add_argument("--page-load-timeout", type=int, default=25)
    catalog.add_argument("--retries", type=int, default=1)
    catalog.add_argument("--category-slug", default="pokemon")
    catalog.add_argument("--product-line-name", default="pokemon")
    catalog.add_argument("--product-type-name", default="Sealed Products")
    catalog.add_argument("--headless", action="store_true")
    catalog.add_argument("--workers", type=int, default=4)
    catalog.add_argument("--dry-run", action="store_true")

    sales = subparsers.add_parser("sales", help="Run batched latest-sales ingestion")
    sales.add_argument("--db", default="sealed_market.db")
    sales.add_argument("--source", default="TCGplayer")
    sales.add_argument("--target-kind", choices=["sealed", "cards"], default="sealed")
    sales.add_argument("--set-id", type=int, default=0)
    sales.add_argument("--set-name", default="")
    sales.add_argument("--product-id", type=int, default=0)
    sales.add_argument("--product-url", default="")
    sales.add_argument("--sale-date", default="")
    sales.add_argument("--all-dates", action="store_true")
    sales.add_argument("--snapshot-file", default="")
    sales.add_argument("--limit", type=int, default=0)
    sales.add_argument("--commit-every", type=int, default=10)
    sales.add_argument("--no-browser-fallback", action="store_true")
    sales.add_argument("--headless", action="store_true")
    sales.add_argument("--workers", type=int, default=4)
    sales.add_argument("--dry-run", action="store_true")

    return parser


def main(argv=None):
    parser = make_parser()
    args = parser.parse_args(argv)

    if args.task == "catalog":
        return run_catalog_batch(args)

    if args.task == "scrape" and int(args.workers) > 1 and is_sqlite_target(resolve_database_target(args.db)):
        if int(args.commit_every) != 1:
            print(
                "[batch] SQLite detected for batched daily scrape; forcing --commit-every 1 to reduce database lock windows.",
                flush=True,
            )
            args.commit_every = 1

    commands = plan_worker_commands(args.task, args, args.workers)

    if args.dry_run:
        for index, command in enumerate(commands, start=1):
            print(f"worker {index}/{len(commands)}: {shlex.join(command)}")
        return 0

    return run_worker_group(commands)


if __name__ == "__main__":
    raise SystemExit(main())
