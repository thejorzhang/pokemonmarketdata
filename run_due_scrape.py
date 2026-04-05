"""Run a due-only sealed scrape from the refresh-priority planner."""

import argparse
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

from build_set_plan import export_worker_csvs
from db import configure_connection, connect_database, resolve_database_target
from plan_refresh import build_worker_plan, load_due_rows
from populate_db import ensure_runtime_schema
from refresh_priority import refresh_priority
from refresh_sets import refresh_sets


ROOT = Path(__file__).resolve().parent


def run_commands(commands):
    processes = []
    for command in commands:
        print(f"$ {shlex.join(command)}", flush=True)
        processes.append(
            subprocess.Popen(
                command,
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        )
    for process in processes:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
    returncodes = [process.wait() for process in processes]
    return max(returncodes) if any(code != 0 for code in returncodes) else 0


def main():
    parser = argparse.ArgumentParser(description="Run due-only sealed scrape workers from refresh-priority planning")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--source", default="TCGplayer")
    parser.add_argument("--snapshot-date", default="")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--tier", default="")
    parser.add_argument("--set-name", default="")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--no-selenium", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    refresh_sets(conn)
    refresh_priority(conn)
    rows = load_due_rows(conn, "sealed", tier=args.tier, set_name=args.set_name, limit=args.limit)
    plan = build_worker_plan(rows, max(1, int(args.workers)))

    with tempfile.TemporaryDirectory(prefix="due_scrape_") as tempdir:
        csv_paths = export_worker_csvs(conn, plan, tempdir, target_kind="sealed")
        conn.close()

        commands = []
        for index, csv_path in enumerate(csv_paths, start=1):
            command = [
                sys.executable,
                "populate_db.py",
                "--db",
                args.db,
                "--csv",
                csv_path,
                "--source",
                args.source,
                "--snapshot-date",
                args.snapshot_date,
                "--limit",
                "0",
            ]
            if args.no_selenium:
                command.append("--no-selenium")
            if args.headless:
                command.append("--headless")
            commands.append(command)

        if args.dry_run:
            for command in commands:
                print(shlex.join(command), flush=True)
            return 0

        return run_commands(commands)


if __name__ == "__main__":
    raise SystemExit(main())
