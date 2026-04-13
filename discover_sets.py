"""Discover canonical sets from current product metadata.

This is the lightweight explicit set-discovery step. It updates the shared
`sets` table from currently known product metadata without doing any product
linking work.
"""

import argparse
import json

from db import configure_connection, connect_database, resolve_database_target
from populate_db import ensure_runtime_schema
from refresh_sets import discover_sets


def main():
    parser = argparse.ArgumentParser(description="Discover sets from current product metadata")
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--kind", choices=["all", "sealed", "cards"], default="all")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)

    include_sealed = args.kind in ("all", "sealed")
    include_cards = args.kind in ("all", "cards")
    result = discover_sets(conn, include_sealed=include_sealed, include_cards=include_cards)
    conn.close()

    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
