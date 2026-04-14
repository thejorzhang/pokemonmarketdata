"""Create the market tracking database schema.

The default backend is still SQLite, but the schema bootstrap is now written
with backend-aware helpers so it can be moved toward Postgres later without
rewriting every call site.
"""

import argparse

from collection_manager import ensure_collection_schema
from db import (
    configure_connection,
    connect_database,
    get_dialect,
    id_column_sql,
    resolve_database_target,
    table_columns,
)


def create_schema(conn):
    dialect = get_dialect(conn)
    pk = id_column_sql(dialect)

    c = conn.cursor()

    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS products (
            {pk},
            name TEXT NOT NULL,
            url TEXT,
            release_date TEXT,
            sku_code TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT,
            catalog_active INTEGER NOT NULL DEFAULT 1,
            catalog_scrape_date TEXT,
            last_sales_refresh_at TEXT,
            sales_backfill_completed_at TEXT
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS listings (
            {pk},
            product_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            snapshot_date TEXT,
            listing_count INTEGER,
            lowest_price REAL,
            lowest_shipping REAL,
            lowest_total_price REAL,
            median_price REAL,
            market_price REAL,
            current_quantity INTEGER,
            current_sellers INTEGER,
            set_name TEXT,
            condition TEXT,
            source TEXT,
            run_id INTEGER,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS scrape_runs (
            {pk},
            source TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,
            csv_path TEXT,
            args_json TEXT,
            attempted_count INTEGER DEFAULT 0,
            processed_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            parse_failed_count INTEGER DEFAULT 0
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS scrape_failures (
            {pk},
            run_id INTEGER NOT NULL,
            product_name TEXT,
            url TEXT,
            stage TEXT NOT NULL,
            reason TEXT NOT NULL,
            http_status INTEGER,
            attempts INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES scrape_runs (id)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            {pk},
            product_id INTEGER NOT NULL,
            sale_date TEXT NOT NULL,
            condition_raw TEXT,
            variant TEXT,
            language TEXT,
            quantity INTEGER,
            purchase_price REAL,
            shipping_price REAL,
            listing_type TEXT,
            title TEXT,
            custom_listing_key TEXT,
            custom_listing_id TEXT,
            source TEXT NOT NULL,
            sale_fingerprint TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        """.format(pk=pk)
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS product_details (
            product_id INTEGER PRIMARY KEY,
            set_id INTEGER,
            tcgplayer_product_id INTEGER,
            source_url TEXT,
            url_slug TEXT,
            raw_title TEXT,
            set_name TEXT,
            product_line TEXT,
            product_type TEXT,
            product_subtype TEXT,
            release_date TEXT,
            source TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (id),
            FOREIGN KEY (set_id) REFERENCES sets (id)
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS sets (
            {pk},
            name TEXT NOT NULL,
            category_slug TEXT,
            product_line TEXT,
            source TEXT NOT NULL,
            set_type TEXT,
            release_date TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS scrape_activity (
            {pk},
            target_kind TEXT NOT NULL,
            target_id INTEGER NOT NULL,
            set_id INTEGER,
            priority_tier TEXT NOT NULL,
            priority_score REAL NOT NULL,
            recent_sales_30d INTEGER DEFAULT 0,
            last_sale_at TEXT,
            last_snapshot_at TEXT,
            next_due_at TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (set_id) REFERENCES sets (id)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS card_sales (
            {pk},
            card_product_id INTEGER NOT NULL,
            sale_date TEXT NOT NULL,
            condition_raw TEXT,
            variant TEXT,
            language TEXT,
            quantity INTEGER,
            purchase_price REAL,
            shipping_price REAL,
            listing_type TEXT,
            title TEXT,
            custom_listing_key TEXT,
            custom_listing_id TEXT,
            source TEXT NOT NULL,
            sale_fingerprint TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (card_product_id) REFERENCES card_products (id)
        )
        """.format(pk=pk)
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS price_history (
            {pk},
            product_id INTEGER NOT NULL,
            endpoint_kind TEXT NOT NULL,
            history_range TEXT NOT NULL,
            bucket_index INTEGER,
            bucket_start_date TEXT NOT NULL,
            bucket_end_date TEXT,
            bucket_label TEXT,
            market_price REAL,
            quantity_sold INTEGER,
            transaction_count INTEGER,
            low_sale_price REAL,
            low_sale_price_with_shipping REAL,
            high_sale_price REAL,
            high_sale_price_with_shipping REAL,
            avg_sale_price REAL,
            avg_sale_price_with_shipping REAL,
            total_sale_value REAL,
            source TEXT NOT NULL,
            history_fingerprint TEXT NOT NULL,
            bucket_json TEXT,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS card_price_history (
            {pk},
            card_product_id INTEGER NOT NULL,
            endpoint_kind TEXT NOT NULL,
            history_range TEXT NOT NULL,
            bucket_index INTEGER,
            bucket_start_date TEXT NOT NULL,
            bucket_end_date TEXT,
            bucket_label TEXT,
            market_price REAL,
            quantity_sold INTEGER,
            transaction_count INTEGER,
            low_sale_price REAL,
            low_sale_price_with_shipping REAL,
            high_sale_price REAL,
            high_sale_price_with_shipping REAL,
            avg_sale_price REAL,
            avg_sale_price_with_shipping REAL,
            total_sale_value REAL,
            source TEXT NOT NULL,
            history_fingerprint TEXT NOT NULL,
            bucket_json TEXT,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (card_product_id) REFERENCES card_products (id)
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS card_products (
            {pk},
            set_id INTEGER,
            tcgplayer_product_id INTEGER,
            name TEXT NOT NULL,
            url TEXT,
            category_slug TEXT,
            product_line TEXT,
            set_name TEXT,
            source TEXT NOT NULL,
            discovered_at TEXT NOT NULL,
            first_seen_at TEXT,
            last_seen_at TEXT,
            catalog_active INTEGER NOT NULL DEFAULT 1,
            catalog_scrape_date TEXT,
            last_sales_refresh_at TEXT,
            sales_backfill_completed_at TEXT,
            FOREIGN KEY (set_id) REFERENCES sets (id)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS card_details (
            card_product_id INTEGER PRIMARY KEY,
            tcgplayer_product_id INTEGER,
            source_url TEXT,
            raw_title TEXT,
            set_name TEXT,
            card_number TEXT,
            rarity TEXT,
            finish TEXT,
            language TEXT,
            supertype TEXT,
            subtype TEXT,
            release_date TEXT,
            source TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            FOREIGN KEY (card_product_id) REFERENCES card_products (id)
        )
        """
    )
    c.execute(
        f"""
        CREATE TABLE IF NOT EXISTS refresh_priority (
            {pk},
            target_kind TEXT NOT NULL,
            target_id INTEGER NOT NULL,
            set_id INTEGER,
            set_name TEXT,
            activity_score REAL NOT NULL DEFAULT 0,
            priority_tier TEXT NOT NULL DEFAULT 'dormant',
            refresh_interval_hours INTEGER NOT NULL DEFAULT 168,
            sales_7d INTEGER NOT NULL DEFAULT 0,
            sales_30d INTEGER NOT NULL DEFAULT 0,
            last_sale_at TEXT,
            last_snapshot_at TEXT,
            next_refresh_at TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS set_stats (
            set_id INTEGER PRIMARY KEY,
            set_name TEXT NOT NULL,
            category_slug TEXT,
            product_line TEXT,
            set_type TEXT,
            release_date TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT,
            sealed_product_count INTEGER NOT NULL DEFAULT 0,
            sealed_detail_count INTEGER NOT NULL DEFAULT 0,
            sealed_listing_count INTEGER NOT NULL DEFAULT 0,
            sealed_sale_count INTEGER NOT NULL DEFAULT 0,
            sealed_products_with_sales INTEGER NOT NULL DEFAULT 0,
            sealed_last_listing_at TEXT,
            sealed_last_sale_at TEXT,
            card_product_count INTEGER NOT NULL DEFAULT 0,
            card_detail_count INTEGER NOT NULL DEFAULT 0,
            card_sale_count INTEGER NOT NULL DEFAULT 0,
            card_products_with_sales INTEGER NOT NULL DEFAULT 0,
            card_last_sale_at TEXT,
            priority_target_count INTEGER NOT NULL DEFAULT 0,
            priority_hot_count INTEGER NOT NULL DEFAULT 0,
            priority_warm_count INTEGER NOT NULL DEFAULT 0,
            priority_cold_count INTEGER NOT NULL DEFAULT 0,
            priority_dormant_count INTEGER NOT NULL DEFAULT 0,
            priority_avg_score REAL,
            priority_max_score REAL,
            total_product_count INTEGER NOT NULL DEFAULT 0,
            total_detail_count INTEGER NOT NULL DEFAULT 0,
            total_sale_count INTEGER NOT NULL DEFAULT 0,
            total_products_with_sales INTEGER NOT NULL DEFAULT 0,
            total_last_sale_at TEXT,
            detail_coverage_pct REAL,
            sales_coverage_pct REAL,
            refreshed_at TEXT NOT NULL,
            summary_json TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS set_details (
            set_id INTEGER PRIMARY KEY,
            set_name TEXT NOT NULL,
            category_slug TEXT,
            product_line TEXT,
            set_type TEXT,
            release_date TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT,
            sealed_product_count INTEGER NOT NULL DEFAULT 0,
            card_product_count INTEGER NOT NULL DEFAULT 0,
            total_product_count INTEGER NOT NULL DEFAULT 0,
            total_sale_count INTEGER NOT NULL DEFAULT 0,
            detail_coverage_pct REAL,
            sales_coverage_pct REAL,
            sealed_last_listing_at TEXT,
            total_last_sale_at TEXT,
            refreshed_at TEXT NOT NULL,
            summary_json TEXT
        )
        """
    )

    # Backfill columns when the schema is opened against an older SQLite DB.
    cols = table_columns(conn, "listings")
    if "snapshot_date" not in cols:
        c.execute("ALTER TABLE listings ADD COLUMN snapshot_date TEXT")
        c.execute("UPDATE listings SET snapshot_date = substr(timestamp, 1, 10) WHERE snapshot_date IS NULL")
    if "run_id" not in cols:
        c.execute("ALTER TABLE listings ADD COLUMN run_id INTEGER")
    if "lowest_shipping" not in cols:
        c.execute("ALTER TABLE listings ADD COLUMN lowest_shipping REAL")
    if "lowest_total_price" not in cols:
        c.execute("ALTER TABLE listings ADD COLUMN lowest_total_price REAL")

    sales_cols = table_columns(conn, "sales")
    if "shipping_price" not in sales_cols:
        c.execute("ALTER TABLE sales ADD COLUMN shipping_price REAL")
    card_sales_cols = table_columns(conn, "card_sales")
    if "shipping_price" not in card_sales_cols:
        c.execute("ALTER TABLE card_sales ADD COLUMN shipping_price REAL")
    product_details_cols = table_columns(conn, "product_details")
    if "set_id" not in product_details_cols:
        try:
            c.execute("ALTER TABLE product_details ADD COLUMN set_id INTEGER")
        except Exception as exc:
            if "duplicate column name" not in str(exc).lower():
                raise
    card_product_cols = table_columns(conn, "card_products")
    if "set_id" not in card_product_cols:
        try:
            c.execute("ALTER TABLE card_products ADD COLUMN set_id INTEGER")
        except Exception as exc:
            if "duplicate column name" not in str(exc).lower():
                raise
    product_cols = table_columns(conn, "products")
    if "last_sales_refresh_at" not in product_cols:
        c.execute("ALTER TABLE products ADD COLUMN last_sales_refresh_at TEXT")
    if "sales_backfill_completed_at" not in product_cols:
        c.execute("ALTER TABLE products ADD COLUMN sales_backfill_completed_at TEXT")
    if "first_seen_at" not in product_cols:
        c.execute("ALTER TABLE products ADD COLUMN first_seen_at TEXT")
    if "last_seen_at" not in product_cols:
        c.execute("ALTER TABLE products ADD COLUMN last_seen_at TEXT")
    if "catalog_active" not in product_cols:
        c.execute("ALTER TABLE products ADD COLUMN catalog_active INTEGER NOT NULL DEFAULT 1")
    if "last_sales_refresh_at" not in card_product_cols:
        c.execute("ALTER TABLE card_products ADD COLUMN last_sales_refresh_at TEXT")
    if "sales_backfill_completed_at" not in card_product_cols:
        c.execute("ALTER TABLE card_products ADD COLUMN sales_backfill_completed_at TEXT")
    if "first_seen_at" not in card_product_cols:
        c.execute("ALTER TABLE card_products ADD COLUMN first_seen_at TEXT")
    if "last_seen_at" not in card_product_cols:
        c.execute("ALTER TABLE card_products ADD COLUMN last_seen_at TEXT")
    if "catalog_active" not in card_product_cols:
        c.execute("ALTER TABLE card_products ADD COLUMN catalog_active INTEGER NOT NULL DEFAULT 1")

    c.execute(
        """
        DELETE FROM listings
        WHERE id IN (
            SELECT older.id
            FROM listings AS older
            JOIN listings AS newer
              ON older.product_id = newer.product_id
             AND COALESCE(older.source, '') = COALESCE(newer.source, '')
             AND older.snapshot_date = newer.snapshot_date
             AND older.snapshot_date IS NOT NULL
             AND (
                 older.timestamp < newer.timestamp
                 OR (older.timestamp = newer.timestamp AND older.id < newer.id)
             )
        )
        """
    )

    # Helpful indexes for analytics and dedupe.
    c.execute("CREATE INDEX IF NOT EXISTS idx_products_url ON products (url)")
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_products_url_unique
        ON products (url)
        WHERE url IS NOT NULL AND url != ''
        """
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_listings_product_source_timestamp ON listings (product_id, source, timestamp)")
    c.execute("DROP INDEX IF EXISTS idx_listings_product_source_run")
    c.execute("CREATE INDEX IF NOT EXISTS idx_listings_product_source_snapshot_date ON listings (product_id, source, snapshot_date)")
    c.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_product_source_snapshot_unique
        ON listings (product_id, source, snapshot_date)
        WHERE snapshot_date IS NOT NULL
        """
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_listings_run_id ON listings (run_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_scrape_failures_run ON scrape_failures (run_id, stage, reason)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sales_product_sale_date ON sales (product_id, sale_date)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_product_fingerprint_unique ON sales (product_id, sale_fingerprint)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_sales_product_sale_date ON card_sales (card_product_id, sale_date)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_card_sales_product_fingerprint_unique ON card_sales (card_product_id, sale_fingerprint)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_price_history_product_range_date ON price_history (product_id, endpoint_kind, history_range, bucket_start_date)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_price_history_product_fingerprint_unique ON price_history (product_id, history_fingerprint)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_price_history_product_range_date ON card_price_history (card_product_id, endpoint_kind, history_range, bucket_start_date)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_card_price_history_product_fingerprint_unique ON card_price_history (card_product_id, history_fingerprint)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sets_name_product_line_unique ON sets (name, product_line)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sets_product_line_name ON sets (product_line, name)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_set_stats_set_id_unique ON set_stats (set_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_set_stats_sales ON set_stats (sealed_sale_count, card_sale_count)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_set_stats_priority ON set_stats (priority_hot_count, priority_warm_count)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_set_details_set_id_unique ON set_details (set_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_product_details_tcgplayer_product_id ON product_details (tcgplayer_product_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_product_details_set_id ON product_details (set_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_products_tcgplayer_product_id ON card_products (tcgplayer_product_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_products_set_id ON card_products (set_id)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_card_products_url_unique ON card_products (url) WHERE url IS NOT NULL AND url != ''")
    c.execute("CREATE INDEX IF NOT EXISTS idx_card_details_tcgplayer_product_id ON card_details (tcgplayer_product_id)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_refresh_priority_target_unique ON refresh_priority (target_kind, target_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_refresh_priority_due ON refresh_priority (target_kind, next_refresh_at, activity_score)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_refresh_priority_set ON refresh_priority (target_kind, set_id, priority_tier)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_scrape_activity_target_unique ON scrape_activity (target_kind, target_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_scrape_activity_set_due ON scrape_activity (set_id, next_due_at)")

    ensure_collection_schema(conn)
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Create or refresh the project database schema")
    parser.add_argument("--db", default=None, help="SQLite path or postgres:// DSN. Defaults to env DATABASE_URL/DB_PATH or sealed_market.db")
    args = parser.parse_args()

    target = resolve_database_target(args.db)
    conn = connect_database(target)
    try:
        configure_connection(conn)
        create_schema(conn)
    finally:
        conn.close()

    print(f"Database schema created successfully: {target}")


if __name__ == "__main__":
    raise SystemExit(main())
