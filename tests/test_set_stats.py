import json
import sqlite3
import unittest

from populate_db import ensure_runtime_schema
from build_set_stats import build_set_stats


class TestSetStats(unittest.TestCase):
    def make_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT,
                release_date TEXT,
                sku_code TEXT,
                last_sales_refresh_at TEXT,
                sales_backfill_completed_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                snapshot_date TEXT,
                listing_count INTEGER,
                lowest_price REAL,
                median_price REAL,
                market_price REAL,
                current_quantity INTEGER,
                current_sellers INTEGER,
                set_name TEXT,
                condition TEXT,
                source TEXT,
                run_id INTEGER,
                lowest_shipping REAL,
                lowest_total_price REAL
            )
            """
        )
        ensure_runtime_schema(conn)
        return conn

    def test_build_set_stats_materializes_set_row(self):
        conn = self.make_conn()
        conn.execute(
            "INSERT INTO sets (id, name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "Test Set", "pokemon", "pokemon", "src", "sealed", "2024-01-01", "now", "now"),
        )
        conn.execute(
            "INSERT INTO products (name, url) VALUES (?, ?)",
            ("Test Product", "https://www.tcgplayer.com/product/1/test"),
        )
        conn.execute(
            """
            INSERT INTO product_details (
                product_id, set_id, tcgplayer_product_id, source_url, url_slug, raw_title,
                set_name, product_line, product_type, product_subtype, release_date, source, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 1, 1, "u", "slug", "Test Product", "Test Set", "pokemon", "booster_pack", None, "2024-01-01", "src", "now"),
        )
        conn.execute(
            "INSERT INTO listings (product_id, timestamp, snapshot_date, listing_count, lowest_price, source) VALUES (?, ?, ?, ?, ?, ?)",
            (1, "2026-04-05T00:00:00", "2026-04-05", 18, 4.99, "TCGplayer"),
        )
        conn.execute(
            "INSERT INTO sales (product_id, sale_date, condition_raw, variant, language, quantity, purchase_price, shipping_price, listing_type, title, custom_listing_key, custom_listing_id, source, sale_fingerprint, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "2026-04-04", "Near Mint", None, "English", 1, 5.99, 0.99, "marketplace", "Test Product", "k", "1", "TCGplayer", "fp1", "now"),
        )
        conn.execute(
            "INSERT INTO card_products (set_id, tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, 2, "Card Product", "https://www.tcgplayer.com/product/2/test", "pokemon", "pokemon", "Test Set", "TCGplayer Cards", "now"),
        )
        conn.execute(
            "INSERT INTO card_sales (card_product_id, sale_date, condition_raw, variant, language, quantity, purchase_price, shipping_price, listing_type, title, custom_listing_key, custom_listing_id, source, sale_fingerprint, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "2026-04-04", "Near Mint", "Holofoil", "English", 1, 9.99, 0.99, "marketplace", "Card Product", "k2", "2", "TCGplayer Cards", "fp2", "now"),
        )
        conn.execute(
            "INSERT INTO refresh_priority (target_kind, target_id, set_id, set_name, activity_score, priority_tier, refresh_interval_hours, sales_7d, sales_30d, last_sale_at, last_snapshot_at, next_refresh_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("sealed", 1, 1, "Test Set", 50, "hot", 24, 1, 1, "2026-04-04", "2026-04-05", "2026-04-06", "now"),
        )
        conn.execute(
            "INSERT INTO refresh_priority (target_kind, target_id, set_id, set_name, activity_score, priority_tier, refresh_interval_hours, sales_7d, sales_30d, last_sale_at, last_snapshot_at, next_refresh_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("cards", 1, 1, "Test Set", 50, "warm", 72, 1, 1, "2026-04-04", None, "2026-04-08", "now"),
        )

        result = build_set_stats(conn)
        self.assertEqual(result["sets_written"], 1)

        row = conn.execute(
            """
            SELECT set_name, total_product_count, total_sale_count, priority_hot_count, priority_warm_count, summary_json
            FROM set_stats
            """
        ).fetchone()
        self.assertEqual(row[0], "Test Set")
        self.assertEqual(row[1], 2)
        self.assertEqual(row[2], 2)
        self.assertEqual(row[3], 1)
        self.assertEqual(row[4], 1)
        payload = json.loads(row[5])
        self.assertEqual(payload["set_name"], "Test Set")
        self.assertEqual(payload["total_product_count"], 2)


if __name__ == "__main__":
    unittest.main()
