import sqlite3
import unittest

from populate_db import ensure_runtime_schema
from refresh_sets import discover_sets


class TestDiscoverSets(unittest.TestCase):
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
                listing_count INTEGER,
                lowest_price REAL,
                median_price REAL,
                market_price REAL,
                current_quantity INTEGER,
                current_sellers INTEGER,
                set_name TEXT,
                condition TEXT,
                source TEXT
            )
            """
        )
        ensure_runtime_schema(conn)
        return conn

    def test_discover_sets_materializes_sets_without_linking(self):
        conn = self.make_conn()
        conn.execute(
            """
            INSERT INTO product_details (
                product_id, tcgplayer_product_id, source_url, url_slug, raw_title, set_name,
                product_line, product_type, product_subtype, release_date, source, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                111,
                "https://www.tcgplayer.com/product/111/test-sealed",
                "test-sealed",
                "Elite Trainer Box",
                "Scarlet & Violet",
                "pokemon",
                "elite_trainer_box",
                None,
                "2023-03-31",
                "TCGplayer Product Details",
                "2026-04-02T00:00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO card_products (
                tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                222,
                "Charizard ex",
                "https://www.tcgplayer.com/product/222/test-card",
                "pokemon",
                "pokemon",
                "Obsidian Flames",
                "TCGplayer Cards",
                "2026-04-02T00:00:00",
            ),
        )
        conn.commit()

        result = discover_sets(conn)
        self.assertEqual(result["sets_seen"], 2)
        self.assertEqual(result["new_sets"], 2)

        sets = conn.execute("SELECT name, set_type FROM sets ORDER BY name").fetchall()
        self.assertEqual(sets, [("Obsidian Flames", "cards"), ("Scarlet & Violet", "sealed")])


if __name__ == "__main__":
    unittest.main()
