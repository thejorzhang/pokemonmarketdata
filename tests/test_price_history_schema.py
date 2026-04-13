import sqlite3
import unittest

from populate_db import ensure_runtime_schema


class TestPriceHistorySchema(unittest.TestCase):
    def test_price_history_tables_and_indexes_exist(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT,
                release_date TEXT,
                sku_code TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE card_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                set_id INTEGER,
                tcgplayer_product_id INTEGER,
                name TEXT NOT NULL,
                url TEXT,
                category_slug TEXT,
                product_line TEXT,
                set_name TEXT,
                source TEXT NOT NULL,
                discovered_at TEXT NOT NULL
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
                source TEXT,
                FOREIGN KEY (product_id) REFERENCES products (id)
            )
            """
        )

        ensure_runtime_schema(conn)

        table_names = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        self.assertIn("price_history", table_names)
        self.assertIn("card_price_history", table_names)

        history_columns = {row[1] for row in conn.execute("PRAGMA table_info(price_history)").fetchall()}
        self.assertIn("endpoint_kind", history_columns)
        self.assertIn("history_range", history_columns)
        self.assertIn("bucket_json", history_columns)

        card_history_columns = {row[1] for row in conn.execute("PRAGMA table_info(card_price_history)").fetchall()}
        self.assertIn("endpoint_kind", card_history_columns)
        self.assertIn("history_range", card_history_columns)
        self.assertIn("bucket_json", card_history_columns)

        history_index_names = {row[1] for row in conn.execute("PRAGMA index_list(price_history)").fetchall()}
        self.assertIn("idx_price_history_product_range_date", history_index_names)
        self.assertIn("idx_price_history_product_fingerprint_unique", history_index_names)

        card_history_index_names = {row[1] for row in conn.execute("PRAGMA index_list(card_price_history)").fetchall()}
        self.assertIn("idx_card_price_history_product_range_date", card_history_index_names)
        self.assertIn("idx_card_price_history_product_fingerprint_unique", card_history_index_names)


if __name__ == "__main__":
    unittest.main()
