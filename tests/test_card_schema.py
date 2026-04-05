import sqlite3
import unittest

from populate_db import ensure_runtime_schema


class TestCardSchema(unittest.TestCase):
    def test_card_tables_exist(self):
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
        self.assertIn("card_products", table_names)
        self.assertIn("card_details", table_names)

        card_product_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(card_products)").fetchall()
        }
        self.assertIn("tcgplayer_product_id", card_product_columns)
        self.assertIn("category_slug", card_product_columns)

        card_detail_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(card_details)").fetchall()
        }
        self.assertIn("card_number", card_detail_columns)
        self.assertIn("finish", card_detail_columns)


if __name__ == "__main__":
    unittest.main()
