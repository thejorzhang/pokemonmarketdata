import sqlite3
import unittest

from populate_db import ensure_runtime_schema


class TestSalesSchema(unittest.TestCase):
    def test_sales_table_and_indexes_exist(self):
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
        conn.execute(
            """
            CREATE TABLE sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                sale_date TEXT NOT NULL,
                condition_raw TEXT,
                variant TEXT,
                language TEXT,
                quantity INTEGER,
                purchase_price REAL,
                listing_type TEXT,
                title TEXT,
                custom_listing_key TEXT,
                custom_listing_id TEXT,
                source TEXT NOT NULL,
                sale_fingerprint TEXT NOT NULL,
                scraped_at TEXT NOT NULL,
                FOREIGN KEY (product_id) REFERENCES products (id)
            )
            """
        )

        ensure_runtime_schema(conn)

        table_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        self.assertIn("sales", table_names)

        index_names = {
            row[1] for row in conn.execute("PRAGMA index_list(sales)").fetchall()
        }
        self.assertIn("idx_sales_product_sale_date", index_names)
        self.assertIn("idx_sales_product_fingerprint_unique", index_names)

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(sales)").fetchall()
        }
        self.assertIn("shipping_price", columns)

        card_table_names = table_names
        self.assertIn("card_sales", card_table_names)
        card_index_names = {
            row[1] for row in conn.execute("PRAGMA index_list(card_sales)").fetchall()
        }
        self.assertIn("idx_card_sales_product_sale_date", card_index_names)
        self.assertIn("idx_card_sales_product_fingerprint_unique", card_index_names)


if __name__ == "__main__":
    unittest.main()
