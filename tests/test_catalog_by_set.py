import sqlite3
import unittest

from catalog_by_set import load_sets
from populate_db import ensure_runtime_schema


class TestCatalogBySet(unittest.TestCase):
    def make_conn(self):
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
        return conn

    def test_load_sets_filters_and_shards(self):
        conn = self.make_conn()
        conn.execute("INSERT INTO sets (name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("Set A", "pokemon", "pokemon", "src", "cards", "2023-01-01", "now", "now"))
        conn.execute("INSERT INTO sets (name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("Set B", "pokemon", "pokemon", "src", "cards", "2023-01-01", "now", "now"))
        conn.execute("INSERT INTO sets (name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("Set C", "pokemon", "pokemon", "src", "sealed", "2023-01-01", "now", "now"))
        rows = load_sets(conn, set_type="cards", shard_index=0, shard_count=2)
        self.assertEqual([row[1] for row in rows], ["Set A"])


if __name__ == "__main__":
    unittest.main()
