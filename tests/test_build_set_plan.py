import sqlite3
import tempfile
import unittest
from pathlib import Path

from build_set_plan import export_worker_csvs
from populate_db import ensure_runtime_schema


class TestBuildSetPlan(unittest.TestCase):
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

    def test_export_worker_csvs_for_sealed(self):
        conn = self.make_conn()
        conn.execute("INSERT INTO products (name, url) VALUES (?, ?)", ("Prod A", "https://www.tcgplayer.com/product/1/a"))
        conn.execute("INSERT INTO products (name, url) VALUES (?, ?)", ("Prod B", "https://www.tcgplayer.com/product/2/b"))
        plan = [
            {"worker": 1, "items": [{"set_name": "Set A", "count": 1, "score": 50.0, "target_ids": [1]}], "score": 50.0},
            {"worker": 2, "items": [{"set_name": "Set B", "count": 1, "score": 40.0, "target_ids": [2]}], "score": 40.0},
        ]
        with tempfile.TemporaryDirectory() as tempdir:
            paths = export_worker_csvs(conn, plan, tempdir, target_kind="sealed")
            self.assertEqual(len(paths), 2)
            self.assertIn("Prod A", Path(paths[0]).read_text(encoding="utf-8"))
            self.assertIn("Prod B", Path(paths[1]).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
