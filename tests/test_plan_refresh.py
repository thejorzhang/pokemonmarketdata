import sqlite3
import tempfile
import unittest
from pathlib import Path

from plan_refresh import build_worker_plan, export_sealed_csv, load_due_rows
from populate_db import ensure_runtime_schema


class TestPlanRefresh(unittest.TestCase):
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

    def test_build_worker_plan_groups_by_set(self):
        rows = [
            (1, "Set A", 100.0, "hot"),
            (2, "Set A", 50.0, "warm"),
            (3, "Set B", 20.0, "cold"),
        ]
        plan = build_worker_plan(rows, 2)
        self.assertEqual(len(plan), 2)
        self.assertEqual(sum(len(worker["items"]) for worker in plan), 2)

    def test_load_due_rows_and_export_sealed_csv(self):
        conn = self.make_conn()
        conn.execute("INSERT INTO products (name, url) VALUES (?, ?)", ("Prod A", "https://www.tcgplayer.com/product/1/a"))
        conn.execute(
            """
            INSERT INTO refresh_priority (
                target_kind, target_id, set_name, activity_score, priority_tier, refresh_interval_hours,
                sales_7d, sales_30d, next_refresh_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("sealed", 1, "Set A", 80.0, "hot", 24, 1, 1, "2000-01-01T00:00:00+00:00", "2026-04-05T00:00:00+00:00"),
        )
        rows = load_due_rows(conn, "sealed")
        self.assertEqual(len(rows), 1)
        with tempfile.TemporaryDirectory() as tempdir:
            out_path = str(Path(tempdir) / "due.csv")
            count = export_sealed_csv(conn, rows, out_path)
            self.assertEqual(count, 1)
            self.assertIn("Prod A", Path(out_path).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
