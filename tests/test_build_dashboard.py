import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class TestBuildDashboard(unittest.TestCase):
    def make_empty_db(self, db_path):
        conn = sqlite3.connect(db_path)
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
                run_id INTEGER
            )
            """
        )
        conn.commit()
        conn.close()

    def test_writes_dashboard_without_scrape_tables(self):
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            db_path = workdir / "empty.db"
            out_path = workdir / "dashboard.html"
            self.make_empty_db(db_path)

            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "build_dashboard.py"),
                    "--db",
                    str(db_path),
                    "--source",
                    "TCGplayer",
                    "--out",
                    out_path.name,
                ],
                cwd=workdir,
                capture_output=True,
                text=True,
                check=True,
            )

            self.assertIn("Wrote dashboard:", result.stdout)
            self.assertTrue(out_path.exists())
            html = out_path.read_text(encoding="utf-8")
            self.assertIn("No rows found for this source yet", html)

    def test_dashboard_includes_collection_section_when_holdings_exist(self):
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            db_path = workdir / "collection.db"
            out_path = workdir / "dashboard.html"

            conn = sqlite3.connect(db_path)
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
                    run_id INTEGER
                )
                """
            )
            conn.execute(
                "INSERT INTO products (id, name, url, release_date) VALUES (1, 'Sample Box', 'https://example.test/product/1', '2026-01-01')"
            )
            conn.execute(
                """
                INSERT INTO listings (product_id, timestamp, snapshot_date, market_price, source)
                VALUES (1, '2026-04-10 00:00:00', '2026-04-10', 50.0, 'TCGplayer')
                """
            )
            conn.commit()
            conn.close()

            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "collection_manager.py"),
                    "--db",
                    str(db_path),
                    "add",
                    "--collection",
                    "My Collection",
                    "--target-kind",
                    "sealed",
                    "--tracked-product-id",
                    "1",
                    "--quantity",
                    "2",
                    "--unit-cost",
                    "40",
                ],
                cwd=workdir,
                capture_output=True,
                text=True,
                check=True,
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "build_dashboard.py"),
                    "--db",
                    str(db_path),
                    "--source",
                    "TCGplayer",
                    "--out",
                    out_path.name,
                ],
                cwd=workdir,
                capture_output=True,
                text=True,
                check=True,
            )

            self.assertIn("Wrote dashboard:", result.stdout)
            html = out_path.read_text(encoding="utf-8")
            self.assertIn("Collection Overview", html)
            self.assertIn("Collection Holdings", html)
            self.assertIn("Sample Box", html)


if __name__ == "__main__":
    unittest.main()
