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


if __name__ == "__main__":
    unittest.main()
