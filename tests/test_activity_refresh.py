import sqlite3
import unittest

from populate_db import ensure_runtime_schema
from refresh_activity import refresh_activity


class TestActivityRefresh(unittest.TestCase):
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

    def test_refresh_activity_populates_scrape_activity(self):
        conn = self.make_conn()
        conn.execute("INSERT INTO products (name, url) VALUES (?, ?)", ("Sealed A", "https://www.tcgplayer.com/product/111/a"))
        conn.execute(
            """
            INSERT INTO listings (product_id, timestamp, snapshot_date, listing_count, lowest_price, market_price, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "2026-04-05T00:00:00", "2026-04-05", 10, 5.0, 6.0, "TCGplayer"),
        )
        conn.execute(
            """
            INSERT INTO sales (product_id, sale_date, source, sale_fingerprint, scraped_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (1, "2026-04-04", "TCGplayer", "fp1", "2026-04-05T00:00:00"),
        )
        conn.execute(
            """
            INSERT INTO card_products (tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (222, "Card A", "https://www.tcgplayer.com/product/222/a", "pokemon", "pokemon", "Obsidian Flames", "src", "now"),
        )
        conn.execute(
            """
            INSERT INTO card_sales (card_product_id, sale_date, source, sale_fingerprint, scraped_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (1, "2026-04-04", "TCGplayer Cards", "cfp1", "2026-04-05T00:00:00"),
        )
        result = refresh_activity(conn)
        self.assertEqual(result["targets"], 2)
        rows = conn.execute(
            "SELECT target_kind, priority_tier, recent_sales_30d FROM scrape_activity ORDER BY target_kind"
        ).fetchall()
        self.assertEqual(rows, [("cards", "warm", 1), ("sealed", "warm", 1)])
