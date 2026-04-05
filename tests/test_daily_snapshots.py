import sqlite3
import unittest

from populate_db import ensure_runtime_schema, insert_snapshot


class TestDailySnapshots(unittest.TestCase):
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

    def test_same_day_snapshot_upserts_in_place(self):
        conn = self.make_conn()
        conn.execute("INSERT INTO products (name, url) VALUES (?, ?)", ("Test Product", "https://example.com/a"))
        product_id = conn.execute("SELECT id FROM products").fetchone()[0]

        insert_snapshot(
            conn,
            product_id,
            listing_count=10,
            lowest_price=100.0,
            lowest_shipping=2.5,
            lowest_total_price=102.5,
            market_price=105.0,
            source="TCGplayer",
            run_id=1,
            snapshot_timestamp="2026-03-24T03:00:00",
            snapshot_date="2026-03-24",
        )
        insert_snapshot(
            conn,
            product_id,
            listing_count=8,
            lowest_price=98.0,
            lowest_shipping=3.0,
            lowest_total_price=101.0,
            market_price=101.0,
            source="TCGplayer",
            run_id=2,
            snapshot_timestamp="2026-03-24T05:00:00",
            snapshot_date="2026-03-24",
        )
        conn.commit()

        rows = conn.execute(
            """
            SELECT snapshot_date, timestamp, listing_count, lowest_price, lowest_shipping, lowest_total_price, market_price, run_id
            FROM listings
            WHERE product_id = ?
            """,
            (product_id,),
        ).fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], ("2026-03-24", "2026-03-24T05:00:00", 8, 98.0, 3.0, 101.0, 101.0, 2))


if __name__ == "__main__":
    unittest.main()
