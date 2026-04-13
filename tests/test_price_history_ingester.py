import sqlite3
import unittest
from unittest.mock import patch

from populate_db import ensure_runtime_schema
from price_history_ingester import (
    fetch_all_history_json,
    ingest_history_target,
    insert_history_rows,
    load_history_targets,
    merge_history_payloads,
    normalize_history_payload,
)


class TestPriceHistoryIngester(unittest.TestCase):
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
            CREATE TABLE product_details (
                product_id INTEGER PRIMARY KEY,
                set_id INTEGER,
                tcgplayer_product_id INTEGER,
                source_url TEXT,
                url_slug TEXT,
                raw_title TEXT,
                set_name TEXT,
                product_line TEXT,
                product_type TEXT,
                product_subtype TEXT,
                release_date TEXT,
                source TEXT NOT NULL,
                scraped_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE card_details (
                card_product_id INTEGER PRIMARY KEY,
                tcgplayer_product_id INTEGER,
                source_url TEXT,
                raw_title TEXT,
                set_name TEXT,
                card_number TEXT,
                rarity TEXT,
                finish TEXT,
                language TEXT,
                supertype TEXT,
                subtype TEXT,
                release_date TEXT,
                source TEXT NOT NULL,
                scraped_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE sets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category_slug TEXT,
                product_line TEXT,
                source TEXT NOT NULL,
                set_type TEXT,
                release_date TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
            """
        )
        ensure_runtime_schema(conn)
        conn.execute(
            "INSERT INTO products (name, url) VALUES (?, ?)",
            ("Test Product", "https://www.tcgplayer.com/product/593294/test-product"),
        )
        conn.execute(
            "INSERT INTO card_products (set_id, tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, 593294, "Test Card", "https://www.tcgplayer.com/product/593294/test-card", "pokemon", "pokemon", "Obsidian Flames", "TCGplayer Cards", "2026-04-02T00:00:00"),
        )
        conn.execute(
            "INSERT INTO product_details (product_id, set_id, tcgplayer_product_id, set_name, source, scraped_at) VALUES (?, ?, ?, ?, ?, ?)",
            (1, 1, 593294, "Obsidian Flames", "TCGplayer", "2026-04-02T00:00:00"),
        )
        conn.execute(
            "INSERT INTO card_details (card_product_id, tcgplayer_product_id, set_name, source, scraped_at) VALUES (?, ?, ?, ?, ?)",
            (1, 593294, "Obsidian Flames", "TCGplayer Cards", "2026-04-02T00:00:00"),
        )
        conn.execute(
            "INSERT INTO sets (name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("Obsidian Flames", "pokemon", "pokemon", "TCGplayer", "sealed", "2023-08-11", "2026-04-02T00:00:00", "2026-04-02T00:00:00"),
        )
        return conn

    def test_normalize_history_payload_extracts_bucket_rows(self):
        payload = {
            "data": [
                {
                    "bucketStartDate": "2026-03-01",
                    "bucketEndDate": "2026-03-31",
                    "marketPrice": 12.5,
                    "quantitySold": 4,
                    "transactionCount": 5,
                    "lowSalePrice": 10.0,
                    "lowSalePriceWithShipping": 10.5,
                    "highSalePrice": 15.0,
                    "highSalePriceWithShipping": 15.5,
                    "avgSalePrice": 12.0,
                    "avgSalePriceWithShipping": 12.5,
                    "bucketLabel": "March 2026",
                }
            ]
        }
        rows = normalize_history_payload(payload, endpoint_kind="detailed", history_range="quarter")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["bucket_start_date"], "2026-03-01")
        self.assertEqual(rows[0]["bucket_end_date"], "2026-03-31")
        self.assertEqual(rows[0]["market_price"], 12.5)
        self.assertEqual(rows[0]["quantity_sold"], 4)
        self.assertEqual(rows[0]["transaction_count"], 5)
        self.assertEqual(rows[0]["bucket_label"], "March 2026")
        self.assertTrue(rows[0]["bucket_json"])

    def test_merge_history_payloads_combines_pages(self):
        page1 = {"nextPage": 2, "resultCount": 1, "totalResults": 2, "data": [{"bucketStartDate": "2026-03-01", "marketPrice": 12.5}]}
        page2 = {"nextPage": "", "resultCount": 1, "totalResults": 2, "data": [{"bucketStartDate": "2026-04-01", "marketPrice": 13.5}]}
        payload, source = merge_history_payloads([(page1, "requests"), (page2, "requests")])
        self.assertEqual(source, "requests")
        self.assertEqual(payload["resultCount"], 2)
        self.assertEqual(len(payload["data"]), 2)

    def test_insert_history_rows_writes_to_price_history(self):
        conn = self.make_conn()
        rows = normalize_history_payload(
            {
                "data": [
                    {
                        "bucketStartDate": "2026-03-01",
                        "bucketEndDate": "2026-03-31",
                        "marketPrice": 12.5,
                        "quantitySold": 4,
                        "transactionCount": 5,
                        "lowSalePrice": 10.0,
                        "lowSalePriceWithShipping": 10.5,
                        "highSalePrice": 15.0,
                        "highSalePriceWithShipping": 15.5,
                        "avgSalePrice": 12.0,
                        "avgSalePriceWithShipping": 12.5,
                        "bucketLabel": "March 2026",
                    }
                ]
            },
            endpoint_kind="summary",
            history_range="quarter",
        )
        inserted = insert_history_rows(conn, 1, rows, endpoint_kind="summary", history_range="quarter", target_kind="sealed")
        self.assertEqual(inserted, 1)
        row = conn.execute(
            """
            SELECT endpoint_kind, history_range, bucket_start_date, market_price, quantity_sold, transaction_count
            FROM price_history
            """
        ).fetchone()
        self.assertEqual(row, ("summary", "quarter", "2026-03-01", 12.5, 4, 5))

    def test_insert_history_rows_writes_to_card_price_history(self):
        conn = self.make_conn()
        rows = normalize_history_payload(
            {
                "data": [
                    {
                        "bucketStartDate": "2026-03-01",
                        "marketPrice": 9.5,
                        "quantitySold": 2,
                        "transactionCount": 2,
                    }
                ]
            },
            endpoint_kind="detailed",
            history_range="annual",
        )
        inserted = insert_history_rows(conn, 1, rows, endpoint_kind="detailed", history_range="annual", target_kind="cards")
        self.assertEqual(inserted, 1)
        row = conn.execute(
            """
            SELECT endpoint_kind, history_range, bucket_start_date, market_price, quantity_sold, transaction_count
            FROM card_price_history
            """
        ).fetchone()
        self.assertEqual(row, ("detailed", "annual", "2026-03-01", 9.5, 2, 2))

    def test_load_history_targets_can_filter_by_set_name(self):
        conn = self.make_conn()
        targets = load_history_targets(conn, target_kind="cards", set_name="Obsidian Flames")
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0][1], 593294)

    def test_fetch_all_history_json_follows_next_page(self):
        page1 = {"nextPage": 2, "resultCount": 1, "totalResults": 2, "data": [{"bucketStartDate": "2026-03-01", "marketPrice": 12.5}]}
        page2 = {"nextPage": "", "resultCount": 1, "totalResults": 2, "data": [{"bucketStartDate": "2026-04-01", "marketPrice": 13.5}]}
        with patch("price_history_ingester.fetch_history_json", side_effect=[(page1, "requests"), (page2, "requests")]) as mocked:
            payload, source = fetch_all_history_json(593294, use_browser_fallback=False, headless=True)
        self.assertEqual(source, "requests")
        self.assertEqual(payload["resultCount"], 2)
        self.assertEqual(len(payload["data"]), 2)
        self.assertEqual(mocked.call_count, 2)

    def test_ingest_history_target_populates_both_endpoint_kinds(self):
        conn = self.make_conn()
        payload = {
            "data": [
                {
                    "bucketStartDate": "2026-03-01",
                    "bucketEndDate": "2026-03-31",
                    "marketPrice": 12.5,
                    "quantitySold": 4,
                    "transactionCount": 5,
                    "bucketLabel": "March 2026",
                }
            ]
        }
        with patch("price_history_ingester.fetch_all_history_json", return_value=(payload, "requests")) as mocked:
            result = ingest_history_target(
                conn,
                product_id=593294,
                target_kind="sealed",
                history_ranges=["quarter"],
                endpoint_kind="both",
                use_browser_fallback=False,
                headless=True,
            )
        self.assertEqual(result["inserted_rows"], 2)
        self.assertEqual(mocked.call_count, 2)
        row_count = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
        self.assertEqual(row_count, 2)


if __name__ == "__main__":
    unittest.main()
