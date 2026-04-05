import json
import sqlite3
import unittest
from pathlib import Path

from populate_db import ensure_runtime_schema
from sales_ingester import (
    extract_tcgplayer_product_id,
    ingest_latest_sales,
    load_sales_targets,
    insert_sales_rows,
    normalize_latest_sales_payload,
    sale_fingerprint,
    should_initial_backfill,
    target_has_completed_sales_backfill,
    target_has_existing_sales,
)


FIXTURES = Path(__file__).parent / "fixtures"


class TestSalesIngester(unittest.TestCase):
    def fixture(self, name):
        return json.loads((FIXTURES / name).read_text(encoding="utf-8"))

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
            CREATE TABLE sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                sale_date TEXT NOT NULL,
                condition_raw TEXT,
                variant TEXT,
                language TEXT,
                quantity INTEGER,
                purchase_price REAL,
                shipping_price REAL,
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
        conn.execute("INSERT INTO products (name, url) VALUES (?, ?)", ("Test Product", "https://example.com/p/1"))
        return conn

    def make_card_conn(self):
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
        conn.execute(
            """
            INSERT INTO card_products (tcgplayer_product_id, name, url, category_slug, product_line, source, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                593294,
                "Test Card",
                "https://www.tcgplayer.com/product/593294/test-card",
                "pokemon",
                "pokemon",
                "TCGplayer Cards",
                "2026-04-02T00:00:00",
            ),
        )
        return conn

    def test_extract_product_id_from_url(self):
        self.assertEqual(
            extract_tcgplayer_product_id(
                "https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1"
            ),
            593294,
        )

    def test_normalize_latest_sales_payload_filters_and_dedupes(self):
        payload = self.fixture("tcgplayer_latestsales.json")
        rows = normalize_latest_sales_payload(payload, sale_date="2026-03-28")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["sale_date"], "2026-03-28")
        self.assertEqual(rows[0]["purchase_price"], 11.99)
        self.assertEqual(rows[0]["shipping_price"], 0.5)
        self.assertEqual(rows[0]["quantity"], 1)
        self.assertTrue(rows[0]["sale_fingerprint"])
        self.assertEqual(rows[0]["sale_fingerprint"], sale_fingerprint(rows[0]))

    def test_insert_sales_rows_ignores_duplicate_fingerprints(self):
        conn = self.make_conn()
        payload = self.fixture("tcgplayer_latestsales.json")
        rows = normalize_latest_sales_payload(payload, sale_date="2026-03-28")
        inserted_first = insert_sales_rows(conn, 1, rows)
        inserted_second = insert_sales_rows(conn, 1, rows)
        conn.commit()

        self.assertEqual(inserted_first, 2)
        self.assertEqual(inserted_second, 0)
        row = conn.execute(
            """
            SELECT sale_date, purchase_price, shipping_price, quantity, source
            FROM sales
            ORDER BY id
            LIMIT 1
            """
        ).fetchone()
        self.assertEqual(row, ("2026-03-28", 11.99, 0.5, 1, "TCGplayer"))

    def test_snapshot_file_style_ingest_creates_or_resolves_product_row(self):
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

        payload = self.fixture("tcgplayer_latestsales.json")
        rows = normalize_latest_sales_payload(payload, sale_date="2026-03-28")
        self.assertEqual(len(rows), 2)

        conn.execute(
            "INSERT INTO products (name, url) VALUES (?, ?)",
            (
                "Prismatic Evolutions Booster Pack",
                "https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1",
            ),
        )
        inserted = insert_sales_rows(conn, 1, rows)
        conn.commit()
        self.assertEqual(inserted, 2)
        count = conn.execute("SELECT COUNT(*) FROM sales WHERE product_id = 1").fetchone()[0]
        self.assertEqual(count, 2)

    def test_ingest_latest_sales_uses_internal_product_row_for_snapshot_file_path_shape(self):
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

        fixture_path = FIXTURES / "tcgplayer_latestsales.json"
        payload = self.fixture("tcgplayer_latestsales.json")
        conn.execute(
            "INSERT INTO products (name, url) VALUES (?, ?)",
            ("Placeholder", "https://www.tcgplayer.com/product/593294/")
        )
        conn.commit()

        from unittest.mock import patch

        with patch("sales_ingester.fetch_latest_sales_json", return_value=(payload, "snapshot")):
            result = ingest_latest_sales(
                conn,
                product_id=593294,
                product_url="https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1",
                sale_date="2026-03-28",
            )

        self.assertEqual(result["product_id"], 1)
        self.assertEqual(result["tcgplayer_product_id"], 593294)
        self.assertEqual(result["inserted_rows"], 2)
        sales_rows = conn.execute(
            "SELECT product_id, sale_date, shipping_price FROM sales ORDER BY id"
        ).fetchall()
        self.assertEqual(sales_rows, [(1, "2026-03-28", 0.5), (1, "2026-03-28", 0.5)])

    def test_load_sales_targets_defaults_to_all_tcgplayer_products(self):
        conn = self.make_conn()
        conn.execute(
            "INSERT INTO products (name, url) VALUES (?, ?)",
            (
                "Prismatic Evolutions Booster Pack",
                "https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1",
            ),
        )
        conn.execute(
            "INSERT INTO products (name, url) VALUES (?, ?)",
            ("Non TCGplayer Product", "https://example.com/not-tcgplayer"),
        )
        conn.commit()

        targets = load_sales_targets(conn)
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0][1], 593294)

    def test_load_sales_targets_for_cards_uses_card_products(self):
        conn = self.make_card_conn()
        targets = load_sales_targets(conn, target_kind="cards")
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0][1], 593294)

    def test_load_sales_targets_can_filter_by_card_set(self):
        conn = self.make_card_conn()
        conn.execute(
            """
            INSERT INTO sets (id, name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (7, "Obsidian Flames", "pokemon", "pokemon", "src", "cards", None, "now", "now"),
        )
        conn.execute("UPDATE card_products SET set_id = 7 WHERE id = 1")
        targets = load_sales_targets(conn, target_kind="cards", set_name="Obsidian Flames")
        self.assertEqual(len(targets), 1)

    def test_insert_sales_rows_for_cards_writes_to_card_sales(self):
        conn = self.make_card_conn()
        payload = self.fixture("tcgplayer_latestsales.json")
        rows = normalize_latest_sales_payload(payload, sale_date="2026-03-28")
        inserted = insert_sales_rows(conn, 1, rows, source="TCGplayer Cards", target_kind="cards")
        conn.commit()
        self.assertEqual(inserted, 2)
        row = conn.execute(
            "SELECT card_product_id, sale_date, shipping_price, source FROM card_sales ORDER BY id LIMIT 1"
        ).fetchone()
        self.assertEqual(row, (1, "2026-03-28", 0.5, "TCGplayer Cards"))

    def test_target_has_existing_sales_for_cards(self):
        conn = self.make_card_conn()
        self.assertFalse(target_has_existing_sales(conn, 1, target_kind="cards"))
        self.assertFalse(target_has_completed_sales_backfill(conn, 1, target_kind="cards"))
        conn.execute(
            """
            INSERT INTO card_sales (card_product_id, sale_date, purchase_price, shipping_price, source, sale_fingerprint, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "2026-03-28", 11.99, 0.5, "TCGplayer Cards", "existing-card-sale", "now"),
        )
        self.assertTrue(target_has_existing_sales(conn, 1, target_kind="cards"))

    def test_ingest_latest_sales_backfills_all_returned_rows_for_first_card_refresh(self):
        conn = self.make_card_conn()
        payload = self.fixture("tcgplayer_latestsales.json")

        from unittest.mock import patch

        with patch("sales_ingester.fetch_latest_sales_json", return_value=(payload, "snapshot")):
            result = ingest_latest_sales(
                conn,
                product_id=593294,
                product_url="https://www.tcgplayer.com/product/593294/test-card",
                sale_date="2099-01-01",
                source="TCGplayer Cards",
                target_kind="cards",
            )

        self.assertTrue(result["initial_backfill"])
        self.assertEqual(result["fetched_rows"], 2)
        self.assertEqual(result["inserted_rows"], 2)
        self.assertTrue(target_has_completed_sales_backfill(conn, 1, target_kind="cards"))
        refresh_row = conn.execute(
            "SELECT last_sales_refresh_at, sales_backfill_completed_at FROM card_products WHERE id = 1"
        ).fetchone()
        self.assertIsNotNone(refresh_row[0])
        self.assertIsNotNone(refresh_row[1])

    def test_first_card_backfill_only_happens_once_even_with_no_sales(self):
        conn = self.make_card_conn()
        payload = {"data": []}

        from unittest.mock import patch

        with patch("sales_ingester.fetch_latest_sales_json", return_value=(payload, "snapshot")):
            first = ingest_latest_sales(
                conn,
                product_id=593294,
                product_url="https://www.tcgplayer.com/product/593294/test-card",
                sale_date="2026-04-04",
                source="TCGplayer Cards",
                target_kind="cards",
            )
            second = ingest_latest_sales(
                conn,
                product_id=593294,
                product_url="https://www.tcgplayer.com/product/593294/test-card",
                sale_date="2026-04-04",
                source="TCGplayer Cards",
                target_kind="cards",
            )

        self.assertTrue(first["initial_backfill"])
        self.assertFalse(second["initial_backfill"])
        self.assertTrue(target_has_completed_sales_backfill(conn, 1, target_kind="cards"))

    def test_card_backfill_is_skipped_before_release_date(self):
        conn = self.make_card_conn()
        conn.execute(
            """
            INSERT INTO card_details (
                card_product_id, tcgplayer_product_id, source_url, raw_title, set_name,
                card_number, rarity, finish, language, supertype, subtype, release_date, source, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                593294,
                "https://www.tcgplayer.com/product/593294/test-card",
                "Test Card",
                "Test Set",
                "1/100",
                "Rare",
                "Normal",
                "English",
                "Pokemon",
                "Basic",
                "2026-04-10",
                "TCGplayer Cards",
                "now",
            ),
        )

        self.assertFalse(should_initial_backfill(conn, 1, sale_date="2026-04-04", target_kind="cards"))


if __name__ == "__main__":
    unittest.main()
