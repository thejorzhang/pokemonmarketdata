import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from populate_db import ensure_runtime_schema
from sales_ingester import (
    build_requests_cookie_jar,
    extract_tcgplayer_product_id,
    fetch_all_latest_sales_json,
    ingest_latest_sales_pages,
    ingest_latest_sales,
    load_exported_session,
    load_sales_targets,
    insert_sales_rows,
    merge_latest_sales_payloads,
    normalize_latest_sales_payload,
    prepare_profile_clone,
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

    def test_merge_latest_sales_payloads_combines_pages(self):
        page1 = {
            "previousPage": "",
            "nextPage": 2,
            "resultCount": 2,
            "totalResults": 4,
            "data": [
                {"orderDate": "2026-03-28T12:00:00Z", "purchasePrice": 10.0, "shippingPrice": 0.5, "title": "A"},
                {"orderDate": "2026-03-28T13:00:00Z", "purchasePrice": 11.0, "shippingPrice": 0.5, "title": "B"},
            ],
        }
        page2 = {
            "previousPage": 1,
            "nextPage": "",
            "resultCount": 2,
            "totalResults": 4,
            "data": [
                {"orderDate": "2026-03-28T14:00:00Z", "purchasePrice": 12.0, "shippingPrice": 0.5, "title": "C"},
                {"orderDate": "2026-03-28T15:00:00Z", "purchasePrice": 13.0, "shippingPrice": 0.5, "title": "D"},
            ],
        }
        payload, source = merge_latest_sales_payloads([(page1, "requests"), (page2, "requests")])
        self.assertEqual(source, "requests")
        self.assertEqual(payload["totalResults"], 4)
        self.assertEqual(payload["resultCount"], 4)
        self.assertEqual(len(payload["data"]), 4)

    def test_fetch_all_latest_sales_json_follows_next_page(self):
        page1 = {
            "previousPage": "",
            "nextPage": 2,
            "resultCount": 2,
            "totalResults": 4,
            "data": [
                {"orderDate": "2026-03-28T12:00:00Z", "purchasePrice": 10.0, "shippingPrice": 0.5, "title": "A"},
                {"orderDate": "2026-03-28T13:00:00Z", "purchasePrice": 11.0, "shippingPrice": 0.5, "title": "B"},
            ],
        }
        page2 = {
            "previousPage": 1,
            "nextPage": "",
            "resultCount": 2,
            "totalResults": 4,
            "data": [
                {"orderDate": "2026-03-28T14:00:00Z", "purchasePrice": 12.0, "shippingPrice": 0.5, "title": "C"},
                {"orderDate": "2026-03-28T15:00:00Z", "purchasePrice": 13.0, "shippingPrice": 0.5, "title": "D"},
            ],
        }
        with patch("sales_ingester.fetch_latest_sales_json", side_effect=[(page1, "requests"), (page2, "requests")]) as mocked:
            payload, source = fetch_all_latest_sales_json(593294, use_browser_fallback=False, headless=True)
        self.assertEqual(source, "requests")
        self.assertEqual(payload["resultCount"], 4)
        self.assertEqual(len(payload["data"]), 4)
        self.assertEqual(mocked.call_count, 2)

    def test_fetch_all_latest_sales_json_passes_profile_args(self):
        page = {
            "previousPage": "",
            "nextPage": "",
            "resultCount": 1,
            "totalResults": 1,
            "data": [{"orderDate": "2026-03-28T12:00:00Z", "purchasePrice": 10.0, "shippingPrice": 0.5, "title": "A"}],
        }
        with patch("sales_ingester.fetch_latest_sales_json", return_value=(page, "selenium")) as mocked:
            payload, source = fetch_all_latest_sales_json(
                593294,
                use_browser_fallback=True,
                headless=False,
                user_data_dir="/tmp/tcgplayer-profile",
                profile_directory="Default",
            )
        self.assertEqual(source, "selenium")
        self.assertEqual(payload["resultCount"], 1)
        self.assertEqual(mocked.call_args.kwargs["user_data_dir"], "/tmp/tcgplayer-profile")
        self.assertEqual(mocked.call_args.kwargs["profile_directory"], "Default")

    def test_fetch_all_latest_sales_json_uses_offset_paging_with_session_file(self):
        page1 = {
            "previousPage": "",
            "nextPage": "Yes",
            "resultCount": 25,
            "totalResults": 40,
            "data": [{"orderDate": f"2026-03-28T12:{i:02d}:00Z", "purchasePrice": 10.0 + i, "shippingPrice": 0.5, "title": f"A{i}"} for i in range(25)],
        }
        page2 = {
            "previousPage": "",
            "nextPage": "",
            "resultCount": 15,
            "totalResults": 40,
            "data": [{"orderDate": f"2026-03-29T12:{i:02d}:00Z", "purchasePrice": 20.0 + i, "shippingPrice": 0.5, "title": f"B{i}"} for i in range(15)],
        }
        with patch("sales_ingester.fetch_latest_sales_json", side_effect=[(page1, "requests"), (page2, "requests")]) as mocked:
            payload, source = fetch_all_latest_sales_json(
                242811,
                use_browser_fallback=False,
                headless=True,
                session_file="/tmp/tcgplayer_session.json",
            )
        self.assertEqual(source, "requests")
        self.assertEqual(payload["resultCount"], 40)
        self.assertEqual(payload["totalResults"], 40)
        self.assertEqual(len(payload["data"]), 40)
        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(mocked.call_args_list[0].kwargs["offset"], 0)
        self.assertEqual(mocked.call_args_list[0].kwargs["limit"], 25)
        self.assertEqual(mocked.call_args_list[1].kwargs["offset"], 25)
        self.assertEqual(mocked.call_args_list[1].kwargs["limit"], 25)

    def test_prepare_profile_clone_copies_local_state_and_profile(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_root = root / "source"
            source_profile = source_root / "Default"
            source_profile.mkdir(parents=True)
            (source_root / "Local State").write_text('{"ok":true}', encoding="utf-8")
            (source_profile / "Cookies").write_text("cookie-data", encoding="utf-8")
            clone_root, clone_profile = prepare_profile_clone(str(source_root), "Default")
            try:
                clone_root = Path(clone_root)
                self.assertEqual(clone_profile, "Default")
                self.assertTrue((clone_root / "Local State").exists())
                self.assertEqual((clone_root / "Default" / "Cookies").read_text(encoding="utf-8"), "cookie-data")
            finally:
                import shutil

                shutil.rmtree(clone_root, ignore_errors=True)

    def test_load_exported_session_and_cookie_jar(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            session_path = Path(tmpdir) / "session.json"
            session_path.write_text(
                json.dumps(
                    {
                        "cookies": [
                            {
                                "name": "TCGAuthTicket_Production",
                                "value": "secret",
                                "domain": ".tcgplayer.com",
                                "path": "/",
                                "secure": True,
                                "httpOnly": True,
                                "sameSite": "Lax",
                            }
                        ],
                        "local_storage": {"foo": "bar"},
                        "session_storage": {"baz": "qux"},
                    }
                ),
                encoding="utf-8",
            )
            session = load_exported_session(str(session_path))
            self.assertEqual(session["local_storage"]["foo"], "bar")
            self.assertEqual(session["session_storage"]["baz"], "qux")
            jar = build_requests_cookie_jar(str(session_path))
            self.assertEqual(jar.get("TCGAuthTicket_Production"), "secret")

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

    def test_ingest_latest_sales_pages_inserts_chunk_by_chunk(self):
        conn = self.make_conn()
        conn.execute(
            "UPDATE products SET url = ? WHERE id = 1",
            ("https://www.tcgplayer.com/product/593294/test-product",),
        )
        page1 = {
            "previousPage": "",
            "nextPage": "Yes",
            "resultCount": 2,
            "totalResults": 3,
            "data": [
                {"orderDate": "2026-03-28T12:00:00Z", "purchasePrice": 10.0, "shippingPrice": 0.5, "title": "A"},
                {"orderDate": "2026-03-28T13:00:00Z", "purchasePrice": 11.0, "shippingPrice": 0.5, "title": "B"},
            ],
        }
        page2 = {
            "previousPage": "",
            "nextPage": "",
            "resultCount": 1,
            "totalResults": 3,
            "data": [
                {"orderDate": "2026-03-28T14:00:00Z", "purchasePrice": 12.0, "shippingPrice": 0.5, "title": "C"},
            ],
        }
        with patch("sales_ingester.iter_latest_sales_pages", return_value=iter([(page1, "requests"), (page2, "requests")])):
            result = ingest_latest_sales_pages(
                conn,
                internal_product_id=1,
                tcgplayer_product_id=593294,
                resolved_url="https://www.tcgplayer.com/product/593294/test-product",
                sale_date="2026-03-28",
            )
        self.assertEqual(result["fetch_source"], "requests")
        self.assertEqual(result["pages_processed"], 2)
        self.assertEqual(result["fetched_rows"], 3)
        self.assertEqual(result["inserted_rows"], 3)
        count = conn.execute("SELECT COUNT(*) FROM sales WHERE product_id = 1").fetchone()[0]
        self.assertEqual(count, 3)

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
