import sqlite3
import unittest

from populate_db import ensure_runtime_schema
from refresh_priority import refresh_priority


class TestRefreshPriority(unittest.TestCase):
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

    def test_refresh_priority_populates_sealed_and_cards(self):
        conn = self.make_conn()
        conn.execute("INSERT INTO sets (name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("Scarlet & Violet", "pokemon", "pokemon", "src", "sealed", "2023-01-01", "now", "now"))
        conn.execute("INSERT INTO sets (name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("Obsidian Flames", "pokemon", "pokemon", "src", "cards", "2023-08-11", "now", "now"))
        conn.execute("INSERT INTO products (name, url) VALUES (?, ?)", ("Booster Pack", "https://www.tcgplayer.com/product/111/test-sealed"))
        conn.execute(
            "INSERT INTO product_details (product_id, set_id, tcgplayer_product_id, source_url, url_slug, raw_title, set_name, product_line, product_type, product_subtype, release_date, source, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, 1, 111, "u", "slug", "Booster Pack", "Scarlet & Violet", "pokemon", "booster_pack", None, "2023-01-01", "src", "now"),
        )
        conn.execute(
            "INSERT INTO listings (product_id, timestamp, snapshot_date, listing_count, lowest_price, source) VALUES (?, ?, ?, ?, ?, ?)",
            (1, "2026-04-05T00:00:00", "2026-04-05", 18, 4.99, "TCGplayer"),
        )
        conn.execute(
            "INSERT INTO sales (product_id, sale_date, condition_raw, variant, language, quantity, purchase_price, shipping_price, listing_type, title, custom_listing_key, custom_listing_id, source, sale_fingerprint, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "2026-04-04", "Near Mint", None, "English", 1, 5.99, 0.99, "marketplace", "Booster Pack", "x", "1", "TCGplayer", "fp1", "now"),
        )
        conn.execute(
            "INSERT INTO card_products (set_id, tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (2, 222, "Charizard ex", "https://www.tcgplayer.com/product/222/test-card", "pokemon", "pokemon", "Obsidian Flames", "src", "now"),
        )
        conn.execute(
            "INSERT INTO card_sales (card_product_id, sale_date, condition_raw, variant, language, quantity, purchase_price, shipping_price, listing_type, title, custom_listing_key, custom_listing_id, source, sale_fingerprint, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "2026-04-04", "Near Mint", "Holofoil", "English", 1, 12.99, 0.99, "marketplace", "Charizard ex", "y", "2", "TCGplayer Cards", "cfp1", "now"),
        )

        result = refresh_priority(conn)
        self.assertGreaterEqual(result["updated_targets"], 2)

        rows = conn.execute("SELECT target_kind, priority_tier, sales_30d FROM refresh_priority ORDER BY target_kind, target_id").fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], "cards")
        self.assertEqual(rows[1][0], "sealed")


if __name__ == "__main__":
    unittest.main()
