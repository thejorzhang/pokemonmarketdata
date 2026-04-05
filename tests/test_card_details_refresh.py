import sqlite3
import unittest

from card_details_refresh import (
    extract_tcgplayer_product_id,
    filter_card_products_for_shard,
    load_missing_card_products,
    parse_card_details,
    upsert_card_details,
)
from populate_db import ensure_runtime_schema


CARD_HTML = """
<html>
  <body>
    <h1>Charizard ex - Reverse Holofoil</h1>
    <span data-testid="lblProductDetailsSetName">Obsidian Flames</span>
    <div>Number: 125</div>
    <div>Rarity: Double Rare</div>
    <div>Release Date: 2023-08-11</div>
  </body>
</html>
"""


class TestCardDetailsRefresh(unittest.TestCase):
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
        conn.execute(
            """
            INSERT INTO sets (id, name, category_slug, product_line, source, set_type, release_date, first_seen_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "Obsidian Flames", "pokemon", "pokemon", "src", "cards", None, "now", "now"),
        )
        return conn

    def test_extract_tcgplayer_product_id(self):
        self.assertEqual(
            extract_tcgplayer_product_id("https://www.tcgplayer.com/product/123456/sample-card"),
            123456,
        )

    def test_parse_card_details(self):
        details = parse_card_details(
            CARD_HTML,
            fallback_name="Charizard ex",
            source_url="https://www.tcgplayer.com/product/123456/sample-card",
        )
        self.assertEqual(details["tcgplayer_product_id"], 123456)
        self.assertEqual(details["set_name"], "Obsidian Flames")
        self.assertEqual(details["card_number"], "125")
        self.assertEqual(details["rarity"], "Double Rare")
        self.assertEqual(details["finish"], "reverse_holofoil")
        self.assertEqual(details["language"], "English")

    def test_filter_card_products_for_shard(self):
        rows = [(1, "A", "u1", None), (2, "B", "u2", None), (3, "C", "u3", None)]
        filtered = filter_card_products_for_shard(rows, shard_index=1, shard_count=2)
        self.assertEqual(filtered, [(1, "A", "u1", None), (3, "C", "u3", None)])

    def test_upsert_card_details(self):
        conn = self.make_conn()
        conn.execute(
            """
            INSERT INTO card_products (
                tcgplayer_product_id, name, url, category_slug, product_line, source, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                123456,
                "Charizard ex",
                "https://www.tcgplayer.com/product/123456/sample-card",
                "pokemon",
                "pokemon",
                "TCGplayer Cards",
                "2026-04-01T00:00:00",
            ),
        )
        details = parse_card_details(
            CARD_HTML,
            fallback_name="Charizard ex",
            source_url="https://www.tcgplayer.com/product/123456/sample-card",
        )
        upsert_card_details(conn, 1, details)
        upsert_card_details(conn, 1, {**details, "rarity": "Illustration Rare"})

        row = conn.execute(
            "SELECT tcgplayer_product_id, card_number, rarity, finish FROM card_details WHERE card_product_id = 1"
        ).fetchone()
        self.assertEqual(row, (123456, "125", "Illustration Rare", "reverse_holofoil"))

    def test_load_missing_card_products_can_filter_by_set_name(self):
        conn = self.make_conn()
        conn.execute(
            """
            INSERT INTO card_products (
                set_id, tcgplayer_product_id, name, url, category_slug, product_line, set_name, source, discovered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 987654, "Card A", "https://www.tcgplayer.com/product/987654/card-a", "pokemon", "pokemon", "Obsidian Flames", "src", "now"),
        )
        rows = load_missing_card_products(conn, set_name="Obsidian Flames")
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
