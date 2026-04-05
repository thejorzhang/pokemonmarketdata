import sqlite3
import unittest
from pathlib import Path

from populate_db import ensure_runtime_schema
from product_details_refresh import (
    classify_product_type,
    extract_tcgplayer_product_id,
    parse_product_details,
    upsert_product_details,
)


FIXTURES = Path(__file__).parent / "fixtures"


class TestProductDetailsRefresh(unittest.TestCase):
    def fixture(self, name):
        return (FIXTURES / name).read_text(encoding="utf-8")

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

    def test_extract_product_id(self):
        self.assertEqual(
            extract_tcgplayer_product_id(
                "https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1"
            ),
            593294,
        )

    def test_classify_product_type(self):
        self.assertEqual(classify_product_type("Journey Together Elite Trainer Box"), "elite_trainer_box")
        self.assertEqual(classify_product_type("Destined Rivals Sleeved Booster Pack"), "sleeved_booster_pack")
        self.assertEqual(classify_product_type("Pokemon Day 2026 Collection"), "collection")

    def test_parse_product_details(self):
        parsed = parse_product_details(
            self.fixture("tcgplayer_product_details.html"),
            fallback_name="Prismatic Evolutions Booster Pack",
            source_url="https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1",
        )
        self.assertEqual(parsed["tcgplayer_product_id"], 593294)
        self.assertEqual(parsed["set_name"], "Scarlet & Violet")
        self.assertEqual(parsed["product_type"], "booster_pack")
        self.assertEqual(parsed["release_date"], "2026-01-17")

    def test_upsert_product_details(self):
        conn = self.make_conn()
        conn.execute(
            "INSERT INTO products (name, url) VALUES (?, ?)",
            (
                "Prismatic Evolutions Booster Pack",
                "https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1",
            ),
        )
        details = parse_product_details(
            self.fixture("tcgplayer_product_details.html"),
            fallback_name="Prismatic Evolutions Booster Pack",
            source_url="https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1",
        )
        upsert_product_details(conn, 1, details)
        conn.commit()
        row = conn.execute(
            "SELECT tcgplayer_product_id, set_name, product_type, release_date FROM product_details WHERE product_id = 1"
        ).fetchone()
        self.assertEqual(row, (593294, "Scarlet & Violet", "booster_pack", "2026-01-17"))


if __name__ == "__main__":
    unittest.main()
