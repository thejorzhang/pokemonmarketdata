import sqlite3
import unittest

from card_catalog_refresh import extract_tcgplayer_product_id, upsert_card_product
from populate_db import ensure_runtime_schema


class TestCardCatalogRefresh(unittest.TestCase):
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

    def test_extract_tcgplayer_product_id(self):
        self.assertEqual(
            extract_tcgplayer_product_id(
                "https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1"
            ),
            593294,
        )

    def test_upsert_card_product(self):
        conn = self.make_conn()
        upsert_card_product(
            conn,
            name="Charizard ex",
            url="https://www.tcgplayer.com/product/123456/pokemon-sv-sample-charizard-ex?page=1",
            category_slug="pokemon",
            product_line="pokemon",
            source="TCGplayer Cards",
        )
        upsert_card_product(
            conn,
            name="Charizard ex Updated",
            url="https://www.tcgplayer.com/product/123456/pokemon-sv-sample-charizard-ex?page=1",
            category_slug="pokemon",
            product_line="pokemon",
            source="TCGplayer Cards",
        )
        conn.commit()

        rows = conn.execute(
            "SELECT tcgplayer_product_id, name, category_slug, product_line, set_name, source FROM card_products"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], (123456, "Charizard ex Updated", "pokemon", "pokemon", None, "TCGplayer Cards"))


if __name__ == "__main__":
    unittest.main()
