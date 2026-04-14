import sqlite3
import unittest

from collection_manager import add_collection_item, fetch_collection_summary
from populate_db import ensure_runtime_schema


class TestCollectionManager(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        ensure_runtime_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_add_sealed_item_and_value_it_from_latest_listing(self):
        self.conn.execute(
            """
            INSERT INTO products (id, name, url, release_date)
            VALUES (1, 'Celebrations Elite Trainer Box', 'https://www.tcgplayer.com/product/242811/pokemon-celebrations-celebrations-elite-trainer-box', '2021-10-08')
            """
        )
        self.conn.execute(
            """
            INSERT INTO product_details (product_id, tcgplayer_product_id, set_name, source, scraped_at)
            VALUES (1, 242811, 'Celebrations', 'TCGplayer Product Details', '2026-04-13 00:00:00')
            """
        )
        self.conn.execute(
            """
            INSERT INTO listings (
                product_id, timestamp, snapshot_date, market_price, lowest_total_price, source
            ) VALUES
                (1, '2026-04-10 12:00:00', '2026-04-10', 82.00, 79.99, 'TCGplayer'),
                (1, '2026-04-09 12:00:00', '2026-04-09', 76.00, 73.99, 'TCGplayer')
            """
        )
        self.conn.commit()

        added = add_collection_item(
            self.conn,
            collection_name="Test Collection",
            target_kind="sealed",
            tcgplayer_product_id=242811,
            quantity=2,
            unit_cost=70.0,
        )
        summary = fetch_collection_summary(self.conn, "Test Collection")

        self.assertEqual(added["tracked_product_id"], 1)
        self.assertEqual(summary["item_count"], 1)
        self.assertAlmostEqual(summary["estimated_value"], 164.0)
        self.assertAlmostEqual(summary["cost_basis"], 140.0)
        self.assertAlmostEqual(summary["unrealized_pnl"], 24.0)
        self.assertEqual(summary["items"][0]["price_source"], "listing_market")

    def test_add_card_item_and_value_it_from_card_price_history(self):
        self.conn.execute(
            """
            INSERT INTO card_products (
                id, tcgplayer_product_id, name, url, set_name, source, discovered_at
            ) VALUES (
                10,
                560350,
                'Genesect',
                'https://www.tcgplayer.com/product/560350/pokemon-sv-some-set-genesect',
                'Some Set',
                'TCGplayer Cards',
                '2026-04-13 00:00:00'
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO card_price_history (
                card_product_id, endpoint_kind, history_range, bucket_start_date,
                market_price, avg_sale_price, source, history_fingerprint, scraped_at
            ) VALUES
                (10, 'detailed', 'annual', '2026-04-10', 14.50, 13.90, 'TCGplayer Cards', 'fp-1', '2026-04-13 00:00:00'),
                (10, 'detailed', 'annual', '2026-04-09', 12.00, 11.50, 'TCGplayer Cards', 'fp-2', '2026-04-13 00:00:00')
            """
        )
        self.conn.commit()

        add_collection_item(
            self.conn,
            collection_name="Cards",
            target_kind="cards",
            tcgplayer_product_id=560350,
            quantity=3,
            unit_cost=10.0,
        )
        summary = fetch_collection_summary(self.conn, "Cards")

        self.assertEqual(summary["item_count"], 1)
        self.assertAlmostEqual(summary["estimated_value"], 43.5)
        self.assertAlmostEqual(summary["cost_basis"], 30.0)
        self.assertAlmostEqual(summary["unrealized_pnl"], 13.5)
        self.assertEqual(summary["items"][0]["price_source"], "card_history_market")


if __name__ == "__main__":
    unittest.main()
