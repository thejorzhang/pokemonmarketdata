import unittest

from operator_console import build_command


class TestOperatorConsole(unittest.TestCase):
    def test_build_scrape_command_is_db_driven_by_default(self):
        command = build_command(
            "scrape",
            {
                "db": "sealed_market.db",
                "source": "TCGplayer",
                "snapshot_date": "2026-04-05",
                "limit": 0,
                "commit_every": 25,
                "delay_min": 2.0,
                "delay_max": 5.0,
                "workers": 4,
                "selenium": True,
                "headless": True,
            },
        )
        self.assertEqual(command[:3], ["python3", "batch_workers.py", "scrape"])
        self.assertNotIn("--csv", command)

    def test_build_card_catalog_scrape_command_batched(self):
        command = build_command(
            "card_catalog_scrape",
            {
                "out": "pokemon_cards.csv",
                "mode": "fresh",
                "category_slug": "pokemon",
                "product_line_name": "pokemon",
                "product_type_name": "Cards",
                "workers": 4,
                "all_pages": True,
                "headless": True,
            },
        )
        self.assertEqual(command[:3], ["python3", "batch_workers.py", "catalog"])
        self.assertIn("--product-type-name", command)
        self.assertIn("Cards", command)
        self.assertIn("--all", command)

    def test_build_card_catalog_load_command(self):
        command = build_command(
            "card_catalog",
            {
                "db": "sealed_market.db",
                "csv": "pokemon_cards.csv",
                "category_slug": "pokemon",
                "product_line_name": "pokemon",
                "source": "TCGplayer Cards",
            },
        )
        self.assertEqual(command[:2], ["python3", "card_catalog_refresh.py"])
        self.assertIn("--csv", command)
        self.assertIn("pokemon_cards.csv", command)

    def test_build_card_details_command_batched(self):
        command = build_command(
            "card_details",
            {
                "db": "sealed_market.db",
                "source": "TCGplayer Card Details",
                "delay_min": 0.5,
                "delay_max": 1.5,
                "selenium": True,
                "headless": True,
                "workers": 4,
            },
        )
        self.assertEqual(command[:3], ["python3", "batch_workers.py", "card-details"])
        self.assertIn("--headless", command)

    def test_build_card_sales_command_batched(self):
        command = build_command(
            "card_sales",
            {
                "db": "sealed_market.db",
                "source": "TCGplayer Cards",
                "sale_date": "2026-04-01",
                "all_dates": False,
                "workers": 4,
                "limit": 0,
                "browser_fallback": True,
                "headless": True,
            },
        )
        self.assertEqual(command[:3], ["python3", "batch_workers.py", "sales"])
        self.assertIn("--target-kind", command)
        self.assertIn("cards", command)

    def test_build_card_pipeline_command(self):
        command = build_command(
            "card_pipeline",
            {
                "db": "sealed_market.db",
                "out": "pokemon_cards.csv",
                "category_slug": "pokemon",
                "product_line_name": "pokemon",
                "product_type_name": "Cards",
                "workers": 4,
                "mode": "fresh",
                "all_pages": True,
                "headless": True,
            },
        )
        self.assertEqual(command[:2], ["python3", "card_pipeline.py"])
        self.assertIn("--all", command)


if __name__ == "__main__":
    unittest.main()
