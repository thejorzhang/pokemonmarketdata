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
                "db": "sealed_market.db",
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
        self.assertIn("--target-kind", command)
        self.assertIn("cards", command)
        self.assertIn("--product-type-name", command)
        self.assertIn("Cards", command)
        self.assertIn("--all", command)

    def test_build_card_catalog_scrape_command_single_worker(self):
        command = build_command(
            "card_catalog_scrape",
            {
                "db": "sealed_market.db",
                "mode": "fresh",
                "category_slug": "pokemon",
                "product_line_name": "pokemon",
                "product_type_name": "Cards",
                "workers": 1,
            },
        )
        self.assertEqual(command[:2], ["python3", "card_catalog_refresh.py"])
        self.assertIn("--scrape", command)

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
                "session_file": "/tmp/tcgplayer_session.json",
                "browser_fallback": True,
                "headless": True,
            },
        )
        self.assertEqual(command[:3], ["python3", "batch_workers.py", "sales"])
        self.assertIn("--target-kind", command)
        self.assertIn("cards", command)
        self.assertIn("--session-file", command)

    def test_build_sales_command_batched_with_session_file(self):
        command = build_command(
            "sales",
            {
                "db": "sealed_market.db",
                "source": "TCGplayer",
                "sale_date": "2026-04-01",
                "all_dates": False,
                "workers": 4,
                "limit": 0,
                "session_file": "/tmp/tcgplayer_session.json",
                "browser_fallback": True,
                "headless": True,
            },
        )
        self.assertEqual(command[:3], ["python3", "batch_workers.py", "sales"])
        self.assertIn("--session-file", command)

    def test_build_sales_command_batched_with_session_file_and_no_browser_fallback(self):
        command = build_command(
            "sales",
            {
                "db": "sealed_market.db",
                "source": "TCGplayer",
                "all_dates": True,
                "workers": 4,
                "limit": 0,
                "session_file": "/tmp/tcgplayer_session.json",
                "browser_fallback": False,
                "headless": True,
            },
        )
        self.assertEqual(command[:3], ["python3", "batch_workers.py", "sales"])
        self.assertIn("--session-file", command)
        self.assertIn("--no-browser-fallback", command)

    def test_build_card_pipeline_command(self):
        command = build_command(
            "card_pipeline",
            {
                "db": "sealed_market.db",
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
