import unittest
from argparse import Namespace

from batch_workers import (
    build_card_details_worker_command,
    build_catalog_worker_command,
    build_product_details_worker_command,
    build_sales_worker_command,
    build_scrape_worker_command,
    plan_worker_commands,
)


class TestBatchWorkers(unittest.TestCase):
    def test_build_scrape_worker_command(self):
        args = Namespace(
            db="sealed_market.db",
            csv="",
            source="TCGplayer",
            snapshot_date="2026-04-01",
            commit_every=25,
            delay_min=2.0,
            delay_max=5.0,
            request_timeout=12.0,
            max_retries=3,
            retry_backoff=1.25,
            diagnostics_dir="diagnostics",
            no_selenium=False,
            headless=True,
            debug=False,
            set_id=0,
            set_name="Journey Together",
        )
        command = build_scrape_worker_command(args, shard_index=1, shard_count=4)
        self.assertIn("populate_db.py", command)
        self.assertIn("--headless", command)
        self.assertIn("--shard-index", command)
        self.assertIn("--shard-count", command)
        self.assertIn("--set-name", command)
        self.assertNotIn("--csv", command)

    def test_build_product_details_worker_command(self):
        args = Namespace(
            db="sealed_market.db",
            source="TCGplayer Product Details",
            request_timeout=12.0,
            max_retries=3,
            retry_backoff=1.25,
            delay_min=0.5,
            delay_max=1.5,
            no_selenium=True,
            headless=True,
            set_id=0,
            set_name="Journey Together",
        )
        command = build_product_details_worker_command(args, shard_index=0, shard_count=3)
        self.assertIn("product_details_refresh.py", command)
        self.assertIn("--no-selenium", command)
        self.assertIn("--headless", command)
        self.assertIn("--set-name", command)

    def test_build_sales_worker_command(self):
        args = Namespace(
            db="sealed_market.db",
            source="TCGplayer",
            target_kind="sealed",
            set_id=12,
            set_name="",
            product_id=0,
            product_url="",
            sale_date="2026-03-31",
            all_dates=False,
            snapshot_file="",
            session_file="/tmp/tcgplayer_session.json",
            limit=0,
            commit_every=10,
            no_browser_fallback=False,
            headless=True,
        )
        command = build_sales_worker_command(args, shard_index=2, shard_count=4)
        self.assertIn("sales_ingester.py", command)
        self.assertIn("--sale-date", command)
        self.assertIn("--shard-index", command)
        self.assertIn("--shard-count", command)
        self.assertIn("--target-kind", command)
        self.assertIn("--set-id", command)
        self.assertIn("--session-file", command)

    def test_build_card_details_worker_command(self):
        args = Namespace(
            db="sealed_market.db",
            source="TCGplayer Card Details",
            request_timeout=12.0,
            max_retries=3,
            retry_backoff=1.25,
            delay_min=0.5,
            delay_max=1.5,
            no_selenium=False,
            headless=True,
            set_id=5,
            set_name="",
        )
        command = build_card_details_worker_command(args, shard_index=1, shard_count=4)
        self.assertIn("card_details_refresh.py", command)
        self.assertIn("--headless", command)
        self.assertIn("--shard-index", command)
        self.assertIn("--set-id", command)

    def test_build_catalog_worker_command(self):
        args = Namespace(
            db="sealed_market.db",
            target_kind="cards",
            mode="fresh",
            scrape_date="2026-04-05",
            pages=3,
            all=False,
            wait_time=20,
            page_load_timeout=25,
            retries=1,
            category_slug="pokemon",
            product_line_name="pokemon",
            product_type_name="Cards",
            headless=True,
        )
        command = build_catalog_worker_command(args, shard_index=0, shard_count=4, refresh_token="token-123")
        self.assertIn("card_catalog_refresh.py", command)
        self.assertIn("--scrape", command)
        self.assertIn("--refresh-token", command)
        self.assertIn("--category-slug", command)
        self.assertIn("Cards", command)

    def test_plan_worker_commands(self):
        args = Namespace(
            db="sealed_market.db",
            csv="",
            source="TCGplayer",
            snapshot_date="2026-04-01",
            commit_every=25,
            delay_min=2.0,
            delay_max=5.0,
            request_timeout=12.0,
            max_retries=3,
            retry_backoff=1.25,
            diagnostics_dir="diagnostics",
            no_selenium=False,
            headless=False,
            debug=False,
        )
        commands = plan_worker_commands("scrape", args, 3)
        self.assertEqual(len(commands), 3)
        self.assertNotEqual(commands[0], commands[1])


if __name__ == "__main__":
    unittest.main()
