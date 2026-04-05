import unittest

from link_scraper import build_search_url, filter_pages_for_shard


class TestLinkScraper(unittest.TestCase):
    def test_build_search_url_uses_configurable_catalog_params(self):
        url = build_search_url(
            3,
            category_slug="magic",
            product_line_name="magic",
            product_type_name="Sealed Products",
        )
        self.assertIn("/search/magic/product?", url)
        self.assertIn("productLineName=magic", url)
        self.assertIn("page=3", url)
        self.assertIn("ProductTypeName=Sealed+Products", url)

    def test_filter_pages_for_shard(self):
        self.assertEqual(filter_pages_for_shard(6, shard_index=0, shard_count=2), [1, 3, 5])
        self.assertEqual(filter_pages_for_shard(6, shard_index=1, shard_count=2), [2, 4, 6])


if __name__ == "__main__":
    unittest.main()
