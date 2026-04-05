import unittest
from pathlib import Path

from populate_db import has_minimum_parse_data, page_looks_like_tcgplayer_shell, parse_tcgplayer


FIXTURES = Path(__file__).parent / "fixtures"


class TestParseTcgplayer(unittest.TestCase):
    def fixture(self, name):
        return (FIXTURES / name).read_text(encoding="utf-8")

    def test_jsonld_price_and_listing_count(self):
        parsed = parse_tcgplayer(self.fixture("tcgplayer_jsonld.html"))
        self.assertEqual(parsed["listing_count"], 29)
        self.assertEqual(parsed["lowest_price"], 199.95)
        self.assertEqual(parsed["lowest_shipping"], 0.99)
        self.assertEqual(parsed["lowest_total_price"], 200.94)
        self.assertEqual(parsed["set_name"], "Scarlet & Violet")
        self.assertTrue(has_minimum_parse_data(parsed))

    def test_priceguide_fields_and_fallback_lowest(self):
        parsed = parse_tcgplayer(self.fixture("tcgplayer_priceguide.html"))
        self.assertEqual(parsed["market_price"], 149.99)
        self.assertEqual(parsed["listed_median"], 155.50)
        self.assertEqual(parsed["current_quantity"], 42)
        self.assertEqual(parsed["current_sellers"], 12)
        self.assertEqual(parsed["lowest_price"], 145.00)
        self.assertEqual(parsed["lowest_shipping"], 4.99)
        self.assertEqual(parsed["lowest_total_price"], 149.99)
        self.assertTrue(has_minimum_parse_data(parsed))

    def test_parse_gate_rejects_empty_html(self):
        parsed = parse_tcgplayer("<html><body>No market data here</body></html>")
        self.assertFalse(has_minimum_parse_data(parsed))

    def test_shell_page_detector_flags_generic_tcgplayer_shell(self):
        class FakeDriver:
            title = "Your Trusted Marketplace for Collectible Trading Card Games - TCGplayer"
            page_source = "<html><body><div>loading</div></body></html>"

        self.assertTrue(page_looks_like_tcgplayer_shell(FakeDriver()))


if __name__ == "__main__":
    unittest.main()
