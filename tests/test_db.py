import unittest

from db import is_postgres_target, is_sqlite_target, resolve_database_target


class TestDbHelpers(unittest.TestCase):
    def test_target_detection(self):
        self.assertTrue(is_postgres_target("postgresql://user:pass@localhost/db"))
        self.assertFalse(is_postgres_target("sealed_market.db"))
        self.assertTrue(is_sqlite_target("sealed_market.db"))

    def test_resolve_database_target_prefers_explicit(self):
        self.assertEqual(resolve_database_target("sealed_market.db"), "sealed_market.db")


if __name__ == "__main__":
    unittest.main()
