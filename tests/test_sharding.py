import unittest

from populate_db import filter_rows_for_shard
from product_details_refresh import filter_products_for_shard


class TestSharding(unittest.TestCase):
    def test_populate_db_sharding_partitions_without_loss(self):
        rows = [
            {"name": "A", "url": "https://example.com/a"},
            {"name": "B", "url": "https://example.com/b"},
            {"name": "C", "url": "https://example.com/c"},
            {"name": "D", "url": "https://example.com/d"},
            {"name": "E", "url": "https://example.com/e"},
        ]
        shards = [filter_rows_for_shard(rows, shard_index=i, shard_count=3) for i in range(3)]
        combined_urls = [row["url"] for shard in shards for row in shard]
        self.assertEqual(sorted(combined_urls), sorted(row["url"] for row in rows))
        self.assertEqual(len(combined_urls), len(set(combined_urls)))

    def test_product_details_sharding_partitions_without_loss(self):
        rows = [
            (1, "A", "https://example.com/a"),
            (2, "B", "https://example.com/b"),
            (3, "C", "https://example.com/c"),
            (4, "D", "https://example.com/d"),
            (5, "E", "https://example.com/e"),
        ]
        shards = [filter_products_for_shard(rows, shard_index=i, shard_count=2) for i in range(2)]
        combined_ids = [row[0] for shard in shards for row in shard]
        self.assertEqual(sorted(combined_ids), [1, 2, 3, 4, 5])
        self.assertEqual(len(combined_ids), len(set(combined_ids)))

    def test_invalid_shard_index_raises(self):
        with self.assertRaises(ValueError):
            filter_rows_for_shard([{"url": "https://example.com/a"}], shard_index=2, shard_count=2)
        with self.assertRaises(ValueError):
            filter_products_for_shard([(1, "A", "https://example.com/a")], shard_index=-1, shard_count=2)


if __name__ == "__main__":
    unittest.main()
