#!/usr/bin/env python3
"""Quick test of parse_tcgplayer on a saved HTML file."""

import sys
from populate_db import parse_tcgplayer

if len(sys.argv) < 2:
    print("Usage: python3 test_parser.py <html_file>")
    sys.exit(1)

html_file = sys.argv[1]
with open(html_file, "r", encoding="utf-8") as f:
    html = f.read()

print(f"Testing parser on: {html_file}")
print("=" * 70)

result = parse_tcgplayer(html)

print("\nParsed Results:")
print(f"  listing_count:     {result.get('listing_count')}")
print(f"  lowest_price:      {result.get('lowest_price')}")
print(f"  market_price:      {result.get('market_price')}")
print(f"  listed_median:     {result.get('listed_median')}")
print(f"  current_quantity:  {result.get('current_quantity')}")
print(f"  current_sellers:   {result.get('current_sellers')}")
print("=" * 70)
