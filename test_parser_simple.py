#!/usr/bin/env python3
"""Quick test: parse the sample HTML file we have."""

from bs4 import BeautifulSoup
import json
import re

def parse_tcgplayer_test(html):
    """Test parser - extract market price, median, quantity, sellers."""
    soup = BeautifulSoup(html, "html.parser")
    
    print("\n=== PARSER DEBUG ===")
    
    # Look for market price
    mp = soup.select_one(".price-points__upper__price")
    print(f"Market price element found: {mp is not None}")
    if mp:
        print(f"  Value: {mp.get_text(strip=True)}")
    
    # Look for listed median
    lm_rows = soup.select(".price-points__lower tr")
    print(f"Price points lower rows found: {len(lm_rows)}")
    for i, row in enumerate(lm_rows[:5]):  # show first 5
        label = row.select_one(".text")
        value = row.select_one(".price-points__lower__price")
        if label and value:
            print(f"  Row {i}: {label.get_text(strip=True)} = {value.get_text(strip=True)}")
    
    # Look for current quantity
    qty_match = soup.find(string=re.compile(r"Current Quantity", re.I))
    print(f"Current Quantity text found: {qty_match is not None}")
    if qty_match:
        print(f"  Found: {qty_match}")
    
    print("=== END DEBUG ===\n")

# Test on the sample HTML file
sample_file = "/Users/jordanzhang/Workspace/pokemonmarketdata/Prismatic Evolutions Booster Bundle - SV_ Prismatic Evolutions - Pokemon - TCGplayer.com.html"

try:
    with open(sample_file, "r", encoding="utf-8") as f:
        html = f.read()
    parse_tcgplayer_test(html)
except FileNotFoundError:
    print(f"File not found: {sample_file}")
