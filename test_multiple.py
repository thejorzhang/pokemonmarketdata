#!/usr/bin/env python3
"""Test parser on multiple sample products"""
import subprocess
import sys

# Test on first 5 products
result = subprocess.run([
    "python3",
    "/Users/jordanzhang/Workspace/pokemonmarketdata/populate_db.py",
    "--limit", "5",
    "--selenium",
    "--headless",
    "--delay-min", "1.0",
    "--delay-max", "2.0"
], cwd="/Users/jordanzhang/Workspace/pokemonmarketdata")

sys.exit(result.returncode)
