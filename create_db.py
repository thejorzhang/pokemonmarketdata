
# create_db.py
# This script creates a SQLite database for sealed product market tracking

import sqlite3

# Connect to (or create) the database file
conn = sqlite3.connect("sealed_market.db")
c = conn.cursor()

# Create a table for product information
c.execute("""
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    url TEXT,
    release_date TEXT,
    sku_code TEXT
);
""")

# Create a table for daily/periodic listings
c.execute("""
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    listing_count INTEGER,
    lowest_price REAL,
    median_price REAL,
    condition TEXT,
    source TEXT,
    FOREIGN KEY (product_id) REFERENCES products (id)
);
""")

# Confirm and close
conn.commit()
conn.close()

print("Database created successfully: sealed_market.db")

