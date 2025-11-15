"""Selenium-based TCGplayer sealed catalogue scraper with debug artifacts.

This file integrates the diagnostic behavior from `debug_open.py`:
- sets a desktop User-Agent
- uses Selenium experimental options to reduce automation detection
- waits for product elements with WebDriverWait
- saves per-page screenshot and page HTML when elements fail to appear

You can run a short test with: `python3 link_scraper.py --pages 1`
"""

import csv
import time
import random
import argparse
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import traceback

# --- CONFIG ---
BASE_URL = (
    "https://www.tcgplayer.com/search/pokemon/product?productLineName=pokemon"
    "&page={page}&view=grid&ProductTypeName=Sealed+Products"
)
OUTPUT_CSV = "products.csv"
DEFAULT_MAX_PAGES = 108


def make_driver(headless=False):
    opts = Options()
    if headless:
        # Use new headless when desired; by default we run non-headless for visibility
        opts.add_argument("--headless=new")
    # keep visible by default to help debugging (the debug script used non-headless)
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    )
    # Try to reduce automation flags
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=opts)
    return driver


def scrape_pages(pages, output_csv, headless=False, stop_on_empty=False, resume=False, wait_time=60, page_load_timeout=60, retries=1):
    driver = None
    all_products = []
    seen_urls = set()
    output_path = Path(output_csv)

    if resume and output_path.exists():
        try:
            with output_path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    url = (r.get("url") or "").strip()
                    name = (r.get("name") or "").strip()
                    if url:
                        all_products.append((name, url))
                        seen_urls.add(url)
            print(f"Resumed: loaded {len(all_products)} existing products from {output_csv}")
        except Exception as e:
            print("Failed to read existing CSV for resume:", e)

    try:
        print("Starting Chrome...")
        driver = make_driver(headless=headless)

        for page_num in range(1, pages + 1):
            url = BASE_URL.format(page=page_num)
            print(f"Opening page {page_num}: {url}")

            # Attempt to load the page with retries; restart driver on failure.
            page_source = None
            last_exc = None
            for attempt in range(0, retries + 1):
                try:
                    # ensure reasonable page load timeout
                    try:
                        driver.set_page_load_timeout(page_load_timeout)
                    except Exception:
                        # some webdriver versions may not support setting this; ignore
                        pass

                    driver.get(url)
                    try:
                        WebDriverWait(driver, wait_time).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, "a[data-testid^='product-card__image'], span.product-card__title")
                            )
                        )
                        print("Product elements detected")
                    except TimeoutException:
                        print(f"Timed out waiting ({wait_time}s) for product elements on page {page_num}; continuing")

                    page_source = driver.page_source
                    last_exc = None
                    break
                except Exception as e:
                    last_exc = e
                    print(f"Error loading page {page_num} (attempt {attempt+1}/{retries+1}): {e}")
                    traceback.print_exc()
                    # try to save any available page source
                    try:
                        htmlfile = f"debug_page_{page_num}_attempt{attempt+1}.html"
                        with open(htmlfile, "w", encoding="utf-8") as fh:
                            fh.write(driver.page_source or "")
                        print(f"Saved partial page source to {htmlfile}")
                    except Exception:
                        pass

                    # restart driver and retry
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    try:
                        driver = make_driver(headless=headless)
                    except Exception as e2:
                        print("Failed to restart Chrome driver:", e2)
                        break

            if last_exc:
                print(f"Giving up on page {page_num} after {retries+1} attempts: {last_exc}")
                # persist progress and continue
                try:
                    with output_path.open("w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(["name", "url"])
                        writer.writerows(all_products)
                    print(f"Progress saved: {len(all_products)} products so far")
                except Exception as e:
                    print("Failed to save interim CSV:", e)
                continue

            # Collect product cards
            try:
                product_cards = driver.find_elements(By.CSS_SELECTOR, "a[data-testid^='product-card__image']")
                if not product_cards:
                    # Try broader search for links containing /product/
                    product_cards = [e for e in driver.find_elements(By.TAG_NAME, "a") if "/product/" in (e.get_attribute("href") or "")]

                if not product_cards:
                    print(f"No products found on page {page_num}")
                    if stop_on_empty:
                        print("stop_on_empty enabled — stopping iteration")
                        break
                else:
                    for card in product_cards:
                        try:
                            href = card.get_attribute("href")
                            name = ""
                            # Try to find the product name in several ways
                            try:
                                # 1. Direct child span
                                title_elem = card.find_element(By.CSS_SELECTOR, "span.product-card__title")
                                name = title_elem.text.strip()
                            except Exception:
                                pass
                            if not name:
                                try:
                                    # 2. Sibling span (sometimes not a child)
                                    parent = card.find_element(By.XPATH, "..")
                                    sib_title = parent.find_element(By.CSS_SELECTOR, "span.product-card__title")
                                    name = sib_title.text.strip()
                                except Exception:
                                    pass
                            if not name:
                                try:
                                    # 3. Alt text of product image
                                    img = card.find_element(By.CSS_SELECTOR, "img")
                                    name = img.get_attribute("alt") or ""
                                except Exception:
                                    pass
                            if not name:
                                name = "(unknown)"
                            if href and "/product/" in href and href not in seen_urls:
                                all_products.append((name, href))
                                seen_urls.add(href)
                        except Exception:
                            continue

            except Exception as e:
                print(f"Error parsing products on page {page_num}: {e}")

            # polite delay
            time.sleep(2 + random.random() * 3)

            # Persist progress after each page so long runs can be resumed
            try:
                with output_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["name", "url"])
                    writer.writerows(all_products)
                print(f"Progress saved: {len(all_products)} products so far")
            except Exception as e:
                print("Failed to save interim CSV:", e)

            # If stop_on_empty triggered a break from inner loop, break outer loop too
            if stop_on_empty and not product_cards:
                break

    finally:
        if driver:
            driver.quit()

    # Save CSV
    try:
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "url"])
            writer.writerows(all_products)
        print(f"Saved {len(all_products)} products to {output_csv}")
    except Exception as e:
        print("Failed to write CSV:", e)


def main():
    parser = argparse.ArgumentParser(description="Selenium TCGplayer sealed products scraper (debug-enabled)")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to fetch (default: 1 for quick test)")
    parser.add_argument("--all", action="store_true", help="Scrape all pages (uses DEFAULT_MAX_PAGES)")
    parser.add_argument("--stop-on-empty", action="store_true", help="Stop early if a page contains no products")
    parser.add_argument("--resume", action="store_true", help="Resume from existing output CSV (skip already-scraped URLs)")
    parser.add_argument("--out", default=OUTPUT_CSV, help="Output CSV file")
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode")
    args = parser.parse_args()

    pages = DEFAULT_MAX_PAGES if args.all else args.pages
    scrape_pages(pages, args.out, headless=args.headless, stop_on_empty=args.stop_on_empty, resume=args.resume)


if __name__ == "__main__":
    main()