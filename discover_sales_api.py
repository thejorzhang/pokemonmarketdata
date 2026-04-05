"""Discover TCGplayer sales-history requests for a product page.

Usage:
    python3 discover_sales_api.py --url 'https://www.tcgplayer.com/product/...'
    python3 discover_sales_api.py --url 'https://www.tcgplayer.com/product/593294/pokemon-sv-prismatic-evolutions-prismatic-evolutions-booster-pack?page=1'
"""

import argparse
import json
import time
from collections import OrderedDict

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


CLICK_PATTERNS = [
    "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view more sales')]",
    "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view more data')]",
    "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sales history')]",
    "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'latest sales')]",
    "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view sales')]",
]

KEYWORDS = [
    "sales",
    "history",
    "priceguide",
    "price-guide",
    "latestsales",
    "recentsales",
    "getlatestsales",
    "getmoresaleshistory",
]


def make_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1200")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    return webdriver.Chrome(options=opts)


def click_sales_controls(driver):
    clicked = []
    for xpath in CLICK_PATTERNS:
        elements = driver.find_elements(By.XPATH, xpath)
        for element in elements[:3]:
            try:
                text = (element.text or "").strip()
                if not text or not element.is_displayed():
                    continue
                driver.execute_script("arguments[0].click();", element)
                clicked.append(text)
                time.sleep(2)
            except Exception:
                continue
    return clicked


def collect_network_entries(driver):
    entries = OrderedDict()
    for raw_entry in driver.get_log("performance"):
        try:
            message = json.loads(raw_entry["message"])["message"]
        except Exception:
            continue

        if message.get("method") != "Network.responseReceived":
            continue

        response = message.get("params", {}).get("response", {})
        url = response.get("url", "")
        lower = url.lower()
        if not any(keyword in lower for keyword in KEYWORDS):
            continue

        entries[url] = {
            "status": response.get("status"),
            "mime_type": response.get("mimeType"),
            "url": url,
        }
    return list(entries.values())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="TCGplayer product URL")
    parser.add_argument("--wait-seconds", type=float, default=8.0, help="Initial wait after page load")
    args = parser.parse_args()

    driver = make_driver()
    try:
        driver.get(args.url)
        time.sleep(args.wait_seconds)
        clicked = click_sales_controls(driver)
        network_entries = collect_network_entries(driver)

        payload = {
            "url": args.url,
            "title": driver.title,
            "current_url": driver.current_url,
            "clicked": clicked,
            "network_entries": network_entries,
        }
        print(json.dumps(payload, indent=2))
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
