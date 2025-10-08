# tcgplayer_sealed_selenium.py
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

# --- CONFIG ---
START_URL = "https://www.tcgplayer.com/search/pokemon/product?productLineName=pokemon&page=1&view=grid&ProductTypeName=Sealed+Products"
OUTPUT_CSV = "products.csv"
MAX_PAGES = 108  # Adjust if needed

# --- Set up headless Chrome ---
chrome_options = Options()
#chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--log-level=3")

driver = webdriver.Chrome(options=chrome_options)
print("✅ ChromeDriver started successfully")

# --- MAIN LOOP ---
all_products = []

for page_num in range(1, MAX_PAGES + 1):
    url = START_URL.replace("page=1", f"page={page_num}")
    print(f"Fetching page {page_num}: {url}")
    driver.get(url)

    # Wait for products to load
    time.sleep(10 + random.random())  # adjust if slow
    print(f"✅ Page {page_num} loaded")


    # Find all product cards
    try:
        product_cards = driver.find_elements(By.CSS_SELECTOR, "a[data-testid^='product-card__image']")
        if not product_cards:
            print("No products found, assuming end of catalogue.")
            break

        for card in product_cards:
            try:
                name_elem = card.find_element(By.CSS_SELECTOR, "span.product-card__title")
                name = name_elem.text.strip()
                link = card.get_attribute("href")
                all_products.append({"name": name, "url": link})
            except NoSuchElementException:
                continue

    except Exception as e:
        print(f"Error on page {page_num}: {e}")
        break

    # Random delay between pages/-
    time.sleep(2 + random.random())

# --- Save to CSV ---
df = pd.DataFrame(all_products)
df.to_csv(OUTPUT_CSV, index=False)
print(f"Saved {len(all_products)} products to {OUTPUT_CSV}")

driver.quit()
