"""TCGplayer latest sales ingester.

This module prefers the direct JSON `latestsales` endpoint and falls back to a
browser-context fetch when the endpoint rejects plain HTTP clients.
"""

import argparse
import hashlib
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from db import (
    configure_connection as configure_db_connection,
    connect_database,
    get_dialect,
    insert_row_returning_id,
    resolve_database_target,
    sql_placeholder_list,
)
from populate_db import ensure_runtime_schema

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except Exception:  # pragma: no cover - selenium is available in the working environment
    webdriver = None
    Options = None


LATEST_SALES_URL = "https://mpapi.tcgplayer.com/v2/product/{product_id}/latestsales"
DEFAULT_MPFEV = "4961"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

TARGET_CONFIG = {
    "sealed": {
        "product_table": "products",
        "sales_table": "sales",
        "fk_column": "product_id",
        "url_like": "%tcgplayer.com/product/%",
        "default_source": "TCGplayer",
    },
    "cards": {
        "product_table": "card_products",
        "sales_table": "card_sales",
        "fk_column": "card_product_id",
        "url_like": "%tcgplayer.com/product/%",
        "default_source": "TCGplayer Cards",
    },
}


def get_target_config(target_kind):
    config = TARGET_CONFIG.get(target_kind)
    if not config:
        raise ValueError(f"unsupported_target_kind:{target_kind}")
    return config


def extract_tcgplayer_product_id(url):
    match = re.search(r"/product/(\d+)", url or "")
    if not match:
        return None
    return int(match.group(1))


def canonical_product_url(tcgplayer_product_id):
    return f"https://www.tcgplayer.com/product/{tcgplayer_product_id}/"


def normalize_text(value):
    if value is None:
        return ""
    return str(value).strip().lower()


def parse_money(value):
    if value is None or value == "":
        return None
    try:
        return round(float(value), 2)
    except Exception:
        return None


def parse_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def parse_sale_date(order_date):
    if not order_date:
        return None
    text = str(order_date).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except Exception:
        return text[:10] if len(text) >= 10 else None


def sale_fingerprint(row):
    parts = []
    custom_listing_key = normalize_text(row.get("custom_listing_key"))
    custom_listing_id = normalize_text(row.get("custom_listing_id"))
    if custom_listing_key:
        parts.append(f"custom_listing_key:{custom_listing_key}")
    if custom_listing_id and custom_listing_id != "0":
        parts.append(f"custom_listing_id:{custom_listing_id}")

    parts.extend(
        [
            f"sale_date:{normalize_text(row.get('sale_date'))}",
            f"title:{normalize_text(row.get('title'))}",
            f"condition_raw:{normalize_text(row.get('condition_raw'))}",
            f"variant:{normalize_text(row.get('variant'))}",
            f"language:{normalize_text(row.get('language'))}",
            f"quantity:{row.get('quantity') if row.get('quantity') is not None else ''}",
            f"purchase_price:{row.get('purchase_price') if row.get('purchase_price') is not None else ''}",
            f"shipping_price:{row.get('shipping_price') if row.get('shipping_price') is not None else ''}",
            f"listing_type:{normalize_text(row.get('listing_type'))}",
        ]
    )
    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()
    return digest


def make_driver(headless=True):
    if webdriver is None:
        raise RuntimeError("selenium_not_available")
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1200")
    opts.add_argument("--log-level=3")
    opts.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")
    return webdriver.Chrome(options=opts)


def fetch_latest_sales_json(product_id, product_url=None, mpfev=DEFAULT_MPFEV, use_browser_fallback=True, headless=True):
    endpoint = LATEST_SALES_URL.format(product_id=product_id)
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            endpoint,
            params={"mpfev": mpfev},
            json={"productId": product_id},
            headers=headers,
            timeout=20,
        )
        if resp.ok:
            return resp.json(), "requests"
    except Exception:
        pass

    if not use_browser_fallback:
        raise RuntimeError("latestsales_requests_failed")

    if product_url:
        page_url = product_url
    else:
        page_url = f"https://www.tcgplayer.com/product/{product_id}?view=sales-history"

    driver = make_driver(headless=headless)
    try:
        driver.set_script_timeout(20)
        driver.get(page_url)
        time.sleep(8)
        script = """
        const done = arguments[0];
        fetch(arguments[1], {
          method: 'POST',
          credentials: 'include',
          mode: 'cors',
          headers: {
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json'
          },
          body: JSON.stringify({productId: arguments[2]})
        }).then(async resp => {
          const body = await resp.text();
          done(JSON.stringify({status: resp.status, body}));
        }).catch(err => done(JSON.stringify({error: String(err)})));
        """
        raw = driver.execute_async_script(script, endpoint + f"?mpfev={mpfev}", int(product_id))
        payload = json.loads(raw)
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        if payload.get("status") != 200:
            raise RuntimeError(f"latestsales_browser_http_{payload.get('status')}")
        return json.loads(payload.get("body") or "{}"), "selenium"
    finally:
        driver.quit()


def normalize_latest_sales_payload(payload, sale_date=None):
    data = payload
    if isinstance(payload, dict):
        for key in ("data", "results", "sales", "items"):
            if isinstance(payload.get(key), list):
                data = payload[key]
                break
        else:
            data = []

    target_date = sale_date or None
    seen = set()
    rows = []
    for item in data:
        row = {
            "sale_date": parse_sale_date(item.get("orderDate") or item.get("saleDate") or item.get("date")),
            "condition_raw": item.get("condition") or item.get("conditionRaw") or item.get("condition_name"),
            "variant": item.get("variant"),
            "language": item.get("language"),
            "quantity": parse_int(item.get("quantity")),
            "purchase_price": parse_money(item.get("purchasePrice") or item.get("salePrice") or item.get("price")),
            "shipping_price": parse_money(item.get("shippingPrice") or item.get("shipping") or item.get("shippingCost")),
            "listing_type": item.get("listingType"),
            "title": item.get("title"),
            "custom_listing_key": item.get("customListingKey"),
            "custom_listing_id": item.get("customListingId"),
        }

        if target_date and row["sale_date"] != target_date:
            continue

        row["sale_fingerprint"] = sale_fingerprint(row)
        if row["sale_fingerprint"] in seen:
            continue
        seen.add(row["sale_fingerprint"])
        rows.append(row)

    return rows


def filter_product_records_for_shard(rows, shard_index=0, shard_count=1):
    if shard_count <= 1:
        return rows
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index_out_of_range")
    filtered = []
    for row in rows:
        product_id = int(row[0])
        if product_id % shard_count == shard_index:
            filtered.append(row)
    return filtered


def load_sales_targets(conn, product_id=None, product_url=None, limit=0, shard_index=0, shard_count=1, target_kind="sealed"):
    if product_id or product_url:
        internal_product_id, tcgplayer_product_id, resolved_url = resolve_product_record(
            conn,
            tcgplayer_product_id=product_id,
            product_url=product_url,
            target_kind=target_kind,
        )
        return [(internal_product_id, tcgplayer_product_id, resolved_url)]

    config = get_target_config(target_kind)
    cursor = conn.cursor()
    placeholder = "%s" if get_dialect(conn) == "postgres" else "?"
    query = f"""
        SELECT id, name, url
        FROM {config['product_table']}
        WHERE url IS NOT NULL
          AND url != ''
          AND url LIKE {placeholder}
        ORDER BY id
    """
    rows = cursor.execute(query, (config["url_like"],)).fetchall()
    rows = filter_product_records_for_shard(rows, shard_index=shard_index, shard_count=shard_count)
    if limit and limit > 0:
        rows = rows[: int(limit)]

    targets = []
    for internal_product_id, name, url in rows:
        tcgplayer_product_id = extract_tcgplayer_product_id(url)
        if tcgplayer_product_id is None:
            continue
        targets.append((int(internal_product_id), int(tcgplayer_product_id), url))
    return targets


def resolve_product_record(conn, tcgplayer_product_id=None, product_url=None, product_name=None, target_kind="sealed"):
    cursor = conn.cursor()
    dialect = get_dialect(conn)
    config = get_target_config(target_kind)
    product_table = config["product_table"]
    tcgplayer_product_id = tcgplayer_product_id or extract_tcgplayer_product_id(product_url)
    if tcgplayer_product_id is None:
        raise ValueError("tcgplayer_product_id_or_url_required")

    urls = []
    for candidate in (product_url, canonical_product_url(tcgplayer_product_id)):
        if candidate and candidate not in urls:
            urls.append(candidate)

    for candidate in urls:
        placeholder = "%s" if dialect == "postgres" else "?"
        row = cursor.execute(f"SELECT id, url FROM {product_table} WHERE url = {placeholder}", (candidate,)).fetchone()
        if row:
            return int(row[0]), int(tcgplayer_product_id), row[1]

    pattern = f"%/product/{tcgplayer_product_id}/%"
    placeholder = "%s" if dialect == "postgres" else "?"
    row = cursor.execute(
        f"SELECT id, url FROM {product_table} WHERE url LIKE {placeholder} ORDER BY id LIMIT 1",
        (pattern,),
    ).fetchone()
    if row:
        return int(row[0]), int(tcgplayer_product_id), row[1]

    name = product_name or f"TCGplayer Product {tcgplayer_product_id}"
    url = product_url or canonical_product_url(tcgplayer_product_id)
    if target_kind == "cards":
        product_id = insert_row_returning_id(
            conn,
            "card_products",
            ["tcgplayer_product_id", "name", "url", "source", "discovered_at"],
            (tcgplayer_product_id, name, url, config["default_source"], datetime.utcnow().isoformat()),
        )
    else:
        product_id = insert_row_returning_id(conn, "products", ["name", "url"], (name, url))
    return int(product_id), int(tcgplayer_product_id), url


def insert_sales_rows(conn, product_id, rows, source="TCGplayer", target_kind="sealed"):
    cursor = conn.cursor()
    inserted = 0
    placeholders = sql_placeholder_list(conn, 15)
    config = get_target_config(target_kind)
    fk_column = config["fk_column"]
    sales_table = config["sales_table"]
    for row in rows:
        cursor.execute(
            """
            INSERT INTO {sales_table} (
                {fk_column}, sale_date, condition_raw, variant, language, quantity,
                purchase_price, shipping_price, listing_type, title,
                custom_listing_key, custom_listing_id, source, sale_fingerprint, scraped_at
            ) VALUES ({placeholders})
            ON CONFLICT({fk_column}, sale_fingerprint) DO NOTHING
            """.format(sales_table=sales_table, fk_column=fk_column, placeholders=placeholders),
            (
                product_id,
                row.get("sale_date"),
                row.get("condition_raw"),
                row.get("variant"),
                row.get("language"),
                row.get("quantity"),
                row.get("purchase_price"),
                row.get("shipping_price"),
                row.get("listing_type"),
                row.get("title"),
                row.get("custom_listing_key"),
                row.get("custom_listing_id"),
                source,
                row.get("sale_fingerprint"),
                datetime.utcnow().isoformat(),
            ),
        )
        if cursor.rowcount == 1:
            inserted += 1
    return inserted


def ingest_latest_sales(conn, product_id=None, product_url=None, sale_date=None, source="TCGplayer", use_browser_fallback=True, headless=True, target_kind="sealed"):
    internal_product_id, tcgplayer_product_id, resolved_url = resolve_product_record(
        conn,
        tcgplayer_product_id=product_id,
        product_url=product_url,
        target_kind=target_kind,
    )
    payload, fetch_source = fetch_latest_sales_json(
        tcgplayer_product_id,
        product_url=resolved_url,
        use_browser_fallback=use_browser_fallback,
        headless=headless,
    )
    rows = normalize_latest_sales_payload(payload, sale_date=sale_date)
    inserted = insert_sales_rows(conn, internal_product_id, rows, source=source, target_kind=target_kind)
    return {
        "product_id": internal_product_id,
        "tcgplayer_product_id": tcgplayer_product_id,
        "fetch_source": fetch_source,
        "fetched_rows": len(rows),
        "inserted_rows": inserted,
        "sale_date": sale_date,
    }


def ingest_sales_targets(
    conn,
    targets,
    sale_date=None,
    source="TCGplayer",
    use_browser_fallback=True,
    headless=True,
    commit_every=10,
    target_kind="sealed",
):
    processed = 0
    failed = 0
    fetched_rows = 0
    inserted_rows = 0

    for index, (internal_product_id, tcgplayer_product_id, resolved_url) in enumerate(targets, start=1):
        try:
            payload, fetch_source = fetch_latest_sales_json(
                tcgplayer_product_id,
                product_url=resolved_url,
                use_browser_fallback=use_browser_fallback,
                headless=headless,
            )
            rows = normalize_latest_sales_payload(payload, sale_date=sale_date)
            inserted = insert_sales_rows(conn, internal_product_id, rows, source=source, target_kind=target_kind)
            processed += 1
            fetched_rows += len(rows)
            inserted_rows += inserted
            print(
                f"[{index}/{len(targets)}] sales product={tcgplayer_product_id} fetched={len(rows)} inserted={inserted}",
                flush=True,
            )
        except Exception as exc:
            failed += 1
            print(
                f"[{index}/{len(targets)}] sales product={tcgplayer_product_id} failed={type(exc).__name__}: {exc}",
                flush=True,
            )
        if commit_every > 0 and index % commit_every == 0:
            conn.commit()

    return {
        "products_considered": len(targets),
        "products_processed": processed,
        "products_failed": failed,
        "fetched_rows": fetched_rows,
        "inserted_rows": inserted_rows,
        "sale_date": sale_date,
    }


def yesterday_local_date():
    return (datetime.now().astimezone().date() - timedelta(days=1)).isoformat()


def main():
    parser = argparse.ArgumentParser(description="Ingest TCGplayer latest sales into SQLite")
    parser.add_argument("--db", default="sealed_market.db", help="SQLite database file")
    parser.add_argument("--product-id", type=int, help="Product id to ingest")
    parser.add_argument("--product-url", help="Product URL to resolve the product id from products table")
    parser.add_argument("--sale-date", default="", help="Only ingest sales for this YYYY-MM-DD date; defaults to the prior day in local time")
    parser.add_argument("--all-dates", action="store_true", help="Ingest all returned sales dates instead of a single-day slice")
    parser.add_argument("--source", default="TCGplayer", help="Sales source label")
    parser.add_argument("--target-kind", choices=["sealed", "cards"], default="sealed", help="Choose which product universe and sales table to target")
    parser.add_argument("--no-browser-fallback", action="store_true", help="Disable Selenium fallback when requests are rejected")
    parser.add_argument("--headless", action="store_true", help="Run Chrome headless for browser fallback")
    parser.add_argument("--snapshot-file", help="Optional JSON fixture file instead of hitting the network")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of products when running whole-universe sales refresh")
    parser.add_argument("--commit-every", type=int, default=10, help="Commit DB writes every N products")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index for parallel batch workers")
    parser.add_argument("--shard-count", type=int, default=1, help="Total shard count for parallel batch workers")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_db_connection(conn)
    ensure_runtime_schema(conn)

    if args.snapshot_file:
        payload = json.loads(Path(args.snapshot_file).read_text(encoding="utf-8"))
        target_date = None if args.all_dates else (args.sale_date or yesterday_local_date())
        rows = normalize_latest_sales_payload(payload, sale_date=target_date)
        internal_product_id, tcgplayer_product_id, _ = resolve_product_record(
            conn,
            tcgplayer_product_id=args.product_id,
            product_url=args.product_url,
            product_name=rows[0].get("title") if rows else None,
            target_kind=args.target_kind,
        )
        inserted = insert_sales_rows(conn, internal_product_id, rows, source=args.source, target_kind=args.target_kind)
        conn.commit()
        print(json.dumps({
            "product_id": internal_product_id,
            "tcgplayer_product_id": tcgplayer_product_id,
            "fetch_source": "snapshot_file",
            "fetched_rows": len(rows),
            "inserted_rows": inserted,
            "sale_date": target_date,
        }, indent=2))
        conn.close()
        return 0

    sale_date = None if args.all_dates else (args.sale_date or yesterday_local_date())
    targets = load_sales_targets(
        conn,
        product_id=args.product_id,
        product_url=args.product_url,
        limit=args.limit,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        target_kind=args.target_kind,
    )
    if args.product_id or args.product_url:
        result = ingest_latest_sales(
            conn,
            product_id=args.product_id,
            product_url=args.product_url,
            sale_date=sale_date,
            source=args.source,
            use_browser_fallback=not args.no_browser_fallback,
            headless=args.headless,
            target_kind=args.target_kind,
        )
    else:
        print(
            f"Refreshing sales for {len(targets)} product(s), sale_date={sale_date or 'all returned dates'}, shard={args.shard_index + 1}/{args.shard_count}",
            flush=True,
        )
        result = ingest_sales_targets(
            conn,
            targets=targets,
            sale_date=sale_date,
            source=args.source,
            use_browser_fallback=not args.no_browser_fallback,
            headless=args.headless,
            commit_every=args.commit_every,
            target_kind=args.target_kind,
        )
    conn.commit()
    conn.close()
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
