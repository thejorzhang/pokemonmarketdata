"""TCGplayer aggregated price-history ingester.

This ingests the aggregated price history buckets exposed by the infinite-api
price/history endpoints into dedicated tables without disturbing raw sales.
"""

import argparse
import hashlib
import json
import re
import time
from datetime import datetime

import requests

from db import (
    configure_connection as configure_db_connection,
    connect_database,
    get_dialect,
    resolve_database_target,
    sql_placeholder_list,
)
from populate_db import ensure_runtime_schema
from sales_ingester import (
    canonical_product_url,
    extract_tcgplayer_product_id,
    make_driver,
    prepare_profile_clone,
    resolve_product_record,
)


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
DEFAULT_SOURCE = "TCGplayer Price History"

TARGET_CONFIG = {
    "sealed": {
        "product_table": "products",
        "history_table": "price_history",
        "fk_column": "product_id",
        "url_like": "%tcgplayer.com/product/%",
        "default_source": DEFAULT_SOURCE,
    },
    "cards": {
        "product_table": "card_products",
        "history_table": "card_price_history",
        "fk_column": "card_product_id",
        "url_like": "%tcgplayer.com/product/%",
        "default_source": DEFAULT_SOURCE,
    },
}


def get_target_config(target_kind):
    config = TARGET_CONFIG.get(target_kind)
    if not config:
        raise ValueError(f"unsupported_target_kind:{target_kind}")
    return config


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


def parse_history_date(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except Exception:
        return text[:10] if len(text) >= 10 else text


def history_fingerprint(product_id, endpoint_kind, history_range, row):
    parts = [
        f"product_id:{int(product_id)}",
        f"endpoint_kind:{normalize_text(endpoint_kind)}",
        f"history_range:{normalize_text(history_range)}",
        f"bucket_start_date:{normalize_text(row.get('bucket_start_date'))}",
        f"bucket_end_date:{normalize_text(row.get('bucket_end_date'))}",
        f"bucket_index:{row.get('bucket_index') if row.get('bucket_index') is not None else ''}",
        f"bucket_label:{normalize_text(row.get('bucket_label'))}",
        f"market_price:{row.get('market_price') if row.get('market_price') is not None else ''}",
        f"quantity_sold:{row.get('quantity_sold') if row.get('quantity_sold') is not None else ''}",
        f"transaction_count:{row.get('transaction_count') if row.get('transaction_count') is not None else ''}",
        f"low_sale_price:{row.get('low_sale_price') if row.get('low_sale_price') is not None else ''}",
        f"low_sale_price_with_shipping:{row.get('low_sale_price_with_shipping') if row.get('low_sale_price_with_shipping') is not None else ''}",
        f"high_sale_price:{row.get('high_sale_price') if row.get('high_sale_price') is not None else ''}",
        f"high_sale_price_with_shipping:{row.get('high_sale_price_with_shipping') if row.get('high_sale_price_with_shipping') is not None else ''}",
        f"avg_sale_price:{row.get('avg_sale_price') if row.get('avg_sale_price') is not None else ''}",
        f"avg_sale_price_with_shipping:{row.get('avg_sale_price_with_shipping') if row.get('avg_sale_price_with_shipping') is not None else ''}",
        f"total_sale_value:{row.get('total_sale_value') if row.get('total_sale_value') is not None else ''}",
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def make_history_endpoint(product_id, endpoint_kind, history_range, page=None):
    endpoint = f"https://infinite-api.tcgplayer.com/price/history/{int(product_id)}"
    if endpoint_kind == "detailed":
        endpoint += "/detailed"
    endpoint += f"?range={history_range}"
    if page not in (None, "", 0):
        endpoint += f"&page={page}"
    return endpoint


def _next_page_value(payload):
    if not isinstance(payload, dict):
        return None
    value = payload.get("nextPage")
    if value in (None, "", 0, "0", False):
        return None
    return value


def fetch_history_json(
    product_id,
    endpoint_kind="summary",
    history_range="quarter",
    product_url=None,
    use_browser_fallback=True,
    headless=True,
    user_data_dir=None,
    profile_directory=None,
    page=None,
    endpoint_override=None,
):
    endpoint = endpoint_override or make_history_endpoint(product_id, endpoint_kind, history_range, page=page)
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.tcgplayer.com",
        "Referer": product_url or canonical_product_url(product_id),
    }
    try:
        resp = requests.get(endpoint, headers=headers, timeout=20)
        if resp.ok:
            return resp.json(), "requests"
    except Exception:
        pass

    if not use_browser_fallback:
        raise RuntimeError(f"price_history_requests_failed:{endpoint_kind}:{history_range}")

    page_url = product_url or f"https://www.tcgplayer.com/product/{product_id}?view=sales-history"
    runtime_user_data_dir = user_data_dir
    runtime_profile_directory = profile_directory
    clone_root = None
    if user_data_dir:
        runtime_user_data_dir, runtime_profile_directory = prepare_profile_clone(
            user_data_dir,
            profile_directory=profile_directory,
        )
        clone_root = runtime_user_data_dir

    driver = make_driver(
        headless=headless,
        user_data_dir=runtime_user_data_dir,
        profile_directory=runtime_profile_directory,
    )
    try:
        driver.set_script_timeout(60 if user_data_dir else 20)
        driver.set_page_load_timeout(40)
        driver.get(page_url)
        time.sleep(8)
        script = """
        const done = arguments[0];
        fetch(arguments[1], {
          method: 'GET',
          credentials: 'include',
          mode: 'cors',
          headers: {
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json'
          }
        }).then(async resp => {
          const body = await resp.text();
          done(JSON.stringify({status: resp.status, body}));
        }).catch(err => done(JSON.stringify({error: String(err)})));
        """
        raw = driver.execute_async_script(script, endpoint)
        payload = json.loads(raw)
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        if payload.get("status") != 200:
            raise RuntimeError(f"price_history_browser_http_{payload.get('status')}")
        return json.loads(payload.get("body") or "{}"), "selenium"
    finally:
        driver.quit()
        if clone_root:
            import shutil

            shutil.rmtree(clone_root, ignore_errors=True)


def extract_history_buckets(payload):
    keys_of_interest = {
        "bucketstartdate",
        "bucket_start_date",
        "startdate",
        "start_date",
        "bucketenddate",
        "bucket_end_date",
        "enddate",
        "end_date",
        "marketprice",
        "market_price",
        "quantitysold",
        "quantity_sold",
        "transactioncount",
        "transaction_count",
        "lowsaleprice",
        "low_sale_price",
        "lowsalepricewithshipping",
        "low_sale_price_with_shipping",
        "highsaleprice",
        "high_sale_price",
        "highsalepricewithshipping",
        "high_sale_price_with_shipping",
        "avgsaleprice",
        "avg_sale_price",
        "avgsalepricewithshipping",
        "avg_sale_price_with_shipping",
    }
    seen = set()
    buckets = []

    def looks_like_bucket(node):
        if not isinstance(node, dict):
            return False
        normalized_keys = {re.sub(r"[^a-z0-9]", "", str(key).lower()) for key in node.keys()}
        has_date = any(key in normalized_keys for key in ("bucketstartdate", "startdate", "date", "periodstartdate"))
        has_metric = any(key in normalized_keys for key in keys_of_interest)
        return has_date and has_metric

    def visit(node):
        if isinstance(node, dict):
            if looks_like_bucket(node):
                marker = json.dumps(node, sort_keys=True, default=str)
                if marker not in seen:
                    seen.add(marker)
                    buckets.append(node)
            for value in node.values():
                if isinstance(value, (dict, list)):
                    visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(payload)
    return buckets


def merge_history_payloads(payloads):
    combined = []
    total_results = 0
    result_count = 0
    fetch_source = None
    for payload, source in payloads:
        fetch_source = fetch_source or source
        if isinstance(payload, dict):
            combined.extend(extract_history_buckets(payload))
            total_results = max(total_results, int(payload.get("totalResults") or 0))
            result_count += int(payload.get("resultCount") or 0)
        elif isinstance(payload, list):
            combined.extend([item for item in payload if isinstance(item, dict)])
            result_count += len(payload)
    if not result_count:
        result_count = len(combined)
    return {
        "previousPage": "",
        "nextPage": "",
        "resultCount": result_count,
        "totalResults": total_results or result_count,
        "data": combined,
    }, fetch_source or "unknown"


def fetch_all_history_json(
    product_id,
    endpoint_kind="summary",
    history_range="quarter",
    product_url=None,
    use_browser_fallback=True,
    headless=True,
    user_data_dir=None,
    profile_directory=None,
    max_pages=20,
):
    payloads = []
    seen_pages = set()
    seen_urls = set()
    page = None
    endpoint_override = None

    for _ in range(max_pages):
        payload, fetch_source = fetch_history_json(
            product_id,
            endpoint_kind=endpoint_kind,
            history_range=history_range,
            product_url=product_url,
            use_browser_fallback=use_browser_fallback,
            headless=headless,
            user_data_dir=user_data_dir,
            profile_directory=profile_directory,
            page=page,
            endpoint_override=endpoint_override,
        )
        payloads.append((payload, fetch_source))
        next_page = _next_page_value(payload)
        if not next_page:
            break
        if isinstance(next_page, str) and next_page.startswith("http"):
            if next_page in seen_urls:
                break
            seen_urls.add(next_page)
            endpoint_override = next_page
            page = None
            continue
        page_key = str(next_page)
        if page_key in seen_pages:
            break
        seen_pages.add(page_key)
        endpoint_override = None
        page = next_page

    return merge_history_payloads(payloads)


def normalize_history_payload(payload, endpoint_kind="summary", history_range="quarter"):
    data = payload
    if isinstance(payload, dict):
        for key in ("data", "results", "buckets", "history", "items", "rows", "points"):
            if isinstance(payload.get(key), list):
                data = payload[key]
                break
        else:
            data = extract_history_buckets(payload)

    rows = []
    seen = set()
    for index, item in enumerate(data or []):
        if not isinstance(item, dict):
            continue
        bucket_start_date = parse_history_date(
            item.get("bucketStartDate")
            or item.get("bucket_start_date")
            or item.get("startDate")
            or item.get("start_date")
            or item.get("date")
            or item.get("bucketDate")
            or item.get("periodStartDate")
        ) or f"bucket-{index}"
        bucket_end_date = parse_history_date(
            item.get("bucketEndDate")
            or item.get("bucket_end_date")
            or item.get("endDate")
            or item.get("end_date")
            or item.get("periodEndDate")
        )
        row = {
            "endpoint_kind": endpoint_kind,
            "history_range": history_range,
            "bucket_index": parse_int(item.get("bucketIndex") or item.get("bucket_index") or item.get("index") or index),
            "bucket_start_date": bucket_start_date,
            "bucket_end_date": bucket_end_date,
            "bucket_label": item.get("bucketLabel") or item.get("bucket_label") or item.get("label") or item.get("name") or item.get("period"),
            "market_price": parse_money(item.get("marketPrice") or item.get("market_price")),
            "quantity_sold": parse_int(item.get("quantitySold") or item.get("quantity_sold")),
            "transaction_count": parse_int(item.get("transactionCount") or item.get("transaction_count")),
            "low_sale_price": parse_money(item.get("lowSalePrice") or item.get("low_sale_price")),
            "low_sale_price_with_shipping": parse_money(item.get("lowSalePriceWithShipping") or item.get("low_sale_price_with_shipping")),
            "high_sale_price": parse_money(item.get("highSalePrice") or item.get("high_sale_price")),
            "high_sale_price_with_shipping": parse_money(item.get("highSalePriceWithShipping") or item.get("high_sale_price_with_shipping")),
            "avg_sale_price": parse_money(item.get("avgSalePrice") or item.get("averageSalePrice") or item.get("avg_sale_price")),
            "avg_sale_price_with_shipping": parse_money(item.get("avgSalePriceWithShipping") or item.get("averageSalePriceWithShipping") or item.get("avg_sale_price_with_shipping")),
            "total_sale_value": parse_money(item.get("totalSaleValue") or item.get("totalSalesValue") or item.get("salesValue") or item.get("total_sale_value")),
            "bucket_json": json.dumps(item, sort_keys=True, default=str),
        }
        marker = hashlib.sha1(
            "|".join(
                [
                    f"endpoint_kind:{normalize_text(endpoint_kind)}",
                    f"history_range:{normalize_text(history_range)}",
                    f"bucket_start_date:{normalize_text(row.get('bucket_start_date'))}",
                    f"bucket_end_date:{normalize_text(row.get('bucket_end_date'))}",
                    f"bucket_index:{row.get('bucket_index') if row.get('bucket_index') is not None else ''}",
                    f"bucket_label:{normalize_text(row.get('bucket_label'))}",
                    f"bucket_json:{row.get('bucket_json')}",
                ]
            ).encode("utf-8")
        ).hexdigest()
        if marker in seen:
            continue
        seen.add(marker)
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


def load_history_targets(
    conn,
    product_id=None,
    product_url=None,
    limit=0,
    shard_index=0,
    shard_count=1,
    target_kind="sealed",
    set_id=None,
    set_name=None,
):
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
    """
    params = [config["url_like"]]
    if target_kind == "sealed" and set_id:
        query += f" AND id IN (SELECT product_id FROM product_details WHERE set_id = {placeholder})"
        params.append(int(set_id))
    elif target_kind == "sealed" and set_name:
        query += (
            " AND id IN (SELECT d.product_id FROM product_details d "
            "JOIN sets s ON s.id = d.set_id WHERE s.name = {placeholder})"
        ).format(placeholder=placeholder)
        params.append(set_name)
    elif target_kind == "cards" and set_id:
        query += f" AND set_id = {placeholder}"
        params.append(int(set_id))
    elif target_kind == "cards" and set_name:
        query += f" AND set_id IN (SELECT id FROM sets WHERE name = {placeholder})"
        params.append(set_name)
    query += " ORDER BY id"
    rows = cursor.execute(query, tuple(params)).fetchall()
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


def insert_history_rows(conn, product_id, rows, endpoint_kind="summary", history_range="quarter", source=DEFAULT_SOURCE, target_kind="sealed"):
    cursor = conn.cursor()
    inserted = 0
    config = get_target_config(target_kind)
    fk_column = config["fk_column"]
    history_table = config["history_table"]
    placeholders = sql_placeholder_list(conn, 21)
    for index, row in enumerate(rows):
        row = dict(row)
        if not row.get("history_fingerprint"):
            row["history_fingerprint"] = history_fingerprint(product_id, endpoint_kind, history_range, row)
        cursor.execute(
            """
            INSERT INTO {history_table} (
                {fk_column}, endpoint_kind, history_range, bucket_index,
                bucket_start_date, bucket_end_date, bucket_label,
                market_price, quantity_sold, transaction_count,
                low_sale_price, low_sale_price_with_shipping,
                high_sale_price, high_sale_price_with_shipping,
                avg_sale_price, avg_sale_price_with_shipping,
                total_sale_value, source, history_fingerprint,
                bucket_json, scraped_at
            ) VALUES ({placeholders})
            ON CONFLICT({fk_column}, history_fingerprint) DO NOTHING
            """.format(history_table=history_table, fk_column=fk_column, placeholders=placeholders),
            (
                product_id,
                endpoint_kind,
                history_range,
                row.get("bucket_index", index),
                row.get("bucket_start_date"),
                row.get("bucket_end_date"),
                row.get("bucket_label"),
                row.get("market_price"),
                row.get("quantity_sold"),
                row.get("transaction_count"),
                row.get("low_sale_price"),
                row.get("low_sale_price_with_shipping"),
                row.get("high_sale_price"),
                row.get("high_sale_price_with_shipping"),
                row.get("avg_sale_price"),
                row.get("avg_sale_price_with_shipping"),
                row.get("total_sale_value"),
                source,
                row.get("history_fingerprint"),
                row.get("bucket_json"),
                datetime.utcnow().isoformat(),
            ),
        )
        if cursor.rowcount == 1:
            inserted += 1
    return inserted


def ingest_history_target(
    conn,
    product_id=None,
    product_url=None,
    source=DEFAULT_SOURCE,
    target_kind="sealed",
    history_ranges=None,
    endpoint_kind="both",
    use_browser_fallback=True,
    headless=True,
    user_data_dir=None,
    profile_directory=None,
):
    internal_product_id, tcgplayer_product_id, resolved_url = resolve_product_record(
        conn,
        tcgplayer_product_id=product_id,
        product_url=product_url,
        target_kind=target_kind,
    )
    history_ranges = list(history_ranges or ["quarter", "annual"])
    endpoint_kinds = ["summary", "detailed"] if endpoint_kind == "both" else [endpoint_kind]
    fetched_rows = 0
    inserted_rows = 0
    for current_range in history_ranges:
        for current_kind in endpoint_kinds:
            payload, fetch_source = fetch_all_history_json(
                tcgplayer_product_id,
                endpoint_kind=current_kind,
                history_range=current_range,
                product_url=resolved_url,
                use_browser_fallback=use_browser_fallback,
                headless=headless,
                user_data_dir=user_data_dir,
                profile_directory=profile_directory,
            )
            rows = normalize_history_payload(payload, endpoint_kind=current_kind, history_range=current_range)
            inserted = insert_history_rows(
                conn,
                internal_product_id,
                rows,
                endpoint_kind=current_kind,
                history_range=current_range,
                source=source,
                target_kind=target_kind,
            )
            fetched_rows += len(rows)
            inserted_rows += inserted
            print(
                f"history product={tcgplayer_product_id} range={current_range} kind={current_kind} "
                f"fetch_source={fetch_source} fetched={len(rows)} inserted={inserted}",
                flush=True,
            )
    return {
        "product_id": internal_product_id,
        "tcgplayer_product_id": tcgplayer_product_id,
        "fetched_rows": fetched_rows,
        "inserted_rows": inserted_rows,
        "endpoint_kind": endpoint_kind,
        "history_ranges": history_ranges,
    }


def ingest_history_targets(
    conn,
    targets,
    source=DEFAULT_SOURCE,
    target_kind="sealed",
    history_ranges=None,
    endpoint_kind="both",
    use_browser_fallback=True,
    headless=True,
    commit_every=10,
    user_data_dir=None,
    profile_directory=None,
):
    processed = 0
    failed = 0
    fetched_rows = 0
    inserted_rows = 0

    for index, (internal_product_id, tcgplayer_product_id, resolved_url) in enumerate(targets, start=1):
        try:
            payload_result = []
            history_ranges = list(history_ranges or ["quarter", "annual"])
            endpoint_kinds = ["summary", "detailed"] if endpoint_kind == "both" else [endpoint_kind]
            for current_range in history_ranges:
                for current_kind in endpoint_kinds:
                    payload, fetch_source = fetch_all_history_json(
                        tcgplayer_product_id,
                        endpoint_kind=current_kind,
                        history_range=current_range,
                        product_url=resolved_url,
                        use_browser_fallback=use_browser_fallback,
                        headless=headless,
                        user_data_dir=user_data_dir,
                        profile_directory=profile_directory,
                    )
                    rows = normalize_history_payload(payload, endpoint_kind=current_kind, history_range=current_range)
                    inserted = insert_history_rows(
                        conn,
                        internal_product_id,
                        rows,
                        endpoint_kind=current_kind,
                        history_range=current_range,
                        source=source,
                        target_kind=target_kind,
                    )
                    fetched_rows += len(rows)
                    inserted_rows += inserted
                    payload_result.append((current_range, current_kind, len(rows), inserted, fetch_source))
            processed += 1
            print(
                f"[{index}/{len(targets)}] history product={tcgplayer_product_id} "
                + ", ".join(
                    f"{rng}:{kind} fetched={fetched} inserted={inserted}"
                    for rng, kind, fetched, inserted, _ in payload_result
                ),
                flush=True,
            )
        except Exception as exc:
            failed += 1
            print(
                f"[{index}/{len(targets)}] history product={tcgplayer_product_id} failed={type(exc).__name__}: {exc}",
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
        "endpoint_kind": endpoint_kind,
        "history_ranges": history_ranges or ["quarter", "annual"],
    }


def main():
    parser = argparse.ArgumentParser(description="Ingest TCGplayer aggregated price history into the database")
    parser.add_argument("--db", default="sealed_market.db", help="Database file or postgres:// DSN")
    parser.add_argument("--product-id", type=int, default=0, help="TCGplayer product id to ingest")
    parser.add_argument("--product-url", default="", help="Product URL to resolve the product id from the tracked catalog")
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="Source label")
    parser.add_argument("--target-kind", choices=["sealed", "cards"], default="sealed", help="Choose the product universe and history table")
    parser.add_argument("--endpoint-kind", choices=["summary", "detailed", "both"], default="both", help="Choose summary, detailed, or both history endpoints")
    parser.add_argument("--range", dest="ranges", action="append", choices=["quarter", "annual"], help="History range to ingest. Repeat to include both.")
    parser.add_argument("--no-browser-fallback", action="store_true", help="Disable Selenium fallback")
    parser.add_argument("--headless", action="store_true", help="Run Chrome headless for browser fallback")
    parser.add_argument("--chrome-user-data-dir", default="", help="Reuse an existing Chrome user data dir for authenticated fallback")
    parser.add_argument("--chrome-profile-directory", default="", help="Optional Chrome profile directory name inside the user data dir")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of products when running whole-universe history ingestion")
    parser.add_argument("--commit-every", type=int, default=10, help="Commit DB writes every N products")
    parser.add_argument("--set-id", type=int, default=0)
    parser.add_argument("--set-name", default="")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index for parallel batch workers")
    parser.add_argument("--shard-count", type=int, default=1, help="Total shard count for parallel batch workers")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_db_connection(conn)
    ensure_runtime_schema(conn)

    history_ranges = args.ranges or ["quarter", "annual"]
    if args.product_id or args.product_url:
        result = ingest_history_target(
            conn,
            product_id=args.product_id or None,
            product_url=args.product_url or None,
            source=args.source,
            target_kind=args.target_kind,
            history_ranges=history_ranges,
            endpoint_kind=args.endpoint_kind,
            use_browser_fallback=not args.no_browser_fallback,
            headless=args.headless,
            user_data_dir=args.chrome_user_data_dir.strip() or None,
            profile_directory=args.chrome_profile_directory.strip() or None,
        )
        conn.commit()
        conn.close()
        print(json.dumps(result, indent=2))
        return 0

    targets = load_history_targets(
        conn,
        limit=args.limit,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        target_kind=args.target_kind,
        set_id=args.set_id or None,
        set_name=args.set_name.strip() or None,
    )
    print(
        f"Refreshing aggregated history for {len(targets)} product(s), ranges={','.join(history_ranges)}, "
        f"endpoints={args.endpoint_kind}, shard={args.shard_index + 1}/{args.shard_count}",
        flush=True,
    )
    result = ingest_history_targets(
        conn,
        targets=targets,
        source=args.source,
        target_kind=args.target_kind,
        history_ranges=history_ranges,
        endpoint_kind=args.endpoint_kind,
        use_browser_fallback=not args.no_browser_fallback,
        headless=args.headless,
        commit_every=args.commit_every,
        user_data_dir=args.chrome_user_data_dir.strip() or None,
        profile_directory=args.chrome_profile_directory.strip() or None,
    )
    conn.commit()
    conn.close()
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
