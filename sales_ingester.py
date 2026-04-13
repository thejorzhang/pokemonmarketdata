"""TCGplayer latest sales ingester.

This module prefers the direct JSON `latestsales` endpoint and falls back to a
browser-context fetch when the endpoint rejects plain HTTP clients.
"""

import argparse
import hashlib
import json
import re
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.cookies import RequestsCookieJar, create_cookie

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
DEFAULT_AUTHENTICATED_SALES_PAGE_SIZE = 25
DEFAULT_REQUEST_TIMEOUT = 20
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


def load_exported_session(session_file):
    if not session_file:
        return {"cookies": [], "local_storage": {}, "session_storage": {}}
    payload = json.loads(Path(session_file).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        cookies = payload
        local_storage = {}
        session_storage = {}
    elif isinstance(payload, dict):
        cookies = payload.get("cookies") or []
        local_storage = payload.get("local_storage") or {}
        session_storage = payload.get("session_storage") or {}
    else:
        raise ValueError("invalid_session_file")
    if not isinstance(cookies, list):
        cookies = []
    if not isinstance(local_storage, dict):
        local_storage = {}
    if not isinstance(session_storage, dict):
        session_storage = {}
    return {
        "cookies": cookies,
        "local_storage": {str(k): "" if v is None else str(v) for k, v in local_storage.items()},
        "session_storage": {str(k): "" if v is None else str(v) for k, v in session_storage.items()},
    }


def normalize_exported_cookie(cookie):
    if not isinstance(cookie, dict):
        return None
    name = cookie.get("name")
    value = cookie.get("value")
    domain = cookie.get("domain")
    if not name or value is None or not domain:
        return None
    normalized = {
        "name": str(name),
        "value": str(value),
        "domain": str(domain),
        "path": str(cookie.get("path") or "/"),
    }
    if cookie.get("expires") not in (None, "", 0, "0"):
        try:
            normalized["expires"] = int(float(cookie.get("expires")))
        except Exception:
            pass
    if cookie.get("secure") is not None:
        normalized["secure"] = bool(cookie.get("secure"))
    if cookie.get("httpOnly") is not None:
        normalized["httpOnly"] = bool(cookie.get("httpOnly"))
    same_site = cookie.get("sameSite")
    if same_site in ("Lax", "Strict", "None"):
        normalized["sameSite"] = same_site
    return normalized


def build_requests_cookie_jar(session_file):
    jar = RequestsCookieJar()
    if not session_file:
        return jar
    session = load_exported_session(session_file)
    for cookie in session["cookies"]:
        normalized = normalize_exported_cookie(cookie)
        if not normalized:
            continue
        jar.set(
            normalized["name"],
            normalized["value"],
            domain=normalized["domain"],
            path=normalized["path"],
            expires=normalized.get("expires"),
            secure=normalized.get("secure", False),
            rest={"HttpOnly": normalized.get("httpOnly", False)},
        )
    return jar


def apply_exported_session_to_driver(driver, session_file):
    if not session_file:
        return
    session = load_exported_session(session_file)
    cookies = [normalize_exported_cookie(cookie) for cookie in session["cookies"]]
    cookies = [cookie for cookie in cookies if cookie]
    if not cookies and not session["local_storage"] and not session["session_storage"]:
        return

    driver.execute_cdp_cmd("Network.enable", {})
    driver.get("https://www.tcgplayer.com/")
    if cookies:
        driver.execute_cdp_cmd("Network.setCookies", {"cookies": cookies})
    if session["local_storage"]:
        driver.execute_script(
            """
            const entries = arguments[0];
            for (const [key, value] of Object.entries(entries)) {
              localStorage.setItem(key, value);
            }
            """,
            session["local_storage"],
        )
    if session["session_storage"]:
        driver.execute_script(
            """
            const entries = arguments[0];
            for (const [key, value] of Object.entries(entries)) {
              sessionStorage.setItem(key, value);
            }
            """,
            session["session_storage"],
        )


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


def make_driver(headless=True, user_data_dir=None, profile_directory=None):
    if webdriver is None:
        raise RuntimeError("selenium_not_available")
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1200")
    opts.add_argument("--log-level=3")
    opts.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")
    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    if profile_directory:
        opts.add_argument(f"--profile-directory={profile_directory}")
    return webdriver.Chrome(options=opts)


def prepare_profile_clone(user_data_dir, profile_directory=None):
    if not user_data_dir:
        return None, None
    source_root = Path(user_data_dir).expanduser().resolve()
    source_profile = source_root / (profile_directory or "Default")
    if not source_root.exists():
        raise RuntimeError(f"chrome_user_data_dir_missing:{source_root}")
    if not source_profile.exists():
        raise RuntimeError(f"chrome_profile_missing:{source_profile}")

    clone_root = Path(tempfile.mkdtemp(prefix="tcgplayer-chrome-clone-"))
    profile_name = profile_directory or "Default"

    local_state = source_root / "Local State"
    if local_state.exists():
        shutil.copy2(local_state, clone_root / "Local State")

    target_profile = clone_root / profile_name
    ignore_names = shutil.ignore_patterns(
        "Singleton*",
        "lockfile",
        "LOCK",
        "Crashpad",
        "Crash Reports",
        "Code Cache",
        "GPUCache",
        "DawnCache",
        "ShaderCache",
        "BrowserMetrics",
        "GrShaderCache",
        "GraphiteDawnCache",
        "*.tmp",
    )
    shutil.copytree(source_profile, target_profile, ignore=ignore_names, dirs_exist_ok=True)
    return str(clone_root), profile_name


def _next_page_value(payload):
    if not isinstance(payload, dict):
        return None
    value = payload.get("nextPage")
    if value in (None, "", 0, "0", False):
        return None
    return value


def fetch_latest_sales_json(
    product_id,
    product_url=None,
    mpfev=DEFAULT_MPFEV,
    use_browser_fallback=True,
    headless=True,
    user_data_dir=None,
    profile_directory=None,
    session_file=None,
    browser_script_timeout=None,
    request_timeout=None,
    request_retries=0,
    request_retry_backoff=1.5,
    page=None,
    offset=None,
    limit=None,
    time_filter=None,
    endpoint_override=None,
):
    endpoint = endpoint_override or LATEST_SALES_URL.format(product_id=product_id)
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
    }
    params = {"mpfev": mpfev}
    if page not in (None, "", 0):
        params["page"] = page
    requests_session = requests.Session()
    requests_session.headers.update(headers)
    if session_file:
        requests_session.cookies.update(build_requests_cookie_jar(session_file))
    request_body = {"productId": product_id}
    if offset not in (None, "", 0):
        request_body["offset"] = int(offset)
    if limit not in (None, "", 0):
        request_body["limit"] = int(limit)
    if time_filter not in (None, ""):
        request_body["time"] = time_filter

    timeout_seconds = float(request_timeout or DEFAULT_REQUEST_TIMEOUT)
    attempts = max(1, int(request_retries or 0) + 1)
    last_error = None
    for attempt in range(attempts):
        try:
            resp = requests_session.post(
                endpoint,
                params=params,
                json=request_body,
                timeout=timeout_seconds,
            )
            if resp.ok:
                return resp.json(), "requests"
            last_error = RuntimeError(f"latestsales_requests_http_{resp.status_code}")
        except Exception as exc:
            last_error = exc
        if attempt + 1 < attempts:
            time.sleep(float(request_retry_backoff) ** attempt)

    if not use_browser_fallback:
        raise last_error or RuntimeError("latestsales_requests_failed")

    if product_url:
        page_url = product_url
    else:
        page_url = f"https://www.tcgplayer.com/product/{product_id}?view=sales-history"

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
        if session_file:
            apply_exported_session_to_driver(driver, session_file)
        # Authenticated browser-context fetches can take noticeably longer
        # than the unauthenticated request path, especially when the page is
        # hydrating account-aware sales-history UI and paging through large
        # full-history result sets.
        timeout_seconds = browser_script_timeout
        if timeout_seconds in (None, "", 0):
            if session_file:
                timeout_seconds = 120
            elif user_data_dir:
                timeout_seconds = 60
            else:
                timeout_seconds = 20
        driver.set_script_timeout(int(timeout_seconds))
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
        page_query = f"&page={page}" if page not in (None, "", 0) else ""
        raw = driver.execute_async_script(script, endpoint + f"?mpfev={mpfev}{page_query}", int(product_id))
        # Re-run with the actual request payload when offset/limit auth paging is active.
        if any(value not in (None, "", 0) for value in (offset, limit)) or time_filter not in (None, ""):
            raw = driver.execute_async_script(
                """
                const done = arguments[0];
                fetch(arguments[1], {
                  method: 'POST',
                  credentials: 'include',
                  mode: 'cors',
                  headers: {
                    'accept': 'application/json, text/plain, */*',
                    'content-type': 'application/json'
                  },
                  body: JSON.stringify(arguments[2])
                }).then(async resp => {
                  const body = await resp.text();
                  done(JSON.stringify({status: resp.status, body}));
                }).catch(err => done(JSON.stringify({error: String(err)})));
                """,
                endpoint + f"?mpfev={mpfev}{page_query}",
                request_body,
            )
        payload = json.loads(raw)
        if payload.get("error"):
            raise RuntimeError(payload["error"])
        if payload.get("status") != 200:
            raise RuntimeError(f"latestsales_browser_http_{payload.get('status')}")
        return json.loads(payload.get("body") or "{}"), "selenium"
    finally:
        driver.quit()
        if clone_root:
            shutil.rmtree(clone_root, ignore_errors=True)


def merge_latest_sales_payloads(payloads):
    combined = []
    total_results = 0
    result_count = 0
    fetch_source = None
    for payload, source in payloads:
        fetch_source = fetch_source or source
        if isinstance(payload, dict):
            page_rows = payload.get("data")
            if isinstance(page_rows, list):
                combined.extend(page_rows)
            total_results = max(int(payload.get("totalResults") or 0), total_results)
            result_count += int(payload.get("resultCount") or (len(page_rows) if isinstance(page_rows, list) else 0))
    return {
        "previousPage": "",
        "nextPage": "",
        "resultCount": result_count,
        "totalResults": total_results or result_count,
        "data": combined,
    }, fetch_source or "unknown"


def fetch_all_latest_sales_json(
    product_id,
    product_url=None,
    mpfev=DEFAULT_MPFEV,
    use_browser_fallback=True,
    headless=True,
    user_data_dir=None,
    profile_directory=None,
    session_file=None,
    browser_script_timeout=None,
    request_timeout=None,
    request_retries=0,
    request_retry_backoff=1.5,
    max_pages=50,
):
    payloads = []
    authenticated_offset_mode = bool(session_file)
    total_results = None
    offset = 0 if authenticated_offset_mode else None
    seen_pages = set()
    seen_urls = set()
    page = None
    endpoint_override = None

    for _ in range(max_pages):
        payload, fetch_source = fetch_latest_sales_json(
            product_id,
            product_url=product_url,
            mpfev=mpfev,
            use_browser_fallback=use_browser_fallback,
            headless=headless,
            user_data_dir=user_data_dir,
            profile_directory=profile_directory,
            session_file=session_file,
            browser_script_timeout=browser_script_timeout,
            request_timeout=request_timeout,
            request_retries=request_retries,
            request_retry_backoff=request_retry_backoff,
            page=page,
            offset=offset,
            limit=DEFAULT_AUTHENTICATED_SALES_PAGE_SIZE if authenticated_offset_mode else None,
            endpoint_override=endpoint_override,
        )
        payloads.append((payload, fetch_source))
        if authenticated_offset_mode:
            rows = payload.get("data") if isinstance(payload, dict) else None
            rows = rows if isinstance(rows, list) else []
            if total_results is None:
                try:
                    total_results = int(payload.get("totalResults") or 0)
                except Exception:
                    total_results = 0
            if not rows:
                break
            offset += len(rows)
            next_page = _next_page_value(payload)
            if not next_page:
                break
            if total_results and offset >= total_results:
                break
            continue
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
        if str(next_page) in seen_pages:
            break
        seen_pages.add(str(next_page))
        endpoint_override = None
        page = next_page

    return merge_latest_sales_payloads(payloads)


def iter_latest_sales_pages(
    product_id,
    product_url=None,
    mpfev=DEFAULT_MPFEV,
    use_browser_fallback=True,
    headless=True,
    user_data_dir=None,
    profile_directory=None,
    session_file=None,
    browser_script_timeout=None,
    request_timeout=None,
    request_retries=0,
    request_retry_backoff=1.5,
    max_pages=50,
):
    authenticated_offset_mode = bool(session_file)
    total_results = None
    offset = 0 if authenticated_offset_mode else None
    seen_pages = set()
    seen_urls = set()
    page = None
    endpoint_override = None

    for _ in range(max_pages):
        payload, fetch_source = fetch_latest_sales_json(
            product_id,
            product_url=product_url,
            mpfev=mpfev,
            use_browser_fallback=use_browser_fallback,
            headless=headless,
            user_data_dir=user_data_dir,
            profile_directory=profile_directory,
            session_file=session_file,
            browser_script_timeout=browser_script_timeout,
            request_timeout=request_timeout,
            request_retries=request_retries,
            request_retry_backoff=request_retry_backoff,
            page=page,
            offset=offset,
            limit=DEFAULT_AUTHENTICATED_SALES_PAGE_SIZE if authenticated_offset_mode else None,
            endpoint_override=endpoint_override,
        )
        yield payload, fetch_source
        if authenticated_offset_mode:
            rows = payload.get("data") if isinstance(payload, dict) else None
            rows = rows if isinstance(rows, list) else []
            if total_results is None:
                try:
                    total_results = int(payload.get("totalResults") or 0)
                except Exception:
                    total_results = 0
            if not rows:
                break
            offset += len(rows)
            next_page = _next_page_value(payload)
            if not next_page:
                break
            if total_results and offset >= total_results:
                break
            continue
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
        if str(next_page) in seen_pages:
            break
        seen_pages.add(str(next_page))
        endpoint_override = None
        page = next_page


def normalize_latest_sales_payload(payload, sale_date=None, seen_fingerprints=None):
    data = payload
    if isinstance(payload, dict):
        for key in ("data", "results", "sales", "items"):
            if isinstance(payload.get(key), list):
                data = payload[key]
                break
        else:
            data = []

    target_date = sale_date or None
    seen = seen_fingerprints if seen_fingerprints is not None else set()
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


def load_sales_targets(conn, product_id=None, product_url=None, limit=0, shard_index=0, shard_count=1, target_kind="sealed", set_id=None, set_name=None):
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
        query += " AND id IN (SELECT product_id FROM product_details WHERE set_id = {placeholder})".format(placeholder=placeholder)
        params.append(int(set_id))
    elif target_kind == "sealed" and set_name:
        query += " AND id IN (SELECT d.product_id FROM product_details d JOIN sets s ON s.id = d.set_id WHERE s.name = {placeholder})".format(placeholder=placeholder)
        params.append(set_name)
    elif target_kind == "cards" and set_id:
        query += f" AND set_id = {placeholder}"
        params.append(int(set_id))
    elif target_kind == "cards" and set_name:
        query += " AND set_id IN (SELECT id FROM sets WHERE name = {placeholder})".format(placeholder=placeholder)
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


def ingest_latest_sales_pages(
    conn,
    internal_product_id,
    tcgplayer_product_id,
    resolved_url,
    sale_date=None,
    source="TCGplayer",
    use_browser_fallback=True,
    headless=True,
    target_kind="sealed",
    user_data_dir=None,
    profile_directory=None,
    session_file=None,
    browser_script_timeout=None,
    request_timeout=None,
    request_retries=0,
    request_retry_backoff=1.5,
    max_pages=50,
    commit_every_pages=1,
):
    effective_sale_date = sale_date
    initial_backfill = should_initial_backfill(conn, internal_product_id, sale_date=sale_date, target_kind=target_kind)
    if initial_backfill:
        effective_sale_date = None

    seen_fingerprints = set()
    fetched_rows = 0
    inserted_rows = 0
    fetch_source = None
    pages_processed = 0

    for payload, page_fetch_source in iter_latest_sales_pages(
        tcgplayer_product_id,
        product_url=resolved_url,
        use_browser_fallback=use_browser_fallback,
        headless=headless,
        user_data_dir=user_data_dir,
        profile_directory=profile_directory,
        session_file=session_file,
        browser_script_timeout=browser_script_timeout,
        request_timeout=request_timeout,
        request_retries=request_retries,
        request_retry_backoff=request_retry_backoff,
        max_pages=max_pages,
    ):
        fetch_source = fetch_source or page_fetch_source
        rows = normalize_latest_sales_payload(payload, sale_date=effective_sale_date, seen_fingerprints=seen_fingerprints)
        fetched_rows += len(rows)
        inserted_rows += insert_sales_rows(conn, internal_product_id, rows, source=source, target_kind=target_kind)
        pages_processed += 1
        if commit_every_pages > 0 and pages_processed % commit_every_pages == 0:
            conn.commit()

    mark_sales_refresh_state(conn, internal_product_id, target_kind=target_kind, backfill_completed=initial_backfill)
    return {
        "product_id": internal_product_id,
        "tcgplayer_product_id": tcgplayer_product_id,
        "fetch_source": fetch_source or "unknown",
        "fetched_rows": fetched_rows,
        "inserted_rows": inserted_rows,
        "sale_date": effective_sale_date,
        "initial_backfill": initial_backfill,
        "pages_processed": pages_processed,
    }


def target_has_existing_sales(conn, product_id, target_kind="sealed"):
    config = get_target_config(target_kind)
    fk_column = config["fk_column"]
    sales_table = config["sales_table"]
    placeholder = "%s" if get_dialect(conn) == "postgres" else "?"
    row = conn.execute(
        f"SELECT 1 FROM {sales_table} WHERE {fk_column} = {placeholder} LIMIT 1",
        (int(product_id),),
    ).fetchone()
    return row is not None


def target_has_completed_sales_backfill(conn, product_id, target_kind="sealed"):
    config = get_target_config(target_kind)
    product_table = config["product_table"]
    placeholder = "%s" if get_dialect(conn) == "postgres" else "?"
    row = conn.execute(
        f"SELECT sales_backfill_completed_at FROM {product_table} WHERE id = {placeholder}",
        (int(product_id),),
    ).fetchone()
    return bool(row and row[0])


def target_release_date(conn, product_id, target_kind="sealed"):
    placeholder = "%s" if get_dialect(conn) == "postgres" else "?"
    if target_kind == "cards":
        row = conn.execute(
            """
            SELECT d.release_date
            FROM card_products p
            LEFT JOIN card_details d ON d.card_product_id = p.id
            WHERE p.id = ?
            """.replace("?", placeholder),
            (int(product_id),),
        ).fetchone()
        value = row[0] if row else None
        if value and re.match(r"^\d{4}-\d{2}-\d{2}$", str(value)):
            return value
        return None

    row = conn.execute(
        """
        SELECT COALESCE(d.release_date, p.release_date)
        FROM products p
        LEFT JOIN product_details d ON d.product_id = p.id
        WHERE p.id = ?
        """.replace("?", placeholder),
        (int(product_id),),
    ).fetchone()
    value = row[0] if row else None
    if value and re.match(r"^\d{4}-\d{2}-\d{2}$", str(value)):
        return value
    return None


def should_initial_backfill(conn, product_id, sale_date=None, target_kind="sealed"):
    if target_kind != "cards" or not sale_date:
        return False
    if target_has_completed_sales_backfill(conn, product_id, target_kind=target_kind):
        return False
    release_date = target_release_date(conn, product_id, target_kind=target_kind)
    if release_date and release_date > sale_date:
        return False
    return True


def mark_sales_refresh_state(conn, product_id, target_kind="sealed", backfill_completed=False):
    config = get_target_config(target_kind)
    product_table = config["product_table"]
    placeholder = "%s" if get_dialect(conn) == "postgres" else "?"
    now = datetime.utcnow().isoformat()
    if backfill_completed:
        conn.execute(
            f"""
            UPDATE {product_table}
               SET last_sales_refresh_at = {placeholder},
                   sales_backfill_completed_at = COALESCE(sales_backfill_completed_at, {placeholder})
             WHERE id = {placeholder}
            """,
            (now, now, int(product_id)),
        )
        return
    conn.execute(
        f"UPDATE {product_table} SET last_sales_refresh_at = {placeholder} WHERE id = {placeholder}",
        (now, int(product_id)),
    )


def ingest_latest_sales(
    conn,
    product_id=None,
    product_url=None,
    sale_date=None,
    source="TCGplayer",
    use_browser_fallback=True,
    headless=True,
    target_kind="sealed",
    user_data_dir=None,
    profile_directory=None,
    session_file=None,
    browser_script_timeout=None,
    request_timeout=None,
    request_retries=0,
    request_retry_backoff=1.5,
):
    internal_product_id, tcgplayer_product_id, resolved_url = resolve_product_record(
        conn,
        tcgplayer_product_id=product_id,
        product_url=product_url,
        target_kind=target_kind,
    )
    return ingest_latest_sales_pages(
        conn,
        internal_product_id=internal_product_id,
        tcgplayer_product_id=tcgplayer_product_id,
        resolved_url=resolved_url,
        sale_date=sale_date,
        source=source,
        use_browser_fallback=use_browser_fallback,
        headless=headless,
        target_kind=target_kind,
        user_data_dir=user_data_dir,
        profile_directory=profile_directory,
        session_file=session_file,
        browser_script_timeout=browser_script_timeout,
        request_timeout=request_timeout,
        request_retries=request_retries,
        request_retry_backoff=request_retry_backoff,
    )


def ingest_sales_targets(
    conn,
    targets,
    sale_date=None,
    source="TCGplayer",
    use_browser_fallback=True,
    headless=True,
    commit_every=10,
    target_kind="sealed",
    user_data_dir=None,
    profile_directory=None,
    session_file=None,
    browser_script_timeout=None,
    request_timeout=None,
    request_retries=0,
    request_retry_backoff=1.5,
):
    processed = 0
    failed = 0
    fetched_rows = 0
    inserted_rows = 0

    for index, (internal_product_id, tcgplayer_product_id, resolved_url) in enumerate(targets, start=1):
        try:
            product_result = ingest_latest_sales_pages(
                conn,
                internal_product_id=internal_product_id,
                tcgplayer_product_id=tcgplayer_product_id,
                resolved_url=resolved_url,
                sale_date=sale_date,
                source=source,
                use_browser_fallback=use_browser_fallback,
                headless=headless,
                target_kind=target_kind,
                user_data_dir=user_data_dir,
                profile_directory=profile_directory,
                session_file=session_file,
                browser_script_timeout=browser_script_timeout,
                request_timeout=request_timeout,
                request_retries=request_retries,
                request_retry_backoff=request_retry_backoff,
            )
            processed += 1
            fetched_rows += product_result["fetched_rows"]
            inserted_rows += product_result["inserted_rows"]
            print(
                f"[{index}/{len(targets)}] sales tcgplayer={tcgplayer_product_id} internal={internal_product_id} source={product_result['fetch_source']} pages={product_result['pages_processed']} fetched={product_result['fetched_rows']} inserted={product_result['inserted_rows']}"
                + (" mode=initial_backfill" if product_result["initial_backfill"] else ""),
                flush=True,
            )
        except Exception as exc:
            failed += 1
            print(
                f"[{index}/{len(targets)}] sales tcgplayer={tcgplayer_product_id} internal={internal_product_id} failed={type(exc).__name__}: {exc}",
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
    parser.add_argument("--chrome-user-data-dir", default="", help="Reuse an existing Chrome user data dir for authenticated browser fallback")
    parser.add_argument("--chrome-profile-directory", default="", help="Optional Chrome profile directory name inside the user data dir")
    parser.add_argument("--session-file", default="", help="JSON file exported from a signed-in Chrome session")
    parser.add_argument("--snapshot-file", help="Optional JSON fixture file instead of hitting the network")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of products when running whole-universe sales refresh")
    parser.add_argument("--commit-every", type=int, default=1, help="Commit DB writes every N products")
    parser.add_argument("--browser-script-timeout", type=int, default=0, help="Override Selenium async script timeout in seconds (0 uses automatic defaults)")
    parser.add_argument("--request-timeout", type=float, default=0, help="Override requests timeout in seconds for latestsales fetches (0 uses automatic defaults)")
    parser.add_argument("--request-retries", type=int, default=2, help="Retry latestsales HTTP requests this many times before Selenium fallback")
    parser.add_argument("--request-retry-backoff", type=float, default=1.5, help="Backoff multiplier between latestsales HTTP request retries")
    parser.add_argument("--set-id", type=int, default=0)
    parser.add_argument("--set-name", default="")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index for parallel batch workers")
    parser.add_argument("--shard-count", type=int, default=1, help="Total shard count for parallel batch workers")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_db_connection(conn)
    ensure_runtime_schema(conn)

    browser_script_timeout = args.browser_script_timeout or None
    if browser_script_timeout is None and args.session_file.strip() and args.all_dates:
        # Full authenticated backfills routinely require more time than
        # the normal recent-sales path, so make the heavy case more patient
        # by default without slowing everyday incremental runs.
        browser_script_timeout = 240
    request_timeout = args.request_timeout or None
    request_retries = args.request_retries
    request_retry_backoff = args.request_retry_backoff
    if request_timeout is None and args.session_file.strip() and args.all_dates:
        request_timeout = 45

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
        set_id=args.set_id or None,
        set_name=args.set_name.strip() or None,
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
            user_data_dir=args.chrome_user_data_dir.strip() or None,
            profile_directory=args.chrome_profile_directory.strip() or None,
            session_file=args.session_file.strip() or None,
            browser_script_timeout=browser_script_timeout,
            request_timeout=request_timeout,
            request_retries=request_retries,
            request_retry_backoff=request_retry_backoff,
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
            user_data_dir=args.chrome_user_data_dir.strip() or None,
            profile_directory=args.chrome_profile_directory.strip() or None,
            session_file=args.session_file.strip() or None,
            browser_script_timeout=browser_script_timeout,
            request_timeout=request_timeout,
            request_retries=request_retries,
            request_retry_backoff=request_retry_backoff,
        )
    conn.commit()
    conn.close()
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
