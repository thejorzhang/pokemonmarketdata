"""Analyze sealed product snapshots for price movement outliers and market insights.

Usage:
    python3 analyze_outliers.py --db sealed_market.db
    python3 analyze_outliers.py --db sealed_market.db --source TCGplayer --top 20
"""

import argparse
import json
import math
from datetime import datetime, timedelta
from statistics import median

from db import connect_database, resolve_database_target

def parse_ts(value):
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def pct_change(old, new):
    if old is None or new is None or old <= 0:
        return None
    return (new - old) / old


def robust_z_scores(values):
    if not values:
        return {}
    m = median(values)
    deviations = [abs(v - m) for v in values]
    mad = median(deviations)
    if mad and mad > 0:
        scale = 1.4826 * mad
        return {v: (v - m) / scale for v in values}

    mean_val = sum(values) / len(values)
    var = sum((v - mean_val) ** 2 for v in values) / len(values)
    std = math.sqrt(var)
    if std == 0:
        return {v: 0.0 for v in values}
    return {v: (v - mean_val) / std for v in values}


def fetch_rows(conn, source):
    c = conn.cursor()
    c.execute(
        """
        SELECT
            l.product_id,
            p.name,
            p.url,
            l.timestamp,
            l.lowest_price,
            l.market_price,
            l.median_price,
            l.listing_count,
            l.current_quantity,
            l.current_sellers,
            l.set_name
        FROM listings l
        JOIN products p ON p.id = l.product_id
        WHERE l.source = ?
        ORDER BY l.product_id, l.timestamp
        """,
        (source,),
    )
    return c.fetchall()


def build_product_histories(rows):
    products = {}
    for (
        product_id,
        name,
        url,
        timestamp,
        lowest_price,
        market_price,
        median_price,
        listing_count,
        current_quantity,
        current_sellers,
        set_name,
    ) in rows:
        ts = parse_ts(timestamp)
        if not ts:
            continue
        entry = {
            "timestamp": timestamp,
            "ts": ts,
            "lowest_price": lowest_price,
            "market_price": market_price,
            "median_price": median_price,
            "listing_count": listing_count,
            "current_quantity": current_quantity,
            "current_sellers": current_sellers,
            "set_name": set_name,
        }
        if product_id not in products:
            products[product_id] = {
                "product_id": product_id,
                "name": name,
                "url": url,
                "history": [entry],
            }
        else:
            products[product_id]["history"].append(entry)
    return products


def nearest_prior_snapshot(history, latest_ts, min_age_days):
    target = latest_ts - timedelta(days=min_age_days)
    prior = [h for h in history[:-1] if h["ts"] <= target]
    if prior:
        return prior[-1]
    if len(history) >= 2:
        return history[-2]
    return None


def compute_metrics(product, lookbacks):
    history = product["history"]
    latest = history[-1]
    metrics = {
        "product_id": product["product_id"],
        "name": product["name"],
        "url": product["url"],
        "set_name": latest.get("set_name"),
        "history_points": len(history),
        "latest_timestamp": latest["timestamp"],
        "latest_lowest_price": latest.get("lowest_price"),
        "latest_market_price": latest.get("market_price"),
        "latest_listing_count": latest.get("listing_count"),
        "latest_current_sellers": latest.get("current_sellers"),
        "changes": {},
        "latest_vs_market_spread": None,
        "flags": [],
    }

    latest_lowest = latest.get("lowest_price")
    latest_market = latest.get("market_price")

    if latest_lowest is not None and latest_market is not None and latest_market > 0:
        metrics["latest_vs_market_spread"] = (latest_lowest - latest_market) / latest_market

    # Immediate change from previous snapshot
    if len(history) >= 2:
        prev = history[-2]
        immediate = pct_change(prev.get("lowest_price"), latest_lowest)
        age_days = (latest["ts"] - prev["ts"]).total_seconds() / 86400
        metrics["changes"]["since_prev"] = {
            "pct_change": immediate,
            "age_days": round(age_days, 2),
            "from_price": prev.get("lowest_price"),
            "to_price": latest_lowest,
        }

    # Lookback windows (1d,7d,30d) with nearest available fallback
    for days in lookbacks:
        baseline = nearest_prior_snapshot(history, latest["ts"], days)
        key = f"{days}d"
        if not baseline:
            metrics["changes"][key] = None
            continue
        chg = pct_change(baseline.get("lowest_price"), latest_lowest)
        age_days = (latest["ts"] - baseline["ts"]).total_seconds() / 86400
        metrics["changes"][key] = {
            "pct_change": chg,
            "age_days": round(age_days, 2),
            "from_price": baseline.get("lowest_price"),
            "to_price": latest_lowest,
        }

    # Basic quality flags for possible scrape oddities
    if latest_lowest is not None and latest_lowest <= 0:
        metrics["flags"].append("non_positive_lowest_price")
    if latest_market is not None and latest_market <= 0:
        metrics["flags"].append("non_positive_market_price")
    if (
        latest_lowest is not None
        and latest_market is not None
        and latest_market > 0
        and (latest_lowest / latest_market > 3 or latest_lowest / latest_market < 0.33)
    ):
        metrics["flags"].append("extreme_lowest_vs_market_ratio")

    return metrics


def generate_insights(metrics_list, top_n=15, z_threshold=3.5):
    insights = {
        "outliers_price_move": [],
        "outliers_spread": [],
        "liquidity_warnings": [],
        "possible_scrape_issues": [],
        "top_movers_up": [],
        "top_movers_down": [],
    }

    # Outliers on immediate price movement
    move_values = []
    metric_by_move = {}
    for m in metrics_list:
        since_prev = m["changes"].get("since_prev")
        if since_prev and since_prev.get("pct_change") is not None:
            val = since_prev["pct_change"]
            move_values.append(val)
            metric_by_move.setdefault(val, []).append(m)

    move_z = robust_z_scores(move_values)
    scored_moves = []
    for val, z in move_z.items():
        for m in metric_by_move.get(val, []):
            scored_moves.append((m, val, z))
    scored_moves.sort(key=lambda x: abs(x[2]), reverse=True)

    for m, val, z in scored_moves:
        if abs(z) >= z_threshold and abs(val) >= 0.15:
            insights["outliers_price_move"].append(
                {
                    "name": m["name"],
                    "url": m["url"],
                    "pct_change_since_prev": round(val * 100, 2),
                    "robust_z": round(z, 2),
                    "latest_lowest_price": m["latest_lowest_price"],
                    "latest_listing_count": m["latest_listing_count"],
                }
            )
            if len(insights["outliers_price_move"]) >= top_n:
                break

    # Outliers on latest lowest-vs-market spread
    spread_values = []
    metric_by_spread = {}
    for m in metrics_list:
        spread = m.get("latest_vs_market_spread")
        if spread is not None:
            spread_values.append(spread)
            metric_by_spread.setdefault(spread, []).append(m)

    spread_z = robust_z_scores(spread_values)
    scored_spreads = []
    for val, z in spread_z.items():
        for m in metric_by_spread.get(val, []):
            scored_spreads.append((m, val, z))
    scored_spreads.sort(key=lambda x: abs(x[2]), reverse=True)

    for m, spread, z in scored_spreads:
        if abs(z) >= z_threshold and abs(spread) >= 0.2:
            insights["outliers_spread"].append(
                {
                    "name": m["name"],
                    "url": m["url"],
                    "lowest_vs_market_spread_pct": round(spread * 100, 2),
                    "robust_z": round(z, 2),
                    "latest_lowest_price": m["latest_lowest_price"],
                    "latest_market_price": m["latest_market_price"],
                }
            )
            if len(insights["outliers_spread"]) >= top_n:
                break

    # Top movers
    mover_pool = []
    for m in metrics_list:
        since_prev = m["changes"].get("since_prev")
        if since_prev and since_prev.get("pct_change") is not None:
            chg = since_prev["pct_change"]
            if abs(chg) >= 0.01:
                mover_pool.append((m, chg))
    mover_pool.sort(key=lambda x: x[1], reverse=True)

    for m, chg in mover_pool[:top_n]:
        insights["top_movers_up"].append(
            {
                "name": m["name"],
                "url": m["url"],
                "pct_change_since_prev": round(chg * 100, 2),
                "latest_lowest_price": m["latest_lowest_price"],
            }
        )
    for m, chg in sorted(mover_pool, key=lambda x: x[1])[:top_n]:
        insights["top_movers_down"].append(
            {
                "name": m["name"],
                "url": m["url"],
                "pct_change_since_prev": round(chg * 100, 2),
                "latest_lowest_price": m["latest_lowest_price"],
            }
        )

    # Low-liquidity warnings
    for m in metrics_list:
        since_prev = m["changes"].get("since_prev")
        if not since_prev or since_prev.get("pct_change") is None:
            continue
        sellers = m.get("latest_current_sellers")
        listings = m.get("latest_listing_count")
        if abs(since_prev["pct_change"]) >= 0.2 and (
            (sellers is not None and sellers <= 3) or (listings is not None and listings <= 3)
        ):
            insights["liquidity_warnings"].append(
                {
                    "name": m["name"],
                    "url": m["url"],
                    "pct_change_since_prev": round(since_prev["pct_change"] * 100, 2),
                    "latest_listing_count": listings,
                    "latest_current_sellers": sellers,
                }
            )
    insights["liquidity_warnings"] = insights["liquidity_warnings"][:top_n]

    for m in metrics_list:
        if m["flags"]:
            insights["possible_scrape_issues"].append(
                {
                    "name": m["name"],
                    "url": m["url"],
                    "flags": m["flags"],
                    "latest_lowest_price": m["latest_lowest_price"],
                    "latest_market_price": m["latest_market_price"],
                }
            )
    insights["possible_scrape_issues"] = insights["possible_scrape_issues"][:top_n]

    return insights


def print_summary(metrics_list, insights, source):
    with_history = sum(1 for m in metrics_list if m["history_points"] >= 2)
    without_history = len(metrics_list) - with_history
    print(f"Source: {source}")
    print(f"Products analyzed: {len(metrics_list)}")
    print(f"Products with >=2 snapshots: {with_history}")
    print(f"Products with only 1 snapshot: {without_history}")
    print()

    def print_block(title, rows, formatter):
        print(title)
        if not rows:
            print("  (none)")
            print()
            return
        for row in rows:
            print(f"  - {formatter(row)}")
        print()

    print_block(
        "Price Move Outliers",
        insights["outliers_price_move"],
        lambda r: f"{r['name']} | {r['pct_change_since_prev']}% | z={r['robust_z']} | {r['url']}",
    )
    print_block(
        "Spread Outliers (Lowest vs Market)",
        insights["outliers_spread"],
        lambda r: f"{r['name']} | spread={r['lowest_vs_market_spread_pct']}% | z={r['robust_z']} | {r['url']}",
    )
    print_block(
        "Liquidity Warnings",
        insights["liquidity_warnings"],
        lambda r: f"{r['name']} | {r['pct_change_since_prev']}% | listings={r['latest_listing_count']} sellers={r['latest_current_sellers']} | {r['url']}",
    )
    print_block(
        "Possible Scrape Issues",
        insights["possible_scrape_issues"],
        lambda r: f"{r['name']} | flags={','.join(r['flags'])} | {r['url']}",
    )
    print_block(
        "Top Movers Up",
        insights["top_movers_up"],
        lambda r: f"{r['name']} | +{r['pct_change_since_prev']}% | {r['url']}",
    )
    print_block(
        "Top Movers Down",
        insights["top_movers_down"],
        lambda r: f"{r['name']} | {r['pct_change_since_prev']}% | {r['url']}",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--source", default="TCGplayer")
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--z-threshold", type=float, default=3.5)
    parser.add_argument("--output-json", default="", help="Optional JSON output path")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    rows = fetch_rows(conn, args.source)
    conn.close()

    if not rows:
        print(f"No rows found for source '{args.source}' in {args.db}")
        return

    products = build_product_histories(rows)
    metrics_list = [compute_metrics(p, lookbacks=[1, 7, 30]) for p in products.values()]
    insights = generate_insights(metrics_list, top_n=args.top, z_threshold=args.z_threshold)

    print_summary(metrics_list, insights, args.source)

    if args.output_json:
        payload = {
            "source": args.source,
            "generated_at_utc": datetime.utcnow().isoformat(),
            "summary": {
                "products_analyzed": len(metrics_list),
                "products_with_history": sum(1 for m in metrics_list if m["history_points"] >= 2),
            },
            "insights": insights,
            "metrics": metrics_list,
        }
        with open(args.output_json, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        print(f"Wrote JSON output: {args.output_json}")


if __name__ == "__main__":
    main()
