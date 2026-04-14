"""Build an MVP HTML dashboard from sealed_market.db.

Usage:
    python3 build_dashboard.py --db sealed_market.db --source TCGplayer
"""

import argparse
import html
import os
from datetime import datetime

from collection_manager import DEFAULT_COLLECTION_NAME, fetch_collection_summary
from db import configure_connection, connect_database, resolve_database_target, table_exists
from analyze_outliers import (
    build_product_histories,
    compute_metrics,
    fetch_rows,
    generate_insights,
)
from build_set_stats import build_set_stats
from populate_db import ensure_runtime_schema


def fmt_money(value):
    if value is None:
        return "-"
    return f"${value:,.2f}"


def fmt_pct(value):
    if value is None:
        return "-"
    return f"{value:.2f}%"


def fetch_latest_run(conn, source):
    if not table_exists(conn, "scrape_runs"):
        return None
    c = conn.cursor()
    c.execute(
        """
        SELECT id, source, status, started_at, ended_at, attempted_count, processed_count, failed_count, parse_failed_count
        FROM scrape_runs
        WHERE source = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (source,),
    )
    row = c.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "source": row[1],
        "status": row[2],
        "started_at": row[3],
        "ended_at": row[4],
        "attempted_count": row[5],
        "processed_count": row[6],
        "failed_count": row[7],
        "parse_failed_count": row[8],
    }


def fetch_failure_breakdown(conn, run_id):
    if not table_exists(conn, "scrape_failures"):
        return []
    c = conn.cursor()
    c.execute(
        """
        SELECT stage, reason, COUNT(*) AS cnt
        FROM scrape_failures
        WHERE run_id = ?
        GROUP BY stage, reason
        ORDER BY cnt DESC, stage, reason
        """,
        (run_id,),
    )
    return c.fetchall()


def fetch_set_stats(conn, limit=20):
    if not table_exists(conn, "set_stats"):
        return []
    c = conn.cursor()
    c.execute(
        """
        SELECT
            set_name,
            COALESCE(product_line, '-') AS product_line,
            COALESCE(set_type, '-') AS set_type,
            COALESCE(total_product_count, 0) AS total_product_count,
            COALESCE(total_sale_count, 0) AS total_sale_count,
            COALESCE(detail_coverage_pct, 0.0) AS detail_coverage_pct,
            COALESCE(sales_coverage_pct, 0.0) AS sales_coverage_pct,
            COALESCE(priority_hot_count, 0) AS hot_count,
            COALESCE(priority_warm_count, 0) AS warm_count,
            COALESCE(priority_cold_count, 0) AS cold_count,
            COALESCE(priority_dormant_count, 0) AS dormant_count,
            COALESCE(total_last_sale_at, '-') AS total_last_sale_at,
            COALESCE(refreshed_at, '-') AS refreshed_at
        FROM set_stats s
        ORDER BY total_sale_count DESC, total_product_count DESC, set_name
        LIMIT ?
        """,
        (limit,),
    )
    return c.fetchall()


def fetch_collection_dashboard(conn, collection_name=DEFAULT_COLLECTION_NAME, limit=100):
    try:
        return fetch_collection_summary(conn, collection_name=collection_name, limit=limit)
    except Exception:
        return None


def build_table(headers, rows):
    if not rows:
        return "<p class='empty'>No rows.</p>"

    head_html = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = []
    for row in rows:
        cols = "".join(f"<td>{c}</td>" for c in row)
        body.append(f"<tr>{cols}</tr>")
    body_html = "".join(body)
    return f"<table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table>"


def render_dashboard(source, metrics, insights, latest_run, failure_breakdown, set_stats_rows, collection_summary=None):
    generated = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with_history = sum(1 for m in metrics if m["history_points"] >= 2)
    empty_notice = ""
    if not metrics:
        empty_notice = "<p class='empty'>No rows found for this source yet. Run a scrape to populate the dashboard.</p>"

    sets_with_sales = sum(1 for row in set_stats_rows if row[4] > 0)
    sets_with_details = sum(1 for row in set_stats_rows if row[5] > 0 or row[6] > 0)
    avg_detail_coverage = (
        sum(float(row[5]) for row in set_stats_rows if row[5] is not None) / len(set_stats_rows)
        if set_stats_rows
        else None
    )
    avg_sales_coverage = (
        sum(float(row[6]) for row in set_stats_rows if row[6] is not None) / len(set_stats_rows)
        if set_stats_rows
        else None
    )

    movers_up_rows = []
    for row in insights["top_movers_up"][:20]:
        movers_up_rows.append(
            (
                html.escape(row["name"]),
                fmt_pct(row["pct_change_since_prev"]),
                fmt_money(row["latest_lowest_price"]),
                f"<a href='{html.escape(row['url'])}' target='_blank' rel='noreferrer'>Link</a>",
            )
        )

    movers_down_rows = []
    for row in insights["top_movers_down"][:20]:
        movers_down_rows.append(
            (
                html.escape(row["name"]),
                fmt_pct(row["pct_change_since_prev"]),
                fmt_money(row["latest_lowest_price"]),
                f"<a href='{html.escape(row['url'])}' target='_blank' rel='noreferrer'>Link</a>",
            )
        )

    spread_rows = []
    for row in insights["outliers_spread"][:20]:
        spread_rows.append(
            (
                html.escape(row["name"]),
                fmt_pct(row["lowest_vs_market_spread_pct"]),
                str(row["robust_z"]),
                fmt_money(row["latest_lowest_price"]),
                fmt_money(row["latest_market_price"]),
            )
        )

    parse_issue_rows = []
    for row in insights["possible_scrape_issues"][:20]:
        parse_issue_rows.append(
            (
                html.escape(row["name"]),
                html.escape(",".join(row["flags"])),
                fmt_money(row["latest_lowest_price"]),
                fmt_money(row["latest_market_price"]),
            )
        )

    run_health_html = "<p class='empty'>No scrape run found for this source.</p>"
    failure_html = "<p class='empty'>No failures recorded.</p>"
    if latest_run:
        success_rate = 0.0
        if latest_run["attempted_count"]:
            success_rate = (latest_run["processed_count"] / latest_run["attempted_count"]) * 100.0

        run_health_html = build_table(
            ["Run ID", "Status", "Started", "Ended", "Attempted", "Processed", "Failed", "Parse Failed", "Success Rate"],
            [
                (
                    str(latest_run["id"]),
                    html.escape(latest_run["status"]),
                    html.escape(latest_run["started_at"] or "-"),
                    html.escape(latest_run["ended_at"] or "-"),
                    str(latest_run["attempted_count"]),
                    str(latest_run["processed_count"]),
                    str(latest_run["failed_count"]),
                    str(latest_run["parse_failed_count"]),
                    f"{success_rate:.2f}%",
                )
            ],
        )

        if failure_breakdown:
            failure_html = build_table(
                ["Stage", "Reason", "Count"],
                [(html.escape(stage), html.escape(reason), str(cnt)) for stage, reason, cnt in failure_breakdown],
            )

    set_stats_html = "<p class='empty'>Build set stats to see set-level coverage and freshness.</p>"
    if set_stats_rows:
        set_stats_html = build_table(
            ["Set", "Line", "Type", "Products", "Sales", "Detail Cov", "Sales Cov", "Hot", "Warm", "Cold", "Dormant", "Latest Sale", "Refreshed"],
            [
                (
                    html.escape(row[0]),
                    html.escape(row[1]),
                    html.escape(row[2]),
                    str(row[3]),
                    str(row[4]),
                    fmt_pct(row[5]),
                    fmt_pct(row[6]),
                    str(row[7]),
                    str(row[8]),
                    str(row[9]),
                    str(row[10]),
                    html.escape(row[11]),
                    html.escape(row[12]),
                )
                for row in set_stats_rows
            ],
        )

    collection_cards_html = "<p class='empty'>Add holdings to your collection to track value, cost basis, and movers.</p>"
    collection_items_html = "<p class='empty'>No collection items yet.</p>"
    collection_movers_html = "<p class='empty'>No collection movers yet.</p>"
    if collection_summary and collection_summary.get("item_count", 0) > 0:
        collection_cards_html = f"""
        <div class="grid">
          <div class="card"><div class="k">Collection Items</div><div class="v">{collection_summary['item_count']}</div></div>
          <div class="card"><div class="k">Total Units</div><div class="v">{collection_summary['total_units']:.2f}</div></div>
          <div class="card"><div class="k">Estimated Value</div><div class="v">{fmt_money(collection_summary['estimated_value'])}</div></div>
          <div class="card"><div class="k">Cost Basis</div><div class="v">{fmt_money(collection_summary['cost_basis'])}</div></div>
          <div class="card"><div class="k">Unrealized PnL</div><div class="v">{fmt_money(collection_summary['unrealized_pnl'])}</div></div>
          <div class="card"><div class="k">Unrealized Return</div><div class="v">{fmt_pct(collection_summary['unrealized_pct'])}</div></div>
        </div>
        """
        collection_items_html = build_table(
            ["Product", "Kind", "Qty", "Unit Cost", "Current Unit", "Current Value", "PnL", "Move", "Source"],
            [
                (
                    f"<a href='{html.escape(item['url'] or '#')}' target='_blank' rel='noreferrer'>{html.escape(item['name'] or '-')}</a>",
                    html.escape(item["target_kind"]),
                    f"{item['quantity']:.2f}",
                    fmt_money(item["unit_cost"]),
                    fmt_money(item["current_unit_value"]),
                    fmt_money(item["current_value"]),
                    fmt_money(item["unrealized_pnl"]),
                    fmt_pct(item["change_pct"]),
                    html.escape(item["price_source"] or "-"),
                )
                for item in collection_summary["items"][:25]
            ],
        )
        collection_movers_html = build_table(
            ["Product", "Kind", "Move", "Current Value", "Valuation Date"],
            [
                (
                    html.escape(item["name"] or "-"),
                    html.escape(item["target_kind"]),
                    fmt_pct(item["change_pct"]),
                    fmt_money(item["current_value"]),
                    html.escape(item["valuation_date"] or "-"),
                )
                for item in collection_summary["top_movers"]
            ],
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Pokemon Sealed Market MVP Dashboard</title>
  <style>
    :root {{
      --bg: #f7f5ef;
      --ink: #151515;
      --accent: #0f766e;
      --card: #ffffff;
      --line: #ddd7cc;
      --muted: #5b5b55;
      --good: #1d7f4f;
      --bad: #b23a33;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 85% 10%, #cdeee2 0%, rgba(205,238,226,0.3) 22%, transparent 60%),
        radial-gradient(circle at 10% 95%, #f5dbb4 0%, rgba(245,219,180,0.25) 20%, transparent 60%),
        var(--bg);
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1 {{
      margin: 0 0 6px;
      letter-spacing: 0.3px;
      font-size: 30px;
    }}
    .sub {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }}
    .grid {{
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      margin-top: 18px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
      box-shadow: 0 1px 0 rgba(0,0,0,0.03);
    }}
    .k {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.8px;
      color: var(--muted);
      margin-bottom: 6px;
    }}
    .v {{
      font-size: 24px;
      font-weight: 700;
    }}
    h2 {{
      margin: 28px 0 8px;
      font-size: 18px;
      border-bottom: 2px solid var(--line);
      padding-bottom: 6px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 10px;
      overflow: hidden;
      display: block;
      overflow-x: auto;
    }}
    th, td {{
      padding: 9px 10px;
      border-bottom: 1px solid var(--line);
      font-size: 13px;
      text-align: left;
      white-space: nowrap;
    }}
    th {{
      background: #f0ece2;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: #3a3a35;
    }}
    tr:last-child td {{ border-bottom: none; }}
    a {{
      color: var(--accent);
      text-decoration: none;
      font-weight: 600;
    }}
    .empty {{
      color: var(--muted);
      font-size: 13px;
      margin: 8px 0 0;
    }}
    .split {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
    }}
    @media (max-width: 900px) {{
      .split {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Pokemon Sealed Market: MVP Dashboard</h1>
    <p class="sub">Source: {html.escape(source)} | Generated: {generated}</p>

    <div class="grid">
      <div class="card"><div class="k">Products Analyzed</div><div class="v">{len(metrics)}</div></div>
      <div class="card"><div class="k">Products With History</div><div class="v">{with_history}</div></div>
      <div class="card"><div class="k">Sets With Sales</div><div class="v">{sets_with_sales}</div></div>
      <div class="card"><div class="k">Sets With Details</div><div class="v">{sets_with_details}</div></div>
      <div class="card"><div class="k">Spread Outliers</div><div class="v">{len(insights["outliers_spread"])}</div></div>
      <div class="card"><div class="k">Scrape Issue Flags</div><div class="v">{len(insights["possible_scrape_issues"])}</div></div>
      <div class="card"><div class="k">Avg Detail Coverage</div><div class="v">{fmt_pct(avg_detail_coverage)}</div></div>
      <div class="card"><div class="k">Avg Sales Coverage</div><div class="v">{fmt_pct(avg_sales_coverage)}</div></div>
    </div>
    {empty_notice}

    <h2>Latest Run Health</h2>
    {run_health_html}

    <h2>Latest Run Failure Breakdown</h2>
    {failure_html}

    <h2>Set Overview</h2>
    {set_stats_html}

    <h2>Collection Overview</h2>
    {collection_cards_html}

    <div class="split">
      <div>
        <h2>Collection Holdings</h2>
        {collection_items_html}
      </div>
      <div>
        <h2>Biggest Collection Movers</h2>
        {collection_movers_html}
      </div>
    </div>

    <div class="split">
      <div>
        <h2>Top Movers Up</h2>
        {build_table(["Product", "Move", "Latest Lowest", "URL"], movers_up_rows)}
      </div>
      <div>
        <h2>Top Movers Down</h2>
        {build_table(["Product", "Move", "Latest Lowest", "URL"], movers_down_rows)}
      </div>
    </div>

    <h2>Lowest vs Market Spread Outliers</h2>
    {build_table(["Product", "Spread", "Robust Z", "Lowest", "Market"], spread_rows)}

    <h2>Possible Scrape Issues</h2>
    {build_table(["Product", "Flags", "Lowest", "Market"], parse_issue_rows)}
  </div>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="sealed_market.db")
    parser.add_argument("--source", default="TCGplayer")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--z-threshold", type=float, default=3.5)
    parser.add_argument("--out", default="dashboard/mvp_dashboard.html")
    args = parser.parse_args()

    conn = connect_database(resolve_database_target(args.db))
    configure_connection(conn)
    ensure_runtime_schema(conn)
    build_set_stats(conn)
    if not table_exists(conn, "products") or not table_exists(conn, "listings"):
        latest_run = None
        failures = []
        products = {}
        metrics = []
        insights = generate_insights(metrics, top_n=args.top, z_threshold=args.z_threshold)
        html_content = render_dashboard(args.source, metrics, insights, latest_run, failures, [], fetch_collection_dashboard(conn))
        output_dir = os.path.dirname(args.out) or "."
        os.makedirs(output_dir, exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(html_content)
        conn.close()
        print(f"Wrote dashboard: {args.out}")
        return

    rows = fetch_rows(conn, args.source)
    products = build_product_histories(rows)
    metrics = [compute_metrics(p, lookbacks=[1, 7, 30]) for p in products.values()]
    insights = generate_insights(metrics, top_n=args.top, z_threshold=args.z_threshold)
    latest_run = fetch_latest_run(conn, args.source)
    failures = fetch_failure_breakdown(conn, latest_run["id"]) if latest_run else []
    set_stats_rows = fetch_set_stats(conn, limit=50)
    collection_summary = fetch_collection_dashboard(conn)
    conn.close()

    output_dir = os.path.dirname(args.out) or "."
    os.makedirs(output_dir, exist_ok=True)
    html_content = render_dashboard(args.source, metrics, insights, latest_run, failures, set_stats_rows, collection_summary)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Wrote dashboard: {args.out}")


if __name__ == "__main__":
    main()
