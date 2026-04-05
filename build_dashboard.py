"""Build an MVP HTML dashboard from sealed_market.db.

Usage:
    python3 build_dashboard.py --db sealed_market.db --source TCGplayer
"""

import argparse
import html
import os
from datetime import datetime

from db import connect_database, resolve_database_target, table_exists
from analyze_outliers import (
    build_product_histories,
    compute_metrics,
    fetch_rows,
    generate_insights,
)


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


def render_dashboard(source, metrics, insights, latest_run, failure_breakdown):
    generated = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    with_history = sum(1 for m in metrics if m["history_points"] >= 2)
    empty_notice = ""
    if not metrics:
        empty_notice = "<p class='empty'>No rows found for this source yet. Run a scrape to populate the dashboard.</p>"

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
      <div class="card"><div class="k">Spread Outliers</div><div class="v">{len(insights["outliers_spread"])}</div></div>
      <div class="card"><div class="k">Scrape Issue Flags</div><div class="v">{len(insights["possible_scrape_issues"])}</div></div>
    </div>
    {empty_notice}

    <h2>Latest Run Health</h2>
    {run_health_html}

    <h2>Latest Run Failure Breakdown</h2>
    {failure_html}

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
    if not table_exists(conn, "products") or not table_exists(conn, "listings"):
        latest_run = None
        failures = []
        products = {}
        metrics = []
        insights = generate_insights(metrics, top_n=args.top, z_threshold=args.z_threshold)
        html_content = render_dashboard(args.source, metrics, insights, latest_run, failures)
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
    conn.close()

    output_dir = os.path.dirname(args.out) or "."
    os.makedirs(output_dir, exist_ok=True)
    html_content = render_dashboard(args.source, metrics, insights, latest_run, failures)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"Wrote dashboard: {args.out}")


if __name__ == "__main__":
    main()
