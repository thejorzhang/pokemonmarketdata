"""Local operator console for scraper workflows.

Usage:
    python3 operator_console.py
    python3 operator_console.py --host 127.0.0.1 --port 8765
"""

import argparse
import html
import json
import os
import shlex
import signal
import subprocess
import threading
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parent
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765


HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Pokemon Market Operator Console</title>
  <style>
    :root {
      --bg: #efe7d3;
      --panel: #fffaf1;
      --panel-strong: #171717;
      --ink: #191511;
      --muted: #6f6558;
      --line: #d8cab2;
      --accent: #0e7c66;
      --accent-2: #e28c37;
      --good: #1d7f4f;
      --bad: #b23a33;
      --terminal: #0d1117;
      --terminal-ink: #c9d1d9;
      --terminal-dim: #7d8590;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 10% 15%, rgba(226,140,55,0.22), transparent 28%),
        radial-gradient(circle at 88% 12%, rgba(14,124,102,0.18), transparent 25%),
        linear-gradient(180deg, #f8f2e5 0%, var(--bg) 100%);
    }
    .wrap {
      max-width: 1320px;
      margin: 0 auto;
      padding: 28px 20px 40px;
    }
    .hero {
      display: grid;
      gap: 16px;
      grid-template-columns: 1.15fr 0.85fr;
      align-items: start;
      margin-bottom: 18px;
    }
    .hero-card, .panel {
      background: rgba(255, 250, 241, 0.94);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 10px 28px rgba(58, 42, 24, 0.08);
    }
    .hero-card {
      padding: 22px;
    }
    h1 {
      margin: 0 0 8px;
      font-size: clamp(28px, 4vw, 42px);
      line-height: 1;
      letter-spacing: 0.02em;
    }
    .sub {
      margin: 0;
      color: var(--muted);
      font-size: 15px;
      max-width: 60ch;
    }
    .badges {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }
    .badge {
      padding: 8px 12px;
      border-radius: 999px;
      background: #f3ead8;
      border: 1px solid var(--line);
      color: var(--ink);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .status-card {
      padding: 22px;
      background: linear-gradient(180deg, rgba(23,23,23,0.98), rgba(23,23,23,0.92));
      color: #f7f4ee;
    }
    .status-label {
      color: #b2aca4;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 8px;
    }
    .status-value {
      font-size: 28px;
      font-weight: 700;
      margin-bottom: 8px;
    }
    .status-meta {
      color: #d0c9be;
      font-size: 14px;
      min-height: 40px;
    }
    .layout {
      display: grid;
      gap: 18px;
      grid-template-columns: 420px minmax(0, 1fr);
    }
    .panel {
      padding: 18px;
    }
    h2 {
      margin: 0 0 14px;
      font-size: 18px;
    }
    .stack {
      display: grid;
      gap: 14px;
    }
    .form-block {
      padding: 16px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fffdf8;
    }
    .form-block h3 {
      margin: 0 0 10px;
      font-size: 15px;
    }
    .help {
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }
    .grid2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    input[type="text"], input[type="number"], input[type="date"] {
      width: 100%;
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid #cdbda2;
      background: #fff;
      color: var(--ink);
      font: inherit;
    }
    .checks {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin: 10px 0 12px;
      color: var(--ink);
      font-size: 14px;
    }
    .checks label {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      margin: 0;
      color: var(--ink);
      text-transform: none;
      letter-spacing: 0;
      font-size: 14px;
    }
    button {
      border: 0;
      cursor: pointer;
      border-radius: 12px;
      padding: 12px 14px;
      font: inherit;
      font-weight: 700;
      transition: transform 120ms ease, opacity 120ms ease;
    }
    button:hover { transform: translateY(-1px); }
    button:disabled { opacity: 0.55; cursor: not-allowed; transform: none; }
    .primary {
      background: var(--accent);
      color: #fff;
    }
    .secondary {
      background: #efe6d4;
      color: var(--ink);
      border: 1px solid var(--line);
    }
    .toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    .terminal-shell {
      border-radius: 18px;
      overflow: hidden;
      border: 1px solid #20262f;
      background: var(--terminal);
      min-height: 640px;
      display: flex;
      flex-direction: column;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.03);
    }
    .terminal-top {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 12px 14px;
      background: linear-gradient(180deg, #171b22, #11161d);
      border-bottom: 1px solid #21262d;
      color: #d7dee6;
      font-size: 13px;
    }
    .lights {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    .light {
      width: 11px;
      height: 11px;
      border-radius: 50%;
      display: inline-block;
    }
    .red { background: #ff5f57; }
    .yellow { background: #febc2e; }
    .green { background: #28c840; }
    #terminal {
      flex: 1;
      margin: 0;
      padding: 18px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font: 13px/1.45 "SFMono-Regular", "Menlo", "Consolas", monospace;
      color: var(--terminal-ink);
    }
    .terminal-empty {
      color: var(--terminal-dim);
    }
    .meta-grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      margin-top: 14px;
    }
    .meta-box {
      background: #f4ecdc;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
    }
    .meta-box .k {
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
    }
    .meta-box .v {
      font-size: 16px;
      font-weight: 700;
    }
    .links {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 14px;
    }
    .links a {
      color: var(--accent);
      font-weight: 700;
      text-decoration: none;
    }
    @media (max-width: 1000px) {
      .hero, .layout {
        grid-template-columns: 1fr;
      }
      .terminal-shell {
        min-height: 480px;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="hero-card">
        <h1>Pokemon Market Operator Console</h1>
        <p class="sub">Run the daily scrape, rebuild the dashboard, and watch the process stream like a terminal without needing to use the command line.</p>
        <div class="badges">
          <span class="badge">Daily Snapshots</span>
          <span class="badge">Live Log Readout</span>
          <span class="badge">Nontechnical Workflow</span>
        </div>
      </div>
      <div class="hero-card status-card">
        <div class="status-label">Current Job</div>
        <div class="status-value" id="hero-status">Idle</div>
        <div class="status-meta" id="hero-meta">No process running.</div>
      </div>
    </section>

    <section class="layout">
      <div class="stack">
        <div class="panel">
          <h2>Controls</h2>

          <div class="form-block">
            <h3>Daily Price Scrape</h3>
            <p class="help">Main operator flow. Set `Workers` above 1 to fan out across shards; batch mode ignores the spot-check limit and uses the full product set.</p>
            <div class="grid2">
              <div><label for="scrape-db">Database</label><input id="scrape-db" type="text" value="sealed_market.db" /></div>
              <div><label for="scrape-csv">Product CSV</label><input id="scrape-csv" type="text" value="products.csv" /></div>
              <div><label for="scrape-source">Source</label><input id="scrape-source" type="text" value="TCGplayer" /></div>
              <div><label for="scrape-date">Snapshot Date</label><input id="scrape-date" type="date" /></div>
              <div><label for="scrape-limit">Limit</label><input id="scrape-limit" type="number" min="0" value="0" /></div>
              <div><label for="scrape-workers">Workers</label><input id="scrape-workers" type="number" min="1" value="4" /></div>
              <div><label for="scrape-commit">Commit Every</label><input id="scrape-commit" type="number" min="1" value="25" /></div>
              <div><label for="scrape-delay-min">Delay Min</label><input id="scrape-delay-min" type="number" step="0.1" min="0" value="2.0" /></div>
              <div><label for="scrape-delay-max">Delay Max</label><input id="scrape-delay-max" type="number" step="0.1" min="0" value="5.0" /></div>
            </div>
            <div class="checks">
              <label><input id="scrape-selenium" type="checkbox" checked /> Use Selenium fallback</label>
              <label><input id="scrape-headless" type="checkbox" checked /> Headless browser</label>
            </div>
            <button class="primary" id="run-scrape">Run Batched Daily Scrape</button>
          </div>

          <div class="form-block">
            <h3>Dashboard Build</h3>
            <p class="help">Regenerates the HTML dashboard from the latest database state.</p>
            <div class="grid2">
              <div><label for="dash-db">Database</label><input id="dash-db" type="text" value="sealed_market.db" /></div>
              <div><label for="dash-source">Source</label><input id="dash-source" type="text" value="TCGplayer" /></div>
            </div>
            <button class="secondary" id="build-dashboard">Build Dashboard</button>
          </div>

          <div class="form-block">
            <h3>Catalog Refresh</h3>
            <p class="help">Choose how to update the TCGplayer sealed product list before a scrape. Newest uses the page count and merges in new links without dupes. Complete Fresh and Reconcile always scan the full catalog. Set workers above 1 to fan out the page crawl.</p>
            <div class="grid2">
              <div><label for="catalog-pages">Pages</label><input id="catalog-pages" type="number" min="1" value="3" /></div>
              <div><label for="catalog-out">Output CSV</label><input id="catalog-out" type="text" value="products.csv" /></div>
              <div><label for="catalog-workers">Workers</label><input id="catalog-workers" type="number" min="1" value="4" /></div>
              <div><label for="catalog-category-slug">Category Slug</label><input id="catalog-category-slug" type="text" value="pokemon" /></div>
              <div><label for="catalog-product-line-name">Product Line</label><input id="catalog-product-line-name" type="text" value="pokemon" /></div>
              <div style="grid-column: 1 / -1;"><label for="catalog-product-type-name">Product Type</label><input id="catalog-product-type-name" type="text" value="Sealed Products" /></div>
            </div>
            <div class="checks">
              <label><input id="catalog-headless" type="checkbox" checked /> Headless browser</label>
            </div>
            <div class="toolbar">
              <button class="secondary" id="catalog-newest">Newest Links Refresh</button>
              <button class="secondary" id="catalog-fresh">Complete Fresh Refresh</button>
              <button class="secondary" id="catalog-reconcile">Reconcile Catalog</button>
            </div>
          </div>

          <div class="form-block">
            <h3>Card Catalog Load</h3>
            <p class="help">Keep cards separate from sealed for now. First scrape a card CSV with Card filters, then load that CSV into the `card_products` table.</p>
            <div class="grid2">
              <div><label for="card-catalog-db">Database</label><input id="card-catalog-db" type="text" value="sealed_market.db" /></div>
              <div><label for="card-catalog-csv">Card CSV</label><input id="card-catalog-csv" type="text" value="pokemon_cards.csv" /></div>
              <div><label for="card-catalog-category-slug">Category Slug</label><input id="card-catalog-category-slug" type="text" value="pokemon" /></div>
              <div><label for="card-catalog-product-line-name">Product Line</label><input id="card-catalog-product-line-name" type="text" value="pokemon" /></div>
              <div style="grid-column: 1 / -1;"><label for="card-catalog-source">Source</label><input id="card-catalog-source" type="text" value="TCGplayer Cards" /></div>
            </div>
            <button class="secondary" id="run-card-catalog">Load Card Catalog</button>
          </div>

          <div class="form-block">
            <h3>Pokemon Cards Catalog Refresh</h3>
            <p class="help">Dedicated card discovery flow. This keeps the card crawl separate from sealed and writes to a separate CSV before loading into `card_products`.</p>
            <div class="grid2">
              <div><label for="cards-pages">Pages</label><input id="cards-pages" type="number" min="1" value="5" /></div>
              <div><label for="cards-out">Output CSV</label><input id="cards-out" type="text" value="pokemon_cards.csv" /></div>
              <div><label for="cards-workers">Workers</label><input id="cards-workers" type="number" min="1" value="4" /></div>
              <div><label for="cards-category-slug">Category Slug</label><input id="cards-category-slug" type="text" value="pokemon" /></div>
              <div><label for="cards-product-line-name">Product Line</label><input id="cards-product-line-name" type="text" value="pokemon" /></div>
              <div style="grid-column: 1 / -1;"><label for="cards-product-type-name">Product Type</label><input id="cards-product-type-name" type="text" value="Cards" /></div>
            </div>
            <div class="checks">
              <label><input id="cards-headless" type="checkbox" checked /> Headless browser</label>
            </div>
            <div class="toolbar">
              <button class="secondary" id="cards-newest">Newest Card Links Refresh</button>
              <button class="secondary" id="cards-fresh">Complete Fresh Card Refresh</button>
              <button class="secondary" id="cards-reconcile">Reconcile Card Catalog</button>
              <button class="primary" id="run-card-pipeline">Run Card Expansion Pipeline</button>
            </div>
          </div>

          <div class="form-block">
            <h3>Sales Refresh</h3>
            <p class="help">Fetch latest TCGplayer sales into the `sales` table. By default this ingests prior-day sales across the full tracked catalog. Fill Product ID or Product URL only when you want to narrow to one product.</p>
            <div class="grid2">
              <div><label for="sales-db">Database</label><input id="sales-db" type="text" value="sealed_market.db" /></div>
              <div><label for="sales-source">Source</label><input id="sales-source" type="text" value="TCGplayer" /></div>
              <div><label for="sales-product-id">Product ID</label><input id="sales-product-id" type="number" min="0" value="0" /></div>
              <div><label for="sales-date">Sale Date</label><input id="sales-date" type="date" /></div>
              <div><label for="sales-workers">Workers</label><input id="sales-workers" type="number" min="1" value="4" /></div>
              <div><label for="sales-limit">Limit</label><input id="sales-limit" type="number" min="0" value="0" /></div>
              <div style="grid-column: 1 / -1;"><label for="sales-product-url">Product URL</label><input id="sales-product-url" type="text" value="" /></div>
              <div style="grid-column: 1 / -1;"><label for="sales-snapshot-file">Snapshot File</label><input id="sales-snapshot-file" type="text" value="" /></div>
            </div>
            <div class="checks">
              <label><input id="sales-all-dates" type="checkbox" /> Ingest all returned dates</label>
              <label><input id="sales-browser-fallback" type="checkbox" checked /> Use browser fallback</label>
              <label><input id="sales-headless" type="checkbox" checked /> Headless browser</label>
            </div>
            <button class="secondary" id="run-sales">Run Sales Refresh</button>
          </div>

          <div class="form-block">
            <h3>Card Sales Refresh</h3>
            <p class="help">Fetch latest sales into `card_sales` from the tracked `card_products` universe. Prior-day whole-catalog is the default.</p>
            <div class="grid2">
              <div><label for="card-sales-db">Database</label><input id="card-sales-db" type="text" value="sealed_market.db" /></div>
              <div><label for="card-sales-source">Source</label><input id="card-sales-source" type="text" value="TCGplayer Cards" /></div>
              <div><label for="card-sales-product-id">Product ID</label><input id="card-sales-product-id" type="number" min="0" value="0" /></div>
              <div><label for="card-sales-date">Sale Date</label><input id="card-sales-date" type="date" /></div>
              <div><label for="card-sales-workers">Workers</label><input id="card-sales-workers" type="number" min="1" value="4" /></div>
              <div><label for="card-sales-limit">Limit</label><input id="card-sales-limit" type="number" min="0" value="0" /></div>
              <div style="grid-column: 1 / -1;"><label for="card-sales-product-url">Product URL</label><input id="card-sales-product-url" type="text" value="" /></div>
            </div>
            <div class="checks">
              <label><input id="card-sales-all-dates" type="checkbox" /> Ingest all returned dates</label>
              <label><input id="card-sales-browser-fallback" type="checkbox" checked /> Use browser fallback</label>
              <label><input id="card-sales-headless" type="checkbox" checked /> Headless browser</label>
            </div>
            <button class="secondary" id="run-card-sales">Run Card Sales Refresh</button>
          </div>

          <div class="form-block">
            <h3>Product Details Refresh</h3>
            <p class="help">Visit missing product pages and fill the `product_details` table. Set `Workers` above 1 to fan out across shards; batch mode ignores the spot-check limit and only visits missing rows.</p>
            <div class="grid2">
              <div><label for="details-db">Database</label><input id="details-db" type="text" value="sealed_market.db" /></div>
              <div><label for="details-source">Source</label><input id="details-source" type="text" value="TCGplayer Product Details" /></div>
              <div><label for="details-limit">Limit</label><input id="details-limit" type="number" min="0" value="0" /></div>
              <div><label for="details-workers">Workers</label><input id="details-workers" type="number" min="1" value="4" /></div>
              <div><label for="details-delay-min">Delay Min</label><input id="details-delay-min" type="number" step="0.1" min="0" value="0.5" /></div>
              <div><label for="details-delay-max">Delay Max</label><input id="details-delay-max" type="number" step="0.1" min="0" value="1.5" /></div>
            </div>
            <div class="checks">
              <label><input id="details-selenium" type="checkbox" checked /> Use Selenium fallback</label>
              <label><input id="details-headless" type="checkbox" checked /> Headless browser</label>
            </div>
            <button class="secondary" id="run-details">Run Batched Product Details Refresh</button>
          </div>

          <div class="form-block">
            <h3>Card Details Refresh</h3>
            <p class="help">Visit missing `card_products` rows and fill `card_details`. This is the separate card-processing track while sealed keeps using the existing tables.</p>
            <div class="grid2">
              <div><label for="card-details-db">Database</label><input id="card-details-db" type="text" value="sealed_market.db" /></div>
              <div><label for="card-details-source">Source</label><input id="card-details-source" type="text" value="TCGplayer Card Details" /></div>
              <div><label for="card-details-limit">Limit</label><input id="card-details-limit" type="number" min="0" value="0" /></div>
              <div><label for="card-details-workers">Workers</label><input id="card-details-workers" type="number" min="1" value="4" /></div>
              <div><label for="card-details-delay-min">Delay Min</label><input id="card-details-delay-min" type="number" step="0.1" min="0" value="0.5" /></div>
              <div><label for="card-details-delay-max">Delay Max</label><input id="card-details-delay-max" type="number" step="0.1" min="0" value="1.5" /></div>
            </div>
            <div class="checks">
              <label><input id="card-details-selenium" type="checkbox" checked /> Use Selenium fallback</label>
              <label><input id="card-details-headless" type="checkbox" checked /> Headless browser</label>
            </div>
            <button class="secondary" id="run-card-details">Run Batched Card Details Refresh</button>
          </div>

          <div class="links">
            <a href="/dashboard/mvp_dashboard.html" target="_blank" rel="noreferrer">Open dashboard output</a>
          </div>
        </div>

        <div class="panel">
          <h2>Process Details</h2>
          <div class="meta-grid">
            <div class="meta-box"><div class="k">Job Type</div><div class="v" id="job-type">None</div></div>
            <div class="meta-box"><div class="k">State</div><div class="v" id="job-state">Idle</div></div>
            <div class="meta-box"><div class="k">Started</div><div class="v" id="job-started">-</div></div>
            <div class="meta-box"><div class="k">Finished</div><div class="v" id="job-finished">-</div></div>
          </div>
        </div>
      </div>

      <div class="panel" style="padding: 0;">
        <div class="toolbar" style="padding: 18px 18px 0;">
          <button class="secondary" id="refresh-status">Refresh Status</button>
          <button class="secondary" id="stop-job">Stop Active Job</button>
          <button class="secondary" id="clear-log">Clear View</button>
        </div>
        <div class="terminal-shell">
          <div class="terminal-top">
            <div class="lights">
              <span class="light red"></span>
              <span class="light yellow"></span>
              <span class="light green"></span>
            </div>
            <div id="command-preview">Waiting for job...</div>
          </div>
          <pre id="terminal"><span class="terminal-empty">No output yet. Start a process from the left.</span></pre>
        </div>
      </div>
    </section>
  </div>

  <script>
    const state = { activeJobId: null, pollTimer: null, lastRenderedLog: "" };

    function setTodayDefault() {
      const input = document.getElementById("scrape-date");
      if (!input.value) {
        const now = new Date();
        const localDate = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
        input.value = localDate.toISOString().slice(0, 10);
      }
    }

    function setYesterdayDefault() {
      const input = document.getElementById("sales-date");
      if (!input.value) {
        const now = new Date();
        now.setDate(now.getDate() - 1);
        const localDate = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
        input.value = localDate.toISOString().slice(0, 10);
      }
      const cardInput = document.getElementById("card-sales-date");
      if (!cardInput.value) {
        cardInput.value = input.value;
      }
    }

    async function api(path, options = {}) {
      const response = await fetch(path, {
        headers: { "Content-Type": "application/json" },
        ...options
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error || "Request failed");
      }
      return data;
    }

    function applyStatus(payload) {
      const job = payload.active_job || payload.last_job || null;
      const status = job ? job.status : "idle";
      const statusText = job && job.returncode !== null ? `${status.toUpperCase()} (${job.returncode})` : status.toUpperCase();
      document.getElementById("hero-status").textContent = statusText;
      document.getElementById("hero-meta").textContent = job
        ? `${job.job_type} | ${job.command}`
        : "No process running.";
      document.getElementById("job-type").textContent = job ? job.job_type : "None";
      document.getElementById("job-state").textContent = job ? job.status : "Idle";
      document.getElementById("job-started").textContent = job && job.started_at ? job.started_at : "-";
      document.getElementById("job-finished").textContent = job && job.finished_at ? job.finished_at : "-";
      document.getElementById("command-preview").textContent = job ? job.command : "Waiting for job...";
      document.getElementById("stop-job").disabled = !payload.active_job;
      if (job) {
        state.activeJobId = job.id;
      }
      if (!payload.active_job && state.pollTimer) {
        clearTimeout(state.pollTimer);
        state.pollTimer = null;
      }
    }

    function normalizeLog(logText) {
      return (logText || "")
        .replace(/\\r\\n/g, "\\n")
        .replace(/\\r/g, "\\n");
    }

    function tailLog(logText, maxLines = 120) {
      const normalized = normalizeLog(logText);
      const lines = normalized
        .split("\\n")
        .map((line) => line.trimEnd());
      while (lines.length && lines[0].trim().length === 0) {
        lines.shift();
      }
      const tail = lines.slice(-maxLines);
      return tail.join("\\n").trimEnd();
    }

    function renderLog(job) {
      const terminal = document.getElementById("terminal");
      if (!job) {
        terminal.innerHTML = '<span class="terminal-empty">No output yet. Start a process from the left.</span>';
        state.lastRenderedLog = "";
        return;
      }
      const text = tailLog(job.log || "");
      terminal.textContent = text || "Process started. Waiting for output...";
      if (text !== state.lastRenderedLog) {
        terminal.scrollTop = terminal.scrollHeight;
        state.lastRenderedLog = text;
      }
    }

    async function refreshStatus() {
      const payload = await api("/api/status");
      applyStatus(payload);
      if (payload.active_job || payload.last_job) {
        renderLog(payload.active_job || payload.last_job);
      }
      if (payload.active_job) {
        state.pollTimer = setTimeout(refreshStatus, 1200);
      }
    }

    async function startJob(jobType, args) {
      const payload = await api("/api/jobs", {
        method: "POST",
        body: JSON.stringify({ job_type: jobType, args })
      });
      state.activeJobId = payload.job.id;
      renderLog(payload.job);
      await refreshStatus();
    }

    async function stopActiveJob() {
      const payload = await api("/api/jobs/stop", {
        method: "POST",
        body: JSON.stringify({})
      });
      if (payload.job) {
        renderLog(payload.job);
      }
      await refreshStatus();
    }

    function getNumber(id) {
      return Number(document.getElementById(id).value || "0");
    }

    function getText(id) {
      return document.getElementById(id).value.trim();
    }

    function getChecked(id) {
      return document.getElementById(id).checked;
    }

    function wireEvents() {
      document.getElementById("run-scrape").addEventListener("click", async () => {
        try {
          const workers = getNumber("scrape-workers");
          await startJob("scrape", {
            db: getText("scrape-db"),
            csv: getText("scrape-csv"),
            source: getText("scrape-source"),
            snapshot_date: getText("scrape-date"),
            limit: workers > 1 ? 0 : getNumber("scrape-limit"),
            commit_every: getNumber("scrape-commit"),
            delay_min: getNumber("scrape-delay-min"),
            delay_max: getNumber("scrape-delay-max"),
            selenium: getChecked("scrape-selenium"),
            headless: getChecked("scrape-headless"),
            workers
          });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("build-dashboard").addEventListener("click", async () => {
        try {
          await startJob("dashboard", {
            db: getText("dash-db"),
            source: getText("dash-source")
          });
        } catch (error) {
          alert(error.message);
        }
      });

      function catalogArgs(mode) {
        return {
          pages: getNumber("catalog-pages"),
          out: getText("catalog-out"),
          category_slug: getText("catalog-category-slug"),
          product_line_name: getText("catalog-product-line-name"),
          product_type_name: getText("catalog-product-type-name"),
          headless: getChecked("catalog-headless"),
          mode,
          workers: getNumber("catalog-workers")
        };
      }

      document.getElementById("catalog-newest").addEventListener("click", async () => {
        try {
          await startJob("catalog", catalogArgs("newest"));
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("catalog-fresh").addEventListener("click", async () => {
        try {
          await startJob("catalog", { ...catalogArgs("fresh"), all_pages: true });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("catalog-reconcile").addEventListener("click", async () => {
        try {
          await startJob("catalog", { ...catalogArgs("reconcile"), all_pages: true });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("run-card-catalog").addEventListener("click", async () => {
        try {
          await startJob("card_catalog", {
            db: getText("card-catalog-db"),
            csv: getText("card-catalog-csv"),
            category_slug: getText("card-catalog-category-slug"),
            product_line_name: getText("card-catalog-product-line-name"),
            source: getText("card-catalog-source")
          });
        } catch (error) {
          alert(error.message);
        }
      });

      function cardsCatalogArgs(mode) {
        return {
          pages: getNumber("cards-pages"),
          out: getText("cards-out"),
          category_slug: getText("cards-category-slug"),
          product_line_name: getText("cards-product-line-name"),
          product_type_name: getText("cards-product-type-name"),
          headless: getChecked("cards-headless"),
          mode,
          workers: getNumber("cards-workers")
        };
      }

      document.getElementById("cards-newest").addEventListener("click", async () => {
        try {
          await startJob("card_catalog_scrape", cardsCatalogArgs("newest"));
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("cards-fresh").addEventListener("click", async () => {
        try {
          await startJob("card_catalog_scrape", { ...cardsCatalogArgs("fresh"), all_pages: true });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("cards-reconcile").addEventListener("click", async () => {
        try {
          await startJob("card_catalog_scrape", { ...cardsCatalogArgs("reconcile"), all_pages: true });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("run-card-pipeline").addEventListener("click", async () => {
        try {
          await startJob("card_pipeline", { ...cardsCatalogArgs("fresh"), all_pages: true });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("run-sales").addEventListener("click", async () => {
        try {
          const workers = getNumber("sales-workers");
          await startJob("sales", {
            db: getText("sales-db"),
            source: getText("sales-source"),
            product_id: getNumber("sales-product-id"),
            product_url: getText("sales-product-url"),
            sale_date: getText("sales-date"),
            all_dates: getChecked("sales-all-dates"),
            limit: workers > 1 ? 0 : getNumber("sales-limit"),
            workers,
            browser_fallback: getChecked("sales-browser-fallback"),
            headless: getChecked("sales-headless"),
            snapshot_file: getText("sales-snapshot-file")
          });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("run-card-sales").addEventListener("click", async () => {
        try {
          const workers = getNumber("card-sales-workers");
          await startJob("card_sales", {
            db: getText("card-sales-db"),
            source: getText("card-sales-source"),
            product_id: getNumber("card-sales-product-id"),
            product_url: getText("card-sales-product-url"),
            sale_date: getText("card-sales-date"),
            all_dates: getChecked("card-sales-all-dates"),
            limit: workers > 1 ? 0 : getNumber("card-sales-limit"),
            workers,
            browser_fallback: getChecked("card-sales-browser-fallback"),
            headless: getChecked("card-sales-headless")
          });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("run-details").addEventListener("click", async () => {
        try {
          const workers = getNumber("details-workers");
          await startJob("product_details", {
            db: getText("details-db"),
            source: getText("details-source"),
            limit: workers > 1 ? 0 : getNumber("details-limit"),
            delay_min: Number(document.getElementById("details-delay-min").value || "0.5"),
            delay_max: Number(document.getElementById("details-delay-max").value || "1.5"),
            selenium: getChecked("details-selenium"),
            headless: getChecked("details-headless"),
            workers
          });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("run-card-details").addEventListener("click", async () => {
        try {
          const workers = getNumber("card-details-workers");
          await startJob("card_details", {
            db: getText("card-details-db"),
            source: getText("card-details-source"),
            limit: workers > 1 ? 0 : getNumber("card-details-limit"),
            delay_min: Number(document.getElementById("card-details-delay-min").value || "0.5"),
            delay_max: Number(document.getElementById("card-details-delay-max").value || "1.5"),
            selenium: getChecked("card-details-selenium"),
            headless: getChecked("card-details-headless"),
            workers
          });
        } catch (error) {
          alert(error.message);
        }
      });

      document.getElementById("refresh-status").addEventListener("click", refreshStatus);
      document.getElementById("stop-job").addEventListener("click", async () => {
        try {
          await stopActiveJob();
        } catch (error) {
          alert(error.message);
        }
      });
      document.getElementById("clear-log").addEventListener("click", () => {
        document.getElementById("terminal").innerHTML = '<span class="terminal-empty">View cleared. Run or refresh a process to load output again.</span>';
        state.lastRenderedLog = "";
      });
    }

    setTodayDefault();
    setYesterdayDefault();
    wireEvents();
    refreshStatus();
  </script>
</body>
</html>
"""


class JobStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.active_job = None
        self.last_job = None

    def snapshot(self):
        with self.lock:
            return {
                "active_job": self._serialize(self.active_job),
                "last_job": self._serialize(self.last_job),
            }

    def start(self, job):
        with self.lock:
            if self.active_job and self.active_job["status"] == "running":
                raise RuntimeError("A job is already running. Wait for it to finish before starting another.")
            self.active_job = job
            self.last_job = job

    def append_log(self, job_id, chunk):
        with self.lock:
            job = self._find(job_id)
            if job:
                job["log"] += chunk

    def finish(self, job_id, returncode):
        with self.lock:
            job = self._find(job_id)
            if not job:
                return
            job["returncode"] = returncode
            if job["status"] != "stopped":
                job["status"] = "completed" if returncode == 0 else "failed"
            job["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            job["process"] = None
            self.last_job = job
            if self.active_job and self.active_job["id"] == job_id:
                self.active_job = None

    def attach_process(self, job_id, process):
        with self.lock:
            job = self._find(job_id)
            if job:
                job["process"] = process
                job["pid"] = process.pid

    def stop_active(self):
        with self.lock:
            job = self.active_job
            if not job or job["status"] != "running":
                raise RuntimeError("No active job is running.")
            process = job.get("process")
            if not process:
                raise RuntimeError("Active job has not fully started yet. Try again in a moment.")
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except Exception:
                process.terminate()
            job["status"] = "stopping"
            job["log"] += "\n[operator] Stop requested. Waiting for process to exit...\n"
            return self._serialize(job)

    def _find(self, job_id):
        if self.active_job and self.active_job["id"] == job_id:
            return self.active_job
        if self.last_job and self.last_job["id"] == job_id:
            return self.last_job
        return None

    def _serialize(self, job):
        if not job:
            return None
        serialized = dict(job)
        serialized.pop("process", None)
        return serialized


STORE = JobStore()


def build_command(job_type, args):
    python = "python3"
    if job_type == "scrape":
        workers = max(1, int(args.get("workers", 1) or 1))
        if workers > 1:
            command = [
                python,
                "batch_workers.py",
                "scrape",
                "--db",
                args.get("db", "sealed_market.db"),
                "--csv",
                args.get("csv", "products.csv"),
                "--source",
                args.get("source", "TCGplayer"),
                "--snapshot-date",
                args.get("snapshot_date", ""),
                "--commit-every",
                str(int(args.get("commit_every", 25))),
                "--delay-min",
                str(args.get("delay_min", 2.0)),
                "--delay-max",
                str(args.get("delay_max", 5.0)),
                "--workers",
                str(workers),
            ]
            if not args.get("selenium", True):
                command.append("--no-selenium")
            if args.get("headless"):
                command.append("--headless")
            return command

        command = [python, "populate_db.py"]
        command.extend(["--db", args.get("db", "sealed_market.db")])
        command.extend(["--csv", args.get("csv", "products.csv")])
        command.extend(["--source", args.get("source", "TCGplayer")])
        command.extend(["--snapshot-date", args.get("snapshot_date", "")])
        command.extend(["--limit", str(int(args.get("limit", 0)))])
        command.extend(["--commit-every", str(int(args.get("commit_every", 25)))])
        command.extend(["--delay-min", str(args.get("delay_min", 2.0))])
        command.extend(["--delay-max", str(args.get("delay_max", 5.0))])
        if not args.get("selenium", True):
            command.append("--no-selenium")
        if args.get("headless"):
            command.append("--headless")
        return command

    if job_type == "dashboard":
        command = [
            python,
            "build_dashboard.py",
            "--db",
            args.get("db", "sealed_market.db"),
            "--source",
            args.get("source", "TCGplayer"),
        ]
        return command

    if job_type == "catalog":
        workers = max(1, int(args.get("workers", 1) or 1))
        if workers > 1:
            command = [
                python,
                "batch_workers.py",
                "catalog",
                "--out",
                args.get("out", "products.csv"),
                "--mode",
                args.get("mode", "fresh"),
                "--category-slug",
                args.get("category_slug", "pokemon"),
                "--product-line-name",
                args.get("product_line_name", "pokemon"),
                "--product-type-name",
                args.get("product_type_name", "Sealed Products"),
                "--workers",
                str(workers),
                "--wait-time",
                str(int(args.get("wait_time", 20))),
                "--page-load-timeout",
                str(int(args.get("page_load_timeout", 25))),
                "--retries",
                str(int(args.get("retries", 1))),
            ]
            if args.get("all_pages"):
                command.append("--all")
            else:
                command.extend(["--pages", str(int(args.get("pages", 3)))])
            if args.get("headless"):
                command.append("--headless")
            return command

        command = [
            python,
            "link_scraper.py",
            "--out",
            args.get("out", "products.csv"),
            "--mode",
            args.get("mode", "fresh"),
            "--category-slug",
            args.get("category_slug", "pokemon"),
            "--product-line-name",
            args.get("product_line_name", "pokemon"),
            "--product-type-name",
            args.get("product_type_name", "Sealed Products"),
            "--wait-time",
            str(int(args.get("wait_time", 20))),
            "--page-load-timeout",
            str(int(args.get("page_load_timeout", 25))),
            "--retries",
            str(int(args.get("retries", 1))),
        ]
        if args.get("all_pages"):
            command.append("--all")
        else:
            command.extend(["--pages", str(int(args.get("pages", 3)))])
        if args.get("headless"):
            command.append("--headless")
        return command

    if job_type == "sales":
        workers = max(1, int(args.get("workers", 1) or 1))
        product_id = int(args.get("product_id", 0) or 0)
        product_url = args.get("product_url", "").strip()
        snapshot_file = args.get("snapshot_file", "").strip()

        if workers > 1 and not product_id and not product_url and not snapshot_file:
            command = [
                python,
                "batch_workers.py",
                "sales",
                "--db",
                args.get("db", "sealed_market.db"),
                "--source",
                args.get("source", "TCGplayer"),
                "--workers",
                str(workers),
                "--limit",
                str(int(args.get("limit", 0))),
            ]
            if args.get("all_dates"):
                command.append("--all-dates")
            else:
                sale_date = args.get("sale_date", "").strip()
                if sale_date:
                    command.extend(["--sale-date", sale_date])
            if not args.get("browser_fallback", True):
                command.append("--no-browser-fallback")
            if args.get("headless"):
                command.append("--headless")
            return command

        command = [
            python,
            "sales_ingester.py",
            "--db",
            args.get("db", "sealed_market.db"),
            "--source",
            args.get("source", "TCGplayer"),
        ]
        if product_id > 0:
            command.extend(["--product-id", str(product_id)])
        if product_url:
            command.extend(["--product-url", product_url])
        if snapshot_file:
            command.extend(["--snapshot-file", snapshot_file])
        command.extend(["--limit", str(int(args.get("limit", 0)))])
        if args.get("all_dates"):
            command.append("--all-dates")
        else:
            sale_date = args.get("sale_date", "").strip()
            if sale_date:
                command.extend(["--sale-date", sale_date])
        if not args.get("browser_fallback", True):
            command.append("--no-browser-fallback")
        if args.get("headless"):
            command.append("--headless")
        return command

    if job_type == "card_sales":
        workers = max(1, int(args.get("workers", 1) or 1))
        product_id = int(args.get("product_id", 0) or 0)
        product_url = args.get("product_url", "").strip()

        if workers > 1 and not product_id and not product_url:
            command = [
                python,
                "batch_workers.py",
                "sales",
                "--db",
                args.get("db", "sealed_market.db"),
                "--source",
                args.get("source", "TCGplayer Cards"),
                "--target-kind",
                "cards",
                "--workers",
                str(workers),
                "--limit",
                str(int(args.get("limit", 0))),
            ]
            if args.get("all_dates"):
                command.append("--all-dates")
            else:
                sale_date = args.get("sale_date", "").strip()
                if sale_date:
                    command.extend(["--sale-date", sale_date])
            if not args.get("browser_fallback", True):
                command.append("--no-browser-fallback")
            if args.get("headless"):
                command.append("--headless")
            return command

        command = [
            python,
            "sales_ingester.py",
            "--db",
            args.get("db", "sealed_market.db"),
            "--source",
            args.get("source", "TCGplayer Cards"),
            "--target-kind",
            "cards",
        ]
        if product_id > 0:
            command.extend(["--product-id", str(product_id)])
        if product_url:
            command.extend(["--product-url", product_url])
        command.extend(["--limit", str(int(args.get("limit", 0)))])
        if args.get("all_dates"):
            command.append("--all-dates")
        else:
            sale_date = args.get("sale_date", "").strip()
            if sale_date:
                command.extend(["--sale-date", sale_date])
        if not args.get("browser_fallback", True):
            command.append("--no-browser-fallback")
        if args.get("headless"):
            command.append("--headless")
        return command

    if job_type == "card_catalog":
        return [
            python,
            "card_catalog_refresh.py",
            "--db",
            args.get("db", "sealed_market.db"),
            "--csv",
            args.get("csv", "pokemon_cards.csv"),
            "--category-slug",
            args.get("category_slug", "pokemon"),
            "--product-line-name",
            args.get("product_line_name", "pokemon"),
            "--source",
            args.get("source", "TCGplayer Cards"),
        ]

    if job_type == "card_catalog_scrape":
        workers = max(1, int(args.get("workers", 1) or 1))
        if workers > 1:
            command = [
                python,
                "batch_workers.py",
                "catalog",
                "--out",
                args.get("out", "pokemon_cards.csv"),
                "--mode",
                args.get("mode", "fresh"),
                "--category-slug",
                args.get("category_slug", "pokemon"),
                "--product-line-name",
                args.get("product_line_name", "pokemon"),
                "--product-type-name",
                args.get("product_type_name", "Cards"),
                "--workers",
                str(workers),
                "--wait-time",
                "20",
                "--page-load-timeout",
                "25",
                "--retries",
                "1",
            ]
            if args.get("all_pages"):
                command.append("--all")
            else:
                command.extend(["--pages", str(int(args.get("pages", 5)))])
            if args.get("headless"):
                command.append("--headless")
            return command

        command = [
            python,
            "link_scraper.py",
            "--out",
            args.get("out", "pokemon_cards.csv"),
            "--mode",
            args.get("mode", "fresh"),
            "--category-slug",
            args.get("category_slug", "pokemon"),
            "--product-line-name",
            args.get("product_line_name", "pokemon"),
            "--product-type-name",
            args.get("product_type_name", "Cards"),
            "--wait-time",
            "20",
            "--page-load-timeout",
            "25",
            "--retries",
            "1",
        ]
        if args.get("all_pages"):
            command.append("--all")
        else:
            command.extend(["--pages", str(int(args.get("pages", 5)))])
        if args.get("headless"):
            command.append("--headless")
        return command

    if job_type == "card_pipeline":
        command = [
            python,
            "card_pipeline.py",
            "--db",
            args.get("db", "sealed_market.db"),
            "--csv",
            args.get("out", "pokemon_cards.csv"),
            "--category-slug",
            args.get("category_slug", "pokemon"),
            "--product-line-name",
            args.get("product_line_name", "pokemon"),
            "--product-type-name",
            args.get("product_type_name", "Cards"),
            "--workers",
            str(max(1, int(args.get("workers", 1) or 1))),
            "--mode",
            args.get("mode", "fresh"),
        ]
        if args.get("all_pages"):
            command.append("--all")
        else:
            command.extend(["--pages", str(int(args.get("pages", 5)))])
        if args.get("headless"):
            command.append("--headless")
        return command

    if job_type == "product_details":
        workers = max(1, int(args.get("workers", 1) or 1))
        if workers > 1:
            command = [
                python,
                "batch_workers.py",
                "product-details",
                "--db",
                args.get("db", "sealed_market.db"),
                "--source",
                args.get("source", "TCGplayer Product Details"),
                "--delay-min",
                str(args.get("delay_min", 0.5)),
                "--delay-max",
                str(args.get("delay_max", 1.5)),
                "--workers",
                str(workers),
            ]
            if not args.get("selenium", True):
                command.append("--no-selenium")
            if args.get("headless"):
                command.append("--headless")
            return command

        command = [
            python,
            "product_details_refresh.py",
            "--db",
            args.get("db", "sealed_market.db"),
            "--source",
            args.get("source", "TCGplayer Product Details"),
            "--limit",
            str(int(args.get("limit", 0))),
            "--delay-min",
            str(args.get("delay_min", 0.5)),
            "--delay-max",
            str(args.get("delay_max", 1.5)),
        ]
        if not args.get("selenium", True):
            command.append("--no-selenium")
        if args.get("headless"):
            command.append("--headless")
        return command

    if job_type == "card_details":
        workers = max(1, int(args.get("workers", 1) or 1))
        if workers > 1:
            command = [
                python,
                "batch_workers.py",
                "card-details",
                "--db",
                args.get("db", "sealed_market.db"),
                "--source",
                args.get("source", "TCGplayer Card Details"),
                "--delay-min",
                str(args.get("delay_min", 0.5)),
                "--delay-max",
                str(args.get("delay_max", 1.5)),
                "--workers",
                str(workers),
            ]
            if not args.get("selenium", True):
                command.append("--no-selenium")
            if args.get("headless"):
                command.append("--headless")
            return command

        command = [
            python,
            "card_details_refresh.py",
            "--db",
            args.get("db", "sealed_market.db"),
            "--source",
            args.get("source", "TCGplayer Card Details"),
            "--limit",
            str(int(args.get("limit", 0))),
            "--delay-min",
            str(args.get("delay_min", 0.5)),
            "--delay-max",
            str(args.get("delay_max", 1.5)),
        ]
        if not args.get("selenium", True):
            command.append("--no-selenium")
        if args.get("headless"):
            command.append("--headless")
        return command

    raise ValueError(f"Unsupported job type: {job_type}")


def launch_job(job_type, args):
    command = build_command(job_type, args)
    job = {
        "id": uuid.uuid4().hex[:10],
        "job_type": job_type,
        "command": shlex.join(command),
        "status": "running",
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "finished_at": None,
        "returncode": None,
        "log": f"$ {shlex.join(command)}\n",
        "pid": None,
        "process": None,
    }
    STORE.start(job)

    def runner():
        process = subprocess.Popen(
            command,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        STORE.attach_process(job["id"], process)
        try:
            assert process.stdout is not None
            for line in process.stdout:
                STORE.append_log(job["id"], line)
        finally:
            returncode = process.wait()
            if returncode == -signal.SIGTERM:
                STORE.append_log(job["id"], "[operator] Process terminated.\n")
                with STORE.lock:
                    finished = STORE._find(job["id"])
                    if finished:
                        finished["status"] = "stopped"
            STORE.finish(job["id"], returncode)

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    return STORE._serialize(job)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(HTML_PAGE)
            return
        if parsed.path == "/api/status":
            self._send_json(STORE.snapshot())
            return
        if parsed.path.startswith("/dashboard/"):
            self._serve_file(ROOT / parsed.path.lstrip("/"))
            return
        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/jobs":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            payload = json.loads(raw or "{}")
            job_type = payload.get("job_type", "")
            args = payload.get("args", {})
            try:
                job = launch_job(job_type, args)
            except (RuntimeError, ValueError) as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json({"ok": True, "job": job})
            return
        if parsed.path == "/api/jobs/stop":
            try:
                job = STORE.stop_active()
            except RuntimeError as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json({"ok": True, "job": job})
            return
        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format, *args):
        return

    def _serve_file(self, path):
        if not path.exists() or not path.is_file():
            self._send_json({"error": "File not found"}, status=HTTPStatus.NOT_FOUND)
            return
        body = path.read_bytes()
        content_type = "text/html; charset=utf-8" if path.suffix == ".html" else "text/plain; charset=utf-8"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, body):
        encoded = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Operator console running at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down operator console...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
