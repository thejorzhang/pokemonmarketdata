"""Export a manually signed-in TCGplayer browser session to JSON.

Usage:
    python3 launch_tcgplayer_profile.py --remote-debugging-port 9222
    # sign in manually in the opened Chrome window
    python3 export_tcgplayer_session.py --remote-debugging-port 9222 --output tcgplayer_session.json

The exported JSON can be fed back into sales_ingester.py via --session-file.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def attach_driver(remote_debugging_port):
    opts = Options()
    opts.add_experimental_option("debuggerAddress", f"127.0.0.1:{int(remote_debugging_port)}")
    return webdriver.Chrome(options=opts)


def main():
    parser = argparse.ArgumentParser(description="Export cookies/local storage from a signed-in TCGplayer Chrome session")
    parser.add_argument("--remote-debugging-port", type=int, default=9222, help="Chrome remote debugging port")
    parser.add_argument("--output", default="tcgplayer_session.json", help="Path to write the exported session JSON")
    parser.add_argument("--url", default="https://www.tcgplayer.com/", help="Page to load before reading storage")
    args = parser.parse_args()

    driver = attach_driver(args.remote_debugging_port)
    try:
        driver.get(args.url)
        cookies_payload = driver.execute_cdp_cmd("Network.getAllCookies", {})
        cookies = cookies_payload.get("cookies") or []
        local_storage = driver.execute_script("return Object.fromEntries(Object.entries(localStorage));")
        session_storage = driver.execute_script("return Object.fromEntries(Object.entries(sessionStorage));")
        payload = {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source_url": args.url,
            "cookies": cookies,
            "local_storage": local_storage or {},
            "session_storage": session_storage or {},
        }
        Path(args.output).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        print(f"Wrote {len(cookies)} cookies to {args.output}")
        print("You can now close Chrome if you want; the JSON export is self-contained.")
        return 0
    finally:
        driver.quit()


if __name__ == "__main__":
    raise SystemExit(main())
