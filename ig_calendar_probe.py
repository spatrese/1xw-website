#!/usr/bin/env python3
"""
ig_calendar_probe.py

Purpose:
- Open IG economic calendar in a real browser context
- Capture network requests that may contain the calendar data
- Save rendered HTML for inspection
- Print candidate API/XHR URLs

Usage:
    python ig_calendar_probe.py

Requirements:
    pip install playwright
    playwright install chromium
"""

from pathlib import Path
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

URL = "https://www.ig.com/en/economic-calendar"
OUTDIR = Path("debug_ig_calendar")
OUTDIR.mkdir(exist_ok=True)

KEYWORDS = (
    "calendar", "economic", "events", "event", "macro",
    "earnings", "dividend", "ipo", "split", "graphql", "api"
)

def is_interesting(url: str) -> bool:
    u = url.lower()
    return any(k in u for k in KEYWORDS)

def main():
    captured = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1440, "height": 2200},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"),
            locale="en-GB",
        )
        page = context.new_page()

        def on_response(resp):
            try:
                url = resp.url
                ctype = (resp.headers or {}).get("content-type", "")
                if is_interesting(url) or "json" in ctype.lower():
                    captured.append({
                        "url": url,
                        "status": resp.status,
                        "content_type": ctype,
                    })
            except Exception:
                pass

        page.on("response", on_response)

        print(f"Opening {URL} ...")
        page.goto(URL, wait_until="networkidle", timeout=90000)

        # Try to dismiss cookie banners / modal buttons heuristically
        for label in [
            "Accept", "Accept all", "I accept", "Agree", "OK", "Got it"
        ]:
            try:
                page.get_by_role("button", name=label).click(timeout=1500)
                break
            except Exception:
                pass

        # Save initial rendered page
        html_path = OUTDIR / "rendered_initial.html"
        html_path.write_text(page.content(), encoding="utf-8")
        print(f"Saved {html_path}")

        # Try some generic interactions to force lazy-loaded data
        # Tabs mentioned on the page: macroeconomic, earnings, dividends, IPOs, stock splits
        tab_names = [
            "Macroeconomic", "Economic", "Earnings", "Dividends",
            "IPOs", "Stock splits"
        ]
        for tab in tab_names:
            try:
                page.get_by_role("tab", name=tab).click(timeout=2000)
                page.wait_for_timeout(2500)
            except Exception:
                pass

        # Scroll to encourage lazy loading
        for _ in range(8):
            page.mouse.wheel(0, 1800)
            page.wait_for_timeout(1000)

        html_path_2 = OUTDIR / "rendered_after_interaction.html"
        html_path_2.write_text(page.content(), encoding="utf-8")
        print(f"Saved {html_path_2}")

        # Unique + sorted responses
        uniq = {}
        for item in captured:
            uniq[item["url"]] = item
        interesting = sorted(uniq.values(), key=lambda x: x["url"])

        txt_path = OUTDIR / "network_candidates.txt"
        with txt_path.open("w", encoding="utf-8") as f:
            for item in interesting:
                f.write(f'{item["status"]}\t{item["content_type"]}\t{item["url"]}\n')
        print(f"Saved {txt_path}")

        print("\nCandidate network URLs:\n")
        for item in interesting[:120]:
            print(f'{item["status"]:>3}  {item["content_type"][:40]:40}  {item["url"]}')

        # Optional: save bodies for JSON endpoints
        saved = 0
        for resp in page.context.pages[0].context.request.storage_state():
            pass  # no-op; kept to avoid confusion with request context API

        browser.close()

if __name__ == "__main__":
    main()
