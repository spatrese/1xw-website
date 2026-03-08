#!/usr/bin/env python3
"""
ig_calendar_probe_v2.py

More robust probe for IG economic calendar:
- avoids waiting for networkidle
- saves partial HTML even if page keeps background requests open
- logs candidate XHR/fetch/document responses
"""

from pathlib import Path
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
        browser = p.chromium.launch(headless=False, slow_mo=100)
        context = browser.new_context(
            viewport={"width": 1440, "height": 2200},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"),
            locale="en-GB",
        )
        page = context.new_page()
        page.set_default_timeout(15000)

        def on_response(resp):
            try:
                url = resp.url
                ctype = (resp.headers or {}).get("content-type", "")
                rtype = resp.request.resource_type
                if is_interesting(url) or "json" in ctype.lower() or rtype in ("xhr", "fetch"):
                    captured.append({
                        "url": url,
                        "status": resp.status,
                        "content_type": ctype,
                        "resource_type": rtype,
                    })
            except Exception:
                pass

        page.on("response", on_response)

        print(f"Opening {URL} ...")
        try:
            page.goto(URL, wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            print("goto warning:", e)

        # Give the page time to hydrate instead of waiting for networkidle
        for state in ("domcontentloaded", "load"):
            try:
                page.wait_for_load_state(state, timeout=10000)
            except Exception:
                pass
        page.wait_for_timeout(8000)

        # Try cookie buttons heuristically
        for label in [
            "Accept", "Accept all", "I accept", "Agree", "OK", "Got it", "Continue"
        ]:
            try:
                page.get_by_role("button", name=label).click(timeout=1500)
                page.wait_for_timeout(1500)
                break
            except Exception:
                pass

        html_path = OUTDIR / "rendered_initial.html"
        html_path.write_text(page.content(), encoding="utf-8")
        print(f"Saved {html_path}")

        # Try tabs / buttons that may trigger requests
        tab_names = [
            "Macroeconomic", "Economic", "Earnings", "Dividends",
            "IPOs", "Stock splits"
        ]
        for tab in tab_names:
            try:
                page.get_by_role("tab", name=tab).click(timeout=2000)
                page.wait_for_timeout(2500)
            except Exception:
                try:
                    page.get_by_role("button", name=tab).click(timeout=2000)
                    page.wait_for_timeout(2500)
                except Exception:
                    pass

        # Scroll to trigger lazy load
        for _ in range(6):
            page.mouse.wheel(0, 1600)
            page.wait_for_timeout(1200)

        html_path_2 = OUTDIR / "rendered_after_interaction.html"
        html_path_2.write_text(page.content(), encoding="utf-8")
        print(f"Saved {html_path_2}")

        uniq = {}
        for item in captured:
            uniq[item["url"]] = item
        interesting = sorted(uniq.values(), key=lambda x: x["url"])

        txt_path = OUTDIR / "network_candidates.txt"
        with txt_path.open("w", encoding="utf-8") as f:
            for item in interesting:
                f.write(
                    f'{item["status"]}\t{item["resource_type"]}\t'
                    f'{item["content_type"]}\t{item["url"]}\n'
                )
        print(f"Saved {txt_path}")

        print("\nCandidate network URLs:\n")
        for item in interesting[:120]:
            print(
                f'{item["status"]:>3}  {item["resource_type"]:<8}  '
                f'{item["content_type"][:40]:40}  {item["url"]}'
            )

        browser.close()

if __name__ == "__main__":
    main()
