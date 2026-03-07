import argparse
import html
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from urllib.parse import urljoin

import feedparser
import requests

ASSET_CLASSES = ["Equities", "Rates", "FX", "Commodities", "Crypto"]

DEFAULT_USER_AGENT = "1XW-NewsBot"
DEFAULT_TIMEOUT = 20


# =========================================================
# ALL SOURCES ACTIVE
# =========================================================

SOURCES: List[Dict[str, Any]] = [

    # Central Banks / Institutions
    {"name": "Federal Reserve", "type": "rss",
     "url": "https://www.federalreserve.gov/feeds/press_all.xml",
     "hint": "Rates", "tier": 1},

    {"name": "ECB", "type": "rss",
     "url": "https://www.ecb.europa.eu/rss/press.html",
     "hint": "Rates", "tier": 1},

    {"name": "Bank of England", "type": "rss",
     "url": "https://www.bankofengland.co.uk/rss/news",
     "hint": "Rates", "tier": 1},

    {"name": "Bank of Japan", "type": "rss",
     "url": "https://www.boj.or.jp/en/rss/whatsnew.xml",
     "hint": "Rates", "tier": 1},

    {"name": "BIS", "type": "rss",
     "url": "https://www.bis.org/rss/index.htm",
     "hint": "Rates", "tier": 1},

    {"name": "IMF", "type": "rss",
     "url": "https://www.imf.org/en/News/RSS",
     "hint": "Rates", "tier": 1},

    {"name": "World Bank", "type": "rss",
     "url": "https://www.worldbank.org/en/news/all/rss",
     "hint": "Rates", "tier": 1},

    {"name": "OECD", "type": "rss",
     "url": "https://www.oecd.org/newsroom/rss.xml",
     "hint": "Rates", "tier": 1},

    # Energy
    {"name": "EIA Today in Energy", "type": "rss",
     "url": "https://www.eia.gov/rss/todayinenergy.xml",
     "hint": "Commodities", "tier": 1},

    # Market News
    {"name": "CNBC", "type": "rss",
     "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
     "hint": "Equities", "tier": 2},

    {"name": "Financial Times", "type": "rss",
     "url": "https://www.ft.com/?format=rss",
     "hint": "Equities", "tier": 2},

    {"name": "Yahoo Finance", "type": "rss",
     "url": "https://finance.yahoo.com/news/rssindex",
     "hint": "Equities", "tier": 2},

    {"name": "Investing.com", "type": "rss",
     "url": "https://www.investing.com/rss/news.rss",
     "hint": "Equities", "tier": 2},

    # Crypto
    {"name": "CoinDesk", "type": "rss",
     "url": "https://feeds.feedburner.com/CoinDesk",
     "hint": "Crypto", "tier": 2},

]

# =========================================================
# KEYWORDS
# =========================================================

KEYWORDS = {

    "Rates": [
        r"\bfed\b", r"\becb\b", r"\bboj\b", r"\bboe\b",
        r"\brates?\b", r"\byields?\b", r"\bbonds?\b",
        r"\binflation\b", r"\bcpi\b"
    ],

    "FX": [
        r"\bdollar\b", r"\beuro\b", r"\byen\b",
        r"\bcurrency\b", r"\bfx\b"
    ],

    "Commodities": [
        r"\boil\b", r"\bbrent\b", r"\bgold\b",
        r"\bcopper\b", r"\bcommodit"
    ],

    "Crypto": [
        r"\bbitcoin\b", r"\beth\b", r"\bcrypto\b",
        r"\bblockchain\b"
    ],

    "Equities": [
        r"\bstocks?\b", r"\bequities\b", r"\bearnings\b",
        r"\bshares\b", r"\bnasdaq\b", r"\bs&p\b"
    ]

}


# =========================================================
# UTILS
# =========================================================

def iso_now():
    return datetime.now(timezone.utc).isoformat()


def week_key():
    y, w, _ = datetime.now().isocalendar()
    return f"{y}-W{w:02d}"


def strip_html(text):
    text = html.unescape(text or "")
    return re.sub(r"<[^>]+>", "", text).strip()


def fetch(url):
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    r = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.text


# =========================================================
# CLASSIFICATION
# =========================================================

def classify(title, summary, hint):

    text = (title + " " + summary).lower()

    scores = {a: 0 for a in ASSET_CLASSES}

    for asset, patterns in KEYWORDS.items():
        for p in patterns:
            if re.search(p, text):
                scores[asset] += 1

    if hint in scores:
        scores[hint] += 2

    return max(scores, key=scores.get)


# =========================================================
# RANKING
# =========================================================

def rank(title, summary, tier):

    score = 0.0

    score += {1: 0.45, 2: 0.25}.get(tier, 0)

    if len(title) > 60:
        score += 0.05

    if "fed" in title.lower():
        score += 0.1

    return round(score, 3)


# =========================================================
# MAIN
# =========================================================

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--per_class", type=int, default=5)
    parser.add_argument("--out", default="content/news_digest.json")

    args = parser.parse_args()

    buckets = {a: [] for a in ASSET_CLASSES}

    for source in SOURCES:

        try:

            raw = fetch(source["url"])
            feed = feedparser.parse(raw)

        except Exception as e:

            print(f"⚠️ source failed: {source['name']} ({e})")
            continue

        for e in feed.entries[:40]:

            title = strip_html(e.get("title", ""))
            summary = strip_html(e.get("summary", ""))

            asset = classify(title, summary, source["hint"])

            buckets[asset].append({

                "title": title,
                "source": source["name"],
                "url": e.get("link", ""),
                "summary": summary[:200],
                "score": rank(title, summary, source["tier"]),
                "date": iso_now()

            })

    for a in ASSET_CLASSES:

        buckets[a] = sorted(
            buckets[a],
            key=lambda x: x["score"],
            reverse=True
        )[:args.per_class]

    out = {
        "updatedAt": iso_now(),
        "week": week_key(),
        "by_asset_class": buckets
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    with open(args.out, "w", encoding="utf8") as f:

        json.dump(out, f, indent=2)

    print("✅ Wrote:", args.out)


if __name__ == "__main__":
    main()