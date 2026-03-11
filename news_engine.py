#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import feedparser
import requests
from bs4 import BeautifulSoup

ASSET_CLASSES = ["Equities", "Rates", "FX", "Commodities", "Crypto"]
DEFAULT_TIMEOUT = 20
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}

SOURCES: List[Dict[str, Any]] = [
    # Central banks / institutions
    {"name": "Federal Reserve", "type": "rss", "url": "https://www.federalreserve.gov/feeds/press_all.xml", "hint": "Rates", "tier": 1},
    {"name": "ECB", "type": "rss", "url": "https://www.ecb.europa.eu/rss/press.html", "hint": "Rates", "tier": 1},
    {"name": "Bank of England", "type": "rss", "url": "https://www.bankofengland.co.uk/rss/news", "hint": "Rates", "tier": 1},
    {"name": "Bank of Japan", "type": "rss", "url": "https://www.boj.or.jp/en/rss/whatsnew.xml", "hint": "Rates", "tier": 1},

    {"name": "PBOC", "type": "html", "url": "https://www.pbc.gov.cn/english/130721/index.html", "hint": "Rates", "tier": 1},

    # Energy / macro
    {"name": "EIA Today in Energy", "type": "rss", "url": "https://www.eia.gov/rss/todayinenergy.xml", "hint": "Commodities", "tier": 1},
    {"name": "FXStreet", "type": "rss","url": "https://www.fxstreet.com/rss/news","hint": "FX", "tier": 1},
    
    # Market news
    {"name": "CNBC", "type": "rss", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "hint": "Equities", "tier": 2},
    {"name": "Financial Times", "type": "rss", "url": "https://www.ft.com/?format=rss", "hint": "Equities", "tier": 2},
    {"name": "Yahoo Finance", "type": "rss", "url": "https://finance.yahoo.com/news/rssindex", "hint": "Equities", "tier": 2},
    {"name": "Investing.com", "type": "rss", "url": "https://www.investing.com/rss/news.rss", "hint": "Equities", "tier": 2},
    {"name": "Finviz News", "type": "finviz", "url": "https://finviz.com/news.ashx", "hint": "Equities", "tier": 1},

    # Crypto
    {"name": "CoinDesk", "type": "rss", "url": "https://feeds.feedburner.com/CoinDesk", "hint": "Crypto", "tier": 2},
]

KEYWORDS = {
    "Rates": [
        r"\bfed\b", r"\becb\b", r"\bboj\b", r"\bboe\b", r"\bimf\b", r"\bpboc\b",
        r"\brates?\b", r"\byields?\b", r"\bbonds?\b", r"\btreasur(y|ies)\b",
        r"\binflation\b", r"\bcpi\b", r"\bppi\b", r"\bcentral bank\b", r"\bpolicy\b",
        r"\bmonetary\b", r"\bliquidity\b", r"\binterest rate\b"
    ],
    "FX": [
        r"\bdollar\b", r"\beuro\b", r"\byen\b", r"\bsterling\b", r"\bfx\b", r"\bcurrency\b",
        r"\bforeign exchange\b", r"\bdxy\b", r"\busd\b", r"\beur\b", r"\bjpy\b", r"\bgbp\b",
        r"\brmb\b", r"\byuan\b", r"\brenminbi\b"
    ],
    "Commodities": [
        r"\boil\b", r"\bbrent\b", r"\bwti\b", r"\bgold\b", r"\bsilver\b", r"\bcopper\b",
        r"\bnatural gas\b", r"\bopec\b", r"\bcommodit(y|ies)\b", r"\benergy\b", r"\beia\b"
    ],
    "Crypto": [
        r"\bbitcoin\b", r"\bbtc\b", r"\beth\b", r"\bethereum\b", r"\bcrypto\b",
        r"\bblockchain\b", r"\betf\b", r"\bstablecoin\b"
    ],
    "Equities": [
        r"\bstocks?\b", r"\bequities\b", r"\bearnings\b", r"\bshares\b", r"\bnasdaq\b",
        r"\bs&p\b", r"\bdow\b", r"\bprofit\b", r"\bguidance\b", r"\bipo\b", r"\bdeal(s)?\b"
    ],
}

IMPORTANT_KWS = [
    "fed", "fomc", "ecb", "boj", "pboc", "imf", "inflation", "cpi", "ppi", "rates", "yield",
    "opec", "oil", "earnings", "guidance", "etf", "sec", "yuan", "renminbi"
]


def iso_now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def week_key_local() -> str:
    y, w, _ = datetime.now().isocalendar()
    return f"{y}-W{int(w):02d}"


def strip_html(s: str) -> str:
    s = s or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_text(s: str) -> str:
    return strip_html(s).lower()


def within_days(dt: Optional[datetime], days: int) -> bool:
    if not dt:
        return True
    return dt >= datetime.now(timezone.utc) - timedelta(days=days)


def iso_from_datetime(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_possible_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    if isinstance(value, (tuple, list)) and len(value) >= 6:
        try:
            return datetime(*value[:6], tzinfo=timezone.utc)
        except Exception:
            return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d %B %Y",
        "%b %d, %Y",
        "%B %d, %Y",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def classify_item(title: str, summary: str, source_hint: str) -> str:
    text = normalize_text(title) + " " + normalize_text(summary)
    scores = {ac: 0 for ac in ASSET_CLASSES}
    for ac, pats in KEYWORDS.items():
        for p in pats:
            if re.search(p, text, flags=re.IGNORECASE):
                scores[ac] += 1
    best = max(scores, key=lambda k: scores[k])
    if scores[best] == 0 and source_hint in ASSET_CLASSES:
        return source_hint
    top_score = scores[best]
    tied = [ac for ac, sc in scores.items() if sc == top_score and sc > 0]
    if len(tied) > 1 and source_hint in tied:
        return source_hint
    return best


def rank_score(title: str, summary: str, published_dt: Optional[datetime], tier: int) -> float:
    base = {1: 0.45, 2: 0.25}.get(tier, 0.0)
    text = normalize_text(title) + " " + normalize_text(summary)
    if published_dt:
        age_hours = (datetime.now(timezone.utc) - published_dt).total_seconds() / 3600.0
        base += max(0.0, 72.0 - age_hours) / 72.0
    for kw in IMPORTANT_KWS:
        if kw in text:
            base += 0.20
    if len(strip_html(title)) >= 50:
        base += 0.10
    return round(float(base), 3)


def fetch_text(url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def publisher_from_url(url: str) -> str:
    u = (url or "").lower()
    mapping = [
        ("reuters.com", "Reuters"),
        ("bloomberg.com", "Bloomberg"),
        ("wsj.com", "WSJ"),
        ("ft.com", "Financial Times"),
        ("cnbc.com", "CNBC"),
        ("marketwatch.com", "MarketWatch"),
        ("barrons.com", "Barron's"),
        ("finance.yahoo.com", "Yahoo Finance"),
        ("yahoo.com", "Yahoo Finance"),
        ("investing.com", "Investing.com"),
        ("seekingalpha.com", "Seeking Alpha"),
        ("nytimes.com", "New York Times"),
        ("bbc.com", "BBC"),
        ("zerohedge.com", "ZeroHedge"),
    ]
    for needle, label in mapping:
        if needle in u:
            return label
    host = re.sub(r"^https?://", "", u).split("/")[0]
    host = re.sub(r"^www\.", "", host)
    return host or "Finviz"


def parse_finviz_source(src: Dict[str, Any], days: int) -> List[Dict[str, Any]]:
    html = fetch_text(src["url"])
    soup = BeautifulSoup(html, "lxml")
    items: List[Dict[str, Any]] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        title = strip_html(a.get_text(" ", strip=True))
        if not href or not title or len(title) < 20:
            continue
        link = urljoin(src["url"], href)
        if link.startswith(src["url"]):
            continue
        if any(bad in link for bad in ["finviz.com", "elite.finviz.com"]):
            continue
        key = (title.lower(), link)
        if key in seen:
            continue
        seen.add(key)
        items.append({
            "title": title,
            "summary": "",
            "link": link,
            "published": None,
            "publisher": publisher_from_url(link),
        })
    return items[:80]


def parse_rss_source(src: Dict[str, Any], days: int) -> List[Dict[str, Any]]:
    text = fetch_text(src["url"])
    feed = feedparser.parse(text)
    out: List[Dict[str, Any]] = []
    for ent in (getattr(feed, "entries", []) or [])[:80]:
        title = strip_html(ent.get("title", ""))
        if not title:
            continue
        summary = strip_html(ent.get("summary", "") or ent.get("description", ""))
        link = (ent.get("link", "") or "").strip()
        published = parse_possible_datetime(ent.get("published_parsed") or ent.get("updated_parsed") or ent.get("published") or ent.get("updated"))
        if not within_days(published, days):
            continue
        out.append({"title": title, "summary": summary, "link": link, "published": published})
    return out


def parse_html_source(src: Dict[str, Any], days: int) -> List[Dict[str, Any]]:
    html = fetch_text(src["url"])
    soup = BeautifulSoup(html, "lxml")
    name = src["name"]
    items: List[Dict[str, Any]] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        title = strip_html(a.get_text(" ", strip=True))
        href = a.get("href", "").strip()
        if not title or not href:
            continue

        if name == "PBOC":
            if len(title) < 24:
                continue
            link = urljoin(src["url"], href)
            if "pbc.gov.cn" not in link:
                continue
            if "/english/" not in link:
                continue
            if any(bad in title.lower() for bad in ["home", "about pbc", "management team", "former governors", "site map"]):
                continue
        else:
            if len(title) < 22:
                continue
            link = urljoin(src["url"], href)

        key = (title.lower(), link)
        if key in seen:
            continue
        seen.add(key)
        items.append({"title": title, "summary": "", "link": link, "published": None})

    return items[:60]


def parse_source(src: Dict[str, Any], days: int) -> List[Dict[str, Any]]:
    st = src.get("type")
    if st == "rss":
        return parse_rss_source(src, days)
    if st == "html":
        return parse_html_source(src, days)
    if st == "finviz":
        return parse_finviz_source(src, days)
    return []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Lookback window for items")
    parser.add_argument("--per_class", type=int, default=5, help="Max items per asset class")
    parser.add_argument("--out", default="content/news_digest.json")
    args = parser.parse_args()

    wk = week_key_local()
    updated_at = iso_now_utc()
    buckets: Dict[str, List[Dict[str, Any]]] = {ac: [] for ac in ASSET_CLASSES}

    for src in SOURCES:
        try:
            entries = parse_source(src, args.days)
        except Exception as e:
            print(f"⚠️ source failed: {src['name']} ({e})")
            continue

        if not entries:
            print(f"⚠️ source empty: {src['name']}")
            continue

        for ent in entries:
            title = strip_html(ent.get("title", ""))
            if not title:
                continue
            summary = strip_html(ent.get("summary", ""))
            link = (ent.get("link", "") or "").strip()
            published = ent.get("published")
            if not within_days(published, args.days):
                continue

            ac = classify_item(title, summary, src.get("hint", ""))
            score = rank_score(title, summary, published, int(src.get("tier", 2)))
            buckets[ac].append(
                {
                    "title": title,
                    "source": ent.get("publisher") or src["name"],
                    "url": link,
                    "summary": summary[:260],
                    "score": score,
                    "date": iso_from_datetime(published) or updated_at,
                }
            )

    for ac in ASSET_CLASSES:
        seen = set()
        deduped: List[Dict[str, Any]] = []
        for item in sorted(buckets[ac], key=lambda x: x["score"], reverse=True):
            key = item.get("url") or (item.get("title", "") + "|" + item.get("source", ""))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        buckets[ac] = deduped[: max(0, int(args.per_class))]

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    digest = {"updatedAt": updated_at, "week": wk, "by_asset_class": buckets}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)
    print(f"✅ Wrote: {args.out}")


if __name__ == "__main__":
    main()
