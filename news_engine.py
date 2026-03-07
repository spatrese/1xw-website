import argparse
import json
import os
import re
from datetime import datetime, timezone, timedelta

import feedparser
import requests

ASSET_CLASSES = ["Equities", "Rates", "FX", "Commodities", "Crypto"]
RSS_SOURCES = [
    {"name": "Federal Reserve (Monetary Policy)", "url": "https://www.federalreserve.gov/feeds/press_monetary.xml", "hint": "Rates"},
    {"name": "ECB (Press/Speeches/Interviews)", "url": "https://www.ecb.europa.eu/rss/press.html", "hint": "Rates"},
    {"name": "CNBC Top News", "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html", "hint": "Equities"},
    {"name": "EIA Today in Energy", "url": "https://www.eia.gov/rss/todayinenergy.xml", "hint": "Commodities"},
    {"name": "CoinDesk", "url": "https://feeds.feedburner.com/CoinDesk", "hint": "Crypto"},
]
KEYWORDS = {
    "Rates": [r"fed", r"fomc", r"powell", r"rate(s)?", r"yield(s)?", r"treasury", r"ecb", r"central bank", r"bundesbank", r"boj", r"inflation", r"cpi", r"ppi", r"minutes", r"policy"],
    "FX": [r"dollar", r"dxy", r"eur", r"euro", r"gbp", r"pound", r"jpy", r"yen", r"fx", r"foreign exchange", r"currency"],
    "Commodities": [r"oil", r"brent", r"wti", r"gold", r"silver", r"copper", r"natural gas", r"opec", r"inventory", r"eia", r"commodit(y|ies)"],
    "Crypto": [r"bitcoin", r"btc", r"ethereum", r"eth", r"crypto", r"stablecoin", r"sec", r"etf", r"exchange"],
    "Equities": [r"stock(s)?", r"equities", r"earnings", r"s&p", r"nasdaq", r"dow", r"ipo", r"shares", r"profit", r"guidance"]
}
IMPORTANT_KWS = ["fed", "fomc", "ecb", "inflation", "cpi", "rates", "yield", "opec", "etf", "sec"]


def iso_now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def week_key_local():
    today = datetime.now()
    y, w, _ = today.isocalendar()
    return f"{y}-W{int(w):02d}"


def strip_html(s: str) -> str:
    s = s or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_text(s: str) -> str:
    return strip_html(s).lower()


def within_days(published_struct, days: int) -> bool:
    if not published_struct:
        return True
    try:
        dt = datetime(*published_struct[:6], tzinfo=timezone.utc)
        return dt >= datetime.now(timezone.utc) - timedelta(days=days)
    except Exception:
        return True


def iso_from_struct(published_struct) -> str:
    if not published_struct:
        return ""
    try:
        return datetime(*published_struct[:6], tzinfo=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return ""


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


def rank_score(title: str, summary: str, published_struct) -> float:
    base = 0.0
    text = normalize_text(title) + " " + normalize_text(summary)
    if published_struct:
        try:
            dt = datetime(*published_struct[:6], tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
            base += max(0.0, 72.0 - age_hours) / 72.0
        except Exception:
            pass
    for kw in IMPORTANT_KWS:
        if kw in text:
            base += 0.25
    if len(strip_html(title)) >= 50:
        base += 0.10
    return float(base)


def fetch_feed(url: str, timeout=15):
    headers = {"User-Agent": "1XW-NewsBot/2.1 (+https://1xwtrading.com)"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return feedparser.parse(r.text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="Lookback window for RSS items")
    parser.add_argument("--per_class", type=int, default=5, help="Max items per asset class")
    args = parser.parse_args()

    wk = week_key_local()
    updatedAt = iso_now_utc()
    buckets = {ac: [] for ac in ASSET_CLASSES}

    for src in RSS_SOURCES:
        name = src["name"]
        url = src["url"]
        hint = src.get("hint", "")
        try:
            feed = fetch_feed(url)
        except Exception as e:
            print(f"⚠️ feed failed: {name} ({e})")
            continue

        entries = getattr(feed, "entries", []) or []
        for ent in entries[:70]:
            title = strip_html(ent.get("title", ""))
            link = (ent.get("link", "") or "").strip()
            summary = strip_html(ent.get("summary", "") or ent.get("description", ""))
            published = ent.get("published_parsed") or ent.get("updated_parsed")
            if not within_days(published, args.days):
                continue
            if not title:
                continue

            ac = classify_item(title, summary, hint)
            score = rank_score(title, summary, published)
            buckets[ac].append({
                "title": title,
                "source": name,
                "url": link,
                "summary": summary[:260],
                "score": round(score, 3),
                "date": iso_from_struct(published),
            })

    for ac in ASSET_CLASSES:
        seen = set()
        dedup = []
        for it in sorted(buckets[ac], key=lambda x: x["score"], reverse=True):
            key = it.get("url") or (it.get("title", "") + "|" + it.get("source", ""))
            if key in seen:
                continue
            seen.add(key)
            dedup.append(it)
        buckets[ac] = dedup[: max(0, int(args.per_class))]

    os.makedirs("content", exist_ok=True)
    digest_path = "content/news_digest.json"
    digest = {"updatedAt": updatedAt, "week": wk, "by_asset_class": buckets}
    with open(digest_path, "w", encoding="utf-8") as f:
        json.dump(digest, f, ensure_ascii=False, indent=2)
    print(f"✅ Wrote: {digest_path}")


if __name__ == "__main__":
    main()
