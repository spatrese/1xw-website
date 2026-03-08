#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta
from io import StringIO
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

FMP_EARNINGS_URL = "https://financialmodelingprep.com/stable/earnings-calendar"

# Fixed whitelist requested by user; all other earnings rules stay unchanged.
PRIORITY_EARNINGS = {
    "AAPL", "ABBV", "AMAT", "AMD", "AMZN", "ASML", "AVGO", "AXP", "AZN",
    "BABA", "BAC", "BRK-A", "BRK-B", "BRK.A", "BRK.B", "CAT", "COST", "CSCO", "CVX",
    "GE", "GEV", "GOOG", "GOOGL", "GS", "HD", "HSBC", "IBM", "INTC",
    "JNJ", "JPM", "KO", "LIN", "LLY", "LRCX", "MA", "MCD", "META", "MRK",
    "MS", "MSFT", "MU", "NFLX", "NVDA", "NVS", "ORCL", "PEP", "PG",
    "PLTR", "PM", "RTX", "RY", "SAP", "SHEL", "T", "TM", "TMUS", "TSLA",
    "TSM", "UNH", "V", "VZ", "WFC", "WMT", "XOM",
}
CHINA_TICKERS = {"BABA", "TSM"}
EU_TICKERS = {"ASML", "AZN", "HSBC", "NVS", "SAP", "SHEL"}

HEADERS = {
    "User-Agent": "1XW MacroCalendar/1.0 (+https://1xwtrading.com)",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 25


# -------------------------
# Generic helpers
# -------------------------
def week_bounds(today: Optional[date] = None) -> Tuple[date, date]:
    today = today or date.today()
    return today, today + timedelta(days=7)


def in_range(d: date, start: date, end: date) -> bool:
    return start <= d <= end


def session_get_text(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def normalize_space(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def parse_date_any(s: str, default_year: Optional[int] = None) -> Optional[date]:
    s = normalize_space(s)
    if not s:
        return None

    formats = [
        "%A, %B %d, %Y",
        "%A, %B %d %Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%d/%m/%Y",
        "%d %B %Y",
        "%d %b %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass

    if default_year is not None:
        for fmt in ["%B %d", "%b %d"]:
            try:
                return datetime.strptime(f"{s} {default_year}", f"{fmt} %Y").date()
            except Exception:
                pass

    return None


def month_label(month_num: int) -> str:
    return [
        "Jan.", "Feb.", "Mar.", "Apr.", "May", "Jun.",
        "Jul.", "Aug.", "Sep.", "Oct.", "Nov.", "Dec.",
    ][month_num - 1]


def add_event(
    out: List[Dict[str, Any]],
    d: date,
    title: str,
    country: str,
    importance: str,
    markets: List[str],
    source: str,
    url: str = "",
) -> None:
    out.append(
        {
            "date": d.isoformat(),
            "type": "Macro",
            "title": title,
            "country": country,
            "importance": importance,
            "markets": markets,
            "source": source,
            "url": url,
        }
    )


def dedupe_events(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for ev in sorted(events, key=lambda x: (x.get("date", ""), x.get("type", ""), x.get("title", ""))):
        key = (ev.get("date"), ev.get("title"), ev.get("country"), ev.get("type"))
        if key in seen:
            continue
        seen.add(key)
        out.append(ev)
    return out


def html_tables(url: str) -> List[pd.DataFrame]:
    html = session_get_text(url)
    try:
        return pd.read_html(StringIO(html))
    except Exception:
        return []


# -------------------------
# Official macro sources
# -------------------------
def fetch_bls_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = f"https://www.bls.gov/schedule/{start.year}/home.htm"
    events: List[Dict[str, Any]] = []

    # First try HTML tables.
    for df in html_tables(url):
        cols = {normalize_space(c).lower(): c for c in df.columns}
        if not {"date", "release"}.issubset(cols):
            continue
        date_col = cols["date"]
        release_col = cols["release"]
        for _, row in df.iterrows():
            d = parse_date_any(str(row.get(date_col, "")))
            release = normalize_space(row.get(release_col, ""))
            if not d or not in_range(d, start, end):
                continue
            title = release.lower()
            if "consumer price index" in title:
                add_event(events, d, "US CPI", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)
            elif "producer price index" in title:
                add_event(events, d, "US PPI", "US", "high", ["Rates", "FX", "Equities", "Commodities"], "BLS", url)
            elif "employment situation" in title:
                add_event(events, d, "US Non-Farm Payrolls", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)
                add_event(events, d, "US Unemployment Rate", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)

    if events:
        return dedupe_events(events)

    # Fallback: parse visible page text.
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    lines = [normalize_space(x) for x in soup.stripped_strings]
    i = 0
    while i < len(lines):
        d = parse_date_any(lines[i])
        if d and in_range(d, start, end):
            j = i + 1
            if j < len(lines) and re.match(r"^\d{1,2}:\d{2}\s*[AP]M$", lines[j], flags=re.I):
                j += 1
            if j < len(lines):
                title = lines[j].lower()
                if "consumer price index" in title:
                    add_event(events, d, "US CPI", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)
                elif "producer price index" in title:
                    add_event(events, d, "US PPI", "US", "high", ["Rates", "FX", "Equities", "Commodities"], "BLS", url)
                elif "employment situation" in title:
                    add_event(events, d, "US Non-Farm Payrolls", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)
                    add_event(events, d, "US Unemployment Rate", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)
        i += 1
    return dedupe_events(events)


def fetch_bea_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://www.bea.gov/news/schedule"
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    lines = [normalize_space(x) for x in soup.stripped_strings]
    events: List[Dict[str, Any]] = []

    targets = {
        "gdp (advance estimate)": ("US GDP", "high", ["Rates", "FX", "Equities"]),
        "gdp (second estimate)": ("US GDP", "high", ["Rates", "FX", "Equities"]),
        "gdp (third estimate)": ("US GDP", "high", ["Rates", "FX", "Equities"]),
        "personal income and outlays": ("US Personal Income & Outlays", "medium", ["Rates", "FX", "Equities"]),
        "u.s. international trade in goods and services": ("US Trade Balance", "medium", ["FX", "Rates", "Equities"]),
    }

    for i, line in enumerate(lines):
        lower = line.lower()
        matched = None
        for key, val in targets.items():
            if key in lower:
                matched = val
                break
        if not matched:
            continue

        d = None
        for j in range(i + 1, min(i + 5, len(lines))):
            d = parse_date_any(lines[j], default_year=start.year)
            if d:
                break
        if d and in_range(d, start, end):
            title, importance, markets = matched
            add_event(events, d, title, "US", importance, markets, "BEA", url)

    return dedupe_events(events)


def fetch_census_retail_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://www.census.gov/retail/release_schedule.html"
    text = BeautifulSoup(session_get_text(url), "html.parser").get_text(" ")
    events: List[Dict[str, Any]] = []

    patterns = [
        r"Advance Monthly Sales for Retail and Food Services.*?release on ([A-Z][a-z]+ \d{1,2}, \d{4})",
        r"Advance Monthly Sales for Retail and Food Services.*?scheduled for ([A-Z][a-z]+ \d{1,2}, \d{4})",
    ]
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.I | re.S):
            d = parse_date_any(m.group(1))
            if d and in_range(d, start, end):
                add_event(events, d, "US Retail Sales", "US", "high", ["Equities", "Rates", "FX"], "U.S. Census", url)
    return dedupe_events(events)


def fetch_ecb_policy_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html"
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    lines = [normalize_space(x) for x in soup.stripped_strings]
    events: List[Dict[str, Any]] = []

    for i, line in enumerate(lines):
        d = parse_date_any(line, default_year=start.year)
        if not d or not in_range(d, start, end):
            continue
        if i + 1 >= len(lines):
            continue
        title = lines[i + 1].lower()
        if "monetary policy meeting" in title and ("day 2" in title or "press conference" in title):
            add_event(events, d, "ECB Rate Decision", "Euro Area", "high", ["Rates", "FX", "Equities"], "ECB", url)
    return dedupe_events(events)


def fetch_ecb_stats_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html"
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    lines = [normalize_space(x) for x in soup.stripped_strings]
    events: List[Dict[str, Any]] = []

    for i, line in enumerate(lines):
        d = parse_date_any(line)
        if not d or not in_range(d, start, end):
            continue
        if i + 1 >= len(lines):
            continue
        title = lines[i + 1].lower()
        if "hicp flash estimate" in title:
            add_event(events, d, "Euro Area CPI Flash", "Euro Area", "high", ["Rates", "FX", "Equities"], "ECB Statistical Calendar", url)
        elif "seasonally adjusted hicp" in title:
            add_event(events, d, "Euro Area CPI Final", "Euro Area", "medium", ["Rates", "FX", "Equities"], "ECB Statistical Calendar", url)
    return dedupe_events(events)


def fetch_nbs_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/"
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    article_url = ""
    for a in soup.find_all("a", href=True):
        txt = normalize_space(a.get_text(" "))
        href = a["href"]
        if "Regular Press Release Calendar" in txt and str(start.year) in txt:
            article_url = urljoin(url, href)
            break
    if article_url and article_url.startswith("/"):
        article_url = "https://www.stats.gov.cn" + article_url
    if not article_url:
        article_url = f"https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/{start.year - 1}12/t{start.year - 1}1226_1962154.html"

    tables = html_tables(article_url)
    events: List[Dict[str, Any]] = []
    month_col = month_label(start.month)

    keyword_map = {
        "national economic performance": ("China National Economic Performance", "high", ["Commodities", "Equities", "FX"]),
        "purchasing managers": ("China PMI", "high", ["Commodities", "Equities", "FX"]),
        "consumer price index": ("China CPI", "high", ["Commodities", "Rates", "FX", "Equities"]),
        "producer price index": ("China PPI", "high", ["Commodities", "FX", "Equities"]),
    }

    for df in tables:
        cols = {normalize_space(c): c for c in df.columns}
        content_col = cols.get("Content")
        m_col = cols.get(month_col)
        if content_col is None or m_col is None:
            continue
        for _, row in df.iterrows():
            content = normalize_space(row.get(content_col, "")).lower()
            raw_day = normalize_space(row.get(m_col, ""))
            if not content or not raw_day or raw_day in {"……", "..."}:
                continue
            day_match = re.search(r"(\d{1,2})", raw_day)
            if not day_match:
                continue
            d = date(start.year, start.month, int(day_match.group(1)))
            if not in_range(d, start, end):
                continue
            for key, (title, importance, markets) in keyword_map.items():
                if key in content:
                    add_event(events, d, title, "China", importance, markets, "NBS China", article_url)
                    break

    return dedupe_events(events)


def macro_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    fetchers = [
        fetch_bls_events,
        fetch_bea_events,
        fetch_census_retail_events,
        fetch_ecb_policy_events,
        fetch_ecb_stats_events,
        fetch_nbs_events,
    ]
    for fn in fetchers:
        try:
            events.extend(fn(start, end))
        except Exception as e:
            print(f"⚠️ {fn.__name__}: {e}")
    return dedupe_events(events)


# -------------------------
# Earnings - intentionally kept structurally unchanged
# -------------------------
def earnings_events(start: date, end: date) -> List[Dict[str, Any]]:
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        return []
    params = {"from": str(start), "to": str(end), "apikey": api_key}
    r = requests.get(FMP_EARNINGS_URL, params=params, headers=HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()
    out: List[Dict[str, Any]] = []
    for e in data:
        ticker = normalize_space(e.get("symbol"))
        if ticker not in PRIORITY_EARNINGS:
            continue
        raw_d = str(e.get("date") or "")[:10]
        try:
            d = datetime.strptime(raw_d, "%Y-%m-%d").date()
        except Exception:
            continue
        country = "US"
        if ticker in CHINA_TICKERS:
            country = "China"
        elif ticker in EU_TICKERS:
            country = "EU"
        out.append(
            {
                "date": d.isoformat(),
                "type": "Earnings",
                "title": f"{ticker} earnings",
                "ticker": ticker,
                "company": e.get("companyName", ticker),
                "country": country,
                "importance": "high",
                "markets": ["Equities"],
                "source": "FMP",
            }
        )
    out.sort(key=lambda x: (x["date"], x["title"]))
    return out[:12]


def build_event_calendar(start: Optional[date] = None, end: Optional[date] = None) -> List[Dict[str, Any]]:
    if start is None or end is None:
        start, end = week_bounds()
    events = macro_events(start, end) + earnings_events(start, end)
    return dedupe_events(events)


if __name__ == "__main__":
    s, e = week_bounds()
    print(json.dumps(build_event_calendar(s, e), indent=2, ensure_ascii=False))
