#!/usr/bin/env python3
from __future__ import annotations

import calendar
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

def load_local_env(path: str = "FMP_API_KEY.env") -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and v and not os.getenv(k):
                    os.environ[k] = v
    except Exception:
        pass

load_local_env()


FMP_EARNINGS_URL = "https://financialmodelingprep.com/stable/earnings-calendar"

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


def week_bounds(today: Optional[date] = None) -> Tuple[date, date]:
    today = today or date.today()
    return today - timedelta(days=1), today + timedelta(days=7)


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
        "%A, %B %d, %Y", "%A, %B %d %Y", "%B %d, %Y", "%b %d, %Y",
        "%d/%m/%Y", "%d %B %Y", "%d %b %Y", "%Y-%m-%d",
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


def add_event(out: List[Dict[str, Any]], d: date, title: str, country: str,
              importance: str, markets: List[str], source: str, url: str = "") -> None:
    out.append({
        "date": d.isoformat(), "type": "Macro", "title": title, "country": country,
        "importance": importance, "markets": markets, "source": source, "url": url,
    })


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


def fetch_bls_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = f"https://www.bls.gov/schedule/{start.year}/home.htm"
    events: List[Dict[str, Any]] = []
    for df in html_tables(url):
        cols = {normalize_space(c).lower(): c for c in df.columns}
        if not {"date", "release"}.issubset(cols):
            continue
        for _, row in df.iterrows():
            d = parse_date_any(str(row.get(cols["date"], "")))
            release = normalize_space(row.get(cols["release"], "")).lower()
            if not d or not in_range(d, start, end):
                continue
            if "consumer price index" in release:
                add_event(events, d, "US CPI", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)
            elif "producer price index" in release:
                add_event(events, d, "US PPI", "US", "high", ["Rates", "FX", "Equities", "Commodities"], "BLS", url)
            elif "employment situation" in release:
                add_event(events, d, "US Non-Farm Payrolls", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)
                add_event(events, d, "US Unemployment Rate", "US", "high", ["Rates", "FX", "Equities"], "BLS", url)
    return dedupe_events(events)


def fetch_bea_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://www.bea.gov/news/schedule/full"
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    lines = [normalize_space(x) for x in soup.stripped_strings if normalize_space(x)]

    events: List[Dict[str, Any]] = []
    current_date: Optional[date] = None

    targets = {
        "u.s. international trade in goods and services": ("US Trade Balance", "medium", ["FX", "Rates", "Equities"]),
        "gdp (advance estimate)": ("US GDP", "high", ["Rates", "FX", "Equities"]),
        "gdp (second estimate)": ("US GDP", "high", ["Rates", "FX", "Equities"]),
        "gdp (third estimate)": ("US GDP", "high", ["Rates", "FX", "Equities"]),
        "personal income and outlays": ("US Personal Income & Outlays", "medium", ["Rates", "FX", "Equities"]),
    }

    for line in lines:
        d = parse_date_any(line, default_year=start.year)
        if d:
            current_date = d
            continue

        if not current_date or not in_range(current_date, start, end):
            continue

        low = line.lower()
        for key, (title, importance, markets) in targets.items():
            if key in low:
                add_event(events, current_date, title, "US", importance, markets, "BEA", url)
                break

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
        if not d or not in_range(d, start, end) or i + 1 >= len(lines):
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
        if not d or not in_range(d, start, end) or i + 1 >= len(lines):
            continue
        title = lines[i + 1].lower()
        if "hicp flash estimate" in title:
            add_event(events, d, "Euro Area CPI Flash", "Euro Area", "high", ["Rates", "FX", "Equities"], "ECB Statistical Calendar", url)
        elif "seasonally adjusted hicp" in title:
            add_event(events, d, "Euro Area CPI Final", "Euro Area", "medium", ["Rates", "FX", "Equities"], "ECB Statistical Calendar", url)
    return dedupe_events(events)


def fetch_eurostat_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://ec.europa.eu/eurostat/news/euro-indicators/release-calendar"
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    out: List[Dict[str, Any]] = []
    text = soup.get_text("\n", strip=True)
    lines = [normalize_space(x) for x in text.splitlines() if normalize_space(x)]
    current_date: Optional[date] = None
    for line in lines:
        d = parse_date_any(line)
        if d:
            current_date = d
            continue
        if not current_date or not in_range(current_date, start, end):
            continue
        low = line.lower()
        if "gdp and employment" in low:
            add_event(out, current_date, "Euro Area GDP", "Euro Area", "high", ["Rates", "FX", "Equities"], "Eurostat", url)
        elif "unemployment" in low:
            add_event(out, current_date, "Euro Area Unemployment", "Euro Area", "high", ["Rates", "FX", "Equities"], "Eurostat", url)
        elif "flash estimate" in low and "hicp" in low:
            add_event(out, current_date, "Euro Area CPI Flash", "Euro Area", "high", ["Rates", "FX", "Equities"], "Eurostat", url)
    return dedupe_events(out)


def _month_from_line(line: str) -> Optional[int]:
    low = normalize_space(line).lower().strip(".: ")
    for m in range(1, 13):
        if low in {calendar.month_name[m].lower(), calendar.month_abbr[m].lower().strip(".")}:
            return m
        if low == calendar.month_abbr[m].lower() + ".":
            return m
    return None


def fetch_nbs_events(start: date, end: date) -> List[Dict[str, Any]]:
    base_url = "https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/"
    soup = BeautifulSoup(session_get_text(base_url), "html.parser")

    article_url = ""
    for a in soup.find_all("a", href=True):
        txt = normalize_space(a.get_text(" "))
        href = a["href"]
        if "Regular Press Release Calendar" in txt and str(start.year) in txt:
            article_url = urljoin(base_url, href)
            break

    if not article_url:
        return []

    article = BeautifulSoup(session_get_text(article_url), "html.parser")
    lines = [normalize_space(x) for x in article.stripped_strings if normalize_space(x)]

    month_headers = ["Jan.", "Feb.", "Mar.", "Apr.", "May", "Jun.", "Jul.", "Aug.", "Sep.", "Oct.", "Nov.", "Dec."]
    try:
        header_idx = lines.index("No.")
    except ValueError:
        return []

    # cerca l'inizio del corpo tabella dopo "Dec."
    body_start = None
    for i in range(header_idx, min(header_idx + 40, len(lines))):
        if lines[i] == "Dec.":
            body_start = i + 1
            break
    if body_start is None:
        return []

    events: List[Dict[str, Any]] = []

    keyword_map = {
        "monthly report on consumer price index": ("China CPI", "high", ["Commodities", "Rates", "FX", "Equities"]),
        "monthly report on industrial producer price index": ("China PPI", "high", ["Commodities", "FX", "Equities"]),
        "monthly report on purchasing managers’ index": ("China PMI", "high", ["Commodities", "Equities", "FX"]),
        "monthly report on purchasing managers' index": ("China PMI", "high", ["Commodities", "Equities", "FX"]),
        "national economic performance": ("China National Economic Performance", "high", ["Commodities", "Equities", "FX"]),
        "monthly report on total retail sales of consumer goods": ("China Retail Sales", "high", ["Commodities", "Equities", "FX"]),
        "monthly report on industrial production operation above the designated size": ("China Industrial Production", "high", ["Commodities", "Equities", "FX"]),
        "monthly report on investment in fixed assets": ("China Fixed Asset Investment", "high", ["Commodities", "Equities", "FX"]),
    }

    i = body_start
    while i < len(lines):
        if not re.fullmatch(r"\d+", lines[i]):
            i += 1
            continue

        # struttura attesa: numero, contenuto, 12 giorni, 12 orari
        if i + 13 >= len(lines):
            break

        content = lines[i + 1].lower()
        day_cells = lines[i + 2:i + 14]

        for key, (title, importance, markets) in keyword_map.items():
            if key not in content:
                continue

            for month_num in range(1, 13):
                if month_num < start.month - 1 or month_num > end.month + 1:
                    continue

                raw_day = day_cells[month_num - 1]
                m = re.search(r"(\d{1,2})", raw_day)
                if not m:
                    continue

                try:
                    d = date(start.year, month_num, int(m.group(1)))
                except Exception:
                    continue

                if in_range(d, start, end):
                    add_event(events, d, title, "China", importance, markets, "NBS China", article_url)

            break

        i += 26  # 1 numero + 1 contenuto + 12 giorni + 12 orari

    return dedupe_events(events)

def macro_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    fetchers = [
        fetch_bls_events,
        fetch_bea_events,
        fetch_census_retail_events,
        fetch_ecb_policy_events,
        fetch_ecb_stats_events,
        fetch_eurostat_events,
        fetch_nbs_events,
    ]
    for fn in fetchers:
        try:
            events.extend(fn(start, end))
        except Exception as e:
            print(f"⚠️ {fn.__name__}: {e}")
    return dedupe_events(events)


def _earnings_country(ticker: str) -> str:
    if ticker in CHINA_TICKERS:
        return "China"
    if ticker in EU_TICKERS:
        return "EU"
    return "US"


def fetch_finviz_earnings(start: date, end: date) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    day = start

    while day <= end:
        url = f"https://finviz.com/calendar/earnings?dateFrom={day.isoformat()}&page=1"
        try:
            tables = html_tables(url)
        except Exception:
            day += timedelta(days=1)
            continue

        for df in tables:
            try:
                cols_norm = [normalize_space(c).lower() for c in df.columns]
            except Exception:
                continue

            if not any("ticker" in c for c in cols_norm):
                continue

            col_map = {normalize_space(c).lower(): c for c in df.columns}
            ticker_col = next((col_map[k] for k in col_map if "ticker" in k), None)
            company_col = next((col_map[k] for k in col_map if "company" in k), None)
            date_col = next((col_map[k] for k in col_map if k == "date" or "date" in k), None)

            if not ticker_col:
                continue

            for _, row in df.iterrows():
                ticker = normalize_space(row.get(ticker_col, "")).upper()
                if not ticker or ticker not in PRIORITY_EARNINGS:
                    continue

                event_date = day
                if date_col:
                    parsed = parse_date_any(str(row.get(date_col, "")), default_year=day.year)
                    if parsed:
                        event_date = parsed

                if not in_range(event_date, start, end):
                    continue

                company = ticker
                if company_col:
                    company = normalize_space(row.get(company_col, "")) or ticker

                out.append({
                    "date": event_date.isoformat(),
                    "type": "Earnings",
                    "title": ticker,
                    "ticker": ticker,
                    "company": company,
                    "country": _earnings_country(ticker),
                    "importance": "high",
                    "markets": ["Equities"],
                    "source": "Finviz",
                    "url": url,
                })

        day += timedelta(days=1)

    return dedupe_events(out)


def fetch_fmp_earnings(start: date, end: date) -> List[Dict[str, Any]]:
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        return []

    params = {"from": str(start), "to": str(end), "apikey": api_key}
    r = requests.get(FMP_EARNINGS_URL, params=params, headers=HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()

    out: List[Dict[str, Any]] = []
    for e in data:
        ticker = normalize_space(e.get("symbol")).upper()
        if ticker not in PRIORITY_EARNINGS:
            continue

        raw_d = str(e.get("date") or "")[:10]
        try:
            d = datetime.strptime(raw_d, "%Y-%m-%d").date()
        except Exception:
            continue

        if not in_range(d, start, end):
            continue

        out.append({
            "date": d.isoformat(),
            "type": "Earnings",
            "title": ticker,
            "ticker": ticker,
            "company": e.get("companyName", ticker),
            "country": _earnings_country(ticker),
            "importance": "high",
            "markets": ["Equities"],
            "source": "FMP",
        })
    return dedupe_events(out)


def earnings_events(start: date, end: date) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    try:
        out.extend(fetch_finviz_earnings(start, end))
    except Exception as e:
        print(f"⚠️ fetch_finviz_earnings: {e}")

    try:
        out.extend(fetch_fmp_earnings(start, end))
    except Exception as e:
        print(f"⚠️ fetch_fmp_earnings: {e}")

    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for ev in sorted(out, key=lambda x: (x.get("date", ""), x.get("title", ""), x.get("source", ""))):
        key = (ev.get("date", ""), ev.get("ticker", ""))
        if key not in deduped:
            deduped[key] = ev

    return list(deduped.values())


def build_event_calendar(start: Optional[date] = None, end: Optional[date] = None) -> List[Dict[str, Any]]:
    if start is None or end is None:
        start, end = week_bounds()
    return dedupe_events(macro_events(start, end) + earnings_events(start, end))


if __name__ == "__main__":
    s, e = week_bounds()
    print(json.dumps(build_event_calendar(s, e), indent=2, ensure_ascii=False))
