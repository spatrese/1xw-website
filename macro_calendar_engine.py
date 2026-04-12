#!/usr/bin/env python3
from __future__ import annotations

import calendar
import json
import os
import re
from datetime import date, datetime, timedelta
from io import StringIO
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urljoin

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
FINVIZ_SCREENER_URL = "https://finviz.com/screener.ashx"
FINVIZ_QUOTE_URL = "https://finviz.com/quote.ashx"

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
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finviz.com/",
}
TIMEOUT = 20


# ---------------------------------------------------------
# Canonical macro registry
# ---------------------------------------------------------
EVENT_REGISTRY: Dict[str, Dict[str, Any]] = {
    # US — BLS / BEA / Census / Fed / ISM / Conference Board
    "US_NFP": {
        "title": "US Non-Farm Payrolls",
        "country": "US",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "BLS",
    },
    "US_UNEMPLOYMENT": {
        "title": "US Unemployment Rate",
        "country": "US",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "BLS",
    },
    "US_CPI": {
        "title": "US CPI",
        "country": "US",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "BLS",
    },
    "US_CORE_CPI": {
        "title": "US Core CPI",
        "country": "US",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "BLS",
    },
    "US_PPI": {
        "title": "US PPI",
        "country": "US",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities", "Commodities"],
        "source_family": "BLS",
    },
    "US_JOLTS": {
        "title": "US JOLTS Job Openings",
        "country": "US",
        "importance": "medium",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "BLS",
    },
    "US_IMPORT_EXPORT_PRICES": {
        "title": "US Import/Export Prices",
        "country": "US",
        "importance": "medium",
        "markets": ["Rates", "FX", "Commodities", "Equities"],
        "source_family": "BLS",
    },
    "US_GDP": {
        "title": "US GDP",
        "country": "US",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "BEA",
    },
    "US_PERSONAL_INCOME_OUTLAYS": {
        "title": "US Personal Income & Outlays",
        "country": "US",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "BEA",
    },
    "US_TRADE_BALANCE": {
        "title": "US Trade Balance",
        "country": "US",
        "importance": "medium",
        "markets": ["FX", "Rates", "Equities"],
        "source_family": "BEA",
    },
    "US_RETAIL_SALES": {
        "title": "US Retail Sales",
        "country": "US",
        "importance": "high",
        "markets": ["Equities", "Rates", "FX"],
        "source_family": "U.S. Census",
    },
    "US_DURABLE_GOODS_ORDERS": {
        "title": "US Durable Goods Orders",
        "country": "US",
        "importance": "medium",
        "markets": ["Equities", "Rates", "FX", "Commodities"],
        "source_family": "U.S. Census",
    },
    "US_HOUSING_STARTS_BUILDING_PERMITS": {
        "title": "US Housing Starts / Building Permits",
        "country": "US",
        "importance": "medium",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "U.S. Census",
    },
    "US_ISM_MANUFACTURING_PMI": {
        "title": "ISM Manufacturing PMI",
        "country": "US",
        "importance": "medium",
        "markets": ["Equities", "Rates", "FX", "Commodities"],
        "source_family": "ISM",
    },
    "US_ISM_SERVICES_PMI": {
        "title": "ISM Services PMI",
        "country": "US",
        "importance": "medium",
        "markets": ["Equities", "Rates", "FX"],
        "source_family": "ISM",
    },
    "US_CONSUMER_CONFIDENCE": {
        "title": "US Consumer Confidence",
        "country": "US",
        "importance": "medium",
        "markets": ["Equities", "Rates", "FX"],
        "source_family": "Conference Board",
    },
    "FOMC_RATE_DECISION": {
        "title": "FOMC Rate Decision",
        "country": "US",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities", "Commodities"],
        "source_family": "Federal Reserve",
    },

    # Euro Area — Eurostat / ECB
    "EA_CPI_FLASH": {
        "title": "Euro Area CPI Flash",
        "country": "Euro Area",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "Eurostat",
    },
    "EA_CPI_FINAL": {
        "title": "Euro Area CPI Final",
        "country": "Euro Area",
        "importance": "medium",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "Eurostat",
    },
    "EA_GDP": {
        "title": "Euro Area GDP",
        "country": "Euro Area",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "Eurostat",
    },
    "EA_UNEMPLOYMENT": {
        "title": "Euro Area Unemployment",
        "country": "Euro Area",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "Eurostat",
    },
    "EA_INDUSTRIAL_PRODUCTION": {
        "title": "Euro Area Industrial Production",
        "country": "Euro Area",
        "importance": "medium",
        "markets": ["Rates", "FX", "Equities", "Commodities"],
        "source_family": "Eurostat",
    },
    "EA_RETAIL_SALES": {
        "title": "Euro Area Retail Sales",
        "country": "Euro Area",
        "importance": "medium",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "Eurostat",
    },
    "EA_TRADE_BALANCE": {
        "title": "Euro Area Trade Balance",
        "country": "Euro Area",
        "importance": "medium",
        "markets": ["FX", "Rates", "Equities"],
        "source_family": "Eurostat",
    },
    "EA_ECONOMIC_SENTIMENT": {
        "title": "Euro Area Economic Sentiment Indicators",
        "country": "Euro Area",
        "importance": "medium",
        "markets": ["Equities", "Rates", "FX"],
        "source_family": "European Commission",
    },
    "EA_PMI": {
        "title": "Euro Area PMI (Manufacturing / Services)",
        "country": "Euro Area",
        "importance": "medium",
        "markets": ["Equities", "Rates", "FX", "Commodities"],
        "source_family": "S&P Global",
    },
    "ECB_RATE_DECISION": {
        "title": "ECB Rate Decision",
        "country": "Euro Area",
        "importance": "high",
        "markets": ["Rates", "FX", "Equities"],
        "source_family": "ECB",
    },

    # China — NBS / Customs / PBOC (where needed)
    "CN_CPI": {
        "title": "China CPI",
        "country": "China",
        "importance": "high",
        "markets": ["Commodities", "Rates", "FX", "Equities"],
        "source_family": "NBS China",
    },
    "CN_PPI": {
        "title": "China PPI",
        "country": "China",
        "importance": "high",
        "markets": ["Commodities", "FX", "Equities"],
        "source_family": "NBS China",
    },
    "CN_PMI": {
        "title": "China PMI",
        "country": "China",
        "importance": "high",
        "markets": ["Commodities", "Equities", "FX"],
        "source_family": "NBS China",
    },
    "CN_PMI_SPLIT": {
        "title": "China Manufacturing / Non-Manufacturing PMI",
        "country": "China",
        "importance": "medium",
        "markets": ["Commodities", "Equities", "FX"],
        "source_family": "NBS China",
    },
    "CN_NATIONAL_ECONOMIC_PERFORMANCE": {
        "title": "China National Economic Performance",
        "country": "China",
        "importance": "high",
        "markets": ["Commodities", "Equities", "FX"],
        "source_family": "NBS China",
    },
    "CN_RETAIL_SALES": {
        "title": "China Retail Sales",
        "country": "China",
        "importance": "high",
        "markets": ["Commodities", "Equities", "FX"],
        "source_family": "NBS China",
    },
    "CN_INDUSTRIAL_PRODUCTION": {
        "title": "China Industrial Production",
        "country": "China",
        "importance": "high",
        "markets": ["Commodities", "Equities", "FX"],
        "source_family": "NBS China",
    },
    "CN_FIXED_ASSET_INVESTMENT": {
        "title": "China Fixed Asset Investment",
        "country": "China",
        "importance": "high",
        "markets": ["Commodities", "Equities", "FX"],
        "source_family": "NBS China",
    },
    "CN_HOUSE_PRICES": {
        "title": "China House Prices",
        "country": "China",
        "importance": "medium",
        "markets": ["Commodities", "Equities", "FX"],
        "source_family": "NBS China",
    },
    "CN_TRADE_BALANCE": {
        "title": "China Trade Balance",
        "country": "China",
        "importance": "medium",
        "markets": ["FX", "Commodities", "Equities"],
        "source_family": "General Administration of Customs of China",
    },
    "CN_TSF": {
        "title": "China Credit / Total Social Financing (TSF)",
        "country": "China",
        "importance": "medium",
        "markets": ["Rates", "FX", "Equities", "Commodities"],
        "source_family": "PBOC",
    },
    "CN_NEW_LOANS": {
        "title": "China New Loans",
        "country": "China",
        "importance": "medium",
        "markets": ["Rates", "FX", "Equities", "Commodities"],
        "source_family": "PBOC",
    },
}


# ---------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------
def week_bounds(today: Optional[date] = None) -> Tuple[date, date]:
    today = today or date.today()
    return today - timedelta(days=1), today + timedelta(days=7)


def in_range(d: date, start: date, end: date) -> bool:
    return start <= d <= end


def session_get_text(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    session: Optional[requests.Session] = None,
) -> str:
    sess = session or requests.Session()
    r = sess.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
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
        "%b. %d, %Y", "%B %d %Y", "%b %d %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    if default_year is not None:
        for fmt in ["%B %d", "%b %d", "%b. %d"]:
            try:
                return datetime.strptime(f"{s} {default_year}", f"{fmt} %Y").date()
            except Exception:
                pass
    return None


def add_registry_event(
    out: List[Dict[str, Any]],
    event_key: str,
    d: date,
    source: str,
    url: str = "",
    *,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    meta = EVENT_REGISTRY[event_key]
    ev = {
        "date": d.isoformat(),
        "type": "Macro",
        "title": meta["title"],
        "country": meta["country"],
        "importance": meta["importance"],
        "markets": list(meta["markets"]),
        "source": source,
        "url": url,
        "event_key": event_key,
        "source_family": meta.get("source_family", source),
    }
    if extra:
        ev.update(extra)
    out.append(ev)


def dedupe_events(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    def norm(x: Any) -> str:
        return normalize_space(x).lower()

    for ev in sorted(events, key=lambda x: (x.get("date", ""), x.get("type", ""), x.get("title", ""))):
        key = (
            norm(ev.get("date")),
            norm(ev.get("type")),
            norm(ev.get("ticker") or ev.get("title")),
            norm(ev.get("country")),
        )
        if key not in merged:
            new_ev = dict(ev)
            src = normalize_space(ev.get("source"))
            url = normalize_space(ev.get("url"))
            new_ev["sources"] = [src] if src else []
            new_ev["urls"] = [url] if url else []
            merged[key] = new_ev
            continue

        cur = merged[key]
        src = normalize_space(ev.get("source"))
        url = normalize_space(ev.get("url"))
        if src and src not in cur.get("sources", []):
            cur.setdefault("sources", []).append(src)
        if url and url not in cur.get("urls", []):
            cur.setdefault("urls", []).append(url)
        if not cur.get("source") and src:
            cur["source"] = src
        if not cur.get("url") and url:
            cur["url"] = url
    return list(merged.values())


def html_tables(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    session: Optional[requests.Session] = None,
) -> List[pd.DataFrame]:
    html = session_get_text(url, params=params, session=session)
    try:
        return pd.read_html(StringIO(html))
    except Exception:
        return []


# ---------------------------------------------------------
# US macro block
# ---------------------------------------------------------

def fetch_us_calendar_proxy_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    session = requests.Session()

    label_map = {
        "employment situation": [("US_NFP", "BLS"), ("US_UNEMPLOYMENT", "BLS")],
        "consumer price index": [("US_CPI", "BLS"), ("US_CORE_CPI", "BLS")],
        "producer price index": [("US_PPI", "BLS")],
        "jolts": [("US_JOLTS", "BLS")],
        "imports and exports": [("US_IMPORT_EXPORT_PRICES", "BLS")],
        "advance retail sales": [("US_RETAIL_SALES", "U.S. Census")],
        "new residential construction": [("US_HOUSING_STARTS_BUILDING_PERMITS", "U.S. Census")],
        "advance durable goods": [("US_DURABLE_GOODS_ORDERS", "U.S. Census")],
        "consumer confidence": [("US_CONSUMER_CONFIDENCE", "Conference Board")],
        "ism manufacturing": [("US_ISM_MANUFACTURING_PMI", "ISM")],
        "ism non-manufacturing": [("US_ISM_SERVICES_PMI", "ISM")],
        "trade balance": [("US_TRADE_BALANCE", "BEA")],
        "gross domestic product 1st release": [("US_GDP", "BEA")],
        "gross domestic product 2nd release": [("US_GDP", "BEA")],
        "gross domestic product 3rd release": [("US_GDP", "BEA")],
        "gross domestic product": [("US_GDP", "BEA")],
        "personal income and the pce deflator": [("US_PERSONAL_INCOME_OUTLAYS", "BEA")],
        "personal income": [("US_PERSONAL_INCOME_OUTLAYS", "BEA")],
    }

    months_to_pull = sorted({
        (start.year, start.month),
        (end.year, end.month),
    })

    for year_num, month_num in months_to_pull:
        try:
            # es. https://www.newyorkfed.org/research/calendars/i-apr26.html
            month_abbr = calendar.month_abbr[month_num].lower()
            yy = str(year_num)[2:]
            url = f"https://www.newyorkfed.org/research/calendars/i-{month_abbr}{yy}.html"

            html = session_get_text(url, session=session)
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text("\n", strip=True)
            lines = [normalize_space(x) for x in text.splitlines() if normalize_space(x)]

            current_day: Optional[int] = None

            for line in lines:
                # giorno del mese isolato
                if re.fullmatch(r"\d{1,2}", line):
                    current_day = int(line)
                    continue

                if current_day is None:
                    continue

                low = line.lower()

                for label, mappings in label_map.items():
                    if label in low:
                        try:
                            d = date(year_num, month_num, current_day)
                        except Exception:
                            continue

                        if not in_range(d, start, end):
                            continue

                        for event_key, source_name in mappings:
                            add_registry_event(
                                events,
                                event_key,
                                d,
                                source_name,
                                url,
                                extra={"scheduler_source": "New York Fed Economic Indicators Calendar"},
                            )
                        break

        except Exception as e:
            print(f"⚠️ fetch_us_calendar_proxy_events [{year_num}-{month_num:02d}]: {e}")

    return dedupe_events(events)


def fetch_us_bls_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    session = requests.Session()

    def _extract_date(text: str) -> Optional[date]:
        m = re.search(
            r"([A-Z][a-z]+ \d{1,2}, \d{4})",
            text
        )
        return parse_date_any(m.group(1)) if m else None

    # --- CPI ---
    try:
        url = "https://www.bls.gov/cpi/"
        text = BeautifulSoup(session_get_text(url, session=session), "html.parser").get_text(" ", strip=True)
        d = _extract_date(text)
        if d and in_range(d, start, end):
            add_registry_event(events, "US_CPI", d, "BLS", url)
            add_registry_event(events, "US_CORE_CPI", d, "BLS", url)
    except Exception as e:
        print(f"⚠️ CPI fallback: {e}")

    # --- PPI ---
    try:
        url = "https://www.bls.gov/ppi/"
        text = BeautifulSoup(session_get_text(url, session=session), "html.parser").get_text(" ", strip=True)
        d = _extract_date(text)
        if d and in_range(d, start, end):
            add_registry_event(events, "US_PPI", d, "BLS", url)
    except Exception as e:
        print(f"⚠️ PPI fallback: {e}")

    # --- NFP ---
    try:
        url = "https://www.bls.gov/news.release/empsit.nr0.htm"
        text = BeautifulSoup(session_get_text(url, session=session), "html.parser").get_text(" ", strip=True)

        m = re.search(
            r"scheduled to be released on\s+(?:[A-Z][a-z]+,\s+)?([A-Z][a-z]+ \d{1,2}, \d{4})",
            text,
            flags=re.I
        )

        d = parse_date_any(m.group(1)) if m else None

        if d and in_range(d, start, end):
            add_registry_event(events, "US_NFP", d, "BLS", url)
            add_registry_event(events, "US_UNEMPLOYMENT", d, "BLS", url)
    except Exception as e:
        print(f"⚠️ NFP fallback: {e}")

    # --- JOLTS ---
    try:
        url = "https://www.bls.gov/jlt/"
        text = BeautifulSoup(session_get_text(url, session=session), "html.parser").get_text(" ", strip=True)
        d = _extract_date(text)
        if d and in_range(d, start, end):
            add_registry_event(events, "US_JOLTS", d, "BLS", url)
    except Exception as e:
        print(f"⚠️ JOLTS fallback: {e}")

    # --- Import/Export Prices ---
    try:
        url = "https://www.bls.gov/mxp/"
        text = BeautifulSoup(session_get_text(url, session=session), "html.parser").get_text(" ", strip=True)
        d = _extract_date(text)
        if d and in_range(d, start, end):
            add_registry_event(events, "US_IMPORT_EXPORT_PRICES", d, "BLS", url)
    except Exception as e:
        print(f"⚠️ MXP fallback: {e}")

    return dedupe_events(events)

def fetch_us_bea_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://www.bea.gov/news/schedule/full"
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    lines = [normalize_space(x) for x in soup.stripped_strings if normalize_space(x)]

    events: List[Dict[str, Any]] = []
    current_date: Optional[date] = None

    targets = {
        "u.s. international trade in goods and services": "US_TRADE_BALANCE",
        "gdp (advance estimate)": "US_GDP",
        "gdp (second estimate)": "US_GDP",
        "gdp (third estimate)": "US_GDP",
        "personal income and outlays": "US_PERSONAL_INCOME_OUTLAYS",
    }

    for line in lines:
        d = parse_date_any(line, default_year=start.year)
        if d:
            current_date = d
            continue

        if not current_date or not in_range(current_date, start, end):
            continue

        low = line.lower()
        for key, event_key in targets.items():
            if key in low:
                add_registry_event(events, event_key, current_date, "BEA", url)
                break

    return dedupe_events(events)

def extract_census_release_date(text: str) -> Optional[date]:
    text = normalize_space(text)

    patterns = [
        r"have been rescheduled for release on ([A-Z][a-z]+ \d{1,2}, \d{4})",
        r"is scheduled for release on ([A-Z][a-z]+ \d{1,2}, \d{4})",
        r"next release[:\s]+([A-Z][a-z]+ \d{1,2}, \d{4})",
        r"next release.*?([A-Z][a-z]+ \d{1,2}, \d{4})",
    ]

    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if m:
            return parse_date_any(m.group(1))

    return None


def fetch_us_census_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    session = requests.Session()

    # Retail Sales
    try:
        url = "https://www.census.gov/retail/release_schedule.html"
        text = BeautifulSoup(session_get_text(url, session=session), "html.parser").get_text(" ", strip=True)
        d = extract_census_release_date(text)
        if d and in_range(d, start, end):
            add_registry_event(events, "US_RETAIL_SALES", d, "U.S. Census", url)
    except Exception as e:
        print(f"⚠️ fetch_us_census_events [retail]: {e}")

    # Durable Goods Orders
    try:
        url = "https://www.census.gov/manufacturing/m3/release_schedule.html"
        text = BeautifulSoup(session_get_text(url, session=session), "html.parser").get_text(" ", strip=True)
        d = extract_census_release_date(text)
        if d and in_range(d, start, end):
            add_registry_event(events, "US_DURABLE_GOODS_ORDERS", d, "U.S. Census", url)
    except Exception as e:
        print(f"⚠️ fetch_us_census_events [durable_goods]: {e}")

    # Housing Starts / Building Permits
    try:
        url = "https://www.census.gov/construction/nrc/current/index.html"
        text = BeautifulSoup(session_get_text(url, session=session), "html.parser").get_text(" ", strip=True)
        d = extract_census_release_date(text)
        if d and in_range(d, start, end):
            add_registry_event(events, "US_HOUSING_STARTS_BUILDING_PERMITS", d, "U.S. Census", url)
    except Exception as e:
        print(f"⚠️ fetch_us_census_events [housing]: {e}")

    return dedupe_events(events)

def fetch_us_fed_events(start: date, end: date) -> List[Dict[str, Any]]:
    url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    soup = BeautifulSoup(session_get_text(url), "html.parser")
    lines = [normalize_space(x) for x in soup.stripped_strings if normalize_space(x)]

    events: List[Dict[str, Any]] = []

    year_idx = None
    year_pat = re.compile(rf"^{start.year}\s+FOMC\s+Meetings$", re.I)
    for i, line in enumerate(lines):
        if year_pat.match(line):
            year_idx = i
            break

    if year_idx is None:
        return []

    next_year_idx = len(lines)
    for j in range(year_idx + 1, len(lines)):
        if re.match(r"^\d{4}\s+FOMC\s+Meetings$", lines[j], re.I):
            next_year_idx = j
            break

    section = lines[year_idx + 1:next_year_idx]
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }

    current_month = None
    seen = set()

    for line in section:
        low = line.lower().strip()

        if low in month_map:
            current_month = month_map[low]
            continue

        if current_month is None or "notation vote" in low:
            continue

        m = re.match(r"^(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\*?", low)
        if not m:
            continue

        day1 = int(m.group(1))
        day2 = m.group(2)
        event_day = int(day2) if day2 else day1

        try:
            d = date(start.year, current_month, event_day)
        except Exception:
            continue

        if not in_range(d, start, end):
            continue

        key = d.isoformat()
        if key in seen:
            continue
        seen.add(key)

        add_registry_event(events, "FOMC_RATE_DECISION", d, "Federal Reserve", url)

    return dedupe_events(events)


def fetch_us_ism_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    url = "https://www.ismworld.org/supply-management-news-and-reports/reports/rob-report-calendar/"

    try:
        html = session_get_text(url)
        soup = BeautifulSoup(html, "html.parser")

        tables = soup.find_all("table")
        if not tables:
            return []

        month_map = {calendar.month_name[i].lower(): i for i in range(1, 13)}

        for table in tables:
            text = normalize_space(table.get_text(" ", strip=True)).lower()
            if "manufacturing" not in text or "services" not in text:
                continue

            rows = table.find_all("tr")
            for tr in rows:
                cells = [normalize_space(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
                if len(cells) < 3:
                    continue

                # prova a trovare una riga tipo: April 2026 | 1 | 6
                m = re.match(r"^([A-Za-z]+)\s+(\d{4})$", cells[0])
                if not m:
                    continue

                month_name = m.group(1).lower()
                year_num = int(m.group(2))
                if month_name not in month_map:
                    continue

                month_num = month_map[month_name]

                mfg_day = re.search(r"\b(\d{1,2})\b", cells[1])
                srv_day = re.search(r"\b(\d{1,2})\b", cells[2])

                if mfg_day:
                    try:
                        d = date(year_num, month_num, int(mfg_day.group(1)))
                        if in_range(d, start, end):
                            add_registry_event(events, "US_ISM_MANUFACTURING_PMI", d, "ISM", url)
                    except Exception:
                        pass

                if srv_day:
                    try:
                        d = date(year_num, month_num, int(srv_day.group(1)))
                        if in_range(d, start, end):
                            add_registry_event(events, "US_ISM_SERVICES_PMI", d, "ISM", url)
                    except Exception:
                        pass

            break

    except Exception as e:
        print(f"⚠️ fetch_us_ism_events: {e}")

    return dedupe_events(events)

def fetch_us_conference_board_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    url = "https://www.conference-board.org/topics/consumer-confidence"
    try:
        text = BeautifulSoup(session_get_text(url), "html.parser").get_text(" ", strip=True)
        m = re.search(r"next release.*?([A-Z][a-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.I | re.S)
        d = parse_date_any(m.group(1)) if m else None
        if d and in_range(d, start, end):
            add_registry_event(events, "US_CONSUMER_CONFIDENCE", d, "Conference Board", url)
    except Exception:
        pass
    return dedupe_events(events)


def us_macro_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    fetchers = [
        fetch_us_calendar_proxy_events,
        fetch_us_census_events,
        fetch_us_bea_events, 
        fetch_us_fed_events,
    ]
    for fn in fetchers:
        try:
            events.extend(fn(start, end))
        except Exception as e:
            print(f"⚠️ {fn.__name__}: {e}")
    return dedupe_events(events)


# ---------------------------------------------------------
# Euro Area macro block
# ---------------------------------------------------------
def fetch_euro_area_ecb_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    try:
        url = "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html"
        soup = BeautifulSoup(session_get_text(url), "html.parser")
        lines = [normalize_space(x) for x in soup.stripped_strings]
        for i, line in enumerate(lines):
            d = parse_date_any(line, default_year=start.year)
            if not d or not in_range(d, start, end) or i + 1 >= len(lines):
                continue
            title = lines[i + 1].lower()
            if "monetary policy meeting" in title and ("day 2" in title or "press conference" in title):
                add_registry_event(events, "ECB_RATE_DECISION", d, "ECB", url)
    except Exception as e:
        print(f"⚠️ fetch_euro_area_ecb_events [policy]: {e}")

    try:
        url = "https://www.ecb.europa.eu/press/calendars/statscal/html/index.en.html"
        soup = BeautifulSoup(session_get_text(url), "html.parser")
        lines = [normalize_space(x) for x in soup.stripped_strings]
        for i, line in enumerate(lines):
            d = parse_date_any(line)
            if not d or not in_range(d, start, end) or i + 1 >= len(lines):
                continue
            title = lines[i + 1].lower()
            if "hicp flash estimate" in title:
                add_registry_event(events, "EA_CPI_FLASH", d, "ECB Statistical Calendar", url)
            elif "seasonally adjusted hicp" in title or "hicp" in title and "final" in title:
                add_registry_event(events, "EA_CPI_FINAL", d, "ECB Statistical Calendar", url)
    except Exception as e:
        print(f"⚠️ fetch_euro_area_ecb_events [stats]: {e}")

    return dedupe_events(events)


def fetch_euro_area_eurostat_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    candidates = [
        "https://ec.europa.eu/eurostat/news/euro-indicators/release-calendar",
        "https://ec.europa.eu/eurostat/web/products-euro-indicators",
    ]

    keyword_map = [
        (r"flash estimate.*hicp|euro area annual inflation|flash estimate inflation", "EA_CPI_FLASH"),
        (r"final.*hicp|seasonally adjusted hicp|harmonised index of consumer prices", "EA_CPI_FINAL"),
        (r"gdp and employment|gross domestic product|gdp", "EA_GDP"),
        (r"unemployment", "EA_UNEMPLOYMENT"),
        (r"industrial production", "EA_INDUSTRIAL_PRODUCTION"),
        (r"volume of retail trade|retail trade|retail sales", "EA_RETAIL_SALES"),
        (r"international trade in goods|trade balance|balance of trade", "EA_TRADE_BALANCE"),
    ]

    for url in candidates:
        try:
            soup = BeautifulSoup(session_get_text(url), "html.parser")
            text = soup.get_text("\n", strip=True)
            lines = [normalize_space(x) for x in text.splitlines() if normalize_space(x)]
            current_date: Optional[date] = None
            for line in lines:
                d = parse_date_any(line, default_year=start.year)
                if d:
                    current_date = d
                    continue
                if not current_date or not in_range(current_date, start, end):
                    continue
                low = line.lower()
                for pat, event_key in keyword_map:
                    if re.search(pat, low):
                        add_registry_event(events, event_key, current_date, "Eurostat", url)
                        break
        except Exception as e:
            print(f"⚠️ fetch_euro_area_eurostat_events [{url}]: {e}")

    return dedupe_events(events)


def fetch_euro_area_optional_events(start: date, end: date) -> List[Dict[str, Any]]:
    """
    Optional additions required by the universe doc but outside the strict
    Eurostat/ECB primary-source rule.
    """
    events: List[Dict[str, Any]] = []

    # Economic Sentiment
    try:
        url = "https://economy-finance.ec.europa.eu/economic-forecast-and-surveys/business-and-consumer-surveys_en"
        text = BeautifulSoup(session_get_text(url), "html.parser").get_text(" ", strip=True)
        m = re.search(r"next release.*?([A-Z][a-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.I | re.S)
        d = parse_date_any(m.group(1)) if m else None
        if d and in_range(d, start, end):
            add_registry_event(events, "EA_ECONOMIC_SENTIMENT", d, "European Commission", url)
    except Exception:
        pass

    # Euro Area PMI - forward-looking release calendar
    try:
        url = "https://www.pmi.spglobal.com/Public/Release/ReleaseDates"
        html = session_get_text(url)
        text = normalize_space(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))

        # Esempi target:
        # "April 01 2026 08:00 UTC S&P Global Eurozone Manufacturing PMI"
        # "01 Apr 08:00 UTC S&P Global Eurozone Manufacturing PMI"
        full_re = re.compile(
            r"(January|February|March|April|May|June|July|August|September|October|November|December)"
            r"\s+(\d{1,2})\s+(\d{4})\s+\d{2}:\d{2}(?:\s+UTC)?\s+.*?\bEurozone\b.*?\bPMI\b",
            flags=re.I,
        )

        compact_re = re.compile(
            r"(\d{1,2})\s+"
            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
            r"\s+\d{2}:\d{2}(?:\s+UTC)?\s+.*?\bEurozone\b.*?\bPMI\b",
            flags=re.I,
        )

        month_map_short = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }

        candidate_years = sorted({start.year, end.year})

        for m in full_re.finditer(text):
            month_name = m.group(1)
            day_num = int(m.group(2))
            year_num = int(m.group(3))
            try:
                d = datetime.strptime(f"{month_name} {day_num} {year_num}", "%B %d %Y").date()
            except Exception:
                continue
            if in_range(d, start, end):
                add_registry_event(
                    events,
                    "EA_PMI",
                    d,
                    "S&P Global",
                    url,
                    extra={"scheduler_source": "S&P Global PMI ReleaseDates"},
                )

        for m in compact_re.finditer(text):
            day_num = int(m.group(1))
            month_num = month_map_short[m.group(2).lower()]
            for year_num in candidate_years:
                try:
                    d = date(year_num, month_num, day_num)
                except Exception:
                    continue
                if in_range(d, start, end):
                    add_registry_event(
                        events,
                        "EA_PMI",
                        d,
                        "S&P Global",
                        url,
                        extra={"scheduler_source": "S&P Global PMI ReleaseDates"},
                    )
                    break

    except Exception:
        pass

    return dedupe_events(events)

def fetch_euro_area_hicp_release(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    index_url = "https://ec.europa.eu/eurostat/web/products-euro-indicators"

    try:
        html = session_get_text(index_url)
        soup = BeautifulSoup(html, "html.parser")

        article_url = None

        # trova articolo inflation
        for a in soup.find_all("a", href=True):
            title = normalize_space(a.get_text(" "))
            low = title.lower()

            if "inflation" in low and "euro" in low:
                article_url = urljoin(index_url, a["href"])
                break

        if not article_url:
            return []

        # apri articolo
        article_html = session_get_text(article_url)
        article_text = BeautifulSoup(article_html, "html.parser").get_text(" ", strip=True)

        # cerca data ufficiale release
        patterns = [
            r"next release with full data .*? scheduled for (\d{1,2} [A-Z][a-z]+ \d{4})",
            r"next release .*? scheduled for (\d{1,2} [A-Z][a-z]+ \d{4})",
            r"next release with full data .*? scheduled for ([A-Z][a-z]+ \d{1,2}, \d{4})",
            r"next release .*? scheduled for ([A-Z][a-z]+ \d{1,2}, \d{4})",
        ]

        release_date = None
        for pat in patterns:
            m = re.search(pat, article_text, flags=re.I | re.S)
            if m:
                release_date = parse_date_any(m.group(1))
                if release_date:
                    break

        if release_date and in_range(release_date, start, end):
            add_registry_event(events, "EA_CPI_FINAL", release_date, "Eurostat", article_url)

    except Exception as e:
        print(f"⚠️ fetch_euro_area_hicp_release: {e}")

    return dedupe_events(events)


def euro_area_macro_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    fetchers = [
        fetch_euro_area_ecb_events,
        fetch_euro_area_eurostat_events,
        fetch_euro_area_optional_events,
        fetch_euro_area_hicp_release,
    ]
    for fn in fetchers:
        try:
            events.extend(fn(start, end))
        except Exception as e:
            print(f"⚠️ {fn.__name__}: {e}")
    return dedupe_events(events)


# ---------------------------------------------------------
# China macro block
# ---------------------------------------------------------
def fetch_china_nbs_events(start: date, end: date) -> List[Dict[str, Any]]:
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

    try:
        header_idx = lines.index("No.")
    except ValueError:
        return []

    body_start = None
    for i in range(header_idx, min(header_idx + 40, len(lines))):
        if lines[i] == "Dec.":
            body_start = i + 1
            break
    if body_start is None:
        return []

    events: List[Dict[str, Any]] = []

    keyword_map = {
        "monthly report on consumer price index": ["CN_CPI"],
        "monthly report on industrial producer price index": ["CN_PPI"],
        "monthly report on purchasing managers’ index": ["CN_PMI", "CN_PMI_SPLIT"],
        "monthly report on purchasing managers' index": ["CN_PMI", "CN_PMI_SPLIT"],
        "national economic performance": [
            "CN_NATIONAL_ECONOMIC_PERFORMANCE",
            "CN_RETAIL_SALES",
            "CN_INDUSTRIAL_PRODUCTION",
            "CN_FIXED_ASSET_INVESTMENT",
        ],
        "monthly report on total retail sales of consumer goods": ["CN_RETAIL_SALES"],
        "monthly report on industrial production operation above the designated size": ["CN_INDUSTRIAL_PRODUCTION"],
        "monthly report on investment in fixed assets": ["CN_FIXED_ASSET_INVESTMENT"],
        "sales prices of residential buildings": ["CN_HOUSE_PRICES"],
        "70 medium and large-sized cities": ["CN_HOUSE_PRICES"],
    }

    i = body_start
    while i < len(lines):
        if not re.fullmatch(r"\d+", lines[i]):
            i += 1
            continue

        if i + 13 >= len(lines):
            break

        content = lines[i + 1].lower()
        day_cells = lines[i + 2:i + 14]

        matched_keys: List[str] = []
        for key, event_keys in keyword_map.items():
            if key in content:
                matched_keys.extend(event_keys)

        if matched_keys:
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
                    for event_key in set(matched_keys):
                        add_registry_event(events, event_key, d, "NBS China", article_url)

        i += 26

    return dedupe_events(events)


def fetch_china_optional_events(start: date, end: date) -> List[Dict[str, Any]]:
    """
    Optional additions outside strict NBS-only rule.
    We keep them best-effort because the universe document explicitly lists them.
    """
    events: List[Dict[str, Any]] = []

    try:
        url = "http://english.customs.gov.cn/"
        text = BeautifulSoup(session_get_text(url), "html.parser").get_text(" ", strip=True)
        m = re.search(r"(?:next release|release date).*?([A-Z][a-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.I | re.S)
        d = parse_date_any(m.group(1)) if m else None
        if d and in_range(d, start, end):
            add_registry_event(events, "CN_TRADE_BALANCE", d, "China Customs", url)
    except Exception:
        pass

    try:
        url = "http://www.pbc.gov.cn/en/3688110/index.html"
        text = BeautifulSoup(session_get_text(url), "html.parser").get_text(" ", strip=True)
        m = re.search(r"(?:next release|release date).*?([A-Z][a-z]+\s+\d{1,2},\s+\d{4})", text, flags=re.I | re.S)
        d = parse_date_any(m.group(1)) if m else None
        if d and in_range(d, start, end):
            add_registry_event(events, "CN_TSF", d, "PBOC", url)
            add_registry_event(events, "CN_NEW_LOANS", d, "PBOC", url)
    except Exception:
        pass

    return dedupe_events(events)


def china_macro_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    fetchers = [fetch_china_nbs_events, fetch_china_optional_events]
    for fn in fetchers:
        try:
            events.extend(fn(start, end))
        except Exception as e:
            print(f"⚠️ {fn.__name__}: {e}")
    return dedupe_events(events)


# ---------------------------------------------------------
# Top-level macro calendar
# ---------------------------------------------------------
def macro_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    events.extend(us_macro_events(start, end))
    events.extend(euro_area_macro_events(start, end))
    events.extend(china_macro_events(start, end))
    return dedupe_events(events)


# ---------------------------------------------------------
# Earnings
# ---------------------------------------------------------
def normalize_ticker(ticker: str) -> str:
    t = normalize_space(ticker).upper().replace("/", "-")
    if t == "BRK.A":
        return "BRK-A"
    if t == "BRK.B":
        return "BRK-B"
    return t


def finviz_symbol(ticker: str) -> str:
    return normalize_ticker(ticker).replace(".", "-")


def ticker_country(ticker: str) -> str:
    t = normalize_ticker(ticker)
    if t in CHINA_TICKERS:
        return "China"
    if t in EU_TICKERS:
        return "EU"
    return "US"


_FINVIZ_SUFFIX_RE = re.compile(
    r"\b(?:BMO|AMC|DMH|TAS|Before\s+Open|After\s+Close|Time\s+Not\s+Supplied|Unconfirmed)\b",
    flags=re.I,
)


def parse_finviz_earnings_date(raw: Any, ref_start: date, ref_end: date) -> Optional[date]:
    s = normalize_space(raw)
    if not s or s in {"-", "—", "N/A"}:
        return None

    s = s.replace("/a", " BMO").replace("/b", " AMC")
    s = _FINVIZ_SUFFIX_RE.sub("", s)
    s = re.sub(r"\b\d{1,2}:\d{2}\s*[AP]M\b", "", s, flags=re.I)
    s = normalize_space(s.replace(".", " "))

    candidates: List[str] = [s]
    if re.match(r"^[A-Za-z]{3}\s+\d{1,2}$", s):
        candidates = [f"{s} {ref_start.year}", f"{s} {ref_end.year}"]

    for cand in candidates:
        for fmt in ("%b %d %Y", "%b %d, %Y", "%Y-%m-%d", "%b-%d-%y", "%b-%d-%Y"):
            try:
                d = datetime.strptime(cand, fmt).date()
                if in_range(d, ref_start - timedelta(days=30), ref_end + timedelta(days=365)):
                    return d
            except Exception:
                pass

    m = re.search(r"([A-Za-z]{3})\s+(\d{1,2})", s)
    if m:
        mon = datetime.strptime(m.group(1), "%b").month
        day_num = int(m.group(2))
        for y in (ref_start.year, ref_end.year, ref_start.year + 1):
            try:
                d = date(y, mon, day_num)
            except Exception:
                continue
            if abs((d - ref_start).days) <= 370:
                return d
    return None


def event_from_earnings_row(
    ticker: str,
    d: date,
    company: str,
    source: str,
    url: str,
) -> Dict[str, Any]:
    t = normalize_ticker(ticker)
    return {
        "date": d.isoformat(),
        "type": "Earnings",
        "title": t,
        "ticker": t,
        "company": normalize_space(company) or t,
        "country": ticker_country(t),
        "importance": "high",
        "markets": ["Equities"],
        "source": source,
        "url": url,
    }


def fetch_finviz_earnings_from_screener(
    start: date,
    end: date,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    sess = session or requests.Session()
    filters = ["earningsdate_thisweek", "earningsdate_nextweek"]
    found: List[Dict[str, Any]] = []

    for filt in filters:
        page = 1
        while page <= 20:
            params = {
                "v": "111",
                "f": filt,
                "o": "earningsdate",
                "r": str((page - 1) * 20 + 1),
            }
            try:
                tables = html_tables(FINVIZ_SCREENER_URL, params=params, session=sess)
            except Exception:
                break
            if not tables:
                break

            matched_rows = 0
            for df in tables:
                cols = {normalize_space(c).lower(): c for c in df.columns}
                ticker_col = next((cols[k] for k in cols if "ticker" in k), None)
                earnings_col = next((cols[k] for k in cols if "earn" in k), None)
                company_col = next((cols[k] for k in cols if "company" in k or "name" in k), None)
                if not ticker_col or not earnings_col:
                    continue

                for _, row in df.iterrows():
                    ticker = normalize_ticker(row.get(ticker_col, ""))
                    if ticker not in PRIORITY_EARNINGS:
                        continue
                    ed = parse_finviz_earnings_date(row.get(earnings_col, ""), start, end)
                    if not ed or not in_range(ed, start, end):
                        continue
                    company = normalize_space(row.get(company_col, "")) if company_col else ticker
                    found.append(
                        event_from_earnings_row(
                            ticker=ticker,
                            d=ed,
                            company=company,
                            source="Finviz",
                            url=f"{FINVIZ_QUOTE_URL}?t={quote_plus(finviz_symbol(ticker))}",
                        )
                    )
                    matched_rows += 1

            if matched_rows == 0:
                break
            page += 1

    return dedupe_events(found)


def extract_finviz_snapshot_field(soup: BeautifulSoup, field_name: str) -> str:
    target = field_name.strip().lower()
    tables = soup.find_all("table")
    for table in tables:
        cells = [normalize_space(td.get_text(" ")) for td in table.find_all(["td", "th"])]
        for i, cell in enumerate(cells[:-1]):
            if cell.lower() == target:
                return cells[i + 1]
    text = normalize_space(soup.get_text(" "))
    m = re.search(rf"\b{re.escape(field_name)}\b\s+([^|]+?)\s{{2,}}", text, flags=re.I)
    return normalize_space(m.group(1)) if m else ""


def extract_finviz_company_name(soup: BeautifulSoup, ticker: str) -> str:
    def _clean_name(text: str) -> str:
        text = normalize_space(text)
        text = re.sub(rf"^{re.escape(normalize_ticker(ticker))}\s*-\s*", "", text, flags=re.I)
        text = re.sub(r"\s+Stock\s+Price\s+and\s+Quote.*$", "", text, flags=re.I)
        text = re.sub(r"\s*-\s*Stock\s+Price\s+and\s+Quote.*$", "", text, flags=re.I)
        text = re.sub(r"\s*-\s*Finviz.*$", "", text, flags=re.I)
        return normalize_space(text)

    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        content = _clean_name(og_title["content"])
        if content:
            return content

    title = soup.title.string if soup.title and soup.title.string else ""
    title = _clean_name(title)
    return title or normalize_ticker(ticker)


def fetch_finviz_earnings_from_quotes(
    start: date,
    end: date,
    tickers: Optional[Iterable[str]] = None,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    sess = session or requests.Session()
    out: List[Dict[str, Any]] = []
    names = sorted({normalize_ticker(t) for t in (tickers or PRIORITY_EARNINGS)})

    for ticker in names:
        url = f"{FINVIZ_QUOTE_URL}?t={quote_plus(finviz_symbol(ticker))}"
        try:
            html = session_get_text(url, session=sess)
        except Exception as e:
            print(f"⚠️ Finviz quote failed: {ticker} ({e})")
            continue

        soup = BeautifulSoup(html, "html.parser")
        earnings_raw = extract_finviz_snapshot_field(soup, "Earnings")
        if not earnings_raw:
            continue

        ed = parse_finviz_earnings_date(earnings_raw, start, end)
        if not ed or not in_range(ed, start, end):
            continue

        company = extract_finviz_company_name(soup, ticker)
        out.append(event_from_earnings_row(ticker, ed, company, "Finviz", url))

    return dedupe_events(out)


def fetch_fmp_earnings(start: date, end: date) -> List[Dict[str, Any]]:
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        return []
    params = {"from": str(start), "to": str(end), "apikey": api_key}
    r = requests.get(FMP_EARNINGS_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    out: List[Dict[str, Any]] = []
    for e in data:
        ticker = normalize_ticker(e.get("symbol"))
        if ticker not in PRIORITY_EARNINGS:
            continue
        raw_d = str(e.get("date") or "")[:10]
        try:
            d = datetime.strptime(raw_d, "%Y-%m-%d").date()
        except Exception:
            continue
        out.append({
            "date": d.isoformat(),
            "type": "Earnings",
            "title": ticker,
            "ticker": ticker,
            "company": normalize_space(e.get("companyName", ticker)) or ticker,
            "country": ticker_country(ticker),
            "importance": "high",
            "markets": ["Equities"],
            "source": "FMP",
            "url": "",
        })
    return dedupe_events(out)


def earnings_events(start: date, end: date) -> List[Dict[str, Any]]:
    sess = requests.Session()

    finviz_events: List[Dict[str, Any]] = []
    finviz_hit_tickers: Set[str] = set()

    try:
        finviz_events = fetch_finviz_earnings_from_screener(start, end, session=sess)
        finviz_hit_tickers = {normalize_ticker(x.get("ticker", "")) for x in finviz_events}
    except Exception as e:
        print(f"⚠️ fetch_finviz_earnings_from_screener: {e}")

    missing = {normalize_ticker(t) for t in PRIORITY_EARNINGS} - finviz_hit_tickers
    if not finviz_events or missing:
        try:
            quote_events = fetch_finviz_earnings_from_quotes(
                start,
                end,
                tickers=missing or PRIORITY_EARNINGS,
                session=sess,
            )
            finviz_events = dedupe_events(finviz_events + quote_events)
            finviz_hit_tickers = {normalize_ticker(x.get("ticker", "")) for x in finviz_events}
        except Exception as e:
            print(f"⚠️ fetch_finviz_earnings_from_quotes: {e}")

    if finviz_events:
        missing = {normalize_ticker(t) for t in PRIORITY_EARNINGS} - finviz_hit_tickers
        if missing:
            try:
                fmp_events = [
                    x for x in fetch_fmp_earnings(start, end)
                    if normalize_ticker(x.get("ticker", "")) in missing
                ]
                return dedupe_events(finviz_events + fmp_events)
            except Exception as e:
                print(f"⚠️ fetch_fmp_earnings supplement: {e}")
        return dedupe_events(finviz_events)

    try:
        print("⚠️ Finviz returned no usable earnings rows, falling back to FMP")
        return fetch_fmp_earnings(start, end)
    except Exception as e:
        print(f"⚠️ fetch_fmp_earnings fallback: {e}")
        return []


def build_event_calendar(start: Optional[date] = None, end: Optional[date] = None) -> List[Dict[str, Any]]:
    if start is None or end is None:
        start, end = week_bounds()
    return dedupe_events(macro_events(start, end) + earnings_events(start, end))


if __name__ == "__main__":
    s, e = week_bounds()
    print(json.dumps(build_event_calendar(s, e), indent=2, ensure_ascii=False))
