#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


BASE_URL = "https://www.alphavantage.co/query"
TIMEOUT = 30

MAX_MACRO = 12
MAX_EARNINGS = 12

# -----------------------------
# Earnings universe
# -----------------------------
CORE_EARNINGS_WATCHLIST = {
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "JPM", "GS", "MS",
    "TSM", "ASML",
    "XOM", "CVX",
}

# Manual megacap overlay (>500bn bucket idea).
# Keep this easy to update occasionally.
MEGACAP_OVERLAY = {
    "BRK.B", "LLY", "AVGO", "V", "MA",
    "WMT", "ORCL", "COST", "NFLX", "AMD",
}

PRIORITY_EARNINGS = CORE_EARNINGS_WATCHLIST | MEGACAP_OVERLAY

# Higher priority first for trimming when > MAX_EARNINGS
EARNINGS_PRIORITY_RANK = {
    "NVDA": 100, "AAPL": 99, "MSFT": 98, "AMZN": 97, "GOOGL": 96, "META": 95, "TSLA": 94,
    "JPM": 93, "GS": 92, "MS": 91,
    "TSM": 90, "ASML": 89,
    "XOM": 88, "CVX": 87,
    "AVGO": 86, "LLY": 85, "BRK.B": 84, "V": 83, "MA": 82,
    "WMT": 81, "ORCL": 80, "COST": 79, "NFLX": 78, "AMD": 77,
}

# -----------------------------
# Macro whitelist
# -----------------------------
MACRO_EVENT_RULES: List[Tuple[str, str, List[str], str]] = [
    ("Consumer Price Index", "US CPI", ["Rates", "FX", "Equities"], "US"),
    ("CPI", "US CPI", ["Rates", "FX", "Equities"], "US"),
    ("Producer Price Index", "US PPI", ["Rates", "FX", "Equities"], "US"),
    ("PPI", "US PPI", ["Rates", "FX", "Equities"], "US"),
    ("Real GDP", "US GDP", ["Rates", "FX", "Equities"], "US"),
    ("GDP", "US GDP", ["Rates", "FX", "Equities"], "US"),
    ("Retail Sales", "US Retail Sales", ["Equities", "FX", "Rates"], "US"),
    ("Nonfarm Payroll", "US Non-Farm Payrolls", ["Rates", "FX", "Equities"], "US"),
    ("Payroll", "US Non-Farm Payrolls", ["Rates", "FX", "Equities"], "US"),
    ("University of Michigan Consumer Sentiment", "Michigan Sentiment", ["Equities", "FX"], "US"),
    ("Michigan", "Michigan Sentiment", ["Equities", "FX"], "US"),
    ("Industrial Production", "Industrial Production", ["Equities", "Commodities"], "US"),
    ("Housing Starts", "Housing Starts", ["Rates", "Equities"], "US"),
    ("Consumer Confidence", "Consumer Confidence", ["Equities", "FX"], "US"),
    ("ISM Manufacturing", "ISM PMI", ["Equities", "FX", "Commodities"], "US"),
    ("ISM Services", "ISM PMI", ["Equities", "FX"], "US"),
    ("PMI", "ISM PMI", ["Equities", "FX", "Commodities"], "US"),
    ("Federal Funds Rate", "FOMC", ["Rates", "FX", "Equities"], "US"),
    ("ECB", "ECB Meeting", ["Rates", "FX", "Equities"], "EU"),
    ("Bank of England", "BoE Meeting", ["Rates", "FX"], "UK"),
    ("Bank of Japan", "BoJ Meeting", ["Rates", "FX"], "JP"),
    ("China CPI", "China CPI", ["Commodities", "Equities", "FX"], "CN"),
    ("China GDP", "China GDP", ["Commodities", "Equities", "FX"], "CN"),
]

# -----------------------------
# Helpers
# -----------------------------
def load_api_key() -> Optional[str]:
    key = os.getenv("ALPHAVANTAGE_API_KEY") or os.getenv("ALPHA_VANTAGE_API_KEY")
    if key:
        return key.strip()

    for path in (".env", "alpha_vantage.env", "ALPHAVANTAGE_API_KEY.env"):
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#") or "=" not in s:
                        continue
                    k, v = s.split("=", 1)
                    if k.strip() in {"ALPHAVANTAGE_API_KEY", "ALPHA_VANTAGE_API_KEY"} and v.strip():
                        return v.strip()
        except Exception:
            continue
    return None


def parse_iso_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def week_bounds(today: Optional[date] = None) -> Tuple[date, date]:
    today = today or date.today()
    return today, today + timedelta(days=7)


def in_range(d: Optional[date], start: date, end: date) -> bool:
    return d is not None and start <= d <= end


def fetch_csv(function_name: str, apikey: str, **params: Any) -> List[Dict[str, str]]:
    query = {"function": function_name, "apikey": apikey, "datatype": "csv"}
    query.update(params)
    r = requests.get(BASE_URL, params=query, timeout=TIMEOUT)
    r.raise_for_status()
    text = r.text.strip()

    # Alpha Vantage may return JSON-ish error text if throttled / invalid key.
    if text.startswith("{") and ("Note" in text or "Error" in text or "Information" in text):
        return []

    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def normalize_country(country: str) -> str:
    c = (country or "").strip().lower()
    mapping = {
        "united states": "US",
        "usa": "US",
        "us": "US",
        "euro zone": "EU",
        "euro area": "EU",
        "european union": "EU",
        "united kingdom": "UK",
        "uk": "UK",
        "japan": "JP",
        "china": "CN",
    }
    return mapping.get(c, country or "")


def classify_macro_event(event_name: str, country: str) -> Optional[Dict[str, Any]]:
    name = (event_name or "").strip()
    country_norm = normalize_country(country)
    low = name.lower()

    for needle, title, markets, expected_country in MACRO_EVENT_RULES:
        if needle.lower() in low:
            # If the rule is country-specific, enforce it where relevant
            if expected_country in {"US", "EU", "UK", "JP", "CN"}:
                if country_norm and country_norm != expected_country:
                    continue
            return {
                "title": title,
                "country": expected_country if expected_country else country_norm,
                "markets": markets,
            }
    return None


def dedupe_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for e in sorted(events, key=lambda x: (x.get("date", ""), x.get("type", ""), x.get("title", ""))):
        key = (e.get("date"), e.get("type"), e.get("title"))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


# -----------------------------
# Macro from Alpha Vantage
# -----------------------------
def fetch_macro_events(start: date, end: date, apikey: str) -> List[Dict[str, Any]]:
    rows = fetch_csv("ECONOMIC_CALENDAR", apikey, horizon="3month")
    out: List[Dict[str, Any]] = []

    for row in rows:
        d = parse_iso_date(row.get("date", ""))
        if not in_range(d, start, end):
            continue

        classified = classify_macro_event(row.get("event", ""), row.get("country", ""))
        if not classified:
            continue

        out.append({
            "date": d.isoformat(),
            "type": "Macro",
            "title": classified["title"],
            "country": classified["country"],
            "importance": "high",
            "markets": classified["markets"],
            "source": "Alpha Vantage",
        })

    out = dedupe_events(out)
    return out[:MAX_MACRO]


# -----------------------------
# Earnings from Alpha Vantage
# -----------------------------
def fetch_earnings_events(start: date, end: date, apikey: str) -> List[Dict[str, Any]]:
    rows = fetch_csv("EARNINGS_CALENDAR", apikey, horizon="3month")
    out: List[Dict[str, Any]] = []

    for row in rows:
        symbol = (row.get("symbol") or "").strip().upper()
        if not symbol or symbol not in PRIORITY_EARNINGS:
            continue

        report_date = parse_iso_date(row.get("reportDate", "") or row.get("date", ""))
        if not in_range(report_date, start, end):
            continue

        out.append({
            "date": report_date.isoformat(),
            "type": "Earnings",
            "title": f"{symbol} earnings",
            "ticker": symbol,
            "company": (row.get("name") or row.get("company") or symbol).strip(),
            "country": "",
            "importance": "high",
            "markets": ["Equities"],
            "source": "Alpha Vantage",
            "_priority": EARNINGS_PRIORITY_RANK.get(symbol, 0),
        })

    out = dedupe_events(out)
    out = sorted(out, key=lambda x: (-int(x.get("_priority", 0)), x.get("date", ""), x.get("ticker", "")))
    out = out[:MAX_EARNINGS]

    for e in out:
        e.pop("_priority", None)
    return out


# -----------------------------
# Public API expected by weekly_compiler.py
# -----------------------------
def build_event_calendar(start_date: Optional[date] = None, end_date: Optional[date] = None) -> List[Dict[str, Any]]:
    start, end = (start_date, end_date) if (start_date and end_date) else week_bounds()
    apikey = load_api_key()
    if not apikey:
        print("⚠️ Alpha Vantage API key not found. Returning empty event calendar.")
        return []

    macro = fetch_macro_events(start, end, apikey)
    earnings = fetch_earnings_events(start, end, apikey)

    events = dedupe_events(macro + earnings)
    events = sorted(events, key=lambda x: (x.get("date", ""), x.get("type", ""), x.get("title", "")))
    return events


if __name__ == "__main__":
    s, e = week_bounds()
    events = build_event_calendar(s, e)
    for evt in events:
        print(evt)