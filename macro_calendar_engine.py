#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

ALPHAVANTAGE_EARNINGS_URL = "https://www.alphavantage.co/query"
DEFAULT_USER_AGENT = "1XW-CalendarBot/3.0 (+https://1xwtrading.com)"

CORE_EARNINGS_WATCHLIST: Dict[str, str] = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "NVDA": "NVIDIA",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
    "META": "Meta",
    "TSLA": "Tesla",
    "JPM": "JPMorgan Chase",
    "GS": "Goldman Sachs",
    "MS": "Morgan Stanley",
    "TSM": "TSMC",
    "ASML": "ASML",
    "XOM": "Exxon Mobil",
    "CVX": "Chevron",
}

MEGACAP_OVERLAY: Dict[str, str] = {
    "AVGO": "Broadcom",
    "BRK.B": "Berkshire Hathaway",
    "LLY": "Eli Lilly",
    "V": "Visa",
    "MA": "Mastercard",
    "WMT": "Walmart",
    "ORCL": "Oracle",
    "NFLX": "Netflix",
    "COST": "Costco",
    "UNH": "UnitedHealth Group",
}

PRIORITY_EARNINGS: Dict[str, str] = {**CORE_EARNINGS_WATCHLIST, **MEGACAP_OVERLAY}

TICKER_COUNTRY = {
    "TSM": "Taiwan",
    "ASML": "EU",
    "BABA": "China",
    "TCEHY": "China",
}

FOMC_SCHEDULE = {
    2026: [
        date(2026, 1, 28), date(2026, 3, 18), date(2026, 4, 29), date(2026, 6, 17),
        date(2026, 7, 29), date(2026, 9, 16), date(2026, 10, 28), date(2026, 12, 9),
    ]
}

ECB_SCHEDULE = {
    2026: [
        date(2026, 1, 22), date(2026, 3, 5), date(2026, 4, 16), date(2026, 6, 4),
        date(2026, 7, 23), date(2026, 9, 10), date(2026, 10, 22), date(2026, 12, 10),
    ]
}


MACRO_EVENT_SPECS = [
    {"title": "US CPI", "country": "US", "importance": "high", "markets": ["Rates", "FX", "Equities"], "rule": "us_cpi"},
    {"title": "US Non-Farm Payrolls", "country": "US", "importance": "high", "markets": ["Rates", "FX", "Equities"], "rule": "us_nfp"},
    {"title": "FOMC Meeting", "country": "US", "importance": "high", "markets": ["Rates", "FX", "Equities"], "rule": "fomc"},
    {"title": "ECB Meeting", "country": "EU", "importance": "high", "markets": ["Rates", "FX", "Equities"], "rule": "ecb"},
    {"title": "China CPI", "country": "China", "importance": "high", "markets": ["Commodities", "Equities", "FX"], "rule": "china_cpi"},
    {"title": "China GDP", "country": "China", "importance": "high", "markets": ["Commodities", "Equities", "FX"], "rule": "china_gdp"},
    {"title": "ISM Manufacturing PMI", "country": "US", "importance": "medium", "markets": ["Equities", "FX", "Commodities"], "rule": "ism_pmi"},
    {"title": "US Retail Sales", "country": "US", "importance": "medium", "markets": ["Equities", "FX", "Rates"], "rule": "retail_sales"},
    {"title": "US GDP", "country": "US", "importance": "medium", "markets": ["Rates", "FX", "Equities"], "rule": "us_gdp"},
    {"title": "US PPI", "country": "US", "importance": "medium", "markets": ["Rates", "FX", "Equities"], "rule": "us_ppi"},
    {"title": "China Manufacturing PMI", "country": "China", "importance": "medium", "markets": ["Commodities", "Equities", "FX"], "rule": "china_pmi"},
    {"title": "University of Michigan Sentiment", "country": "US", "importance": "medium", "markets": ["Equities", "Rates", "FX"], "rule": "michigan_sentiment"},
    {"title": "US Industrial Production", "country": "US", "importance": "medium", "markets": ["Equities", "Commodities", "FX"], "rule": "industrial_production"},
    {"title": "US Housing Starts", "country": "US", "importance": "low", "markets": ["Equities", "Rates"], "rule": "housing_starts"},
    {"title": "US Consumer Confidence", "country": "US", "importance": "low", "markets": ["Equities", "FX"], "rule": "consumer_confidence"},
]


def week_bounds(today: Optional[date] = None) -> Tuple[date, date]:
    today = today or date.today()
    return today, today + timedelta(days=7)


def in_range(day: date, start: date, end: date) -> bool:
    return start <= day <= end


def month_iter(start: date, end: date) -> List[Tuple[int, int]]:
    y, m = start.year, start.month
    out: List[Tuple[int, int]] = []
    while (y, m) <= (end.year, end.month):
        out.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return out


def quarter_iter(start: date, end: date) -> List[Tuple[int, int]]:
    out: Set[Tuple[int, int]] = set()
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        quarter = ((cursor.month - 1) // 3) + 1
        out.add((cursor.year, quarter))
        next_month = cursor.month + 1
        next_year = cursor.year + (1 if next_month == 13 else 0)
        cursor = date(next_year, 1 if next_month == 13 else next_month, 1)
    return sorted(out)


def first_weekday(year: int, month: int, weekday: int) -> date:
    d = date(year, month, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    return d


def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    d = first_weekday(year, month, weekday)
    d += timedelta(days=7 * (n - 1))
    return d


def business_day(year: int, month: int, day_num: int) -> date:
    d = date(year, month, min(day_num, 28 if month == 2 else 30 if month in {4, 6, 9, 11} else 31))
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def first_business_day(year: int, month: int) -> date:
    d = date(year, month, 1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def us_cpi_dates(start: date, end: date) -> List[date]:
    out: List[date] = []
    for year, month in month_iter(start, end):
        out.append(nth_weekday(year, month, 2, 2))
    return [d for d in out if in_range(d, start, end)]


def us_nfp_dates(start: date, end: date) -> List[date]:
    out: List[date] = []
    for year, month in month_iter(start, end):
        out.append(first_weekday(year, month, 4))
    return [d for d in out if in_range(d, start, end)]


def fomc_dates(start: date, end: date) -> List[date]:
    schedule = FOMC_SCHEDULE.get(start.year, []) + ([] if end.year == start.year else FOMC_SCHEDULE.get(end.year, []))
    return [d for d in schedule if in_range(d, start, end)]


def ecb_dates(start: date, end: date) -> List[date]:
    schedule = ECB_SCHEDULE.get(start.year, []) + ([] if end.year == start.year else ECB_SCHEDULE.get(end.year, []))
    return [d for d in schedule if in_range(d, start, end)]


def china_cpi_dates(start: date, end: date) -> List[date]:
    out: List[date] = []
    for year, month in month_iter(start, end):
        out.append(business_day(year, month, 10))
    return [d for d in out if in_range(d, start, end)]


def china_gdp_dates(start: date, end: date) -> List[date]:
    release_month = {1: 4, 2: 7, 3: 10, 4: 1}
    out: List[date] = []
    for year, quarter in quarter_iter(start, end):
        month = release_month[quarter]
        release_year = year if quarter < 4 else year + 1
        out.append(business_day(release_year, month, 15))
    return [d for d in out if in_range(d, start, end)]


def ism_pmi_dates(start: date, end: date) -> List[date]:
    return [d for year, month in month_iter(start, end) for d in [first_business_day(year, month)] if in_range(d, start, end)]


def retail_sales_dates(start: date, end: date) -> List[date]:
    return [d for year, month in month_iter(start, end) for d in [business_day(year, month, 15)] if in_range(d, start, end)]


def us_gdp_dates(start: date, end: date) -> List[date]:
    quarter_map = {1: (4, 29), 2: (7, 30), 3: (10, 29), 4: (1, 29)}
    out: List[date] = []
    for year, quarter in quarter_iter(start, end):
        month, day_num = quarter_map[quarter]
        release_year = year if quarter < 4 else year + 1
        out.append(business_day(release_year, month, day_num))
    return [d for d in out if in_range(d, start, end)]


def us_ppi_dates(start: date, end: date) -> List[date]:
    return [d for year, month in month_iter(start, end) for d in [business_day(year, month, 13)] if in_range(d, start, end)]


def china_pmi_dates(start: date, end: date) -> List[date]:
    out: List[date] = []
    for year, month in month_iter(start, end):
        month_for_release = month
        year_for_release = year
        out.append(business_day(year_for_release, month_for_release, 1))
    return [d for d in out if in_range(d, start, end)]


def michigan_sentiment_dates(start: date, end: date) -> List[date]:
    return [d for year, month in month_iter(start, end) for d in [nth_weekday(year, month, 4, 2)] if in_range(d, start, end)]


def industrial_production_dates(start: date, end: date) -> List[date]:
    return [d for year, month in month_iter(start, end) for d in [business_day(year, month, 16)] if in_range(d, start, end)]


def housing_starts_dates(start: date, end: date) -> List[date]:
    return [d for year, month in month_iter(start, end) for d in [business_day(year, month, 18)] if in_range(d, start, end)]


def consumer_confidence_dates(start: date, end: date) -> List[date]:
    return [d for year, month in month_iter(start, end) for d in [nth_weekday(year, month, 1, 4)] if in_range(d, start, end)]


RULE_MAP = {
    "us_cpi": us_cpi_dates,
    "us_nfp": us_nfp_dates,
    "fomc": fomc_dates,
    "ecb": ecb_dates,
    "china_cpi": china_cpi_dates,
    "china_gdp": china_gdp_dates,
    "ism_pmi": ism_pmi_dates,
    "retail_sales": retail_sales_dates,
    "us_gdp": us_gdp_dates,
    "us_ppi": us_ppi_dates,
    "china_pmi": china_pmi_dates,
    "michigan_sentiment": michigan_sentiment_dates,
    "industrial_production": industrial_production_dates,
    "housing_starts": housing_starts_dates,
    "consumer_confidence": consumer_confidence_dates,
}


def macro_events(start: date, end: date, max_events: int = 12) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    importance_rank = {"high": 0, "medium": 1, "low": 2}
    for spec in MACRO_EVENT_SPECS:
        rule_name = spec["rule"]
        rule = RULE_MAP[rule_name]
        for event_date in rule(start, end):
            events.append(
                {
                    "date": event_date.isoformat(),
                    "type": "Macro",
                    "title": spec["title"],
                    "country": spec["country"],
                    "importance": spec["importance"],
                    "markets": spec["markets"],
                    "source": "Macro Calendar",
                }
            )
    events.sort(key=lambda item: (item["date"], importance_rank.get(item["importance"], 9), item["title"]))
    return events[:max_events]


def av_headers() -> Dict[str, str]:
    return {"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json, text/csv;q=0.9,*/*;q=0.8"}


def parse_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def load_earnings_from_alpha_vantage(start: date, end: date) -> List[Dict[str, Any]]:
    api_key = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
    if not api_key:
        return []
    params = {
        "function": "EARNINGS_CALENDAR",
        "horizon": "3month",
        "apikey": api_key,
    }
    response = requests.get(ALPHAVANTAGE_EARNINGS_URL, params=params, headers=av_headers(), timeout=30)
    response.raise_for_status()
    raw = response.text
    if not raw.strip():
        return []
    rows = list(csv.DictReader(io.StringIO(raw)))
    out: List[Dict[str, Any]] = []
    for row in rows:
        ticker = (row.get("symbol") or row.get("ticker") or "").strip().upper()
        if ticker not in PRIORITY_EARNINGS:
            continue
        event_date = parse_date(row.get("reportDate") or row.get("fiscalDateEnding") or row.get("date"))
        if not event_date or not in_range(event_date, start, end):
            continue
        company = PRIORITY_EARNINGS.get(ticker) or row.get("name") or row.get("company") or ticker
        out.append(
            {
                "date": event_date.isoformat(),
                "type": "Earnings",
                "title": f"{ticker} earnings",
                "ticker": ticker,
                "company": company,
                "country": TICKER_COUNTRY.get(ticker, "US"),
                "importance": "high",
                "markets": ["Equities"],
                "source": "Alpha Vantage",
            }
        )
    return out


def earnings_priority(item: Dict[str, Any]) -> Tuple[int, str, str]:
    ticker = str(item.get("ticker", ""))
    if ticker in CORE_EARNINGS_WATCHLIST:
        base_rank = 0
    elif ticker in MEGACAP_OVERLAY:
        base_rank = 1
    else:
        base_rank = 2
    return (base_rank, str(item.get("date", "")), ticker)


def earnings_events(start: date, end: date, max_events: int = 12) -> List[Dict[str, Any]]:
    events = load_earnings_from_alpha_vantage(start, end)
    if not events:
        print("⚠️ earnings_events: no Alpha Vantage data available (missing key or request failure).")
        return []
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for event in events:
        key = (str(event.get("ticker", "")), str(event.get("date", "")))
        dedup[key] = event
    ordered = sorted(dedup.values(), key=earnings_priority)
    return ordered[:max_events]


def build_event_calendar(
    start: Optional[date] = None,
    end: Optional[date] = None,
    max_macro_events: int = 12,
    max_earnings_events: int = 12,
) -> List[Dict[str, Any]]:
    if start is None or end is None:
        start, end = week_bounds()
    events = macro_events(start, end, max_events=max_macro_events) + earnings_events(start, end, max_events=max_earnings_events)
    type_rank = {"Macro": 0, "Earnings": 1}
    importance_rank = {"high": 0, "medium": 1, "low": 2}
    events.sort(key=lambda item: (item["date"], type_rank.get(str(item.get("type")), 9), importance_rank.get(str(item.get("importance")), 9), str(item.get("title", ""))))
    return events


if __name__ == "__main__":
    s, e = week_bounds()
    print(json.dumps(build_event_calendar(s, e), indent=2))
