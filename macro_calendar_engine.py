#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

# -----------------------------------------------------------------------------
# Earnings universe
# -----------------------------------------------------------------------------
CORE_EARNINGS_WATCHLIST = {
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "JPM", "GS", "MS",
    "TSM", "ASML",
    "XOM", "CVX",
}

MEGACAP_OVERLAY = {
    "AVGO", "BRK.B", "LLY", "V", "MA", "ORCL", "NFLX", "COST", "WMT",
}

PRIORITY_EARNINGS = CORE_EARNINGS_WATCHLIST | MEGACAP_OVERLAY

# -----------------------------------------------------------------------------
# 2026 official / semi-official date tables
#   - Fed / ECB / BoE / BoJ dates are official meeting dates.
#   - CPI / PPI / GDP dates are official release dates.
#   - Retail Sales is partly exact for March and otherwise a disciplined rule.
# -----------------------------------------------------------------------------
FED_2026 = [
    date(2026, 1, 28), date(2026, 3, 18), date(2026, 4, 29), date(2026, 6, 17),
    date(2026, 7, 29), date(2026, 9, 16), date(2026, 10, 28), date(2026, 12, 9),
]

ECB_2026 = [
    date(2026, 2, 5), date(2026, 3, 19), date(2026, 4, 30), date(2026, 6, 11),
    date(2026, 7, 23), date(2026, 9, 10), date(2026, 10, 29), date(2026, 12, 17),
]

BOE_2026 = [
    date(2026, 2, 5), date(2026, 3, 19), date(2026, 4, 30), date(2026, 6, 18),
    date(2026, 7, 30), date(2026, 9, 17), date(2026, 11, 5), date(2026, 12, 17),
]

BOJ_2026 = [
    date(2026, 1, 23), date(2026, 3, 19), date(2026, 4, 28), date(2026, 6, 16),
    date(2026, 7, 31), date(2026, 9, 18), date(2026, 10, 30), date(2026, 12, 18),
]

US_CPI_2026 = [
    date(2026, 1, 13), date(2026, 2, 13), date(2026, 3, 11), date(2026, 4, 10),
    date(2026, 5, 12), date(2026, 6, 10), date(2026, 7, 14), date(2026, 8, 12),
    date(2026, 9, 11), date(2026, 10, 14), date(2026, 11, 10),
]

US_PPI_2026 = [
    date(2026, 1, 14), date(2026, 1, 30), date(2026, 2, 27), date(2026, 3, 18),
    date(2026, 4, 14), date(2026, 5, 13), date(2026, 6, 11), date(2026, 7, 15),
    date(2026, 8, 13), date(2026, 9, 10), date(2026, 10, 15), date(2026, 11, 13),
]

US_NFP_2026 = [
    date(2026, 2, 13),  # Jan 2026 release was delayed in the post-shutdown schedule
    date(2026, 3, 6), date(2026, 4, 3), date(2026, 5, 8), date(2026, 6, 5),
    date(2026, 7, 2), date(2026, 8, 7), date(2026, 9, 4), date(2026, 10, 2),
    date(2026, 11, 6), date(2026, 12, 4),
]

US_GDP_2026 = [
    date(2026, 3, 13), date(2026, 4, 30), date(2026, 5, 28), date(2026, 6, 25),
    date(2026, 7, 30), date(2026, 8, 26), date(2026, 9, 30), date(2026, 10, 29),
]

# Exact March 2026 release from Census, then disciplined rule-based fallback for later months.
US_RETAIL_SALES_2026_KNOWN = [date(2026, 3, 6)]

# University of Michigan preliminary releases: second Friday convention works well.
# March 2026 official preliminary release is 13 Mar.

# China conventions (kept rule-based; source label reflects this)
# CPI around the 10th; GDP around mid-Jan/Apr/Jul/Oct.


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def week_bounds(today: Optional[date] = None) -> Tuple[date, date]:
    today = today or date.today()
    return today, today + timedelta(days=7)


def in_range(d: date, start: date, end: date) -> bool:
    return start <= d <= end


def business_day_or_next(d: date) -> date:
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def second_friday(year: int, month: int) -> date:
    d = date(year, month, 1)
    fridays: List[date] = []
    while d.month == month:
        if d.weekday() == 4:
            fridays.append(d)
        d += timedelta(days=1)
    return fridays[1]


def first_business_day(year: int, month: int) -> date:
    return business_day_or_next(date(year, month, 1))


def quarter_months() -> Sequence[int]:
    return (1, 4, 7, 10)


def month_range(start: date, end: date) -> Iterable[Tuple[int, int]]:
    y, m = start.year, start.month
    end_key = (end.year, end.month)
    while (y, m) <= end_key:
        yield y, m
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def event(
    d: date,
    title: str,
    type_: str,
    markets: List[str],
    source: str,
    country: str = "",
    importance: str = "high",
    **extra: Any,
) -> Dict[str, Any]:
    out = {
        "date": d.isoformat(),
        "type": type_,
        "title": title,
        "country": country,
        "importance": importance,
        "markets": markets,
        "source": source,
    }
    out.update(extra)
    return out


def dedupe_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[Tuple[str, str, str]] = set()
    clean: List[Dict[str, Any]] = []
    for ev in sorted(events, key=lambda x: (x["date"], x["type"], x["title"])):
        key = (ev.get("date", ""), ev.get("type", ""), ev.get("title", ""))
        if key in seen:
            continue
        seen.add(key)
        clean.append(ev)
    return clean


# -----------------------------------------------------------------------------
# Macro event builders
# -----------------------------------------------------------------------------
def add_from_calendar(
    events: List[Dict[str, Any]],
    dates: Sequence[date],
    start: date,
    end: date,
    *,
    title: str,
    markets: List[str],
    source: str,
    country: str,
    importance: str = "high",
) -> None:
    for d in dates:
        if in_range(d, start, end):
            events.append(event(d, title, "Macro", markets, source, country=country, importance=importance))


def add_us_retail_sales(events: List[Dict[str, Any]], start: date, end: date) -> None:
    # Keep exact known release(s), then a sensible rule around the 15th for later months.
    for d in US_RETAIL_SALES_2026_KNOWN:
        if in_range(d, start, end):
            events.append(event(d, "US Retail Sales", "Macro", ["Equities", "FX", "Rates"], "Retail Calendar", country="US"))

    for y, m in month_range(start, end):
        if y != 2026 or m <= 3:
            continue
        d = business_day_or_next(date(y, m, 15))
        if in_range(d, start, end):
            events.append(event(d, "US Retail Sales", "Macro", ["Equities", "FX", "Rates"], "Retail Calendar", country="US"))


def add_ism_pmi(events: List[Dict[str, Any]], start: date, end: date) -> None:
    for y, m in month_range(start, end):
        d = first_business_day(y, m)
        if in_range(d, start, end):
            events.append(event(d, "ISM PMI", "Macro", ["Equities", "Rates", "FX", "Commodities"], "ISM Calendar", country="US"))


def add_michigan(events: List[Dict[str, Any]], start: date, end: date) -> None:
    for y, m in month_range(start, end):
        d = second_friday(y, m)
        if in_range(d, start, end):
            events.append(event(d, "University of Michigan Sentiment", "Macro", ["Equities", "Rates", "FX"], "Michigan Survey", country="US", importance="medium"))


def add_china_cpi(events: List[Dict[str, Any]], start: date, end: date) -> None:
    for y, m in month_range(start, end):
        d = business_day_or_next(date(y, m, 10))
        if in_range(d, start, end):
            events.append(event(d, "China CPI", "Macro", ["Equities", "FX", "Commodities"], "China Macro Calendar", country="CN"))


def add_china_gdp(events: List[Dict[str, Any]], start: date, end: date) -> None:
    for y, m in month_range(start, end):
        if m in quarter_months():
            d = business_day_or_next(date(y, m, 15))
            if in_range(d, start, end):
                events.append(event(d, "China GDP", "Macro", ["Equities", "FX", "Commodities"], "China Macro Calendar", country="CN"))


def macro_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    add_from_calendar(events, US_CPI_2026, start, end, title="US CPI", markets=["Equities", "Rates", "FX"], source="BLS CPI Schedule", country="US")
    add_from_calendar(events, US_PPI_2026, start, end, title="US PPI", markets=["Equities", "Rates", "FX"], source="BLS PPI Schedule", country="US")
    add_from_calendar(events, US_NFP_2026, start, end, title="US Non-Farm Payrolls", markets=["Equities", "Rates", "FX"], source="BLS Employment Schedule", country="US")
    add_from_calendar(events, US_GDP_2026, start, end, title="US GDP", markets=["Equities", "Rates", "FX"], source="BEA GDP Schedule", country="US")

    add_from_calendar(events, FED_2026, start, end, title="FOMC Meeting", markets=["Equities", "Rates", "FX"], source="Fed Calendar", country="US")
    add_from_calendar(events, ECB_2026, start, end, title="ECB Meeting", markets=["Equities", "Rates", "FX"], source="ECB Calendar", country="EU")
    add_from_calendar(events, BOE_2026, start, end, title="BoE Meeting", markets=["Rates", "FX"], source="BoE Calendar", country="UK")
    add_from_calendar(events, BOJ_2026, start, end, title="BoJ Meeting", markets=["Rates", "FX"], source="BoJ Calendar", country="JP")

    add_us_retail_sales(events, start, end)
    add_ism_pmi(events, start, end)
    add_michigan(events, start, end)
    add_china_cpi(events, start, end)
    add_china_gdp(events, start, end)

    return dedupe_events(events)


# -----------------------------------------------------------------------------
# Earnings
# -----------------------------------------------------------------------------
def fetch_alpha_vantage_calendar(api_key: str, horizon: str = "3month") -> List[Dict[str, str]]:
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "EARNINGS_CALENDAR",
        "horizon": horizon,
        "apikey": api_key,
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    text = response.text.strip()
    # Alpha Vantage returns CSV for this endpoint.
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(r) for r in reader]
    return rows


def earnings_events(start: date, end: date) -> List[Dict[str, Any]]:
    api_key = os.getenv("ALPHAVANTAGE_API_KEY") or os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        return []

    try:
        raw_rows = fetch_alpha_vantage_calendar(api_key, horizon="3month")
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for row in raw_rows:
        symbol = (row.get("symbol") or "").strip().upper()
        if symbol not in PRIORITY_EARNINGS:
            continue
        report_date = (row.get("reportDate") or row.get("report_date") or "").strip()
        if not report_date:
            continue
        try:
            d = datetime.strptime(report_date[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if not in_range(d, start, end):
            continue

        company = (row.get("name") or row.get("companyName") or symbol).strip() or symbol
        out.append(
            event(
                d,
                f"{symbol} earnings",
                "Earnings",
                ["Equities"],
                "Alpha Vantage",
                country="US",
                importance="high",
                ticker=symbol,
                company=company,
            )
        )

    out.sort(key=lambda x: (x["date"], x.get("ticker", ""), x["title"]))
    return dedupe_events(out)[:12]


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def build_event_calendar(start: Optional[date] = None, end: Optional[date] = None) -> List[Dict[str, Any]]:
    if start is None or end is None:
        start, end = week_bounds()

    events = macro_events(start, end) + earnings_events(start, end)
    events = dedupe_events(events)

    macro = [e for e in events if e["type"] == "Macro"][:12]
    earn = [e for e in events if e["type"] == "Earnings"][:12]
    out = sorted(macro + earn, key=lambda x: (x["date"], x["type"], x["title"]))
    return out


if __name__ == "__main__":
    s, e = week_bounds()
    print(json.dumps(build_event_calendar(s, e), indent=2))
