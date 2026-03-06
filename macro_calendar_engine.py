#!/usr/bin/env python3
from __future__ import annotations
import json, os, requests
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

FMP_EARNINGS_URL = "https://financialmodelingprep.com/stable/earnings-calendar"
PRIORITY_EARNINGS = {
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","NFLX","AMD","AVGO",
    "JPM","V","MA","BRK.B","XOM","LLY","UNH","COST","WMT","ORCL",
    "ASML","SAP","NESN","NOVO-B","ROG","SHEL","TTE","MC","RMS","AZN",
    "TSM","BABA","TCEHY","PDD","JD","BIDU"
}
CHINA_TICKERS = {"BABA","TCEHY","PDD","JD","BIDU"}
EU_TICKERS = {"ASML","SAP","NESN","NOVO-B","ROG","SHEL","TTE","MC","RMS","AZN"}

def week_bounds(today: Optional[date] = None) -> Tuple[date, date]:
    today = today or date.today()
    return today, today + timedelta(days=7)

def in_range(d: date, start: date, end: date) -> bool:
    return start <= d <= end

def second_tuesday(year: int, month: int) -> date:
    d = date(year, month, 1)
    tuesdays = []
    i = 0
    while len(tuesdays) < 2:
        dd = d + timedelta(days=i)
        if dd.month != month:
            break
        if dd.weekday() == 1:
            tuesdays.append(dd)
        i += 1
    return tuesdays[1]

def first_friday(year: int, month: int) -> date:
    d = date(year, month, 1)
    i = 0
    while True:
        dd = d + timedelta(days=i)
        if dd.weekday() == 4:
            return dd
        i += 1

def macro_events(start: date, end: date) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    year = start.year
    cpi = second_tuesday(year, start.month)
    if in_range(cpi, start, end):
        events.append({"date": cpi.isoformat(), "type": "Macro", "title": "US CPI", "country": "US", "importance": "high", "markets": ["Rates", "FX", "Equities"], "source": "1XW Rules"})
    nfp = first_friday(year, start.month)
    if in_range(nfp, start, end):
        events.append({"date": nfp.isoformat(), "type": "Macro", "title": "US Non-Farm Payrolls", "country": "US", "importance": "high", "markets": ["Rates", "FX", "Equities"], "source": "1XW Rules"})
    china_cpi = date(year, start.month, 9)
    if in_range(china_cpi, start, end):
        events.append({"date": china_cpi.isoformat(), "type": "Macro", "title": "China CPI", "country": "China", "importance": "high", "markets": ["Commodities", "Equities", "FX"], "source": "1XW Rules"})
    events.sort(key=lambda x: (x["date"], x["title"]))
    return events

def earnings_events(start: date, end: date) -> List[Dict[str, Any]]:
    api_key = os.getenv("FMP_API_KEY")
    if not api_key:
        return []
    params = {"from": str(start), "to": str(end), "apikey": api_key}
    r = requests.get(FMP_EARNINGS_URL, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()
    out: List[Dict[str, Any]] = []
    for e in data:
        ticker = e.get("symbol")
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
        out.append({"date": d.isoformat(), "type": "Earnings", "title": f"{ticker} earnings", "ticker": ticker, "company": e.get("companyName", ticker), "country": country, "importance": "high", "markets": ["Equities"], "source": "FMP"})
    out.sort(key=lambda x: (x["date"], x["title"]))
    return out[:12]

def build_event_calendar(start: Optional[date] = None, end: Optional[date] = None) -> List[Dict[str, Any]]:
    if start is None or end is None:
        start, end = week_bounds()
    events = macro_events(start, end) + earnings_events(start, end)
    events.sort(key=lambda x: (x["date"], x["type"], x["title"]))
    return events

if __name__ == "__main__":
    s, e = week_bounds()
    print(json.dumps(build_event_calendar(s, e), indent=2))
