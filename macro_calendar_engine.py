#!/usr/bin/env python3
from __future__ import annotations
import json, os, re, requests
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

USER_AGENT = "Mozilla/5.0"
BLS_SCHEDULE_URL = "https://www.bls.gov/schedule/2026/home.htm"
FED_FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
ECB_MGCGC_URL = "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html"
NBS_2026_CAL_URL = "https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/202512/t20251226_1962154.html"
FMP_EARNINGS_URL = "https://financialmodelingprep.com/stable/earnings-calendar"
PRIORITY_EARNINGS_TICKERS = {
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","NFLX","AMD","AVGO",
    "JPM","V","MA","BRK.B","XOM","LLY","UNH","COST","WMT","ORCL",
    "ASML","SAP","NESN","NOVO-B","ROG","SHEL","TTE","MC","RMS","AZN",
    "TSM","BABA","TCEHY","PDD","JD","BIDU",
}
MIN_MARKET_CAP_USD = 100_000_000_000
MONTH_NUM = {"January":1,"February":2,"March":3,"April":4,"May":5,"June":6,"July":7,"August":8,"September":9,"October":10,"November":11,"December":12}

def _norm(x: Any) -> str: return str(x or "").strip()
def _strip_html(html: str) -> str:
    txt = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    txt = re.sub(r"(?is)<style.*?>.*?</style>", " ", txt)
    txt = re.sub(r"(?s)<[^>]+>", " ", txt)
    return re.sub(r"\s+", " ", txt).strip()
def _fetch_text(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=25)
    r.raise_for_status()
    return r.text
def _week_bounds(today: Optional[date]=None) -> Tuple[date,date]:
    today = today or date.today()
    return today, today + timedelta(days=7)
def _date_in_range(d: date, start: date, end: date) -> bool: return start <= d <= end
def _markets_for_macro(title: str, country: str) -> List[str]:
    t = title.lower(); tags = set()
    if any(k in t for k in ["cpi","gdp","payroll","employment","unemployment","pmi","retail sales","industrial production"]):
        tags.update(["Rates","FX","Equities"])
    if any(k in t for k in ["fomc","fed","ecb","rate decision","interest rate"]):
        tags.update(["Rates","FX"])
    if country == "China": tags.update(["Commodities","Equities"])
    if not tags: tags.add("Equities")
    order = ["Rates","FX","Equities","Commodities","Crypto"]
    return [x for x in order if x in tags]
def _dedup(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out, seen = [], set()
    for ev in events:
        key = (_norm(ev.get("date")), _norm(ev.get("type")).lower(), _norm(ev.get("title")).lower())
        if key in seen: continue
        seen.add(key); out.append(ev)
    out.sort(key=lambda x: (_norm(x.get("date")), 0 if x.get("type")=="Macro" else 1, _norm(x.get("title"))))
    return out

def fetch_bls_events(start_date: date, end_date: date) -> List[Dict[str, Any]]:
    text = _strip_html(_fetch_text(BLS_SCHEDULE_URL))
    pat = re.compile(r"((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Z][a-z]+\s+\d{1,2},\s+2026)\s+(?:\d{2}:\d{2}\s+[AP]M\s+)?(Employment Situation|Consumer Price Index)\s+for\s+[A-Z][a-z]+\s+2026", re.I)
    mapped = {"Employment Situation":"US Non-Farm Payrolls","Consumer Price Index":"US CPI"}
    events = []
    for m in pat.finditer(text):
        try: d = datetime.strptime(m.group(1), "%A, %B %d, %Y").date()
        except Exception: continue
        if _date_in_range(d, start_date, end_date):
            title = mapped[m.group(2)]
            events.append({"date":d.isoformat(),"type":"Macro","title":title,"country":"US","importance":"high","markets":_markets_for_macro(title,"US"),"source":"BLS"})
    return events

def fetch_fomc_events(start_date: date, end_date: date) -> List[Dict[str, Any]]:
    text = _strip_html(_fetch_text(FED_FOMC_URL))
    m = re.search(r"2026 FOMC Meetings(.*?)(?:2027 FOMC Meetings|$)", text, re.I)
    if not m: return []
    block = m.group(1)
    pat = re.compile(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})-(\d{1,2})", re.I)
    events = []
    for mm in pat.finditer(block):
        d = date(2026, MONTH_NUM[mm.group(1).title()], int(mm.group(3)))
        if _date_in_range(d, start_date, end_date):
            events.append({"date":d.isoformat(),"type":"Macro","title":"FOMC Rate Decision","country":"US","importance":"high","markets":["Rates","FX","Equities"],"source":"Federal Reserve"})
    return events

def fetch_ecb_events(start_date: date, end_date: date) -> List[Dict[str, Any]]:
    text = _strip_html(_fetch_text(ECB_MGCGC_URL))
    pat = re.compile(r"(\d{2})/(\d{2})/2026\s+Governing Council of the ECB:\s+monetary policy meeting.*?(?:Day 2|press conference)", re.I)
    events = []
    for m in pat.finditer(text):
        d = date(2026, int(m.group(2)), int(m.group(1)))
        if _date_in_range(d, start_date, end_date):
            events.append({"date":d.isoformat(),"type":"Macro","title":"ECB Rate Decision","country":"EU","importance":"high","markets":["Rates","FX"],"source":"ECB"})
    return events

def _extract_nbs_month_days(block: str) -> List[int]:
    return [int(x) for x in re.findall(r"(\d{1,2})/(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)", block)[:12]]

def fetch_nbs_events(start_date: date, end_date: date) -> List[Dict[str, Any]]:
    text = _strip_html(_fetch_text(NBS_2026_CAL_URL))
    events = []
    pmi_match = re.search(r"Monthly Report on Purchasing Managers.? Index \(PMI\)(.*?)(?:Monthly Report on Consumer Price Index \(CPI\)|6\s+)", text, re.I)
    cpi_match = re.search(r"Monthly Report on Consumer Price Index \(CPI\)(.*?)(?:6\s+|Monthly Report on Producer Price Index|$)", text, re.I)
    if pmi_match:
        days = _extract_nbs_month_days(pmi_match.group(1))
        if len(days) >= 12:
            d = date(2026, start_date.month, days[start_date.month-1])
            if _date_in_range(d, start_date, end_date):
                events.append({"date":d.isoformat(),"type":"Macro","title":"China PMI","country":"China","importance":"high","markets":["Commodities","Equities","FX"],"source":"NBS China"})
    if cpi_match:
        days = _extract_nbs_month_days(cpi_match.group(1))
        if len(days) >= 12:
            d = date(2026, start_date.month, days[start_date.month-1])
            if _date_in_range(d, start_date, end_date):
                events.append({"date":d.isoformat(),"type":"Macro","title":"China CPI","country":"China","importance":"high","markets":["Commodities","Equities","FX"],"source":"NBS China"})
    return events

def fetch_fmp_earnings(start_date: date, end_date: date, api_key: Optional[str]=None) -> List[Dict[str, Any]]:
    api_key = api_key or os.getenv("FMP_API_KEY")
    if not api_key: return []
    r = requests.get(FMP_EARNINGS_URL, params={"from":start_date.isoformat(),"to":end_date.isoformat(),"apikey":api_key}, headers={"User-Agent":USER_AGENT}, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list): return []
    events = []
    for row in data:
        ticker = _norm(row.get("symbol"))
        if not ticker: continue
        try: market_cap_val = float(row.get("marketCap")) if row.get("marketCap") is not None else None
        except Exception: market_cap_val = None
        if not (ticker in PRIORITY_EARNINGS_TICKERS or (market_cap_val is not None and market_cap_val >= MIN_MARKET_CAP_USD)): continue
        try: d = datetime.strptime(_norm(row.get("date"))[:10], "%Y-%m-%d").date()
        except Exception: continue
        if not _date_in_range(d, start_date, end_date): continue
        exchange = _norm(row.get("exchangeShortName") or row.get("exchange")).lower()
        country = "US"
        if any(x in exchange for x in ["euronext","xetra","london","milan","paris","frankfurt","swiss","amsterdam"]): country = "EU"
        elif any(x in exchange for x in ["hong kong","shanghai","shenzhen","sse","szse"]): country = "China"
        events.append({"date":d.isoformat(),"type":"Earnings","title":f"{ticker} earnings","country":country,"importance":"high" if ticker in PRIORITY_EARNINGS_TICKERS else "medium","markets":["Equities"],"ticker":ticker,"company":_norm(row.get("companyName")) or ticker,"source":"FMP Earnings Calendar"})
    return events[:12]

def build_event_calendar(start_date: Optional[date]=None, end_date: Optional[date]=None) -> List[Dict[str, Any]]:
    start_date, end_date = start_date or _week_bounds()[0], end_date or _week_bounds()[1]
    events = []
    for fn in (fetch_bls_events, fetch_fomc_events, fetch_ecb_events, fetch_nbs_events):
        try: events.extend(fn(start_date, end_date))
        except Exception as e: print(f"⚠️ macro_calendar_engine: {fn.__name__} failed ({e})")
    try: events.extend(fetch_fmp_earnings(start_date, end_date))
    except Exception as e: print(f"⚠️ macro_calendar_engine: fetch_fmp_earnings failed ({e})")
    return _dedup(events)

if __name__ == "__main__":
    print(json.dumps(build_event_calendar(), ensure_ascii=False, indent=2))
