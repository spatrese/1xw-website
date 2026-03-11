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
        key = (
            ev.get("date"),
            ev.get("type"),
            ev.get("ticker") or ev.get("title"),
            ev.get("country"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(ev)
    return out


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


# ----------------------------
# Earnings: Finviz primary, FMP fallback
# ----------------------------
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
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        content = normalize_space(og_title["content"])
        content = re.sub(rf"^{re.escape(normalize_ticker(ticker))}\s*-\s*", "", content, flags=re.I)
        content = re.sub(r"\s*-\s*Stock.*$", "", content, flags=re.I)
        if content:
            return content

    title = soup.title.string if soup.title and soup.title.string else ""
    title = normalize_space(title)
    title = re.sub(rf"^{re.escape(normalize_ticker(ticker))}\s*-\s*", "", title, flags=re.I)
    title = re.sub(r"\s*-\s*Stock.*$", "", title, flags=re.I)
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
