import requests
import datetime as dt
import re
import os

# -----------------------------
# CONFIG
# -----------------------------

INVESTING_URL = "https://www.investing.com/economic-calendar/"

ALPHAVANTAGE_KEY = (
    os.getenv("ALPHAVANTAGE_API_KEY")
    or os.getenv("ALPHA_VANTAGE_API_KEY")
)

EARNINGS_URL = "https://www.alphavantage.co/query"

MAX_MACRO = 12
MAX_EARNINGS = 12


MACRO_WHITELIST = [
    "CPI",
    "Nonfarm",
    "FOMC",
    "ECB",
    "GDP",
    "Retail Sales",
    "PPI",
    "ISM",
    "Michigan",
    "Industrial Production",
    "Housing Starts",
    "Consumer Confidence",
    "China CPI",
    "China GDP"
]


CORE_EARNINGS = {
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA",
    "JPM","GS","MS","TSM","ASML","XOM","CVX"
}


# -----------------------------
# MACRO
# -----------------------------

def fetch_macro_calendar():

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(INVESTING_URL, headers=headers)
    html = r.text

    events = []

    rows = re.findall(r'data-event-datetime="([^"]+)"(.*?)</tr>', html, re.S)

    for date_str, row in rows:

        title_match = re.search(r'title="([^"]+)"', row)
        if not title_match:
            continue

        title = title_match.group(1)

        if not any(k.lower() in title.lower() for k in MACRO_WHITELIST):
            continue

        date = date_str.split(" ")[0]

        events.append({
            "date": date,
            "type": "Macro",
            "title": title,
            "markets": ["Macro"],
            "source": "Investing.com"
        })

    return events


# -----------------------------
# EARNINGS
# -----------------------------

def fetch_earnings():

    if not ALPHAVANTAGE_KEY:
        return []

    params = {
        "function": "EARNINGS_CALENDAR",
        "horizon": "3month",
        "apikey": ALPHAVANTAGE_KEY
    }

    r = requests.get(EARNINGS_URL, params=params)
    data = r.json()

    events = []

    for e in data:

        ticker = e.get("symbol")

        if not ticker:
            continue

        if ticker not in CORE_EARNINGS:
            continue

        events.append({
            "date": e.get("reportDate"),
            "type": "Earnings",
            "title": ticker,
            "markets": ["Equities"],
            "source": "Alpha Vantage"
        })

    return events


# -----------------------------
# BUILD CALENDAR
# -----------------------------

def build_calendar():

    macro = fetch_macro_calendar()
    earnings = fetch_earnings()

    events = macro + earnings

    seen = set()
    clean = []

    for e in events:

        key = (e["date"], e["type"], e["title"])

        if key in seen:
            continue

        seen.add(key)
        clean.append(e)

    clean.sort(key=lambda x: (x["date"], x["type"], x["title"]))

    macro_events = [e for e in clean if e["type"] == "Macro"][:MAX_MACRO]
    earnings_events = [e for e in clean if e["type"] == "Earnings"][:MAX_EARNINGS]

    return macro_events + earnings_events


# -----------------------------
# MAIN
# -----------------------------

if __name__ == "__main__":

    calendar = build_calendar()

    for e in calendar:
        print(e)