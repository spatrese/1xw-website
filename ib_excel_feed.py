#!/usr/bin/env python
"""
IB / Excel Live Feed — v3 (patched for Excel floats like 202606.0)

- Reads contracts from sheet "Symbols"
- Writes live prices (+ option greeks if available) to sheet "Prices"
- Logs to sheet "Log"
- Supports: FUT, FOP (futures options), STK, IND, OPT, CRYPTO (PAXOS)
- Fixes Excel float artifacts: 202606.0, 25.0, etc.

Requirements:
  pip install ib_insync xlwings openpyxl

Usage:
  python ib_excel_feed.py --file "C:\\path\\to\\YourMaster.xlsx"

TWS:
  Configure > API > Settings:
   - Enable ActiveX and Socket Clients
   - Socket port: 7497
"""

import argparse
import os
import time
import math
import datetime as dt
from typing import Dict, Any, List, Optional

import xlwings as xw
from ib_insync import (
    IB, Contract,
    Future, FuturesOption,
    Stock, Index, Option,
    Crypto
)

PRICES_HEADERS = [
    "name", "conId", "secType", "symbol", "localSymbol", "exchange", "currency",
    "last", "bid", "ask", "mid", "close", "open", "high", "low", "volume",
    "impliedVol", "delta", "gamma", "vega", "theta",
    "timestamp", "status"
]


# -----------------------
# Helpers (Excel sanitation)
# -----------------------
def now_ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_num(x):
    """Convert to float if possible, else None."""
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            if isinstance(x, float) and math.isnan(x):
                return None
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def clean_int_str(x) -> str:
    """
    Convert Excel numbers like 202606.0 or "202606.0" to "202606".
    Keeps non-numeric strings as-is (trimmed).
    """
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    # common Excel artifact
    if s.endswith(".0"):
        s = s[:-2]
    # if it's numeric, force int
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return s


def clean_float_str(x) -> str:
    """
    Convert Excel numbers like 25.0 to "25" (string),
    keep decimals if actually fractional.
    """
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        return str(f)
    except Exception:
        return s


def clean_right(x) -> str:
    s = str(x or "").strip().upper()
    if s in ("C", "CALL"):
        return "C"
    if s in ("P", "PUT"):
        return "P"
    return s


def ensure_sheet(wb: xw.Book, name: str) -> xw.Sheet:
    try:
        return wb.sheets[name]
    except Exception:
        wb.sheets.add(name)
        return wb.sheets[name]


def ensure_prices_header(ws_prices: xw.Sheet):
    hdr = ws_prices.range("A1").expand("right").value
    if hdr != PRICES_HEADERS:
        ws_prices.range("A1").value = PRICES_HEADERS


def ensure_log_header(ws_log: xw.Sheet):
    hdr = ws_log.range("A1").expand("right").value
    if hdr != ["timestamp", "level", "message"]:
        ws_log.range("A1").value = ["timestamp", "level", "message"]


def log(ws_log: xw.Sheet, level: str, msg: str):
    last = ws_log.range("A" + str(ws_log.cells.last_cell.row)).end("up").row
    row = 2 if last < 2 else last + 1
    ws_log.range(f"A{row}").value = [now_ts(), level, msg]


# -----------------------
# Config
# -----------------------
def read_config(wb: xw.Book) -> Dict[str, Any]:
    ws = ensure_sheet(wb, "Config")

    def cell(addr, default=None):
        v = ws.range(addr).value
        return default if v is None or str(v).strip() == "" else v

    host = str(cell("B3", "127.0.0.1")).strip()
    port = int(float(cell("B4", 7497)))
    client_id = int(float(cell("B5", 11)))
    interval = float(cell("B6", 0.25))
    mode = str(cell("B7", "STREAM")).strip().upper()

    raw = cell("B8", 1)
    s = str(raw).strip().upper()
    mapping = {
        "1": 1, "LIVE": 1,
        "2": 2, "FROZEN": 2,
        "3": 3, "DELAYED": 3,
        "4": 4, "DELAYED_FROZEN": 4, "DELAYED-FROZEN": 4
    }
    mkt = mapping.get(s)
    if mkt is None:
        mkt = int(float(raw))

    if mode not in ("STREAM", "SNAPSHOT"):
        mode = "STREAM"

    return {
        "host": host,
        "port": port,
        "clientId": client_id,
        "interval": interval,
        "mode": mode,
        "mktDataType": mkt
    }


# -----------------------
# Symbols sheet parsing
# -----------------------
def read_symbols(wb: xw.Book) -> List[Dict[str, Any]]:
    ws = ensure_sheet(wb, "Symbols")
    rng = ws.range("A1").expand("table")
    values = rng.value
    if not values or len(values) < 2:
        return []

    headers = [str(h).strip() for h in values[0]]
    rows: List[Dict[str, Any]] = []
    for row in values[1:]:
        if row is None:
            continue
        d = {headers[i]: (row[i] if i < len(row) else None) for i in range(len(headers))}
        if all((d.get(h) is None or str(d.get(h)).strip() == "") for h in headers):
            continue
        rows.append(d)
    return rows


def contract_from_row(r: Dict[str, Any]) -> Optional[Contract]:
    # enabled must be 1
    enabled = r.get("enabled", 0)
    try:
        if int(float(enabled)) != 1:
            return None
    except Exception:
        return None

    secType = str(r.get("secType", "") or "").strip().upper()
    symbol = str(r.get("symbol", "") or "").strip()
    exchange = str(r.get("exchange", "") or "").strip()
    currency = str(r.get("currency", "") or "").strip() or "USD"

    # clean the Excel artifacts
    lastTrade = clean_int_str(r.get("lastTradeDateOrContractMonth"))
    localSymbol = str(r.get("localSymbol", "") or "").strip()
    tradingClass = str(r.get("tradingClass", "") or "").strip()
    primaryExchange = str(r.get("primaryExchange", "") or "").strip()

    # multiplier: DO NOT pass to futures (IB resolves); keep only for some options if needed
    mult = clean_float_str(r.get("multiplier"))

    if not secType or not symbol or not exchange:
        return None

    # FUT
    if secType == "FUT":
        c = Future(symbol=symbol, lastTradeDateOrContractMonth=lastTrade, exchange=exchange, currency=currency)
        if localSymbol:
            c.localSymbol = localSymbol
        if tradingClass:
            c.tradingClass = tradingClass
        # IMPORTANT: do NOT set c.multiplier (avoids field #541 issues)
        return c

    # Futures options
    if secType in ("FOP", "FUTOPT", "FUTUREOPTION"):
        strike = safe_num(r.get("strike"))
        right = clean_right(r.get("right"))
        if strike is None or right not in ("C", "P") or not lastTrade:
            return None
        c = FuturesOption(
            symbol=symbol,
            lastTradeDateOrContractMonth=lastTrade,
            strike=float(strike),
            right=right,
            exchange=exchange,
            currency=currency
        )
        if tradingClass:
            c.tradingClass = tradingClass
        if localSymbol:
            c.localSymbol = localSymbol
        # optional: multiplier is usually resolved; set only if you really need it
        if mult:
            c.multiplier = mult
        return c

    # Stocks
    if secType == "STK":
        c = Stock(symbol=symbol, exchange=exchange, currency=currency)
        if primaryExchange:
            c.primaryExchange = primaryExchange
        return c

    # Indices
    if secType == "IND":
        return Index(symbol=symbol, exchange=exchange, currency=currency)

    # Equity options
    if secType == "OPT":
        strike = safe_num(r.get("strike"))
        right = clean_right(r.get("right"))
        if strike is None or right not in ("C", "P") or not lastTrade:
            return None
        c = Option(
            symbol=symbol,
            lastTradeDateOrContractMonth=lastTrade,
            strike=float(strike),
            right=right,
            exchange=exchange,
            currency=currency
        )
        if localSymbol:
            c.localSymbol = localSymbol
        if tradingClass:
            c.tradingClass = tradingClass
        if mult:
            c.multiplier = mult
        return c

    # Crypto (IBKR: PAXOS)
    if secType == "CRYPTO":
        return Crypto(symbol=symbol, exchange=exchange, currency=currency)

    return None


# -----------------------
# Write Prices
# -----------------------
def write_prices(ws_prices: xw.Sheet, data_rows: List[List[Any]]):
    start_row = 2
    if not data_rows:
        ws_prices.range(f"A{start_row}:W{start_row+300}").clear_contents()
        return
    n = len(data_rows)
    ws_prices.range(f"A{start_row}:W{start_row+n-1}").value = data_rows
    ws_prices.range(f"A{start_row+n}:W{start_row+n+200}").clear_contents()


# -----------------------
# Main
# -----------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Excel file path (master workbook)")
    args = ap.parse_args()

    xlsx_path = os.path.abspath(os.path.expanduser(args.file))

    app = xw.App(visible=True, add_book=False)
    app.display_alerts = False
    app.screen_updating = True

    wb = app.books.open(xlsx_path)

    ws_prices = ensure_sheet(wb, "Prices")
    ws_log = ensure_sheet(wb, "Log")
    ensure_prices_header(ws_prices)
    ensure_log_header(ws_log)

    cfg = read_config(wb)
    log(ws_log, "INFO", f"Starting. host={cfg['host']} port={cfg['port']} clientId={cfg['clientId']} mktDataType={cfg['mktDataType']} interval={cfg['interval']}s")

    ib = IB()

    def connect():
        ib.connect(cfg["host"], cfg["port"], clientId=cfg["clientId"])
        ib.reqMarketDataType(cfg["mktDataType"])

    try:
        connect()
        log(ws_log, "INFO", "Connected to TWS.")
    except Exception as e:
        log(ws_log, "ERROR", f"Failed to connect to TWS: {e}")
        raise

    # Build contracts
    symbol_rows = read_symbols(wb)
    contracts: List[Contract] = []
    names: List[str] = []

    for r in symbol_rows:
        c = contract_from_row(r)
        if c is None:
            continue
        nm = str(r.get("name", "") or "").strip() or f"{r.get('secType','')}:{r.get('symbol','')}"
        contracts.append(c)
        names.append(nm)

    if not contracts:
        log(ws_log, "WARN", "No enabled contracts found in Symbols sheet.")
    else:
        log(ws_log, "INFO", f"Loaded {len(contracts)} contracts from Symbols sheet.")

    # Qualify + subscribe
    tickers = []
    kept_names = []

    for nm, c in zip(names, contracts):
        try:
            # qualify resolves conId/localSymbol etc.
            ib.qualifyContracts(c)
            t = ib.reqMktData(c, "", False, False)
            tickers.append(t)
            kept_names.append(nm)
        except Exception as e:
            log(ws_log, "ERROR", f"Subscribe failed for {nm}: {e}")

    names = kept_names

    # Main loop
    while True:
        try:
            ts = now_ts()
            out = []

            for name, t in zip(names, tickers):
                c = t.contract

                last = safe_num(getattr(t, "last", None))
                bid = safe_num(getattr(t, "bid", None))
                ask = safe_num(getattr(t, "ask", None))
                mid = (bid + ask) / 2.0 if (bid is not None and ask is not None) else None

                close = safe_num(getattr(t, "close", None))
                opn = safe_num(getattr(t, "open", None))
                high = safe_num(getattr(t, "high", None))
                low = safe_num(getattr(t, "low", None))
                vol = safe_num(getattr(t, "volume", None))

                iv = delta = gamma = vega = theta = None
                gm = getattr(t, "modelGreeks", None)
                if gm:
                    iv = safe_num(getattr(gm, "impliedVol", None))
                    delta = safe_num(getattr(gm, "delta", None))
                    gamma = safe_num(getattr(gm, "gamma", None))
                    vega = safe_num(getattr(gm, "vega", None))
                    theta = safe_num(getattr(gm, "theta", None))

                status = "OK"
                if last is None and bid is None and ask is None:
                    status = "NO_DATA"

                out.append([
                    name,
                    getattr(c, "conId", None),
                    getattr(c, "secType", None),
                    getattr(c, "symbol", None),
                    getattr(c, "localSymbol", None),
                    getattr(c, "exchange", None),
                    getattr(c, "currency", None),
                    last, bid, ask, mid, close, opn, high, low, vol,
                    iv, delta, gamma, vega, theta,
                    ts, status
                ])

            write_prices(ws_prices, out)
            ib.sleep(cfg["interval"])

        except KeyboardInterrupt:
            log(ws_log, "INFO", "Stopped by user (Ctrl+C).")
            break
        except Exception as e:
            log(ws_log, "ERROR", f"Loop error: {e}. Reconnecting in 2s.")
            try:
                ib.disconnect()
            except Exception:
                pass
            time.sleep(2)
            try:
                connect()
                log(ws_log, "INFO", "Reconnected to TWS.")
            except Exception as e2:
                log(ws_log, "ERROR", f"Reconnect failed: {e2}")
                time.sleep(3)

    # cleanup
    try:
        ib.disconnect()
    except Exception:
        pass

    try:
        wb.save()
        wb.close()
    except Exception:
        pass

    try:
        app.quit()
    except Exception:
        pass


if __name__ == "__main__":
    main()