from ib_insync import *
import pandas as pd
import numpy as np
import argparse
from datetime import datetime
import os
import json

# ----------------------------
# Contracts
# ----------------------------
def get_contract(symbol, contract_type, exchange, currency):
    contract_type = str(contract_type).strip().upper()
    symbol = str(symbol).strip()
    exchange = str(exchange).strip()
    currency = str(currency).strip()

    if contract_type == "FUT":
        return ContFuture(symbol=symbol, exchange=exchange, currency=currency)
    if contract_type == "CASH":
        return Contract(secType="CASH", symbol=symbol, exchange=exchange, currency=currency)
    if contract_type == "IND":
        return Index(symbol=symbol, exchange=exchange, currency=currency)

    # fallback
    return Contract(secType=contract_type, symbol=symbol, exchange=exchange, currency=currency)

def req_hist(ib, contract, duration, bar, what, useRTH):
    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr=duration,
        barSizeSetting=bar,
        whatToShow=what,
        useRTH=useRTH,
        formatDate=1
    )
    if not bars:
        return None
    return util.df(bars)

def fetch_data(ib, contract, contract_type, symbol):
    try:
        ib.qualifyContracts(contract)
    except:
        return None

    ct = str(contract_type).strip().upper()
    sym = str(symbol).strip().upper()

    # FX spot
    if ct == "CASH":
        return req_hist(ib, contract, "5 Y", "1 day", "MIDPOINT", False)

    # VIX Index special-case (IB rejects whatToShow=INDEX for you)
    if ct == "IND" and sym == "VIX":
        df = req_hist(ib, contract, "5 Y", "1 day", "TRADES", False)
        if df is not None:
            return df
        return req_hist(ib, contract, "5 Y", "1 day", "MIDPOINT", False)

    # Other indices: try TRADES then MIDPOINT
    if ct == "IND":
        df = req_hist(ib, contract, "5 Y", "1 day", "TRADES", False)
        if df is not None:
            return df
        return req_hist(ib, contract, "5 Y", "1 day", "MIDPOINT", False)

    # Futures / rest
    return req_hist(ib, contract, "5 Y", "1 day", "TRADES", True)

# ----------------------------
# Analytics (Trend + Momentum only)
# ----------------------------
def add_indicators(df):
    # expects df with columns: date, open, high, low, close, volume
    df = df.copy()
    df["ma50"]  = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()
    df["ret_20d"] = df["close"].pct_change(20)
    df["ret_60d"] = df["close"].pct_change(60)
    df["ret_5d"]  = df["close"].pct_change(5)
    df["hh_20"] = df["high"].rolling(20).max()
    df["ll_20"] = df["low"].rolling(20).min()
    return df

def classify_setup(row):
    # lightweight classification (optional, but useful for site)
    if pd.isna(row["ma50"]) or pd.isna(row["hh_20"]) or pd.isna(row["ll_20"]):
        return "Neutral"

    close = row["close"]
    ma50 = row["ma50"]
    ma200 = row["ma200"]
    hh20 = row["hh_20"]
    ll20 = row["ll_20"]
    r20 = row["ret_20d"]
    r60 = row["ret_60d"]

    # Breakout/Breakdown (simple)
    if close >= hh20 and close > ma50:
        return "Breakout"
    if close <= ll20 and close < ma50:
        return "Breakdown"

    # Trend continuation
    if close > ma50 and (not pd.isna(ma200) and close > ma200) and r20 > 0:
        return "Trend continuation"
    if close < ma50 and (not pd.isna(ma200) and close < ma200) and r20 < 0:
        return "Trend continuation (down)"

    # Mean reversion hint
    if close < ma50 and r20 < 0 and (not pd.isna(row["ret_5d"]) and row["ret_5d"] > 0):
        return "Mean reversion (bounce)"
    if close > ma50 and r20 > 0 and (not pd.isna(row["ret_5d"]) and row["ret_5d"] < 0):
        return "Mean reversion (pullback)"

    return "Neutral"

def compute_score(row):
    # A: trend + momentum (no vol filter)
    # Score in [-4, +4]
    score = 0

    # trend
    if not pd.isna(row["ma50"]):
        score += 1 if row["close"] > row["ma50"] else -1
    if not pd.isna(row["ma200"]):
        score += 1 if row["close"] > row["ma200"] else -1

    # momentum
    if not pd.isna(row["ret_20d"]):
        score += 1 if row["ret_20d"] > 0 else -1
    if not pd.isna(row["ret_60d"]):
        score += 1 if row["ret_60d"] > 0 else -1

    return int(score)

def make_assetclass_commentary(df_screener):
    # very compact auto-summary per asset class
    out = {}
    for ac, g in df_screener.groupby("asset_class"):
        n = len(g)
        if n == 0:
            continue
        bulls = int((g["score"] >= 2).sum())
        bears = int((g["score"] <= -2).sum())
        avg = float(g["score"].mean())

        if avg >= 1.0:
            tone = "Bullish breadth"
        elif avg <= -1.0:
            tone = "Bearish breadth"
        else:
            tone = "Mixed / range"

        out[ac] = f"{tone}. Avg score {avg:.2f}. Bullish(>=2): {bulls}/{n}, Bearish(<=-2): {bears}/{n}."
    return out

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    print(f"Connecting to IB on 127.0.0.1:{args.port} ...")
    ib = IB()
    ib.connect("127.0.0.1", args.port, clientId=1)
    ib.reqMarketDataType(3)

    universe = pd.read_csv("universe_1xw.csv")

    rows = []
    for _, r in universe.iterrows():
        symbol = str(r["symbol"]).strip()
        name = str(r.get("name", "")).strip()
        asset_class = str(r.get("asset_class", "")).strip()
        contract_type = str(r.get("contract_type", "")).strip().upper()
        exchange = str(r.get("exchange", "")).strip()
        currency = str(r.get("currency", "")).strip()

        c = get_contract(symbol, contract_type, exchange, currency)
        df = fetch_data(ib, c, contract_type, symbol)

        if df is None or len(df) < 220:
            print(f"⚠️ {symbol}: no/low data")
            continue

        df = add_indicators(df)
        last = df.iloc[-1]

        score = compute_score(last)
        setup = classify_setup(last)

        rows.append({
            "symbol": symbol,
            "name": name,
            "asset_class": asset_class,
            "contract_type": contract_type,
            "exchange": exchange,
            "currency": currency,
            "date": str(last["date"]) if "date" in df.columns else "",
            "close": float(last["close"]),
            "ret_20d_%": None if pd.isna(last["ret_20d"]) else float(last["ret_20d"] * 100.0),
            "ret_60d_%": None if pd.isna(last["ret_60d"]) else float(last["ret_60d"] * 100.0),
            "ma50": None if pd.isna(last["ma50"]) else float(last["ma50"]),
            "ma200": None if pd.isna(last["ma200"]) else float(last["ma200"]),
            "score": int(score),
            "setup": setup,
        })
        print(f"✅ {symbol} ok")

    ib.disconnect()

    if not rows:
        print("No data collected.")
        return

    df_s = pd.DataFrame(rows).sort_values(["asset_class", "score"], ascending=[True, False])

    # Top lists
    top_long = df_s.sort_values("score", ascending=False).head(10)
    top_short = df_s.sort_values("score", ascending=True).head(10)

    # Output folders
    os.makedirs("outputs", exist_ok=True)
    os.makedirs("content", exist_ok=True)

    today = datetime.today().strftime("%Y%m%d")

    # Excel
    xlsx_path = f"outputs/weekly_report_{today}.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df_s.to_excel(writer, sheet_name="Screener", index=False)
        top_long.to_excel(writer, sheet_name="TopLong", index=False)
        top_short.to_excel(writer, sheet_name="TopShort", index=False)

    # JSON for site (Trade ideas)
    trade_json = {
        "asof": today,
        "top_long": [
            {"symbol": r.symbol, "asset_class": r.asset_class, "score": int(r.score), "setup": r.setup}
            for r in top_long.itertuples(index=False)
        ],
        "top_short": [
            {"symbol": r.symbol, "asset_class": r.asset_class, "score": int(r.score), "setup": r.setup}
            for r in top_short.itertuples(index=False)
        ]
    }
    with open("content/site_screener.json", "w", encoding="utf-8") as f:
        json.dump(trade_json, f, ensure_ascii=False, indent=2)

    # JSON for site (Market commentary)
    comm = make_assetclass_commentary(df_s)
    comm_json = {"asof": today, "by_asset_class": comm}
    with open("content/site_commentary.json", "w", encoding="utf-8") as f:
        json.dump(comm_json, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote: {xlsx_path}")
    print("✅ Wrote: content/site_screener.json")
    print("✅ Wrote: content/site_commentary.json")

if __name__ == "__main__":
    main()