from ib_insync import *
import pandas as pd
import numpy as np
import argparse
from datetime import datetime
import os

def get_contract(row):
    ctype = row["contract_type"]
    if ctype == "FUT":
        return ContFuture(symbol=row["symbol"], exchange=row["exchange"], currency=row["currency"])
    elif ctype == "CASH":
        return Contract(secType="CASH", symbol=row["symbol"], exchange=row["exchange"], currency=row["currency"])
    elif ctype == "IND":
        return Index(symbol=row["symbol"], exchange=row["exchange"], currency=row["currency"])
    else:
        return Contract(secType=ctype, symbol=row["symbol"], exchange=row["exchange"], currency=row["currency"])

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

    # FX spot
    if contract_type == "CASH":
        return req_hist(ib, contract, "2 Y", "1 day", "MIDPOINT", False)

    # VIX Index special-case (IB rejects whatToShow=INDEX)
    if contract_type == "IND" and symbol.upper() == "VIX":
        df = req_hist(ib, contract, "2 Y", "1 day", "TRADES", False)
        if df is not None:
            return df
        return req_hist(ib, contract, "2 Y", "1 day", "MIDPOINT", False)

    # Other indices: try TRADES (most compatible)
    if contract_type == "IND":
        df = req_hist(ib, contract, "2 Y", "1 day", "TRADES", False)
        if df is not None:
            return df
        return req_hist(ib, contract, "2 Y", "1 day", "MIDPOINT", False)

    # Futures / rest
    return req_hist(ib, contract, "2 Y", "1 day", "TRADES", True)

def analyze(df):
    if df is None or len(df) < 50:
        return None

    df["ret_20"] = df["close"].pct_change(20)
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()

    last = df.iloc[-1]

    return {
        "last": round(float(last["close"]), 4),
        "ret_20": round(float(last["ret_20"]) * 100, 2) if not np.isnan(last["ret_20"]) else None,
        "above_ma50": bool(last["close"] > last["ma50"]),
        "above_ma200": bool(last["close"] > last["ma200"])
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    print(f"Connecting to IB on 127.0.0.1:{args.port} ...")
    ib = IB()
    ib.connect("127.0.0.1", args.port, clientId=1)
    ib.reqMarketDataType(3)

    universe = pd.read_csv("universe_1xw.csv")
    results = []

    for _, row in universe.iterrows():
        sym = str(row["symbol"]).strip()
        ctype = str(row["contract_type"]).strip().upper()

        contract = get_contract({
            "symbol": sym,
            "exchange": str(row["exchange"]).strip(),
            "currency": str(row["currency"]).strip(),
            "contract_type": ctype
        })

        df = fetch_data(ib, contract, ctype, sym)
        if df is None:
            print(f"⚠️ {sym}: no/low data")
            continue

        stats = analyze(df)
        if stats:
            print(f"✅ {sym} ok")
            stats["symbol"] = sym
            results.append(stats)
        else:
            print(f"⚠️ {sym}: insufficient history")

    ib.disconnect()

    if not results:
        print("No data collected.")
        return

    out = pd.DataFrame(results)
    os.makedirs("outputs", exist_ok=True)
    fname = f"outputs/weekly_report_{datetime.today().strftime('%Y%m%d')}.xlsx"
    out.to_excel(fname, index=False)
    print(f"✅ Wrote: {fname}")

if __name__ == "__main__":
    main()