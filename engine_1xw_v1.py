from ib_insync import *
import pandas as pd
import argparse
from datetime import datetime, timezone
import os
import json

UNIVERSE_CSV = "universe_1xw.csv"
SCREENER_JSON = "content/site_screener.json"
MAX_IDEAS_TOTAL = 6
MIN_ABS_SCORE = 3


def iso_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def week_key_local():
    today = datetime.now()
    y, w, _ = today.isocalendar()
    return f"{y}-W{int(w):02d}"


def safe_str(x):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def load_json_if_exists(path, fallback):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return fallback


def upsert_week_batch_tradeideas(existing_tradeideas, new_tradeideas, wk):
    old = [x for x in (existing_tradeideas or []) if str(x.get("week", "")) != wk]
    for x in new_tradeideas:
        x["week"] = wk
    return old + new_tradeideas


def get_contract(symbol, contract_type, exchange, currency):
    ct = safe_str(contract_type).upper()
    sym = safe_str(symbol)
    exch = safe_str(exchange)
    ccy = safe_str(currency)

    if ct == "FUT":
        return ContFuture(symbol=sym, exchange=exch, currency=ccy)
    if ct == "CASH":
        return Contract(secType="CASH", symbol=sym, exchange=exch, currency=ccy)
    if ct == "IND":
        return Index(symbol=sym, exchange=exch, currency=ccy)
    return Contract(secType=ct, symbol=sym, exchange=exch, currency=ccy)


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
    except Exception:
        return None

    ct = safe_str(contract_type).upper()
    sym = safe_str(symbol).upper()

    if ct == "CASH":
        return req_hist(ib, contract, "5 Y", "1 day", "MIDPOINT", False)
    if ct == "IND" and sym == "VIX":
        df = req_hist(ib, contract, "5 Y", "1 day", "TRADES", False)
        if df is not None:
            return df
        return req_hist(ib, contract, "5 Y", "1 day", "MIDPOINT", False)
    if ct == "IND":
        df = req_hist(ib, contract, "5 Y", "1 day", "TRADES", False)
        if df is not None:
            return df
        return req_hist(ib, contract, "5 Y", "1 day", "MIDPOINT", False)
    return req_hist(ib, contract, "5 Y", "1 day", "TRADES", True)


def add_indicators(df):
    df = df.copy()
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()
    df["ret_20d"] = df["close"].pct_change(20)
    df["ret_60d"] = df["close"].pct_change(60)
    df["ret_5d"] = df["close"].pct_change(5)
    df["hh_20"] = df["high"].rolling(20).max()
    df["ll_20"] = df["low"].rolling(20).min()
    return df


def classify_setup(row):
    if pd.isna(row["ma50"]) or pd.isna(row["hh_20"]) or pd.isna(row["ll_20"]):
        return "Neutral"

    close = row["close"]
    ma50 = row["ma50"]
    ma200 = row["ma200"]
    hh20 = row["hh_20"]
    ll20 = row["ll_20"]
    r20 = row["ret_20d"]

    if close >= hh20 and close > ma50:
        return "Breakout"
    if close <= ll20 and close < ma50:
        return "Breakdown"
    if close > ma50 and (not pd.isna(ma200) and close > ma200) and r20 > 0:
        return "Trend continuation"
    if close < ma50 and (not pd.isna(ma200) and close < ma200) and r20 < 0:
        return "Trend continuation (down)"
    if close < ma50 and r20 < 0 and (not pd.isna(row["ret_5d"]) and row["ret_5d"] > 0):
        return "Mean reversion (bounce)"
    if close > ma50 and r20 > 0 and (not pd.isna(row["ret_5d"]) and row["ret_5d"] < 0):
        return "Mean reversion (pullback)"
    return "Neutral"


def compute_score(row):
    score = 0
    if not pd.isna(row["ma50"]):
        score += 1 if row["close"] > row["ma50"] else -1
    if not pd.isna(row["ma200"]):
        score += 1 if row["close"] > row["ma200"] else -1
    if not pd.isna(row["ret_20d"]):
        score += 1 if row["ret_20d"] > 0 else -1
    if not pd.isna(row["ret_60d"]):
        score += 1 if row["ret_60d"] > 0 else -1
    return int(score)


def build_tradeideas(df_ranked):
    today_str = datetime.now().strftime("%Y-%m-%d")
    ideas = []
    for r in df_ranked.itertuples(index=False):
        score = int(r.score)
        direction = "Long" if score > 0 else "Short"
        ideas.append({
            "id": f"IB-{direction.upper()}-{safe_str(r.symbol)}",
            "date": today_str,
            "asset": safe_str(r.symbol),
            "direction": direction,
            "status": "Open",
            "setup": safe_str(r.setup),
            "thesis": f"Score {score} (trend + momentum).",
            "structure": "Signal (IB screener)",
            "horizon": "2-8w",
            "confidence": 3,
            "tags": [safe_str(r.asset_class), "screener"],
            "notes": ""
        })
    return ideas


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--clientId", type=int, default=1)
    args = parser.parse_args()

    if not os.path.exists(UNIVERSE_CSV):
        raise FileNotFoundError(f"Missing universe file: {UNIVERSE_CSV}")

    universe = pd.read_csv(UNIVERSE_CSV)
    print(f"Connecting to IB on 127.0.0.1:{args.port} (clientId={args.clientId}) ...")
    ib = IB()
    ib.connect("127.0.0.1", args.port, clientId=args.clientId)
    ib.reqMarketDataType(3)

    rows = []
    for _, r in universe.iterrows():
        symbol = safe_str(r.get("symbol"))
        if not symbol:
            continue

        asset_class = safe_str(r.get("asset_class", ""))
        contract_type = safe_str(r.get("contract_type", "")).upper()
        exchange = safe_str(r.get("exchange", ""))
        currency = safe_str(r.get("currency", ""))
        name = safe_str(r.get("name", symbol))
        if not contract_type:
            continue

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
            "score": int(score),
            "setup": setup,
            "close": float(last["close"]),
            "ret_20d_pct": round(float(last["ret_20d"]) * 100.0, 2) if not pd.isna(last["ret_20d"]) else None,
            "ret_60d_pct": round(float(last["ret_60d"]) * 100.0, 2) if not pd.isna(last["ret_60d"]) else None,
        })
        print(f"✅ {symbol} ok")

    ib.disconnect()
    if not rows:
        print("No data collected.")
        return

    df_s = pd.DataFrame(rows)
    df_f = df_s[df_s["score"].abs() >= MIN_ABS_SCORE].copy()
    if len(df_f) == 0:
        df_ranked = df_f
    else:
        df_f["abs_score"] = df_f["score"].abs()
        df_ranked = df_f.sort_values(["abs_score", "score"], ascending=[False, False]).head(MAX_IDEAS_TOTAL)

    tradeIdeas_new = build_tradeideas(df_ranked)
    os.makedirs("content", exist_ok=True)
    updatedAt = iso_now()
    wk = week_key_local()

    existing = load_json_if_exists(
        SCREENER_JSON,
        fallback={"updatedAt": updatedAt, "tradeIdeas": [], "modelTrades": [], "openPositions": [], "universe": []}
    )

    modelTrades = existing.get("modelTrades", [])
    openPositions = existing.get("openPositions", [])
    tradeIdeas_existing = existing.get("tradeIdeas", [])
    tradeIdeas_merged = upsert_week_batch_tradeideas(tradeIdeas_existing, tradeIdeas_new, wk)

    out = {
        "updatedAt": updatedAt,
        "tradeIdeas": tradeIdeas_merged,
        "modelTrades": modelTrades,
        "openPositions": openPositions,
        "universe": rows,
        "by_symbol": {r["symbol"]: r for r in rows}
    }

    with open(SCREENER_JSON, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote: {SCREENER_JSON} (week={wk}, universe={len(rows)}, newIdeas={len(tradeIdeas_new)})")


if __name__ == "__main__":
    main()
