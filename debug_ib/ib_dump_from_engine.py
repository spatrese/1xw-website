from ib_insync import *
import pandas as pd
import os

ib = IB()
ib.connect("127.0.0.1", 7497, clientId=99)

universe = pd.read_csv("universe_1xw.csv")
os.makedirs("debug_ib", exist_ok=True)

for _, row in universe.iterrows():
    symbol = row["symbol"]
    contract_type = str(row["contract_type"]).strip()
    exchange = str(row["exchange"]).strip()
    currency = str(row["currency"]).strip()

    print("Downloading:", symbol)

    try:
        if contract_type == "FUT":
            contract = ContFuture(symbol=symbol, exchange=exchange, currency=currency)
            what_to_show = "TRADES"

        elif contract_type == "IND":
            contract = Index(symbol=symbol, exchange=exchange, currency=currency)
            what_to_show = "TRADES"

        elif contract_type == "CASH":
            contract = Forex(symbol)
            what_to_show = "MIDPOINT"

        elif contract_type == "CRYPTO":
            contract = Crypto(symbol=symbol, exchange=exchange, currency=currency)
            what_to_show = "TRADES"

        elif contract_type == "STK":
            contract = Stock(symbol=symbol, exchange=exchange, currency=currency)
            what_to_show = "TRADES"

        else:
            print("Skipping unsupported contract_type:", symbol, contract_type)
            continue

        qualified = ib.qualifyContracts(contract)
        if not qualified:
            print("❌ Contract not qualified:", symbol)
            continue

        contract = qualified[0]

        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="5 Y",
            barSizeSetting="1 day",
            whatToShow=what_to_show,
            useRTH=True,
            formatDate=1
        )

        if not bars:
            print("❌ No data:", symbol)
            continue

        df = util.df(bars)
        if df.empty:
            print("❌ Empty dataframe:", symbol)
            continue

        df["date"] = pd.to_datetime(df["date"])

        keep = ["date", "open", "high", "low", "close", "volume"]
        keep = [c for c in keep if c in df.columns]
        df = df[keep]

        out = f"debug_ib/{symbol}.csv"
        df.to_csv(out, index=False)
        print("✅ Saved:", out)

    except Exception as e:
        print("❌ Error:", symbol, e)

ib.disconnect()
print("Finished.")