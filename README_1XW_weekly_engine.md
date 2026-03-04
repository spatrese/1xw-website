# 1XW Weekly Engine (v1)

## What this does
- Pulls **historical bars** from Interactive Brokers (TWS) for your 1XW universe
- Produces **multi-timeframe** (4H / 1D / 1W) technical summary + long/short screening
- Generates short **asset-class commentary** (Equity / Rates / Commodities / FX / Crypto)
- Outputs:
  - `outputs/weekly_report_YYYYMMDD.xlsx`
  - `outputs/weekly_report_YYYYMMDD.md`

## Setup (Windows + TWS)
1. Open **TWS**
2. Enable API:
   - Global Configuration → API → Settings
   - Enable ActiveX and Socket Clients
   - Port:
     - **7496** = LIVE
     - **7497** = PAPER
3. Install Python packages:
   ```bat
   pip install ib_insync pandas numpy openpyxl
   ```

## Universe
Edit `universe_1xw.csv` (symbol / name / asset_class / contract_type).
Contract types:
- `FUT` = futures (uses IB `CONTFUT`)
- `IND` = index (VIX)
- `STK` = ETF
- `CRYPTO` = crypto (IB PAXOS; may require permissions)

## Run
From the folder containing the files:
```bat
python weekly_engine_1xw.py --port 7497
```

## Notes
- Some exchanges (e.g., **Eurex**) may require permissions and sometimes different exchange codes.
  The script tries multiple exchange candidates per symbol; if still failing, check the `Errors` sheet.
- This is **screening / research** output. The final trade decision remains manual by design (1XW rule).
