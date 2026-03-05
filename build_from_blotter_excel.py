import argparse
import json
import os
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ----------------------------
# Files / JSON helpers
# ----------------------------
def ensure_dir(path: str) -> None:
    if not path:
        return
    os.makedirs(path, exist_ok=True)


def read_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, obj: Any) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# ----------------------------
# Type / parsing helpers
# ----------------------------
def safe_str(x: Any) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, float) and pd.isna(x):
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None


def normalize_date(x: Any) -> str:
    """
    Returns YYYY-MM-DD if possible; else "".
    Supports:
      - pandas Timestamp / datetime / date
      - strings in common formats
      - numeric Excel serial (fallback)
    """
    if x is None:
        return ""
    try:
        if isinstance(x, float) and pd.isna(x):
            return ""
    except Exception:
        pass

    try:
        if isinstance(x, pd.Timestamp):
            return x.date().isoformat()
        if isinstance(x, datetime):
            return x.date().isoformat()
        if isinstance(x, date):
            return x.isoformat()

        if isinstance(x, str):
            s = x.strip()
            if not s:
                return ""
            # try common formats
            fmts = (
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d/%m/%Y",
                "%d-%m-%Y",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
            )
            for fmt in fmts:
                try:
                    return datetime.strptime(s[:19], fmt).date().isoformat()
                except Exception:
                    pass
            # fallback: ISO-like prefix
            if len(s) >= 10 and s[4] in "-/" and s[7] in "-/":
                return s[:10].replace("/", "-")
            return ""

        # numeric Excel serial fallback
        n = to_float(x)
        if n is not None:
            # Excel serial days from 1899-12-30
            d = datetime.utcfromtimestamp((n - 25569) * 86400).date()
            return d.isoformat()

    except Exception:
        return ""

    return ""


# ----------------------------
# Column detection (ROBUST)
# ----------------------------
def _col_key(c: Any) -> str:
    # Excel can produce non-string column headers (e.g. datetime)
    try:
        return str(c).strip().lower()
    except Exception:
        return ""


def find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Exact match (case-insensitive) among candidates.
    Works even if df.columns contain non-string objects (e.g. datetime headers).
    """
    cols = {_col_key(c): c for c in df.columns}
    for cand in candidates:
        key = _col_key(cand)
        if key and key in cols:
            return cols[key]
    return None


def find_col_contains(df: pd.DataFrame, contains_any: List[str]) -> Optional[str]:
    """
    Find first column whose name contains any substring (case-insensitive).
    Works even if df.columns contain non-string objects.
    """
    needles = [str(k).strip().lower() for k in contains_any]
    for c in df.columns:
        cl = _col_key(c)
        if not cl:
            continue
        for k in needles:
            if k and k in cl:
                return c
    return None


# ----------------------------
# Business logic
# ----------------------------
def compute_ytd(dates: List[str], nav: List[float]) -> Optional[float]:
    """
    YTD from first NAV of current year to last NAV (decimal return).
    dates are YYYY-MM-DD.
    """
    if not dates or not nav or len(dates) != len(nav) or len(nav) < 2:
        return None
    try:
        last_date = dates[-1]
        year = int(last_date[:4])
        first_idx = None
        for i, d in enumerate(dates):
            if d and int(d[:4]) == year:
                first_idx = i
                break
        if first_idx is None:
            return None
        first_nav = nav[first_idx]
        last_nav = nav[-1]
        if first_nav == 0:
            return None
        return (last_nav / first_nav) - 1.0
    except Exception:
        return None


def extract_open_positions_from_blotter(df_blotter: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Rule (your spec): QUANTITY > 0 => OPEN.
    We also infer side from sign (though OPEN uses >0 per your rule):
      - quantity > 0 => LONG
      - quantity < 0 => SHORT (not included in OPEN per your rule)
    """
    qty_col = find_col(df_blotter, ["QUANTITY"]) or find_col_contains(df_blotter, ["quantity", "qty"])
    if not qty_col:
        return []

    sym_col = (
        find_col(df_blotter, ["UNDERLYING", "SYMBOL", "TICKER", "INSTRUMENT", "LOCALSYMBOL", "NAME"])
        or find_col_contains(df_blotter, ["under", "symbol", "ticker", "instr", "local", "name"])
    )

    asset_col = find_col(df_blotter, ["ASSET_CLASS", "ASSET CLASS", "ASSETCLASS"]) or find_col_contains(df_blotter, ["asset"])
    exch_col  = find_col(df_blotter, ["EXCHANGE"]) or find_col_contains(df_blotter, ["exch"])
    ccy_col   = find_col(df_blotter, ["CURRENCY", "CCY"]) or find_col_contains(df_blotter, ["curr", "ccy"])

    out: List[Dict[str, Any]] = []

    for _, r in df_blotter.iterrows():
        q = to_float(r.get(qty_col))
        if q is None:
            continue

        # OPEN definition per your rule
        if q <= 0:
            continue

        sym = safe_str(r.get(sym_col)) if sym_col else ""
        if not sym:
            # fallback: find first reasonable string cell
            for c in df_blotter.columns:
                v = safe_str(r.get(c))
                if 1 <= len(v) <= 30 and any(ch.isalpha() for ch in v):
                    sym = v
                    break

        side = "LONG" if q > 0 else "SHORT"

        out.append({
            "symbol": sym,
            "side": side,
            "quantity": q,
            "asset_class": safe_str(r.get(asset_col)) if asset_col else "",
            "exchange": safe_str(r.get(exch_col)) if exch_col else "",
            "currency": safe_str(r.get(ccy_col)) if ccy_col else "",
        })

    out.sort(key=lambda x: (x.get("asset_class",""), x.get("symbol","")))
    return out


def extract_model_trades(df_trades: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Export model trades with trade_date in YYYY-MM-DD (from Trades.TradeDate).
    """
    date_col = find_col(df_trades, ["TradeDate", "TRADEDATE", "DATE"]) or find_col_contains(df_trades, ["trade", "date"])
    inst_col = (
        find_col(df_trades, ["Instrument", "INSTRUMENT", "Underlying", "UNDERLYING", "Symbol", "SYMBOL", "Ticker", "TICKER"])
        or find_col_contains(df_trades, ["instrument", "under", "symbol", "ticker"])
    )
    struct_col = find_col(df_trades, ["Structure", "STRUCTURE", "Strategy", "STRATEGY", "Type", "TYPE"]) or find_col_contains(df_trades, ["struct", "strat", "type"])
    status_col = find_col(df_trades, ["Status", "STATUS", "State", "STATE"]) or find_col_contains(df_trades, ["status", "state"])

    out: List[Dict[str, Any]] = []

    for _, r in df_trades.iterrows():
        dt = normalize_date(r.get(date_col)) if date_col else ""
        inst = safe_str(r.get(inst_col)) if inst_col else ""
        if not (dt or inst):
            continue
        out.append({
            "trade_date": dt,
            "instrument": inst,
            "structure": safe_str(r.get(struct_col)) if struct_col else "",
            "status": safe_str(r.get(status_col)) if status_col else "",
        })

    out.sort(key=lambda x: x.get("trade_date",""), reverse=True)
    return out


def extract_nav_series(df_perf: pd.DataFrame) -> Tuple[List[str], List[float]]:
    """
    Find date + NAV columns in a generic Performance sheet.
    """
    dcol = find_col(df_perf, ["DATE", "Date", "AsOf", "ASOF"]) or find_col_contains(df_perf, ["date", "asof"])
    navcol = find_col(df_perf, ["NAV", "Nav", "nav"]) or find_col_contains(df_perf, ["nav"])

    if not dcol or not navcol:
        return [], []

    labels: List[str] = []
    values: List[float] = []

    for _, r in df_perf.iterrows():
        d = normalize_date(r.get(dcol))
        v = to_float(r.get(navcol))
        if not d or v is None:
            continue
        labels.append(d)
        values.append(float(v))

    if labels:
        pairs = sorted(zip(labels, values), key=lambda x: x[0])
        labels = [p[0] for p in pairs]
        values = [p[1] for p in pairs]
    return labels, values


def extract_plb_usd(df_plb: pd.DataFrame) -> Optional[float]:
    """
    Use column 'EoP MIN NAV $' in sheet PLB. Take last non-null numeric value.
    """
    col = find_col(df_plb, ["EoP MIN NAV $", "EOP MIN NAV $"]) or find_col_contains(df_plb, ["eop", "min nav", "min_nav", "plb", "floor"])
    if not col:
        return None

    s = df_plb[col].dropna()
    if s.empty:
        return None

    for v in reversed(s.tolist()):
        f = to_float(v)
        if f is not None:
            return float(f)
    return None


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Path to 1XW_TradeBlotter_Web.xlsx")
    args = parser.parse_args()

    xlsx = args.file
    if not os.path.exists(xlsx):
        raise FileNotFoundError(xlsx)

    ensure_dir("content")

    xl = pd.ExcelFile(xlsx)

    # Load required sheets (best-effort)
    def load_sheet_exact(name_exact: str) -> Optional[pd.DataFrame]:
        for nm in xl.sheet_names:
            if nm.strip().lower() == name_exact.strip().lower():
                return xl.parse(nm)
        return None

    df_blotter = load_sheet_exact("Blotter")
    df_trades  = load_sheet_exact("Trades")
    df_plb     = load_sheet_exact("PLB")

    # Performance sheet: try common names, else auto-detect by having nav+date
    df_perf = None
    perf_candidates = ["performance", "perf", "nav", "equitycurve", "equity curve"]
    for nm in xl.sheet_names:
        if nm.strip().lower() in perf_candidates:
            df_perf = xl.parse(nm)
            break
    if df_perf is None:
        for nm in xl.sheet_names:
            tmp = xl.parse(nm, nrows=10)
            if find_col_contains(tmp, ["nav"]) and find_col_contains(tmp, ["date", "asof"]):
                df_perf = xl.parse(nm)
                break

    # Extractors
    open_positions = extract_open_positions_from_blotter(df_blotter) if df_blotter is not None else []
    model_trades   = extract_model_trades(df_trades) if df_trades is not None else []

    labels, nav_values = extract_nav_series(df_perf) if df_perf is not None else ([], [])
    nav_last = nav_values[-1] if nav_values else None

    plb_usd = extract_plb_usd(df_plb) if df_plb is not None else None
    gap_to_plb = None
    if nav_last is not None and plb_usd is not None and nav_last != 0:
        gap_to_plb = (plb_usd / nav_last) - 1.0

    ytd = compute_ytd(labels, nav_values)
    asof = labels[-1] if labels else datetime.today().date().isoformat()

    # Write site_performance.json
    perf_json = {
        "asof": asof,
        "nav": {
            "labels": labels,
            "values": nav_values
        },
        "snapshot": {
            "nav_usd": nav_last,
            "plb_usd": plb_usd,
            "gap_to_plb": gap_to_plb,
            "performance_ytd": ytd
        }
    }
    write_json("content/site_performance.json", perf_json)
    print("✅ Wrote: content/site_performance.json")

    # Inject into site_screener.json
    screener_path = "content/site_screener.json"
    screener = read_json(screener_path)

    screener["openPositions"] = open_positions
    screener["modelTrades"] = model_trades
    screener["asof"] = screener.get("asof") or asof

    write_json(screener_path, screener)
    print("✅ Updated: content/site_screener.json (injected modelTrades + openPositions)")

    # Console sanity
    print(f"   Open positions: {len(open_positions)} (rule QUANTITY>0)")
    print(f"   Model trades: {len(model_trades)} (with trade_date)")
    if nav_last is not None:
        print(f"   NAV last: {nav_last}")
    if plb_usd is not None:
        print(f"   PLB USD: {plb_usd}")
    if gap_to_plb is not None:
        print(f"   Gap to PLB: {gap_to_plb:.6f}")


if __name__ == "__main__":
    main()