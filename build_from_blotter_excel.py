import argparse
import json
import os
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ----------------------------
# JSON helpers
# ----------------------------
def ensure_dir(path: str) -> None:
    if path:
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
# Basic parsing
# ----------------------------
def safe_str(x: Any) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None


def normalize_date(x: Any) -> str:
    """
    Returns YYYY-MM-DD or "".
    Handles pd.NaT safely.
    """
    if x is None:
        return ""
    try:
        if pd.isna(x):
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
            if not s or s.lower() == "nat":
                return ""
            for fmt in (
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%d/%m/%Y",
                "%d-%m-%Y",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
            ):
                try:
                    return datetime.strptime(s[:19], fmt).date().isoformat()
                except Exception:
                    pass
            if len(s) >= 10 and s[4] in "-/" and s[7] in "-/":
                return s[:10].replace("/", "-")
            return ""

        n = to_float(x)
        if n is not None:
            d = datetime.utcfromtimestamp((n - 25569) * 86400).date()
            return d.isoformat()
    except Exception:
        return ""
    return ""


def _key(x: Any) -> str:
    try:
        return str(x).strip().lower()
    except Exception:
        return ""


def find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {_key(c): c for c in df.columns}
    for cand in candidates:
        k = _key(cand)
        if k and k in cols:
            return cols[k]
    return None


def find_col_contains(df: pd.DataFrame, needles: List[str]) -> Optional[str]:
    ns = [str(n).strip().lower() for n in needles]
    for c in df.columns:
        cl = _key(c)
        if not cl:
            continue
        for n in ns:
            if n and n in cl:
                return c
    return None


# ----------------------------
# Table detection in sheets
# ----------------------------
def parse_table_by_tokens_all(
    xl: pd.ExcelFile,
    sheet_name: str,
    header_must_contain: List[str],
    max_scan_rows: int = 350
) -> pd.DataFrame:
    raw = xl.parse(sheet_name, header=None)
    scan_rows = min(max_scan_rows, raw.shape[0])

    tokens = [t.strip().lower() for t in header_must_contain if t.strip()]
    header_row_idx = None

    for i in range(scan_rows):
        row_vals = raw.iloc[i].tolist()
        row_str = " | ".join([safe_str(v).lower() for v in row_vals if safe_str(v)])
        if not row_str:
            continue
        if all(tok in row_str for tok in tokens):
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError(f"Could not find header row in sheet '{sheet_name}' with tokens: {header_must_contain}")

    header = [safe_str(v) if safe_str(v) else f"col_{j}" for j, v in enumerate(raw.iloc[header_row_idx].tolist())]
    df = raw.iloc[header_row_idx + 1:].copy()
    df.columns = header
    df = df.dropna(how="all")
    return df


# ----------------------------
# Extractors
# ----------------------------
def compute_ytd(labels: List[str], nav: List[float]) -> Optional[float]:
    if not labels or not nav or len(labels) != len(nav) or len(nav) < 2:
        return None
    try:
        year = int(labels[-1][:4])
        first_idx = None
        for i, d in enumerate(labels):
            if d and int(d[:4]) == year:
                first_idx = i
                break
        if first_idx is None:
            return None
        a = nav[first_idx]
        b = nav[-1]
        if a == 0:
            return None
        return (b / a) - 1.0
    except Exception:
        return None


def extract_open_positions(df_pos: pd.DataFrame) -> List[Dict[str, Any]]:
    qty_col = find_col(df_pos, ["QUANTITY"]) or find_col_contains(df_pos, ["quantity", "qty"])
    sym_col = (
        find_col(df_pos, ["TICKER", "Ticker", "SYMBOL", "Symbol"])
        or find_col_contains(df_pos, ["ticker", "symbol", "under"])
    )

    if not qty_col or not sym_col:
        return []

    out = []
    for _, r in df_pos.iterrows():
        q = to_float(r.get(qty_col))
        if q is None or q <= 0:
            continue
        sym = safe_str(r.get(sym_col))
        out.append({
            "symbol": sym,
            "side": "LONG",
            "quantity": q
        })

    out.sort(key=lambda x: x.get("symbol", ""))
    return out


def extract_nav(df_nav: pd.DataFrame) -> Tuple[List[str], List[float]]:
    date_col = find_col(df_nav, ["Date", "DATE"]) or find_col_contains(df_nav, ["date"])
    nav_col = find_col(df_nav, ["EoP Capital $", "EOP CAPITAL $"]) or find_col_contains(df_nav, ["eop capital", "capital $", "nav"])

    if not date_col or not nav_col:
        return [], []

    labels, values = [], []
    for _, r in df_nav.iterrows():
        d = normalize_date(r.get(date_col))
        v = to_float(r.get(nav_col))
        if not d or v is None:
            continue
        labels.append(d)
        values.append(float(v))

    pairs = sorted(zip(labels, values), key=lambda x: x[0])
    return [p[0] for p in pairs], [p[1] for p in pairs]


def extract_plb_usd(df_plb: pd.DataFrame) -> Optional[float]:
    usd_col = find_col(df_plb, ["EoP MIN NAV $", "EOP MIN NAV $"]) or find_col_contains(df_plb, ["eop min nav", "min nav"])
    if not usd_col:
        return None

    s = df_plb[usd_col].dropna()
    if s.empty:
        return None

    for v in reversed(s.tolist()):
        f = to_float(v)
        if f is not None:
            return float(f)
    return None


def build_plb_percent_series(labels: List[str], nav: List[float], plb_usd: Optional[float]) -> Dict[str, Any]:
    """
    Always produce PLB chart with same points as NAV:
      perf% = NAV/NAV0 - 1
      plb%  = PLB_USD/NAV0 - 1
      init% = same as plb%
    Values are decimals.
    """
    if not labels or not nav:
        return {"labels": [], "init": [], "plb": [], "perf": []}

    nav0 = nav[0]
    if nav0 is None or nav0 == 0:
        return {"labels": [], "init": [], "plb": [], "perf": []}

    perf = [(v / nav0 - 1.0) if v is not None else None for v in nav]

    if plb_usd is None:
        return {
            "labels": labels,
            "init": [None] * len(labels),
            "plb": [None] * len(labels),
            "perf": perf
        }

    plb_pct = (plb_usd / nav0) - 1.0
    return {
        "labels": labels,
        "init": [plb_pct] * len(labels),
        "plb": [plb_pct] * len(labels),
        "perf": perf
    }


def extract_model_trades_from_blotter(df_tr: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Blotter has no TradeDate; use WEEK as first field.
    Structure = Instrument
    Status = derived from Quantity
    """

    week_col = (
        find_col(df_tr, ["WEEK", "Week"])
        or find_col_contains(df_tr, ["week"])
    )

    ticker_col = (
        find_col(df_tr, ["TICKER", "Ticker", "SYMBOL", "Symbol", "UNDERLYING", "Underlying"])
        or find_col_contains(df_tr, ["ticker", "symbol", "under"])
    )

    instrument_col = (
        find_col(df_tr, ["Instrument", "INSTRUMENT"])
        or find_col_contains(df_tr, ["instrument"])
    )

    qty_col = (
        find_col(df_tr, ["QUANTITY", "Quantity"])
        or find_col_contains(df_tr, ["quantity", "qty"])
    )

    if not week_col:
        return []

    out = []

    for _, r in df_tr.iterrows():
        wk = safe_str(r.get(week_col))
        if not wk:
            continue

        inst = safe_str(r.get(ticker_col)) if ticker_col else ""
        structure = safe_str(r.get(instrument_col)) if instrument_col else ""

        q = to_float(r.get(qty_col)) if qty_col else None
        status = "OPEN" if (q is not None and q > 0) else "CLOSED"

        out.append({
            "week": wk,
            "instrument": inst,
            "structure": structure,
            "status": status,
        })

    def sort_key(x: Dict[str, Any]):
        try:
            return int(float(x.get("week", "")))
        except Exception:
            return 0

    out.sort(key=sort_key, reverse=True)
    return out


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    args = parser.parse_args()

    xl = pd.ExcelFile(args.file)
    ensure_dir("content")

    # Open positions from Blotter
    df_pos = parse_table_by_tokens_all(xl, "Blotter", ["TICKER", "QUANTITY"], max_scan_rows=400)
    open_positions = extract_open_positions(df_pos)

    # Model trades from Blotter
    df_blotter_trades = parse_table_by_tokens_all(xl, "Blotter", ["WEEK", "TICKER"], max_scan_rows=600)
    model_trades = extract_model_trades_from_blotter(df_blotter_trades)

    # NAV
    df_nav = parse_table_by_tokens_all(xl, "NAV", ["Date", "EoP"], max_scan_rows=250)
    labels, nav_values = extract_nav(df_nav)
    nav_last = nav_values[-1] if nav_values else None
    asof = labels[-1] if labels else datetime.today().date().isoformat()

    # PLB
    df_plb = parse_table_by_tokens_all(xl, "PLB", ["DATE", "EoP"], max_scan_rows=300)
    plb_usd = extract_plb_usd(df_plb)

    gap_to_plb = None
    if nav_last is not None and plb_usd is not None and nav_last != 0:
        gap_to_plb = (plb_usd / nav_last) - 1.0

    ytd = compute_ytd(labels, nav_values)
    plb_block = build_plb_percent_series(labels, nav_values, plb_usd)

    perf_json: Dict[str, Any] = {
        "asof": asof,
        "nav": {"labels": labels, "values": nav_values},
        "snapshot": {
            "nav_usd": nav_last,
            "plb_usd": plb_usd,
            "gap_to_plb": gap_to_plb,
            "performance_ytd": ytd
        },
        "plb": plb_block
    }
    write_json("content/site_performance.json", perf_json)
    print("✅ Wrote: content/site_performance.json")

    screener_path = "content/site_screener.json"
    screener = read_json(screener_path)
    screener["openPositions"] = open_positions
    screener["modelTrades"] = model_trades
    screener["modelTrades_source"] = "Blotter"
    screener["asof"] = screener.get("asof") or asof
    write_json(screener_path, screener)
    print("✅ Updated: content/site_screener.json (injected modelTrades + openPositions)")

    print(f"   Open positions: {len(open_positions)} (QUANTITY>0)")
    print(f"   NAV points: {len(labels)} (EoP Capital $)")
    print(f"   NAV last: {nav_last}")
    print(f"   PLB USD: {plb_usd}")
    print(f"   PLB chart points: {len(plb_block.get('labels', []))}")
    print(f"   Model trades: {len(model_trades)} (source: Blotter, field: week)")


if __name__ == "__main__":
    main()