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
    Handles pd.NaT safely (never returns "NaT").
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
            # could be NaT but handled above
            return x.date().isoformat()
        if isinstance(x, datetime):
            return x.date().isoformat()
        if isinstance(x, date):
            return x.isoformat()

        if isinstance(x, str):
            s = x.strip()
            if not s:
                return ""
            if s.lower() == "nat":
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

        # numeric Excel serial fallback
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


def to_decimal_pct(x: Any) -> Optional[float]:
    """
    Convert a value that may be:
      - decimal (0.08)
      - percent (8 => 0.08)
    Return decimal.
    """
    v = to_float(x)
    if v is None:
        return None
    if abs(v) <= 2.0:
        return float(v)
    if abs(v) <= 200.0:
        return float(v) / 100.0
    # too large to be pct
    return None


# ----------------------------
# Table detection inside sheets
# ----------------------------
def parse_table_from_sheet(
    xl: pd.ExcelFile,
    sheet_name: str,
    header_must_contain: List[str],
    max_scan_rows: int = 80
) -> pd.DataFrame:
    """
    Reads sheet with header=None, scans to find a header row containing ALL tokens.
    Returns dataframe with that header row and data below it.
    """
    raw = xl.parse(sheet_name, header=None)
    scan_rows = min(max_scan_rows, raw.shape[0])

    tokens = [t.strip().lower() for t in header_must_contain if t.strip()]
    header_row_idx = None

    for i in range(scan_rows):
        row_vals = raw.iloc[i].tolist()
        row_str = " | ".join([safe_str(v).lower() for v in row_vals if safe_str(v)])
        if not row_str:
            continue
        ok = True
        for tok in tokens:
            if tok not in row_str:
                ok = False
                break
        if ok:
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
# Business logic extractors
# ----------------------------
def compute_ytd(labels: List[str], values: List[float]) -> Optional[float]:
    if not labels or not values or len(labels) != len(values) or len(values) < 2:
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
        a = values[first_idx]
        b = values[-1]
        if a == 0:
            return None
        return (b / a) - 1.0
    except Exception:
        return None


def extract_open_positions_from_blotter_table(df_pos: pd.DataFrame) -> List[Dict[str, Any]]:
    qty_col = find_col(df_pos, ["QUANTITY"]) or find_col_contains(df_pos, ["quantity", "qty"])
    if not qty_col:
        return []

    sym_col = (
        find_col(df_pos, ["TICKER", "SYMBOL", "UNDERLYING", "INSTRUMENT", "UnderlyingSymbol"])
        or find_col_contains(df_pos, ["ticker", "symbol", "under", "instr"])
    )

    out = []
    for _, r in df_pos.iterrows():
        q = to_float(r.get(qty_col))
        if q is None or q <= 0:   # your rule: OPEN if QUANTITY > 0
            continue
        sym = safe_str(r.get(sym_col)) if sym_col else ""
        side = "LONG" if q > 0 else "SHORT"
        out.append({"symbol": sym, "side": side, "quantity": q})

    out.sort(key=lambda x: x.get("symbol", ""))
    return out


def extract_nav_series_from_nav_table(df_nav: pd.DataFrame) -> Tuple[List[str], List[float]]:
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


def extract_plb_block_from_plb_table(df_plb: pd.DataFrame) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Returns:
      plb_usd_last (from EoP MIN NAV $)
      plb_block for charts: {labels, init, plb, perf} in decimals
    """
    # USD floor
    usd_col = find_col(df_plb, ["EoP MIN NAV $", "EOP MIN NAV $"]) or find_col_contains(df_plb, ["eop min nav", "min nav $", "min nav"])
    date_col = find_col(df_plb, ["Date", "DATE"]) or find_col_contains(df_plb, ["date"])

    # chart columns (try many names; your file likely has something like these)
    init_col = (
        find_col(df_plb, ["Initial PLB %", "INIT PLB %", "Init PLB %"])
        or find_col_contains(df_plb, ["initial", "init"])
    )
    plb_pct_col = (
        find_col(df_plb, ["PLB %", "PLB%"])
        or find_col_contains(df_plb, ["plb %", "plb%"])
    )
    perf_col = (
        find_col(df_plb, ["PERF %", "PERF%"])
        or find_col_contains(df_plb, ["perf %", "perf%"])
    )

    # last USD
    plb_usd_last = None
    if usd_col:
        s = df_plb[usd_col].dropna()
        if not s.empty:
            for v in reversed(s.tolist()):
                f = to_float(v)
                if f is not None:
                    plb_usd_last = float(f)
                    break

    # chart series
    plb_block = {"labels": [], "init": [], "plb": [], "perf": []}

    if date_col:
        for _, r in df_plb.iterrows():
            d = normalize_date(r.get(date_col))
            if not d:
                continue
            plb_block["labels"].append(d)

            # convert pct columns to decimals
            plb_block["init"].append(to_decimal_pct(r.get(init_col)) if init_col else None)
            plb_block["plb"].append(to_decimal_pct(r.get(plb_pct_col)) if plb_pct_col else None)
            plb_block["perf"].append(to_decimal_pct(r.get(perf_col)) if perf_col else None)

        # sort by date (labels)
        if plb_block["labels"]:
            pairs = list(zip(plb_block["labels"], plb_block["init"], plb_block["plb"], plb_block["perf"]))
            pairs.sort(key=lambda x: x[0])
            plb_block["labels"] = [p[0] for p in pairs]
            plb_block["init"]   = [p[1] for p in pairs]
            plb_block["plb"]    = [p[2] for p in pairs]
            plb_block["perf"]   = [p[3] for p in pairs]

    return plb_usd_last, plb_block


def extract_model_trades_from_trades_table(df_trades: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Extract model trades. Enforce trade_date (YYYY-MM-DD) and never output 'NaT'.
    """
    # strongly prefer exact TradeDate
    date_col = find_col(df_trades, ["TradeDate", "TRADEDATE"]) or find_col_contains(df_trades, ["trade date", "tradedate"])
    inst_col = (
        find_col(df_trades, ["UnderlyingSymbol", "UNDERLYINGSYMBOL", "Symbol", "SYMBOL", "Description", "DESCRIPTION"])
        or find_col_contains(df_trades, ["underlyingsymbol", "symbol", "description"])
    )
    status_col = find_col(df_trades, ["Status", "STATUS"]) or find_col_contains(df_trades, ["status"])
    struct_col = find_col(df_trades, ["Structure", "STRUCTURE", "Strategy", "STRATEGY", "Type", "TYPE"]) or find_col_contains(df_trades, ["structure", "strategy", "type"])

    out = []
    for _, r in df_trades.iterrows():
        dt = normalize_date(r.get(date_col)) if date_col else ""
        inst = safe_str(r.get(inst_col)) if inst_col else ""
        # keep row if it has something meaningful
        if not dt and not inst:
            continue
        out.append({
            "trade_date": dt,  # "" if missing (NOT "NaT")
            "instrument": inst,
            "structure": safe_str(r.get(struct_col)) if struct_col else "",
            "status": safe_str(r.get(status_col)) if status_col else "",
        })

    # newest first if dates exist
    out.sort(key=lambda x: x.get("trade_date", ""), reverse=True)
    return out


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

    # 1) Open positions from Blotter table (header contains TICKER + QUANTITY)
    df_pos = parse_table_from_sheet(xl, "Blotter", header_must_contain=["TICKER", "QUANTITY"], max_scan_rows=160)
    open_positions = extract_open_positions_from_blotter_table(df_pos)

    # 2) NAV series from NAV table (header contains Date + EoP)
    df_nav_table = parse_table_from_sheet(xl, "NAV", header_must_contain=["Date", "EoP"], max_scan_rows=80)
    labels, nav_values = extract_nav_series_from_nav_table(df_nav_table)
    nav_last = nav_values[-1] if nav_values else None

    # 3) PLB: USD + chart series from PLB table
    df_plb_table = parse_table_from_sheet(xl, "PLB", header_must_contain=["DATE", "EoP"], max_scan_rows=90)
    plb_usd, plb_block = extract_plb_block_from_plb_table(df_plb_table)

    gap_to_plb = None
    if nav_last is not None and plb_usd is not None and nav_last != 0:
        gap_to_plb = (plb_usd / nav_last) - 1.0

    ytd = compute_ytd(labels, nav_values)
    asof = labels[-1] if labels else datetime.today().date().isoformat()

    # 4) Trades: detect header row too (fix NaT/NaT)
    df_trades_table = parse_table_from_sheet(xl, "Trades", header_must_contain=["TradeDate"], max_scan_rows=120)
    model_trades = extract_model_trades_from_trades_table(df_trades_table)

    # Write site_performance.json
    perf_json: Dict[str, Any] = {
        "asof": asof,
        "nav": {"labels": labels, "values": nav_values},
        "snapshot": {
            "nav_usd": nav_last,
            "plb_usd": plb_usd,
            "gap_to_plb": gap_to_plb,
            "performance_ytd": ytd
        },
        # PLB chart series (optional but now we try to produce it)
        "plb": plb_block
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

    # sanity
    print(f"   Open positions: {len(open_positions)} (QUANTITY>0)")
    print(f"   NAV points: {len(labels)} (EoP Capital $)")
    print(f"   NAV last: {nav_last}")
    print(f"   PLB USD: {plb_usd}")
    print(f"   PLB chart points: {len(plb_block.get('labels', []))}")
    if gap_to_plb is not None:
        print(f"   Gap to PLB: {gap_to_plb:.6f}")
    print(f"   Model trades: {len(model_trades)}")


if __name__ == "__main__":
    main()