import argparse
import json
import os
from datetime import date, datetime
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
        if isinstance(x, str):
            s = x.strip()
            if not s:
                return None
            s = s.replace(",", "")
            if s.endswith("%"):
                return float(s[:-1]) / 100.0
            return float(s)
        return float(x)
    except Exception:
        return None


def normalize_date(x: Any) -> str:
    """Return YYYY-MM-DD or ''."""
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
                "%d/%m/%Y %H:%M:%S",
                "%d-%m-%Y %H:%M:%S",
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


def dedupe_headers(headers: List[str]) -> List[str]:
    out: List[str] = []
    seen: Dict[str, int] = {}
    for h in headers:
        base = h if h else "col"
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}__{seen[base]}")
    return out


def find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {_key(c): c for c in df.columns}
    for cand in candidates:
        k = _key(cand)
        if k and k in cols:
            return cols[k]
    return None


def find_col_contains(df: pd.DataFrame, needles: List[str], exclude: Optional[List[str]] = None) -> Optional[str]:
    ns = [str(n).strip().lower() for n in needles if str(n).strip()]
    ex = [str(x).strip().lower() for x in (exclude or []) if str(x).strip()]
    for c in df.columns:
        cl = _key(c)
        if not cl:
            continue
        if any(bad in cl for bad in ex):
            continue
        if all(n in cl for n in ns):
            return c
    return None


# ----------------------------
# Table detection in sheets
# ----------------------------
def parse_table_by_tokens_all(
    xl: pd.ExcelFile,
    sheet_name: str,
    header_must_contain: List[str],
    max_scan_rows: int = 350,
) -> pd.DataFrame:
    raw = xl.parse(sheet_name, header=None)
    scan_rows = min(max_scan_rows, raw.shape[0])

    tokens = [t.strip().lower() for t in header_must_contain if t and t.strip()]
    header_row_idx = None

    for i in range(scan_rows):
        row_vals = raw.iloc[i].tolist()
        row_text = " | ".join([safe_str(v).lower() for v in row_vals if safe_str(v)])
        if not row_text:
            continue
        if all(tok in row_text for tok in tokens):
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError(
            f"Could not find header row in sheet '{sheet_name}' with tokens: {header_must_contain}"
        )

    raw_header = [safe_str(v) if safe_str(v) else f"col_{j}" for j, v in enumerate(raw.iloc[header_row_idx].tolist())]
    header = dedupe_headers(raw_header)

    df = raw.iloc[header_row_idx + 1 :].copy()
    df.columns = header
    df = df.dropna(how="all")
    return df


# ----------------------------
# Extractors
# ----------------------------
def compute_ytd(labels: List[str], nav: List[float]) -> Optional[float]:
    if not labels or not nav or len(labels) != len(nav):
        return None
    try:
        year = int(labels[-1][:4])
        first_idx = next((i for i, d in enumerate(labels) if d and int(d[:4]) == year), None)
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
    qty_col = find_col(df_pos, ["QUANTITY"]) or find_col_contains(df_pos, ["quantity"])
    sym_col = (
        find_col(df_pos, ["TICKER", "SYMBOL", "UNDERLYING"])
        or find_col_contains(df_pos, ["ticker"])
        or find_col_contains(df_pos, ["symbol"])
        or find_col_contains(df_pos, ["under"])
    )
    asset_class_col = (
        find_col(df_pos, ["ASSET CLASS", "ASSET_CLASS", "Asset Class"])
        or find_col_contains(df_pos, ["asset", "class"])
    )

    if not qty_col or not sym_col:
        return []

    out: List[Dict[str, Any]] = []
    for _, r in df_pos.iterrows():
        q = to_float(r.get(qty_col))
        if q is None or q == 0:
            continue

        sym = safe_str(r.get(sym_col))
        if not sym:
            continue

        side = "LONG" if q > 0 else "SHORT"
        out.append({
            "symbol": sym,
            "side": side,
            "quantity": abs(q),
            "asset_class": safe_str(r.get(asset_class_col)) if asset_class_col else "",
        })

    out.sort(key=lambda x: x.get("symbol", ""))
    return out


def extract_nav(df_nav: pd.DataFrame) -> Tuple[List[str], List[float]]:
    date_col = find_col(df_nav, ["Date", "DATE"]) or find_col_contains(df_nav, ["date"], exclude=["week"])
    nav_col = (
        find_col(df_nav, ["EoP Capital $", "EOP CAPITAL $"])
        or find_col_contains(df_nav, ["eop", "capital"])
        or find_col_contains(df_nav, ["nav"], exclude=["min"])
    )

    if not date_col or not nav_col:
        return [], []

    rows: List[Tuple[str, float]] = []
    for _, r in df_nav.iterrows():
        d = normalize_date(r.get(date_col))
        v = to_float(r.get(nav_col))
        if not d or v is None:
            continue
        rows.append((d, float(v)))

    rows.sort(key=lambda x: x[0])
    labels = [x[0] for x in rows]
    values = [x[1] for x in rows]
    return labels, values


def extract_plb_usd(df_plb: pd.DataFrame) -> Optional[float]:
    usd_col = (
        find_col(df_plb, ["EoP MIN NAV $", "EOP MIN NAV $"])
        or find_col_contains(df_plb, ["eop", "min", "nav"])
        or find_col_contains(df_plb, ["min nav"], exclude=["bop"])
    )
    if not usd_col:
        return None

    for v in reversed(df_plb[usd_col].tolist()):
        f = to_float(v)
        if f is not None:
            return float(f)
    return None


def extract_plb_percent_series(df_plb: pd.DataFrame) -> Dict[str, Any]:
    # With duplicated headers in the sheet, the key is to pick the FIRST block,
    # not any later duplicate like DATE__1.
    date_col = find_col(df_plb, ["DATE", "Date"]) or find_col_contains(df_plb, ["date"], exclude=["__", "week"])
    perf_col = (
        find_col(df_plb, ["PERF %"])
        or find_col_contains(df_plb, ["perf", "%"], exclude=["wk", "week", "initial"])
    )
    plb_col = (
        find_col(df_plb, ["PLB %"])
        or find_col_contains(df_plb, ["plb", "%"], exclude=["wk", "week", "initial"])
    )
    init_col = (
        find_col(df_plb, ["Initial PLB %"])
        or find_col_contains(df_plb, ["initial", "plb"], exclude=["__"])
    )

    if not date_col:
        return {"labels": [], "init": [], "plb": [], "perf": []}

    rows: List[Tuple[str, Optional[float], Optional[float], Optional[float]]] = []
    for _, r in df_plb.iterrows():
        d = normalize_date(r.get(date_col))
        if not d:
            continue
        init_v = to_float(r.get(init_col)) if init_col else None
        plb_v = to_float(r.get(plb_col)) if plb_col else None
        perf_v = to_float(r.get(perf_col)) if perf_col else None

        # Keep the row if there is at least one useful series value.
        if init_v is None and plb_v is None and perf_v is None:
            continue

        rows.append((d, init_v, plb_v, perf_v))

    if not rows:
        return {"labels": [], "init": [], "plb": [], "perf": []}

    rows.sort(key=lambda x: x[0])
    return {
        "labels": [x[0] for x in rows],
        "init": [x[1] for x in rows],
        "plb": [x[2] for x in rows],
        "perf": [x[3] for x in rows],
    }


def extract_model_trades_from_blotter(df_tr: pd.DataFrame) -> List[Dict[str, Any]]:
    week_col = find_col(df_tr, ["WEEK", "Week"]) or find_col_contains(df_tr, ["week"])
    ticker_col = (
        find_col(df_tr, ["TICKER", "SYMBOL", "UNDERLYING"])
        or find_col_contains(df_tr, ["ticker"])
        or find_col_contains(df_tr, ["symbol"])
        or find_col_contains(df_tr, ["under"])
    )
    instrument_col = find_col(df_tr, ["Instrument", "INSTRUMENT"]) or find_col_contains(df_tr, ["instrument"])
    asset_class_col = find_col(df_tr, ["ASSET CLASS", "Asset Class"]) or find_col_contains(df_tr, ["asset", "class"])
    init_qty_col = find_col(df_tr, ["INITIAL QUANTITY", "Initial Quantity"]) or find_col_contains(df_tr, ["initial", "quantity"])
    qty_col = find_col(df_tr, ["QUANTITY", "Quantity"]) or find_col_contains(df_tr, ["quantity"])

    if not week_col:
        return []

    out: List[Dict[str, Any]] = []
    for _, r in df_tr.iterrows():
        wk = safe_str(r.get(week_col))
        if not wk:
            continue

        inst = safe_str(r.get(ticker_col)) if ticker_col else ""
        structure = safe_str(r.get(instrument_col)) if instrument_col else ""
        asset_class = safe_str(r.get(asset_class_col)) if asset_class_col else "" 

        init_q = to_float(r.get(init_qty_col)) if init_qty_col else None
        q = to_float(r.get(qty_col)) if qty_col else None

        status = "OPEN" if (q is not None and q != 0) else "CLOSED"

        side = None
        if init_q is not None:
            if init_q > 0:
                side = "LONG"
            elif init_q < 0:
                side = "SHORT"

        out.append(
            {
                "week": wk,
                "instrument": inst,
                "asset_class": asset_class,
                "structure": structure,
                "status": status,
                "side": side,
            }
        )

    def sort_key(x: Dict[str, Any]) -> int:
        try:
            return int(float(x.get("week", "0")))
        except Exception:
            return 0

    out.sort(key=sort_key, reverse=True)
    return out

def normalize_option_strike(underlying: str, raw_strike: float) -> float:
    u = safe_str(underlying).upper()

    if u in {"CL"}:
        return raw_strike / 100.0

    return raw_strike

def fallback_expiry_from_ticker(ticker: str) -> str:
    import re

    month_map = {
        "F": "Jan", "G": "Feb", "H": "Mar", "J": "Apr",
        "K": "May", "M": "Jun", "N": "Jul", "Q": "Aug",
        "U": "Sep", "V": "Oct", "X": "Nov", "Z": "Dec",
    }

    t = safe_str(ticker).upper()
    m = re.search(r"([FGHJKMNQUVXZ])(\d)", t)
    if not m:
        return ""

    mon = month_map.get(m.group(1), "")
    yr = f"202{m.group(2)}"
    return f"{mon} {yr}" if mon else ""


def format_expiry_display(x: Any, ticker: str = "") -> str:
    d = normalize_date(x)
    if d:
        try:
            return pd.to_datetime(d).strftime("%d %b %Y")
        except Exception:
            return d
    return fallback_expiry_from_ticker(ticker)


def build_open_positions_detailed(df_blotter, df_plb_trades, aum, name_map, trades_expiry_map):
    out = []

    col_week = find_col(df_blotter, ["WEEK", "Week"]) or find_col_contains(df_blotter, ["week"])
    col_ticker = find_col(df_blotter, ["TICKER", "SYMBOL"])
    col_under = find_col(df_blotter, ["UNDERLYING", "SYMBOL"])
    col_instr = find_col(df_blotter, ["INSTRUMENT"])
    col_asset = find_col(df_blotter, ["ASSET CLASS"])
    col_entry = find_col(df_blotter, ["ENTRY PRICE"])
    col_risk = find_col(df_blotter, ["RISK $", "INITIAL RISK $"])
    col_side = find_col(df_blotter, ["SIDE"])
    col_qty = find_col(df_blotter, ["QUANTITY"])
    col_tech_sl = find_col(df_blotter, ["TECH SL"])
    
    col_expiry = (
        find_col(df_blotter, ["EXPIRY", "LAST TRADE DATE", "LAST TRADE DATE OR CONTRACT MONTH"])
        or find_col_contains(df_blotter, ["expiry"])
        or find_col_contains(df_blotter, ["last", "trade", "date"])
    )
    
    col_plb_week = find_col(df_plb_trades, ["WEEK", "Week"]) or find_col_contains(df_plb_trades, ["week"])
    col_plb_instr = find_col(df_plb_trades, ["INSTRUMENT"])
    col_theme = find_col(df_plb_trades, ["THEME"])
    col_setup = (
        find_col(df_plb_trades, ["TECHNICAL SET-UP"])
        or find_col(df_plb_trades, ["TECHNICAL SET UP"])
        or find_col_contains(df_plb_trades, ["technical", "set"])
    )

    import re

    for _, r in df_blotter.iterrows():
        q = to_float(r.get(col_qty))
        if q is None or q == 0:
            continue

        week = safe_str(r.get(col_week))
        ticker = safe_str(r.get(col_ticker))
        expiry = trades_expiry_map.get(ticker, fallback_expiry_from_ticker(ticker))
        underlying = safe_str(r.get(col_under))
        description = name_map.get(underlying.upper(), underlying)
        structure = safe_str(r.get(col_instr))
        asset = safe_str(r.get(col_asset))

        if col_side:
            side = safe_str(r.get(col_side)).upper()
        else:
            side = "LONG" if q > 0 else "SHORT"

        if side not in {"LONG", "SHORT"}:
            side = "LONG" if q > 0 else "SHORT"

        entry = to_float(r.get(col_entry))
        risk_dollar = to_float(r.get(col_risk))
        tech_sl = to_float(r.get(col_tech_sl))

        if not underlying or not ticker or risk_dollar is None:
            continue

        obj = {
            "week": week,
            "ticker": ticker,
            "underlying": underlying,
            "description": description,
            "structure": structure,
            "asset_class": asset,
            "side": side,
            "expiry": expiry,
            "risk_dollar_raw": risk_dollar,
            "risk_dollar": abs(risk_dollar) if risk_dollar is not None else None,
            "max_loss_pct_capital": (abs(risk_dollar) / aum * 100.0) if aum else None,
        }

        if structure.strip().lower() in {"futures", "spot"}:
            obj["entry_price"] = entry
            obj["tech_stop"] = tech_sl

            if entry is not None and tech_sl is not None and entry != 0:
                if side == "LONG":
                    dist = (tech_sl - entry) / entry
                else:
                    dist = (entry - tech_sl) / entry
                obj["stop_distance_pct"] = min(dist * 100.0, 0.0)
        else:
            m = re.search(r"[CP](\d+)", ticker.upper())
            if m:
                raw_strike = float(m.group(1))
                obj["strike"] = normalize_option_strike(underlying, raw_strike)

        for _, p in df_plb_trades.iterrows():
            plb_week = safe_str(p.get(col_plb_week)).strip()
            plb_instr = safe_str(p.get(col_plb_instr)).strip().lower()

            if plb_week == week.strip() and underlying.lower() in plb_instr:
                obj["theme"] = safe_str(p.get(col_theme))
                obj["technical_setup"] = safe_str(p.get(col_setup))
                break

        out.append(obj)

    return out

def build_underlying_name_map(screener: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}

    by_symbol = screener.get("by_symbol", {})
    if isinstance(by_symbol, dict):
        for sym, row in by_symbol.items():
            name = safe_str((row or {}).get("name"))
            if sym and name:
                out[sym.upper()] = name

    universe = screener.get("universe", [])
    if isinstance(universe, list):
        for row in universe:
            sym = safe_str((row or {}).get("symbol")).upper()
            name = safe_str((row or {}).get("name"))
            if sym and name and sym not in out:
                out[sym] = name

    # fallback manuali utili
    out.setdefault("CC", "Cocoa")
    out.setdefault("CL", "WTI Crude")
    out.setdefault("GC", "Gold")
    out.setdefault("HG", "Copper")
    out.setdefault("NKD", "Nikkei 225")
    out.setdefault("ETH_PAXOS", "Ethereum")
    out.setdefault("6E", "EURUSD")

    return out

def aggregate_positions_for_alerts(positions: List[Dict[str, Any]], aum: float) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    for p in positions:
        week = safe_str(p.get("week"))
        underlying = safe_str(p.get("underlying"))
        if not week or not underlying:
            continue
        grouped.setdefault((week, underlying), []).append(p)

    out: List[Dict[str, Any]] = []

    for (week, underlying), items in grouped.items():
        option_legs = [x for x in items if safe_str(x.get("structure")) in {"Call", "Put"}]
        non_option_legs = [x for x in items if safe_str(x.get("structure")) not in {"Call", "Put"}]

        # Se c'è una singola posizione non-opzione, la lasciamo invariata
        if len(items) == 1 and non_option_legs:
            out.append(items[0])
            continue

        # Se ci sono esattamente 2 gambe opzioni dello stesso tipo, aggrega in spread
        if len(option_legs) == 2:
            leg1, leg2 = option_legs[0], option_legs[1]

            type1 = safe_str(leg1.get("structure"))
            type2 = safe_str(leg2.get("structure"))

            if type1 == type2 and type1 in {"Call", "Put"}:
                strikes = []
                for leg in option_legs:
                    strike = leg.get("strike")
                    if strike is not None:
                        strikes.append(float(strike))
                strikes = sorted(strikes, reverse=True)

                total_risk_dollar_raw = sum(
                    float(x.get("risk_dollar_raw", 0.0) or 0.0)
                    for x in option_legs
                )

                premium_max_loss = (abs(total_risk_dollar_raw) / aum * 100.0) if aum else None

                description = safe_str(leg1.get("description"))
                asset_class = safe_str(leg1.get("asset_class"))
                theme = safe_str(leg1.get("theme"))
                technical_setup = safe_str(leg1.get("technical_setup"))
                expiries = sorted(
                    {safe_str(x.get("expiry")) for x in option_legs if safe_str(x.get("expiry"))}
                )
                expiry_value = expiries[0] if len(expiries) == 1 else ""

                out.append({
                    "week": week,
                    "underlying": underlying,
                    "description": description,
                    "structure": f"{type1} Spread",
                    "asset_class": asset_class,
                    "side": "LONG",
                    "strikes": strikes,
                    "risk_dollar_raw": total_risk_dollar_raw,
                    "risk_dollar": abs(total_risk_dollar_raw),
                    "premium_max_loss_pct_capital": premium_max_loss,
                    "theme": theme,
                    "technical_setup": technical_setup,
                    "expiry": expiry_value,
                    "expiries": expiries if len(expiries) > 1 else None,
                    "legs": [
                        {
                            "ticker": safe_str(leg1.get("ticker")),
                            "side": safe_str(leg1.get("side")),
                            "strike": leg1.get("strike"),
                        },
                        {
                            "ticker": safe_str(leg2.get("ticker")),
                            "side": safe_str(leg2.get("side")),
                            "strike": leg2.get("strike"),
                        },
                    ],
                })
                continue

        # Fallback: se non rientra nei casi sopra, lascia gli elementi originali
        out.extend(items)

    def sort_key(x: Dict[str, Any]) -> int:
        try:
            return int(float(safe_str(x.get("week"))))
        except Exception:
            return 0

    out.sort(key=sort_key)
    return out

def build_trades_expiry_map(df_trades: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}

    col_symbol = (
        find_col(df_trades, ["Symbol", "SYMBOL"])
        or find_col_contains(df_trades, ["symbol"])
    )
    col_expiry = (
        find_col(df_trades, ["Expiry", "EXPIRY"])
        or find_col_contains(df_trades, ["expiry"])
    )

    if not col_symbol or not col_expiry:
        return out

    for _, r in df_trades.iterrows():
        sym = safe_str(r.get(col_symbol))
        if not sym:
            continue

        raw = r.get(col_expiry)
        try:
            dt = pd.to_datetime(raw, errors="coerce")
            if pd.notna(dt):
                out[sym] = dt.strftime("%d %b %Y")
                continue
        except Exception:
            pass

        s = safe_str(raw)
        if s:
            out[sym] = s

    return out


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    args = parser.parse_args()

    xl = pd.ExcelFile(args.file)
    ensure_dir("content")

    # Blotter
    df_blotter = parse_table_by_tokens_all(xl, "Blotter", ["TICKER", "QUANTITY"], max_scan_rows=600)
    open_positions = extract_open_positions(df_blotter)
    model_trades = extract_model_trades_from_blotter(df_blotter)

    df_trades = parse_table_by_tokens_all(
        xl,
        "Trades",
        ["Symbol", "Expiry"],
        max_scan_rows=600,
    )

    trades_expiry_map = build_trades_expiry_map(df_trades)

    # NAV
    df_nav = parse_table_by_tokens_all(xl, "NAV", ["Date", "EoP"], max_scan_rows=250)
    nav_labels, nav_values = extract_nav(df_nav)
    nav_last = nav_values[-1] if nav_values else None
    asof = nav_labels[-1] if nav_labels else datetime.today().date().isoformat()

    # PLB
    df_plb = parse_table_by_tokens_all(
        xl,
        "PLB",
        ["DATE", "PERF %", "PLB %", "Initial PLB %"],
        max_scan_rows=300,
    )
    df_plb_trades = parse_table_by_tokens_all(
        xl,
        "PLB",
        ["INSTRUMENT", "THEME", "TECHNICAL"],
        max_scan_rows=600,
    )
    
    print("PLB TRADES COLUMNS:", df_plb_trades.columns.tolist())
    print(df_plb_trades.head(10))
    
    plb_usd = extract_plb_usd(df_plb)
    plb_block = extract_plb_percent_series(df_plb)

    gap_to_plb = None
    if nav_last is not None and plb_usd is not None and nav_last != 0:
        gap_to_plb = (plb_usd / nav_last) - 1.0

    ytd = compute_ytd(nav_labels, nav_values)

    perf_json: Dict[str, Any] = {
     
        "nav": {"labels": nav_labels, "values": nav_values},
        "snapshot": {
            "nav_usd": nav_last,
            "plb_usd": plb_usd,
            "gap_to_plb": gap_to_plb,
            "performance_ytd": ytd,
        },
        "plb": plb_block,
    }
    write_json("content/site_performance.json", perf_json)
    print("✅ Wrote: content/site_performance.json")

    screener_path = "content/site_screener.json"
    screener = read_json(screener_path)
    screener["openPositions"] = open_positions
    name_map = build_underlying_name_map(screener)

    # ---- AUM (NAV ultimo) ----
    aum = nav_last if nav_last else 1.0

    open_positions_detailed = build_open_positions_detailed(
        df_blotter,
        df_plb_trades,
        aum,
        name_map,
        trades_expiry_map,
    )
    open_positions_aggregated = aggregate_positions_for_alerts(open_positions_detailed, aum)

    screener["openPositionsDetailed"] = open_positions_detailed
    screener["openPositionsAggregated"] = open_positions_aggregated
    screener["modelTrades"] = model_trades
    screener["modelTrades_source"] = "Blotter"
    screener["asof"] = screener.get("asof") or asof
    write_json(screener_path, screener)
    print("✅ Updated: content/site_screener.json (injected modelTrades + openPositions)")

    print(f"   Open positions: {len(open_positions)}")
    print(f"   NAV points: {len(nav_labels)}")
    print(f"   NAV last: {nav_last}")
    print(f"   PLB USD: {plb_usd}")
    print(f"   PLB chart points: {len(plb_block.get('labels', []))}")
    print(f"   Model trades: {len(model_trades)}")


if __name__ == "__main__":
    main()
