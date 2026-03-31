#!/usr/bin/env python3
from __future__ import annotations
import argparse, glob, json, os, re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from macro_calendar_engine import build_event_calendar


def read_json(path: str) -> Any:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def iso_week_id(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"


def safe_str(x: Any) -> str:
    return '' if x is None else str(x)


def parse_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace('%', '')
        return float(s) if s else None
    except Exception:
        return None


def canonical_asset_class(ac: str) -> str:
    s = safe_str(ac).strip().lower()
    return {
        'equity': 'Equities',
        'equities': 'Equities',
        'rates': 'Rates',
        'fx': 'FX',
        'commodity': 'Commodities',
        'commodities': 'Commodities',
        'crypto': 'Crypto',
    }.get(s, safe_str(ac).strip() or 'Other')

def fx_display_symbol(symbol: str) -> str:
    s = safe_str(symbol).strip().upper()
    if len(s) == 6 and s.startswith("USD"):
        # USDJPY -> JPYUSD ; USDCAD -> CADUSD
        return s[3:] + "USD"
    return s


def fx_row_score(symbol: str, score: Optional[float]) -> Optional[float]:
    s = safe_str(symbol).strip().upper()
    sc = parse_float(score)
    if sc is None:
        return None

    # We want the score to reflect strength of the non-USD currency.
    # EURUSD up -> + score stays as is
    # USDJPY up -> JPYUSD should read as weaker JPY -> invert sign
    if len(s) == 6 and s.startswith("USD"):
        return -sc
    return sc

def normalize_score_long(score: float) -> float:
    return clamp((score + 4.0) / 8.0)


def normalize_score_short(score: float) -> float:
    return clamp((-score + 4.0) / 8.0)


SETUP_BONUS_LONG = {
    'breakout': 0.10,
    'trend continuation': 0.05,
    'trend continuation (down)': -0.05,
    'neutral': 0.0,
    'breakdown': -0.05,
}
SETUP_BONUS_SHORT = {
    'breakdown': 0.10,
    'trend continuation (down)': 0.05,
    'trend continuation': 0.05,
    'neutral': 0.0,
    'breakout': -0.05,
}


def setup_key(s: str) -> str:
    return safe_str(s).strip().lower()


def momentum_assist(ret_20d_pct: Optional[float], side: str) -> float:
    if ret_20d_pct is None:
        return 0.0
    return clamp((ret_20d_pct if side == 'long' else -ret_20d_pct) / 10.0, -0.10, 0.10)


def tech_scores(row: Dict[str, Any]) -> Tuple[float, float]:
    score = parse_float(row.get('score')) or 0.0
    sk = setup_key(row.get('setup', ''))
    r20 = parse_float(row.get('ret_20d_%')) if row.get('ret_20d_%') is not None else parse_float(row.get('ret_20d_pct'))
    tlong = normalize_score_long(score) + SETUP_BONUS_LONG.get(sk, 0.0) + momentum_assist(r20, 'long')
    tshort = normalize_score_short(score) + SETUP_BONUS_SHORT.get(sk, 0.0) + momentum_assist(r20, 'short')
    return clamp(tlong, 0.0, 1.0), clamp(tshort, 0.0, 1.0)


POS_WORDS = ['rally', 'gain', 'surge', 'rebound', 'beat', 'upgrade', 'cuts', 'cut', 'easing', 'dovish', 'stimulus', 'growth']
NEG_WORDS = ['selloff', 'drop', 'plunge', 'crash', 'miss', 'downgrade', 'hikes', 'tightening', 'hawkish', 'recession', 'slowdown', 'risk-off']


def text_score(title: str, summary: str) -> int:
    txt = f"{title} {summary}".lower()
    return sum(1 for w in POS_WORDS if w in txt) - sum(1 for w in NEG_WORDS if w in txt)

def fx_row_bias_score(title: str, summary: str) -> int:
    txt = f"{safe_str(title)} {safe_str(summary)}".lower()
    score = 0

    row_positive = [
        'euro rises', 'euro gains', 'eur rises', 'eur gains',
        'yen rises', 'yen gains', 'jpy rises', 'jpy gains',
        'pound rises', 'pound gains', 'sterling rises', 'sterling gains',
        'aud rises', 'aud gains', 'aussie rises', 'aussie gains',
        'cad rises', 'cad gains', 'loonie rises', 'loonie gains',
        'dollar falls', 'dollar drops', 'weaker dollar', 'usd falls',
        'fed dovish', 'dovish fed', 'lower yields', 'treasury yields fall',
        'ecb hawkish', 'boj hawkish', 'boe hawkish', 'rba hawkish', 'boc hawkish',
    ]

    row_negative = [
        'euro falls', 'euro drops', 'eur falls', 'eur drops',
        'yen falls', 'yen drops', 'jpy falls', 'jpy drops',
        'pound falls', 'pound drops', 'sterling falls', 'sterling drops',
        'aud falls', 'aud drops', 'aussie falls', 'aussie drops',
        'cad falls', 'cad drops', 'loonie falls', 'loonie drops',
        'dollar rises', 'dollar gains', 'stronger dollar', 'usd gains',
        'fed hawkish', 'hawkish fed', 'higher yields', 'treasury yields rise',
        'ecb dovish', 'boj dovish', 'boe dovish', 'rba dovish', 'boc dovish',
    ]

    for w in row_positive:
        if w in txt:
            score += 1
    for w in row_negative:
        if w in txt:
            score -= 1

    return score

def build_fund_commentary(ac: str, tone: str, bias: float, conf: float, top_news: List[Dict[str, Any]]) -> str:
    leads = {
        'Equities': {
            'Supportive': 'Risk sentiment remains constructive, though leadership is selective.',
            'Cautious': 'Equity tone is cautious, with macro uncertainty limiting broad participation.',
            'Mixed': 'Equity signals are balanced, with supportive pockets offset by an uneven backdrop.',
        },
        'Rates': {
            'Supportive': 'Rates are leaning supportive for duration, but incoming data remains key.',
            'Cautious': 'Rates remain cautious, with inflation and policy communication driving repricing risk.',
            'Mixed': 'Rates remain data-dependent, with no clean trend yet across inflation and policy expectations.',
        },
        'FX': {
            'Supportive': 'Major currencies are broadly supported against the dollar, with macro divergence favouring the rest of the world.',
            'Cautious': 'The dollar backdrop remains supportive, limiting follow-through in major currencies.',
            'Mixed': 'Major currencies show mixed signals against the dollar, with no clean macro direction yet.',
        },
        'Commodities': {
            'Supportive': 'Commodity tone is supportive, with supply dynamics still relevant.',
            'Cautious': 'Commodity tone is cautious, with growth uncertainty weighing against supply support.',
            'Mixed': 'Commodity signals are mixed, with demand uncertainty offsetting selective support.',
        },
        'Crypto': {
            'Supportive': 'Crypto tone is constructive, but still sensitive to liquidity and regulation.',
            'Cautious': 'Crypto remains fragile, with policy and risk appetite still key swing factors.',
            'Mixed': 'Crypto tone is balanced, with supportive narratives competing against uncertainty.',
        },
    }
    lead = leads.get(ac, {}).get(tone, f'{ac} tone is {tone.lower()} this week.')
    src = safe_str(top_news[0].get('source')) if top_news else ''
    return f"{lead} Bias {bias:+.2f}, confidence {conf:.2f}.{(' Primary flow source: ' + src + '.') if src else ''}"


DISPLAY_STRONG_MACRO = {
    "Equities": [
        ("fed", 2.2), ("fomc", 2.2), ("inflation", 2.0), ("cpi", 2.0),
        ("ppi", 1.5), ("yields", 1.8), ("yield", 1.6), ("rates", 1.4),
        ("treasury", 1.4), ("policy", 1.3), ("recession", 1.5),
        ("growth", 1.1), ("risk sentiment", 2.0), ("risk-off", 1.8),
        ("risk-on", 1.8), ("stocks", 1.4), ("equities", 1.4),
        ("s&p", 1.8), ("nasdaq", 1.8), ("volatility", 1.2), ("vix", 1.2),
        ("oil", 1.0), ("geopolitical", 1.2), ("tariff", 1.2)
    ],
    "Rates": [
        ("fed", 2.4), ("ecb", 2.4), ("boj", 2.0), ("boe", 2.0),
        ("inflation", 2.3), ("cpi", 2.2), ("ppi", 1.7),
        ("yield", 2.0), ("yields", 2.0), ("treasury", 1.8),
        ("rate cut", 2.0), ("rate hike", 2.0), ("policy", 1.5),
        ("central bank", 1.6), ("liquidity", 1.2)
    ],
    "FX": [
        ("dollar", 2.0), ("dxy", 2.0), ("euro", 1.4), ("yen", 1.4),
        ("currency", 1.4), ("fx", 1.4), ("foreign exchange", 1.6),
        ("fed", 1.2), ("ecb", 1.2), ("boj", 1.2), ("boe", 1.0),
        ("rate differential", 1.8), ("yield spread", 1.5)
    ],
    "Commodities": [
        ("oil", 2.2), ("wti", 2.0), ("brent", 2.0), ("gold", 1.6),
        ("silver", 1.2), ("copper", 1.3), ("natural gas", 1.6),
        ("opec", 2.0), ("inventory", 1.6), ("supply", 1.4),
        ("demand", 1.4), ("energy", 1.2), ("middle east", 1.3),
        ("strait of hormuz", 1.8)
    ],
    "Crypto": [
        ("bitcoin", 2.2), ("btc", 1.8), ("ethereum", 1.8), ("eth", 1.4),
        ("crypto", 1.6), ("etf", 1.7), ("sec", 1.5), ("regulation", 1.4),
        ("stablecoin", 1.3), ("exchange", 1.1), ("liquidity", 1.2)
    ],
}

LOW_SIGNAL_PATTERNS = [
    "airport", "tsa", "security wait", "flight",
    "celebrity", "sports", "weather", "traffic", "crime", "real estate listing",
    "restaurant", "consumer tip", "shopping", "holiday travel"
]

SYSTEMIC_EQUITY_TERMS = [
    "fed", "fomc", "inflation", "cpi", "ppi", "yields", "rates",
    "s&p", "nasdaq", "stocks", "equities", "risk-on", "risk-off",
    "recession", "growth", "policy", "treasury", "volatility", "vix",
    "oil", "geopolitical", "tariff", "earnings", "guidance"
]

MICRO_EQUITY_TERMS = [
    "dividend", "ceo", "quarter", "q1", "q2", "q3", "q4", "eps",
    "buyback", "shareholder", "merger talks", "product launch", "store opening"
]

def contains_any(text: str, terms: List[str]) -> bool:
    return any(t in text for t in terms)

def source_quality_bonus(source: str) -> float:
    s = source.lower()
    if "federal reserve" in s or "ecb" in s or "bank of england" in s or "bank of japan" in s or "eia" in s:
        return 0.8
    if "financial times" in s or "reuters" in s or "bloomberg" in s or "wsj" in s:
        return 0.5
    if "cnbc" in s or "yahoo" in s or "investing.com" in s or "finviz" in s or "fxstreet" in s:
        return 0.1
    return 0.0

def idiosyncratic_penalty(ac: str, text: str) -> float:
    penalty = 0.0
    for pat in LOW_SIGNAL_PATTERNS:
        if pat in text:
            penalty += 2.0

    if ac == "Equities":
        systemic = sum(1 for t in SYSTEMIC_EQUITY_TERMS if t in text)
        micro = sum(1 for t in MICRO_EQUITY_TERMS if t in text)

        if systemic == 0 and micro > 0:
            penalty += 1.8
        if "earnings" in text and systemic == 0:
            penalty += 1.2

    return penalty

def display_news_score(ac: str, item: Dict[str, Any]) -> float:
    title_raw = safe_str(item.get("title"))
    title = title_raw.lower()
    summary = safe_str(item.get("summary")).lower()
    source = safe_str(item.get("source"))
    
    text = f"{title} {summary}"
 
    score = 0.0

    if any(p in text for p in [
       "what happens if",
       "should you buy",
       "stocks of the week",
       "how to invest",
       "retirement",
       "tax refund",
       "analysis:",
       "opinion",
       "commentary",
   ]):
       score -= 1.5

    try:
        score += float(item.get("score") or 0.0)   # score dal news_engine
    except Exception:
        pass

    for kw, bonus in DISPLAY_STRONG_MACRO.get(ac, []):
        if kw in text:
            score += bonus

    score += source_quality_bonus(source)

    # headline con summary più ricco sono spesso più utili
    if len(summary) >= 120:
        score += 0.25

    # penalità micro / noise
    score -= idiosyncratic_penalty(ac, text)

    if ac == "Equities":
        # ticker soup / company-specific clutter
        upper_tokens = re.findall(r"\b[A-Z]{2,5}\b", title_raw)
        if len(upper_tokens) >= 2:
            score -= 1.0

        # premia earnings solo se sembrano market-relevant
        if "earnings" in text or "guidance" in text:
            if contains_any(text, ["s&p", "nasdaq", "market", "stocks", "fed", "rates", "ai", "chip"]):
                score += 0.8
            else:
                score -= 0.8
    if ac != "Crypto":
        if any(t in text for t in ["bitcoin", "btc", "ethereum", "eth", "crypto", "xrp", "stablecoin", "token"]):
            score -= 1.5

    if ac == "Commodities":
        core = ["oil", "wti", "brent", "gold", "silver", "copper", "gas", "opec", "energy", "commodity"]
        if not any(t in text for t in core):
            score -= 1.8
    return score

def news_theme_key(ac: str, item: Dict[str, Any]) -> str:
    text = f"{safe_str(item.get('title'))} {safe_str(item.get('summary'))}".lower()

    if ac == "Equities":
        if contains_any(text, ["fed", "fomc", "rates", "yields", "inflation", "cpi", "ppi"]):
            return "macro-policy"
        if contains_any(text, ["earnings", "guidance", "eps"]):
            return "earnings"
        if contains_any(text, ["oil", "war", "geopolitical", "tariff"]):
            return "macro-shock"
        return "broad-market"

    if ac == "Rates":
        if contains_any(text, ["inflation", "cpi", "ppi"]):
            return "inflation"
        if contains_any(text, ["fed", "ecb", "boj", "boe", "policy", "rate cut", "rate hike"]):
            return "central-bank"
        return "yields"

    if ac == "FX":
        if contains_any(text, ["dollar", "dxy"]):
            return "usd"
        if contains_any(text, ["yen", "boj"]):
            return "jpy"
        if contains_any(text, ["euro", "ecb"]):
            return "eur"
        return "fx-macro"

    if ac == "Commodities":
        if contains_any(text, ["oil", "wti", "brent", "opec"]):
            return "energy"
        if contains_any(text, ["gold", "silver"]):
            return "precious"
        return "commodities-macro"

    if ac == "Crypto":
        if contains_any(text, ["etf", "sec", "regulation"]):
            return "regulation"
        return "crypto-market"

    return "other"

def editorial_score(ac: str, title: str, summary: str) -> float:
    text = f"{safe_str(title)} {safe_str(summary)}".lower()
    score = 0.0

    core = {
        "Equities": ["s&p", "nasdaq", "dow", "stocks", "equities", "shares", "earnings", "guidance", "wall street"],
        "Rates": ["fed", "fomc", "ecb", "boj", "boe", "yield", "yields", "treasury", "bond", "bonds", "inflation", "cpi", "ppi", "policy", "central bank"],
        "FX": ["usd", "dollar", "eur", "euro", "yen", "jpy", "gbp", "sterling", "fx", "currency", "currencies", "dxy", "foreign exchange"],
        "Commodities": ["oil", "wti", "brent", "gold", "silver", "copper", "gas", "natural gas", "opec", "commodity", "commodities", "energy",
                        "corn", "wheat", "soy", "soybean", "coffee", "cocoa", "sugar", "cotton", "cattle", "hog", "livestock"],
        "Crypto": ["bitcoin", "btc", "ethereum", "eth", "crypto", "etf", "token", "blockchain", "stablecoin", "xrp"]
    }

    macro = [
        "war", "conflict", "iran", "russia", "ukraine", "china",
        "inflation", "growth", "recession", "tariff", "tariffs", "trade",
        "sanctions", "rates", "policy", "supply", "demand"
    ]

    noise = [
        "should you buy", "what happens if", "opinion", "commentary",
        "analysis:", "stocks of the week", "how to invest", "retirement",
        "tax refund"
    ]

    micro = [
        "price target", "upgrades", "downgrades", "hold rating",
        "reiterates", "interim results", "six months results",
        "quarter results", "earnings report"
    ]

    if any(k in text for k in core.get(ac, [])):
        score += 2.0

    if any(k in text for k in macro):
        score += 1.2

    if any(k in text for k in noise):
        score -= 2.0

    if any(k in text for k in micro):
        score -= 1.2

    return score


def choose_display_news(ac: str, items: List[Dict[str, Any]], n: int = 3) -> List[Dict[str, Any]]:
    scored = []

    for it in items:
        score = editorial_score(
            ac,
            safe_str(it.get("title")),
            safe_str(it.get("summary"))
        )
        scored.append((score, it))

    scored.sort(
        key=lambda x: (
            x[0],
            safe_str(x[1].get("date") or x[1].get("published") or "")
        ),
        reverse=True
    )

    out = []
    used_title_starts = set()

    for score, it in scored:
        title = safe_str(it.get("title"))
        if not title:
            continue

        title_key = title[:50].lower()
        if title_key in used_title_starts:
            continue

        out.append({
            "title": it.get("title") or "",
            "source": it.get("source") or "",
            "date": it.get("date") or "",
            "url": it.get("url") or "",
            "summary": it.get("summary") or ""
        })

        used_title_starts.add(title_key)

        if len(out) >= n:
            break

    return out

def build_fundamentals(news_digest: Dict[str, Any], per_class_news: int = 3) -> Dict[str, Any]:
    by = news_digest.get('by_asset_class')
    ac_map: Dict[str, List[Dict[str, Any]]] = {}
    if isinstance(by, dict):
        for ac, lst in by.items():
            if isinstance(lst, list):
                ac_map[canonical_asset_class(ac)] = lst
    out = {'by_asset_class': {}}
    for ac, lst in ac_map.items():
        lst_sorted = sorted(lst, key=lambda it: safe_str(it.get('date') or it.get('published') or ''), reverse=True)
        if ac == 'FX':
            scores = [fx_row_bias_score(safe_str(it.get('title')), safe_str(it.get('summary'))) for it in lst_sorted]
        else:
            scores = [text_score(safe_str(it.get('title')), safe_str(it.get('summary'))) for it in lst_sorted]
        if scores:
            s_sum = sum(scores)
            bias = clamp(s_sum / max(8.0, 2.5 * len(scores)), -1.0, 1.0)
            mean = s_sum / len(scores)
            conf = clamp(0.35 + 0.35 * clamp(len(scores) / 8.0, 0, 1) + 0.30 * clamp(abs(mean) / 3.0, 0, 1), 0, 1)
        else:
            bias, conf = 0.0, 0.0
        tone = 'Supportive' if bias >= 0.25 else 'Cautious' if bias <= -0.25 else 'Mixed'
        top_news = choose_display_news(ac, lst_sorted, n=per_class_news)

        out['by_asset_class'][ac] = {
            'tone': tone,
            'bias': bias,
            'confidence': conf,
            'commentary': build_fund_commentary(ac, tone, bias, conf, top_news),
            'top_news': top_news,
            'macro_calendar': [],
            'earnings_calendar': [],
            'key_watchpoints': [],
            'key_events': [],
        }
    return out


def extract_universe_rows(screener: Dict[str, Any]) -> List[Dict[str, Any]]:
    for k in ['universe', 'rows', 'screener', 'data']:
        v = screener.get(k)
        if isinstance(v, list) and v:
            return v
    by_symbol = screener.get('by_symbol') or screener.get('bySymbol')
    if isinstance(by_symbol, dict):
        out = []
        for sym, row in by_symbol.items():
            if isinstance(row, dict):
                rr = dict(row)
                rr.setdefault('symbol', sym)
                out.append(rr)
        return out
    return []


def build_technical_overview(universe_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_symbol, by_ac = {}, {}
    for r in universe_rows:
        sym = r.get('symbol') or r.get('ticker') or r.get('instrument')
        if not sym:
            continue
        ac = canonical_asset_class(r.get('asset_class') or r.get('assetClass') or 'Other')
        display_symbol = fx_display_symbol(sym) if ac == 'FX' else str(sym).strip()
        display_score = fx_row_score(sym, r.get('score')) if ac == 'FX' else parse_float(r.get('score'))

        display_setup = r.get('setup') or ''
        
        row = {
            'symbol': display_symbol,
            'name': display_symbol if ac == 'FX' else (r.get('name') or ''),
            'asset_class': ac,
            'setup': display_setup,
            'score': display_score,
            'ret_20d_pct': r.get('ret_20d_%') if r.get('ret_20d_%') is not None else r.get('ret_20d_pct'),
            'ret_60d_pct': r.get('ret_60d_%') if r.get('ret_60d_%') is not None else r.get('ret_60d_pct'),
       }
        by_symbol[row['symbol']] = row
        by_ac.setdefault(ac, []).append(row)
    by_asset_class = {}
    for ac, rows in by_ac.items():
        breadth_rows = rows
        if ac == 'FX':
            breadth_rows = [
                rr for rr in rows
                if safe_str(rr.get('symbol')).strip().upper() not in {'DOLLAR INDEX', 'DX', 'DXY'}
                and safe_str(rr.get('name')).strip().upper() not in {'DOLLAR INDEX', 'DX', 'DXY'}
            ]

        scores = [parse_float(rr.get('score')) for rr in breadth_rows if parse_float(rr.get('score')) is not None]
        bullish = sum(1 for s in scores if s >= 2)
        bearish = sum(1 for s in scores if s <= -2)
        avg = sum(scores) / len(scores) if scores else 0.0
        tone = 'constructive' if avg >= 0.5 else 'defensive' if avg <= -0.5 else 'balanced'
        n_rows = len(breadth_rows)

        by_asset_class[ac] = {
            'n': n_rows,
            'avg_score': round(avg, 3),
            'bullish': bullish,
            'bearish': bearish,
            'text': f'Breadth is {tone}: avg score {avg:.2f}, bullish {bullish}/{n_rows}, bearish {bearish}/{n_rows}.',
        }

    return {'by_symbol': by_symbol, 'by_asset_class': by_asset_class}


def map_event_to_asset_classes(ev: Dict[str, Any]) -> List[str]:
    tags = ev.get('markets') or []
    out = []
    if isinstance(tags, list):
        for t in tags:
            ct = canonical_asset_class(t)
            if ct not in out:
                out.append(ct)
    return out


def short_event_label(title: str) -> str:
    t = safe_str(title).lower()
    mapping = [
        (r'\bcpi\b', 'CPI'),
        (r'\bnon[- ]farm payrolls?\b|\bpayrolls?\b', 'Payrolls'),
        (r'\bgdp\b', 'GDP'),
        (r'\bpmi\b', 'PMI'),
        (r'\bretail sales\b', 'Retail Sales'),
        (r'\bfomc\b|\bfed\b', 'Fed'),
        (r'\becb\b', 'ECB'),
    ]
    for p, lbl in mapping:
        if re.search(p, t):
            return lbl
    return safe_str(title)

def attach_events_to_fundamentals(fundamental_overview: Dict[str, Any], event_calendar: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_ac = fundamental_overview.get('by_asset_class', {})
    if not isinstance(by_ac, dict):
        return fundamental_overview

    for node in by_ac.values():
        node.setdefault('macro_calendar', [])
        node.setdefault('earnings_calendar', [])
        node.setdefault('key_watchpoints', [])
        node.setdefault('key_events', [])

    for ev in event_calendar:
        is_earnings = ev.get('type') == 'Earnings'
        title = safe_str(ev.get('title'))
        ticker = safe_str(ev.get('ticker'))
        company = safe_str(ev.get('company'))

        item = {
            'label': company if is_earnings and company else short_event_label(title),
            'title': title,
            'ticker': ticker,
            'company': company,
            'source': safe_str(ev.get('source')),
            'date': safe_str(ev.get('date')),
            'url': safe_str(ev.get('url')),
        }

        for ac in map_event_to_asset_classes(ev):
            if ac not in by_ac:
                continue
            if ev.get('type') == 'Macro':
                by_ac[ac]['macro_calendar'].append(item)
                by_ac[ac]['key_watchpoints'].append(item)
                by_ac[ac]['key_events'].append(item)
            elif is_earnings and ac == 'Equities':
                by_ac[ac]['earnings_calendar'].append(item)
                by_ac[ac]['key_watchpoints'].append(item)
                by_ac[ac]['key_events'].append(item)

    for node in by_ac.values():
        node['macro_calendar'] = node['macro_calendar'][:5]
        node['earnings_calendar'] = node['earnings_calendar'][:5]
        node['key_watchpoints'] = node['key_watchpoints'][:5]
        node['key_events'] = node['key_events'][:5]

    return fundamental_overview

def build_rationale(side: str, setup: str, asset_class: str, tone: str, symbol: str = '') -> str:
    s = setup_key(setup)

    if safe_str(symbol).upper() == 'VIX':
        if side == 'LONG':
            return 'Volatility expansion with risk-off dynamics.'
        else:
            return 'Volatility compression as risk sentiment stabilises.'

    if side == 'LONG':
        tech_text = 'Breakout setup' if 'breakout' in s else 'Trend strength' if 'trend continuation' in s else 'Constructive technical setup'
    else:
        tech_text = 'Breakdown setup' if 'breakdown' in s else 'Downtrend remains in place' if 'down' in s else 'Fragile technical setup'

    return f'{tech_text} in {asset_class} with {tone.lower()} macro tone.'


def build_top_ideas(universe_rows: List[Dict[str, Any]], fund_overview: Dict[str, Any], n_top: int = 3) -> Dict[str, Any]:
    fund_by = fund_overview.get('by_asset_class', {}) if isinstance(fund_overview, dict) else {}
    longs, shorts = [], []
    for r in universe_rows:
        sym = r.get('symbol') or r.get('ticker') or r.get('instrument')
        if not sym:
            continue
        ac = canonical_asset_class(r.get('asset_class') or r.get('assetClass') or 'Other')
        setup = safe_str(r.get('setup') or '')

        score = parse_float(r.get('score')) or 0.0
        score_for_top = fx_row_score(sym, score) if ac == 'FX' else score

        r_for_top = dict(r)
        r_for_top['score'] = score_for_top
        r_for_top['setup'] = setup

        tlong, tshort = tech_scores(r_for_top)
        f = fund_by.get(ac, {})
        bias = parse_float(f.get('bias')) or 0.0
        conf = parse_float(f.get('confidence')) or 0.0
        tone = safe_str(f.get('tone') or 'Mixed')
        fund = bias * conf
        f_long, f_short = clamp((fund + 1.0) / 2.0), clamp((-fund + 1.0) / 2.0)
        display_symbol = fx_display_symbol(sym) if ac == 'FX' else str(sym).strip()

        base = {
             'symbol': display_symbol,
             'name': display_symbol if ac == 'FX' else (r.get('name') or ''),
             'asset_class': ac,
             'setup': setup,
             'score': float(score_for_top),
             'ret_20d_pct': parse_float(r.get('ret_20d_%')) if r.get('ret_20d_%') is not None else parse_float(r.get('ret_20d_pct')),
        }
        longs.append({
            **base,
            'side': 'LONG',
            'tech_score': tlong,
            'fund_support': f_long,
            'macro_alignment': f_long,
            'final_score': 0.75 * tlong + 0.25 * f_long,
            'rationale': build_rationale('LONG', setup, ac, tone, display_symbol),
        })
        shorts.append({
            **base,
            'side': 'SHORT',
            'tech_score': tshort,
            'fund_support': f_short,
            'macro_alignment': f_short,
            'final_score': 0.75 * tshort + 0.25 * f_short,
            'rationale': build_rationale('SHORT', setup, ac, tone, display_symbol),
        })
    return {
        'methodology': {
            'final': {'tech_weight': 0.75, 'fund_weight': 0.25},
            'no_gating': True,
            'no_event_tilt': True,
        },
        'top_long': sorted(longs, key=lambda x: x['final_score'], reverse=True)[:n_top],
        'top_short': sorted(shorts, key=lambda x: x['final_score'], reverse=True)[:n_top],
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--screener', default='content/site_screener.json')
    p.add_argument('--news', default='content/news_digest.json')
    p.add_argument('--out', default='content/site_weekly.json')
    p.add_argument('--history_dir', default='content/history/weeklies')
    p.add_argument('--per_class_news', type=int, default=3)
    p.add_argument('--top_n', type=int, default=3)
    p.add_argument('--asof', default='')
    args = p.parse_args()

    asof_d = datetime.strptime(args.asof.strip(), '%Y-%m-%d').date() if args.asof.strip() else date.today()
    screener = read_json(args.screener)
    news_digest = read_json(args.news) if os.path.exists(args.news) else {}

    universe_rows = extract_universe_rows(screener)
    technical_overview = build_technical_overview(universe_rows) if universe_rows else {'by_symbol': {}, 'by_asset_class': {}}
    fundamental_overview = build_fundamentals(news_digest, per_class_news=args.per_class_news) if news_digest else {'by_asset_class': {}}
    event_calendar = build_event_calendar(asof_d, asof_d + timedelta(days=7))
    fundamental_overview = attach_events_to_fundamentals(fundamental_overview, event_calendar)
    top_ideas = build_top_ideas(universe_rows, fundamental_overview, n_top=args.top_n) if universe_rows else {
        'methodology': {
            'final': {'tech_weight': 0.75, 'fund_weight': 0.25},
            'no_gating': True,
            'no_event_tilt': True,
        },
        'top_long': [],
        'top_short': [],
    }

    weekly = {
        'asof': asof_d.isoformat(),
        'week_id': iso_week_id(asof_d),
        'technical_overview': technical_overview,
        'fundamental_overview': fundamental_overview,
        'top_ideas': top_ideas,
        'event_calendar': event_calendar,
    }

    write_json(args.out, weekly)
    os.makedirs(args.history_dir, exist_ok=True)
    hist = os.path.join(args.history_dir, f"{iso_week_id(asof_d)}.json")
    write_json(hist, weekly)

    # ---- rebuild weekly index ----
    week_files = glob.glob(os.path.join(args.history_dir, '*-W*.json'))

    weeks = []
    for wf in week_files:
        try:
            with open(wf, 'r', encoding='utf-8') as f:
                data = json.load(f)

            wid = data.get('week_id')
            if not wid:
                continue

            weeks.append({
                'id': wid,
                'asof': data.get('asof'),
                'path': f"content/history/weeklies/{os.path.basename(wf)}",
            })
        except Exception:
            continue

    weeks = sorted(weeks, key=lambda x: x['id'], reverse=True)

    index = {
        'current': iso_week_id(asof_d),
        'weeks': weeks,
    }

    index_path = os.path.join(args.history_dir, 'index.json')
    write_json(index_path, index)

    print(f"✅ Wrote index: {index_path}")
    print(f"✅ Wrote: {args.out}")
    print(f"✅ Wrote: {hist}")
    print(
        f"   Instruments: {len(technical_overview.get('by_symbol', {}))} | "
        f"Tech AC: {len(technical_overview.get('by_asset_class', {}))} | "
        f"Fund AC: {len(fundamental_overview.get('by_asset_class', {}))} | "
        f"Events: {len(event_calendar)}"
    )
    print(f"   Top ideas: {len(top_ideas.get('top_long', []))} long / {len(top_ideas.get('top_short', []))} short")


if __name__ == '__main__':
    main()
