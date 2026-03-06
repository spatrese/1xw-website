import argparse
import json
import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple


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
    return "" if x is None else str(x)


def parse_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace('%', '')
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def normalize_score_long(score: float) -> float:
    return clamp((score + 4.0) / 8.0)


def normalize_score_short(score: float) -> float:
    return clamp((-score + 4.0) / 8.0)


SETUP_BONUS_LONG = {
    'breakout': 0.10,
    'trend continuation': 0.05,
    'trend continuation (down)': -0.05,
    'neutral': 0.00,
    'mean reversion (bounce)': -0.03,
    'mean reversion (pullback)': -0.03,
    'breakdown': -0.05,
}
SETUP_BONUS_SHORT = {
    'breakdown': 0.10,
    'trend continuation (down)': 0.05,
    'trend continuation': 0.05,
    'neutral': 0.00,
    'mean reversion (bounce)': -0.03,
    'mean reversion (pullback)': -0.03,
    'breakout': -0.05,
}


POS_WORDS = [
    'rally', 'gain', 'surge', 'rebound', 'beat', 'upgrade', 'cuts', 'cut', 'easing', 'dovish',
    'disinflation', 'cooling inflation', 'soft landing', 'stimulus', 'expansion', 'strong demand',
    'risk-on', 'optimism', 'upside', 'record high', 'growth', 'supportive', 'resilient',
]
NEG_WORDS = [
    'selloff', 'drop', 'plunge', 'crash', 'miss', 'downgrade', 'hikes', 'hike', 'tightening', 'hawkish',
    'inflation rises', 'sticky inflation', 'recession', 'slowdown', 'war', 'conflict', 'sanctions',
    'risk-off', 'warning', 'defaults', 'stress', 'shock', 'downside', 'weakness', 'pressure',
]

MACRO_PATTERNS: List[Tuple[str, str]] = [
    (r'\bcpi\b|consumer price', 'CPI release'),
    (r'\bppi\b|producer price', 'PPI release'),
    (r'payroll|non-farm', 'US payrolls'),
    (r'unemployment|jobless', 'Labor market data'),
    (r'\bgdp\b', 'GDP data'),
    (r'\bpmi\b|\bism\b', 'PMI / activity survey'),
    (r'\bfed\b|\bfomc\b|powell', 'Fed communication'),
    (r'\becb\b|lagarde', 'ECB communication'),
    (r'\bboj\b', 'BoJ communication'),
    (r'\bboe\b', 'BoE communication'),
    (r'treasury auction', 'Treasury auction'),
    (r'minutes', 'Central bank minutes'),
    (r'opec', 'OPEC-related headline'),
]

EARNINGS_PATTERNS: List[Tuple[str, str]] = [
    (r'earnings|results|guidance|eps|revenue|profit|quarter|q1|q2|q3|q4', 'Earnings / results'),
]

COMMENTARY_TEMPLATES = {
    'Equities': {
        'Supportive': 'Risk appetite remains constructive, with headline flow supportive for broad equity sentiment even if leadership may stay selective.',
        'Mixed': 'Equity signals are balanced, with supportive pockets offset by a still uneven macro backdrop and selective leadership.',
        'Cautious': 'Headline flow points to a more defensive equity backdrop, with macro sensitivity likely to dominate stock-specific optimism.',
    },
    'Rates': {
        'Supportive': 'Duration is seeing a friendlier backdrop as inflation and policy headlines lean less restrictive than before.',
        'Mixed': 'Rates remain data-dependent, with no clean trend yet as inflation and policy communication pull expectations in both directions.',
        'Cautious': 'Rates headlines still argue for caution on duration, as policy and inflation risks keep the market sensitive to repricing.',
    },
    'FX': {
        'Supportive': 'Currency flows look more supportive for directional opportunities as policy divergence remains a live driver.',
        'Mixed': 'FX remains range-prone, with cross-currents from policy, growth and risk sentiment keeping conviction moderate.',
        'Cautious': 'FX tone is defensive, with headline flow favoring safe-haven or carry-unwind dynamics over broad risk-taking.',
    },
    'Commodities': {
        'Supportive': 'Commodity tone is constructive, with supply and macro headlines broadly consistent with firmer price action.',
        'Mixed': 'Commodity signals are mixed, with demand uncertainty offsetting pockets of supply support across markets.',
        'Cautious': 'Commodity headlines argue for a more defensive stance, as growth concerns and volatile macro inputs cloud conviction.',
    },
    'Crypto': {
        'Supportive': 'Crypto sentiment remains constructive, supported by a generally favorable flow backdrop and improving risk appetite.',
        'Mixed': 'Crypto tone is balanced, with supportive narratives still competing against policy and positioning uncertainty.',
        'Cautious': 'Crypto remains vulnerable to swings in regulation, liquidity and broader risk sentiment, keeping the tone defensive.',
    },
}


def setup_key(s: str) -> str:
    return safe_str(s).strip().lower()


def momentum_assist(ret_20d_pct: Optional[float], side: str) -> float:
    if ret_20d_pct is None:
        return 0.0
    if side == 'long':
        return clamp(ret_20d_pct / 10.0, -0.10, 0.10)
    return clamp((-ret_20d_pct) / 10.0, -0.10, 0.10)


def tech_scores(row: Dict[str, Any]) -> Tuple[float, float]:
    score = parse_float(row.get('score'))
    if score is None:
        score = 0.0

    sk = setup_key(row.get('setup', ''))
    base_long = normalize_score_long(score)
    base_short = normalize_score_short(score)

    bonus_long = SETUP_BONUS_LONG.get(sk, 0.0)
    bonus_short = SETUP_BONUS_SHORT.get(sk, 0.0)

    r20 = parse_float(row.get('ret_20d_%'))
    if r20 is None:
        r20 = parse_float(row.get('ret_20d_pct'))
    mom_long = momentum_assist(r20, 'long')
    mom_short = momentum_assist(r20, 'short')

    tlong = clamp(base_long + bonus_long + mom_long, 0.0, 1.0)
    tshort = clamp(base_short + bonus_short + mom_short, 0.0, 1.0)
    return tlong, tshort


def text_score(title: str, summary: str) -> int:
    txt = f"{title} {summary}".lower()
    pos = sum(1 for w in POS_WORDS if w in txt)
    neg = sum(1 for w in NEG_WORDS if w in txt)
    return pos - neg


def detect_event_type(title: str, summary: str) -> Optional[str]:
    txt = f"{title} {summary}".lower()
    for pattern, _ in MACRO_PATTERNS:
        if re.search(pattern, txt, flags=re.IGNORECASE):
            return 'macro'
    for pattern, _ in EARNINGS_PATTERNS:
        if re.search(pattern, txt, flags=re.IGNORECASE):
            return 'earnings'
    return None


def simplify_event_label(title: str, summary: str, event_type: str) -> str:
    txt = f"{title} {summary}".lower()
    patterns = MACRO_PATTERNS if event_type == 'macro' else EARNINGS_PATTERNS
    for pattern, label in patterns:
        if re.search(pattern, txt, flags=re.IGNORECASE):
            if event_type == 'earnings':
                # try to preserve company / subject from title if present
                cleaned = re.sub(r'\s+', ' ', safe_str(title)).strip()
                cleaned = re.sub(r'\s*[\-–—|:].*$', '', cleaned).strip()
                return cleaned if cleaned else label
            return label
    return re.sub(r'\s+', ' ', safe_str(title)).strip() or ('Macro event' if event_type == 'macro' else 'Earnings event')


def unique_compact(items: List[Dict[str, Any]], key_fields: Tuple[str, ...]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        key = tuple(safe_str(it.get(k)).strip().lower() for k in key_fields)
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def build_commentary(asset_class: str, tone: str, bias: float, confidence: float, top_news: List[Dict[str, Any]]) -> str:
    first = COMMENTARY_TEMPLATES.get(asset_class, {}).get(tone)
    if not first:
        first = COMMENTARY_TEMPLATES.get('Equities', {}).get(tone, 'Market tone remains mixed across recent headlines.')

    source_names = [safe_str(n.get('source')).strip() for n in top_news if safe_str(n.get('source')).strip()]
    source_names = list(dict.fromkeys(source_names))[:2]
    source_text = f"Key flow is coming from {', '.join(source_names)}." if source_names else 'Recent headlines remain the main driver.'

    if confidence >= 0.75:
        conf_text = 'Conviction is relatively high for this week.'
    elif confidence >= 0.55:
        conf_text = 'Conviction is moderate.'
    else:
        conf_text = 'Conviction remains limited and should be monitored closely.'

    if bias >= 0.20:
        bias_text = 'Headline balance is tilted positively.'
    elif bias <= -0.20:
        bias_text = 'Headline balance is tilted defensively.'
    else:
        bias_text = 'Headline balance is broadly neutral.'

    return f"{first} {bias_text} {conf_text} {source_text}"


def build_fundamentals(news_digest: Dict[str, Any], per_class_news: int = 3, per_class_events: int = 2) -> Dict[str, Any]:
    by = news_digest.get('by_asset_class')
    items_flat = news_digest.get('items')

    ac_map: Dict[str, List[Dict[str, Any]]] = {}
    if isinstance(by, dict):
        for ac, lst in by.items():
            if isinstance(lst, list):
                ac_map[str(ac)] = lst
    elif isinstance(items_flat, list):
        for it in items_flat:
            ac = it.get('asset_class') or it.get('assetClass') or 'General'
            ac_map.setdefault(str(ac), []).append(it)

    out: Dict[str, Any] = {'by_asset_class': {}}

    for ac, lst in ac_map.items():
        def sort_key(it: Dict[str, Any]) -> str:
            return safe_str(it.get('date') or it.get('published') or it.get('pubDate') or '')

        lst_sorted = sorted(lst, key=sort_key, reverse=True)
        scores = [
            text_score(safe_str(it.get('title') or it.get('headline') or ''), safe_str(it.get('summary') or it.get('description') or ''))
            for it in lst_sorted
        ]

        if scores:
            s_sum = sum(scores)
            bias = clamp(s_sum / max(8.0, 2.5 * len(scores)), -1.0, 1.0)
            mean = s_sum / len(scores)
            coherence = clamp(abs(mean) / 3.0, 0.0, 1.0)
            count_factor = clamp(len(scores) / 8.0, 0.0, 1.0)
            confidence = clamp(0.35 + 0.35 * count_factor + 0.30 * coherence, 0.0, 1.0)
        else:
            bias = 0.0
            confidence = 0.0

        if bias >= 0.25:
            tone = 'Supportive'
        elif bias <= -0.25:
            tone = 'Cautious'
        else:
            tone = 'Mixed'

        top_news: List[Dict[str, Any]] = []
        macro_calendar: List[Dict[str, Any]] = []
        earnings_calendar: List[Dict[str, Any]] = []

        for it in lst_sorted:
            title = safe_str(it.get('title') or it.get('headline') or '').strip()
            summary = safe_str(it.get('summary') or it.get('description') or '').strip()
            source = safe_str(it.get('source') or it.get('feed') or '').strip()
            date_value = safe_str(it.get('date') or it.get('published') or it.get('pubDate') or '').strip()
            url = safe_str(it.get('url') or it.get('link') or '').strip()
            if not title:
                continue

            if len(top_news) < per_class_news:
                top_news.append({
                    'title': title,
                    'source': source,
                    'date': date_value,
                    'url': url,
                    'summary': summary,
                })

            event_type = detect_event_type(title, summary)
            if event_type == 'macro' and len(macro_calendar) < per_class_events:
                macro_calendar.append({
                    'label': simplify_event_label(title, summary, 'macro'),
                    'title': title,
                    'source': source,
                    'date': date_value,
                    'url': url,
                })
            elif event_type == 'earnings' and len(earnings_calendar) < per_class_events:
                earnings_calendar.append({
                    'label': simplify_event_label(title, summary, 'earnings'),
                    'title': title,
                    'source': source,
                    'date': date_value,
                    'url': url,
                })

        top_news = unique_compact(top_news, ('title', 'source'))[:per_class_news]
        macro_calendar = unique_compact(macro_calendar, ('label',))[:per_class_events]
        earnings_calendar = unique_compact(earnings_calendar, ('label',))[:per_class_events]

        key_watchpoints = (macro_calendar + earnings_calendar)[:2]
        commentary = build_commentary(ac, tone, bias, confidence, top_news)

        out['by_asset_class'][ac] = {
            'tone': tone,
            'bias': round(float(bias), 3),
            'confidence': round(float(confidence), 3),
            'commentary': commentary,
            'top_news': top_news,
            'macro_calendar': macro_calendar,
            'earnings_calendar': earnings_calendar,
            'key_watchpoints': key_watchpoints,
            'key_events': key_watchpoints,
        }

    return out


def extract_universe_rows(screener: Dict[str, Any]) -> List[Dict[str, Any]]:
    for k in ['universe', 'rows', 'screener', 'data']:
        v = screener.get(k)
        if isinstance(v, list) and v:
            return v

    by_symbol = screener.get('by_symbol') or screener.get('bySymbol')
    if isinstance(by_symbol, dict) and by_symbol:
        out = []
        for sym, row in by_symbol.items():
            if isinstance(row, dict):
                rr = dict(row)
                rr.setdefault('symbol', sym)
                out.append(rr)
        return out
    return []


def build_technical_overview(universe_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_symbol: Dict[str, Any] = {}
    by_ac: Dict[str, List[Dict[str, Any]]] = {}

    for r in universe_rows:
        sym = r.get('symbol') or r.get('ticker') or r.get('instrument')
        if not sym:
            continue
        sym = str(sym).strip()
        ac = str(r.get('asset_class') or r.get('assetClass') or 'Other').strip()

        row = {
            'symbol': sym,
            'name': r.get('name') or '',
            'asset_class': ac,
            'setup': r.get('setup') or '',
            'score': r.get('score'),
            'ret_20d_pct': r.get('ret_20d_%') if r.get('ret_20d_%') is not None else r.get('ret_20d_pct'),
            'ret_60d_pct': r.get('ret_60d_%') if r.get('ret_60d_%') is not None else r.get('ret_60d_pct'),
        }
        by_symbol[sym] = row
        by_ac.setdefault(ac, []).append(row)

    by_asset_class: Dict[str, Any] = {}
    for ac, rows in by_ac.items():
        scores = []
        bullish = 0
        bearish = 0
        for rr in rows:
            s = parse_float(rr.get('score'))
            if s is None:
                continue
            scores.append(s)
            if s >= 2:
                bullish += 1
            if s <= -2:
                bearish += 1
        avg = (sum(scores) / len(scores)) if scores else 0.0
        n = len(rows)
        balance = 'balanced'
        if bullish > bearish:
            balance = 'constructive'
        elif bearish > bullish:
            balance = 'defensive'
        text = f"Breadth is {balance}: avg score {avg:.2f}, bullish {bullish}/{n}, bearish {bearish}/{n}."
        by_asset_class[ac] = {
            'n': n,
            'avg_score': round(float(avg), 3),
            'bullish': int(bullish),
            'bearish': int(bearish),
            'text': text,
        }
    return {'by_symbol': by_symbol, 'by_asset_class': by_asset_class}


def build_top_ideas(universe_rows: List[Dict[str, Any]], fund_overview: Dict[str, Any], n_top: int = 3) -> Dict[str, Any]:
    fund_by = (fund_overview or {}).get('by_asset_class', {}) if isinstance(fund_overview, dict) else {}
    longs: List[Dict[str, Any]] = []
    shorts: List[Dict[str, Any]] = []

    for r in universe_rows:
        sym = r.get('symbol') or r.get('ticker') or r.get('instrument')
        if not sym:
            continue
        sym = str(sym).strip()
        ac = str(r.get('asset_class') or r.get('assetClass') or 'Other').strip()
        setup = safe_str(r.get('setup') or '')
        score = parse_float(r.get('score'))
        if score is None:
            score = 0.0

        tlong, tshort = tech_scores(r)

        f = fund_by.get(ac, {})
        bias = parse_float(f.get('bias')) or 0.0
        confidence = parse_float(f.get('confidence')) or 0.0
        fund_signal = bias * confidence
        fund_support_long = clamp((fund_signal + 1.0) / 2.0)
        fund_support_short = clamp((-fund_signal + 1.0) / 2.0)

        final_long = 0.75 * tlong + 0.25 * fund_support_long
        final_short = 0.75 * tshort + 0.25 * fund_support_short

        r20 = parse_float(r.get('ret_20d_%'))
        if r20 is None:
            r20 = parse_float(r.get('ret_20d_pct'))

        base = {
            'symbol': sym,
            'name': r.get('name') or '',
            'asset_class': ac,
            'setup': setup,
            'score': float(score),
            'ret_20d_pct': r20,
        }

        longs.append({
            **base,
            'side': 'LONG',
            'tech_score': round(float(tlong), 4),
            'fund_support': round(float(fund_support_long), 4),
            'macro_alignment': round(float(fund_support_long), 4),
            'final_score': round(float(final_long), 4),
            'rationale': f"Technical strength in {sym} is supported by the current weekly macro tone for {ac}."
        })
        shorts.append({
            **base,
            'side': 'SHORT',
            'tech_score': round(float(tshort), 4),
            'fund_support': round(float(fund_support_short), 4),
            'macro_alignment': round(float(fund_support_short), 4),
            'final_score': round(float(final_short), 4),
            'rationale': f"Technical downside in {sym} is supported by the current weekly macro tone for {ac}."
        })

    longs_sorted = sorted(longs, key=lambda x: x['final_score'], reverse=True)[:n_top]
    shorts_sorted = sorted(shorts, key=lambda x: x['final_score'], reverse=True)[:n_top]

    return {
        'methodology': {
            'final': {'tech_weight': 0.75, 'fund_weight': 0.25},
            'no_gating': True,
            'no_event_tilt': True,
        },
        'top_long': longs_sorted,
        'top_short': shorts_sorted,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--screener', default='content/site_screener.json')
    parser.add_argument('--news', default='content/news_digest.json')
    parser.add_argument('--out', default='content/site_weekly.json')
    parser.add_argument('--history_dir', default='content/history/weeklies')
    parser.add_argument('--per_class_news', type=int, default=3)
    parser.add_argument('--per_class_events', type=int, default=2)
    parser.add_argument('--top_n', type=int, default=3)
    parser.add_argument('--asof', default='')
    args = parser.parse_args()

    asof_d = datetime.strptime(args.asof.strip(), '%Y-%m-%d').date() if args.asof.strip() else datetime.today().date()
    asof = asof_d.isoformat()
    week_id = iso_week_id(asof_d)

    if not os.path.exists(args.screener):
        raise FileNotFoundError(f'Missing {args.screener}')
    screener = read_json(args.screener)
    news_digest = read_json(args.news) if os.path.exists(args.news) else {}

    universe_rows = extract_universe_rows(screener)
    technical_overview = build_technical_overview(universe_rows) if universe_rows else {'by_symbol': {}, 'by_asset_class': {}}
    fundamental_overview = build_fundamentals(news_digest, per_class_news=args.per_class_news, per_class_events=args.per_class_events) if news_digest else {'by_asset_class': {}}
    top_ideas = build_top_ideas(universe_rows, fundamental_overview, n_top=args.top_n) if universe_rows else {
        'methodology': {'final': {'tech_weight': 0.75, 'fund_weight': 0.25}, 'no_gating': True, 'no_event_tilt': True},
        'top_long': [],
        'top_short': [],
    }

    weekly = {
        'asof': asof,
        'week_id': week_id,
        'technical_overview': technical_overview,
        'fundamental_overview': fundamental_overview,
        'top_ideas': top_ideas,
    }

    write_json(args.out, weekly)
    os.makedirs(args.history_dir, exist_ok=True)
    hist_path = os.path.join(args.history_dir, f'{week_id}.json')
    write_json(hist_path, weekly)

    n_inst = len(technical_overview.get('by_symbol', {}) or {})
    n_ac_t = len(technical_overview.get('by_asset_class', {}) or {})
    n_ac_f = len(fundamental_overview.get('by_asset_class', {}) or {})
    print(f'✅ Wrote: {args.out}')
    print(f'✅ Wrote: {hist_path}')
    print(f'   Instruments: {n_inst} | Tech AC: {n_ac_t} | Fund AC: {n_ac_f}')
    print(f"   Top ideas: {len(top_ideas.get('top_long', []))} long / {len(top_ideas.get('top_short', []))} short")


if __name__ == '__main__':
    main()
