from __future__ import annotations

import argparse, base64, html, json, re
from pathlib import Path
from typing import Any


def esc(v: Any) -> str:
    return html.escape(str(v if v is not None else ""))


def signed(v: Any, digits: int = 1) -> str:
    try:
        n = float(v)
    except Exception:
        return "—"
    return f"{n:+.{digits}f}" if n != 0 else f"{n:.{digits}f}"


def pct(v: Any, digits: int = 1) -> str:
    try:
        n = float(v)
    except Exception:
        return "—"
    return f"{n:+.{digits}f}%" if n != 0 else f"{n:.{digits}f}%"


def score_pct(v: Any) -> str:
    try:
        return str(int(round(float(v) * 100)))
    except Exception:
        return "—"


def class_for_num(v: Any) -> str:
    try:
        n = float(v)
    except Exception:
        return ""
    return "num-pos" if n > 0 else ("num-neg" if n < 0 else "")


def tone_class(v: Any) -> str:
    x = str(v or "").lower()
    if "support" in x:
        return "supportive"
    if any(k in x for k in ["cautious", "fragile", "negative"]):
        return "cautious"
    return "mixed"


def first_sentence(text: Any) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    i = s.find(". ")
    return s[: i + 1] if i > -1 else s


def asset_state(row: dict[str, Any]) -> tuple[str, str]:
    try:
        avg = float(row.get("avg_score"))
    except Exception:
        return "Mixed", "mix"
    if avg >= 1:
        return "Positive", "pos"
    if avg <= -1:
        return "Weak", "neg"
    return "Mixed", "mix"


def build_idea_rationale(item: dict[str, Any]) -> str:
    parts = []
    if item.get("setup"):
        parts.append(str(item["setup"]))
    try:
        r20 = float(item.get("ret_20d_pct"))
        parts.append(f"{r20:+.1f}% 20d")
    except Exception:
        pass
    if item.get("rationale"):
        parts.append(first_sentence(item["rationale"]))
    return " • ".join(p for p in parts if p)


def pretty_date(v: Any) -> str:
    s = str(v or "")
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if not m:
        return "Watch"
    _, mo, d = map(int, m.groups())
    mons = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return f"{d:02d} {mons[mo-1]}"


def data_uri(path: Path) -> str:
    mime = "image/png"
    if path.suffix.lower() in {".jpg", ".jpeg"}:
        mime = "image/jpeg"
    return f"data:{mime};base64," + base64.b64encode(path.read_bytes()).decode("ascii")


def td(inner: str, cls: str = "", wrap: bool = False) -> str:
    c = f' class="{cls}{" wrap" if wrap else ""}"' if (cls or wrap) else ""
    return f"<td{c}>{inner}</td>"


def ths(cols: list[str]) -> str:
    return "<thead><tr>" + "".join(f"<th>{esc(c)}</th>" for c in cols) + "</tr></thead>"


def table_html(classes: str, cols: list[str], body_rows: list[str]) -> str:
    body = "".join(body_rows) if body_rows else '<tr><td colspan="99" class="empty-cell">No data available.</td></tr>'
    return f'<table class="pdf-table {classes}">{ths(cols)}<tbody>{body}</tbody></table>'


def render_top_ideas(data: dict[str, Any]) -> str:
    top = data.get("top_ideas", {})
    longs = list(top.get("top_long") or [])[:3]
    shorts = list(top.get("top_short") or [])[:3]

    def fmt_pct_score(x: Any) -> str:
        try:
            return str(int(round(float(x) * 100)))
        except Exception:
            return "—"

    rows = []

    for item in [*[{**x, "_side": "LONG"} for x in longs], *[{**x, "_side": "SHORT"} for x in shorts]]:
        side = item["_side"]
        side_class = "long" if side == "LONG" else "short"
        tone = "tone supportive" if side_class == "long" else "tone cautious"

        final_score = fmt_pct_score(item.get("final_score"))
        tech_score = fmt_pct_score(item.get("tech_score"))
        macro_score = fmt_pct_score(item.get("macro_alignment", item.get("fund_support")))

        rows.append(
            "<tr>"
            + td(f'<span class="label">{esc(side)} {esc(item.get("name") or "—")}</span>')
            + td(esc(item.get("setup") or "—"))
            + td(f'<span class="pill {tone}">{final_score}</span>')
            + td(f'<span class="pill">{tech_score}</span>')
            + td(f'<span class="pill side {side_class}">{macro_score}</span>')
            + td(esc(build_idea_rationale(item) or "No specific rationale available."), wrap=True)
            + "</tr>"
        )

    return table_html(
        "ideas-table",
        ["Instrument", "Setup", "Final", "Tech", "Macro", "Rationale"],
        rows
    )

def render_breadth(data: dict[str, Any]) -> str:
    rows = []
    for ac, row in (data.get("technical_overview", {}).get("by_asset_class", {}) or {}).items():
        state, cls = asset_state(row)
        rows.append(
            "<tr>"
            + td(f'<span class="label">{esc(ac)}</span>')
            + td(f'avg {esc(signed(row.get("avg_score"), 1))}')
            + td(f'bull {esc(row.get("bullish", "—"))}')
            + td(f'bear {esc(row.get("bearish", "—"))}')
            + td(f'<span class="pill state {cls}">{esc(state)}</span>')
            + td(esc(first_sentence(row.get("text") or "")), wrap=True)
            + "</tr>"
        )
    return table_html("breadth-table", ["Asset Class", "Avg Score", "Bull", "Bear", "State", "Summary"], rows)


def render_tone(data: dict[str, Any]) -> str:
    rows = []
    for ac, row in (data.get("fundamental_overview", {}).get("by_asset_class", {}) or {}).items():
        rows.append(
            "<tr>"
            + td(f'<span class="label">{esc(ac)}</span>')
            + td(f'<span class="pill tone {tone_class(row.get("tone"))}">{esc(row.get("tone") or "Mixed")}</span>')
            + td(esc(first_sentence(row.get("commentary") or "")), wrap=True)
            + "</tr>"
        )
    return table_html("tone-table", ["Asset Class", "Tone", "Commentary"], rows)


def render_tech_overview(data: dict[str, Any]) -> str:
    by_symbol = data.get("technical_overview", {}).get("by_symbol", {}) or {}
    rows_data = sorted(by_symbol.values(), key=lambda r: float(r.get("score") or 0), reverse=True)
    rows = []
    for row in rows_data:
        rows.append(
            "<tr>"
            + td(f'<span class="label">{esc(row.get("name") or row.get("symbol") or "")}</span>')
            + td(esc(row.get("asset_class") or ""))
            + td(esc(row.get("setup") or ""), wrap=True)
            + td(f'<span class="{class_for_num(row.get("score"))}">{esc(signed(row.get("score"), 0))}</span>')
            + td(f'<span class="{class_for_num(row.get("ret_20d_pct"))}">{esc(pct(row.get("ret_20d_pct"), 1))}</span>')
            + td(f'<span class="{class_for_num(row.get("ret_60d_pct"))}">{esc(pct(row.get("ret_60d_pct"), 1))}</span>')
            + "</tr>"
        )
    return table_html("tech-table", ["Instrument", "Asset Class", "Setup", "Score", "20d", "60d"], rows)


def render_fund_overview(data: dict[str, Any]) -> str:
    rows = []
    for ac, row in (data.get("fundamental_overview", {}).get("by_asset_class", {}) or {}).items():
        headlines = [str(n.get("title") or "").strip() for n in (row.get("top_news") or [])[:2] if str(n.get("title") or "").strip()]
        parts = [first_sentence(row.get("commentary") or "")]
        if headlines:
            parts.append(" • ".join(headlines))
        rows.append(
            "<tr>"
            + td(f'<span class="label">{esc(ac)}</span>')
            + td(f'<span class="pill tone {tone_class(row.get("tone"))}">{esc(row.get("tone") or "Mixed")}</span>')
            + td(f'bias {esc(signed(row.get("bias"), 2))} · conf {esc(score_pct(row.get("confidence")))}')
            + td(esc(" — ".join(p for p in parts if p)), wrap=True)
            + "</tr>"
        )
    return table_html("fund-table", ["Asset Class", "Tone", "Bias / Conf", "Commentary / Headlines"], rows)


def render_events(data: dict[str, Any]) -> str:
    rows = []

    for e in data.get("event_calendar") or []:
        title = str(e.get("title") or "").strip()
        ticker = str(e.get("ticker") or "").strip()
        company = str(e.get("company") or "").strip()
        etype = str(e.get("type") or "")

        if etype.lower() == "earnings" and company:
            title = f"{company} ({ticker})" if ticker else company
        elif ticker and ticker != title and etype.lower() == "earnings":
            title = f"{title} ({ticker})"

        src = str(e.get("source") or "").strip()
        markets = e.get("markets") or e.get("assets") or []

        market_html = (
            '<div class="inline-tags">'
            + "".join(f'<span class="pill">{esc(m)}</span>' for m in markets)
            + "</div>"
        )

        event_html = (
            f'<div class="event-title">{esc(title)}</div>'
            + (f'<div class="event-src">{esc(src)}</div>' if src else "")
        )

        tone = "tone mixed" if etype.lower() == "earnings" else ""

        rows.append(
            "<tr>"
            + td(esc(pretty_date(e.get("date"))))
            + td(f'<span class="pill {tone}">{esc(etype)}</span>')
            + td(event_html, wrap=True)
            + td(market_html, wrap=True)
            + "</tr>"
        )

    return table_html(
        "events-table",
        ["Date", "Type", "Event", "Markets"],
        rows,
    )

def panel(kicker: str, title: str, subtitle: str, inner: str, extra_class: str = "") -> str:
    return (
        f'<section class="section {extra_class}"><div class="panel"><div class="panel-head">'
        f'<div class="kicker">{esc(kicker)}</div><h2>{esc(title)}</h2><p>{esc(subtitle)}</p>'
        f'</div><div class="table-wrap">{inner}</div></div></section>'
    )


def build_html(data: dict[str, Any], css_theme: str, css_components: str, css_bloomberg: str, logo_uri: str) -> str:
    disclaimer = (
        "The information contained in this report is provided for informational and educational purposes only and does not constitute investment advice, an offer, or a solicitation to buy or sell any financial instrument. "
        "The 1XW model is a systematic research framework based on technical and macro inputs and may change without notice. Past performance and historical signals are not indicative of future results. "
        "Trading financial instruments involves significant risk and may not be suitable for all investors. Users are solely responsible for their investment decisions and should consult qualified advisers where appropriate. "
        "This material is intended to be distributed only in its original form."
    )
    week_id = str(data.get("week_id") or "—")
    asof = str(data.get("asof") or "—")
    ranking = "Ranking: 75% tech / 25% macro"
    try:
        tech_weight = data.get("top_ideas", {}).get("methodology", {}).get("final", {}).get("tech_weight")
        fund_weight = data.get("top_ideas", {}).get("methodology", {}).get("final", {}).get("fund_weight")
        ranking = f"Ranking: {round(float(tech_weight) * 100)}% tech / {round(float(fund_weight) * 100)}% macro"
    except Exception:
        pass

    extra_css = f"""
@page {{ size: A4 portrait; margin: 0; }}
html, body {{ margin:0 !important; padding:0 !important; background:#000 !important; }}
body {{ font-family:'IBM Plex Sans','Inter',sans-serif !important; color:var(--text); }}
main.container {{ max-width:none !important; width:100% !important; padding:0 !important; margin:0 !important; }}
.page {{ min-height:297mm; width:210mm; box-sizing:border-box; padding:14mm 14mm 12mm 14mm; background:#000; page-break-after:always; overflow:hidden; }}
.page:last-child {{ page-break-after:auto; }}
.cover {{ display:flex; flex-direction:column; align-items:center; justify-content:flex-start; text-align:center; padding-top:16mm; }}
.cover-logo {{ width:92mm; max-width:70%; height:auto; display:block; margin:0 auto 10mm; }}
.cover h1 {{ margin:0; font-size:23pt !important; font-weight:700 !important; letter-spacing:-0.03em !important; font-family:'IBM Plex Sans','Inter',sans-serif !important; color:var(--accent) !important; }}
.cover .subline {{ color:var(--muted); margin-top:3mm; font-size:10.5pt; max-width:150mm; }}
.cover .meta-line {{ justify-content:center; margin-top:5mm; }}
.cover .disc-wrap {{ margin-top:30mm; max-width:164mm; text-align:left; }}
.cover .disc-title {{ color:var(--accent-strong); font-size:8pt; letter-spacing:.12em; text-transform:uppercase; margin-bottom:4mm; font-family:'IBM Plex Mono',monospace !important; }}
.cover .disc-text {{ color:#d1d5db; font-size:10.4pt; line-height:1.58; }}
.section {{ margin-top:0 !important; }}
.panel {{ border-radius:6px !important; overflow:hidden; }}
.panel-head {{ padding:14px 16px 12px !important; }}
.panel-head h2, h2 {{ font-family:'IBM Plex Sans','Inter',sans-serif !important; font-weight:700 !important; }}
.kicker, .meta-chip, .pill, th {{ font-family:'IBM Plex Mono','IBM Plex Sans','Inter',sans-serif !important; }}
.table-wrap {{ padding:0 16px 10px !important; }}
.pdf-table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
.pdf-table thead {{ display:table-header-group; }}
.pdf-table tbody {{ display:table-row-group; }}
.pdf-table tr {{ page-break-inside:avoid; break-inside:avoid; }}
.pdf-table th {{ text-align:left; color:var(--accent-strong) !important; background:rgba(255,159,26,.04) !important; border-bottom:1px solid var(--hairline) !important; font-size:10.5px !important; letter-spacing:.08em; text-transform:uppercase; padding:9px 8px; }}
.pdf-table td {{ vertical-align:top; border-bottom:1px solid rgba(255,255,255,.06) !important; padding:8px 8px; font-size:12px; line-height:1.32; color:var(--text); }}
.pdf-table td.wrap {{ word-break:break-word; overflow-wrap:anywhere; }}
.pdf-table .label {{ font-weight:700; letter-spacing:-.02em; }}
.pdf-table .event-title {{ display:block; font-weight:700; margin-bottom:3px; }}
.pdf-table .event-src {{ display:block; font-size:10.5px; color:var(--muted2) !important; }}
.pdf-table .inline-tags {{ display:flex; gap:6px; flex-wrap:wrap; }}
.pdf-table .pill {{ display:inline-flex; align-items:center; gap:6px; padding:4px 7px; border-radius:4px !important; width:max-content; max-width:100%; font-size:10.5px !important; }}
.empty-cell {{ color:var(--muted2) !important; }}
.ideas-table th:nth-child(1), .ideas-table td:nth-child(1) {{ width:21%; }}
.ideas-table th:nth-child(2), .ideas-table td:nth-child(2) {{ width:16%; }}
.ideas-table th:nth-child(3), .ideas-table td:nth-child(3) {{ width:9%; }}
.ideas-table th:nth-child(4), .ideas-table td:nth-child(4) {{ width:9%; }}
.ideas-table th:nth-child(5), .ideas-table td:nth-child(5) {{ width:10%; }}
.ideas-table th:nth-child(6), .ideas-table td:nth-child(6) {{ width:35%; }}
.breadth-table th:nth-child(1), .breadth-table td:nth-child(1) {{ width:18%; }}
.breadth-table th:nth-child(2), .breadth-table td:nth-child(2) {{ width:14%; }}
.breadth-table th:nth-child(3), .breadth-table td:nth-child(3) {{ width:8%; }}
.breadth-table th:nth-child(4), .breadth-table td:nth-child(4) {{ width:8%; }}
.breadth-table th:nth-child(5), .breadth-table td:nth-child(5) {{ width:12%; }}
.breadth-table th:nth-child(6), .breadth-table td:nth-child(6) {{ width:40%; }}
.tone-table th:nth-child(1), .tone-table td:nth-child(1) {{ width:18%; }}
.tone-table th:nth-child(2), .tone-table td:nth-child(2) {{ width:14%; }}
.tone-table th:nth-child(3), .tone-table td:nth-child(3) {{ width:68%; }}
.tech-table th, .tech-table td {{ padding-top:5px; padding-bottom:5px; font-size:10.4px; line-height:1.14; }}
.tech-table th:nth-child(1), .tech-table td:nth-child(1) {{ width:22%; }}
.tech-table th:nth-child(2), .tech-table td:nth-child(2) {{ width:16%; }}
.tech-table th:nth-child(3), .tech-table td:nth-child(3) {{ width:30%; }}
.tech-table th:nth-child(4), .tech-table td:nth-child(4) {{ width:10%; }}
.tech-table th:nth-child(5), .tech-table td:nth-child(5) {{ width:11%; }}
.tech-table th:nth-child(6), .tech-table td:nth-child(6) {{ width:11%; }}
.fund-table th:nth-child(1), .fund-table td:nth-child(1) {{ width:15%; }}
.fund-table th:nth-child(2), .fund-table td:nth-child(2) {{ width:13%; }}
.fund-table th:nth-child(3), .fund-table td:nth-child(3) {{ width:20%; }}
.fund-table th:nth-child(4), .fund-table td:nth-child(4) {{ width:52%; }}
.events-table th:nth-child(1), .events-table td:nth-child(1) {{ width:12%; }}
.events-table th:nth-child(2), .events-table td:nth-child(2) {{ width:12%; }}
.events-table th:nth-child(3), .events-table td:nth-child(3) {{ width:46%; }}
.events-table th:nth-child(4), .events-table td:nth-child(4) {{ width:30%; }}
"""

    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"UTF-8\" />
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
<title>1XW Trading | Weekly Research PDF</title>
<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">
<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>
<link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap\" rel=\"stylesheet\">
<style>{css_theme}\n{css_components}\n{css_bloomberg}\n{extra_css}</style>
</head>
<body data-page=\"app\" data-shell=\"member\">
<main class=\"container\">
  <section class=\"page cover\">
    <img class=\"cover-logo\" src=\"{logo_uri}\" alt=\"1XW Trading logo\" />
    <h1>Weekly Research</h1>
    <div class=\"subline\">Cross-asset weekly snapshot highlighting market breadth,<br>
macro tone and relative long / short strength.</div>
    <div class=\"meta-line\">
      <span class=\"meta-chip\">As of {esc(asof)}</span>
      <span class=\"meta-chip\">Week {esc(week_id)}</span>
      <span class=\"meta-chip\">{esc(ranking)}</span>
    </div>
    <div class=\"disc-wrap\">
      <div class=\"disc-title\">Disclaimer</div>
      <div class=\"disc-text\">{esc(disclaimer)}</div>
    </div>
  </section>

  <section class="page">{panel('Overview', 'Top 3 Long / Short', 'Highest-ranked instruments by technical and macro alignment.', render_top_ideas(data))}</section>
<section class="page">{panel('Overview', 'Technical Breadth', 'Trend balance by asset class.', render_breadth(data))}</section>
<section class="page">{panel('Overview', 'Fundamental Tone', 'Quick macro read by asset class.', render_tone(data))}</section>
<section class="page">{panel('Technical', 'Technical Overview', 'Instrument-level technical output.', render_tech_overview(data))}</section>
<section class="page">{panel('Fundamental', 'Fundamental Overview', 'Commentary and highlights by asset class.', render_fund_overview(data))}</section>
<section class="page">{panel('Events', 'Event Calendar', 'Weekly macro and earnings watchlist.', render_events(data))}</section>
</main>
</body>
</html>"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="content/site_weekly.json")
    ap.add_argument("--theme", default="assets/css/theme.css")
    ap.add_argument("--components", default="assets/css/components.css")
    ap.add_argument("--bloomberg", default="assets/css/theme-bloomberg-trial.css")
    ap.add_argument("--logo", default="assets/logo.png")
    ap.add_argument("--reports-dir", default="reports")
    args = ap.parse_args()

    root = Path.cwd()
    json_path = root / args.json
    theme_path = root / args.theme
    comp_path = root / args.components
    bloom_path = root / args.bloomberg
    logo_path = root / args.logo
    if not logo_path.exists() and (root / "logo.png").exists():
        logo_path = root / "logo.png"

    data = json.loads(json_path.read_text(encoding="utf-8"))
    week_id = str(data.get("week_id") or "Weekly").strip()
    week_label = week_id.split("-")[-1] if week_id else "Weekly"
    reports = root / args.reports_dir
    reports.mkdir(parents=True, exist_ok=True)
    pdf_out = reports / f"1XW_Weekly_Research_{week_label}.pdf"

    html_doc = build_html(
        data,
        theme_path.read_text(encoding="utf-8"),
        comp_path.read_text(encoding="utf-8"),
        bloom_path.read_text(encoding="utf-8"),
        data_uri(logo_path),
    )

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1400, "height": 2000}, device_scale_factor=1)
        page.set_content(html_doc, wait_until="load")
        page.wait_for_timeout(1200)
        page.emulate_media(media="print")
        page.pdf(
            path=str(pdf_out),
            print_background=True,
            prefer_css_page_size=True,
            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
        )
        browser.close()

    print(f"Wrote: {pdf_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
