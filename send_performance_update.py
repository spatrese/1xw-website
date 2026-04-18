import json
from typing import Any, Dict

import requests

BOT_TOKEN = "8789123634:AAHRd1iBDl6Qarz3q13PSgg9v6FP7D4ouR0"
CHAT_ID = "-1003523484797"

PERF_JSON = "content/site_performance.json"
MODEL_URL = "https://1xwtrading.com/model"


def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def fmt_money(x: Any) -> str:
    try:
        return "$" + format(float(x), ",.0f")
    except Exception:
        return "—"


def fmt_pct_decimal(x: Any) -> str:
    try:
        v = float(x) * 100.0
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.2f}%"
    except Exception:
        return "—"


def send_telegram_message(text: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": False,
    }
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")


def build_performance_message(data: Dict[str, Any]) -> str:
    asof = str(data.get("asof") or "").strip()
    snapshot = data.get("snapshot", {}) or {}

    nav = snapshot.get("nav_usd")
    plb = snapshot.get("plb_usd")
    ytd = snapshot.get("performance_ytd")
    gap = snapshot.get("gap_to_plb")

    lines = [
        "1XW MODEL PERFORMANCE UPDATE",
        "",
    ]

    if asof:
        lines.append(f"AS OF: {asof}")

    lines.extend([
        f"NAV: {fmt_money(nav)}",
        f"YTD PERFORMANCE: {fmt_pct_decimal(ytd)}",
        f"PLB: {fmt_money(plb)}",
        f"GAP TO PLB: {fmt_pct_decimal(gap)}",
        "",
        f"MODEL PAGE: {MODEL_URL}",
    ])

    return "\n".join(lines)


def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("Missing BOT_TOKEN")
    if not CHAT_ID:
        raise ValueError("Missing CHAT_ID")

    data = read_json(PERF_JSON)
    msg = build_performance_message(data)
    send_telegram_message(msg)
    print("Done. Performance update sent.")


if __name__ == "__main__":
    main()