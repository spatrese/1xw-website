import json
import os
import shutil
from typing import Any, Dict, List

import requests

PREVIEW_JSON_PATH = "content/position_alerts_preview.json"
CURRENT_PATH = "content/site_screener.json"
PREVIOUS_PATH = "content/site_screener_prev.json"

# Meglio mettere questi in variabili ambiente.
# Per ora puoi anche incollarli direttamente qui per test.
BOT_TOKEN = "8789123634:AAHRd1iBDl6Qarz3q13PSgg9v6FP7D4ouR0"
CHAT_ID = "-1003523484797"


def read_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def format_num(x: Any, decimals: int = 4) -> str:
    try:
        return f"{float(x):.{decimals}f}"
    except Exception:
        return ""


def format_pct(x: Any, decimals: int = 2) -> str:
    try:
        return f"{float(x):.{decimals}f}%"
    except Exception:
        return ""


def format_sentence(text: str) -> str:
    if not text:
        return ""
    text = str(text).strip()
    return text[0].upper() + text[1:]


def build_telegram_message(alert: Dict[str, Any]) -> str:
    alert_type = str(alert.get("type", "")).strip().lower()
    p = alert.get("position", {}) or {}

    if alert_type == "close":
        header = "1XW MODEL ALERT: POSITION CLOSED"
    else:
        header = "1XW MODEL ALERT: NEW POSITION"

    lines: List[str] = [header, ""]

    # --- BASIC INFO ---
    underlying = str(p.get("description", p.get("underlying", ""))).strip()
    structure = str(p.get("structure", "")).strip()
    asset_class = str(p.get("asset_class", "")).strip()

    lines.append(f"UNDERLYING: {underlying}")            # EURUSD resta maiuscolo
    lines.append(f"INSTRUMENT: {structure}")             # Futures
    lines.append(f"ASSET CLASS: {asset_class}")  # FX, COMMODITIES

    if p.get("expiry"):
        lines.append(f"EXPIRY: {p.get('expiry')}")

    # --- STRIKES ---
    if p.get("strikes"):
        strike_text = " / ".join(
            str(int(s)) if float(s).is_integer() else str(s)
            for s in p["strikes"]
        )
        lines.append(f"STRIKES: {strike_text}")

    elif p.get("strike") is not None:
        s = p["strike"]
        strike_text = str(int(s)) if float(s).is_integer() else str(s)
        lines.append(f"STRIKE: {strike_text}")

    # =========================
    # ONLY FOR NEW POSITIONS
    # =========================
    if alert_type == "open":

        if p.get("tech_stop") is not None:
            lines.append(f"STOP LOSS: {format_num(p.get('tech_stop'), 4)}")

        stop_distance = p.get("stop_distance_pct")
        if stop_distance is not None:
            try:
                if abs(float(stop_distance)) > 0.000001:
                    lines.append(f"STOP DISTANCE: {format_pct(stop_distance, 2)}")
            except Exception:
                pass

        premium_loss = p.get("premium_max_loss_pct_capital")
        if premium_loss is None:
            premium_loss = p.get("max_loss_pct_capital")

        if premium_loss is not None:
            lines.append(f"MAX LOSS: {format_pct(premium_loss, 2)} of capital")

        # --- THEME / SETUP (prima lettera maiuscola) ---
        if p.get("theme"):
            lines.append("")
            lines.append(f"THEME: {format_sentence(p.get('theme'))}")

        if p.get("technical_setup"):
            lines.append(f"SET-UP: {format_sentence(p.get('technical_setup'))}")

        # --- RISK FRAMEWORK SOLO PER OPEN ---
        lines.append("")
        lines.append(
            "RISK FRAMEWORK: each position is sized such that, if the stop loss is reached or the premium is fully paid, the maximum loss is equal to or below 1% of portfolio capital."
        )

    return "\n".join(lines)

def send_telegram_message(text: str) -> Dict[str, Any]:
    if not BOT_TOKEN:
        raise ValueError("Missing TELEGRAM_BOT_TOKEN")
    if not CHAT_ID:
        raise ValueError("Missing TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
    }

    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()

    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")

    return data


def update_baseline() -> None:
    if not os.path.exists(CURRENT_PATH):
        raise FileNotFoundError(f"Current screener file not found: {CURRENT_PATH}")
    shutil.copyfile(CURRENT_PATH, PREVIOUS_PATH)


def main() -> None:
    preview = read_json(PREVIEW_JSON_PATH)
    alerts = preview.get("alerts", [])

    if not isinstance(alerts, list) or not alerts:
        print("No alerts to send.")
        return

    sent_count = 0

    for i, alert in enumerate(alerts, start=1):
        msg = build_telegram_message(alert)
        send_telegram_message(msg)
        sent_count += 1
        print(f"Sent Telegram alert {i}/{len(alerts)}")

    update_baseline()
    print(f"Done. Sent {sent_count} Telegram alert(s).")
    print(f"Baseline updated: {PREVIOUS_PATH}")


if __name__ == "__main__":
    main()