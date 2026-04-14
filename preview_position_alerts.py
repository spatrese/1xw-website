import json
import os
from typing import Any, Dict, List, Tuple

CURRENT_PATH = "content/site_screener.json"
PREVIOUS_PATH = "content/site_screener_prev.json"
PREVIEW_JSON_PATH = "content/position_alerts_preview.json"
PREVIEW_TXT_PATH = "content/position_alerts_preview.txt"


def read_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_text(path: str, text: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def alert_key(p: Dict[str, Any]) -> Tuple[str, str, str]:
    week = str(p.get("week", "")).strip()

    underlying = str(p.get("underlying", "")).strip().lower()
    underlying = underlying.replace("_paxos", "").replace("_", "")

    structure = str(p.get("structure", "")).strip().lower()

    return (week, underlying, structure)


def format_pct(x: Any, decimals: int = 2) -> str:
    try:
        return f"{float(x):.{decimals}f}%"
    except Exception:
        return ""

def format_num(x: Any, decimals: int = 4) -> str:
    try:
        return f"{float(x):.{decimals}f}"
    except Exception:
        return ""

def format_futures_email(p: Dict[str, Any]) -> Dict[str, str]:
    subject = "1XW Model Update — New Position Opened"

    lines = [
        "A new position has been opened in the 1XW model portfolio.",
        "",
        f"Underlying: {p.get('description', p.get('underlying', ''))}",
        f"Instrument: {p.get('structure', '')}",
        f"Asset class: {p.get('asset_class', '')}",
    ]

    if p.get("expiry"):
        lines.append(f"Expiry: {p.get('expiry')}")

    if p.get("tech_stop") is not None:
        lines.append(f"Stop loss: {format_num(p.get('tech_stop'), 4)}")

    if p.get("stop_distance_pct") is not None:
        lines.append(f"Stop distance: {format_pct(p.get('stop_distance_pct'), 2)}")

    if p.get("max_loss_pct_capital") is not None:
        lines.extend([
            "",
            f"Maximum loss: {format_pct(p.get('max_loss_pct_capital'), 2)} of capital"
        ])

    if p.get("theme"):
        lines.extend(["", f"Theme: {p.get('theme')}"])

    if p.get("technical_setup"):
        lines.append(f"Technical set-up: {p.get('technical_setup')}")

    return {"subject": subject, "body": "\n".join(lines)}


def format_options_email(p: Dict[str, Any]) -> Dict[str, str]:
    subject = "1XW Model Update — New Position Opened"

    lines = [
        "A new position has been opened in the 1XW model portfolio.",
        "",
        f"Underlying: {p.get('description', p.get('underlying', ''))}",
        f"Instrument: {p.get('structure', '')}",
        f"Asset class: {p.get('asset_class', '')}",
    ]

    if p.get("expiry"):
        lines.append(f"Expiry: {p.get('expiry')}")
    elif p.get("expiries"):
        lines.append(f"Expiries: {' / '.join(p.get('expiries', []))}")

    if p.get("strikes"):
        strike_text = " / ".join(str(int(s)) if float(s).is_integer() else str(s) for s in p["strikes"])
        lines.append(f"Strikes: {strike_text}")
    elif p.get("strike") is not None:
        s = p["strike"]
        strike_text = str(int(s)) if float(s).is_integer() else str(s)
        lines.append(f"Strike: {strike_text}")

    premium_loss = p.get("premium_max_loss_pct_capital")
    if premium_loss is None:
        premium_loss = p.get("max_loss_pct_capital")

    if premium_loss is not None:
        lines.extend([
            "",
            f"Premium / maximum loss: {format_pct(premium_loss, 2)} of capital"
        ])

    if p.get("theme"):
        lines.extend(["", f"Theme: {p.get('theme')}"])

    if p.get("technical_setup"):
        lines.append(f"Technical set-up: {p.get('technical_setup')}")

    return {"subject": subject, "body": "\n".join(lines)}

def format_close_email(p: Dict[str, Any]) -> Dict[str, str]:
    subject = "1XW Model Update — Position Closed"

    lines = [
        "A position has been closed in the 1XW model portfolio.",
        "",
        f"Underlying: {p.get('description', p.get('underlying', ''))}",
        f"Instrument: {p.get('structure', '')}",
        f"Asset class: {p.get('asset_class', '')}",
    ]

    if p.get("expiry"):
        lines.append(f"Expiry: {p.get('expiry')}")

    if p.get("strikes"):
        strike_text = " / ".join(
            str(int(s)) if float(s).is_integer() else str(s)
            for s in p["strikes"]
        )
        lines.append(f"Strikes: {strike_text}")
    elif p.get("strike") is not None:
        s = p["strike"]
        strike_text = str(int(s)) if float(s).is_integer() else str(s)
        lines.append(f"Strike: {strike_text}")

    return {
        "subject": subject,
        "body": "\n".join(lines)
    }

def build_email_preview(p: Dict[str, Any]) -> Dict[str, str]:
    structure = str(p.get("structure", "")).lower()
    if structure in {"futures", "spot"}:
        return format_futures_email(p)
    return format_options_email(p)


def main() -> None:
    current = read_json(CURRENT_PATH)
    previous = read_json(PREVIOUS_PATH)

    current_positions = current.get("openPositionsAggregated", [])
    previous_positions = previous.get("openPositionsAggregated", [])

    previous_keys = {alert_key(p) for p in previous_positions}
    current_keys = {alert_key(p) for p in current_positions}

    print("\n--- DEBUG KEYS ---")

    print("\nCURRENT:")
    for p in current_positions:
        print(alert_key(p))

    print("\nPREVIOUS:")
    for p in previous_positions:
        print(alert_key(p))

    new_positions = [p for p in current_positions if alert_key(p) not in previous_keys]
    closed_positions = [p for p in previous_positions if alert_key(p) not in current_keys]

    previews: List[Dict[str, Any]] = []
    text_blocks: List[str] = []

    for p in new_positions:
        email = build_email_preview(p)
        previews.append({
            "key": alert_key(p),
            "type": "open",
            "position": p,
            "email": email,
        })
        text_blocks.append(email["subject"])
        text_blocks.append("")
        text_blocks.append(email["body"])
        text_blocks.append("\n" + "=" * 80 + "\n")

    for p in closed_positions:
        email = format_close_email(p)
        previews.append({
            "key": alert_key(p),
            "type": "close",
            "position": p,
            "email": email,
        })
        text_blocks.append(email["subject"])
        text_blocks.append("")
        text_blocks.append(email["body"])
        text_blocks.append("\n" + "=" * 80 + "\n")

    write_json(PREVIEW_JSON_PATH, {"alerts": previews})
    write_text(PREVIEW_TXT_PATH, "\n".join(text_blocks))

    print(f"Preview alerts: {len(previews)}")
    print(f"Wrote: {PREVIEW_JSON_PATH}")
    print(f"Wrote: {PREVIEW_TXT_PATH}")


if __name__ == "__main__":
    main()