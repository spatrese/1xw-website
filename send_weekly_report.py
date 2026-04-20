import json
import subprocess
from pathlib import Path
from typing import Any, Dict

import requests

BOT_TOKEN = "8676635408:AAH1A6DSw3P7cyvhUtpzqx8s9-8Nq3RfIM4"
CHAT_ID = "-1003523484797"

PDF_SCRIPT = "generate_weekly_pdf.py"
WEEKLY_JSON = "content/site_weekly.json"
REPORTS_DIR = "reports"
MODEL_URL = "https://1xwtrading.com/app"


def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def send_telegram_pdf(file_path: str, caption: str = "") -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    with open(file_path, "rb") as f:
        files = {"document": f}
        data = {
            "chat_id": CHAT_ID,
            "caption": caption,
        }
        r = requests.post(url, data=data, files=files, timeout=120)
        r.raise_for_status()
        resp = r.json()
        if not resp.get("ok"):
            raise RuntimeError(f"Telegram API error: {resp}")


def build_weekly_message(data: Dict[str, Any]) -> str:
    week_id = str(data.get("week_id") or "").strip()
    asof = str(data.get("asof") or "").strip()

    lines = [
        "1XW WEEKLY RESEARCH",
        "",
    ]

    if asof:
        lines.append(f"AS OF: {asof}")
    if week_id:
        lines.append(f"WEEK: {week_id}")

    lines.extend([
        "",
        "FULL REPORT ATTACHED",
        "",
        f"RESEARCH PAGE: {MODEL_URL}",
    ])

    return "\n".join(lines)


def build_pdf_path(data: Dict[str, Any]) -> Path:
    week_id = str(data.get("week_id") or "Weekly").strip()
    week_label = week_id.split("-")[-1] if week_id else "Weekly"
    return Path(REPORTS_DIR) / f"1XW_Weekly_Research_{week_label}.pdf"


def generate_pdf() -> None:
    cmd = ["python", PDF_SCRIPT]
    r = subprocess.run(cmd, check=True)
    if r.returncode != 0:
        raise RuntimeError("PDF generation failed.")


def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("Missing BOT_TOKEN")
    if not CHAT_ID:
        raise ValueError("Missing CHAT_ID")

    data = read_json(WEEKLY_JSON)

    print("Generating weekly PDF...")
    generate_pdf()

    pdf_path = build_pdf_path(data)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    msg = build_weekly_message(data)
    print("Sending weekly Telegram message...")
    send_telegram_message(msg)

    print("Sending weekly PDF...")
    send_telegram_pdf(str(pdf_path), caption="1XW Weekly Research")

    print("Done. Weekly report sent.")


if __name__ == "__main__":
    main()