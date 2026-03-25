"""
Telegram sender — rich messages + file attachments.
Config: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in clawmarketer.env
"""

import os
import requests
from typing import Optional

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"


def _url(token: str, method: str) -> str:
    return TELEGRAM_API.format(token=token, method=method)


def send_message(text: str, token: str = None, chat_id: str = None, parse_mode: str = "Markdown") -> Optional[int]:
    token   = token   or os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("[Telegram] Not configured — skipping message")
        return None

    resp = requests.post(_url(token, "sendMessage"), json={
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": parse_mode,
    }, timeout=15)

    if not resp.ok:
        print(f"[Telegram] sendMessage failed: {resp.text[:200]}")
        return None
    return resp.json().get("result", {}).get("message_id")


def send_photo(photo_path: str, caption: str = "", token: str = None, chat_id: str = None) -> Optional[int]:
    token   = token   or os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return None

    with open(photo_path, "rb") as f:
        resp = requests.post(_url(token, "sendPhoto"), data={
            "chat_id":    chat_id,
            "caption":    caption,
            "parse_mode": "Markdown",
        }, files={"photo": f}, timeout=30)

    if not resp.ok:
        print(f"[Telegram] sendPhoto failed: {resp.text[:200]}")
        return None
    return resp.json().get("result", {}).get("message_id")


def send_document(file_path: str, caption: str = "", token: str = None, chat_id: str = None) -> Optional[int]:
    token   = token   or os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return None

    with open(file_path, "rb") as f:
        resp = requests.post(_url(token, "sendDocument"), data={
            "chat_id":    chat_id,
            "caption":    caption,
            "parse_mode": "Markdown",
        }, files={"document": f}, timeout=30)

    if not resp.ok:
        print(f"[Telegram] sendDocument failed: {resp.text[:200]}")
        return None
    return resp.json().get("result", {}).get("message_id")


def send_report_bundle(summary: str, chart_paths: list, csv_path: Optional[str] = None,
                       token: str = None, chat_id: str = None):
    """Send the full report: text summary + charts + CSV in one flow."""
    send_message(summary, token=token, chat_id=chat_id)
    for path in chart_paths:
        if os.path.exists(path):
            send_photo(path, token=token, chat_id=chat_id)
    if csv_path and os.path.exists(csv_path):
        send_document(csv_path, caption="📎 Clean campaign data", token=token, chat_id=chat_id)
