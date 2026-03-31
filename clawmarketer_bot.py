"""
ClawMarketer — Standalone Telegram Bot
=======================================
Runs independently from OpenClaw. Polls ClawMarketerBot and routes
commands to the installed skills.

Install:
  cp clawmarketer_bot.py ~/.openclaw/workspace/clawmarketer_bot.py
  launchctl load ~/Library/LaunchAgents/com.clawmarketer.bot.plist

Or run directly for testing:
  python3 ~/.openclaw/workspace/clawmarketer_bot.py
"""

import os
import sys
import time
import json
import threading
import traceback
import requests
from pathlib import Path
from dotenv import load_dotenv

# ── Load config ───────────────────────────────────────────────────────────────

_env_path = os.path.expanduser("~/.openclaw/clawmarketer.env")
load_dotenv(_env_path)

BOT_TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID            = os.getenv("TELEGRAM_CHAT_ID", "")
CLAWMARKETER_URL   = os.getenv("CLAWMARKETER_URL", "https://clawmarketer.vercel.app")
BASE_URL           = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ── Skills path ───────────────────────────────────────────────────────────────

_SKILLS_BASE   = os.path.expanduser("~/.openclaw/workspace/skills")
_META_SKILL    = os.path.join(_SKILLS_BASE, "clawmarketer-meta")
_DATA_SKILL    = os.path.join(_SKILLS_BASE, "clawmarketer-data")
_BRIEF_SKILL   = os.path.join(_SKILLS_BASE, "clawmarketer-morning-brief")
_COPY_SKILL    = os.path.join(_SKILLS_BASE, "clawmarketer-copy")
_ANOMALY_SKILL = os.path.join(_SKILLS_BASE, "clawmarketer-anomaly")

for _p in [_META_SKILL, _DATA_SKILL, _BRIEF_SKILL, _COPY_SKILL, _ANOMALY_SKILL]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── Trigger routing ───────────────────────────────────────────────────────────

_META_TRIGGERS = [
    "analyze ads", "run ads report", "meta ads report", "how are my ads",
    "ads report", "campaign report", "facebook ads", "meta ads",
    "ad performance", "roas", "campaign performance", "my campaigns",
]
_DATA_TRIGGERS = [
    "clean my data", "clean data", "analyze my files", "process my data",
    "clean files", "data cleaning", "fix my csv", "clean csv",
]
_BRIEF_TRIGGERS = [
    "morning brief", "daily brief", "daily summary", "morning report",
    "how did we do", "overnight report", "send brief",
]
_COPY_TRIGGERS = [
    "write ads", "generate copy", "ad copy", "create ads",
    "write ad copy", "ad variations", "generate ads", "make ads",
]
_ANOMALY_TRIGGERS = [
    "check anomalies", "any issues", "check campaigns", "anything wrong",
    "campaign issues", "check alerts", "run anomaly check",
]

def _route_keywords(text: str):
    t = text.lower().strip()
    for trigger in _BRIEF_TRIGGERS:
        if trigger in t:
            return "brief"
    for trigger in _COPY_TRIGGERS:
        if trigger in t:
            return "copy"
    for trigger in _ANOMALY_TRIGGERS:
        if trigger in t:
            return "anomaly"
    for trigger in _META_TRIGGERS:
        if trigger in t:
            return "meta"
    for trigger in _DATA_TRIGGERS:
        if trigger in t:
            return "data"
    return None


def _route_llm(text: str):
    """Use Groq to route ambiguous messages to the right skill."""
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return None
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{
                    "role": "system",
                    "content": (
                        "You are a routing assistant for a digital marketing AI. "
                        "Classify the user's message into one of these intents:\n"
                        "- meta: anything about Meta/Facebook Ads, campaigns, ad performance, ROAS, CTR, spend, impressions, ad reports\n"
                        "- data: anything about cleaning, processing, fixing, or analysing local data files (CSV, Excel)\n"
                        "- unknown: anything else\n"
                        "Reply with exactly one word: meta, data, or unknown."
                    ),
                }, {"role": "user", "content": text}],
                "temperature": 0,
                "max_tokens": 5,
            },
            timeout=8,
        )
        intent = resp.json()["choices"][0]["message"]["content"].strip().lower()
        return intent if intent in ("meta", "data") else None
    except Exception as e:
        print(f"[bot] LLM routing failed: {e}")
        return None


def _route(text: str):
    result = _route_keywords(text)
    if result:
        return result
    return _route_llm(text)

# ── Telegram helpers ──────────────────────────────────────────────────────────

def _send(text: str, parse_mode: str = "Markdown"):
    _send_to(CHAT_ID, text, parse_mode)

def _send_to(chat_id: str, text: str, parse_mode: str = "Markdown"):
    try:
        requests.post(f"{BASE_URL}/sendMessage", json={
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": parse_mode,
        }, timeout=15)
    except Exception as e:
        print(f"[bot] send failed: {e}")

def _set_commands():
    commands = [
        {"command": "start",        "description": "Show welcome message"},
        {"command": "help",         "description": "List available commands"},
        {"command": "analyzeads",   "description": "Run Meta Ads report (last 30 days)"},
        {"command": "ads7d",        "description": "Meta Ads report — last 7 days"},
        {"command": "adsmonth",     "description": "Meta Ads report — last month"},
        {"command": "cleandata",    "description": "Run data cleansing agent"},
        {"command": "brief",        "description": "Get morning performance brief"},
        {"command": "status",       "description": "Check agent status"},
    ]
    try:
        requests.post(f"{BASE_URL}/setMyCommands", json={"commands": commands}, timeout=10)
        print("[bot] Commands registered ✓")
    except Exception as e:
        print(f"[bot] setMyCommands failed: {e}")

# ── Skill runners (run in threads so bot stays responsive) ────────────────────

def _run_meta(message: str):
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "cm_meta_agent", os.path.join(_META_SKILL, "agent.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.handle(message)
    except Exception:
        err = traceback.format_exc()
        print(f"[bot][meta] error:\n{err}")
        _send(f"❌ Meta Ads agent error:\n`{err[-300:]}`")


def _run_data(message: str):
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "cm_data_agent", os.path.join(_DATA_SKILL, "agent.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.handle(message)
    except Exception:
        err = traceback.format_exc()
        print(f"[bot][data] error:\n{err}")
        _send(f"❌ Data agent error:\n`{err[-300:]}`")


def _run_brief(message: str):
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "cm_brief_agent", os.path.join(_BRIEF_SKILL, "agent.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.handle(message)
    except Exception:
        err = traceback.format_exc()
        print(f"[bot][brief] error:\n{err}")
        _send(f"❌ Morning brief error:\n`{err[-300:]}`")


def _run_copy(message: str):
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "cm_copy_agent", os.path.join(_COPY_SKILL, "agent.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.handle(message)
    except Exception:
        err = traceback.format_exc()
        print(f"[bot][copy] error:\n{err}")
        _send(f"❌ Copy agent error:\n`{err[-300:]}`")


def _run_anomaly(message: str):
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "cm_anomaly_agent", os.path.join(_ANOMALY_SKILL, "agent.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.handle(message)
    except Exception:
        err = traceback.format_exc()
        print(f"[bot][anomaly] error:\n{err}")
        _send(f"❌ Anomaly check error:\n`{err[-300:]}`")

# ── General LLM fallback ──────────────────────────────────────────────────────

def _run_general(message: str):
    """Answer any query that doesn't match a specific skill using Groq."""
    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        _send(
            "I can handle ads and data tasks — but I'm not set up for general chat yet.\n\n"
            "Try `analyze ads` or `clean my data`.\nSend /help for the full list."
        )
        return
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are ClawMarketer, a smart digital marketing assistant. "
                            "You specialise in Meta/Facebook Ads, data analysis, and marketing strategy. "
                            "Answer the user's question clearly and concisely. "
                            "When relevant, mention that you can run live Meta Ads reports (say 'analyze ads') "
                            "or clean local data files (say 'clean my data')."
                        ),
                    },
                    {"role": "user", "content": message},
                ],
                "temperature": 0.6,
                "max_tokens": 1024,
            },
            timeout=30,
        )
        answer = resp.json()["choices"][0]["message"]["content"].strip()
        _send(answer)
    except Exception as e:
        print(f"[bot][general] LLM error: {e}")
        _send(f"⚠️ Couldn't get a response right now: {e}")

# ── Command handlers ──────────────────────────────────────────────────────────

_HELP_TEXT = (
    "*ClawMarketer Bot* 🤖\n\n"
    "*Reports:*\n"
    "• `analyze ads` — Meta Ads last 30 days\n"
    "• `analyze ads last 7 days`\n"
    "• `morning brief` — daily performance digest\n\n"
    "*Ad Copy:*\n"
    "• `write ads for [product] targeting [audience]`\n"
    "• `generate copy for [product]`\n\n"
    "*Monitoring:*\n"
    "• `check anomalies` — scan for issues now\n"
    "  _(auto-runs every 4 hours in background)_\n\n"
    "*Data Cleaning:*\n"
    "• `clean my data`\n"
    "• `clean data in ~/path/to/folder`\n\n"
    "Results appear here + on your dashboard:\n"
    "https://clawmarketer.vercel.app"
)

def _handle_message(text: str, chat_id: str):
    t = text.strip()

    # /connect bypasses the CHAT_ID guard — it's the registration command
    if t.lower().startswith("/connect"):
        parts = t.split(maxsplit=1)
        code  = parts[1].strip() if len(parts) > 1 else ""
        if not code:
            _send_to(chat_id, "Send `/connect CODE` with the 6-digit code from the ClawMarketer app.")
            return
        try:
            resp = requests.post(
                f"{CLAWMARKETER_URL}/api/telegram/verify",
                json={"code": code, "chat_id": str(chat_id)},
                timeout=10,
            )
            if resp.status_code == 200:
                _send_to(chat_id,
                    "✅ *Telegram connected!*\n\n"
                    "You'll receive your reports and alerts here.\n"
                    "Send /help to see what I can do."
                )
            else:
                detail = resp.json().get("detail", "Unknown error")
                _send_to(chat_id, f"❌ Could not connect: {detail}\n\nGet a new code at clawmarketer.vercel.app")
        except Exception as e:
            _send_to(chat_id, f"❌ Connection failed: {e}")
        return

    # Security: all other commands only accepted from the configured chat
    if CHAT_ID and str(chat_id) != str(CHAT_ID):
        print(f"[bot] Ignoring message from unknown chat {chat_id}")
        return

    # Built-in commands
    if t in ("/start", "/help"):
        _send(_HELP_TEXT)
        return
    if t == "/analyzeads":
        t = "analyze ads"
    elif t == "/ads7d":
        t = "analyze ads last 7 days"
    elif t == "/adsmonth":
        t = "analyze ads last month"
    elif t == "/cleandata":
        t = "clean my data"
    elif t == "/brief":
        t = "morning brief"
    elif t == "/status":
        _send("✅ ClawMarketer bot is running.\nSend /help to see all commands.")
        return

    skill = _route(t)
    if skill == "meta":
        _send("🚀 Starting Meta Ads analysis...\nCheck your dashboard for live progress.")
        threading.Thread(target=_run_meta, args=(t,), daemon=True).start()
    elif skill == "data":
        _send("🧹 Starting data cleaning agent...\nCheck your dashboard for live progress.")
        threading.Thread(target=_run_data, args=(t,), daemon=True).start()
    elif skill == "brief":
        _send("☀️ Generating your morning brief...")
        threading.Thread(target=_run_brief, args=(t,), daemon=True).start()
    elif skill == "copy":
        threading.Thread(target=_run_copy, args=(t,), daemon=True).start()
    elif skill == "anomaly":
        threading.Thread(target=_run_anomaly, args=(t,), daemon=True).start()
    else:
        threading.Thread(target=_run_general, args=(t,), daemon=True).start()

# ── Long-polling loop ─────────────────────────────────────────────────────────

def _poll():
    if not BOT_TOKEN:
        print("[bot] ERROR: TELEGRAM_BOT_TOKEN not set in ~/.openclaw/clawmarketer.env")
        sys.exit(1)

    print(f"[bot] ClawMarketer bot starting — polling for messages...")
    _set_commands()
    _send("✅ *ClawMarketer bot started.*\nSend /help to see what I can do.")

    offset = None
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset is not None:
                params["offset"] = offset

            resp = requests.get(f"{BASE_URL}/getUpdates", params=params, timeout=40)
            data = resp.json()

            if not data.get("ok"):
                print(f"[bot] getUpdates error: {data}")
                time.sleep(5)
                continue

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                text = msg.get("text", "")
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if text and chat_id:
                    print(f"[bot] Message from {chat_id}: {text!r}")
                    threading.Thread(
                        target=_handle_message, args=(text, chat_id), daemon=True
                    ).start()

        except requests.exceptions.ReadTimeout:
            continue  # Normal for long-polling
        except KeyboardInterrupt:
            print("\n[bot] Stopped.")
            break
        except Exception as e:
            print(f"[bot] Poll error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    _poll()
