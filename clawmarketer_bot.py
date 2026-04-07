"""
ClawMarketer — Context-Aware Telegram Bot
==========================================
The bot knows your business. It remembers conversations, understands who you are,
and gives advice tailored to your company — not generic marketing tips.

Setup:
  Send /setup to the bot to configure your business profile.
  After that, every response is personalised to your context.
"""

import os
import sys
import time
import json
import threading
import traceback
import requests
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── Load config ───────────────────────────────────────────────────────────────

_env_path = os.path.expanduser("~/.openclaw/clawmarketer.env")
load_dotenv(_env_path)

BOT_TOKEN          = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID            = os.getenv("TELEGRAM_CHAT_ID", "")
CLAWMARKETER_URL   = os.getenv("CLAWMARKETER_URL", "https://clawmarketer.vercel.app")
GROQ_API_KEY       = os.getenv("GROQ_API_KEY", "")
BASE_URL           = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ── File paths ─────────────────────────────────────────────────────────────────

_OPENCLAW_DIR  = Path.home() / ".openclaw"
_CONTEXT_FILE  = _OPENCLAW_DIR / "context.json"
_HISTORY_FILE  = _OPENCLAW_DIR / "history.json"
_OPENCLAW_DIR.mkdir(exist_ok=True)

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


# ── Business context ──────────────────────────────────────────────────────────

def _load_context() -> dict:
    if _CONTEXT_FILE.exists():
        try:
            return json.loads(_CONTEXT_FILE.read_text())
        except Exception:
            pass
    return {}

def _save_context(data: dict):
    _CONTEXT_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False))

def _context_summary(ctx: dict) -> str:
    """Build a compact context string for the LLM system prompt."""
    if not ctx or not ctx.get("company"):
        return "No business profile set up yet. User can run /setup to configure."
    lines = [f"Company: {ctx.get('company', '—')}"]
    if ctx.get("product"):   lines.append(f"Product/service: {ctx['product']}")
    if ctx.get("platforms"): lines.append(f"Ad platforms: {ctx['platforms']}")
    if ctx.get("budget"):    lines.append(f"Monthly ad budget: {ctx['budget']}")
    if ctx.get("goal"):      lines.append(f"Primary goal: {ctx['goal']}")
    if ctx.get("notes"):     lines.append(f"Notes: {ctx['notes']}")
    if ctx.get("last_analysis_summary"):
        lines.append(f"Last analysis: {ctx['last_analysis_summary']}")
    return "\n".join(lines)


# ── Conversation history ───────────────────────────────────────────────────────

_MAX_HISTORY = 16  # messages (8 exchanges)

def _load_history() -> list:
    if _HISTORY_FILE.exists():
        try:
            return json.loads(_HISTORY_FILE.read_text())
        except Exception:
            pass
    return []

def _save_history(messages: list):
    # Keep only the last _MAX_HISTORY messages
    _HISTORY_FILE.write_text(json.dumps(messages[-_MAX_HISTORY:], indent=2, ensure_ascii=False))

def _append_history(role: str, content: str):
    history = _load_history()
    history.append({"role": role, "content": content,
                    "ts": datetime.now(timezone.utc).isoformat()})
    _save_history(history)

def _history_for_llm() -> list:
    """Return history in OpenAI message format (no timestamps)."""
    return [{"role": m["role"], "content": m["content"]}
            for m in _load_history()]


# ── Setup wizard state machine ────────────────────────────────────────────────

# In-memory per chat_id: {"step": 1, "data": {}}
_setup_sessions: dict = {}

_SETUP_QUESTIONS = [
    ("company",   "What's your company or brand name?"),
    ("product",   "What do you sell? One sentence is enough."),
    ("platforms", "Which platforms do you advertise on? (e.g. Facebook, Instagram, TikTok, Google)"),
    ("budget",    "What's your rough monthly ad budget? (any currency)"),
    ("goal",      "What's your #1 goal right now? (e.g. increase ROAS, grow sales, build brand awareness)"),
    ("notes",     "Anything else I should know about your business? (or say *skip*)"),
]

def _setup_start(chat_id: str):
    _setup_sessions[chat_id] = {"step": 0, "data": {}}
    _send_to(chat_id,
        "Let's set up your business profile. I'll ask 6 quick questions.\n\n"
        f"*Question 1/6:* {_SETUP_QUESTIONS[0][1]}"
    )

def _setup_handle(chat_id: str, text: str) -> bool:
    """Handle a message during setup. Returns True if setup is still in progress."""
    if chat_id not in _setup_sessions:
        return False

    session = _setup_sessions[chat_id]
    step    = session["step"]
    key, _  = _SETUP_QUESTIONS[step]

    value = text.strip()
    if key == "notes" and value.lower() in ("skip", "no", "none", "-", "—"):
        value = ""
    session["data"][key] = value
    session["step"] += 1

    if session["step"] >= len(_SETUP_QUESTIONS):
        # Done — save context
        ctx = dict(session["data"])
        ctx["setup_done"] = True
        _save_context(ctx)
        del _setup_sessions[chat_id]

        company = ctx.get("company", "your business")
        _send_to(chat_id,
            f"✅ *Got it!* I now know {company}.\n\n"
            f"From now on, everything I say is tailored to your business.\n\n"
            f"Try asking:\n"
            f"• *analyze ads* — run a Meta Ads report\n"
            f"• *what should I focus on this week?*\n"
            f"• *write an ad for my best product*"
        )
        return False  # setup complete

    # Ask next question
    next_step = session["step"]
    _, question = _SETUP_QUESTIONS[next_step]
    _send_to(chat_id,
        f"*Question {next_step + 1}/{len(_SETUP_QUESTIONS)}:* {question}"
    )
    return True  # still in setup


# ── Build LLM system prompt ───────────────────────────────────────────────────

def _system_prompt() -> str:
    ctx = _load_context()
    ctx_str = _context_summary(ctx)
    company = ctx.get("company", "the user's business")

    return (
        f"You are ClawMarketer, the AI marketing assistant for {company}.\n\n"
        f"Business context:\n{ctx_str}\n\n"
        "Your capabilities:\n"
        "1. Run Meta/Facebook Ads analysis → trigger with 'analyze ads'\n"
        "2. Clean and analyse data files → trigger with 'clean my data'\n"
        "3. Morning performance brief → trigger with 'morning brief'\n"
        "4. Write ad copy → trigger with 'write ads for [product]'\n"
        "5. Answer any marketing question based on the user's specific business\n\n"
        "Rules:\n"
        "- Always tailor your response to the business context above\n"
        "- Be direct and specific — no generic advice\n"
        "- When answering strategy questions, reference their actual budget, goal, and platforms\n"
        "- Keep responses concise — this is Telegram, not a blog post\n"
        "- If they ask about their ad performance, encourage 'analyze ads' to get real data\n"
        "- Do NOT offer to run agents yourself — just mention the trigger phrase\n"
    )


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
        if trigger in t: return "brief"
    for trigger in _COPY_TRIGGERS:
        if trigger in t: return "copy"
    for trigger in _ANOMALY_TRIGGERS:
        if trigger in t: return "anomaly"
    for trigger in _META_TRIGGERS:
        if trigger in t: return "meta"
    for trigger in _DATA_TRIGGERS:
        if trigger in t: return "data"
    return None

def _route_llm(text: str):
    if not GROQ_API_KEY:
        return None
    try:
        ctx = _load_context()
        company = ctx.get("company", "")
        ctx_hint = f" The user runs {company}." if company else ""
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{
                    "role": "system",
                    "content": (
                        f"You are a routing assistant for a digital marketing AI.{ctx_hint} "
                        "Classify the user message into one of:\n"
                        "- meta: Meta/Facebook Ads, campaigns, ROAS, CTR, spend, ad reports\n"
                        "- data: cleaning, processing, fixing CSV/Excel files\n"
                        "- brief: morning report, daily summary, overnight performance\n"
                        "- copy: writing ads, generating ad copy, ad variations\n"
                        "- anomaly: checking for campaign issues, alerts, problems\n"
                        "- chat: everything else (questions, strategy, advice)\n"
                        "Reply with exactly one word."
                    ),
                }, {"role": "user", "content": text}],
                "temperature": 0,
                "max_tokens": 5,
            },
            timeout=8,
        )
        intent = resp.json()["choices"][0]["message"]["content"].strip().lower()
        return intent if intent in ("meta", "data", "brief", "copy", "anomaly") else None
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
    if not chat_id:
        return
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
        {"command": "start",      "description": "Welcome message"},
        {"command": "setup",      "description": "Set up your business profile"},
        {"command": "help",       "description": "List all commands"},
        {"command": "analyzeads", "description": "Run Meta Ads report (last 30 days)"},
        {"command": "cleandata",  "description": "Clean data files"},
        {"command": "brief",      "description": "Morning performance brief"},
        {"command": "context",    "description": "Show your current business profile"},
        {"command": "status",     "description": "Check bot status"},
    ]
    try:
        requests.post(f"{BASE_URL}/setMyCommands", json={"commands": commands}, timeout=10)
        print("[bot] Commands registered ✓")
    except Exception as e:
        print(f"[bot] setMyCommands failed: {e}")


# ── Skill runners ─────────────────────────────────────────────────────────────

def _run_skill(name: str, skill_dir: str, message: str):
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(name, os.path.join(skill_dir, "agent.py"))
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.handle(message)
    except Exception:
        err = traceback.format_exc()
        print(f"[bot][{name}] error:\n{err}")
        _send(f"❌ {name} error:\n`{err[-300:]}`")


# ── General LLM chat (context-aware) ─────────────────────────────────────────

def _run_general(message: str):
    if not GROQ_API_KEY:
        _send(
            "I can run ads reports and clean data — but I need a Groq key for general chat.\n"
            "Try: `analyze ads` or `clean my data`"
        )
        return

    # Save user message to history
    _append_history("user", message)

    # Build messages: system + history + current
    history = _history_for_llm()
    # Remove the last entry we just added (we'll add it fresh)
    if history and history[-1]["content"] == message:
        history = history[:-1]

    messages = [{"role": "system", "content": _system_prompt()}] + history + \
               [{"role": "user", "content": message}]

    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model":       "llama-3.3-70b-versatile",
                "messages":    messages,
                "temperature": 0.6,
                "max_tokens":  1024,
            },
            timeout=30,
        )
        answer = resp.json()["choices"][0]["message"]["content"].strip()
        _send(answer)
        _append_history("assistant", answer)
    except Exception as e:
        print(f"[bot][general] LLM error: {e}")
        _send(f"⚠️ Couldn't get a response right now. Try again.")


# ── Help text ─────────────────────────────────────────────────────────────────

def _build_help(ctx: dict) -> str:
    company = ctx.get("company", "")
    header  = f"*ClawMarketer* — AI assistant for {company}\n\n" if company else "*ClawMarketer Bot*\n\n"
    return (
        header +
        "*Agents (run automatically):*\n"
        "• `analyze ads` — Meta Ads report, last 30 days\n"
        "• `analyze ads last 7 days`\n"
        "• `clean my data` — scan & clean CSV/Excel files\n"
        "• `morning brief` — daily performance digest\n"
        "• `write ads for [product]` — generate ad copy\n"
        "• `check anomalies` — scan for campaign issues\n\n"
        "*Commands:*\n"
        "/setup — configure your business profile\n"
        "/context — see your current profile\n"
        "/analyzeads  /cleandata  /brief\n\n"
        "*Just chat:*\n"
        "Ask me anything about marketing strategy, budgets, targeting, "
        "copy ideas — I'll answer based on your business.\n\n"
        f"Dashboard → {CLAWMARKETER_URL}"
    )


# ── Message handler ───────────────────────────────────────────────────────────

def _handle_message(text: str, chat_id: str):
    t = text.strip()

    # /connect bypasses CHAT_ID guard — it's the registration command
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
                    "You'll receive reports and alerts here.\n\n"
                    "Run /setup to tell me about your business so I can give you tailored advice."
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

    # If setup wizard is in progress for this chat, handle it
    if chat_id in _setup_sessions:
        _setup_handle(chat_id, t)
        return

    ctx = _load_context()

    # Built-in commands
    if t in ("/start",):
        if not ctx.get("setup_done"):
            _send(
                "👋 *Welcome to ClawMarketer!*\n\n"
                "I'm your AI marketing assistant. Before we start, let me learn about your business.\n\n"
                "Run /setup — takes 2 minutes."
            )
        else:
            company = ctx.get("company", "")
            _send(f"👋 Back! What can I do for {company} today?\n\nSend /help for the full list.")
        return

    if t == "/help":
        _send(_build_help(ctx))
        return

    if t == "/setup":
        _setup_start(chat_id)
        return

    if t == "/context":
        ctx_str = _context_summary(ctx)
        if not ctx.get("company"):
            _send("No profile set up yet. Run /setup to configure your business.")
        else:
            _send(f"*Your business profile:*\n\n{ctx_str}\n\nRun /setup to update it.")
        return

    if t == "/status":
        company = ctx.get("company", "")
        label   = f" for {company}" if company else ""
        _send(f"✅ ClawMarketer running{label}.\n/help for commands.")
        return

    # Command shortcuts
    cmd_map = {
        "/analyzeads": "analyze ads",
        "/ads7d":      "analyze ads last 7 days",
        "/adsmonth":   "analyze ads last month",
        "/cleandata":  "clean my data",
        "/brief":      "morning brief",
    }
    if t in cmd_map:
        t = cmd_map[t]

    # Route to skill or chat
    skill = _route(t)

    if skill == "meta":
        _send("🚀 Running Meta Ads analysis... Check your dashboard for live progress.")
        threading.Thread(target=_run_skill, args=("cm_meta", _META_SKILL, t), daemon=True).start()
    elif skill == "data":
        _send("🧹 Running data cleaning agent... Check your dashboard for live progress.")
        threading.Thread(target=_run_skill, args=("cm_data", _DATA_SKILL, t), daemon=True).start()
    elif skill == "brief":
        _send("☀️ Generating your morning brief...")
        threading.Thread(target=_run_skill, args=("cm_brief", _BRIEF_SKILL, t), daemon=True).start()
    elif skill == "copy":
        threading.Thread(target=_run_skill, args=("cm_copy", _COPY_SKILL, t), daemon=True).start()
    elif skill == "anomaly":
        threading.Thread(target=_run_skill, args=("cm_anomaly", _ANOMALY_SKILL, t), daemon=True).start()
    else:
        # Context-aware chat
        if not ctx.get("setup_done") and not ctx.get("company"):
            _send(
                "I can answer that — but I'll give much better advice once I know your business.\n\n"
                "Run /setup first (2 minutes), then ask again."
            )
            return
        threading.Thread(target=_run_general, args=(t,), daemon=True).start()


# ── Long-polling loop ─────────────────────────────────────────────────────────

def _poll():
    if not BOT_TOKEN:
        print("[bot] ERROR: TELEGRAM_BOT_TOKEN not set in ~/.openclaw/clawmarketer.env")
        sys.exit(1)

    ctx     = _load_context()
    company = ctx.get("company", "")
    label   = f" for {company}" if company else ""
    print(f"[bot] ClawMarketer starting{label} — polling...")
    _set_commands()

    # Startup message
    if company:
        _send(f"✅ *ClawMarketer back online* — ready for {company}.\nSend /help for commands.")
    else:
        _send("✅ *ClawMarketer started.*\nRun /setup to configure your business profile.")

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
                offset   = update["update_id"] + 1
                msg      = update.get("message", {})
                text     = msg.get("text", "")
                chat_id  = str(msg.get("chat", {}).get("id", ""))
                if text and chat_id:
                    print(f"[bot] {chat_id}: {text!r}")
                    threading.Thread(
                        target=_handle_message, args=(text, chat_id), daemon=True
                    ).start()

        except requests.exceptions.ReadTimeout:
            continue
        except KeyboardInterrupt:
            print("\n[bot] Stopped.")
            break
        except Exception as e:
            print(f"[bot] Poll error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    _poll()
