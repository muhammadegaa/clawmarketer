"""
ClawMarketer — Ad Copy Generator
==================================
Triggered from Telegram: "write ads for [product] targeting [audience]"
Returns 5 ad variations: hook + body + CTA, ready to paste into Meta/Google.

No 3rd party calls — AI runs through ClawMarketer server.
"""

import os
import re
import uuid
import requests
from datetime import datetime
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────

_env_path = os.path.expanduser("~/.openclaw/clawmarketer.env")
load_dotenv(_env_path)

CLAWMARKETER_URL       = os.getenv("CLAWMARKETER_URL", "https://clawmarketer.vercel.app")
CLAWMARKETER_USER_ID   = os.getenv("CLAWMARKETER_USER_ID", "")
CLAWMARKETER_API_TOKEN = os.getenv("CLAWMARKETER_API_TOKEN", "")
TELEGRAM_BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID       = os.getenv("TELEGRAM_CHAT_ID", "")

_HEADERS = {"Authorization": f"Bearer {CLAWMARKETER_API_TOKEN}"}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _send(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=15,
        )
    except Exception as e:
        print(f"  [telegram] {e}")


def _push(run_id, stage, status, message, done=False, result=None):
    payload = {
        "user_id": CLAWMARKETER_USER_ID, "run_id": run_id,
        "skill": "clawmarketer-copy", "stage": stage,
        "status": status, "message": message, "done": done,
    }
    if result:
        payload["result"] = result
    try:
        requests.post(f"{CLAWMARKETER_URL}/api/agent/push", json=payload, timeout=10)
    except Exception:
        pass

# ── Parse intent ──────────────────────────────────────────────────────────────

def _parse(message: str):
    """Extract product and audience from natural language."""
    msg = message.lower()

    # Try "for [product] targeting [audience]"
    m = re.search(r"(?:for|about)\s+(.+?)\s+(?:targeting|for|aimed at|to)\s+(.+)", msg)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # Try "for [product]" alone
    m = re.search(r"(?:write ads?|generate copy|ad copy|create ads?)\s+(?:for|about)\s+(.+)", msg)
    if m:
        return m.group(1).strip(), "general audience"

    # Fallback: everything after the trigger phrase is the product
    for trigger in ["write ads for", "generate copy for", "ad copy for", "create ads for",
                    "write ads", "generate ads", "ad variations for"]:
        if trigger in msg:
            product = message[msg.index(trigger) + len(trigger):].strip()
            return product, "general audience"

    return message.strip(), "general audience"

# ── Generate ──────────────────────────────────────────────────────────────────

def _generate(product: str, audience: str, platform: str = "Meta") -> str:
    prompt = f"""You are an expert direct-response copywriter specialising in {platform} ads.

Product/Service: {product}
Target Audience: {audience}

Write exactly 5 ad copy variations. Each variation must have:
- **Hook** (1 line — grabs attention, creates curiosity or urgency)
- **Body** (1-2 sentences — benefit-focused, speaks to the audience's pain or desire)
- **CTA** (4-6 words — clear action)

Format each like this:
---
**Variation 1**
Hook: [hook text]
Body: [body text]
CTA: [CTA text]
---

Write for conversion, not awareness. Use specific language. No fluff."""

    resp = requests.post(
        f"{CLAWMARKETER_URL}/api/ai/complete",
        headers=_HEADERS,
        json={
            "uid": CLAWMARKETER_USER_ID,
            "prompt": prompt,
            "temperature": 0.7,
            "max_tokens": 1200,
        },
        timeout=45,
    )
    if resp.status_code == 200:
        return resp.json().get("text", "")
    return ""

# ── Main ──────────────────────────────────────────────────────────────────────

def run(product: str, audience: str):
    run_id = "copy_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:4]
    print(f"\n[ClawMarketer] Ad Copy — {product[:40]} → {audience[:30]}")

    _push(run_id, 1, "running", f"Generating copy for: {product[:50]}")
    copy = _generate(product, audience)

    if not copy:
        _send("❌ Could not generate copy. Try again in a moment.")
        _push(run_id, 1, "error", "Generation failed", done=True)
        return

    msg = (
        f"✍️ *Ad Copy — {product[:40]}*\n"
        f"🎯 Audience: _{audience}_\n\n"
        f"{copy[:3800]}"
    )
    _send(msg)
    _push(run_id, 1, "done", "5 variations generated ✅", done=True, result={
        "product":     product,
        "audience":    audience,
        "report_text": copy,
    })
    print("[ClawMarketer] Copy sent.")


def handle(message: str):
    product, audience = _parse(message)
    _send(f"✍️ Writing 5 ad variations for *{product}*...\nTargeting: _{audience}_")
    run(product, audience)


if __name__ == "__main__":
    import sys
    product  = sys.argv[1] if len(sys.argv) > 1 else "premium coffee subscription"
    audience = sys.argv[2] if len(sys.argv) > 2 else "busy professionals aged 25-40"
    run(product, audience)
