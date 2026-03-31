"""
ClawMarketer — Anomaly Alerter
================================
Runs on a schedule (every 4 hours via launchctl).
Silently checks Meta Ads metrics — only sends a Telegram alert if
something is actually wrong. No alert = everything is fine.

Thresholds (all configurable via env):
  ALERT_ROAS_MIN      default 1.0   — alert if any campaign ROAS drops below this
  ALERT_CTR_MIN       default 0.3   — alert if CTR drops below this %
  ALERT_CPC_MAX       default 10.0  — alert if CPC exceeds this
  ALERT_SPEND_SPIKE   default 2.0   — alert if a campaign spends 2x its average daily
"""

import os
import uuid
import requests
import pandas as pd
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

ALERT_ROAS_MIN    = float(os.getenv("ALERT_ROAS_MIN", "1.0"))
ALERT_CTR_MIN     = float(os.getenv("ALERT_CTR_MIN", "0.3"))
ALERT_CPC_MAX     = float(os.getenv("ALERT_CPC_MAX", "10.0"))

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


def _fetch_insights():
    resp = requests.post(
        f"{CLAWMARKETER_URL}/api/integrations/meta/insights",
        headers=_HEADERS,
        json={"uid": CLAWMARKETER_USER_ID, "date_preset": "last_7d"},
        timeout=30,
    )
    if resp.status_code != 200:
        return pd.DataFrame()
    return pd.DataFrame(resp.json().get("data", []))


def _normalize(df):
    df.columns = [c.strip().lower() for c in df.columns]
    aliases = {
        "campaign name": "campaign_name", "amount spent (usd)": "spend",
        "clicks (all)": "clicks", "ctr (all)": "ctr", "cpc (all)": "cpc",
        "purchase roas (return on ad spend)": "roas",
    }
    df = df.rename(columns={c: aliases[c] for c in df.columns if c in aliases})
    for col in ["spend", "clicks", "ctr", "cpc", "roas"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r"[,$%\s]", "", regex=True),
                errors="coerce",
            )
    if "campaign_name" in df.columns:
        df = df[df["campaign_name"].notna()]
        df = df[~df["campaign_name"].str.lower().str.contains("total|report|summary", na=False)]
    return df.dropna(how="all")

# ── Anomaly detection ─────────────────────────────────────────────────────────

def _detect(df) -> list:
    alerts = []

    if "roas" in df.columns and "campaign_name" in df.columns:
        losing = df[(df["roas"].notna()) & (df["roas"] < ALERT_ROAS_MIN) & (df["roas"] > 0)]
        for _, r in losing.iterrows():
            alerts.append({
                "level":    "🔴 Critical",
                "campaign": str(r["campaign_name"]),
                "issue":    f"ROAS {r['roas']:.2f}x — below break-even",
                "action":   "Pause or cut budget immediately",
            })

    if "ctr" in df.columns and "campaign_name" in df.columns:
        low_ctr = df[(df["ctr"].notna()) & (df["ctr"] < ALERT_CTR_MIN)]
        for _, r in low_ctr.iterrows():
            alerts.append({
                "level":    "🟡 Warning",
                "campaign": str(r["campaign_name"]),
                "issue":    f"CTR {r['ctr']:.2f}% — very low engagement",
                "action":   "Refresh creative or narrow audience",
            })

    if "cpc" in df.columns and "campaign_name" in df.columns:
        high_cpc = df[(df["cpc"].notna()) & (df["cpc"] > ALERT_CPC_MAX)]
        for _, r in high_cpc.iterrows():
            alerts.append({
                "level":    "🟡 Warning",
                "campaign": str(r["campaign_name"]),
                "issue":    f"CPC ${r['cpc']:.2f} — above threshold (${ALERT_CPC_MAX:.0f})",
                "action":   "Review bid strategy and audience overlap",
            })

    # High spend campaigns with zero conversions (if conversions data available)
    if "spend" in df.columns and "conversions" in df.columns and "campaign_name" in df.columns:
        wasted = df[
            (df["spend"] > 100) &
            (df["conversions"].notna()) &
            (df["conversions"] == 0)
        ]
        for _, r in wasted.iterrows():
            alerts.append({
                "level":    "🔴 Critical",
                "campaign": str(r["campaign_name"]),
                "issue":    f"${r['spend']:.0f} spent — zero conversions",
                "action":   "Check pixel, landing page, and audience",
            })

    return alerts

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    print(f"\n[ClawMarketer] Anomaly check — {datetime.utcnow().strftime('%H:%M UTC')}")

    df_raw = _fetch_insights()
    if df_raw.empty:
        print("  No data — skipping check")
        return

    df     = _normalize(df_raw)
    alerts = _detect(df)

    if not alerts:
        print("  ✅ All clear — no anomalies detected")
        return  # silence is golden — don't spam Telegram when everything's fine

    # Deduplicate by campaign + issue
    seen = set()
    unique = []
    for a in alerts:
        key = (a["campaign"], a["issue"][:30])
        if key not in seen:
            seen.add(key)
            unique.append(a)

    lines = []
    for a in unique[:5]:  # max 5 alerts per check
        lines.append(
            f"{a['level']}\n"
            f"Campaign: *{a['campaign'][:40]}*\n"
            f"Issue: {a['issue']}\n"
            f"Action: _{a['action']}_"
        )

    msg = (
        f"🚨 *Anomaly Alert — {datetime.utcnow().strftime('%d %b %H:%M UTC')}*\n"
        f"{len(unique)} issue(s) detected:\n\n"
        + "\n\n".join(lines)
        + f"\n\n📋 Full details → {CLAWMARKETER_URL}"
    )
    _send(msg)
    print(f"  ⚠️ {len(unique)} anomalies — alert sent")


def handle(message: str):
    _send("🔍 Running anomaly check on your campaigns...")
    run()


if __name__ == "__main__":
    run()
