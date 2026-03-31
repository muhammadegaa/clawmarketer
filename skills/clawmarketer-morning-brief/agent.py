"""
ClawMarketer — Morning Brief Agent
===================================
Runs on a schedule (8am daily via launchctl).
Pulls last 7 days of Meta Ads data, summarises performance,
flags budget pacing issues, and sends a digest to Telegram.

No creds needed locally — everything routes through ClawMarketer.
"""

import os
import uuid
import json
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
        "skill": "clawmarketer-morning-brief", "stage": stage,
        "status": status, "message": message, "done": done,
    }
    if result:
        payload["result"] = result
    try:
        requests.post(f"{CLAWMARKETER_URL}/api/agent/push", json=payload, timeout=10)
    except Exception:
        pass
    print(f"  {'✅' if status == 'done' else '⚙️'} [{stage}/3] {message}")


def _fetch_insights(date_preset):
    resp = requests.post(
        f"{CLAWMARKETER_URL}/api/integrations/meta/insights",
        headers=_HEADERS,
        json={"uid": CLAWMARKETER_USER_ID, "date_preset": date_preset},
        timeout=30,
    )
    if resp.status_code != 200:
        return pd.DataFrame()
    return pd.DataFrame(resp.json().get("data", []))


def _ai_brief(summary_text):
    resp = requests.post(
        f"{CLAWMARKETER_URL}/api/ai/complete",
        headers=_HEADERS,
        json={
            "uid": CLAWMARKETER_USER_ID,
            "prompt": (
                "You are a digital marketing analyst writing a morning brief for a business owner. "
                "Be concise, direct, and action-oriented. Max 4 sentences.\n\n"
                f"{summary_text}\n\n"
                "Write 1 sentence on what's going well, 1 on what needs attention today, "
                "and 1 concrete action to take."
            ),
            "temperature": 0.3,
            "max_tokens": 256,
        },
        timeout=30,
    )
    if resp.status_code == 200:
        return resp.json().get("text", "")
    return ""

# ── Analysis ──────────────────────────────────────────────────────────────────

def _normalize(df):
    df.columns = [c.strip().lower() for c in df.columns]
    aliases = {
        "campaign name": "campaign_name", "amount spent (usd)": "spend",
        "clicks (all)": "clicks", "ctr (all)": "ctr",
        "purchase roas (return on ad spend)": "roas", "results": "conversions",
    }
    df = df.rename(columns={c: aliases[c] for c in df.columns if c in aliases})
    for col in ["spend", "clicks", "ctr", "roas", "conversions", "impressions"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r"[,$%\s]", "", regex=True),
                errors="coerce",
            )
    if "campaign_name" in df.columns:
        df = df[df["campaign_name"].notna()]
        df = df[~df["campaign_name"].str.lower().str.contains("total|report|summary", na=False)]
    return df.dropna(how="all")


def _analyse(df):
    out = {}

    if "spend" in df.columns:
        out["total_spend_7d"] = round(df["spend"].sum(), 2)
        out["daily_avg_spend"] = round(out["total_spend_7d"] / 7, 2)

    if "impressions" in df.columns:
        out["total_impressions"] = int(df["impressions"].sum())

    if "clicks" in df.columns and "impressions" in df.columns:
        total_clicks = df["clicks"].sum()
        total_imp    = df["impressions"].sum()
        out["overall_ctr"] = round(total_clicks / total_imp * 100, 2) if total_imp else 0

    if "roas" in df.columns:
        valid = df["roas"].dropna()
        if not valid.empty:
            out["avg_roas"] = round(valid.mean(), 2)

    # Top / bottom by ROAS
    if "roas" in df.columns and "campaign_name" in df.columns:
        roas_df = df[df["roas"].notna() & (df["roas"] > 0)].copy()
        if not roas_df.empty:
            top = roas_df.loc[roas_df["roas"].idxmax()]
            bot = roas_df.loc[roas_df["roas"].idxmin()]
            out["top_campaign"]    = {"name": str(top["campaign_name"]), "roas": round(float(top["roas"]), 2)}
            out["bottom_campaign"] = {"name": str(bot["campaign_name"]), "roas": round(float(bot["roas"]), 2)}

    # Flags
    flags = []
    if "roas" in df.columns:
        losing = df[(df["roas"].notna()) & (df["roas"] < 1.0)]
        for _, r in losing.iterrows():
            flags.append(f"ROAS {r['roas']:.2f}x → *{r['campaign_name']}* losing money")
    if "ctr" in df.columns:
        low_ctr = df[(df["ctr"].notna()) & (df["ctr"] < 0.5)]
        for _, r in low_ctr.iterrows():
            flags.append(f"CTR {r['ctr']:.2f}% → *{r['campaign_name']}* needs creative refresh")
    out["flags"] = flags[:3]  # top 3 only

    return out

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    run_id = "brief_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:4]
    print(f"\n[ClawMarketer] Morning Brief — {datetime.utcnow().strftime('%a %d %b %Y')}")

    _push(run_id, 1, "running", "Fetching last 7 days...")
    df_raw = _fetch_insights("last_7d")
    if df_raw.empty:
        _push(run_id, 1, "error", "No data available")
        _send("⚠️ Morning Brief: could not fetch data. Check your Meta connection at the app.")
        return
    _push(run_id, 1, "done", f"{len(df_raw)} campaigns fetched")

    _push(run_id, 2, "running", "Analysing performance...")
    df   = _normalize(df_raw)
    data = _analyse(df)
    _push(run_id, 2, "done", "Analysis complete")

    _push(run_id, 3, "running", "Generating brief...")

    # Build the summary string for the AI
    summary_for_ai = (
        f"Last 7 days: spend ${data.get('total_spend_7d', 0):,.2f} "
        f"(${data.get('daily_avg_spend', 0):,.2f}/day avg), "
        f"impressions {data.get('total_impressions', 0):,}, "
        f"CTR {data.get('overall_ctr', 0):.2f}%, "
        f"avg ROAS {data.get('avg_roas', 0):.2f}x. "
    )
    if data.get("top_campaign"):
        summary_for_ai += f"Best campaign: {data['top_campaign']['name']} ({data['top_campaign']['roas']}x ROAS). "
    if data.get("bottom_campaign"):
        summary_for_ai += f"Worst campaign: {data['bottom_campaign']['name']} ({data['bottom_campaign']['roas']}x ROAS). "
    if data.get("flags"):
        summary_for_ai += "Issues: " + "; ".join(data["flags"])

    ai_insight = _ai_brief(summary_for_ai)

    # Build Telegram message
    today = datetime.utcnow().strftime("%a %d %b")
    flag_lines = "\n".join(f"⚠️ {f}" for f in data.get("flags", []))

    msg = (
        f"☀️ *Morning Brief — {today}*\n\n"
        f"*Last 7 days:*\n"
        f"💰 Spend: *${data.get('total_spend_7d', 0):,.2f}* "
        f"(${data.get('daily_avg_spend', 0):,.2f}/day)\n"
        f"📊 CTR: *{data.get('overall_ctr', 0):.2f}%*\n"
        f"🎯 Avg ROAS: *{data.get('avg_roas', 0):.2f}x*\n"
    )
    if data.get("top_campaign"):
        msg += f"🏆 Top: *{data['top_campaign']['name']}* ({data['top_campaign']['roas']}x)\n"
    if data.get("bottom_campaign"):
        msg += f"📉 Bottom: *{data['bottom_campaign']['name']}* ({data['bottom_campaign']['roas']}x)\n"
    if flag_lines:
        msg += f"\n{flag_lines}\n"
    if ai_insight:
        msg += f"\n💡 {ai_insight}"

    _send(msg)
    _push(run_id, 3, "done", "Brief sent ✅", done=True, result={
        "total_spend_7d":    data.get("total_spend_7d", 0),
        "avg_roas":          data.get("avg_roas", 0),
        "overall_ctr":       data.get("overall_ctr", 0),
        "flags":             data.get("flags", []),
        "report_text":       ai_insight,
    })
    print("[ClawMarketer] Morning brief sent.")


def handle(message: str):
    run()


if __name__ == "__main__":
    run()
