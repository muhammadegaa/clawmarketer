"""
ClawMarketer — Meta Ads Intelligence Agent for OpenClaw
=========================================================
Place this file in your OpenClaw skills/ directory.
Drop clawmarketer.env into your openclaw/ root directory.

What this agent does end-to-end:
  1. Fetches live campaign data from Meta Ads API
  2. Cleans and normalises the raw data
  3. Analyzes performance (CTR, CPC, ROAS, anomalies)
  4. Generates spend/CTR/ROAS charts (PNG)
  5. Exports clean data as CSV
  6. Generates AI report (Groq)
  7. Sends everything to Telegram (summary + charts + CSV)
  8. Pushes live progress to your ClawMarketer dashboard

Telegram triggers:
  "analyze ads"              → last 30 days
  "analyze ads last 7 days"  → last 7 days
  "analyze ads last month"   → last month
  "analyze ads last quarter" → last quarter

OpenClaw skills config (openclaw/skills.json):
  {
    "skills": [
      {
        "name": "meta_ads",
        "trigger": ["analyze ads", "run ads report", "meta ads report"],
        "handler": "skills/openclaw_agent.handle",
        "description": "Analyze Meta Ads performance and send report to Telegram"
      }
    ]
  }
"""

import os
import sys
import uuid
import tempfile
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load the ClawMarketer config file
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "clawmarketer.env"))

CLAWMARKETER_URL     = os.getenv("CLAWMARKETER_URL", "https://clawmarketer.vercel.app")
CLAWMARKETER_USER_ID = os.getenv("CLAWMARKETER_USER_ID", "")
META_ACCESS_TOKEN    = os.getenv("META_ACCESS_TOKEN", "")
META_AD_ACCOUNT_ID   = os.getenv("META_AD_ACCOUNT_ID", "")
GROQ_API_KEY         = os.getenv("GROQ_API_KEY", "")

# Add clawmarketer repo to path so agents/ is importable
_repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo_dir not in sys.path:
    sys.path.insert(0, _repo_dir)


# ── Progress ──────────────────────────────────────────────────────────────────

def _push(run_id: str, stage: int, status: str, message: str,
          done: bool = False, result: dict = None):
    payload = {
        "user_id": CLAWMARKETER_USER_ID,
        "run_id":  run_id,
        "stage":   stage,
        "status":  status,
        "message": message,
        "done":    done,
    }
    if result:
        payload["result"] = result

    try:
        requests.post(f"{CLAWMARKETER_URL}/api/agent/push", json=payload, timeout=10)
    except Exception as e:
        print(f"  [dashboard] Could not push update: {e}")

    icon = {"running": "⚙️", "done": "✅", "error": "❌"}.get(status, "•")
    print(f"  {icon} [{stage}/4] {message}")


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(date_preset: str = "last_30d") -> str:
    from agents.fetcher        import fetch
    from agents.cleaner        import clean
    from agents.analyzer       import run as analyze
    from agents.reporter       import generate
    from agents.charter        import generate_all as make_charts
    from agents.telegram_sender import send_message, send_photo, send_document

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    print(f"\n[ClawMarketer] Meta Ads Agent — run {run_id}")

    # ── Stage 1: Fetch ──────────────────────────────────────────────────────
    _push(run_id, 1, "running", f"Fetching Meta Ads data ({date_preset.replace('_',' ')})...")
    try:
        df_raw = fetch(
            access_token=META_ACCESS_TOKEN,
            ad_account_id=META_AD_ACCOUNT_ID,
            date_preset=date_preset,
        )
    except Exception as e:
        msg = f"Fetch failed: {e}"
        _push(run_id, 1, "error", msg)
        return f"❌ {msg}"

    # ── Stage 2: Clean ──────────────────────────────────────────────────────
    _push(run_id, 2, "running", f"Cleaning {len(df_raw)} rows...")
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            df_raw.to_csv(tmp.name, index=False)
            tmp_path = tmp.name
        df, clean_stats = clean(tmp_path)
        os.unlink(tmp_path)
    except Exception as e:
        msg = f"Clean failed: {e}"
        _push(run_id, 2, "error", msg)
        return f"❌ {msg}"

    # ── Stage 3: Analyze + Charts ───────────────────────────────────────────
    _push(run_id, 3, "running", f"Analyzing {clean_stats['clean_rows']} campaigns...")
    try:
        results  = analyze(df)
        charts, csv_path = make_charts(results)
    except Exception as e:
        msg = f"Analysis failed: {e}"
        _push(run_id, 3, "error", msg)
        return f"❌ {msg}"

    # ── Stage 4: AI Report + Send ───────────────────────────────────────────
    _push(run_id, 4, "running", "Generating AI report...")
    try:
        report_text = generate(results, GROQ_API_KEY)
    except Exception as e:
        report_text = f"AI report unavailable: {e}"

    # Push final result to dashboard
    o = results.get("overall", {})
    _push(run_id, 4, "done", "Report ready ✅", done=True, result={
        "total_spend":   o.get("total_spend", 0),
        "overall_ctr":   o.get("overall_ctr", 0),
        "avg_roas":      o.get("avg_roas", 0),
        "num_campaigns": o.get("num_campaigns", 0),
        "report_text":   report_text,
        "anomalies":     results.get("anomalies", []),
    })

    # ── Send to Telegram ────────────────────────────────────────────────────
    anomaly_lines = ""
    if results.get("anomalies"):
        anomaly_lines = "\n\n⚠️ *Anomalies detected:*\n" + "\n".join(
            f"• {a}" for a in results["anomalies"][:3]
        )

    summary = (
        f"✅ *Meta Ads Report — {date_preset.replace('_', ' ').title()}*\n\n"
        f"💰 Total Spend: *${o.get('total_spend', 0):,.2f}*\n"
        f"👁 Impressions: *{o.get('total_impressions', 0):,}*\n"
        f"🖱 Clicks: *{o.get('total_clicks', 0):,}*\n"
        f"📊 CTR: *{o.get('overall_ctr', 0):.2f}%*\n"
        f"💵 CPC: *${o.get('overall_cpc', 0):.2f}*\n"
        f"🎯 ROAS: *{o.get('avg_roas', 0):.2f}x*\n"
        f"📢 Campaigns: *{o.get('num_campaigns', 0)}*"
        f"{anomaly_lines}\n\n"
        f"📋 Full report → {CLAWMARKETER_URL}"
    )

    send_message(summary)

    for chart in charts:
        send_photo(chart)

    if csv_path:
        send_document(csv_path, caption="📎 Clean campaign data export")

    # Send AI report as separate message (it can be long)
    if report_text and "unavailable" not in report_text:
        # Truncate for Telegram (4096 char limit)
        report_preview = report_text[:3800] + ("…" if len(report_text) > 3800 else "")
        send_message(f"🤖 *AI Analysis:*\n\n{report_preview}")

    print(f"\n[ClawMarketer] Done. Report sent to Telegram + dashboard updated.")
    return summary


# ── OpenClaw entry point ──────────────────────────────────────────────────────

def handle(message: str) -> str:
    """
    Called by OpenClaw when a matching Telegram message is received.
    Returns the response text that OpenClaw sends back to the user.
    """
    msg = message.lower()
    preset_map = {
        "last 7":       "last_7d",
        "last week":    "last_7d",
        "last 14":      "last_14d",
        "last month":   "last_month",
        "this month":   "this_month",
        "last quarter": "last_quarter",
    }
    preset = next((v for k, v in preset_map.items() if k in msg), "last_30d")

    # Immediate acknowledgement back to user
    send_ack = f"🚀 Starting Meta Ads analysis ({preset.replace('_', ' ')})...\nCheck your dashboard for live progress."
    from agents.telegram_sender import send_message
    send_message(send_ack)

    return run(date_preset=preset)


if __name__ == "__main__":
    # Direct test: python openclaw_agent.py
    print(run())
