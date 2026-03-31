"""
ClawMarketer — Meta Ads Intelligence Agent
==========================================
Thin client: all data and AI comes from the ClawMarketer server.
Local work: clean, analyze, make charts, send to Telegram.

Required env (~/.openclaw/clawmarketer.env):
  CLAWMARKETER_URL
  CLAWMARKETER_USER_ID
  CLAWMARKETER_API_TOKEN
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID
  DATA_DIR   (optional — folder to scan for local CSV exports)
"""

import os
import sys
import uuid
import json
import tempfile
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────

_skill_dir = os.path.dirname(os.path.abspath(__file__))
_env_path  = os.path.expanduser("~/.openclaw/clawmarketer.env")
if not os.path.exists(_env_path):
    _env_path = os.path.join(_skill_dir, "clawmarketer.env")
load_dotenv(_env_path)

CLAWMARKETER_URL      = os.getenv("CLAWMARKETER_URL", "https://clawmarketer.vercel.app")
CLAWMARKETER_USER_ID  = os.getenv("CLAWMARKETER_USER_ID", "")
CLAWMARKETER_API_TOKEN = os.getenv("CLAWMARKETER_API_TOKEN", "")
TELEGRAM_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")

_HEADERS = {"Authorization": f"Bearer {CLAWMARKETER_API_TOKEN}"}

# ── Server calls ──────────────────────────────────────────────────────────────

def _fetch_insights(date_preset: str):
    """Request campaign data from ClawMarketer. Server handles Meta API or sample fallback."""
    try:
        resp = requests.post(
            f"{CLAWMARKETER_URL}/api/integrations/meta/insights",
            headers=_HEADERS,
            json={"uid": CLAWMARKETER_USER_ID, "date_preset": date_preset},
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  [insights] Server returned {resp.status_code}")
            return pd.DataFrame(), "error"
        body = resp.json()
        df = pd.DataFrame(body["data"])
        print(f"  [insights] {body['rows']} rows from {body['data_source']}")
        return df, body["data_source"]
    except Exception as e:
        print(f"  [insights] Failed: {e}")
        return pd.DataFrame(), "error"


def _generate_report_via_api(analysis: dict) -> str:
    """Request AI report from ClawMarketer. Server holds the Groq key."""
    try:
        resp = requests.post(
            f"{CLAWMARKETER_URL}/api/ai/report",
            headers=_HEADERS,
            json={
                "uid": CLAWMARKETER_USER_ID,
                "analysis": {
                    "overall":   analysis.get("overall", {}),
                    "anomalies": analysis.get("anomalies", []),
                },
            },
            timeout=45,
        )
        if resp.status_code != 200:
            return f"AI report unavailable: server {resp.status_code}"
        return resp.json().get("report", "")
    except Exception as e:
        return f"AI report unavailable: {e}"

# ── Progress ──────────────────────────────────────────────────────────────────

def _push(run_id, stage, status, message, done=False, result=None, attachments=None):
    payload = {
        "user_id": CLAWMARKETER_USER_ID, "run_id": run_id,
        "skill": "clawmarketer-meta", "stage": stage,
        "status": status, "message": message, "done": done,
    }
    if result:      payload["result"]      = result
    if attachments: payload["attachments"] = attachments
    try:
        requests.post(f"{CLAWMARKETER_URL}/api/agent/push", json=payload, timeout=10)
    except Exception:
        pass
    icon = {"running": "⚙️", "done": "✅", "error": "❌"}.get(status, "•")
    print(f"  {icon} [{stage}/4] {message}")

# ── Telegram ──────────────────────────────────────────────────────────────────

def _tg_post(method, **kwargs):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, timeout=30, **kwargs)
        data = resp.json()
        if data.get("ok"):
            return data.get("result", {}).get("message_id")
    except Exception as e:
        print(f"  [telegram] {method} failed: {e}")
    return None

def send_message(text):
    return _tg_post("sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"})

def send_photo(path):
    with open(path, "rb") as f:
        return _tg_post("sendPhoto", data={"chat_id": TELEGRAM_CHAT_ID}, files={"photo": f})

def send_document(path, caption=""):
    with open(path, "rb") as f:
        return _tg_post("sendDocument", data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption}, files={"document": f})

# ── Local CSV fallback ────────────────────────────────────────────────────────

def _load_local_csv():
    """Scan DATA_DIR for a Meta Ads-like CSV export (user's own file)."""
    data_dir = os.path.expanduser(os.getenv("DATA_DIR", "~/Documents/data"))
    if not os.path.isdir(data_dir):
        return pd.DataFrame(), ""
    ad_keywords = ["meta", "ads", "facebook", "campaign", "fb", "adset"]
    ad_columns  = ["spend", "impressions", "campaign", "clicks", "ctr", "cpc", "cpm", "roas"]
    candidates  = []
    for fname in os.listdir(data_dir):
        if fname.startswith("clean_") or not fname.lower().endswith(".csv"):
            continue
        path = os.path.join(data_dir, fname)
        name_score = sum(1 for kw in ad_keywords if kw in fname.lower())
        try:
            df = pd.read_csv(path, nrows=3)
            cols = " ".join(c.lower() for c in df.columns)
            col_score = sum(1 for kw in ad_columns if kw in cols)
            if col_score >= 2:
                candidates.append((name_score + col_score, path, fname))
        except Exception:
            pass
    if not candidates:
        return pd.DataFrame(), ""
    candidates.sort(reverse=True)
    best_path, best_name = candidates[0][1], candidates[0][2]
    try:
        df = pd.read_csv(best_path)
        print(f"  [data] Using local CSV: {best_name} ({len(df)} rows)")
        return df, best_name
    except Exception as e:
        print(f"  [data] Failed to read {best_name}: {e}")
        return pd.DataFrame(), ""

# ── Data cleaner ──────────────────────────────────────────────────────────────

COLUMN_ALIASES = {
    "campaign name":"campaign_name","amount spent (usd)":"spend","spend":"spend",
    "impressions":"impressions","reach":"reach","clicks (all)":"clicks","clicks":"clicks",
    "ctr (all)":"ctr","ctr":"ctr","cpc (all)":"cpc","cpc":"cpc",
    "cpm (cost per 1,000 impressions)":"cpm","cpm":"cpm",
    "results":"conversions","conversions":"conversions",
    "cost per result":"cost_per_conversion","purchase roas (return on ad spend)":"roas","roas":"roas",
    "reporting starts":"date_start","reporting ends":"date_end",
    "objective":"objective","frequency":"frequency",
}

def clean(df_raw):
    df = df_raw.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={c: COLUMN_ALIASES[c] for c in df.columns if c in COLUMN_ALIASES})
    if "campaign_name" in df.columns:
        df = df[df["campaign_name"].notna()]
        df = df[~df["campaign_name"].str.lower().str.contains("total|report|summary", na=False)]
    df = df.reset_index(drop=True)
    num_cols = ["spend","impressions","reach","clicks","ctr","cpc","cpm","conversions","cost_per_conversion","roas","frequency"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(r"[,$%\s]","",regex=True)
                    .replace({"nan":None,"":None,"-":None,"N/A":None}),
                errors="coerce"
            )
    for col in ["date_start","date_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df = df.dropna(how="all")
    return df, {"clean_rows": len(df), "dropped_rows": len(df_raw) - len(df)}

# ── Analyzer ──────────────────────────────────────────────────────────────────

def _campaign_summary(df):
    if "campaign_name" not in df.columns: return pd.DataFrame()
    agg = {}
    for c in ["spend","impressions","clicks","conversions","reach"]:
        if c in df.columns: agg[c] = "sum"
    for c in ["roas","ctr","cpc","cpm","frequency"]:
        if c in df.columns: agg[c] = "mean"
    s = df.groupby("campaign_name").agg(agg).reset_index()
    if "clicks" in s and "impressions" in s:
        s["ctr_calc"] = (s["clicks"] / s["impressions"] * 100).round(2)
    if "spend" in s and "clicks" in s:
        s["cpc_calc"] = (s["spend"] / s["clicks"]).round(2)
    if "spend" in s and "impressions" in s:
        s["cpm_calc"] = (s["spend"] / s["impressions"] * 1000).round(2)
    return s.sort_values("spend", ascending=False)

def analyze(df):
    summary = _campaign_summary(df)
    overall = {}
    for col, key in [("spend","total_spend"),("impressions","total_impressions"),("clicks","total_clicks"),("conversions","total_conversions"),("reach","total_reach")]:
        if col in df.columns:
            val = df[col].sum()
            overall[key] = round(val, 2) if col == "spend" else int(val)
    if overall.get("total_clicks") and overall.get("total_impressions"):
        overall["overall_ctr"] = round(overall["total_clicks"] / overall["total_impressions"] * 100, 2)
    if overall.get("total_spend") and overall.get("total_clicks"):
        overall["overall_cpc"] = round(overall["total_spend"] / overall["total_clicks"], 2)
    if "roas" in df.columns:
        valid = df["roas"].dropna()
        if not valid.empty:
            overall["avg_roas"] = round(valid.mean(), 2)
    if "campaign_name" in df.columns:
        overall["num_campaigns"] = df["campaign_name"].nunique()

    flags = []
    ctr_col = "ctr_calc" if "ctr_calc" in summary.columns else ("ctr" if "ctr" in summary.columns else None)
    if ctr_col:
        for _, row in summary[summary[ctr_col] < 0.5].iterrows():
            flags.append(f"Low CTR ({row[ctr_col]}%) on: {row['campaign_name']}")
    if "roas" in summary.columns:
        for _, row in summary[(summary["roas"].notna()) & (summary["roas"] < 1.0)].iterrows():
            flags.append(f"ROAS below 1.0 ({row['roas']:.2f}x) — losing money on: {row['campaign_name']}")

    return {"overall": overall, "campaign_summary": summary, "anomalies": flags}

# ── Charts ────────────────────────────────────────────────────────────────────

def make_charts(analysis):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

    summary = analysis.get("campaign_summary")
    out_dir = tempfile.mkdtemp()
    charts  = []

    if summary is None or summary.empty:
        return charts, None

    DARK  = "#0f0f0f"; CARD = "#1a1a1a"; INDIGO = "#6366f1"
    GREEN = "#4ade80"; YELLOW = "#fbbf24"; RED = "#f87171"
    TEXT  = "#e2e8f0"; SUB = "#9ca3af"

    def _fig(title):
        fig, ax = plt.subplots(figsize=(10, 5))
        fig.patch.set_facecolor(DARK); ax.set_facecolor(CARD)
        ax.set_title(title, color=TEXT, fontsize=13, fontweight="bold", pad=14)
        ax.tick_params(colors=SUB, labelsize=9)
        for sp in ["top", "right"]: ax.spines[sp].set_visible(False)
        for sp in ["bottom", "left"]: ax.spines[sp].set_color("#2a2a2a")
        return fig, ax

    def _short(n): return n[:18] + "…" if len(n) > 18 else n

    if "spend" in summary.columns:
        df = summary.nlargest(8, "spend")[["campaign_name", "spend"]].copy()
        df["label"] = df["campaign_name"].apply(_short)
        fig, ax = _fig("Spend by Campaign (USD)")
        bars = ax.barh(df["label"], df["spend"], color=INDIGO, height=0.6)
        ax.bar_label(bars, labels=[f"${v:,.0f}" for v in df["spend"]], color=SUB, fontsize=8, padding=4)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
        ax.invert_yaxis(); ax.tick_params(axis="y", colors=TEXT); fig.tight_layout()
        path = os.path.join(out_dir, "chart_spend.png")
        fig.savefig(path, dpi=130, bbox_inches="tight", facecolor=DARK); plt.close(fig); charts.append(path)

    ctr_col = "ctr_calc" if "ctr_calc" in summary.columns else ("ctr" if "ctr" in summary.columns else None)
    if ctr_col:
        df = summary.copy(); df["label"] = df["campaign_name"].apply(_short)
        df = df.sort_values(ctr_col, ascending=True).tail(8)
        fig, ax = _fig("CTR by Campaign (%)")
        colors = [GREEN if v >= 2 else YELLOW if v >= 1 else RED for v in df[ctr_col]]
        bars = ax.barh(df["label"], df[ctr_col], color=colors, height=0.6)
        ax.bar_label(bars, labels=[f"{v:.2f}%" for v in df[ctr_col]], color=SUB, fontsize=8, padding=4)
        ax.tick_params(axis="y", colors=TEXT); fig.tight_layout()
        path = os.path.join(out_dir, "chart_ctr.png")
        fig.savefig(path, dpi=130, bbox_inches="tight", facecolor=DARK); plt.close(fig); charts.append(path)

    if "roas" in summary.columns:
        df = summary[summary["roas"].notna() & (summary["roas"] > 0)].copy()
        if not df.empty:
            df["label"] = df["campaign_name"].apply(_short)
            df = df.sort_values("roas", ascending=True)
            fig, ax = _fig("ROAS by Campaign")
            colors = [GREEN if v >= 2 else YELLOW if v >= 1 else RED for v in df["roas"]]
            bars = ax.barh(df["label"], df["roas"], color=colors, height=0.6)
            ax.bar_label(bars, labels=[f"{v:.2f}x" for v in df["roas"]], color=SUB, fontsize=8, padding=4)
            ax.tick_params(axis="y", colors=TEXT); fig.tight_layout()
            path = os.path.join(out_dir, "chart_roas.png")
            fig.savefig(path, dpi=130, bbox_inches="tight", facecolor=DARK); plt.close(fig); charts.append(path)

    csv_path = os.path.join(out_dir, "meta_ads_clean.csv")
    summary.to_csv(csv_path, index=False)
    return charts, csv_path

# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(date_preset="last_30d"):
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    print(f"\n[ClawMarketer] Meta Ads Agent — run {run_id}")

    # ── Step 1: Fetch data from ClawMarketer ──────────────────────────────────
    _push(run_id, 1, "running", "Fetching data from ClawMarketer...")

    df_raw, data_source = _fetch_insights(date_preset)

    # Local CSV as secondary fallback (user's own ad export files)
    if df_raw.empty:
        df_raw, csv_name = _load_local_csv()
        if not df_raw.empty:
            data_source = f"local file: {csv_name}"

    if df_raw.empty:
        _push(run_id, 1, "error", "Could not fetch data — check ClawMarketer connection")
        return "❌ Could not fetch data"

    _push(run_id, 1, "done", f"Loaded {len(df_raw)} rows ({data_source})")

    # ── Step 2: Clean ─────────────────────────────────────────────────────────
    _push(run_id, 2, "running", f"Cleaning {len(df_raw)} rows...")
    try:
        df, clean_stats = clean(df_raw)
    except Exception as e:
        _push(run_id, 2, "error", f"Clean failed: {e}")
        return f"❌ Clean failed: {e}"
    _push(run_id, 2, "done", f"Cleaned — {clean_stats['clean_rows']} campaigns ready")

    # ── Step 3: Analyze + Charts ──────────────────────────────────────────────
    _push(run_id, 3, "running", f"Analyzing {clean_stats['clean_rows']} campaigns...")
    try:
        results          = analyze(df)
        charts, csv_path = make_charts(results)
    except Exception as e:
        _push(run_id, 3, "error", f"Analysis failed: {e}")
        return f"❌ Analysis failed: {e}"
    _push(run_id, 3, "done", f"{len(charts)} charts generated, {len(results.get('anomalies', []))} anomalies found")

    # ── Step 4: AI Report (via server) + Telegram ─────────────────────────────
    _push(run_id, 4, "running", "Generating AI report...")
    report_text = _generate_report_via_api(results)

    o = results.get("overall", {})
    anomaly_lines = ""
    if results.get("anomalies"):
        anomaly_lines = "\n\n⚠️ *Anomalies detected:*\n" + "\n".join(f"• {a}" for a in results["anomalies"][:3])

    summary_msg = (
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

    attachments = []
    send_message(summary_msg)
    for chart in charts:
        mid = send_photo(chart)
        attachments.append({"name": os.path.basename(chart), "type": "photo", "telegram_message_id": mid})
    if csv_path:
        mid = send_document(csv_path, caption="📎 Clean campaign data export")
        attachments.append({"name": os.path.basename(csv_path), "type": "document", "telegram_message_id": mid})
    if report_text and "unavailable" not in report_text:
        send_message(f"🤖 *AI Analysis:*\n\n{report_text[:3800]}")

    _push(run_id, 4, "done", "Report ready ✅", done=True,
        result={
            "total_spend":   o.get("total_spend", 0),
            "overall_ctr":   o.get("overall_ctr", 0),
            "avg_roas":      o.get("avg_roas", 0),
            "num_campaigns": o.get("num_campaigns", 0),
            "report_text":   report_text,
            "anomalies":     results.get("anomalies", []),
        },
        attachments=attachments,
    )
    print(f"\n[ClawMarketer] Done. Report sent to Telegram + dashboard updated.")
    return summary_msg


def handle(message: str) -> str:
    msg = message.lower()
    preset_map = {
        "last 7": "last_7d", "last week": "last_7d", "last 14": "last_14d",
        "last month": "last_month", "this month": "this_month", "last quarter": "last_quarter",
    }
    preset = next((v for k, v in preset_map.items() if k in msg), "last_30d")
    send_message(f"🚀 Starting Meta Ads analysis ({preset.replace('_', ' ')})...\nCheck your dashboard for live progress.")
    return run(date_preset=preset)


if __name__ == "__main__":
    print(run())
