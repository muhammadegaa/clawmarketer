"""
ClawMarketer — Data Cleansing Agent (self-contained)
=====================================================
Self-contained: no external package imports beyond pip dependencies.
Install deps: pip install requests pandas openpyxl groq python-dotenv

Config: ~/.openclaw/clawmarketer.env
  DATA_DIR=~/Documents/data   (directory to scan for CSV/Excel files)
"""

import os
import sys
import uuid
import glob
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────

_skill_dir = os.path.dirname(os.path.abspath(__file__))
_env_path  = os.path.expanduser("~/.openclaw/clawmarketer.env")
if not os.path.exists(_env_path):
    _env_path = os.path.join(_skill_dir, "clawmarketer.env")
load_dotenv(_env_path)

CLAWMARKETER_URL       = os.getenv("CLAWMARKETER_URL", "https://clawmarketer.vercel.app")
CLAWMARKETER_USER_ID   = os.getenv("CLAWMARKETER_USER_ID", "")
CLAWMARKETER_API_TOKEN = os.getenv("CLAWMARKETER_API_TOKEN", "")
TELEGRAM_BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID       = os.getenv("TELEGRAM_CHAT_ID", "")
DATA_DIR               = os.path.expanduser(os.getenv("DATA_DIR", "~/Documents/data"))

_HEADERS = {"Authorization": f"Bearer {CLAWMARKETER_API_TOKEN}"}

# ── Progress ──────────────────────────────────────────────────────────────────

def _push(run_id, stage, status, message, done=False, result=None, attachments=None):
    payload = {
        "user_id": CLAWMARKETER_USER_ID, "run_id": run_id,
        "skill": "clawmarketer-data", "stage": stage,
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

def send_document(path, caption=""):
    with open(path, "rb") as f:
        return _tg_post("sendDocument", data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption}, files={"document": f})

# ── Clean a single file ───────────────────────────────────────────────────────

def _clean_file(path):
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        return None

    original_rows = len(df)

    df = df.dropna(how="all").reset_index(drop=True)
    df = df.loc[:, df.notna().any()]
    df = df.drop_duplicates().reset_index(drop=True)

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    df.columns = [c.strip().lower().replace(" ","_").replace("/","_") for c in df.columns]

    clean_dir  = os.path.dirname(path)
    clean_name = "clean_" + os.path.basename(path).replace(".xlsx",".csv").replace(".xls",".csv")
    clean_path = os.path.join(clean_dir, clean_name)
    df.to_csv(clean_path, index=False)

    return {
        "file":          os.path.basename(path),
        "clean_path":    clean_path,
        "original_rows": original_rows,
        "clean_rows":    len(df),
        "rows_removed":  original_rows - len(df),
        "columns":       len(df.columns),
    }

# ── AI summary (via ClawMarketer server) ─────────────────────────────────────

def _ai_summary(file_summaries):
    """Request AI summary from ClawMarketer. No Groq key needed locally."""
    prompt = (
        "You are a data analyst. A data cleaning agent just processed these files:\n\n"
        f"{json.dumps(file_summaries, indent=2)}\n\n"
        "Write a brief 3-4 sentence summary for a business owner covering: "
        "what files were cleaned, how many rows were removed and why, "
        "and whether the data quality looks good. Be direct."
    )
    try:
        resp = requests.post(
            f"{CLAWMARKETER_URL}/api/ai/complete",
            headers=_HEADERS,
            json={"uid": CLAWMARKETER_USER_ID, "prompt": prompt, "temperature": 0.3, "max_tokens": 512},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("text", "")
        return ""
    except Exception as e:
        return f"AI summary unavailable: {e}"

# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(data_dir=None):
    data_dir = data_dir or DATA_DIR
    run_id   = "data_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    print(f"\n[ClawMarketer] Data Cleansing Agent — run {run_id}")

    _push(run_id, 1, "running", f"Scanning {data_dir}...")
    if not os.path.isdir(data_dir):
        msg = f"Directory not found: {data_dir}. Set DATA_DIR in clawmarketer.env"
        _push(run_id, 1, "error", msg)
        return f"❌ {msg}"

    files = []
    for p in ["*.csv","*.xlsx","*.xls"]:
        files.extend(glob.glob(os.path.join(data_dir, p)))
    files = [f for f in files if not os.path.basename(f).startswith("clean_")]

    if not files:
        msg = f"No CSV/Excel files found in {data_dir}"
        _push(run_id, 1, "error", msg)
        return f"📂 {msg}"

    _push(run_id, 1, "running", f"Found {len(files)} file(s) to process")

    _push(run_id, 2, "running", f"Cleaning {len(files)} file(s)...")
    summaries   = []
    clean_paths = []
    for f in files:
        try:
            result = _clean_file(f)
            if result:
                summaries.append(result)
                clean_paths.append(result["clean_path"])
                print(f"    ✓ {result['file']} — {result['rows_removed']} rows removed")
        except Exception as e:
            print(f"    ✗ {os.path.basename(f)} — {e}")

    if not summaries:
        msg = "Could not clean any files"
        _push(run_id, 2, "error", msg)
        return f"❌ {msg}"

    _push(run_id, 3, "running", "Generating data quality summary...")
    total_removed  = sum(s["rows_removed"] for s in summaries)
    total_original = sum(s["original_rows"] for s in summaries)

    _push(run_id, 4, "running", "Generating AI summary and sending to Telegram...")
    ai_summary = _ai_summary(summaries)

    file_lines = "\n".join(
        f"• `{s['file']}` — {s['original_rows']} → {s['clean_rows']} rows"
        + (f" *(removed {s['rows_removed']})*" if s["rows_removed"] > 0 else "")
        for s in summaries
    )

    message = (
        f"🧹 *Data Cleaning Complete*\n\n"
        f"📁 Directory: `{data_dir}`\n"
        f"📄 Files processed: *{len(summaries)}*\n"
        f"🗑 Total rows removed: *{total_removed}* / {total_original}\n\n"
        f"*File breakdown:*\n{file_lines}"
    )
    if ai_summary:
        message += f"\n\n🤖 *AI Summary:*\n{ai_summary}"
    message += f"\n\nClean files saved with `clean_` prefix in the same folder."

    send_message(message)

    attachments = []
    for path in clean_paths:
        fname = os.path.basename(path)
        mid   = send_document(path, caption=f"📎 {fname}")
        attachments.append({"name": fname, "type": "document", "telegram_message_id": mid})

    _push(run_id, 4, "done", f"Done — {len(summaries)} files cleaned", done=True,
        result={
            "files_processed":   len(summaries),
            "total_rows_removed": total_removed,
            "report_text":        ai_summary,
            "anomalies":          [],
        },
        attachments=attachments,
    )
    print(f"\n[ClawMarketer] Done. Files sent to Telegram.")
    return message


def handle(message: str) -> str:
    data_dir = None
    msg = message.lower()
    if " in " in msg:
        parts = message.split(" in ", 1)
        if len(parts) > 1:
            candidate = os.path.expanduser(parts[1].strip())
            if os.path.isdir(candidate):
                data_dir = candidate
    send_message("🧹 Starting data cleaning agent...\nCheck your dashboard for live progress.")
    return run(data_dir=data_dir)


if __name__ == "__main__":
    print(run())
