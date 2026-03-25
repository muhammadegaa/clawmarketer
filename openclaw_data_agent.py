"""
ClawMarketer — Data Cleansing Agent for OpenClaw
=================================================
Place this file in your OpenClaw skills/ directory.

What this agent does end-to-end:
  1. Scans a configured local directory for CSV/Excel files
  2. Cleans each file (removes duplicates, fixes nulls, normalises formats)
  3. Saves clean versions back to disk (prefixed with clean_)
  4. Generates a diff summary (rows removed, columns fixed)
  5. Sends summary + clean files to Telegram as attachments
  6. Pushes live progress to your ClawMarketer dashboard

Telegram triggers:
  "clean my data"          → scans default DATA_DIR
  "clean data in reports"  → scans ~/reports
  "analyze my files"       → same as clean my data

OpenClaw skills config:
  {
    "name": "data_cleanse",
    "trigger": ["clean my data", "clean data", "analyze my files"],
    "handler": "skills/openclaw_data_agent.handle",
    "description": "Scan, clean and analyze CSV/Excel files in your data folder"
  }

Config (in clawmarketer.env):
  DATA_DIR=~/Documents/data    ← directory to scan (default: ~/Documents/data)
"""

import os
import sys
import uuid
import glob
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

_repo_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_repo_dir, "clawmarketer.env"))

# Ensure agents/ package is importable regardless of working directory
if _repo_dir not in sys.path:
    sys.path.insert(0, _repo_dir)

CLAWMARKETER_URL     = os.getenv("CLAWMARKETER_URL", "https://clawmarketer.vercel.app")
CLAWMARKETER_USER_ID = os.getenv("CLAWMARKETER_USER_ID", "")
GROQ_API_KEY         = os.getenv("GROQ_API_KEY", "")
DATA_DIR             = os.path.expanduser(os.getenv("DATA_DIR", "~/Documents/data"))


# ── Progress ──────────────────────────────────────────────────────────────────

def _push(run_id: str, stage: int, status: str, message: str,
          done: bool = False, result: dict = None, attachments: list = None):
    payload = {
        "user_id": CLAWMARKETER_USER_ID,
        "run_id":  run_id,
        "skill":   "clawmarketer-data",
        "stage":   stage,
        "status":  status,
        "message": message,
        "done":    done,
    }
    if result:
        payload["result"] = result
    if attachments:
        payload["attachments"] = attachments
    try:
        requests.post(f"{CLAWMARKETER_URL}/api/agent/push", json=payload, timeout=10)
    except Exception:
        pass

    icon = {"running": "⚙️", "done": "✅", "error": "❌"}.get(status, "•")
    print(f"  {icon} [{stage}/4] {message}")


# ── Clean a single file ───────────────────────────────────────────────────────

def _clean_file(path: str) -> dict:
    """Clean a single CSV or Excel file. Returns a summary dict."""
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        return None

    original_rows = len(df)
    original_cols = list(df.columns)

    # Remove fully empty rows and columns
    df = df.dropna(how="all").reset_index(drop=True)
    df = df.loc[:, df.notna().any()]

    # Remove exact duplicate rows
    df = df.drop_duplicates().reset_index(drop=True)

    # Strip whitespace from string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    clean_rows = len(df)
    clean_dir  = os.path.dirname(path)
    clean_name = "clean_" + os.path.basename(path).replace(".xlsx", ".csv").replace(".xls", ".csv")
    clean_path = os.path.join(clean_dir, clean_name)
    df.to_csv(clean_path, index=False)

    return {
        "file":          os.path.basename(path),
        "clean_path":    clean_path,
        "original_rows": original_rows,
        "clean_rows":    clean_rows,
        "rows_removed":  original_rows - clean_rows,
        "columns":       len(df.columns),
    }


# ── AI summary ────────────────────────────────────────────────────────────────

def _ai_summary(file_summaries: list) -> str:
    if not GROQ_API_KEY:
        return ""
    try:
        from groq import Groq
        import json
        client = Groq(api_key=GROQ_API_KEY)
        prompt = (
            "You are a data analyst. A data cleaning agent just processed these files:\n\n"
            f"{json.dumps(file_summaries, indent=2)}\n\n"
            "Write a brief 3-4 sentence summary for a business owner covering: "
            "what files were cleaned, how many rows were removed and why, "
            "and whether the data quality looks good. Be direct."
        )
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        return resp.choices[0].message.content
    except Exception as e:
        return f"AI summary unavailable: {e}"


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(data_dir: str = None) -> str:
    from agents.telegram_sender import send_message, send_document

    data_dir = data_dir or DATA_DIR
    run_id   = "data_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    print(f"\n[ClawMarketer] Data Cleansing Agent — run {run_id}")

    # ── Stage 1: Scan ───────────────────────────────────────────────────────
    _push(run_id, 1, "running", f"Scanning {data_dir}...")

    if not os.path.isdir(data_dir):
        msg = f"Directory not found: {data_dir}. Set DATA_DIR in clawmarketer.env"
        _push(run_id, 1, "error", msg)
        return f"❌ {msg}"

    patterns = ["*.csv", "*.xlsx", "*.xls"]
    files = []
    for p in patterns:
        files.extend(glob.glob(os.path.join(data_dir, p)))

    # Skip already-cleaned files
    files = [f for f in files if not os.path.basename(f).startswith("clean_")]

    if not files:
        msg = f"No CSV/Excel files found in {data_dir}"
        _push(run_id, 1, "error", msg)
        return f"📂 {msg}"

    _push(run_id, 1, "running", f"Found {len(files)} file(s) to process")

    # ── Stage 2: Clean ──────────────────────────────────────────────────────
    _push(run_id, 2, "running", f"Cleaning {len(files)} file(s)...")
    summaries = []
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

    # ── Stage 3: Analyze ────────────────────────────────────────────────────
    _push(run_id, 3, "running", "Generating data quality summary...")
    total_removed = sum(s["rows_removed"] for s in summaries)
    total_original = sum(s["original_rows"] for s in summaries)

    # ── Stage 4: AI + Send ──────────────────────────────────────────────────
    _push(run_id, 4, "running", "Generating AI summary and sending to Telegram...")

    ai_summary = _ai_summary(summaries)

    file_lines = "\n".join(
        f"• `{s['file']}` — {s['original_rows']} → {s['clean_rows']} rows"
        + (f" *(removed {s['rows_removed']})*" if s['rows_removed'] > 0 else "")
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
        mid = send_document(path, caption=f"📎 {fname}")
        attachments.append({"name": fname, "type": "document", "telegram_message_id": mid})

    _push(run_id, 4, "done", f"Done — {len(summaries)} files cleaned", done=True,
        result={
            "files_processed": len(summaries),
            "total_rows_removed": total_removed,
            "report_text": ai_summary,
            "anomalies": [],
        },
        attachments=attachments,
    )

    print(f"\n[ClawMarketer] Done. Files sent to Telegram.")
    return message


# ── OpenClaw entry point ──────────────────────────────────────────────────────

def handle(message: str) -> str:
    from agents.telegram_sender import send_message

    # Parse custom directory from message e.g. "clean data in ~/reports"
    data_dir = None
    msg = message.lower()
    if " in " in msg:
        parts = message.split(" in ", 1)
        if len(parts) > 1:
            candidate = parts[1].strip()
            expanded = os.path.expanduser(candidate)
            if os.path.isdir(expanded):
                data_dir = expanded

    send_message("🧹 Starting data cleaning agent...\nCheck your dashboard for live progress.")
    return run(data_dir=data_dir)


if __name__ == "__main__":
    print(run())
