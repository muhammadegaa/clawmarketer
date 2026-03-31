import os
import secrets
import tempfile
from urllib.parse import quote
from fastapi import FastAPI, HTTPException, UploadFile, File, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, JSONResponse
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel
from typing import Optional
import requests as http
from dotenv import load_dotenv

load_dotenv()

from agents import fetcher, cleaner, analyzer, reporter

app = FastAPI(title="ClawMarketer POC")
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY", secrets.token_hex(32)))
app.mount("/static", StaticFiles(directory="static"), name="static")

META_APP_ID     = os.getenv("META_APP_ID", "")
META_APP_SECRET = os.getenv("META_APP_SECRET", "")
REDIRECT_URI    = os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")
META_OAUTH_URL  = "https://www.facebook.com/v21.0/dialog/oauth"
META_TOKEN_URL  = "https://graph.facebook.com/v21.0/oauth/access_token"
META_ACCOUNTS_URL = "https://graph.facebook.com/v21.0/me/adaccounts"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_fs(value):
    if value is None:           return {"nullValue": None}
    if isinstance(value, bool): return {"booleanValue": value}
    if isinstance(value, int):  return {"integerValue": str(value)}
    if isinstance(value, float):return {"doubleValue": value}
    if isinstance(value, str):  return {"stringValue": value}
    if isinstance(value, list): return {"arrayValue": {"values": [_to_fs(v) for v in value]}}
    if isinstance(value, dict): return {"mapValue": {"fields": {k: _to_fs(v) for k, v in value.items()}}}
    return {"stringValue": str(value)}


def _from_fs(field: dict):
    """Deserialize a Firestore field value back to a Python value."""
    if "nullValue"    in field: return None
    if "booleanValue" in field: return field["booleanValue"]
    if "integerValue" in field: return int(field["integerValue"])
    if "doubleValue"  in field: return field["doubleValue"]
    if "stringValue"  in field: return field["stringValue"]
    if "arrayValue"   in field:
        return [_from_fs(v) for v in field["arrayValue"].get("values", [])]
    if "mapValue"     in field:
        return {k: _from_fs(v) for k, v in field["mapValue"].get("fields", {}).items()}
    return None


def _read_from_firestore(user_id: str, collection: str, doc_id: str) -> Optional[dict]:
    """Read a single Firestore document and return its fields as a Python dict."""
    project_id = os.getenv("FIREBASE_PROJECT_ID", "")
    api_key    = os.getenv("FIREBASE_API_KEY", "")
    if not project_id:
        return None
    url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
           f"/databases/(default)/documents/users/{user_id}/{collection}/{doc_id}")
    try:
        resp = http.get(url, params={"key": api_key})
        if resp.status_code != 200:
            return None
        doc = resp.json()
        return {k: _from_fs(v) for k, v in doc.get("fields", {}).items()}
    except Exception as e:
        print(f"[Firestore] _read_from_firestore failed: {e}")
        return None


def _write_to_firestore(user_id: str, collection: str, doc_id: str, data: dict):
    """Write arbitrary fields to a Firestore document (merge/patch)."""
    project_id = os.getenv("FIREBASE_PROJECT_ID", "")
    api_key    = os.getenv("FIREBASE_API_KEY", "")
    if not project_id:
        return
    fields = {k: _to_fs(v) for k, v in data.items()}
    url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
           f"/databases/(default)/documents/users/{user_id}/{collection}/{doc_id}")
    try:
        http.patch(url, json={"fields": fields},
                   params={"key": api_key,
                           "updateMask.fieldPaths": list(fields.keys())})
    except Exception as e:
        print(f"[Firestore] _write_to_firestore failed: {e}")


def _push_to_firestore(user_id: str, run_id: str, skill: str, status: str,
                       triggered_by: str = "manual", result: dict = None,
                       stages: list = None, attachments: list = None,
                       created_at: str = None, is_demo: bool = False):
    """Write a completed run document directly to Firestore."""
    project_id = os.getenv("FIREBASE_PROJECT_ID", "")
    api_key    = os.getenv("FIREBASE_API_KEY", "")
    if not project_id:
        return

    from datetime import datetime, timezone
    now = created_at or datetime.now(timezone.utc).isoformat()

    fields = {
        "run_id":       _to_fs(run_id),
        "skill":        _to_fs(skill),
        "status":       _to_fs(status),
        "triggered_by": _to_fs(triggered_by),
        "created_at":   _to_fs(now),
        "updated_at":   _to_fs(now),
        "is_demo":      _to_fs(is_demo),
    }
    if result:      fields["result"]      = _to_fs(result)
    if stages:      fields["stages"]      = _to_fs(stages)
    if attachments: fields["attachments"] = _to_fs(attachments)

    url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
           f"/databases/(default)/documents/users/{user_id}/runs/{run_id}")
    try:
        http.patch(url, json={"fields": fields},
                   params={"key": api_key,
                           "updateMask.fieldPaths": list(fields.keys())})
    except Exception as e:
        print(f"[Firestore] _push_to_firestore failed: {e}")


def _serialize(results: dict, clean_stats: dict) -> dict:
    campaign_rows = []
    if not results["campaign_summary"].empty:
        summary = results["campaign_summary"]
        ctr_col = "ctr_calc" if "ctr_calc" in summary.columns else ("ctr" if "ctr" in summary.columns else None)
        cpc_col = "cpc_calc" if "cpc_calc" in summary.columns else ("cpc" if "cpc" in summary.columns else None)
        for _, row in summary.iterrows():
            campaign_rows.append({
                "name":        str(row.get("campaign_name", "")),
                "spend":       round(float(row["spend"]), 2)       if "spend"       in row and row["spend"]       == row["spend"]       else None,
                "impressions": int(row["impressions"])              if "impressions" in row and row["impressions"] == row["impressions"] else None,
                "clicks":      int(row["clicks"])                   if "clicks"      in row and row["clicks"]      == row["clicks"]      else None,
                "ctr":         round(float(row[ctr_col]), 2)        if ctr_col       and row[ctr_col]              == row[ctr_col]       else None,
                "cpc":         round(float(row[cpc_col]), 2)        if cpc_col       and row[cpc_col]              == row[cpc_col]       else None,
                "roas":        round(float(row["roas"]), 2)         if "roas"        in row and row["roas"]        == row["roas"]        else None,
            })
    overall = results["overall"]
    return {
        "clean_stats": {
            "original_rows": clean_stats["original_rows"],
            "clean_rows":    clean_stats["clean_rows"],
            "dropped_rows":  clean_stats["dropped_rows"],
        },
        "overall":       overall,
        "campaigns":     campaign_rows,
        "anomalies":     results["anomalies"],
        "report":        results.get("report", ""),
        # Flat aliases for dashboard updateStats()
        "total_spend":   overall.get("total_spend", 0),
        "overall_ctr":   overall.get("overall_ctr", 0),
        "avg_roas":      overall.get("avg_roas", 0),
        "num_campaigns": overall.get("num_campaigns", 0),
        "report_text":   results.get("report", ""),
    }


def _run_pipeline(df_raw, groq_key: str) -> dict:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        df_raw.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    try:
        df, clean_stats = cleaner.clean(tmp_path)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    results = analyzer.run(df)
    try:
        results["report"] = reporter.generate(results, groq_key)
    except Exception as e:
        results["report"] = f"AI report unavailable: {str(e)}"

    return _serialize(results, clean_stats)


# ── Static ────────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    return FileResponse("static/index.html")

@app.get("/onboarding")
def onboarding():
    return FileResponse("static/onboarding.html")


@app.get("/api/config")
def get_config():
    """Expose public Firebase config to the frontend."""
    return {
        "firebase": {
            "apiKey":            os.getenv("FIREBASE_API_KEY", ""),
            "authDomain":        os.getenv("FIREBASE_AUTH_DOMAIN", ""),
            "projectId":         os.getenv("FIREBASE_PROJECT_ID", ""),
            "storageBucket":     os.getenv("FIREBASE_STORAGE_BUCKET", ""),
            "messagingSenderId": os.getenv("FIREBASE_MESSAGING_SENDER_ID", ""),
            "appId":             os.getenv("FIREBASE_APP_ID", ""),
        },
        "metaConfigured": bool(os.getenv("META_APP_ID")),
    }


# ── Meta OAuth flow ───────────────────────────────────────────────────────────

@app.get("/auth/meta")
def meta_login(request: Request):
    if not META_APP_ID:
        raise HTTPException(status_code=500, detail="META_APP_ID not configured in .env")
    state = secrets.token_hex(16)
    request.session["oauth_state"] = state
    url = (
        f"{META_OAUTH_URL}"
        f"?client_id={META_APP_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&state={state}"
        f"&scope=ads_read"
    )
    return RedirectResponse(url)


@app.get("/auth/callback")
def meta_callback(request: Request, code: str = None, state: str = None, error: str = None):
    if error:
        return RedirectResponse(f"/?error={error}")

    # Exchange code for token
    resp = http.get(META_TOKEN_URL, params={
        "client_id":     META_APP_ID,
        "client_secret": META_APP_SECRET,
        "redirect_uri":  REDIRECT_URI,
        "code":          code,
    })
    token_data = resp.json()
    if "error" in token_data:
        return RedirectResponse(f"/?error={token_data['error']['message']}")

    access_token = token_data["access_token"]

    # Extend to long-lived token (~60 days)
    extend_resp = http.get(META_TOKEN_URL, params={
        "grant_type":        "fb_exchange_token",
        "client_id":         META_APP_ID,
        "client_secret":     META_APP_SECRET,
        "fb_exchange_token": access_token,
    })
    extended = extend_resp.json()
    if "access_token" in extended:
        access_token = extended["access_token"]

    # Pass token to frontend via URL param (URL-encoded — token can contain | and other special chars).
    return RedirectResponse(f"/?meta_token={quote(access_token, safe='')}")


def _fetch_ad_accounts(token: str) -> list:
    """Try all Meta endpoints to find ad accounts for this token."""
    base = "https://graph.facebook.com/v21.0"
    endpoints = [
        f"{base}/me/adaccounts",
        f"{base}/me/assigned_ad_accounts",
        f"{base}/me/personal_ad_accounts",
    ]
    for url in endpoints:
        resp = http.get(url, params={"access_token": token, "fields": "id,name", "limit": 50})
        data = resp.json()
        if "error" in data:
            continue
        accounts = [{"id": a["id"], "name": a.get("name", a["id"])} for a in data.get("data", [])]
        if accounts:
            return accounts
    return []


@app.get("/auth/status")
def auth_status(token: str = None):
    """Check token validity and return ad accounts. Token passed as query param."""
    if not token:
        return {"connected": False}

    # Verify token works at all
    me = http.get("https://graph.facebook.com/v21.0/me", params={"access_token": token, "fields": "id"})
    if "error" in me.json():
        return {"connected": False}

    accounts = _fetch_ad_accounts(token)
    return {"connected": True, "accounts": accounts}


@app.post("/auth/disconnect")
def disconnect():
    # Token lives in browser localStorage — nothing to clear server-side
    return {"ok": True}


@app.get("/api/debug/meta")
def debug_meta(token: str = None):
    """Inspect what a Meta token can actually see — accounts, campaigns, insights."""
    if not token:
        return {"error": "Pass ?token=YOUR_TOKEN"}

    base = "https://graph.facebook.com/v21.0"
    out = {}

    # 1. Token identity
    me = http.get(f"{base}/me", params={"access_token": token, "fields": "id,name"}).json()
    out["me"] = me

    # 2. All ad account endpoints
    for key, path in [
        ("adaccounts",          f"{base}/me/adaccounts"),
        ("assigned_adaccounts", f"{base}/me/assigned_ad_accounts"),
        ("personal_adaccounts", f"{base}/me/personal_ad_accounts"),
    ]:
        r = http.get(path, params={"access_token": token, "fields": "id,name,account_status", "limit": 20}).json()
        out[key] = r

    # 3. For the first account found, list campaigns + try insights with wide date range
    accounts = _fetch_ad_accounts(token)
    out["accounts_found"] = accounts
    if accounts:
        act = accounts[0]["id"]
        if not act.startswith("act_"):
            act = f"act_{act}"

        # List campaigns (no spend required)
        campaigns = http.get(f"{base}/{act}/campaigns", params={
            "access_token": token,
            "fields": "id,name,status,objective",
            "limit": 20,
        }).json()
        out["campaigns"] = campaigns

        # Try insights with last_year to find any historical data
        for preset in ["last_30d", "last_90d", "last_year"]:
            ins = http.get(f"{base}/{act}/insights", params={
                "access_token": token,
                "fields": "campaign_name,impressions,spend,clicks",
                "level": "campaign",
                "date_preset": preset,
                "limit": 5,
            }).json()
            out[f"insights_{preset}"] = ins
            if ins.get("data"):
                break  # found data, no need to go wider

    return out


# ── Run pipeline with live Meta data ─────────────────────────────────────────

class RunRequest(BaseModel):
    date_preset: str = "last_30d"
    meta_token: Optional[str] = None
    account_id: Optional[str] = None


@app.post("/api/run")
def run_live(body: RunRequest):
    token = body.meta_token
    if not token:
        raise HTTPException(status_code=401, detail="Not connected to Meta. Please reconnect.")

    # Auto-fetch ad account — user never needs to provide this
    account_id = body.account_id
    if not account_id:
        accounts = _fetch_ad_accounts(token)
        if not accounts:
            raise HTTPException(status_code=400, detail="No Meta ad accounts found. Make sure your Meta account has an active Ads account.")
        account_id = accounts[0]["id"]

    try:
        df_raw = fetcher.fetch(
            access_token=token,
            ad_account_id=account_id,
            date_preset=body.date_preset,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Meta API error: {str(e)}")

    if df_raw.empty:
        # No campaigns yet — fall back to sample data so the dashboard is always useful
        import pandas as _pd
        from sample_data import generate as _gen_sample
        csv_path = _gen_sample()
        df_raw = _pd.read_csv(csv_path)
        os.unlink(csv_path)
        result = _run_pipeline(df_raw, os.getenv("GROQ_API_KEY", ""))
        result["is_sample"] = True
        result["sample_notice"] = "No campaigns found in your Meta Ads account yet. Showing sample data so you can explore the dashboard."
        return result

    return _run_pipeline(df_raw, os.getenv("GROQ_API_KEY", ""))


# ── Upload CSV ────────────────────────────────────────────────────────────────

@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...)):
    suffix = ".csv"
    if file.filename.endswith(".xlsx"):
        suffix = ".xlsx"
    elif file.filename.endswith(".xls"):
        suffix = ".xls"

    contents = await file.read()
    with tempfile.NamedTemporaryFile(mode="wb", suffix=suffix, delete=False) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    try:
        df, clean_stats = cleaner.clean(tmp_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {str(e)}")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    results = analyzer.run(df)
    try:
        results["report"] = reporter.generate(results, os.getenv("GROQ_API_KEY", ""))
    except Exception as e:
        results["report"] = f"AI report unavailable: {str(e)}"

    return _serialize(results, clean_stats)


# ── Demo ──────────────────────────────────────────────────────────────────────

_DEMO_REPORT = """**Executive Summary**
Your Meta Ads campaigns delivered strong results this period with $12,450 in spend driving a 3.2x average ROAS. Conversion volume is healthy at 847 purchases, though two campaigns are dragging down the portfolio average.

**What's Working**
- *Summer Sale 2024* leads with 4.8x ROAS on $3,200 spend — your best performer by far
- *Retargeting — Cart Abandoners* converts at 6.2% CTR, 3x the account average
- *Brand Awareness — Video* reaches 180K impressions at a $4.20 CPM

**What Needs Attention**
- *Prospecting — Cold Audiences* has 0.4% CTR and 0.6x ROAS — losing money on every click
- *Engagement — Stories* CPC is $4.80 vs account average of $1.20 — needs creative refresh
- Frequency on *Retargeting* campaigns is hitting 8.2 — audience fatigue risk

**Recommended Actions**
1. Pause *Prospecting — Cold Audiences* or cut budget 70% immediately
2. Reallocate $1,500/week from underperformers to *Summer Sale* and *Cart Abandoners*
3. Refresh creatives for *Engagement — Stories* (current CTR below 1%)
4. Cap retargeting frequency at 5 to prevent audience fatigue
5. Test Lookalike audiences based on the top 10% of purchasers from *Summer Sale*"""

_META_STAGES = [
    {"status": "done", "note": "Fetched 12 campaigns from Meta Ads API"},
    {"status": "done", "note": "Cleaned 12 rows, dropped 0"},
    {"status": "done", "note": "3 anomalies detected across campaigns"},
    {"status": "done", "note": "Report + 3 charts sent to Telegram"},
]

_DATA_STAGES = [
    {"status": "done", "note": "Found 3 CSV files in ~/Documents/data"},
    {"status": "done", "note": "Cleaned all 3 files, removed 47 duplicate rows"},
    {"status": "done", "note": "Data quality score: 94% — looks good"},
    {"status": "done", "note": "3 clean files sent to Telegram"},
]

_META_ATTACHMENTS = [
    {"name": "chart_spend.png",    "type": "photo",    "telegram_message_id": 1001},
    {"name": "chart_ctr.png",      "type": "photo",    "telegram_message_id": 1002},
    {"name": "chart_roas.png",     "type": "photo",    "telegram_message_id": 1003},
    {"name": "meta_ads_clean.csv", "type": "document", "telegram_message_id": 1004},
]

_DATA_ATTACHMENTS = [
    {"name": "clean_leads.csv",    "type": "document", "telegram_message_id": 2001},
    {"name": "clean_orders.csv",   "type": "document", "telegram_message_id": 2002},
    {"name": "clean_customers.csv","type": "document", "telegram_message_id": 2003},
]


@app.post("/api/demo")
def run_demo(uid: str = ""):
    from sample_data import generate
    csv_path = generate()
    df, clean_stats = cleaner.clean(csv_path)
    os.unlink(csv_path)
    results = analyzer.run(df)
    results["report"] = _DEMO_REPORT

    if uid:
        from datetime import datetime, timezone, timedelta
        import uuid as _uuid
        now = datetime.now(timezone.utc)
        o = results.get("overall", {})

        # Run 1: Meta Ads — most recent (today)
        _push_to_firestore(
            uid, f"demo_{now.strftime('%Y%m%d_%H%M%S')}_a1", "clawmarketer-meta", "done",
            triggered_by="demo", is_demo=True,
            created_at=(now).isoformat(),
            stages=_META_STAGES, attachments=_META_ATTACHMENTS,
            result={
                "total_spend":   o.get("total_spend", 12450.80),
                "overall_ctr":   o.get("overall_ctr", 2.14),
                "avg_roas":      o.get("avg_roas", 3.20),
                "num_campaigns": o.get("num_campaigns", 12),
                "report_text":   _DEMO_REPORT,
                "anomalies": [
                    "Low CTR (0.4%) on: Prospecting — Cold Audiences",
                    "ROAS below 1.0 (0.6x) — losing money on: Prospecting — Cold Audiences",
                    "Spend spike ($4,200) on campaign: Summer Sale 2024",
                ],
            },
        )

        # Run 2: Meta Ads — last month
        _push_to_firestore(
            uid, f"demo_{(now - timedelta(days=7)).strftime('%Y%m%d_%H%M%S')}_a2", "clawmarketer-meta", "done",
            triggered_by="demo", is_demo=True,
            created_at=(now - timedelta(days=7)).isoformat(),
            stages=_META_STAGES, attachments=_META_ATTACHMENTS,
            result={
                "total_spend":   9820.40,
                "overall_ctr":   1.87,
                "avg_roas":      2.80,
                "num_campaigns": 10,
                "report_text":   _DEMO_REPORT,
                "anomalies": ["Low CTR (0.5%) on: Brand Awareness — Video"],
            },
        )

        # Run 3: Data Cleaner
        _push_to_firestore(
            uid, f"demo_{(now - timedelta(days=3)).strftime('%Y%m%d_%H%M%S')}_b1", "clawmarketer-data", "done",
            triggered_by="demo", is_demo=True,
            created_at=(now - timedelta(days=3)).isoformat(),
            stages=_DATA_STAGES, attachments=_DATA_ATTACHMENTS,
            result={
                "files_processed":    3,
                "total_rows_removed": 47,
                "report_text": "Processed 3 files (leads.csv, orders.csv, customers.csv). Removed 47 duplicate rows across all files. Data quality looks healthy — column formats are consistent and no critical nulls detected.",
                "anomalies": [],
            },
        )

    return _serialize(results, clean_stats)


@app.delete("/api/demo")
def clear_demo(uid: str = ""):
    """Delete all demo runs for a user from Firestore."""
    if not uid:
        raise HTTPException(status_code=400, detail="Missing uid")
    project_id = os.getenv("FIREBASE_PROJECT_ID", "")
    api_key    = os.getenv("FIREBASE_API_KEY", "")
    if not project_id:
        return {"deleted": 0}

    # List all runs
    list_url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
                f"/databases/(default)/documents/users/{uid}/runs")
    deleted = 0
    try:
        resp = http.get(list_url, params={"key": api_key})
        docs = resp.json().get("documents", [])
        for doc in docs:
            fields = doc.get("fields", {})
            is_demo = fields.get("is_demo", {}).get("booleanValue", False)
            triggered_by = fields.get("triggered_by", {}).get("stringValue", "")
            if is_demo or triggered_by == "demo":
                del_url = f"https://firestore.googleapis.com/v1/{doc['name']}"
                http.delete(del_url, params={"key": api_key})
                deleted += 1
    except Exception as e:
        print(f"[Firestore] clear_demo failed: {e}")

    return {"deleted": deleted}


# ── Credentials API ───────────────────────────────────────────────────────────

def _get_or_create_api_token(uid: str) -> str:
    """Return the existing API token for uid, or generate + store a new one."""
    doc = _read_from_firestore(uid, "meta", "profile") or {}
    if doc.get("api_token"):
        return doc["api_token"]
    token = "sk_cm_" + secrets.token_hex(32)
    _write_to_firestore(uid, "meta", "profile", {"api_token": token})
    return token


def _validate_bearer(request: Request) -> Optional[str]:
    """Extract and return bearer token from Authorization header, or None."""
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return None


@app.get("/api/auth/token")
def get_api_token(uid: str = "", request: Request = None):
    """Return (or lazily create) the API token for this user.
    Must pass Firebase ID token as Bearer — we trust the frontend to send it."""
    if not uid:
        raise HTTPException(status_code=400, detail="Missing uid")
    token = _get_or_create_api_token(uid)
    return {"token": token}


@app.post("/api/auth/token")
def regenerate_api_token(uid: str = ""):
    """Regenerate (rotate) the API token for this user."""
    if not uid:
        raise HTTPException(status_code=400, detail="Missing uid")
    token = "sk_cm_" + secrets.token_hex(32)
    _write_to_firestore(uid, "meta", "profile", {"api_token": token})
    return {"token": token}


@app.get("/api/credentials")
def get_credentials(uid: str = "", request: Request = None):
    """Return all credentials for a user. Validated by CLAWMARKETER_API_TOKEN bearer."""
    if not uid:
        raise HTTPException(status_code=400, detail="Missing uid")

    bearer = _validate_bearer(request)
    if not bearer:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    # Look up the stored token for this uid
    doc = _read_from_firestore(uid, "meta", "profile") or {}
    stored_token = doc.get("api_token", "")
    if not stored_token or bearer != stored_token:
        raise HTTPException(status_code=403, detail="Invalid token")

    # Fetch Meta integration from Firestore
    meta_doc = _read_from_firestore(uid, "integrations", "meta") or {}
    meta_creds = {}
    if meta_doc.get("access_token"):
        meta_creds = {
            "access_token": meta_doc["access_token"],
            "account_id":   meta_doc.get("account_id", ""),
        }

    return {
        "meta":         meta_creds,
        "groq_api_key": os.getenv("GROQ_API_KEY", ""),
        "telegram": {
            "bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
            "chat_id":   "",   # user-specific, not server-level
        },
    }


# ── Agent integration endpoints ───────────────────────────────────────────────

class InsightsRequest(BaseModel):
    uid: str
    date_preset: str = "last_30d"

@app.post("/api/integrations/meta/insights")
def meta_insights(body: InsightsRequest, request: Request):
    """Return Meta Ads campaign data for the agent.
    Uses the user's stored Meta credentials from Firestore.
    Falls back to sample data automatically if no Meta integration is set up."""
    bearer = _validate_bearer(request)
    if not bearer:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    doc = _read_from_firestore(body.uid, "meta", "profile") or {}
    if bearer != doc.get("api_token", ""):
        raise HTTPException(status_code=403, detail="Invalid token")

    # Try real Meta credentials stored in Firestore
    meta_doc = _read_from_firestore(body.uid, "integrations", "meta") or {}
    access_token = meta_doc.get("access_token", "")
    account_id   = meta_doc.get("account_id", "")

    connected_but_empty = False
    if access_token:
        try:
            df = fetcher.fetch(access_token=access_token, ad_account_id=account_id,
                               date_preset=body.date_preset)
            if not df.empty:
                return {
                    "data_source": "meta_api",
                    "rows": len(df),
                    "data": df.to_dict(orient="records"),
                }
            connected_but_empty = True
        except Exception as e:
            print(f"[meta/insights] Meta API failed: {e}")

    # Fallback: sample data (always show something useful)
    from sample_data import generate as _gen
    import pandas as pd
    import math
    csv_path = _gen()
    df = pd.read_csv(csv_path)
    os.unlink(csv_path)
    # Replace NaN/inf with None so the response is JSON-serialisable
    records = [
        {k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
         for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]
    return {
        "data_source": "sample",
        "rows": len(df),
        "data": records,
        "is_sample": True,
        "sample_notice": (
            "Your Meta Ads account is connected but has no campaign data yet. "
            "Showing sample data so you can explore the platform."
        ) if connected_but_empty else (
            "No Meta Ads integration found. Showing sample data."
        ),
    }


class ReportRequest(BaseModel):
    uid: str
    analysis: dict

@app.post("/api/ai/report")
def ai_report(body: ReportRequest, request: Request):
    """Generate a structured Meta Ads AI report via the server's Groq key."""
    bearer = _validate_bearer(request)
    if not bearer:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    doc = _read_from_firestore(body.uid, "meta", "profile") or {}
    if bearer != doc.get("api_token", ""):
        raise HTTPException(status_code=403, detail="Invalid token")

    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return {"report": ""}

    try:
        report = reporter.generate(body.analysis, groq_key)
        return {"report": report}
    except Exception as e:
        print(f"[ai/report] Groq failed: {e}")
        return {"report": f"AI report unavailable: {e}"}


class CompleteRequest(BaseModel):
    uid: str
    prompt: str
    temperature: float = 0.4
    max_tokens: int = 1024

@app.post("/api/ai/complete")
def ai_complete(body: CompleteRequest, request: Request):
    """Generic LLM completion. Any skill can call this — agent never needs Groq key locally."""
    bearer = _validate_bearer(request)
    if not bearer:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    doc = _read_from_firestore(body.uid, "meta", "profile") or {}
    if bearer != doc.get("api_token", ""):
        raise HTTPException(status_code=403, detail="Invalid token")

    groq_key = os.getenv("GROQ_API_KEY", "")
    if not groq_key:
        return {"text": ""}

    try:
        from groq import Groq
        client = Groq(api_key=groq_key)
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": body.prompt}],
            temperature=body.temperature,
            max_tokens=body.max_tokens,
        )
        return {"text": resp.choices[0].message.content}
    except Exception as e:
        print(f"[ai/complete] Groq failed: {e}")
        return {"text": f"AI unavailable: {e}"}


# ── Telegram registration ─────────────────────────────────────────────────────

import random as _random
import string as _string
from datetime import timezone as _tz

def _write_telegram_code(code: str, uid: str, expires_at: str):
    """Store a one-time Telegram connection code in a root-level Firestore collection."""
    project_id = os.getenv("FIREBASE_PROJECT_ID", "")
    api_key    = os.getenv("FIREBASE_API_KEY", "")
    if not project_id:
        return
    url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
           f"/databases/(default)/documents/telegram_codes/{code}")
    fields = {
        "uid":        _to_fs(uid),
        "expires_at": _to_fs(expires_at),
        "used":       _to_fs(False),
    }
    try:
        http.patch(url, json={"fields": fields},
                   params={"key": api_key,
                           "updateMask.fieldPaths": list(fields.keys())})
    except Exception as e:
        print(f"[Firestore] write_telegram_code failed: {e}")


def _read_telegram_code(code: str):
    """Read a Telegram connection code document."""
    project_id = os.getenv("FIREBASE_PROJECT_ID", "")
    api_key    = os.getenv("FIREBASE_API_KEY", "")
    if not project_id:
        return None
    url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
           f"/databases/(default)/documents/telegram_codes/{code}")
    try:
        resp = http.get(url, params={"key": api_key})
        if resp.status_code != 200:
            return None
        return {k: _from_fs(v) for k, v in resp.json().get("fields", {}).items()}
    except Exception as e:
        print(f"[Firestore] read_telegram_code failed: {e}")
        return None


@app.post("/api/telegram/register")
def telegram_register(uid: str = ""):
    """Generate a 6-digit one-time code the user pastes into the Telegram bot."""
    if not uid:
        raise HTTPException(status_code=400, detail="Missing uid")
    from datetime import datetime, timezone, timedelta
    code       = "".join(_random.choices(_string.digits, k=6))
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
    _write_telegram_code(code, uid, expires_at)
    return {"code": code, "expires_in": 600}


class TelegramVerifyBody(BaseModel):
    code: str
    chat_id: str

@app.post("/api/telegram/verify")
def telegram_verify(body: TelegramVerifyBody):
    """Called by the bot when user sends /connect CODE.
    Stores the chat_id against the uid and marks the code used."""
    from datetime import datetime, timezone
    rec = _read_telegram_code(body.code)
    if not rec:
        raise HTTPException(status_code=404, detail="Invalid code")
    if rec.get("used"):
        raise HTTPException(status_code=410, detail="Code already used")
    # Check expiry
    try:
        exp = datetime.fromisoformat(rec["expires_at"])
        if datetime.now(timezone.utc) > exp:
            raise HTTPException(status_code=410, detail="Code expired")
    except ValueError:
        pass

    uid = rec["uid"]
    # Store chat_id in user profile
    _write_to_firestore(uid, "meta", "profile", {"telegram_chat_id": body.chat_id})
    # Mark code as used
    _write_telegram_code(body.code, uid, rec.get("expires_at", ""))
    project_id = os.getenv("FIREBASE_PROJECT_ID", "")
    api_key    = os.getenv("FIREBASE_API_KEY", "")
    if project_id:
        url = (f"https://firestore.googleapis.com/v1/projects/{project_id}"
               f"/databases/(default)/documents/telegram_codes/{body.code}")
        http.patch(url, json={"fields": {"used": _to_fs(True)}},
                   params={"key": api_key, "updateMask.fieldPaths": ["used"]})

    return {"ok": True, "uid": uid}


@app.get("/api/telegram/status")
def telegram_status(uid: str = ""):
    """Return Telegram connection status for a user."""
    if not uid:
        raise HTTPException(status_code=400, detail="Missing uid")
    doc = _read_from_firestore(uid, "meta", "profile") or {}
    chat_id = doc.get("telegram_chat_id", "")
    return {"connected": bool(chat_id), "chat_id": chat_id}


# ── OpenClaw config download ───────────────────────────────────────────────────

@app.get("/api/openclaw-config")
def openclaw_config(uid: str, request: Request = None):
    """Generate a pre-filled .env for the user's OpenClaw setup.
    Only emits CLAWMARKETER_URL, CLAWMARKETER_USER_ID, CLAWMARKETER_API_TOKEN, DATA_DIR.
    Skills fetch Meta/Groq credentials at runtime via /api/credentials."""
    if not uid:
        raise HTTPException(status_code=400, detail="Missing uid")

    api_token = _get_or_create_api_token(uid)

    deployed_url = os.getenv("VERCEL_URL", "https://clawmarketer.vercel.app")
    if deployed_url and not deployed_url.startswith("http"):
        deployed_url = f"https://{deployed_url}"

    # Include Telegram chat_id if connected
    profile      = _read_from_firestore(uid, "meta", "profile") or {}
    chat_id      = profile.get("telegram_chat_id", "")
    bot_token    = os.getenv("TELEGRAM_BOT_TOKEN", "")

    lines = [
        "# ClawMarketer — OpenClaw Agent Config",
        "# Generated from clawmarketer.vercel.app",
        "# Skills fetch all credentials automatically — no Meta/Groq keys needed here.",
        "",
        f"CLAWMARKETER_URL={deployed_url}",
        f"CLAWMARKETER_USER_ID={uid}",
        f"CLAWMARKETER_API_TOKEN={api_token}",
        "",
        "# Telegram — agents send reports to this chat",
        f"TELEGRAM_BOT_TOKEN={bot_token}",
        f"TELEGRAM_CHAT_ID={chat_id}" if chat_id else "TELEGRAM_CHAT_ID=  # Connect Telegram in the app first",
        "",
        "# Folder the data cleansing agent scans for CSV/Excel files",
        "DATA_DIR=~/Documents/data",
    ]

    from fastapi.responses import Response
    return Response(
        content="\n".join(lines),
        media_type="text/plain",
        headers={"Content-Disposition": "attachment; filename=clawmarketer.env"}
    )


# ── OpenClaw progress push ─────────────────────────────────────────────────────

class ProgressPayload(BaseModel):
    user_id: str
    run_id: str
    skill: str = "unknown"
    stage: int
    status: str
    message: str
    done: bool = False
    result: Optional[dict] = None
    attachments: Optional[list] = None  # [{"name": str, "type": "photo"|"document", "telegram_message_id": int}]

@app.post("/api/agent/push")
def agent_push(payload: ProgressPayload):
    """OpenClaw pushes progress/results here. Forwarded to Firestore via REST."""
    project_id = os.getenv("FIREBASE_PROJECT_ID", "")
    api_key    = os.getenv("FIREBASE_API_KEY", "")

    if not project_id:
        # No Firebase — just ack (local dev without Firebase)
        return {"ok": True}

    def to_fs(value):
        if value is None:           return {"nullValue": None}
        if isinstance(value, bool): return {"booleanValue": value}
        if isinstance(value, int):  return {"integerValue": str(value)}
        if isinstance(value, float):return {"doubleValue": value}
        if isinstance(value, str):  return {"stringValue": value}
        if isinstance(value, list): return {"arrayValue": {"values": [to_fs(v) for v in value]}}
        if isinstance(value, dict): return {"mapValue": {"fields": {k: to_fs(v) for k, v in value.items()}}}
        return {"stringValue": str(value)}

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    # Overall run status: "running" until done=True
    overall_status = "done" if payload.done else ("error" if payload.status == "error" else "running")

    # Build the stage map entry for this specific stage
    stage_entry = {
        "status":  payload.status,
        "message": payload.message,
    }

    # Top-level document fields
    doc_fields: dict = {
        "run_id":     to_fs(payload.run_id),
        "skill":      to_fs(payload.skill),
        "status":     to_fs(overall_status),
        "updated_at": to_fs(now),
        f"stages.{payload.stage}": to_fs(stage_entry),
    }

    # On first stage, also set created_at and triggered_by
    if payload.stage == 1:
        doc_fields["created_at"]    = to_fs(now)
        doc_fields["triggered_by"]  = to_fs("telegram")

    # If final result provided, store it
    if payload.result:
        doc_fields["result"] = to_fs(payload.result)

    # Attachments sent via Telegram
    if payload.attachments:
        doc_fields["attachments"] = to_fs(payload.attachments)

    url = (
        f"https://firestore.googleapis.com/v1/projects/{project_id}"
        f"/databases/(default)/documents"
        f"/users/{payload.user_id}/runs/{payload.run_id}"
    )

    # Use field mask update so we don't wipe other stages
    update_mask_fields = list(doc_fields.keys())
    params = {"key": api_key}
    for field in update_mask_fields:
        params.setdefault("updateMask.fieldPaths", [])
        if isinstance(params["updateMask.fieldPaths"], list):
            params["updateMask.fieldPaths"].append(field)

    resp = http.patch(url, json={"fields": doc_fields}, params=params)

    if resp.status_code not in (200, 201):
        # Log but don't fail — agent should keep running even if dashboard push fails
        print(f"[Firestore] Push failed {resp.status_code}: {resp.text[:200]}")

    return {"ok": True}
