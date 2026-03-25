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
        raise HTTPException(status_code=404, detail="No data returned for this account and date range.")

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

@app.post("/api/demo")
def run_demo():
    groq_key = os.getenv("GROQ_API_KEY", "")
    from sample_data import generate
    csv_path = generate()
    df, clean_stats = cleaner.clean(csv_path)
    os.unlink(csv_path)
    results = analyzer.run(df)
    try:
        results["report"] = reporter.generate(results, groq_key)
    except Exception as e:
        results["report"] = f"AI report unavailable: {str(e)}"
    return _serialize(results, clean_stats)


# ── OpenClaw config download ───────────────────────────────────────────────────

@app.get("/api/openclaw-config")
def openclaw_config(uid: str, token: str = ""):
    """Generate a pre-filled .env for the user's OpenClaw setup."""
    if not uid:
        raise HTTPException(status_code=400, detail="Missing uid")

    meta_token = token

    deployed_url = os.getenv("VERCEL_URL", "https://clawmarketer.vercel.app")
    if deployed_url and not deployed_url.startswith("http"):
        deployed_url = f"https://{deployed_url}"

    lines = [
        "# ClawMarketer — OpenClaw Agent Config",
        "# Generated from clawmarketer.vercel.app",
        "",
        f"CLAWMARKETER_URL={deployed_url}",
        f"CLAWMARKETER_USER_ID={uid}",
        "",
        "# Meta Ads token (pre-filled from your connected account)",
        f"META_ACCESS_TOKEN={meta_token}",
        "",
        f"GROQ_API_KEY={os.getenv('GROQ_API_KEY', '')}",
        "",
        "# Set this to the folder you want the data cleansing agent to scan",
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
    stage: int
    status: str
    message: str
    done: bool = False
    result: Optional[dict] = None

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
        "status":     to_fs(overall_status),
        "updated_at": to_fs(now),
        f"stages.{payload.stage}": to_fs(stage_entry),
    }

    # On first stage, also set created_at
    if payload.stage == 1:
        doc_fields["created_at"] = to_fs(now)

    # If final result provided, store it
    if payload.result:
        doc_fields["result"] = to_fs(payload.result)

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
