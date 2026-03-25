import os
import secrets
import tempfile
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
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
    return {
        "clean_stats": {
            "original_rows": clean_stats["original_rows"],
            "clean_rows":    clean_stats["clean_rows"],
            "dropped_rows":  clean_stats["dropped_rows"],
        },
        "overall":   results["overall"],
        "campaigns": campaign_rows,
        "anomalies": results["anomalies"],
        "report":    results.get("report", ""),
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

@app.get("/prompts")
def prompts():
    return FileResponse("static/prompts.html")

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
    if state != request.session.get("oauth_state"):
        return RedirectResponse("/?error=invalid_state")

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

    # Extend to long-lived token (60 days)
    extend_resp = http.get(META_TOKEN_URL, params={
        "grant_type":    "fb_exchange_token",
        "client_id":     META_APP_ID,
        "client_secret": META_APP_SECRET,
        "fb_exchange_token": access_token,
    })
    extended = extend_resp.json()
    if "access_token" in extended:
        access_token = extended["access_token"]

    request.session["meta_token"] = access_token
    request.session.pop("oauth_state", None)
    return RedirectResponse("/?connected=1")


@app.get("/auth/status")
def auth_status(request: Request):
    token = request.session.get("meta_token")
    if not token:
        return {"connected": False}

    # Fetch ad accounts
    resp = http.get(META_ACCOUNTS_URL, params={
        "access_token": token,
        "fields": "id,name,account_status",
    })
    data = resp.json()
    if "error" in data:
        request.session.pop("meta_token", None)
        return {"connected": False}

    accounts = [
        {"id": a["id"], "name": a.get("name", a["id"])}
        for a in data.get("data", [])
        if a.get("account_status") == 1  # 1 = active
    ]
    return {"connected": True, "accounts": accounts}


@app.post("/auth/disconnect")
def disconnect(request: Request):
    request.session.pop("meta_token", None)
    return {"ok": True}


# ── Run pipeline with live Meta data ─────────────────────────────────────────

@app.post("/api/run")
def run_live(
    request: Request,
    account_id: str = Form(...),
    groq_api_key: str = Form(...),
    date_preset: str = Form("last_30d"),
):
    token = request.session.get("meta_token")
    if not token:
        raise HTTPException(status_code=401, detail="Not connected to Meta. Please reconnect.")

    try:
        df_raw = fetcher.fetch(
            access_token=token,
            ad_account_id=account_id,
            date_preset=date_preset,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Meta API error: {str(e)}")

    if df_raw.empty:
        raise HTTPException(status_code=404, detail="No data returned for this account and date range.")

    return _run_pipeline(df_raw, groq_api_key)


# ── Upload CSV ────────────────────────────────────────────────────────────────

@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...), groq_api_key: str = Form(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV.")

    contents = await file.read()
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    try:
        df, clean_stats = cleaner.clean(tmp_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {str(e)}")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    results = analyzer.run(df)
    try:
        results["report"] = reporter.generate(results, groq_api_key)
    except Exception as e:
        results["report"] = f"AI report unavailable: {str(e)}"

    return _serialize(results, clean_stats)


# ── Demo ──────────────────────────────────────────────────────────────────────

@app.post("/api/demo")
def run_demo(groq_api_key: str):
    from sample_data import generate
    csv_path = generate()
    df, clean_stats = cleaner.clean(csv_path)
    os.unlink(csv_path)
    results = analyzer.run(df)
    try:
        results["report"] = reporter.generate(results, groq_api_key)
    except Exception as e:
        results["report"] = f"AI report unavailable: {str(e)}"
    return _serialize(results, clean_stats)
