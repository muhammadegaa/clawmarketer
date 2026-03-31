"""
Tests for ClawMarketer FastAPI server endpoints.
Run: pytest tests/test_server.py -v
"""
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from server import app

client = TestClient(app)

VALID_UID   = "user_abc123"
VALID_TOKEN = "sk_cm_testtoken123"
VALID_AUTH  = {"Authorization": f"Bearer {VALID_TOKEN}"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _mock_profile(uid, collection, doc_id):
    if collection == "meta" and doc_id == "profile":
        return {"api_token": VALID_TOKEN, "telegram_chat_id": "999111"}
    if collection == "integrations" and doc_id == "meta":
        return {"access_token": "EAAtest", "account_id": "act_123"}
    return {}


# ── Config ────────────────────────────────────────────────────────────────────

def test_config_returns_firebase_keys():
    resp = client.get("/api/config")
    assert resp.status_code == 200
    body = resp.json()
    assert "firebase" in body
    assert "apiKey" in body["firebase"]


# ── API Token ─────────────────────────────────────────────────────────────────

@patch("server._read_from_firestore", return_value={})
@patch("server._write_to_firestore")
def test_get_api_token_creates_new(mock_write, mock_read):
    resp = client.get(f"/api/auth/token?uid={VALID_UID}")
    assert resp.status_code == 200
    token = resp.json()["token"]
    assert token.startswith("sk_cm_")
    assert len(token) > 10
    mock_write.assert_called_once()


@patch("server._read_from_firestore", return_value={"api_token": VALID_TOKEN})
def test_get_api_token_returns_existing(mock_read):
    resp = client.get(f"/api/auth/token?uid={VALID_UID}")
    assert resp.status_code == 200
    assert resp.json()["token"] == VALID_TOKEN


@patch("server._write_to_firestore")
def test_rotate_api_token(mock_write):
    resp = client.post(f"/api/auth/token?uid={VALID_UID}")
    assert resp.status_code == 200
    new_token = resp.json()["token"]
    assert new_token.startswith("sk_cm_")
    assert new_token != VALID_TOKEN  # rotated — should be different


def test_get_api_token_missing_uid():
    resp = client.get("/api/auth/token")
    assert resp.status_code == 400


# ── Credentials ───────────────────────────────────────────────────────────────

@patch("server._read_from_firestore", side_effect=_mock_profile)
def test_get_credentials_valid_token(mock_read):
    resp = client.get(f"/api/credentials?uid={VALID_UID}", headers=VALID_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert "meta" in body
    assert body["meta"]["access_token"] == "EAAtest"


def test_get_credentials_missing_auth():
    resp = client.get(f"/api/credentials?uid={VALID_UID}")
    assert resp.status_code == 401


@patch("server._read_from_firestore", side_effect=_mock_profile)
def test_get_credentials_wrong_token(mock_read):
    resp = client.get(f"/api/credentials?uid={VALID_UID}",
                      headers={"Authorization": "Bearer sk_cm_wrong"})
    assert resp.status_code == 403


# ── Telegram Registration ─────────────────────────────────────────────────────

@patch("server._write_telegram_code")
def test_telegram_register_generates_code(mock_write):
    resp = client.post(f"/api/telegram/register?uid={VALID_UID}")
    assert resp.status_code == 200
    body = resp.json()
    assert "code" in body
    assert len(body["code"]) == 6
    assert body["code"].isdigit()
    assert body["expires_in"] == 600


def test_telegram_register_missing_uid():
    resp = client.post("/api/telegram/register")
    assert resp.status_code == 400


@patch("server._read_telegram_code")
@patch("server._write_to_firestore")
@patch("server._write_telegram_code")
def test_telegram_verify_valid_code(mock_write_code, mock_write_fs, mock_read_code):
    from datetime import datetime, timezone, timedelta
    mock_read_code.return_value = {
        "uid":        VALID_UID,
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        "used":       False,
    }
    resp = client.post("/api/telegram/verify",
                       json={"code": "123456", "chat_id": "777888999"})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert resp.json()["uid"] == VALID_UID
    # Chat ID should be persisted
    mock_write_fs.assert_called_once_with(VALID_UID, "meta", "profile",
                                          {"telegram_chat_id": "777888999"})


@patch("server._read_telegram_code", return_value=None)
def test_telegram_verify_invalid_code(mock_read):
    resp = client.post("/api/telegram/verify",
                       json={"code": "000000", "chat_id": "777"})
    assert resp.status_code == 404


@patch("server._read_telegram_code")
def test_telegram_verify_expired_code(mock_read):
    from datetime import datetime, timezone, timedelta
    mock_read.return_value = {
        "uid":        VALID_UID,
        "expires_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        "used":       False,
    }
    resp = client.post("/api/telegram/verify",
                       json={"code": "123456", "chat_id": "777"})
    assert resp.status_code == 410


@patch("server._read_telegram_code")
def test_telegram_verify_already_used(mock_read):
    from datetime import datetime, timezone, timedelta
    mock_read.return_value = {
        "uid":        VALID_UID,
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        "used":       True,
    }
    resp = client.post("/api/telegram/verify",
                       json={"code": "123456", "chat_id": "777"})
    assert resp.status_code == 410


@patch("server._read_from_firestore",
       return_value={"telegram_chat_id": "999111", "api_token": VALID_TOKEN})
def test_telegram_status_connected(mock_read):
    resp = client.get(f"/api/telegram/status?uid={VALID_UID}")
    assert resp.status_code == 200
    assert resp.json()["connected"] is True
    assert resp.json()["chat_id"] == "999111"


@patch("server._read_from_firestore", return_value={"api_token": VALID_TOKEN})
def test_telegram_status_not_connected(mock_read):
    resp = client.get(f"/api/telegram/status?uid={VALID_UID}")
    assert resp.status_code == 200
    assert resp.json()["connected"] is False


# ── Meta Insights ─────────────────────────────────────────────────────────────

@patch("server._read_from_firestore", side_effect=_mock_profile)
@patch("server.fetcher.fetch")
def test_meta_insights_uses_real_api_when_token_present(mock_fetch, mock_read):
    import pandas as pd
    mock_fetch.return_value = pd.DataFrame([
        {"Campaign name": "Test", "Amount spent (USD)": 100, "Impressions": 1000}
    ])
    resp = client.post("/api/integrations/meta/insights",
                       json={"uid": VALID_UID, "date_preset": "last_7d"},
                       headers=VALID_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["data_source"] == "meta_api"
    assert body["rows"] == 1


@patch("server._read_from_firestore", side_effect=_mock_profile)
@patch("server.fetcher.fetch", side_effect=Exception("API error"))
@patch("server.os.unlink")
def test_meta_insights_falls_back_to_sample_on_api_error(mock_unlink, mock_fetch, mock_read):
    resp = client.post("/api/integrations/meta/insights",
                       json={"uid": VALID_UID, "date_preset": "last_7d"},
                       headers=VALID_AUTH)
    assert resp.status_code == 200
    assert resp.json()["data_source"] == "sample"
    assert resp.json()["rows"] > 0


def test_meta_insights_rejects_missing_auth():
    resp = client.post("/api/integrations/meta/insights",
                       json={"uid": VALID_UID, "date_preset": "last_7d"})
    assert resp.status_code == 401


@patch("server._read_from_firestore", side_effect=_mock_profile)
def test_meta_insights_rejects_wrong_token(mock_read):
    resp = client.post("/api/integrations/meta/insights",
                       json={"uid": VALID_UID, "date_preset": "last_7d"},
                       headers={"Authorization": "Bearer sk_cm_wrong"})
    assert resp.status_code == 403


# ── AI Endpoints ──────────────────────────────────────────────────────────────

@patch("server._read_from_firestore", side_effect=_mock_profile)
@patch("server.reporter.generate", return_value="AI report text")
@patch("server.os.getenv", side_effect=lambda k, d="": "fake_groq_key" if k == "GROQ_API_KEY" else d)
def test_ai_report_returns_text(mock_env, mock_reporter, mock_read):
    resp = client.post("/api/ai/report",
                       json={"uid": VALID_UID, "analysis": {"overall": {}, "anomalies": []}},
                       headers=VALID_AUTH)
    assert resp.status_code == 200
    assert resp.json()["report"] == "AI report text"


@patch("server._read_from_firestore", side_effect=_mock_profile)
def test_ai_report_rejects_wrong_token(mock_read):
    resp = client.post("/api/ai/report",
                       json={"uid": VALID_UID, "analysis": {}},
                       headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 403


@patch("server._read_from_firestore", side_effect=_mock_profile)
@patch("server.os.getenv", side_effect=lambda k, d="": "fake_groq_key" if k == "GROQ_API_KEY" else d)
def test_ai_complete_calls_groq(mock_env, mock_read):
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value.choices[0].message.content = "Answer"
    with patch("groq.Groq", return_value=mock_client):
        resp = client.post("/api/ai/complete",
                           json={"uid": VALID_UID, "prompt": "What is ROAS?"},
                           headers=VALID_AUTH)
    assert resp.status_code == 200
    assert resp.json()["text"] == "Answer"


# ── Agent Push ────────────────────────────────────────────────────────────────

@patch("server.os.getenv", side_effect=lambda k, d="": "proj123" if k == "FIREBASE_PROJECT_ID" else ("key123" if k == "FIREBASE_API_KEY" else d))
@patch("server.http.patch")
def test_agent_push_accepted(mock_patch, mock_env):
    mock_patch.return_value.status_code = 200
    resp = client.post("/api/agent/push", json={
        "user_id": VALID_UID,
        "run_id":  "run_001",
        "skill":   "clawmarketer-meta",
        "stage":   1,
        "status":  "running",
        "message": "Fetching data...",
        "done":    False,
    })
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_agent_push_with_result_and_attachments():
    with patch("server.os.getenv", return_value=""):  # no Firebase = just ack
        resp = client.post("/api/agent/push", json={
            "user_id":     VALID_UID,
            "run_id":      "run_001",
            "skill":       "clawmarketer-meta",
            "stage":       4,
            "status":      "done",
            "message":     "Done",
            "done":        True,
            "result":      {"total_spend": 1200.0, "avg_roas": 3.2},
            "attachments": [{"name": "chart.png", "type": "photo", "telegram_message_id": 42}],
        })
    assert resp.status_code == 200


# ── OpenClaw Config ───────────────────────────────────────────────────────────

@patch("server._read_from_firestore", side_effect=_mock_profile)
@patch("server._get_or_create_api_token", return_value=VALID_TOKEN)
@patch("server.os.getenv", side_effect=lambda k, d="": {
    "VERCEL_URL":           "https://clawmarketer.vercel.app",
    "TELEGRAM_BOT_TOKEN":   "bot123:AAF",
}.get(k, d))
def test_openclaw_config_includes_all_keys(mock_env, mock_token, mock_read):
    resp = client.get(f"/api/openclaw-config?uid={VALID_UID}")
    assert resp.status_code == 200
    content = resp.text
    assert "CLAWMARKETER_URL=https://clawmarketer.vercel.app" in content
    assert f"CLAWMARKETER_USER_ID={VALID_UID}" in content
    assert f"CLAWMARKETER_API_TOKEN={VALID_TOKEN}" in content
    assert "TELEGRAM_BOT_TOKEN=bot123:AAF" in content
    assert "TELEGRAM_CHAT_ID=999111" in content  # from mock profile
    assert "DATA_DIR=~/Documents/data" in content


def test_openclaw_config_missing_uid():
    resp = client.get("/api/openclaw-config")
    assert resp.status_code in (400, 422)
