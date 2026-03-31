"""
Tests for bot routing, Telegram registration flow, and copy agent parsing.
Run: pytest tests/test_bot_routing.py -v
"""
import os
import sys
import importlib.util
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import clawmarketer_bot as bot

# Load copy agent directly from local path to avoid sys.path conflicts
_copy_agent_path = os.path.join(os.path.dirname(__file__), "..", "skills", "clawmarketer-copy", "agent.py")
_spec = importlib.util.spec_from_file_location("copy_agent", _copy_agent_path)
copy_agent = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(copy_agent)


# ── Keyword routing ───────────────────────────────────────────────────────────

class TestRouteKeywords:

    # Meta triggers
    @pytest.mark.parametrize("text", [
        "analyze ads", "Analyze Ads", "run ads report",
        "meta ads report", "how are my ads", "ads report",
        "campaign report", "facebook ads", "meta ads",
        "ad performance", "check my roas", "campaign performance", "my campaigns",
    ])
    def test_meta_triggers(self, text):
        assert bot._route_keywords(text) == "meta"

    # Data triggers
    @pytest.mark.parametrize("text", [
        "clean my data", "clean data", "analyze my files",
        "process my data", "clean files", "data cleaning",
        "fix my csv", "clean csv",
    ])
    def test_data_triggers(self, text):
        assert bot._route_keywords(text) == "data"

    # Morning brief triggers
    @pytest.mark.parametrize("text", [
        "morning brief", "daily brief", "daily summary",
        "morning report", "how did we do", "send brief",
    ])
    def test_brief_triggers(self, text):
        assert bot._route_keywords(text) == "brief"

    # Copy triggers
    @pytest.mark.parametrize("text", [
        "write ads for my product", "generate copy for shoes",
        "ad copy for coffee", "create ads for this service",
        "write ad copy", "ad variations for launch", "generate ads",
    ])
    def test_copy_triggers(self, text):
        assert bot._route_keywords(text) == "copy"

    # Anomaly triggers
    @pytest.mark.parametrize("text", [
        "check anomalies", "any issues with campaigns",
        "check campaigns", "anything wrong", "run anomaly check",
    ])
    def test_anomaly_triggers(self, text):
        assert bot._route_keywords(text) == "anomaly"

    # Unknown — falls through to LLM
    @pytest.mark.parametrize("text", [
        "hello", "help me with marketing",
        "what's the weather", "",
    ])
    def test_unknown_returns_none(self, text):
        assert bot._route_keywords(text) is None

    def test_case_insensitive(self):
        assert bot._route_keywords("ANALYZE ADS") == "meta"
        assert bot._route_keywords("CLEAN MY DATA") == "data"
        assert bot._route_keywords("MORNING BRIEF") == "brief"


# ── LLM routing ───────────────────────────────────────────────────────────────

class TestRouteLlm:

    @patch("clawmarketer_bot.requests.post")
    def test_routes_meta_intent(self, mock_post):
        mock_post.return_value.json.return_value = {
            "choices": [{"message": {"content": "meta"}}]
        }
        with patch.dict(os.environ, {"GROQ_API_KEY": "fake_key"}):
            result = bot._route_llm("show me my Facebook campaign results")
        assert result == "meta"

    @patch("clawmarketer_bot.requests.post")
    def test_routes_data_intent(self, mock_post):
        mock_post.return_value.json.return_value = {
            "choices": [{"message": {"content": "data"}}]
        }
        with patch.dict(os.environ, {"GROQ_API_KEY": "fake_key"}):
            result = bot._route_llm("my spreadsheet is a mess, can you fix it")
        assert result == "data"

    @patch("clawmarketer_bot.requests.post")
    def test_returns_none_for_unknown_intent(self, mock_post):
        mock_post.return_value.json.return_value = {
            "choices": [{"message": {"content": "unknown"}}]
        }
        with patch.dict(os.environ, {"GROQ_API_KEY": "fake_key"}):
            result = bot._route_llm("what should I eat for lunch?")
        assert result is None

    def test_returns_none_when_no_groq_key(self):
        with patch.dict(os.environ, {"GROQ_API_KEY": ""}):
            result = bot._route_llm("anything")
        assert result is None

    @patch("clawmarketer_bot.requests.post", side_effect=Exception("Timeout"))
    def test_returns_none_on_network_failure(self, mock_post):
        with patch.dict(os.environ, {"GROQ_API_KEY": "fake_key"}):
            result = bot._route_llm("show me campaigns")
        assert result is None


# ── /connect command ──────────────────────────────────────────────────────────

class TestConnectCommand:

    @patch("clawmarketer_bot.requests.post")
    @patch("clawmarketer_bot._send_to")
    def test_valid_connect_sends_success_message(self, mock_send, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"ok": True, "uid": "user123"}

        bot._handle_message("/connect 847291", "555666")

        mock_send.assert_called()
        args = mock_send.call_args[0]
        assert "connected" in args[1].lower()

    @patch("clawmarketer_bot.requests.post")
    @patch("clawmarketer_bot._send_to")
    def test_invalid_code_sends_error_message(self, mock_send, mock_post):
        mock_post.return_value.status_code = 404
        mock_post.return_value.json.return_value = {"detail": "Invalid code"}

        bot._handle_message("/connect 000000", "555666")

        args = mock_send.call_args[0]
        assert "❌" in args[1] or "connect" in args[1].lower()

    @patch("clawmarketer_bot._send_to")
    def test_connect_without_code_prompts_user(self, mock_send):
        bot._handle_message("/connect", "555666")
        args = mock_send.call_args[0]
        assert "code" in args[1].lower()

    @patch("clawmarketer_bot._send_to")
    def test_unknown_chat_id_ignored(self, mock_send):
        # CHAT_ID is set — message from different chat should be ignored
        with patch.object(bot, "CHAT_ID", "111111"):
            bot._handle_message("analyze ads", "999999")
        mock_send.assert_not_called()


# ── Message dispatch ──────────────────────────────────────────────────────────

class TestHandleMessage:

    @patch("clawmarketer_bot._run_meta")
    @patch("clawmarketer_bot.threading")
    def test_meta_message_starts_meta_thread(self, mock_threading, mock_run):
        with patch.object(bot, "CHAT_ID", ""):  # no chat_id restriction
            bot._handle_message("analyze ads", "123")
        mock_threading.Thread.assert_called()
        call_kwargs = mock_threading.Thread.call_args[1]
        assert call_kwargs["target"] == bot._run_meta

    @patch("clawmarketer_bot._run_copy")
    @patch("clawmarketer_bot.threading")
    def test_copy_message_starts_copy_thread(self, mock_threading, mock_run):
        with patch.object(bot, "CHAT_ID", ""):
            bot._handle_message("write ads for coffee subscription", "123")
        mock_threading.Thread.assert_called()
        call_kwargs = mock_threading.Thread.call_args[1]
        assert call_kwargs["target"] == bot._run_copy

    @patch("clawmarketer_bot._send")
    def test_start_command_sends_help(self, mock_send):
        with patch.object(bot, "CHAT_ID", ""):
            bot._handle_message("/start", "123")
        mock_send.assert_called()
        assert "ClawMarketer" in mock_send.call_args[0][0]

    @patch("clawmarketer_bot._send")
    def test_status_command_replies(self, mock_send):
        with patch.object(bot, "CHAT_ID", ""):
            bot._handle_message("/status", "123")
        mock_send.assert_called()


# ── Copy agent _parse() ───────────────────────────────────────────────────────

class TestCopyParse:

    @pytest.mark.parametrize("message,expected_product,expected_audience", [
        ("write ads for premium coffee targeting busy professionals",
         "premium coffee", "busy professionals"),
        ("generate copy for SaaS tool targeting startup founders",
         "saas tool", "startup founders"),
        ("ad copy for fitness app for women aged 25-35",
         "fitness app", "women aged 25-35"),
    ])
    def test_extracts_product_and_audience(self, message, expected_product, expected_audience):
        product, audience = copy_agent._parse(message)
        assert expected_product in product.lower()
        assert expected_audience in audience.lower()

    def test_falls_back_to_general_audience_when_none_specified(self):
        product, audience = copy_agent._parse("write ads for my new app")
        assert product  # has a product
        assert audience == "general audience"

    @pytest.mark.parametrize("message", [
        "write ads for coffee",
        "generate ads for shoes",
        "ad copy for software",
        "create ads for this thing",
    ])
    def test_handles_various_trigger_phrases(self, message):
        product, audience = copy_agent._parse(message)
        assert product  # always extracts something

    def test_returns_full_message_as_fallback(self):
        product, audience = copy_agent._parse("something completely different")
        assert product == "something completely different"


# ── Anomaly detection ─────────────────────────────────────────────────────────

class TestAnomalyDetect:
    """Import anomaly agent and test threshold logic directly."""

    def setup_method(self):
        _path = os.path.join(os.path.dirname(__file__), "..",
                             "skills", "clawmarketer-anomaly", "agent.py")
        _spec = importlib.util.spec_from_file_location("anomaly_agent", _path)
        anomaly_mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(anomaly_mod)
        self.anomaly = anomaly_mod

    def test_detects_roas_below_threshold(self):
        import pandas as pd
        df = pd.DataFrame([{
            "campaign_name": "Losing Campaign",
            "roas": 0.7, "ctr": 2.0, "cpc": 1.0,
        }])
        alerts = self.anomaly._detect(df)
        assert any("Losing Campaign" in a["campaign"] for a in alerts)
        assert any("ROAS" in a["issue"] for a in alerts)

    def test_detects_low_ctr(self):
        import pandas as pd
        df = pd.DataFrame([{
            "campaign_name": "Bad Creative",
            "roas": 2.0, "ctr": 0.1, "cpc": 1.0,
        }])
        alerts = self.anomaly._detect(df)
        assert any("Bad Creative" in a["campaign"] for a in alerts)
        assert any("CTR" in a["issue"] for a in alerts)

    def test_detects_high_cpc(self):
        import pandas as pd
        df = pd.DataFrame([{
            "campaign_name": "Expensive Campaign",
            "roas": 2.0, "ctr": 2.0, "cpc": 25.0,
        }])
        with patch.object(self.anomaly, "ALERT_CPC_MAX", 10.0):
            alerts = self.anomaly._detect(df)
        assert any("Expensive Campaign" in a["campaign"] for a in alerts)

    def test_detects_spend_with_zero_conversions(self):
        import pandas as pd
        df = pd.DataFrame([{
            "campaign_name": "Wasted Budget",
            "roas": None, "ctr": 1.5, "cpc": 2.0,
            "spend": 500.0, "conversions": 0,
        }])
        alerts = self.anomaly._detect(df)
        assert any("Wasted Budget" in a["campaign"] for a in alerts)

    def test_no_alerts_on_healthy_campaigns(self):
        import pandas as pd
        df = pd.DataFrame([{
            "campaign_name": "Healthy Campaign",
            "roas": 3.5, "ctr": 2.5, "cpc": 1.2,
            "spend": 500.0, "conversions": 50,
        }])
        alerts = self.anomaly._detect(df)
        assert alerts == []

    def test_critical_level_for_roas_below_one(self):
        import pandas as pd
        df = pd.DataFrame([{
            "campaign_name": "Critical",
            "roas": 0.5, "ctr": 2.0, "cpc": 1.0,
        }])
        alerts = self.anomaly._detect(df)
        critical = [a for a in alerts if "Critical" in a["campaign"]]
        assert any("Critical" in a["level"] for a in critical)
