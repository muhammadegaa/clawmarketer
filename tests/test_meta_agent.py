"""
Tests for the Meta Ads Intelligence Agent.
Run: pytest tests/test_meta_agent.py -v
"""
import os
import sys
import importlib.util
import pytest
import pandas as pd
import tempfile
from unittest.mock import patch, MagicMock

# Load meta agent directly to avoid sys.modules['agent'] collisions
_meta_agent_path = os.path.join(os.path.dirname(__file__), "..", "skills", "clawmarketer-meta", "agent.py")
_spec = importlib.util.spec_from_file_location("meta_agent", _meta_agent_path)
meta_agent = importlib.util.module_from_spec(_spec)
sys.modules["meta_agent"] = meta_agent  # register so patch("meta_agent.x") works
_spec.loader.exec_module(meta_agent)


# ── clean() ───────────────────────────────────────────────────────────────────

class TestClean:

    def test_strips_dollar_signs_from_spend(self):
        df = pd.DataFrame([{"Campaign name": "Test", "Amount spent (USD)": "$1,200.50",
                             "Impressions": "45000", "Clicks (all)": "900"}])
        cleaned, stats = meta_agent.clean(df)
        assert cleaned["spend"].iloc[0] == pytest.approx(1200.50)

    def test_strips_percent_from_ctr(self):
        df = pd.DataFrame([{"Campaign name": "Test", "Amount spent (USD)": "100",
                             "CTR (all)": "2.50%", "Impressions": "10000"}])
        cleaned, _ = meta_agent.clean(df)
        assert cleaned["ctr"].iloc[0] == pytest.approx(2.50)

    def test_drops_total_summary_rows(self):
        df = pd.DataFrame([
            {"Campaign name": "Real Campaign", "Amount spent (USD)": "500"},
            {"Campaign name": "Total", "Amount spent (USD)": "500"},
            {"Campaign name": "REPORT TOTALS", "Amount spent (USD)": "1000"},
        ])
        cleaned, stats = meta_agent.clean(df)
        assert len(cleaned) == 1
        assert stats["dropped_rows"] == 2

    def test_drops_fully_null_rows(self):
        df = pd.DataFrame([
            {"Campaign name": "Real Campaign", "Amount spent (USD)": "500"},
            {"Campaign name": None, "Amount spent (USD)": None},
        ])
        cleaned, stats = meta_agent.clean(df)
        assert len(cleaned) == 1

    def test_normalises_column_names(self):
        df = pd.DataFrame([{
            "Campaign name": "Test",
            "Amount spent (USD)": "100",
            "Clicks (all)": "50",
            "Purchase ROAS (return on ad spend)": "2.5",
        }])
        cleaned, _ = meta_agent.clean(df)
        assert "campaign_name" in cleaned.columns
        assert "spend" in cleaned.columns
        assert "clicks" in cleaned.columns
        assert "roas" in cleaned.columns

    def test_roas_coerced_to_float(self):
        df = pd.DataFrame([{"Campaign name": "Test", "Amount spent (USD)": "100",
                             "Purchase ROAS (return on ad spend)": "3.20"}])
        cleaned, _ = meta_agent.clean(df)
        assert cleaned["roas"].dtype == float

    def test_handles_commas_in_numbers(self):
        df = pd.DataFrame([{"Campaign name": "Test", "Impressions": "1,234,567",
                             "Amount spent (USD)": "2,000.00"}])
        cleaned, _ = meta_agent.clean(df)
        assert cleaned["impressions"].iloc[0] == 1234567

    def test_clean_stats_accurate(self):
        df = pd.DataFrame([
            {"Campaign name": "A", "Amount spent (USD)": "100"},
            {"Campaign name": "B", "Amount spent (USD)": "200"},
            {"Campaign name": "Total", "Amount spent (USD)": "300"},
        ])
        _, stats = meta_agent.clean(df)
        assert stats["clean_rows"] == 2
        assert stats["dropped_rows"] == 1


# ── analyze() ─────────────────────────────────────────────────────────────────

class TestAnalyze:

    def setup_method(self):
        self.df = pd.DataFrame([
            {"campaign_name": "Retargeting",   "spend": 2000.0, "impressions": 60000,
             "clicks": 2400, "conversions": 200, "roas": 3.4, "frequency": 1.3},
            {"campaign_name": "Brand Awareness","spend": 4000.0, "impressions": 380000,
             "clicks": 1520, "conversions": 0,   "roas": None,  "frequency": 1.8},
            {"campaign_name": "Losing Campaign","spend": 400.0,  "impressions": 14000,
             "clicks": 28,   "conversions": 3,   "roas": 0.8,   "frequency": 1.7},
        ])

    def test_total_spend_summed(self):
        result = meta_agent.analyze(self.df)
        assert result["overall"]["total_spend"] == pytest.approx(6400.0)

    def test_total_impressions_summed(self):
        result = meta_agent.analyze(self.df)
        assert result["overall"]["total_impressions"] == 454000

    def test_overall_ctr_calculated(self):
        result = meta_agent.analyze(self.df)
        # (2400 + 1520 + 28) / 454000 * 100
        expected_ctr = round((2400 + 1520 + 28) / 454000 * 100, 2)
        assert result["overall"]["overall_ctr"] == pytest.approx(expected_ctr, rel=0.01)

    def test_avg_roas_excludes_none(self):
        result = meta_agent.analyze(self.df)
        # Only 2 campaigns have ROAS: 3.4 and 0.8
        assert result["overall"]["avg_roas"] == pytest.approx((3.4 + 0.8) / 2, rel=0.01)

    def test_num_campaigns_counted(self):
        result = meta_agent.analyze(self.df)
        assert result["overall"]["num_campaigns"] == 3

    def test_detects_low_roas_anomaly(self):
        result = meta_agent.analyze(self.df)
        anomalies = result["anomalies"]
        assert any("Losing Campaign" in a and "ROAS" in a for a in anomalies)

    def test_detects_low_ctr_anomaly(self):
        result = meta_agent.analyze(self.df)
        anomalies = result["anomalies"]
        # Losing Campaign has CTR 28/14000*100 = 0.2% — below 0.5 threshold
        assert any("Losing Campaign" in a and "CTR" in a for a in anomalies)

    def test_no_false_positive_anomalies_on_healthy_data(self):
        healthy_df = pd.DataFrame([{
            "campaign_name": "Good Campaign",
            "spend": 1000.0, "impressions": 50000, "clicks": 1500,
            "roas": 3.5,
        }])
        result = meta_agent.analyze(healthy_df)
        assert result["anomalies"] == []

    def test_campaign_summary_sorted_by_spend_desc(self):
        result = meta_agent.analyze(self.df)
        spends = result["campaign_summary"]["spend"].tolist()
        assert spends == sorted(spends, reverse=True)


# ── _fetch_insights() ─────────────────────────────────────────────────────────

class TestFetchInsights:

    @patch("meta_agent.requests.post")
    def test_returns_dataframe_on_success(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {
            "data_source": "meta_api",
            "rows": 2,
            "data": [
                {"Campaign name": "A", "Amount spent (USD)": 100},
                {"Campaign name": "B", "Amount spent (USD)": 200},
            ]
        }
        df, source = meta_agent._fetch_insights("last_7d")
        assert len(df) == 2
        assert source == "meta_api"

    @patch("meta_agent.requests.post")
    def test_returns_empty_on_server_error(self, mock_post):
        mock_post.return_value.status_code = 500
        df, source = meta_agent._fetch_insights("last_7d")
        assert df.empty
        assert source == "error"

    @patch("meta_agent.requests.post", side_effect=Exception("Connection refused"))
    def test_returns_empty_on_network_failure(self, mock_post):
        df, source = meta_agent._fetch_insights("last_7d")
        assert df.empty
        assert source == "error"

    @patch("meta_agent.requests.post")
    def test_passes_date_preset_to_server(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"data_source": "sample", "rows": 0, "data": []}
        meta_agent._fetch_insights("last_month")
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["json"]["date_preset"] == "last_month"


# ── _generate_report_via_api() ────────────────────────────────────────────────

class TestGenerateReport:

    @patch("meta_agent.requests.post")
    def test_returns_report_text(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"report": "Great performance this month."}
        result = meta_agent._generate_report_via_api({"overall": {}, "anomalies": []})
        assert result == "Great performance this month."

    @patch("meta_agent.requests.post")
    def test_returns_error_message_on_failure(self, mock_post):
        mock_post.return_value.status_code = 503
        result = meta_agent._generate_report_via_api({"overall": {}, "anomalies": []})
        assert "unavailable" in result.lower() or "503" in result

    @patch("meta_agent.requests.post", side_effect=Exception("Timeout"))
    def test_handles_network_exception(self, mock_post):
        result = meta_agent._generate_report_via_api({"overall": {}, "anomalies": []})
        assert "unavailable" in result.lower()


# ── _load_local_csv() ─────────────────────────────────────────────────────────

class TestLoadLocalCsv:

    def test_finds_ads_csv_by_column_content(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "export.csv")
            pd.DataFrame([{
                "campaign name": "Test", "spend": 100,
                "impressions": 1000, "clicks": 50,
                "ctr": 5.0, "roas": 2.0
            }]).to_csv(path, index=False)

            with patch.dict(os.environ, {"DATA_DIR": tmpdir}):
                df, name = meta_agent._load_local_csv()

            assert not df.empty
            assert name == "export.csv"

    def test_ignores_clean_prefixed_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pd.DataFrame([{"campaign name": "Test", "spend": 100,
                           "impressions": 1000, "clicks": 50}]).to_csv(
                os.path.join(tmpdir, "clean_export.csv"), index=False)

            with patch.dict(os.environ, {"DATA_DIR": tmpdir}):
                df, name = meta_agent._load_local_csv()

            assert df.empty

    def test_returns_empty_when_no_dir(self):
        with patch.dict(os.environ, {"DATA_DIR": "/nonexistent/path/xyz"}):
            df, name = meta_agent._load_local_csv()
        assert df.empty
        assert name == ""

    def test_prefers_ads_named_file_over_generic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for fname in ["sales_data.csv", "meta_ads_export.csv"]:
                pd.DataFrame([{
                    "campaign name": "Test", "spend": 100,
                    "impressions": 1000, "clicks": 50, "ctr": 5.0, "roas": 2.0
                }]).to_csv(os.path.join(tmpdir, fname), index=False)

            with patch.dict(os.environ, {"DATA_DIR": tmpdir}):
                df, name = meta_agent._load_local_csv()

            assert name == "meta_ads_export.csv"


# ── make_charts() ─────────────────────────────────────────────────────────────

class TestMakeCharts:

    def test_generates_three_charts_with_roas(self):
        df = pd.DataFrame([
            {"campaign_name": "A", "spend": 1000, "impressions": 50000,
             "clicks": 1000, "roas": 3.0, "ctr_calc": 2.0},
            {"campaign_name": "B", "spend": 500,  "impressions": 30000,
             "clicks": 300,  "roas": 1.5, "ctr_calc": 1.0},
        ])
        analysis = {"campaign_summary": df, "overall": {}, "anomalies": []}
        charts, csv_path = meta_agent.make_charts(analysis)
        assert len(charts) == 3  # spend, ctr, roas
        for path in charts:
            assert os.path.exists(path)
        assert csv_path and os.path.exists(csv_path)

    def test_skips_roas_chart_when_no_roas_data(self):
        df = pd.DataFrame([
            {"campaign_name": "A", "spend": 1000, "impressions": 50000,
             "clicks": 1000, "ctr_calc": 2.0},
        ])
        analysis = {"campaign_summary": df, "overall": {}, "anomalies": []}
        charts, _ = meta_agent.make_charts(analysis)
        # No ROAS column → only spend + CTR
        assert len(charts) == 2

    def test_returns_empty_on_empty_summary(self):
        analysis = {"campaign_summary": pd.DataFrame(), "overall": {}, "anomalies": []}
        charts, csv_path = meta_agent.make_charts(analysis)
        assert charts == []
        assert csv_path is None


# ── handle() — date preset parsing ───────────────────────────────────────────

class TestHandle:

    @patch("meta_agent.run")
    @patch("meta_agent.send_message")
    def test_last_7_days_preset(self, mock_send, mock_run):
        meta_agent.handle("analyze ads last 7 days")
        mock_run.assert_called_once_with(date_preset="last_7d")

    @patch("meta_agent.run")
    @patch("meta_agent.send_message")
    def test_last_month_preset(self, mock_send, mock_run):
        meta_agent.handle("how are my ads last month")
        mock_run.assert_called_once_with(date_preset="last_month")

    @patch("meta_agent.run")
    @patch("meta_agent.send_message")
    def test_defaults_to_last_30d(self, mock_send, mock_run):
        meta_agent.handle("analyze ads")
        mock_run.assert_called_once_with(date_preset="last_30d")
