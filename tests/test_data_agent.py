"""
Tests for the Data Cleansing Agent.
Run: pytest tests/test_data_agent.py -v
"""
import os
import sys
import importlib.util
import pytest
import pandas as pd
import tempfile
from unittest.mock import patch

# Load data agent directly to avoid sys.modules['agent'] collisions
_data_agent_path = os.path.join(os.path.dirname(__file__), "..", "skills", "clawmarketer-data", "agent.py")
_spec = importlib.util.spec_from_file_location("data_agent", _data_agent_path)
data_agent = importlib.util.module_from_spec(_spec)
sys.modules["data_agent"] = data_agent  # register so patch("data_agent.x") works
_spec.loader.exec_module(data_agent)

try:
    import openpyxl  # noqa: F401
    _OPENPYXL_AVAILABLE = True
except ImportError:
    _OPENPYXL_AVAILABLE = False


# ── _clean_file() ─────────────────────────────────────────────────────────────

class TestCleanFile:

    def _write_csv(self, rows, tmpdir, name="test.csv"):
        path = os.path.join(tmpdir, name)
        pd.DataFrame(rows).to_csv(path, index=False)
        return path

    def test_removes_duplicate_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv([
                {"name": "Alice", "email": "a@b.com"},
                {"name": "Alice", "email": "a@b.com"},  # duplicate
                {"name": "Bob",   "email": "b@b.com"},
            ], tmpdir)
            result = data_agent._clean_file(path)
            assert result["rows_removed"] == 1
            assert result["clean_rows"] == 2

    def test_removes_fully_blank_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv([
                {"name": "Alice", "email": "a@b.com"},
                {"name": None,    "email": None},
                {"name": "Bob",   "email": "b@b.com"},
            ], tmpdir)
            result = data_agent._clean_file(path)
            assert result["clean_rows"] == 2

    def test_normalises_column_names_to_snake_case(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv([
                {"First Name": "Alice", "Last Name": "Smith", "Email Address": "a@b.com"},
            ], tmpdir)
            result = data_agent._clean_file(path)
            clean_df = pd.read_csv(result["clean_path"])
            assert "first_name" in clean_df.columns
            assert "last_name"  in clean_df.columns
            assert "email_address" in clean_df.columns

    def test_strips_whitespace_from_strings(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv([
                {"name": "  Alice  ", "city": " London "},
            ], tmpdir)
            result = data_agent._clean_file(path)
            clean_df = pd.read_csv(result["clean_path"])
            assert clean_df["name"].iloc[0] == "Alice"
            assert clean_df["city"].iloc[0] == "London"

    def test_saves_clean_file_with_prefix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv([{"name": "Alice"}], tmpdir, "customers.csv")
            result = data_agent._clean_file(path)
            assert os.path.basename(result["clean_path"]) == "clean_customers.csv"
            assert os.path.exists(result["clean_path"])

    def test_returns_none_for_unknown_extension(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.txt")
            with open(path, "w") as f:
                f.write("hello")
            result = data_agent._clean_file(path)
            assert result is None

    def test_original_rows_count_is_accurate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            rows = [{"name": f"User{i}", "email": f"u{i}@b.com"} for i in range(10)]
            path = self._write_csv(rows, tmpdir)
            result = data_agent._clean_file(path)
            assert result["original_rows"] == 10

    @pytest.mark.skipif(
        not _OPENPYXL_AVAILABLE,
        reason="openpyxl not installed"
    )
    def test_handles_xlsx(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.xlsx")
            pd.DataFrame([{"name": "Alice", "email": "a@b.com"}]).to_excel(path, index=False)
            result = data_agent._clean_file(path)
            assert result is not None
            assert result["clean_rows"] >= 1


# ── _ai_summary() ─────────────────────────────────────────────────────────────

class TestAiSummary:

    @patch("data_agent.requests.post")
    def test_returns_summary_text(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"text": "3 files cleaned, 47 rows removed."}
        result = data_agent._ai_summary([
            {"file": "leads.csv", "original_rows": 23, "clean_rows": 19, "rows_removed": 4},
        ])
        assert result == "3 files cleaned, 47 rows removed."

    @patch("data_agent.requests.post")
    def test_returns_empty_string_on_server_error(self, mock_post):
        mock_post.return_value.status_code = 500
        result = data_agent._ai_summary([{"file": "test.csv", "original_rows": 10,
                                           "clean_rows": 9, "rows_removed": 1}])
        assert result == ""

    @patch("data_agent.requests.post", side_effect=Exception("Network error"))
    def test_handles_exception_gracefully(self, mock_post):
        result = data_agent._ai_summary([])
        assert "unavailable" in result.lower()


# ── run() ─────────────────────────────────────────────────────────────────────

class TestRun:

    @patch("data_agent.send_message")
    @patch("data_agent.send_document")
    @patch("data_agent._push")
    @patch("data_agent._ai_summary", return_value="Good data quality.")
    def test_processes_csv_files_in_dir(self, mock_ai, mock_push, mock_doc, mock_send):
        with tempfile.TemporaryDirectory() as tmpdir:
            pd.DataFrame([
                {"name": "Alice", "email": "a@b.com"},
                {"name": "Bob",   "email": "b@b.com"},
            ]).to_csv(os.path.join(tmpdir, "customers.csv"), index=False)

            result = data_agent.run(data_dir=tmpdir)

        assert "Data Cleaning Complete" in result
        assert mock_doc.called  # clean files sent to Telegram

    @patch("data_agent._push")
    def test_returns_error_when_dir_missing(self, mock_push):
        result = data_agent.run(data_dir="/nonexistent/path")
        assert "not found" in result.lower() or "❌" in result

    @patch("data_agent._push")
    def test_returns_message_when_no_files(self, mock_push):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = data_agent.run(data_dir=tmpdir)
        assert "No CSV" in result or "📂" in result

    @patch("data_agent.send_message")
    @patch("data_agent.send_document")
    @patch("data_agent._push")
    @patch("data_agent._ai_summary", return_value="")
    def test_skips_already_clean_files(self, mock_ai, mock_push, mock_doc, mock_send):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Only a clean_ prefixed file — should be ignored
            pd.DataFrame([{"name": "Alice"}]).to_csv(
                os.path.join(tmpdir, "clean_customers.csv"), index=False)
            result = data_agent.run(data_dir=tmpdir)
        assert "No CSV" in result or "📂" in result

    @patch("data_agent.send_message")
    @patch("data_agent.send_document")
    @patch("data_agent._push")
    @patch("data_agent._ai_summary", return_value="All good.")
    def test_multiple_files_all_processed(self, mock_ai, mock_push, mock_doc, mock_send):
        with tempfile.TemporaryDirectory() as tmpdir:
            for fname in ["leads.csv", "orders.csv", "customers.csv"]:
                pd.DataFrame([{"col1": "a", "col2": "b"}]).to_csv(
                    os.path.join(tmpdir, fname), index=False)
            result = data_agent.run(data_dir=tmpdir)
        assert "Files processed: *3*" in result


# ── handle() — path parsing ───────────────────────────────────────────────────

class TestHandle:

    @patch("data_agent.run")
    @patch("data_agent.send_message")
    def test_extracts_custom_path(self, mock_send, mock_run):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_agent.handle(f"clean data in {tmpdir}")
            mock_run.assert_called_once_with(data_dir=tmpdir)

    @patch("data_agent.run")
    @patch("data_agent.send_message")
    def test_uses_default_dir_when_no_path(self, mock_send, mock_run):
        data_agent.handle("clean my data")
        mock_run.assert_called_once_with(data_dir=None)

    @patch("data_agent.run")
    @patch("data_agent.send_message")
    def test_ignores_nonexistent_path(self, mock_send, mock_run):
        data_agent.handle("clean data in /nonexistent/path/xyz")
        # Path doesn't exist → falls back to None
        mock_run.assert_called_once_with(data_dir=None)
