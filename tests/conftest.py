"""
Shared fixtures and mock data for ClawMarketer tests.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


# ── Realistic campaign fixture ────────────────────────────────────────────────

SAMPLE_CAMPAIGNS_DF = pd.DataFrame([
    {
        "Campaign name": "Black Friday — Retargeting",
        "Objective": "CONVERSIONS",
        "Reporting starts": "2024-11-01",
        "Reporting ends": "2024-11-30",
        "Reach": 48000,
        "Impressions": 62000,
        "Clicks (all)": 2480,
        "CTR (all)": 4.0,
        "CPC (all)": 0.81,
        "CPM (cost per 1,000 impressions)": 32.4,
        "Amount spent (USD)": 2010.0,
        "Results": 198,
        "Cost per result": 10.15,
        "Purchase ROAS (return on ad spend)": 3.4,
        "Frequency": 1.3,
    },
    {
        "Campaign name": "Q4 Brand Awareness",
        "Objective": "BRAND_AWARENESS",
        "Reporting starts": "2024-11-01",
        "Reporting ends": "2024-11-30",
        "Reach": 210000,
        "Impressions": 380000,
        "Clicks (all)": 1520,
        "CTR (all)": 0.4,
        "CPC (all)": 2.63,
        "CPM (cost per 1,000 impressions)": 10.5,
        "Amount spent (USD)": 4000.0,
        "Results": 0,
        "Cost per result": None,
        "Purchase ROAS (return on ad spend)": None,
        "Frequency": 1.8,
    },
    {
        "Campaign name": "Competitor Audience — Nov Test",
        "Objective": "CONVERSIONS",
        "Reporting starts": "2024-11-01",
        "Reporting ends": "2024-11-30",
        "Reach": 8500,
        "Impressions": 14200,
        "Clicks (all)": 28,
        "CTR (all)": 0.2,
        "CPC (all)": 14.29,
        "CPM (cost per 1,000 impressions)": 28.2,
        "Amount spent (USD)": 400.0,
        "Results": 3,
        "Cost per result": 133.33,
        "Purchase ROAS (return on ad spend)": 0.8,
        "Frequency": 1.7,
    },
    {
        "Campaign name": "Upsell — Past Buyers Q4",
        "Objective": "CONVERSIONS",
        "Reporting starts": "2024-11-01",
        "Reporting ends": "2024-11-30",
        "Reach": 12000,
        "Impressions": 18000,
        "Clicks (all)": 720,
        "CTR (all)": 4.0,
        "CPC (all)": 0.56,
        "CPM (cost per 1,000 impressions)": 22.2,
        "Amount spent (USD)": 400.0,
        "Results": 88,
        "Cost per result": 4.55,
        "Purchase ROAS (return on ad spend)": 4.3,
        "Frequency": 1.5,
    },
])

DIRTY_CAMPAIGNS_DF = pd.DataFrame([
    {
        "Campaign name": "  Summer Sale  ",
        "Amount spent (USD)": "$1,200.50",
        "Impressions": "45,000",
        "Clicks (all)": "900",
        "CTR (all)": "2.00%",
        "Purchase ROAS (return on ad spend)": "3.20",
        "Reporting starts": "2024-11-01",
    },
    {
        "Campaign name": "Bad Campaign",
        "Amount spent (USD)": "$200.00",
        "Impressions": "10,000",
        "Clicks (all)": "20",
        "CTR (all)": "0.20%",
        "Purchase ROAS (return on ad spend)": "0.50",
        "Reporting starts": "2024-11-01",
    },
    {
        "Campaign name": "Total",  # should be dropped
        "Amount spent (USD)": "$1,400.50",
        "Impressions": "55,000",
        "Clicks (all)": "920",
        "CTR (all)": "",
        "Purchase ROAS (return on ad spend)": "",
        "Reporting starts": "",
    },
    {
        "Campaign name": None,  # should be dropped
        "Amount spent (USD)": None,
        "Impressions": None,
        "Clicks (all)": None,
        "CTR (all)": None,
        "Purchase ROAS (return on ad spend)": None,
        "Reporting starts": None,
    },
])


# ── Mock Firestore helpers ────────────────────────────────────────────────────

MOCK_PROFILE = {"api_token": "sk_cm_testtoken123", "telegram_chat_id": "987654321"}
MOCK_META_INTEGRATION = {"access_token": "EAAtest", "account_id": "act_123"}


@pytest.fixture
def mock_firestore_read():
    """Mock _read_from_firestore to return predictable data."""
    def _side_effect(uid, collection, doc_id):
        if collection == "meta" and doc_id == "profile":
            return MOCK_PROFILE
        if collection == "integrations" and doc_id == "meta":
            return MOCK_META_INTEGRATION
        return {}
    with patch("server._read_from_firestore", side_effect=_side_effect) as m:
        yield m


@pytest.fixture
def mock_firestore_write():
    with patch("server._write_to_firestore") as m:
        yield m


@pytest.fixture
def valid_bearer_headers():
    return {"Authorization": "Bearer sk_cm_testtoken123"}


@pytest.fixture
def invalid_bearer_headers():
    return {"Authorization": "Bearer sk_cm_wrongtoken"}
