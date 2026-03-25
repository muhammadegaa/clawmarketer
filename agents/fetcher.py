"""
Meta Marketing API fetcher.
Retrieves campaign-level insights and returns a DataFrame compatible
with the existing cleaner → analyzer → reporter pipeline.

Requires:
  META_ACCESS_TOKEN  — User or System User access token
  META_AD_ACCOUNT_ID — e.g. act_123456789
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List

API_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

FIELDS = [
    "campaign_name",
    "objective",
    "reach",
    "impressions",
    "clicks",
    "spend",
    "ctr",
    "cpc",
    "cpm",
    "frequency",
    "actions",
    "cost_per_action_type",
    "purchase_roas",
    "date_start",
    "date_stop",
]

CONVERSION_ACTION_TYPES = {
    "purchase",
    "offsite_conversion.fb_pixel_purchase",
    "app_install",
    "lead",
    "complete_registration",
    "offsite_conversion.fb_pixel_lead",
}


def _extract_conversions(actions: Optional[List]) -> float:
    if not actions:
        return 0
    total = 0
    for action in actions:
        if action.get("action_type") in CONVERSION_ACTION_TYPES:
            total += float(action.get("value", 0))
    return total


def _extract_cost_per_conversion(cost_per_action: Optional[List]) -> Optional[float]:
    if not cost_per_action:
        return None
    for action in cost_per_action:
        if action.get("action_type") in CONVERSION_ACTION_TYPES:
            return float(action.get("value", 0))
    return None


def _extract_roas(purchase_roas: Optional[List]) -> Optional[float]:
    if not purchase_roas:
        return None
    for r in purchase_roas:
        if r.get("action_type") == "omni_purchase":
            return float(r.get("value", 0))
    # fallback to first entry
    if purchase_roas:
        return float(purchase_roas[0].get("value", 0))
    return None


def _paginate(url: str, params: dict) -> list:
    results = []
    while url:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        if "error" in data:
            raise RuntimeError(f"Meta API error: {data['error']['message']}")

        results.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
        params = {}  # next URL already has params baked in

    return results


def fetch(
    access_token: str,
    ad_account_id: str,
    date_preset: str = None,
    since: str = None,
    until: str = None,
    level: str = "campaign",
) -> pd.DataFrame:
    """
    Fetch insights from Meta Ads API.

    Args:
        access_token: Meta access token
        ad_account_id: Ad account ID (with or without 'act_' prefix)
        date_preset: One of: today, yesterday, last_7d, last_14d, last_30d,
                     last_month, this_month, last_quarter, last_year
        since: Start date (YYYY-MM-DD). Used if date_preset is None.
        until: End date (YYYY-MM-DD). Used if date_preset is None.
        level: campaign, adset, or ad
    """
    if not ad_account_id.startswith("act_"):
        ad_account_id = f"act_{ad_account_id}"

    url = f"{BASE_URL}/{ad_account_id}/insights"

    params = {
        "access_token": access_token,
        "fields": ",".join(FIELDS),
        "level": level,
        "limit": 500,
    }

    if date_preset:
        params["date_preset"] = date_preset
    elif since and until:
        params["time_range"] = '{' + f'"since":"{since}","until":"{until}"' + '}'
    else:
        # Default: last 30 days
        params["date_preset"] = "last_30d"

    raw_rows = _paginate(url, params)

    if not raw_rows:
        return pd.DataFrame()

    rows = []
    for r in raw_rows:
        rows.append({
            "Campaign name": r.get("campaign_name", ""),
            "Objective": r.get("objective", ""),
            "Reporting starts": r.get("date_start", ""),
            "Reporting ends": r.get("date_stop", ""),
            "Reach": r.get("reach", 0),
            "Impressions": r.get("impressions", 0),
            "Clicks (all)": r.get("clicks", 0),
            "CTR (all)": r.get("ctr", 0),
            "CPC (all)": r.get("cpc", 0),
            "CPM (cost per 1,000 impressions)": r.get("cpm", 0),
            "Amount spent (USD)": r.get("spend", 0),
            "Results": _extract_conversions(r.get("actions")),
            "Cost per result": _extract_cost_per_conversion(r.get("cost_per_action_type")),
            "Purchase ROAS (return on ad spend)": _extract_roas(r.get("purchase_roas")),
            "Frequency": r.get("frequency", 0),
        })

    return pd.DataFrame(rows)


def fetch_to_csv(output_path: str, **kwargs) -> str:
    df = fetch(**kwargs)
    df.to_csv(output_path, index=False)
    return output_path
