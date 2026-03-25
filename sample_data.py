"""Generates a realistic Meta Ads CSV export for demo purposes."""
import csv
import random
from pathlib import Path

CAMPAIGNS = [
    ("Retargeting - Website Visitors", "CONVERSIONS"),
    ("Prospecting - Lookalike 1%", "CONVERSIONS"),
    ("Brand Awareness - Video", "BRAND_AWARENESS"),
    ("Catalog Sales - All Products", "CATALOG_SALES"),
    ("Lead Gen - Free Trial", "LEAD_GENERATION"),
]

random.seed(42)


def make_row(campaign_name, objective, date_start, date_end):
    impressions = random.randint(5000, 150000)
    clicks = int(impressions * random.uniform(0.005, 0.04))
    spend = round(random.uniform(50, 800), 2)
    conversions = int(clicks * random.uniform(0.01, 0.12))
    roas = round(random.uniform(0.6, 5.5), 2) if objective == "CONVERSIONS" else ""
    ctr = round(clicks / impressions * 100, 2)
    cpc = round(spend / clicks, 2) if clicks > 0 else 0
    cpm = round(spend / impressions * 1000, 2)
    reach = int(impressions * random.uniform(0.6, 0.9))
    frequency = round(impressions / reach, 2) if reach > 0 else 1.0

    return {
        "Campaign name": campaign_name,
        "Objective": objective,
        "Reporting starts": date_start,
        "Reporting ends": date_end,
        "Reach": reach,
        "Impressions": impressions,
        "Clicks (all)": clicks,
        "CTR (all)": ctr,
        "CPC (all)": cpc,
        "CPM (cost per 1,000 impressions)": cpm,
        "Amount spent (USD)": spend,
        "Results": conversions if conversions > 0 else "",
        "Cost per result": round(spend / conversions, 2) if conversions > 0 else "",
        "Purchase ROAS (return on ad spend)": roas,
        "Frequency": frequency,
    }


def generate(output_path: str = "sample_meta_ads.csv"):
    rows = []
    for name, obj in CAMPAIGNS:
        # Multiple date ranges to simulate weekly data
        for week_start, week_end in [
            ("2025-03-01", "2025-03-07"),
            ("2025-03-08", "2025-03-14"),
            ("2025-03-15", "2025-03-21"),
        ]:
            rows.append(make_row(name, obj, week_start, week_end))

    # Add a junk summary row that cleaner should drop
    rows.append({k: "" for k in rows[0].keys()} | {"Campaign name": "Total"})

    path = Path(output_path)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Sample data saved to: {path.resolve()}")
    return str(path.resolve())


if __name__ == "__main__":
    generate()
