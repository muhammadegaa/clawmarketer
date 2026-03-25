import pandas as pd
import re


COLUMN_ALIASES = {
    "campaign name": "campaign_name",
    "ad set name": "adset_name",
    "adset name": "adset_name",
    "ad name": "ad_name",
    "amount spent": "spend",
    "amount spent (usd)": "spend",
    "spend": "spend",
    "impressions": "impressions",
    "reach": "reach",
    "clicks (all)": "clicks",
    "link clicks": "clicks",
    "clicks": "clicks",
    "ctr (all)": "ctr",
    "ctr (link click-through rate)": "ctr",
    "ctr": "ctr",
    "cpc (all)": "cpc",
    "cpc (cost per link click)": "cpc",
    "cpc": "cpc",
    "cpm (cost per 1,000 impressions)": "cpm",
    "cpm": "cpm",
    "results": "conversions",
    "conversions": "conversions",
    "cost per result": "cost_per_conversion",
    "cost per conversion": "cost_per_conversion",
    "purchase roas (return on ad spend)": "roas",
    "roas": "roas",
    "website purchase roas": "roas",
    "reporting starts": "date_start",
    "reporting ends": "date_end",
    "date start": "date_start",
    "date stop": "date_end",
    "objective": "objective",
    "delivery": "delivery",
    "frequency": "frequency",
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns={c: COLUMN_ALIASES[c] for c in df.columns if c in COLUMN_ALIASES})
    return df


def _drop_junk_rows(df: pd.DataFrame) -> pd.DataFrame:
    # Meta sometimes appends summary rows at the bottom
    if "campaign_name" in df.columns:
        df = df[df["campaign_name"].notna()]
        df = df[~df["campaign_name"].str.lower().str.contains("total|report|summary", na=False)]
    return df.reset_index(drop=True)


def _clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = ["spend", "impressions", "reach", "clicks", "ctr", "cpc", "cpm",
                    "conversions", "cost_per_conversion", "roas", "frequency"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[,$%\s]", "", regex=True)
                .replace({"nan": None, "": None, "-": None, "N/A": None})
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["date_start", "date_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def clean(path: str) -> tuple[pd.DataFrame, dict]:
    raw = pd.read_csv(path)
    original_rows = len(raw)
    original_cols = list(raw.columns)

    df = _normalize_columns(raw)
    df = _drop_junk_rows(df)
    df = _clean_numeric(df)
    df = _clean_dates(df)

    # Drop fully empty rows
    df = df.dropna(how="all")

    stats = {
        "original_rows": original_rows,
        "clean_rows": len(df),
        "dropped_rows": original_rows - len(df),
        "original_columns": original_cols,
        "mapped_columns": list(df.columns),
        "missing_values": df.isnull().sum().to_dict(),
    }

    return df, stats
