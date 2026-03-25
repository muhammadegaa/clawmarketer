import pandas as pd


def _safe(df: pd.DataFrame, col: str):
    return df[col] if col in df.columns else None


def campaign_summary(df: pd.DataFrame) -> pd.DataFrame:
    group_col = "campaign_name" if "campaign_name" in df.columns else None
    if not group_col:
        return pd.DataFrame()

    agg = {}
    for col in ["spend", "impressions", "clicks", "conversions", "reach"]:
        if col in df.columns:
            agg[col] = "sum"
    for col in ["roas", "ctr", "cpc", "cpm", "frequency"]:
        if col in df.columns:
            agg[col] = "mean"

    summary = df.groupby(group_col).agg(agg).reset_index()

    # Recompute derived metrics from aggregated values for accuracy
    if "clicks" in summary and "impressions" in summary:
        summary["ctr_calc"] = (summary["clicks"] / summary["impressions"] * 100).round(2)
    if "spend" in summary and "clicks" in summary:
        summary["cpc_calc"] = (summary["spend"] / summary["clicks"]).round(2)
    if "spend" in summary and "impressions" in summary:
        summary["cpm_calc"] = (summary["spend"] / summary["impressions"] * 1000).round(2)
    if "spend" in summary and "conversions" in summary:
        summary["cost_per_conv_calc"] = (summary["spend"] / summary["conversions"]).round(2)

    return summary.sort_values("spend", ascending=False)


def top_bottom(df: pd.DataFrame, n: int = 3) -> dict:
    summary = campaign_summary(df)
    if summary.empty:
        return {}

    result = {}

    if "spend" in summary.columns:
        result["top_spend"] = summary.nlargest(n, "spend")[["campaign_name", "spend"]].to_dict("records")

    ctr_col = "ctr_calc" if "ctr_calc" in summary.columns else ("ctr" if "ctr" in summary.columns else None)
    if ctr_col:
        result["top_ctr"] = summary.nlargest(n, ctr_col)[["campaign_name", ctr_col]].to_dict("records")
        result["bottom_ctr"] = summary.nsmallest(n, ctr_col)[["campaign_name", ctr_col]].to_dict("records")

    if "roas" in summary.columns:
        valid = summary[summary["roas"].notna() & (summary["roas"] > 0)]
        if not valid.empty:
            result["top_roas"] = valid.nlargest(n, "roas")[["campaign_name", "roas"]].to_dict("records")
            result["bottom_roas"] = valid.nsmallest(n, "roas")[["campaign_name", "roas"]].to_dict("records")

    cpc_col = "cpc_calc" if "cpc_calc" in summary.columns else ("cpc" if "cpc" in summary.columns else None)
    if cpc_col:
        valid = summary[summary[cpc_col].notna() & (summary[cpc_col] > 0)]
        if not valid.empty:
            result["best_cpc"] = valid.nsmallest(n, cpc_col)[["campaign_name", cpc_col]].to_dict("records")
            result["worst_cpc"] = valid.nlargest(n, cpc_col)[["campaign_name", cpc_col]].to_dict("records")

    return result


def overall_metrics(df: pd.DataFrame) -> dict:
    metrics = {}

    if "spend" in df.columns:
        metrics["total_spend"] = round(df["spend"].sum(), 2)
    if "impressions" in df.columns:
        metrics["total_impressions"] = int(df["impressions"].sum())
    if "clicks" in df.columns:
        metrics["total_clicks"] = int(df["clicks"].sum())
    if "conversions" in df.columns:
        metrics["total_conversions"] = int(df["conversions"].sum())
    if "reach" in df.columns:
        metrics["total_reach"] = int(df["reach"].sum())

    # Derived
    if "total_clicks" in metrics and "total_impressions" in metrics and metrics["total_impressions"] > 0:
        metrics["overall_ctr"] = round(metrics["total_clicks"] / metrics["total_impressions"] * 100, 2)
    if "total_spend" in metrics and "total_clicks" in metrics and metrics["total_clicks"] > 0:
        metrics["overall_cpc"] = round(metrics["total_spend"] / metrics["total_clicks"], 2)
    if "total_spend" in metrics and "total_impressions" in metrics and metrics["total_impressions"] > 0:
        metrics["overall_cpm"] = round(metrics["total_spend"] / metrics["total_impressions"] * 1000, 2)
    if "total_spend" in metrics and "total_conversions" in metrics and metrics["total_conversions"] > 0:
        metrics["overall_cost_per_conversion"] = round(metrics["total_spend"] / metrics["total_conversions"], 2)

    if "roas" in df.columns:
        valid_roas = df["roas"].dropna()
        if not valid_roas.empty:
            metrics["avg_roas"] = round(valid_roas.mean(), 2)

    if "date_start" in df.columns:
        metrics["date_range_start"] = str(df["date_start"].min().date())
        metrics["date_range_end"] = str(df["date_start"].max().date()) if "date_end" not in df.columns else str(df["date_end"].max().date())

    if "campaign_name" in df.columns:
        metrics["num_campaigns"] = df["campaign_name"].nunique()

    return metrics


def anomalies(df: pd.DataFrame) -> list[str]:
    flags = []

    summary = campaign_summary(df)
    if summary.empty:
        return flags

    ctr_col = "ctr_calc" if "ctr_calc" in summary.columns else ("ctr" if "ctr" in summary.columns else None)
    if ctr_col:
        low_ctr = summary[summary[ctr_col] < 0.5]
        for _, row in low_ctr.iterrows():
            flags.append(f"Low CTR ({row[ctr_col]}%) on campaign: {row['campaign_name']}")

    if "spend" in summary.columns:
        mean_spend = summary["spend"].mean()
        std_spend = summary["spend"].std()
        if std_spend > 0:
            spikes = summary[summary["spend"] > mean_spend + 2 * std_spend]
            for _, row in spikes.iterrows():
                flags.append(f"Spend spike (${row['spend']:.2f}) on campaign: {row['campaign_name']}")

    if "roas" in summary.columns:
        poor_roas = summary[(summary["roas"].notna()) & (summary["roas"] < 1.0)]
        for _, row in poor_roas.iterrows():
            flags.append(f"ROAS below 1.0 ({row['roas']:.2f}x) — losing money on: {row['campaign_name']}")

    return flags


def run(df: pd.DataFrame) -> dict:
    return {
        "overall": overall_metrics(df),
        "campaign_summary": campaign_summary(df),
        "top_bottom": top_bottom(df),
        "anomalies": anomalies(df),
    }
