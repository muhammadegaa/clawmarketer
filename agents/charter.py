"""
Chart generator — produces PNG charts from campaign analysis data.
Uses matplotlib (already installed).
"""

import os
import tempfile
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # non-interactive backend for servers
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


DARK_BG    = "#0f0f0f"
CARD_BG    = "#1a1a1a"
INDIGO     = "#6366f1"
GREEN      = "#4ade80"
YELLOW     = "#fbbf24"
RED        = "#f87171"
TEXT       = "#e2e8f0"
SUBTEXT    = "#9ca3af"
COLORS     = [INDIGO, GREEN, YELLOW, RED, "#38bdf8", "#f472b6", "#a3e635"]


def _setup_fig(title: str, figsize=(10, 5)):
    fig, ax = plt.subplots(figsize=figsize)
    fig.patch.set_facecolor(DARK_BG)
    ax.set_facecolor(CARD_BG)
    ax.set_title(title, color=TEXT, fontsize=13, fontweight="bold", pad=14)
    ax.tick_params(colors=SUBTEXT, labelsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    for spine in ["bottom", "left"]:
        ax.spines[spine].set_color("#2a2a2a")
    ax.yaxis.label.set_color(SUBTEXT)
    ax.xaxis.label.set_color(SUBTEXT)
    return fig, ax


def _short(name: str, max_len: int = 18) -> str:
    return name[:max_len] + "…" if len(name) > max_len else name


def spend_chart(summary: pd.DataFrame, out_dir: str) -> str:
    if "spend" not in summary.columns or summary.empty:
        return None

    df = summary.nlargest(8, "spend")[["campaign_name", "spend"]].copy()
    df["label"] = df["campaign_name"].apply(_short)

    fig, ax = _setup_fig("Spend by Campaign (USD)")
    bars = ax.barh(df["label"], df["spend"], color=INDIGO, height=0.6)
    ax.bar_label(bars, labels=[f"${v:,.0f}" for v in df["spend"]], color=SUBTEXT, fontsize=8, padding=4)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.invert_yaxis()
    ax.tick_params(axis="y", colors=TEXT)
    fig.tight_layout()

    path = os.path.join(out_dir, "chart_spend.png")
    fig.savefig(path, dpi=130, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    return path


def ctr_chart(summary: pd.DataFrame, out_dir: str) -> str:
    ctr_col = "ctr_calc" if "ctr_calc" in summary.columns else ("ctr" if "ctr" in summary.columns else None)
    if not ctr_col or summary.empty:
        return None

    df = summary.copy()
    df["label"] = df["campaign_name"].apply(_short)
    df = df.sort_values(ctr_col, ascending=True).tail(8)

    fig, ax = _setup_fig("CTR by Campaign (%)")
    bar_colors = [GREEN if v >= 2.0 else YELLOW if v >= 1.0 else RED for v in df[ctr_col]]
    bars = ax.barh(df["label"], df[ctr_col], color=bar_colors, height=0.6)
    ax.bar_label(bars, labels=[f"{v:.2f}%" for v in df[ctr_col]], color=SUBTEXT, fontsize=8, padding=4)
    ax.axvline(x=2.0, color=SUBTEXT, linestyle="--", linewidth=0.8, alpha=0.5)
    ax.text(2.05, -0.5, "avg 2%", color=SUBTEXT, fontsize=7)
    ax.tick_params(axis="y", colors=TEXT)
    fig.tight_layout()

    path = os.path.join(out_dir, "chart_ctr.png")
    fig.savefig(path, dpi=130, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    return path


def roas_chart(summary: pd.DataFrame, out_dir: str) -> str:
    if "roas" not in summary.columns or summary.empty:
        return None

    df = summary[summary["roas"].notna() & (summary["roas"] > 0)].copy()
    if df.empty:
        return None

    df["label"] = df["campaign_name"].apply(_short)
    df = df.sort_values("roas", ascending=True)

    fig, ax = _setup_fig("ROAS by Campaign")
    bar_colors = [GREEN if v >= 2.0 else YELLOW if v >= 1.0 else RED for v in df["roas"]]
    bars = ax.barh(df["label"], df["roas"], color=bar_colors, height=0.6)
    ax.bar_label(bars, labels=[f"{v:.2f}x" for v in df["roas"]], color=SUBTEXT, fontsize=8, padding=4)
    ax.axvline(x=1.0, color=RED, linestyle="--", linewidth=0.8, alpha=0.5)
    ax.text(1.05, -0.5, "break even", color=RED, fontsize=7)
    ax.tick_params(axis="y", colors=TEXT)
    fig.tight_layout()

    path = os.path.join(out_dir, "chart_roas.png")
    fig.savefig(path, dpi=130, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    return path


def generate_all(analysis: dict) -> tuple[list, str]:
    """
    Generate all charts and a clean CSV export.
    Returns: (list of chart PNG paths, clean CSV path)
    """
    summary = analysis.get("campaign_summary")
    out_dir = tempfile.mkdtemp()
    charts = []

    if summary is not None and not summary.empty:
        for fn in [spend_chart, ctr_chart, roas_chart]:
            path = fn(summary, out_dir)
            if path:
                charts.append(path)

        # Export clean CSV
        csv_path = os.path.join(out_dir, "meta_ads_clean.csv")
        summary.to_csv(csv_path, index=False)
    else:
        csv_path = None

    return charts, csv_path
