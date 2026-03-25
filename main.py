import os
import typer
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from agents import cleaner, analyzer, reporter, fetcher

app = typer.Typer()
console = Console()


def print_overall(metrics: dict):
    table = Table(title="Overall Performance", box=box.ROUNDED, style="cyan")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")

    labels = {
        "total_spend": ("Total Spend", "${:,.2f}"),
        "total_impressions": ("Impressions", "{:,}"),
        "total_clicks": ("Clicks", "{:,}"),
        "total_conversions": ("Conversions", "{:,}"),
        "overall_ctr": ("CTR", "{:.2f}%"),
        "overall_cpc": ("CPC", "${:.2f}"),
        "overall_cpm": ("CPM", "${:.2f}"),
        "overall_cost_per_conversion": ("Cost/Conversion", "${:.2f}"),
        "avg_roas": ("Avg ROAS", "{:.2f}x"),
        "num_campaigns": ("Campaigns", "{}"),
    }

    for key, (label, fmt) in labels.items():
        if key in metrics:
            table.add_row(label, fmt.format(metrics[key]))

    if "date_range_start" in metrics:
        table.add_row("Date Range", f"{metrics['date_range_start']} → {metrics['date_range_end']}")

    console.print(table)


def print_campaign_summary(summary):
    if summary.empty:
        return

    table = Table(title="Campaign Breakdown", box=box.ROUNDED, style="green")
    table.add_column("Campaign", style="bold", max_width=35)
    table.add_column("Spend", justify="right")
    table.add_column("Impressions", justify="right")
    table.add_column("Clicks", justify="right")
    table.add_column("CTR", justify="right")
    table.add_column("CPC", justify="right")
    table.add_column("ROAS", justify="right")

    ctr_col = "ctr_calc" if "ctr_calc" in summary.columns else ("ctr" if "ctr" in summary.columns else None)
    cpc_col = "cpc_calc" if "cpc_calc" in summary.columns else ("cpc" if "cpc" in summary.columns else None)

    for _, row in summary.iterrows():
        table.add_row(
            str(row.get("campaign_name", ""))[:35],
            f"${row['spend']:,.2f}" if "spend" in row and row["spend"] == row["spend"] else "-",
            f"{int(row['impressions']):,}" if "impressions" in row and row["impressions"] == row["impressions"] else "-",
            f"{int(row['clicks']):,}" if "clicks" in row and row["clicks"] == row["clicks"] else "-",
            f"{row[ctr_col]:.2f}%" if ctr_col and row[ctr_col] == row[ctr_col] else "-",
            f"${row[cpc_col]:.2f}" if cpc_col and row[cpc_col] == row[cpc_col] else "-",
            f"{row['roas']:.2f}x" if "roas" in row and row["roas"] == row["roas"] else "-",
        )

    console.print(table)


def print_anomalies(anomaly_list: list):
    if not anomaly_list:
        console.print(Panel("[green]No anomalies detected.[/green]", title="Anomalies"))
        return

    content = "\n".join(f"[yellow]⚠[/yellow]  {a}" for a in anomaly_list)
    console.print(Panel(content, title="[bold red]Anomalies Detected[/bold red]", border_style="red"))


@app.command()
def run(
    csv: Path = typer.Argument(..., help="Path to Meta Ads CSV export"),
    api_key: str = typer.Option(None, "--api-key", "-k", envvar="GROQ_API_KEY", help="Groq API key"),
    output: Path = typer.Option(None, "--output", "-o", help="Save report to markdown file"),
    no_ai: bool = typer.Option(False, "--no-ai", help="Skip AI report, show data only"),
):
    """Meta Ads CSV Agent — cleans, analyzes, and generates insights from your Meta Ads export."""

    console.rule("[bold blue]Meta Ads Agent[/bold blue]")

    # Stage 1: Clean
    console.print("\n[bold]Stage 1 / 3:[/bold] Cleaning data...")
    df, clean_stats = cleaner.clean(str(csv))
    console.print(
        f"  [green]✓[/green] {clean_stats['original_rows']} rows in → "
        f"{clean_stats['clean_rows']} clean rows "
        f"([yellow]{clean_stats['dropped_rows']} dropped[/yellow])"
    )

    # Stage 2: Analyze
    console.print("\n[bold]Stage 2 / 3:[/bold] Analyzing...")
    results = analyzer.run(df)
    console.print("  [green]✓[/green] Analysis complete\n")

    print_overall(results["overall"])
    console.print()
    print_campaign_summary(results["campaign_summary"])
    console.print()
    print_anomalies(results["anomalies"])

    # Stage 3: AI Report
    if no_ai:
        console.print("\n[dim]AI report skipped (--no-ai)[/dim]")
        return

    if not api_key:
        console.print("\n[yellow]No Groq API key found. Pass --api-key or set GROQ_API_KEY.[/yellow]")
        console.print("[dim]Run with --no-ai to skip the report.[/dim]")
        raise typer.Exit(1)

    console.print("\n[bold]Stage 3 / 3:[/bold] Generating AI report...")
    report_text = reporter.generate(results, api_key)
    console.print("\n")
    console.print(Panel(report_text, title="[bold magenta]AI Analysis Report[/bold magenta]", border_style="magenta"))

    # Save
    save_path = output or Path(f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md")
    reporter.save_report(report_text, str(save_path))
    console.print(f"\n[green]✓[/green] Report saved to: [bold]{save_path}[/bold]")


@app.command()
def fetch(
    access_token: str = typer.Option(None, "--token", "-t", envvar="META_ACCESS_TOKEN", help="Meta access token"),
    account_id: str = typer.Option(None, "--account", "-a", envvar="META_AD_ACCOUNT_ID", help="Ad account ID"),
    date_preset: str = typer.Option("last_30d", "--preset", "-p", help="last_7d | last_14d | last_30d | last_month | this_month | last_quarter | last_year"),
    since: str = typer.Option(None, "--since", help="Start date YYYY-MM-DD (overrides --preset)"),
    until: str = typer.Option(None, "--until", help="End date YYYY-MM-DD (overrides --preset)"),
    level: str = typer.Option("campaign", "--level", "-l", help="campaign | adset | ad"),
    groq_key: str = typer.Option(None, "--api-key", "-k", envvar="GROQ_API_KEY", help="Groq API key"),
    output: Path = typer.Option(None, "--output", "-o", help="Save report to markdown file"),
    no_ai: bool = typer.Option(False, "--no-ai", help="Skip AI report, show data only"),
    save_csv: Path = typer.Option(None, "--save-csv", help="Save fetched data as CSV"),
):
    """Fetch live data from Meta Ads API, then clean, analyze, and report."""

    console.rule("[bold blue]Meta Ads Agent — Live Fetch[/bold blue]")

    if not access_token:
        console.print("[red]Missing META_ACCESS_TOKEN. Set it in .env or pass --token.[/red]")
        raise typer.Exit(1)
    if not account_id:
        console.print("[red]Missing META_AD_ACCOUNT_ID. Set it in .env or pass --account.[/red]")
        raise typer.Exit(1)

    # Stage 0: Fetch
    preset = None if (since and until) else date_preset
    date_label = f"{since} → {until}" if (since and until) else date_preset

    console.print(f"\n[bold]Stage 1 / 4:[/bold] Fetching {level}-level data ({date_label})...")
    try:
        df_raw = fetcher.fetch(
            access_token=access_token,
            ad_account_id=account_id,
            date_preset=preset,
            since=since,
            until=until,
            level=level,
        )
    except Exception as e:
        console.print(f"[red]Fetch failed: {e}[/red]")
        raise typer.Exit(1)

    console.print(f"  [green]✓[/green] {len(df_raw)} rows fetched from Meta API")

    if save_csv:
        df_raw.to_csv(save_csv, index=False)
        console.print(f"  [green]✓[/green] Raw data saved to: {save_csv}")

    # Stage 1: Clean (cleaner expects CSV path, so we pass through the DataFrame directly)
    console.print("\n[bold]Stage 2 / 4:[/bold] Cleaning data...")
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
        df_raw.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    df, clean_stats = cleaner.clean(tmp_path)
    os.unlink(tmp_path)

    console.print(
        f"  [green]✓[/green] {clean_stats['original_rows']} rows → "
        f"{clean_stats['clean_rows']} clean rows "
        f"([yellow]{clean_stats['dropped_rows']} dropped[/yellow])"
    )

    # Stage 2: Analyze
    console.print("\n[bold]Stage 3 / 4:[/bold] Analyzing...")
    results = analyzer.run(df)
    console.print("  [green]✓[/green] Analysis complete\n")

    print_overall(results["overall"])
    console.print()
    print_campaign_summary(results["campaign_summary"])
    console.print()
    print_anomalies(results["anomalies"])

    # Stage 3: AI Report
    if no_ai:
        console.print("\n[dim]AI report skipped (--no-ai)[/dim]")
        return

    if not groq_key:
        console.print("\n[yellow]No Groq API key. Pass --api-key or set GROQ_API_KEY.[/yellow]")
        raise typer.Exit(1)

    console.print("\n[bold]Stage 4 / 4:[/bold] Generating AI report...")
    report_text = reporter.generate(results, groq_key)
    console.print("\n")
    console.print(Panel(report_text, title="[bold magenta]AI Analysis Report[/bold magenta]", border_style="magenta"))

    save_path = output or Path(f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md")
    reporter.save_report(report_text, str(save_path))
    console.print(f"\n[green]✓[/green] Report saved to: [bold]{save_path}[/bold]")


@app.command()
def demo(
    api_key: str = typer.Option(None, "--api-key", "-k", envvar="GROQ_API_KEY"),
    no_ai: bool = typer.Option(False, "--no-ai"),
):
    """Generate sample Meta Ads data and run the full pipeline."""
    from sample_data import generate
    csv_path = generate()
    console.print(f"[green]✓[/green] Sample data generated: {csv_path}\n")

    run(csv=Path(csv_path), api_key=api_key, output=None, no_ai=no_ai)


if __name__ == "__main__":
    app()
