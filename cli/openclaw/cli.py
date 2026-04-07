"""OpenClaw CLI — set up and control your AI marketing agents."""
import contextlib
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

import click

try:
    from rich.console import Console
    from rich.padding import Padding
    _console = Console()
    HAS_RICH = True
except ImportError:
    _console = None
    HAS_RICH = False

try:
    import requests as _req
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

CONFIG_DIR  = Path.home() / ".openclaw"
CONFIG_FILE = CONFIG_DIR / "config.json"
LOG_DIR     = CONFIG_DIR / "logs"
DEFAULT_SERVER = "https://clawmarketer.vercel.app"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _p(msg: str, **kw):
    if HAS_RICH:
        _console.print(msg, **kw)
    else:
        click.echo(msg)


def _load_config():
    if not CONFIG_FILE.exists():
        return None
    with open(CONFIG_FILE) as f:
        return json.load(f)


def _save_config(data: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=2)


@contextlib.contextmanager
def _spin(msg: str):
    if HAS_RICH:
        with _console.status(msg, spinner="dots"):
            yield
    else:
        click.echo(msg)
        yield


# ── CLI group ─────────────────────────────────────────────────────────────────

@click.group()
def cli():
    """OpenClaw — AI agents for your Meta Ads."""
    pass


# ── init ──────────────────────────────────────────────────────────────────────

@cli.command()
@click.argument("token")
@click.option("--server",   default=DEFAULT_SERVER, show_default=True,
              help="ClawMarketer server URL")
@click.option("--time",     "schedule_time", default="08:00", show_default=True,
              help="Daily run time HH:MM")
def init(token: str, server: str, schedule_time: str):
    """Set up OpenClaw with your API token.

    \b
    Get your one-line command from the ClawMarketer dashboard → Integrations → Install.
    It will look like:
        openclaw init oc_abc123_...
    """
    if not HAS_REQUESTS:
        _p("[red]Missing dependency:[/red] pip install requests")
        sys.exit(1)

    server = server.rstrip("/")

    _p("")
    _p("  [bold]OpenClaw Setup[/bold]")
    _p("  " + "─" * 44)
    _p("")

    # ── Fetch config from server ──────────────────────────────────────────────
    with _spin("  Connecting to ClawMarketer..."):
        try:
            resp = _req.get(
                f"{server}/api/cli/setup",
                headers={"Authorization": f"Bearer {token}"},
                timeout=15,
            )
        except _req.exceptions.ConnectionError:
            _p(f"  [red]✗[/red]  Could not reach {server}")
            _p("  Is your internet connected?")
            sys.exit(1)
        except Exception as exc:
            _p(f"  [red]✗[/red]  {exc}")
            sys.exit(1)

    if resp.status_code == 401:
        _p("  [red]✗[/red]  Invalid token.")
        _p("  Get yours from the dashboard → Integrations → Install.")
        sys.exit(1)
    if resp.status_code == 400:
        _p("  [yellow]![/yellow]  Your token is outdated. Click [bold]Rotate[/bold] in the dashboard and copy the new command.")
        sys.exit(1)
    if not resp.ok:
        _p(f"  [red]✗[/red]  Server error ({resp.status_code}). Try again in a moment.")
        sys.exit(1)

    data = resp.json()
    uid  = data.get("uid", "")

    _p("  [green]✓[/green]  Connected")
    _p("")

    # ── Show what's connected ─────────────────────────────────────────────────
    if data.get("meta_connected"):
        n = data.get("meta_campaigns", 0)
        label = f"{n} campaign{'s' if n != 1 else ''}"
        _p(f"  [green]✓[/green]  Meta Ads    [dim]{label}[/dim]")
    else:
        _p("  [yellow]○[/yellow]  Meta Ads    [dim]not connected yet — dashboard → Integrations[/dim]")

    if data.get("telegram_connected"):
        handle = data.get("telegram_handle", "")
        _p(f"  [green]✓[/green]  Telegram    [dim]{handle}[/dim]")
    else:
        _p("  [yellow]○[/yellow]  Telegram    [dim]not connected yet — dashboard → Telegram[/dim]")

    _p("")

    # ── Save config ───────────────────────────────────────────────────────────
    _save_config({
        "token":    token,
        "uid":      uid,
        "server":   server,
        "schedule": schedule_time,
    })

    # ── Set up scheduler ──────────────────────────────────────────────────────
    _setup_scheduler(schedule_time)

    # ── Done ──────────────────────────────────────────────────────────────────
    _p("  " + "─" * 44)
    _p(f"  [green bold]All set.[/green bold] First report arrives tomorrow at {schedule_time}.")
    _p(f"  Dashboard → {server}")
    _p("")


# ── run ───────────────────────────────────────────────────────────────────────

@cli.command()
def run():
    """Run agents now (also called by the scheduler)."""
    cfg = _load_config()
    if not cfg:
        click.echo("Not configured. Run: openclaw init YOUR_TOKEN")
        sys.exit(1)

    if not HAS_REQUESTS:
        click.echo("Missing dependency: pip install requests")
        sys.exit(1)

    token  = cfg["token"]
    uid    = cfg["uid"]
    server = cfg.get("server", DEFAULT_SERVER)

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    try:
        resp = _req.post(
            f"{server}/api/run",
            params={"uid": uid},
            headers={"Authorization": f"Bearer {token}"},
            timeout=120,
        )
        resp.raise_for_status()
        result = resp.json()
        click.echo(f"[openclaw] Done. {result.get('num_campaigns', '')} campaigns processed.")
    except Exception as exc:
        click.echo(f"[openclaw] Run failed: {exc}", err=True)
        sys.exit(1)


# ── status ────────────────────────────────────────────────────────────────────

@cli.command()
def status():
    """Show current config and scheduler status."""
    cfg = _load_config()
    if not cfg:
        _p("\n  Not configured. Run [bold]openclaw init YOUR_TOKEN[/bold]\n")
        return

    _p("")
    _p("  [bold]OpenClaw Status[/bold]")
    _p("  " + "─" * 44)
    _p(f"  Server      {cfg.get('server', DEFAULT_SERVER)}")
    _p(f"  Schedule    {cfg.get('schedule', '08:00')} daily")
    tok = cfg.get("token", "")
    _p(f"  Token       {tok[:14]}…")

    sys_name = platform.system()
    if sys_name == "Darwin":
        res = subprocess.run(
            ["launchctl", "list", "com.openclaw.agent"],
            capture_output=True, text=True,
        )
        running = res.returncode == 0
        label   = "[green]running[/green]" if running else "[dim]not loaded[/dim]"
        _p(f"  Scheduler   {label}")
    elif sys_name == "Linux":
        res = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        active = "openclaw run" in res.stdout
        label  = "[green]active[/green]" if active else "[dim]not set[/dim]"
        _p(f"  Scheduler   {label}")

    _p("")


# ── logs ──────────────────────────────────────────────────────────────────────

@cli.command()
def logs():
    """Tail the agent log."""
    log_file = LOG_DIR / "agent.log"
    if not log_file.exists():
        _p("No logs yet. They appear after the first scheduled run.")
        return
    os.execvp("tail", ["tail", "-f", str(log_file)])


# ── stop ──────────────────────────────────────────────────────────────────────

@cli.command()
def stop():
    """Stop the scheduler."""
    sys_name = platform.system()
    if sys_name == "Darwin":
        plist = Path.home() / "Library/LaunchAgents/com.openclaw.agent.plist"
        if plist.exists():
            subprocess.run(["launchctl", "unload", str(plist)], capture_output=True)
            _p("  [green]✓[/green]  Scheduler stopped.")
        else:
            _p("  Scheduler not running.")
    elif sys_name == "Linux":
        res   = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        lines = [l for l in res.stdout.splitlines() if "openclaw run" not in l]
        subprocess.run(["crontab", "-"], input="\n".join(lines) + "\n", text=True)
        _p("  [green]✓[/green]  Scheduler removed from crontab.")
    else:
        _p("  Run: schtasks /delete /tn OpenClaw /f")


# ── Scheduler helpers ─────────────────────────────────────────────────────────

def _setup_scheduler(schedule_time: str):
    exe = shutil.which("openclaw") or sys.executable

    parts  = (schedule_time + ":00").split(":")
    hour   = parts[0].zfill(2)
    minute = parts[1].zfill(2)

    sys_name = platform.system()
    if sys_name == "Darwin":
        _scheduler_launchctl(exe, hour, minute)
    elif sys_name == "Linux":
        _scheduler_cron(exe, hour, minute)
    elif sys_name == "Windows":
        _scheduler_windows(exe, hour, minute)
    else:
        _p(f"  [yellow]![/yellow]  Unknown platform. Run manually: openclaw run")


def _scheduler_launchctl(exe: str, hour: str, minute: str):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.openclaw.agent</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>run</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>{int(hour)}</integer>
        <key>Minute</key>
        <integer>{int(minute)}</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>{LOG_DIR}/agent.log</string>
    <key>StandardErrorPath</key>
    <string>{LOG_DIR}/agent.log</string>
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>"""

    agents_dir = Path.home() / "Library" / "LaunchAgents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    plist_path = agents_dir / "com.openclaw.agent.plist"
    plist_path.write_text(plist_content)

    subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)
    res = subprocess.run(["launchctl", "load",   str(plist_path)], capture_output=True, text=True)
    if res.returncode == 0:
        _p("  [green]✓[/green]  Scheduler registered (runs daily at " + f"{hour}:{minute})")
    else:
        _p(f"  [yellow]![/yellow]  Scheduler warning: {res.stderr.strip() or 'check launchctl'}")


def _scheduler_cron(exe: str, hour: str, minute: str):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    cron_line = f"{minute} {hour} * * * {exe} run >> {LOG_DIR}/agent.log 2>&1"
    res = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    existing = res.stdout if res.returncode == 0 else ""
    lines = [l for l in existing.splitlines() if "openclaw run" not in l]
    lines.append(cron_line)
    subprocess.run(["crontab", "-"], input="\n".join(lines) + "\n", text=True)
    _p("  [green]✓[/green]  Scheduler registered (cron)")


def _scheduler_windows(exe: str, hour: str, minute: str):
    cmd = (
        f'schtasks /create /tn "OpenClaw" /tr "{exe} run" '
        f'/sc daily /st {hour}:{minute} /f'
    )
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if res.returncode == 0:
        _p("  [green]✓[/green]  Scheduler registered (Task Scheduler)")
    else:
        _p(f"  [yellow]![/yellow]  Could not auto-register. Run manually:\n  {cmd}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    cli()
