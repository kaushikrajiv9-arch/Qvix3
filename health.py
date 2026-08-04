#!/usr/bin/env python3
"""
health.py — QVIX operational health check and startup validator.

Past incidents this layer would have caught immediately:
  - .env wipe: POLYGON_API_KEY/DISCORD_WEBHOOK_URL missing → exits with clear
    message instead of running blind and posting nothing for hours
  - stale PID lock: health report surfaces PID + liveness so you can spot a
    ghost lock before it blocks a restart
  - circuit_breaker.json missing (Aug 3 2026): explicit ERROR naming the real
    cause instead of "daily loss limit hit" appearing 8+ hours later on real signals
  - DNS-at-boot: _wait_for_network() in qvix.py already handles this; health
    standalone surfaces DNS state for manual checks too

Usage:
  python3 health.py              # terminal report
  python3 health.py --discord    # terminal report + Discord embed
  python3 health.py --quiet      # exit-code only (0=ok/warn, 1=error/crit)

Imported by qvix.py, spy_0dte_uw.py, kraken_spot_mm.py for startup validation.
"""

import json
import os
import socket
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
LOG_DIR  = BASE_DIR / "logs"

ET = timezone(
    __import__("datetime").timedelta(hours=-4)
    if datetime.now(__import__("datetime").timezone.utc).utctimetuple().tm_mon in range(3, 12)
    else __import__("datetime").timedelta(hours=-5)
)


# ─── DATA MODEL ────────────────────────────────────────────────────────────────

LEVEL_ORDER = {"crit": 0, "error": 1, "warn": 2, "ok": 3}
LEVEL_EMOJI = {"crit": "🔴", "error": "🟠", "warn": "⚠️ ", "ok": "✅"}

@dataclass
class Check:
    level:    str   # "crit" | "error" | "warn" | "ok"
    category: str   # "creds" | "config" | "process" | "api"
    name:     str   # short label
    detail:   str   # one-line explanation shown to operator
    fix:      str = ""  # optional remediation hint


def worst_level(checks: List[Check]) -> str:
    if not checks:
        return "ok"
    return min(checks, key=lambda c: LEVEL_ORDER[c.level]).level


# ─── CREDENTIAL CHECKS ─────────────────────────────────────────────────────────

def check_credentials() -> List[Check]:
    """
    Verifies required env vars are set. File presence only — live API
    validity is tested separately in check_api_health().

    Incident: .env wipe (Aug 2025) — POLYGON_API_KEY and DISCORD_WEBHOOK_URL
    vanished silently. qvix kept running but fetched no data and posted
    nothing. These two are now CRITICAL: missing either one means the whole
    system is deaf and dumb.
    """
    results = []

    def _chk(key: str, level: str, name: str, why: str, fix: str = "") -> None:
        val = os.getenv(key, "")
        if val:
            results.append(Check("ok", "creds", name, f"{key} set"))
        else:
            results.append(Check(level, "creds", name,
                                 f"{key} missing — {why}", fix))

    _chk("POLYGON_API_KEY",       "crit",  "Polygon key",
         "qvix.py cannot fetch any market data",
         "Add POLYGON_API_KEY=<key> to .env and restart")

    _chk("DISCORD_WEBHOOK_URL",   "crit",  "Discord webhook",
         "no alerts or status reports can be posted",
         "Add DISCORD_WEBHOOK_URL=<url> to .env and restart")

    _chk("ROBINHOOD_ACCESS_TOKEN", "error", "Robinhood token",
         "BDB auto-execution silently disabled — signals fire on Discord but no RH orders placed",
         "Refresh token from browser DevTools → Network → any api.robinhood.com request → Authorization header")

    _chk("TRADEODDS_API_KEY",     "warn",  "TradeOdds key",
         "BDB and 0DTE TradeOdds gate will fail-open after 10-min grace period",
         "Add TRADEODDS_API_KEY=<key> to .env")

    _chk("UNUSUAL_WHALES_API_KEY","warn",  "Unusual Whales key",
         "spy_0dte_uw.py will refuse to start; UW radar in qvix.py also disabled",
         "Add UNUSUAL_WHALES_API_KEY=<key> to .env")

    _chk("ANTHROPIC_API_KEY",     "warn",  "Anthropic key",
         "AI signal synthesis/narrative features disabled",
         "Add ANTHROPIC_API_KEY=<key> to .env")

    _chk("KRAKEN_API_KEY",        "warn",  "Kraken key",
         "kraken_spot_mm.py cannot authenticate — no market-making",
         "Add KRAKEN_API_KEY + KRAKEN_API_SECRET to .env")

    return results


# ─── CONFIG FILE CHECKS ────────────────────────────────────────────────────────

def check_config_files() -> List[Check]:
    """
    Verifies required config files exist and are valid.

    Incident: circuit_breaker.json missing (Aug 3 2026) — every BDB signal
    that reached the RH auto-execution step sent a Discord alert saying
    "Circuit breaker tripped — daily loss limit hit", which implies a real
    P&L loss. The real cause was a missing config file. This check names the
    actual problem immediately at startup so it never costs a trading day again.
    """
    results = []

    # circuit_breaker.json
    cb_path = BASE_DIR / "circuit_breaker.json"
    if not cb_path.exists():
        results.append(Check(
            "error", "config", "circuit_breaker.json",
            "file missing — RH BDB auto-execution will block every signal with the "
            "misleading message 'Circuit breaker tripped — daily loss limit hit' "
            "(real cause: starting_equity was never seeded)",
            f"echo '{{\"starting_equity\": <your_rh_equity>}}' > {cb_path}",
        ))
    else:
        try:
            cb = json.loads(cb_path.read_text())
            eq = float(cb["starting_equity"])
            if eq <= 0:
                raise ValueError("starting_equity must be > 0")
            today = date.today().isoformat()
            # Read today's closed P&L from signal_log.json (same logic as qvix.py)
            sl = LOG_DIR / "signal_log.json"
            todays_pnl = 0.0
            if sl.exists():
                try:
                    records = json.loads(sl.read_text())
                    todays_pnl = sum(
                        r["pnl"] for r in records
                        if r.get("status") == "closed"
                        and r.get("pnl") is not None
                        and str(r.get("exit_time") or "").startswith(today)
                    )
                except Exception:
                    pass
            limit = 5.0
            if -todays_pnl >= limit:
                results.append(Check(
                    "warn", "config", "circuit_breaker.json",
                    f"HALTED — today closed P&L is {todays_pnl:+.2f}% "
                    f"(limit −{limit:.0f}%, equity=${eq:,.2f}) — RH auto-execution blocked",
                    "Investigate closed positions; breaker resets at midnight",
                ))
            else:
                results.append(Check(
                    "ok", "config", "circuit_breaker.json",
                    f"starting_equity=${eq:,.2f}  today_pnl={todays_pnl:+.2f}%  "
                    f"limit=−{limit:.0f}%  → CLEAR",
                ))
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            results.append(Check(
                "error", "config", "circuit_breaker.json",
                f"file exists but is invalid ({exc}) — RH auto-execution blocked",
                f'Fix: echo \'{{"starting_equity": <your_rh_equity>}}\' > {cb_path}',
            ))

    # rh_kill_switch — presence is a warning (execution intentionally off)
    ks = LOG_DIR / "rh_kill_switch"
    if ks.exists():
        results.append(Check(
            "warn", "config", "rh_kill_switch",
            "present — RH auto-execution is OFF for ALL signals",
            f"rm {ks}  to re-enable",
        ))
    else:
        results.append(Check(
            "ok", "config", "rh_kill_switch",
            "not present — RH auto-execution ACTIVE",
        ))

    return results


# ─── PROCESS STATE CHECKS ──────────────────────────────────────────────────────

def check_process_state() -> List[Check]:
    """
    Checks which QVIX processes are actually running and whether their
    output files are recent.

    Incident: stale PID lock — qvix.pid existed for a dead process, blocked
    restarts. This now surfaces the PID + liveness so you can spot a ghost
    lock before attempting a restart.
    """
    results = []

    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def _check_pid_file(label: str, pid_path: Path) -> Check:
        if not pid_path.exists():
            return Check("warn", "process", label,
                         f"{pid_path.name} not found — process may not be running")
        try:
            pid = int(pid_path.read_text().strip())
            if _pid_alive(pid):
                return Check("ok", "process", label, f"running (PID {pid})")
            else:
                return Check("error", "process", label,
                             f"STALE PID FILE — PID {pid} is dead (ghost lock)",
                             f"rm {pid_path}  then restart")
        except (ValueError, OSError):
            return Check("warn", "process", label, f"{pid_path.name} unreadable")

    def _check_log_freshness(label: str, log_path: Path,
                              stale_secs: int, running_ok: bool = True) -> Optional[Check]:
        if not log_path.exists():
            return Check("warn", "process", label, f"log not found: {log_path.name}")
        age = time.time() - log_path.stat().st_mtime
        if age > stale_secs:
            mins = int(age // 60)
            return Check("warn", "process", label,
                         f"log stale — last write {mins} min ago (expect every "
                         f"{stale_secs // 60} min)",
                         "Check if process is running via launchctl list com.qvix.*")
        return None  # caller decides the ok message

    # qvix.py — has a PID file
    results.append(_check_pid_file("qvix.py", LOG_DIR / "qvix.pid"))

    # spy_0dte_uw.py — no PID file; use log freshness (scans every 5 min but
    # only during 9:30–11:00 ET, so we skip freshness outside that window)
    now_et = datetime.now(ET)
    in_0dte_window = (
        now_et.weekday() < 5
        and (now_et.hour, now_et.minute) >= (9, 30)
        and (now_et.hour, now_et.minute) < (11, 0)
    )
    uw_log = LOG_DIR / "spy_0dte_uw.log"
    if uw_log.exists():
        age = time.time() - uw_log.stat().st_mtime
        if in_0dte_window and age > 600:
            results.append(Check("warn", "process", "spy_0dte_uw",
                                 f"log stale ({int(age//60)} min) during scan window"))
        else:
            status = "in scan window" if in_0dte_window else "outside scan window (idle)"
            results.append(Check("ok", "process", "spy_0dte_uw",
                                 f"log present, {status}"))
    else:
        results.append(Check("warn", "process", "spy_0dte_uw", "log not found"))

    # robinhood_bridge — runs every 5 min (300s), stale if > 12 min old
    rb_log = LOG_DIR / "robinhood_bridge.log"
    stale = _check_log_freshness("robinhood_bridge", rb_log, 720)
    if stale:
        results.append(stale)
    elif rb_log.exists():
        age = time.time() - rb_log.stat().st_mtime
        results.append(Check("ok", "process", "robinhood_bridge",
                             f"last run {int(age//60)}m ago"))

    # kraken_spot_mm — should log every ~10s; stale if > 5 min
    km_log = LOG_DIR / "kraken_spot_mm.log"
    stale = _check_log_freshness("kraken_spot_mm", km_log, 300)
    if stale:
        results.append(stale)
    elif km_log.exists():
        # Parse last known USD balance from log
        usd_balance: Optional[float] = None
        try:
            text = km_log.read_text(errors="replace")
            for line in reversed(text.splitlines()):
                if "USD=" in line and "USDT=" in line:
                    for part in line.split():
                        if part.startswith("USD="):
                            usd_balance = float(part.split("=")[1])
                            break
                    if usd_balance is not None:
                        break
        except Exception:
            pass

        age = time.time() - km_log.stat().st_mtime
        if usd_balance is not None and usd_balance < 50:
            results.append(Check(
                "warn", "process", "kraken_spot_mm",
                f"running (log {int(age//60)}m ago)  USD=${usd_balance:.2f} — "
                "below minimum bid size, all pairs SKIP (no trades until funded)",
                "Deposit USD to Kraken account — see kraken_spot_mm.py header for suggested split",
            ))
        else:
            bal = f"  USD=${usd_balance:.2f}" if usd_balance is not None else ""
            results.append(Check("ok", "process", "kraken_spot_mm",
                                 f"running (log {int(age//60)}m ago){bal}"))

    return results


# ─── API HEALTH CHECKS ─────────────────────────────────────────────────────────

def check_api_health() -> List[Check]:
    """
    Makes live network calls to verify each external API is reachable and
    the relevant credential is accepted.  Only called in standalone mode
    (python3 health.py) — too slow and side-effect-y to run at every startup.
    """
    import requests as _req

    results = []

    # DNS check first
    try:
        socket.getaddrinfo("api.polygon.io", 443, socket.AF_UNSPEC, socket.SOCK_STREAM)
        results.append(Check("ok", "api", "DNS", "resolves api.polygon.io"))
    except socket.gaierror as exc:
        results.append(Check("error", "api", "DNS",
                             f"DNS failure: {exc} — all API calls will fail",
                             "Check network connection; launchctl processes wait up to 45s at boot"))
        return results  # no point testing anything else

    # Polygon
    polygon_key = os.getenv("POLYGON_API_KEY", "")
    if polygon_key:
        try:
            r = _req.get("https://api.polygon.io/v1/marketstatus/now",
                         params={"apiKey": polygon_key}, timeout=8)
            if r.status_code == 200:
                results.append(Check("ok", "api", "Polygon", "key valid, API live"))
            elif r.status_code in (401, 403):
                results.append(Check("crit", "api", "Polygon",
                                     f"HTTP {r.status_code} — key invalid or expired",
                                     "Replace POLYGON_API_KEY in .env"))
            else:
                results.append(Check("warn", "api", "Polygon",
                                     f"unexpected HTTP {r.status_code}"))
        except Exception as exc:
            results.append(Check("warn", "api", "Polygon", f"request failed: {exc}"))
    else:
        results.append(Check("crit", "api", "Polygon", "key not set — skipping live check"))

    # TradeOdds — uses POST /api/v1/analyze
    to_key = os.getenv("TRADEODDS_API_KEY", "")
    if to_key:
        try:
            r = _req.post(
                "https://tradeodds-production.up.railway.app/api/v1/analyze",
                headers={"Authorization": f"Bearer {to_key}"},
                json={"symbol": "SPY", "forward": "1d", "reference": "1d",
                      "direction": "call", "lookback_years": 2},
                timeout=8,
            )
            if r.status_code in (200, 422):  # 422 = bad params but auth accepted
                results.append(Check("ok", "api", "TradeOdds", "key valid, API live"))
            elif r.status_code in (401, 403):
                results.append(Check("error", "api", "TradeOdds",
                                     f"HTTP {r.status_code} — key invalid",
                                     "Replace TRADEODDS_API_KEY in .env"))
            else:
                results.append(Check("warn", "api", "TradeOdds",
                                     f"unexpected HTTP {r.status_code}"))
        except Exception as exc:
            results.append(Check("warn", "api", "TradeOdds", f"request failed: {exc}"))
    else:
        results.append(Check("warn", "api", "TradeOdds", "key not set — skipping live check"))

    # Robinhood token
    rh_token = os.getenv("ROBINHOOD_ACCESS_TOKEN", "")
    if rh_token:
        try:
            r = _req.get(
                "https://api.robinhood.com/user/",
                headers={"Authorization": f"Bearer {rh_token}", "Accept": "application/json"},
                timeout=8,
            )
            if r.status_code == 200:
                results.append(Check("ok", "api", "Robinhood token", "valid, not expired"))
            elif r.status_code == 401:
                results.append(Check("error", "api", "Robinhood token",
                                     "HTTP 401 — token expired (expires every ~24h)",
                                     "Refresh: DevTools → Network → api.robinhood.com → "
                                     "Authorization header → paste to .env ROBINHOOD_ACCESS_TOKEN"))
            else:
                results.append(Check("warn", "api", "Robinhood token",
                                     f"unexpected HTTP {r.status_code}"))
        except Exception as exc:
            results.append(Check("warn", "api", "Robinhood token", f"request failed: {exc}"))
    else:
        results.append(Check("warn", "api", "Robinhood token",
                             "not set — skipping live check"))

    return results


# ─── AGGREGATION ───────────────────────────────────────────────────────────────

def run_startup_checks() -> List[Check]:
    """Fast checks only (no network). Called by qvix.py / spy_0dte_uw.py / kraken_spot_mm.py at startup."""
    return check_credentials() + check_config_files() + check_process_state()


def run_all_checks() -> List[Check]:
    """Full check suite including live API calls. Used by standalone CLI."""
    return check_credentials() + check_config_files() + check_process_state() + check_api_health()


# ─── FORMATTING ────────────────────────────────────────────────────────────────

def _category_label(cat: str) -> str:
    return {
        "creds":   "CREDENTIALS",
        "config":  "CONFIG FILES",
        "process": "PROCESSES",
        "api":     "API HEALTH",
    }.get(cat, cat.upper())


def format_terminal(checks: List[Check]) -> str:
    now = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
    lines = [f"QVIX Health Check — {now}", ""]

    by_cat: dict = {}
    for c in checks:
        by_cat.setdefault(c.category, []).append(c)

    for cat in ["creds", "config", "process", "api"]:
        if cat not in by_cat:
            continue
        lines.append(_category_label(cat))
        for c in by_cat[cat]:
            icon = LEVEL_EMOJI[c.level]
            line = f"  {icon} {c.name:<24} {c.detail}"
            lines.append(line)
            if c.fix and c.level in ("crit", "error"):
                lines.append(f"     → {c.fix}")
        lines.append("")

    worst = worst_level(checks)
    if worst == "ok":
        lines.append("All checks passed.")
    elif worst == "warn":
        lines.append("Warnings present — system operational but review items above.")
    elif worst == "error":
        lines.append("Errors present — some features degraded. Review items above.")
    else:
        lines.append("CRITICAL issues — system should not start until resolved.")

    return "\n".join(lines)


def format_discord_embed(checks: List[Check]) -> dict:
    worst = worst_level(checks)
    color_map = {"ok": 0x2ECC71, "warn": 0xF39C12, "error": 0xE67E22, "crit": 0xFF0000}
    color = color_map[worst]

    fields = []
    by_cat: dict = {}
    for c in checks:
        by_cat.setdefault(c.category, []).append(c)

    for cat in ["creds", "config", "process", "api"]:
        if cat not in by_cat:
            continue
        lines = []
        for c in by_cat[cat]:
            icon = LEVEL_EMOJI[c.level]
            text = f"{icon} **{c.name}** — {c.detail}"
            if c.fix and c.level in ("crit", "error"):
                text += f"\n  `{c.fix}`"
            lines.append(text)
        fields.append({
            "name":   _category_label(cat),
            "value":  "\n".join(lines)[:1024],
            "inline": False,
        })

    status_text = {
        "ok":    "✅ All systems operational",
        "warn":  "⚠️  Warnings — some features may be degraded",
        "error": "🟠 Errors — action required",
        "crit":  "🔴 CRITICAL — system should not be running",
    }[worst]

    now = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
    return {
        "title":       f"🔍 QVIX Health Check — {now}",
        "description": status_text,
        "color":       color,
        "fields":      fields,
        "footer":      {"text": "Run: python3 health.py --discord  to refresh"},
    }


def post_discord(embed: dict, webhook_url: str) -> None:
    import requests as _req
    try:
        r = _req.post(webhook_url, json={"embeds": [embed]}, timeout=10)
        r.raise_for_status()
    except Exception as exc:
        print(f"Discord post failed: {exc}", file=sys.stderr)


# ─── STARTUP VALIDATION (called by other processes) ────────────────────────────

def startup_validate(process_name: str = "QVIX") -> bool:
    """
    Run fast (no-network) checks and log results. Returns True if safe to
    start, False if any CRITICAL issue blocks operation.

    Logs errors/warnings to the root logger so they appear in each process's
    own log file. Posts a Discord embed if there are any non-OK checks and
    DISCORD_WEBHOOK_URL is set.

    Call this early in main(), before _wait_for_network() / the scan loop.
    """
    import logging

    checks = check_credentials() + check_config_files()
    has_crit  = any(c.level == "crit"  for c in checks)
    has_error = any(c.level in ("crit", "error") for c in checks)

    for c in checks:
        msg = f"[startup] {c.name}: {c.detail}"
        if c.level == "crit":
            logging.critical(msg)
            if c.fix:
                logging.critical(f"  Fix: {c.fix}")
        elif c.level == "error":
            logging.error(msg)
            if c.fix:
                logging.error(f"  Fix: {c.fix}")
        elif c.level == "warn":
            logging.warning(msg)
        else:
            logging.info(msg)

    if has_error:
        webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
        if webhook:
            embed = format_discord_embed(checks)
            embed["title"] = f"⚙️ {process_name} startup — config issues found"
            post_discord(embed, webhook)

    if has_crit:
        logging.critical(
            f"{process_name} startup blocked by critical config issue — "
            "fix the above and restart"
        )
        return False

    if not has_error:
        logging.info(f"[startup] All config checks passed")

    return True


# ─── CLI ENTRY POINT ───────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="QVIX operational health check")
    parser.add_argument("--discord", action="store_true",
                        help="Post results to Discord in addition to terminal output")
    parser.add_argument("--quiet",   action="store_true",
                        help="No output; exit code 0=ok/warn, 1=error/crit")
    parser.add_argument("--no-api",  action="store_true",
                        help="Skip live API calls (credentials + config + processes only)")
    args = parser.parse_args()

    checks = run_all_checks() if not args.no_api else run_startup_checks()

    if not args.quiet:
        print(format_terminal(checks))

    if args.discord:
        webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
        if not webhook:
            print("DISCORD_WEBHOOK_URL not set — cannot post embed", file=sys.stderr)
        else:
            embed = format_discord_embed(checks)
            post_discord(embed, webhook)
            print("Discord embed posted.")

    worst = worst_level(checks)
    sys.exit(0 if worst in ("ok", "warn") else 1)


if __name__ == "__main__":
    main()
