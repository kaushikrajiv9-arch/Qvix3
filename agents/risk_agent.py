#!/usr/bin/env python3
"""
risk_agent.py — Risk guardian for QVIX 6.0.

RiskManagerAgent reads circuit_breaker.json, checks LONG_BIAS_TICKERS,
validates position size, and checks today's daily loss limit before
approving or rejecting any signal.
"""

import json
import logging
import os
import threading
from datetime import date, datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

QVIX_DIR             = Path(__file__).parent.parent
CIRCUIT_BREAKER_FILE = QVIX_DIR / "circuit_breaker.json"
SIGNAL_LOG_FILE      = QVIX_DIR / "signal_log.json"
MAX_DAILY_LOSS_PCT   = 5.0
LONG_BIAS_TICKERS    = {"ASTS", "RKLB", "LUNR", "RCAT", "IREN", "IONQ"}

LOG_DIR   = QVIX_DIR / "logs"
LOG_FILE  = LOG_DIR / "agent_decisions.jsonl"
_log_lock = threading.Lock()


def _append_log(record: dict):
    LOG_DIR.mkdir(exist_ok=True)
    with _log_lock:
        with LOG_FILE.open("a") as f:
            f.write(json.dumps(record, default=str) + "\n")


def run_risk_check(ticker: str, synthesis_report: dict) -> dict:
    """
    Gate the synthesis verdict through risk controls.
    Returns APPROVED or REJECTED with full reasoning.
    Defaults to REJECTED on any internal error — always conservative.
    """
    _FALLBACK = {
        "agent": "risk_manager", "decision": "REJECTED",
        "reason": "Risk manager error — defaulting to REJECTED for safety",
        "approved_size_pct": 0.0, "max_loss_usd": 0.0,
        "circuit_breaker_status": "unknown",
    }

    try:
        verdict        = synthesis_report.get("verdict", "HOLD")
        requested_size = float(synthesis_report.get("position_size_pct") or 1)

        # ── Circuit breaker ───────────────────────────────────────────────────
        cb_status       = "ok"
        starting_equity = 0.0

        try:
            cb              = json.loads(CIRCUIT_BREAKER_FILE.read_text())
            starting_equity = float(cb.get("starting_equity") or 0)
        except FileNotFoundError:
            cb_status = "not_configured"
        except (KeyError, ValueError, json.JSONDecodeError):
            cb_status = "not_configured"

        if starting_equity > 0:
            today = date.today().isoformat()
            try:
                records = json.loads(SIGNAL_LOG_FILE.read_text())
            except Exception:
                records = []
            todays_pnl = sum(
                float(r.get("pnl") or 0)
                for r in records
                if r.get("status") == "closed"
                and r.get("pnl") is not None
                and str(r.get("exit_time") or "").startswith(today)
                and (r.get("executed_liquid") or r.get("executed_robinhood"))
            )
            if -todays_pnl >= MAX_DAILY_LOSS_PCT:
                cb_status = "tripped"

        # ── Gate 1: circuit breaker tripped ──────────────────────────────────
        if cb_status == "tripped":
            result = {
                "agent": "risk_manager", "decision": "REJECTED",
                "reason": f"Circuit breaker tripped — daily loss limit ({MAX_DAILY_LOSS_PCT}%) reached",
                "approved_size_pct": 0.0, "max_loss_usd": 0.0,
                "circuit_breaker_status": "tripped",
            }
            _append_log({"ts": datetime.now().isoformat(), "type": "risk",
                         "ticker": ticker, **result})
            return result

        # ── Gate 2: LONG_BIAS_TICKERS — block SELL ────────────────────────────
        if ticker in LONG_BIAS_TICKERS and verdict == "SELL":
            result = {
                "agent": "risk_manager", "decision": "REJECTED",
                "reason": f"{ticker} is a long-bias ticker — SELL signals are blocked by policy",
                "approved_size_pct": 0.0, "max_loss_usd": 0.0,
                "circuit_breaker_status": cb_status,
            }
            _append_log({"ts": datetime.now().isoformat(), "type": "risk",
                         "ticker": ticker, **result})
            return result

        # ── Gate 3a: CALENDAR_SPREAD — always approved at minimum size ──────────
        if verdict == "CALENDAR_SPREAD":
            if starting_equity > 0:
                position_usd = starting_equity * 1.0 / 100.0
                max_loss_usd = round(position_usd * 0.07, 2)
            else:
                max_loss_usd = 0.0
            result = {
                "agent": "risk_manager", "decision": "APPROVED",
                "reason": "Calendar spread approved — 1% position (lowest-risk theta strategy)",
                "approved_size_pct": 1.0,
                "max_loss_usd":      max_loss_usd,
                "circuit_breaker_status": cb_status,
            }
            _append_log({"ts": datetime.now().isoformat(), "type": "risk",
                         "ticker": ticker, **result})
            return result

        # ── Gate 3b: HOLD verdict — no position ──────────────────────────────
        if verdict == "HOLD":
            result = {
                "agent": "risk_manager", "decision": "REJECTED",
                "reason": "Synthesis verdict is HOLD — no position to open",
                "approved_size_pct": 0.0, "max_loss_usd": 0.0,
                "circuit_breaker_status": cb_status,
            }
            _append_log({"ts": datetime.now().isoformat(), "type": "risk",
                         "ticker": ticker, **result})
            return result

        # ── Approved — clamp position size and compute max loss ───────────────
        approved_size = max(1.0, min(3.0, requested_size))
        sl_pct        = 0.07   # 7% stop loss for max-loss calculation
        if starting_equity > 0:
            position_usd = starting_equity * approved_size / 100.0
            max_loss_usd = round(position_usd * sl_pct, 2)
        else:
            max_loss_usd = 0.0   # circuit breaker not seeded — no $ figure available

        result = {
            "agent": "risk_manager", "decision": "APPROVED",
            "reason": f"{verdict} approved — {approved_size:.0f}% position, ${max_loss_usd:.0f} max loss at 7% stop",
            "approved_size_pct": approved_size,
            "max_loss_usd":      max_loss_usd,
            "circuit_breaker_status": cb_status,
        }
        _append_log({"ts": datetime.now().isoformat(), "type": "risk",
                     "ticker": ticker, **result})
        return result

    except Exception as exc:
        logging.warning(f"RiskManager {ticker}: {exc}")
        return _FALLBACK
