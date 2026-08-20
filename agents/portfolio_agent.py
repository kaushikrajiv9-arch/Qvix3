#!/usr/bin/env python3
"""
portfolio_agent.py — Discord reporter and decision logger for QVIX 6.0.

PortfolioManagerAgent formats a rich Discord embed showing all agent
reasoning, posts it to Discord, and appends the full analysis record
to logs/agent_decisions.jsonl.
"""

import json
import logging
import os
import sys
import threading
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from discord_format import build_signal_embed, get_discord_webhook_url

load_dotenv(_ROOT / ".env")

DISCORD_WEBHOOK_URL = get_discord_webhook_url()

LOG_DIR   = Path(__file__).parent.parent / "logs"
LOG_FILE  = LOG_DIR / "agent_decisions.jsonl"
_log_lock = threading.Lock()

_COLORS = {"BUY": 0x00C851, "SELL": 0xFF4444, "HOLD": 0x888888, "CALENDAR_SPREAD": 0x9B59B6}
_EMOJIS = {"BUY": "🚀", "SELL": "📉", "HOLD": "⏸️", "CALENDAR_SPREAD": "📅"}
_VERDICT_ICON = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}


def _append_log(record: dict):
    LOG_DIR.mkdir(exist_ok=True)
    with _log_lock:
        with LOG_FILE.open("a") as f:
            f.write(json.dumps(record, default=str) + "\n")


def _post_discord(embed: dict) -> bool:
    if not DISCORD_WEBHOOK_URL:
        logging.warning("Discord post skipped — DISCORD_WEBHOOK_URL not set")
        return False
    try:
        r = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"username": "QVIX 6.0 · Agent System", "embeds": [embed]},
            timeout=10,
        )
        ok = r.status_code in (200, 204)
        logging.info(f"  Discord {'OK' if ok else 'FAILED'} ({r.status_code})")
        return ok
    except Exception as exc:
        logging.warning(f"  Discord post exception: {exc}")
        return False


def _icon(verdict: str) -> str:
    return _VERDICT_ICON.get(str(verdict).upper(), "⚪")


def _truncate(s: str, n: int = 1024) -> str:
    return s[:n] if s else "—"


def run_portfolio_manager(ticker: str, all_reports: dict) -> dict:
    """
    Format complete multi-agent Discord embed and post it.
    Always posts regardless of risk decision so Discord shows the full
    reasoning even when a signal is rejected.

    all_reports keys: analysts, bull, bear, synthesis, risk
    """
    synthesis = all_reports.get("synthesis", {})
    risk      = all_reports.get("risk", {})
    bull      = all_reports.get("bull", {})
    bear      = all_reports.get("bear", {})
    analysts  = all_reports.get("analysts", [])

    verdict  = synthesis.get("verdict", "HOLD")
    conf     = synthesis.get("confidence", 0)
    decision = risk.get("decision", "REJECTED")
    color    = _COLORS.get(verdict, 0x888888)
    emoji    = _EMOJIS.get(verdict, "⏸️")

    # Index analyst reports by agent name
    by_agent = {r.get("agent"): r for r in analysts}
    tech  = by_agent.get("technical", {})
    sm    = by_agent.get("smart_money", {})
    fund  = by_agent.get("fundamental", {})
    sent  = by_agent.get("sentiment", {})

    def analyst_field(label: str, report: dict) -> dict:
        v = report.get("verdict", "—")
        c = report.get("confidence", 0)
        k = report.get("key_signal", "—")
        return {
            "name":   label,
            "value":  f"{_icon(v)} **{v}** ({c}/100)\n{_truncate(k, 200)}",
            "inline": True,
        }

    fields = [
        # Risk decision banner
        {
            "name":   "🎯 Risk Decision",
            "value":  f"**{decision}** — {_truncate(risk.get('reason', '—'), 300)}",
            "inline": False,
        },
        # 4 analyst verdicts (2 per row)
        analyst_field("📈 Technical",    tech),
        analyst_field("🐋 Smart Money",  sm),
        analyst_field("📊 Fundamental",  fund),
        analyst_field("🌊 Sentiment",    sent),
        # Debate
        {
            "name":   "🐂 Bull Case",
            "value":  _truncate(bull.get("thesis", "—"), 800),
            "inline": False,
        },
        {
            "name":   "🐻 Bear Case",
            "value":  _truncate(bear.get("thesis", "—"), 800),
            "inline": False,
        },
        # Final judgment
        {
            "name": "⚖️ Judgment",
            "value": (
                f"Winner: **{str(synthesis.get('winner','neutral')).upper()}**\n"
                + _truncate(synthesis.get("reasoning", "—"), 600)
            ),
            "inline": False,
        },
    ]

    # Circuit breaker footer field
    cb = risk.get("circuit_breaker_status", "—")
    fields.append({
        "name":   "🛡️ Circuit Breaker",
        "value":  f"**{cb}**",
        "inline": True,
    })

    footer_text = f"QVIX 6.0 Multi-Agent System  ·  {datetime.now().strftime('%Y-%m-%d %I:%M %p ET')}"

    # Actionable + approved verdicts get the shared clean signal card up
    # front (discord_format.py — same visual format as every other QVIX
    # signal type), with the full agent debate above kept as supplementary
    # fields. HOLD / REJECTED aren't a trade to present as a signal card, so
    # they keep the original debate-focused embed unchanged.
    card = None
    if verdict in ("BUY", "SELL") and decision == "APPROVED":
        entry = synthesis.get("suggested_entry", 0)
        tp    = synthesis.get("suggested_tp", 0)
        sl    = synthesis.get("suggested_sl", 0)
        size  = risk.get("approved_size_pct", 0)
        maxl  = risk.get("max_loss_usd", 0)

        strategy_lines = [f"🎯 Entry: ${entry:.2f}"]
        if entry:
            strategy_lines.append(f"✅ Target: ${tp:.2f} (+{abs(tp - entry) / entry * 100:.0f}%)")
            strategy_lines.append(f"🛑 Stop: ${sl:.2f} (-{abs(entry - sl) / entry * 100:.0f}%)")
        strategy_lines.append(f"📐 Size: {size:.0f}% of portfolio · Max loss ${maxl:.0f}")

        sm_verdict = str(sm.get("verdict", "")).upper()
        want_verdict = "BULLISH" if verdict == "BUY" else "BEARISH"
        smart_money_note = sm.get("key_signal") if sm_verdict == want_verdict else None

        card = build_signal_embed(
            ticker=ticker,
            direction="BULLISH" if verdict == "BUY" else "BEARISH",
            direction_label=verdict,
            strategy_lines=strategy_lines,
            confidence=conf,
            smart_money_note=smart_money_note,
            holding_period="Swing trade — 2-3 weeks",
            footer_text=footer_text,
        )
    elif verdict == "CALENDAR_SPREAD" and decision == "APPROVED":
        strike = synthesis.get("suggested_strike", 0)
        front  = synthesis.get("front_month_expiry", "—")
        back   = synthesis.get("back_month_expiry", "—")
        maxp   = synthesis.get("max_profit_scenario", "—")
        size   = risk.get("approved_size_pct", 0)
        maxl   = risk.get("max_loss_usd", 0)

        strategy_lines = [
            f"🏷️ Strike: ${strike:.0f} ATM",
            f"📅 Front month: {front}  ·  Back month: {back}",
            "💰 Sell front month, buy back month — same strike",
            f"✅ Max profit: {maxp}",
            f"📐 Size: {size:.0f}% of portfolio · Max loss ${maxl:.0f}",
        ]

        card = build_signal_embed(
            ticker=ticker,
            direction="NEUTRAL",
            direction_label="CALENDAR SPREAD",
            strategy_lines=strategy_lines,
            confidence=conf,
            holding_period="Hold through front-month expiry",
            footer_text=footer_text,
        )

    if card:
        card["fields"] = fields
        embed = card
    else:
        embed = {
            "title": f"{emoji}  QVIX 6.0  ·  {ticker}  ·  {verdict}  ({conf}/100)",
            "color": color,
            "fields": fields,
            "footer": {"text": footer_text},
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

    posted = _post_discord(embed)

    final = {
        "agent":    "portfolio_manager",
        "ticker":   ticker,
        "verdict":  verdict,
        "decision": decision,
        "posted":   posted,
        "ts":       datetime.now().isoformat(),
    }

    _append_log({
        "ts":         datetime.now().isoformat(),
        "type":       "portfolio_complete",
        "ticker":     ticker,
        "all_reports": all_reports,
        "posted":     posted,
    })

    return final
