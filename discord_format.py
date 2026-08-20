#!/usr/bin/env python3
"""
discord_format.py — Shared QVIX Discord signal-card formatter.

One consistent visual format for every actionable trade signal QVIX posts
(stock swing signals, 0DTE scalps, calendar spreads, ...) so a message reads
the same regardless of which script produced it. Imported directly by
qvix.py, spy_0dte_uw.py, and agents/portfolio_agent.py — all three are
plain sibling/child modules of this file, so a normal import works without
any subprocess or packaging changes.

Never fabricates a number: any field left None/empty is simply omitted
(or replaced with an explicit "check your broker" line for price data),
rather than shown as an approximation. Matches the existing convention
already used by qvix.py's real_data-gated option plans.
"""

from datetime import date, datetime, timezone
from typing import Optional
import json
import os
from pathlib import Path

_TOKENS_PATH = Path.home() / ".qvix_discord_tokens.json"


def get_discord_webhook_url(id_env: str = "DISCORD_WEBHOOK_ID") -> str:
    """
    Assemble a Discord webhook URL from two separate sources:
      • .env supplies the numeric snowflake ID only (DISCORD_WEBHOOK_ID)
      • ~/.qvix_discord_tokens.json supplies the secret token (outside project)
    Neither source alone produces a working URL.  A process that reads .env
    cannot reach the webhook without also reading the token file — which is not
    referenced in any project file by path, only through this function.

    Usage:
      get_discord_webhook_url()                        # reads DISCORD_WEBHOOK_ID
      get_discord_webhook_url("DISCORD_CRYPTO_WEBHOOK_ID")  # reads alternate ID

    ~/.qvix_discord_tokens.json format:
      { "<webhook_id>": "<token>", "<crypto_id>": "<token>" }
    """
    wh_id = os.getenv(id_env, "")
    if not wh_id:
        return ""
    try:
        tokens: dict = json.loads(_TOKENS_PATH.read_text())
        wh_token = tokens.get(wh_id, "")
    except (FileNotFoundError, json.JSONDecodeError):
        return ""
    if not wh_token:
        return ""
    return f"https://discord.com/api/webhooks/{wh_id}/{wh_token}"


DIVIDER = "━" * 27

_DIRECTION_EMOJI = {"BULLISH": "📈", "BEARISH": "📉", "NEUTRAL": "📅"}
_DIRECTION_COLOR = {"BULLISH": 0x00C851, "BEARISH": 0xFF4444, "NEUTRAL": 0x9B59B6}


def _fmt_strike(strike: float) -> str:
    """$390 for a round number, $390.50 otherwise — never a fabricated '.00'."""
    return f"${strike:,.0f}" if float(strike).is_integer() else f"${strike:,.2f}"


def _fmt_expiry(expiry: date) -> str:
    return expiry.strftime("%b %-d, %Y")


def build_signal_embed(
    *,
    ticker: str,
    direction: str,                      # "BULLISH", "BEARISH", or "NEUTRAL" (e.g. calendar spreads) — drives emoji/color/title
    direction_label: str,                # e.g. "BEARISH PUT", "MOMENTUM BREAKOUT", "0DTE SCALP", "CALENDAR SPREAD"
    strike: Optional[float] = None,
    expiry: Optional[date] = None,
    bid: Optional[float] = None,
    ask: Optional[float] = None,
    target: Optional[float] = None,
    target_pct: Optional[float] = None,
    stop: Optional[float] = None,
    stop_pct: Optional[float] = None,
    volume: Optional[int] = None,
    open_interest: Optional[int] = None,
    confidence: int = 0,
    smart_money_note: Optional[str] = None,
    holding_period: str = "",
    strategy_lines: Optional[list] = None,  # pre-formatted lines that REPLACE the strike/bid/entry/target/stop block (e.g. calendar spreads); confidence/smart-money/footer still apply
    footer_text: str = "QVIX",
) -> dict:
    """
    Build the shared QVIX signal embed. `direction` controls the header
    emoji/color/title; everything else is optional and simply omitted (never
    approximated) when not available.
    """
    direction = direction.upper()
    emoji = _DIRECTION_EMOJI.get(direction, "📅")
    color = _DIRECTION_COLOR.get(direction, 0x9B59B6)

    lines = [DIVIDER, f"{emoji} Direction: {direction_label}"]

    if strategy_lines is not None:
        lines.extend(strategy_lines)
    else:
        if strike is not None:
            lines.append(f"🏷️ Strike: {_fmt_strike(strike)}")
        if expiry is not None:
            lines.append(f"📅 Expiry: {_fmt_expiry(expiry)}")

        if bid is not None and ask is not None:
            lines.append(f"💰 Bid: ${bid:.2f} | Ask: ${ask:.2f}")
            lines.append(f"🎯 Entry: ${bid:.2f} - ${ask:.2f}")
        elif strike is not None:
            lines.append("💰 Buy option: Check live price on your broker")

        if target is not None and target_pct is not None:
            lines.append(f"✅ Target: ${target:.2f} (+{target_pct:.0f}%)")
        if stop is not None and stop_pct is not None:
            lines.append(f"🛑 Stop: ${stop:.2f} (-{stop_pct:.0f}%)")

        if volume is not None and open_interest is not None:
            lines.append(f"📊 Volume: {volume:,} | OI: {open_interest:,}")

    lines.append(f"⚡ Confidence: {confidence}/100")
    if smart_money_note:
        lines.append(f"🐋 Smart Money: {smart_money_note}")

    lines.append(DIVIDER)
    if holding_period:
        lines.append(f"⏱️ {holding_period}")
    if stop_pct is not None and target_pct is not None:
        lines.append(f"❌ Close at {stop_pct:.0f}% loss | ✅ Target +{target_pct:.0f}%")
    lines.append(DIVIDER)
    lines.append("Educational only. Not financial advice.")

    return {
        "title":       f"🎯 QVIX SIGNAL — {ticker} {direction}",
        "description": "\n".join(lines),
        "color":       color,
        "footer":      {"text": footer_text},
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }
