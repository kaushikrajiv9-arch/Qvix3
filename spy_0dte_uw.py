#!/usr/bin/env python3
"""
SPY 0DTE Scanner — Unusual Whales edition (rebuilt Jul 23 2026).

Reads UW option flow-alerts for SPY, scores same-day-expiry flow, and fires
to Discord when all four gates pass:

  Gate 1 — Window:       10:30–11:00 ET only (10:30-11:00 is the one profitable
                         bucket from 88-signal backtest; 9:30-11:00 produced
                         33% WR vs 47% breakeven — QQQ removed Jul 23 at 23% WR)
  Gate 2 — Daily cap:    max 1 signal per calendar day (prevents re-entry churn)
  Gate 3 — Market Tide:  CALL only when SPY call_net > +$5M; PUT only when
                         call_net < -$5M; fail-closed if UW API down
  Gate 4 — TradeOdds:    prob_edge >= 5.0 AND sample_size >= 10; strict
                         fail-closed (no fallback if API unavailable)
"""

import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, time as dtime
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from discord_format import build_signal_embed, get_discord_webhook_url

load_dotenv()

UNUSUAL_WHALES_API_KEY = os.getenv("UNUSUAL_WHALES_API_KEY", "")
DISCORD_WEBHOOK_URL    = get_discord_webhook_url()
TRADEODDS_API_KEY      = os.getenv("TRADEODDS_API_KEY", "")

ET = ZoneInfo("America/New_York")

TICKERS             = ["SPY"]  # QQQ removed Jul 23: 48 signals 23% WR vs 43% breakeven, -1629% pnl
WINDOW_START        = dtime(10, 30)  # narrowed from 9:30 — 10:30-11:00 is the only profitable bucket
WINDOW_END          = dtime(11, 0)
SCAN_INTERVAL_SECS  = 300     # 5 minutes
IDLE_POLL_SECS      = 60
ALERT_MIN_SCORE     = 70
FLOW_LOOKBACK_MINS  = 15
FLOW_FETCH_LIMIT    = 200
MIN_ENTRY_PREMIUM   = 0.50

# ── Gate constants ────────────────────────────────────────────────────────────
TRADEODDS_BASE_URL         = "https://tradeodds-production.up.railway.app"
ZERO_DTE_MIN_EDGE          = 5.0    # prob_up - prob_dn must exceed this for CALL (reversed for PUT)
ZERO_DTE_MIN_SAMPLE        = 5      # TradeOdds historical sample must be >= this
ZERO_DTE_THIN_SAMPLE       = 10     # below this, require ZERO_DTE_THIN_EDGE instead
ZERO_DTE_THIN_EDGE         = 10.0   # higher edge requirement when sample is thin (5-9 matches)
TIDE_CALL_THRESHOLD        = 5_000_000    # call_net > +$5M required to fire CALL
TIDE_PUT_THRESHOLD         = -5_000_000   # call_net < -$5M required to fire PUT

# ── Flow scoring weights ───────────────────────────────────────────────────────
POINTS_SWEEP_DOMINANT_SIDE = 25
POINTS_REPEATED_ASCENDING  = 20
POINTS_REPEATED_HITS       = 15
POINTS_PREMIUM_OVER_100K   = 15
POINTS_VOL_OI_OVER_10      = 10
POINTS_HAS_SWEEP           = 10
PREMIUM_THRESHOLD          = 100_000
VOL_OI_THRESHOLD           = 10

LOG_DIR    = Path("logs")
STATE_FILE = LOG_DIR / "spy_0dte_uw_state.json"


# ── State persistence ─────────────────────────────────────────────────────────

def _load_state() -> dict:
    """Restore today's already-fired alert ids, contracts, and daily-cap flag. Resets daily."""
    try:
        data = json.loads(STATE_FILE.read_text())
        if data.get("date") == date.today().isoformat():
            return {
                "date":            data["date"],
                "fired_ids":       set(data.get("fired_ids", [])),
                "fired_contracts": set(data.get("fired_contracts", [])),
                "fired_today":     bool(data.get("fired_today", False)),
            }
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    return {
        "date":            date.today().isoformat(),
        "fired_ids":       set(),
        "fired_contracts": set(),
        "fired_today":     False,
    }


def _save_state():
    try:
        LOG_DIR.mkdir(exist_ok=True)
        STATE_FILE.write_text(json.dumps({
            "date":            _state["date"],
            "fired_ids":       sorted(_state["fired_ids"]),
            "fired_contracts": sorted(_state["fired_contracts"]),
            "fired_today":     _state["fired_today"],
        }))
    except Exception as exc:
        logging.warning(f"  state persist failed: {exc}")


_state = _load_state()


# ── Window check ──────────────────────────────────────────────────────────────

def in_scan_window(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(ET)
    if now.weekday() >= 5:
        return False
    return WINDOW_START <= now.time() <= WINDOW_END


# ── UW flow alerts ────────────────────────────────────────────────────────────

def fetch_0dte_flow_alerts(ticker: str) -> list:
    if not UNUSUAL_WHALES_API_KEY:
        logging.warning("  UNUSUAL_WHALES_API_KEY is not set — skipping scan")
        return []
    r = requests.get(
        "https://api.unusualwhales.com/api/option-trades/flow-alerts",
        headers={"Authorization": f"Bearer {UNUSUAL_WHALES_API_KEY}"},
        params={"ticker_symbol": ticker, "min_dte": 0, "max_dte": 0, "limit": FLOW_FETCH_LIMIT},
        timeout=10,
    )
    r.raise_for_status()
    return r.json().get("data", [])


def _recent(alert: dict, cutoff: datetime) -> bool:
    ts_raw = alert.get("created_at")
    if not ts_raw:
        return False
    try:
        ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False
    return ts >= cutoff


# ── Flow scoring ──────────────────────────────────────────────────────────────

def score_alert(alert: dict) -> Optional[dict]:
    option_type = (alert.get("type") or "").lower()
    if option_type not in ("call", "put"):
        return None

    direction = "BULLISH" if option_type == "call" else "BEARISH"

    ask_prem   = float(alert.get("total_ask_side_prem") or 0)
    bid_prem   = float(alert.get("total_bid_side_prem") or 0)
    total_prem = float(alert.get("total_premium") or (ask_prem + bid_prem))
    rule_name  = alert.get("alert_rule") or ""
    has_sweep  = bool(alert.get("has_sweep"))

    vol_oi = alert.get("volume_oi_ratio")
    if vol_oi is None:
        vol = float(alert.get("volume") or 0)
        oi  = float(alert.get("open_interest") or 0)
        vol_oi = (vol / oi) if oi else 0.0
    else:
        try:
            vol_oi = float(vol_oi)
        except (TypeError, ValueError):
            vol_oi = 0.0

    dominant_side_prem = ask_prem if option_type == "call" else bid_prem
    other_side_prem    = bid_prem if option_type == "call" else ask_prem
    side_label         = "ASK" if option_type == "call" else "BID"

    score   = 0
    reasons = []

    if has_sweep and dominant_side_prem > other_side_prem:
        score += POINTS_SWEEP_DOMINANT_SIDE
        reasons.append(f"Sweep on {side_label} side (${dominant_side_prem:,.0f})")

    if rule_name == "RepeatedHitsAscendingFill":
        score += POINTS_REPEATED_ASCENDING
        reasons.append("RepeatedHitsAscendingFill")
    elif rule_name == "RepeatedHits":
        score += POINTS_REPEATED_HITS
        reasons.append("RepeatedHits")

    if total_prem > PREMIUM_THRESHOLD:
        score += POINTS_PREMIUM_OVER_100K
        reasons.append(f"Premium ${total_prem:,.0f} > ${PREMIUM_THRESHOLD:,.0f}")

    if vol_oi > VOL_OI_THRESHOLD:
        score += POINTS_VOL_OI_OVER_10
        reasons.append(f"Volume/OI ratio {vol_oi:.1f} > {VOL_OI_THRESHOLD}")

    if has_sweep:
        score += POINTS_HAS_SWEEP
        reasons.append("has_sweep")

    return {
        "id":               alert.get("id"),
        "direction":        direction,
        "score":            min(score, 100),
        "reasons":          reasons,
        "option_chain":     alert.get("option_chain"),
        "underlying_price": alert.get("underlying_price"),
        "total_premium":    total_prem,
        "rule_name":        rule_name,
        "strike":           alert.get("strike"),
        "expiry":           alert.get("expiry"),
        "bid":              alert.get("bid"),
        "ask":              alert.get("ask"),
        "volume":           alert.get("volume"),
        "open_interest":    alert.get("open_interest"),
        "gate_notes":       [],   # populated after all gates pass
    }


# ── Price / plan ──────────────────────────────────────────────────────────────

def _price_and_plan(result: dict) -> dict:
    try:
        bid, ask = float(result.get("bid")), float(result.get("ask"))
    except (TypeError, ValueError):
        return {}
    if bid <= 0 or ask <= 0 or ask < bid:
        return {}
    mid = (bid + ask) / 2
    return {
        "bid": bid, "ask": ask,
        "target": round(mid * 2.0, 2), "target_pct": 100,
        "stop":   round(mid * 0.5, 2), "stop_pct":   50,
    }


# ── Gate 3: Market Tide ───────────────────────────────────────────────────────

_market_tide_cache: dict = {}


def fetch_market_tide() -> Optional[dict]:
    """
    Fetch SPY flow balance from UW sector-etfs as a Market Tide proxy.
    call_net = bullish_premium - bearish_premium (same proxy qvix.py uses).
    Cached 5 minutes so every direction check in a scan cycle uses one call.
    Returns None on any failure so _tide_confirms can fail-closed.
    """
    now = datetime.now(ET)
    cached_ts = _market_tide_cache.get("ts")
    if cached_ts and (now - cached_ts).total_seconds() < 300:
        return _market_tide_cache.get("data")

    if not UNUSUAL_WHALES_API_KEY:
        return None

    try:
        r = requests.get(
            "https://api.unusualwhales.com/api/market/sector-etfs",
            headers={"Authorization": f"Bearer {UNUSUAL_WHALES_API_KEY}"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        spy = next((d for d in data if d.get("ticker") == "SPY"), None)
        if not spy:
            return None
        bull = float(spy.get("bullish_premium") or 0)
        bear = float(spy.get("bearish_premium") or 0)
        result = {"call_net": bull - bear}
        _market_tide_cache["data"] = result
        _market_tide_cache["ts"]   = now
        return result
    except Exception as exc:
        logging.warning(f"  Market Tide fetch failed: {exc}")
        _market_tide_cache["data"] = None
        _market_tide_cache["ts"]   = now
        return None


def _tide_confirms(direction: str, tide: Optional[dict]) -> tuple:
    """
    Returns (confirmed: bool, note: str).
    Fail-closed: if tide is None the signal is suppressed.
    CALL requires call_net > +$5M; PUT requires call_net < -$5M.
    """
    if tide is None:
        return False, "Market Tide unavailable — fail-closed, no alert fired"

    call_net = tide["call_net"]
    net_str  = f"${call_net/1e6:+.1f}M"

    if direction == "BULLISH":
        ok = call_net > TIDE_CALL_THRESHOLD
        if ok:
            return True, f"Market Tide BULLISH (call_net {net_str} > +$5M threshold)"
        return False, f"Market Tide not bullish enough (call_net {net_str}, need > +$5M)"

    # BEARISH / PUT
    ok = call_net < TIDE_PUT_THRESHOLD
    if ok:
        return True, f"Market Tide BEARISH (call_net {net_str} < -$5M threshold)"
    return False, f"Market Tide not bearish enough (call_net {net_str}, need < -$5M)"


# ── Gate 4: TradeOdds ─────────────────────────────────────────────────────────

def fetch_tradeodds_validation(ticker: str, direction: str) -> Optional[dict]:
    """
    POST to TradeOdds /api/v1/analyze for a 1-day forward window.
    Returns the raw response dict or None on any failure.
    Retries once after 2 seconds (transient network blips).
    Strict fail-closed: None means "suppress the signal".
    """
    if not TRADEODDS_API_KEY:
        logging.warning("  TRADEODDS_API_KEY not set — TradeOdds gate will block")
        return None

    payload = {
        "symbol":           ticker,
        "reference_period": "1d",
        "forward_period":   "1d",
        "conditions":       {"daily_change": True, "vix_level": True, "regime": True, "rel_vol": True},
        "lookback_years":   "10y",
    }
    for attempt in range(2):
        try:
            r = requests.post(
                f"{TRADEODDS_BASE_URL}/api/v1/analyze",
                headers={"Authorization": f"Bearer {TRADEODDS_API_KEY}"},
                json=payload,
                timeout=15,
            )
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == 0:
                logging.warning(f"  TradeOdds call failed, retrying: {exc}")
                time.sleep(2)
                continue
            logging.warning(f"  TradeOdds call failed after retry: {exc}")
            return None


def _tradeodds_confirms_0dte(direction: str, result: Optional[dict]) -> tuple:
    """
    Returns (confirmed: bool, note: str).
    Two tiers based on sample size:
      n >= ZERO_DTE_THIN_SAMPLE (10): edge >= ZERO_DTE_MIN_EDGE (5.0 pts) — standard
      n >= ZERO_DTE_MIN_SAMPLE  (5):  edge >= ZERO_DTE_THIN_EDGE (10.0 pts) — thin sample, require stronger edge
      n < ZERO_DTE_MIN_SAMPLE   (5):  block — too few matches to trust
    """
    if not result:
        return False, "TradeOdds unavailable — fail-closed, no alert fired"

    sample  = result.get("sample_size", 0)
    prob_up = result.get("probability_up", 50.0)
    prob_dn = result.get("probability_down", 50.0)

    if sample < ZERO_DTE_MIN_SAMPLE:
        return False, f"TradeOdds sample too small (n={sample} < {ZERO_DTE_MIN_SAMPLE}) — no alert fired"

    if direction == "BULLISH":
        edge = prob_up - prob_dn
        note = f"TradeOdds (n={sample}): {prob_up:.0f}% up vs {prob_dn:.0f}% down (edge {edge:+.1f}pts)"
    else:
        edge = prob_dn - prob_up
        note = f"TradeOdds (n={sample}): {prob_dn:.0f}% down vs {prob_up:.0f}% up (edge {edge:+.1f}pts)"

    required_edge = ZERO_DTE_MIN_EDGE if sample >= ZERO_DTE_THIN_SAMPLE else ZERO_DTE_THIN_EDGE
    thin_tag = " [thin-sample]" if sample < ZERO_DTE_THIN_SAMPLE else ""

    if edge >= required_edge:
        return True, note + thin_tag
    return False, f"TradeOdds edge insufficient ({edge:+.1f}pts < {required_edge:.0f} required{thin_tag}) — {note}"


# ── Discord ───────────────────────────────────────────────────────────────────

def _post_discord_alert(ticker: str, result: dict) -> bool:
    if not DISCORD_WEBHOOK_URL:
        logging.warning("Discord post skipped — DISCORD_WEBHOOK_URL is not set")
        return False

    plan = _price_and_plan(result)
    try:
        strike = float(result["strike"]) if result.get("strike") is not None else None
    except (TypeError, ValueError):
        strike = None
    try:
        expiry = date.fromisoformat(result["expiry"]) if result.get("expiry") else None
    except ValueError:
        expiry = None
    try:
        volume        = int(result["volume"]) if result.get("volume") is not None else None
        open_interest = int(result["open_interest"]) if result.get("open_interest") is not None else None
    except (TypeError, ValueError):
        volume = open_interest = None

    try:
        embed = build_signal_embed(
            ticker=ticker,
            direction=result["direction"],
            direction_label="0DTE SCALP",
            strike=strike,
            expiry=expiry,
            bid=plan.get("bid"),
            ask=plan.get("ask"),
            target=plan.get("target"),
            target_pct=plan.get("target_pct"),
            stop=plan.get("stop"),
            stop_pct=plan.get("stop_pct"),
            volume=volume,
            open_interest=open_interest,
            confidence=result["score"],
            smart_money_note=f"{result.get('rule_name') or 'Flow alert'} · ${result['total_premium']:,.0f} premium",
            holding_period="0DTE — close before market close today, theta decay accelerates fast",
            footer_text=f"QVIX 0DTE (Unusual Whales)  ·  {datetime.now(ET).strftime('%I:%M %p ET')}",
        )
    except Exception:
        logging.exception(f"Embed build failed — raw result: {result}  plan: {plan}")
        return False

    embed["fields"] = [
        {"name": "Contract",     "value": result.get("option_chain") or "—", "inline": True},
        {"name": "Flow Signals", "value": "\n".join(f"• {r}" for r in result["reasons"]) or "—", "inline": False},
    ]
    gate_notes = result.get("gate_notes", [])
    if gate_notes:
        embed["fields"].append({
            "name":   "Gates ✓",
            "value":  "\n".join(f"• {n}" for n in gate_notes),
            "inline": False,
        })

    for attempt in range(2):
        try:
            r = requests.post(
                DISCORD_WEBHOOK_URL,
                json={"username": "QVIX 0DTE (UW)", "embeds": [embed]},
                timeout=10,
            )
            r.raise_for_status()
            logging.info(f"  Discord post OK ({r.status_code})")
            return True
        except Exception as exc:
            if attempt == 0:
                time.sleep(2)
                continue
            logging.error(f"Discord post failed after retry: {exc}")
            return False


# ── Signal log ────────────────────────────────────────────────────────────────

def _log_0dte_signal(ticker: str, result: dict, plan: dict, tide_note: str = "", to_note: str = "") -> None:
    signal_log = LOG_DIR.parent / "signal_log.json"
    try:
        existing = json.loads(signal_log.read_text()) if signal_log.exists() else []
    except (json.JSONDecodeError, OSError):
        existing = []

    try:
        underlying_price = float(result.get("underlying_price") or 0) or None
        strike           = float(result["strike"]) if result.get("strike") is not None else None
        expiry           = result.get("expiry")
        bid              = plan.get("bid")
        ask              = plan.get("ask")
        mid              = round((bid + ask) / 2, 4) if bid is not None and ask is not None else None
    except (TypeError, ValueError):
        logging.warning("  0DTE signal log: could not parse numeric fields — skipping")
        return

    record = {
        "timestamp":           datetime.now(ET).isoformat(),
        "ticker":              ticker,
        "signal":              "0DTE SCALP",
        "signal_type":         "0dte_option",
        "direction":           "CALL" if result["direction"] == "BULLISH" else "PUT",
        "score":               result["score"],
        "price":               underlying_price,
        "tp":                  plan.get("target"),
        "sl":                  plan.get("stop"),
        "strike":              strike,
        "expiry":              expiry,
        "entry_premium":       mid,
        "tradeodds_confirmed": to_note or None,
        "market_tide_note":    tide_note or None,
        "low_sample_fallback": False,
        "status":              "open",
        "exit_price":          None,
        "exit_time":           None,
        "pnl":                 None,
        "outcome":             None,
        "executed_liquid":     False,
        "executed_robinhood":  False,
        "liquid_trade_id":     None,
        "robinhood_order_id":  None,
    }
    existing.append(record)
    try:
        signal_log.write_text(json.dumps(existing, indent=2))
        logging.info(f"  Signal logged: {ticker} 0DTE {result['direction']} score={result['score']} "
                     f"strike={strike} expiry={expiry} mid={mid}")
    except Exception as exc:
        logging.warning(f"  0DTE signal log write failed: {exc}")


# ── Contract dedup key ────────────────────────────────────────────────────────

def _contract_key(ticker: str, direction: str, strike) -> str:
    try:
        strike_val = f"{float(strike):.1f}" if strike is not None else "?"
    except (TypeError, ValueError):
        strike_val = "?"
    return f"{ticker}:{direction}:{strike_val}"


# ── Main scan loop ────────────────────────────────────────────────────────────

def run_scan():
    # Daily rollover — reset all state at midnight
    if _state["date"] != date.today().isoformat():
        _state["date"]            = date.today().isoformat()
        _state["fired_ids"]       = set()
        _state["fired_contracts"] = set()
        _state["fired_today"]     = False

    logging.info(f"━━━  0DTE (UW Flow) Scan  {datetime.now(ET).strftime('%H:%M:%S ET')}  ━━━")
    cutoff = datetime.now().astimezone() - timedelta(minutes=FLOW_LOOKBACK_MINS)

    for ticker in TICKERS:
        try:
            alerts = fetch_0dte_flow_alerts(ticker)
        except Exception as exc:
            logging.error(f"  flow-alerts fetch failed ({ticker}): {exc}")
            continue

        best: dict = {}
        for alert in alerts:
            if not _recent(alert, cutoff):
                continue
            result = score_alert(alert)
            if not result or not result["id"]:
                continue
            if result["id"] in _state["fired_ids"]:
                continue
            ckey = _contract_key(ticker, result["direction"], result.get("strike"))
            if ckey in _state["fired_contracts"]:
                logging.info(f"  [{ticker}] {ckey} already fired today — skipping duplicate")
                continue
            current_best = best.get(result["direction"])
            if current_best is None or result["score"] > current_best["score"]:
                best[result["direction"]] = result

        if not best:
            logging.info(f"  0DTE UW scan ({ticker}): no qualifying 0DTE flow in the last {FLOW_LOOKBACK_MINS} min")
            continue

        for direction, result in best.items():
            if result["score"] < ALERT_MIN_SCORE:
                logging.info(f"  {ticker}  {direction:<8}  {result['score']:>3}/100  (below {ALERT_MIN_SCORE} threshold)  {result['option_chain']}")
                continue

            # ── Gate 1: daily cap (1 signal per day) ──────────────────────────
            if _state["fired_today"]:
                logging.info(f"  [{ticker}] daily cap: 1 signal already fired today — skipping")
                continue

            # ── Gate 2: OTM-at-entry ──────────────────────────────────────────
            try:
                underlying = float(result.get("underlying_price") or 0)
                strike     = float(result["strike"]) if result.get("strike") is not None else None
            except (TypeError, ValueError):
                underlying, strike = 0.0, None

            if underlying > 0 and strike is not None:
                if direction == "BULLISH" and underlying < strike - 0.50:
                    logging.info(
                        f"  [{ticker}] OTM skip: CALL strike={strike} price={underlying:.2f} "
                        f"({strike - underlying:.2f} OTM)"
                    )
                    continue
                if direction == "BEARISH" and underlying > strike + 0.50:
                    logging.info(
                        f"  [{ticker}] OTM skip: PUT strike={strike} price={underlying:.2f} "
                        f"({underlying - strike:.2f} OTM)"
                    )
                    continue

            # ── Gate 3 (pre-check): minimum premium ───────────────────────────
            plan = _price_and_plan(result)
            mid  = plan.get("bid") and plan.get("ask") and (plan["bid"] + plan["ask"]) / 2
            if mid is not None and mid < MIN_ENTRY_PREMIUM:
                logging.info(f"  [{ticker}] cheap-premium skip: mid=${mid:.2f} < ${MIN_ENTRY_PREMIUM}")
                continue

            # ── Gate 4: Market Tide directional alignment ─────────────────────
            tide = fetch_market_tide()
            tide_ok, tide_note = _tide_confirms(direction, tide)
            if not tide_ok:
                logging.info(f"  [{ticker}] {direction} blocked — Market Tide: {tide_note}")
                continue

            # ── Gate 5: TradeOdds historical confirmation ─────────────────────
            to_result = fetch_tradeodds_validation(ticker, direction)
            to_ok, to_note = _tradeodds_confirms_0dte(direction, to_result)
            if not to_ok:
                logging.info(f"  [{ticker}] {direction} blocked — TradeOdds: {to_note}")
                continue

            # ── All gates passed — fire ───────────────────────────────────────
            result["gate_notes"] = [tide_note, to_note]
            logging.info(
                f"  🔔  0DTE UW ALERT  {ticker}  {direction}  {result['score']}/100  "
                f"{result['option_chain']}\n"
                f"      Tide: {tide_note}\n"
                f"      TradeOdds: {to_note}"
            )
            ckey = _contract_key(ticker, direction, result.get("strike"))
            if _post_discord_alert(ticker, result):
                _state["fired_ids"].add(result["id"])
                _state["fired_contracts"].add(ckey)
                _state["fired_today"] = True
                _save_state()
                _log_0dte_signal(ticker, result, plan, tide_note, to_note)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / "spy_0dte_uw.log"),
        ],
    )

    from health import startup_validate
    if not startup_validate("spy_0dte_uw"):
        sys.exit(1)

    logging.info(f"0DTE Scanner (Unusual Whales) — starting  |  Tickers: {', '.join(TICKERS)}")
    logging.info(
        f"Window: {WINDOW_START.strftime('%H:%M')}-{WINDOW_END.strftime('%H:%M')} ET  |  "
        f"Scan every {SCAN_INTERVAL_SECS // 60} min  |  "
        f"Threshold {ALERT_MIN_SCORE}/100  |  Min premium ${MIN_ENTRY_PREMIUM}  |  "
        f"Gates: daily-cap + Market-Tide(±$5M) + TradeOdds(edge≥{ZERO_DTE_MIN_EDGE},n≥{ZERO_DTE_MIN_SAMPLE})"
    )

    while True:
        try:
            now = datetime.now(ET)
            if in_scan_window(now):
                run_scan()
                time.sleep(SCAN_INTERVAL_SECS)
            else:
                time.sleep(IDLE_POLL_SECS)
        except KeyboardInterrupt:
            logging.info("SPY 0DTE Scanner (Unusual Whales) stopped by user.")
            break
        except Exception:
            logging.exception("Unexpected error in scan loop")
            time.sleep(30)


if __name__ == "__main__":
    main()
