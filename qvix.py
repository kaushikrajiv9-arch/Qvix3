#!/usr/bin/env python3
"""
QVIX 5.1 — Autonomous Trading Intelligence System

Stocks    : 54 tickers, scanned every 5 minutes during market hours (9:30–4:00 ET).
Crypto    : 8 assets (BTC ETH SOL BNB XRP DOGE ADA AVAX), scanned every 5 minutes 24/7.
0DTE      : SPY opening-range scalp scanner, fires once daily ~9:35–9:50 ET.
Pre-Market: briefing ~9:00 ET + opening bell ~9:30 ET, runs 8:00–9:33 ET.
Signals   : MOMENTUM BREAKOUT | BEARISH PUT | MARKET CRASH ALERT | 0DTE SCALP | PRE-MARKET BRIEFING | OPENING BELL

Scoring: each signal is evaluated 0–100.  Alerts fire at >= ALERT_MIN_SCORE.
Decisions (all scores + full reasoning) are appended to logs/qvix_decisions.jsonl.
"""

import os
import socket
import sys
import time
import json
import logging
import threading
from datetime import datetime, date, timedelta, timezone, time as dtime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np
import requests
from dotenv import load_dotenv

from discord_format import build_signal_embed

load_dotenv()

# Code modification timestamp — included in startup notice so a stale daemon
# (file modified but process not restarted) is immediately visible in Discord.
_QVIX_MTIME = datetime.fromtimestamp(
    Path(__file__).stat().st_mtime, tz=ZoneInfo("America/New_York")
).strftime("%Y-%m-%d %H:%M ET")

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────

POLYGON_API_KEY     = os.getenv("POLYGON_API_KEY", "")
DISCORD_WEBHOOK_URL        = os.getenv("DISCORD_WEBHOOK_URL", "")
DISCORD_CRYPTO_WEBHOOK_URL = os.getenv("DISCORD_CRYPTO_WEBHOOK_URL", "")
TRADEODDS_API_KEY   = os.getenv("TRADEODDS_API_KEY", "")
ANTHROPIC_API_KEY      = os.getenv("ANTHROPIC_API_KEY", "")
UNUSUAL_WHALES_API_KEY = os.getenv("UNUSUAL_WHALES_API_KEY", "")
ROBINHOOD_ACCESS_TOKEN = os.getenv("ROBINHOOD_ACCESS_TOKEN", "")
TWILIO_ACCOUNT_SID     = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN      = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_TO              = os.getenv("TWILIO_TO", "")       # your cell — e.g. +15551234567
TWILIO_FROM            = "+17078270001"
TRADEODDS_BASE_URL     = "https://tradeodds-production.up.railway.app"
RH_BASE                = "https://api.robinhood.com"

# Edit this list to match your actual tickers
WATCHLIST = [
    # Core
    "SPY",  "QQQ",  "IWM",  "DIA",  "AAPL", "NVDA", "TSLA", "MSFT", "META",
    "AMZN", "GOOGL", "AMD", "PLTR", "COIN", "MSTR", "HOOD", "SOFI", "NIO",
    "RIVN", "LCID", "SMCI", "IONQ", "RKLB", "MARA", "RIOT", "HUT",  "CLSK",
    "BTBT", "WULF", "APLD", "CORZ", "IREN",

    # Physical AI Infrastructure (CALL bias) — CORZ, IREN already listed above
    "CEG", "VST", "BE", "CRWV", "NRG", "VRT", "ETN",

    # Semi Hedge (PUT bias) — AMD already listed above
    "SMH", "AVGO", "MU", "TSM",

    # Commodities
    "GLD", "SLV", "COPX", "URA", "USO", "FCX",

    # Physical AI Materials
    "MP", "ON", "NXP", "TER", "AMBA", "CGNX",

    # Space/Pre-IPO
    "AMKR", "SPCX",
]

# Maps each watchlist ticker to its SPDR sector ETF for the sector flow gate.
# Tickers not in this map (indexes, ETFs, idiosyncratic names) skip the gate.
TICKER_SECTOR: dict[str, str] = {
    # Technology (XLK)
    "AAPL": "XLK", "NVDA": "XLK", "MSFT": "XLK", "AMD": "XLK", "PLTR": "XLK",
    "SMCI": "XLK", "IONQ": "XLK", "AVGO": "XLK", "MU": "XLK", "TSM": "XLK",
    "SMH":  "XLK", "ON":   "XLK", "NXP":  "XLK", "TER": "XLK", "AMBA": "XLK",
    "CGNX": "XLK", "AMKR": "XLK", "CRWV": "XLK",
    # Crypto miners trade as tech/speculative
    "MARA": "XLK", "RIOT": "XLK", "HUT":  "XLK", "CLSK": "XLK",
    "BTBT": "XLK", "WULF": "XLK", "APLD": "XLK", "CORZ": "XLK", "IREN": "XLK",
    # Communication Services (XLC)
    "META": "XLC", "GOOGL": "XLC",
    # Consumer Discretionary (XLY)
    "TSLA": "XLY", "AMZN": "XLY", "NIO": "XLY", "RIVN": "XLY", "LCID": "XLY",
    # Financials (XLF)
    "COIN": "XLF", "HOOD": "XLF", "SOFI": "XLF", "MSTR": "XLF",
    # Utilities / Power Generation (XLU)
    "CEG": "XLU", "VST": "XLU", "BE": "XLU", "NRG": "XLU",
    # Industrials — data center power & cooling infrastructure
    "VRT": "XLI", "ETN": "XLI",
    # Energy (XLE)
    "USO": "XLE",
    # Basic Materials (XLB) — metals, mining, commodities
    "FCX": "XLB", "MP": "XLB", "COPX": "XLB",
    # Space/Aerospace → Industrials (XLI)
    "RKLB": "XLI", "SPCX": "XLI",
    # Commodities ETFs — GLD/SLV/URA have no sector; leave unmapped → gate skips
}

CRYPTO_WATCHLIST = ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX"]
# Coinbase Advanced Trade v3 product IDs — public API, no auth required.
# CRYPTO_SYMBOLS kept for any code that still references it; it now matches
# Coinbase's "{sym}-USD" convention (which is the same as the old UW format).
CRYPTO_SYMBOLS   = {sym: f"{sym}-USD" for sym in CRYPTO_WATCHLIST}
COINBASE_BASE_URL = "https://api.coinbase.com/api/v3/brokerage/market"

ET                       = ZoneInfo("America/New_York")
SCAN_INTERVAL_SECS       = 300    # 5 minutes between scans
ALERT_MIN_SCORE              = 72  # Score threshold to fire Discord alert (BEARISH PUT, 0DTE, BIG DROP BOUNCE)
CRYPTO_ALERT_MIN_SCORE       = 62  # Lower bar for crypto — 24/7 market, wider scoring spread
MOMENTUM_BREAKOUT_MIN_SCORE  = 78  # floor: below this, breakout is too weak to act on
MOMENTUM_BREAKOUT_MAX_SCORE  = 85  # ceiling: above this, setup is overbought not high-conviction (90-100 range: 20% WR, -197% net)
MULTI_AGENT_SCORE_THRESHOLD  = 75  # At/above this score, run full multi-agent pipeline instead of basic alert
ALERT_COOLDOWN_SECS      = 86400  # 24-hour cooldown for crypto (runs 24/7 across midnight)
STOCK_COOLDOWN_SECS      = 43200  # 12-hour cooldown for stocks + calendar-day gate (belt-and-suspenders)
BDB_SUSPENDED            = False  # re-enabled 2026-07-31: rebuilt with 10:30+ window, ticker exclusions, TradeOdds gate
BDB_EXCLUDED_TICKERS     = {"TER", "TSM", "AMBA", "ON", "URA"}  # low-liquidity names where OI ratio is unreliable
REQUEST_DELAY_SECS       = 13.0   # 5 calls/min free tier → 12 s min gap; only used for once-daily history cache
CRASH_MIN_SCORE          = 60     # Lower bar for market-wide alerts

# ── Unusual Activity Radar ─────────────────────────────────────────────────────
# Starting thresholds — loosen/tighten after watching live output for a few days.
# Dashboard (loose): anything ≥ 2× rel-vol AND ≥ 40 IV rank → broad discovery.
# Discord  (strict): 3×/75/$10M combo, hard cap 5/day, 2h per-ticker cooldown.
RADAR_MIN_REL_VOL           = 2.0         # 2× 30d avg vol     → dashboard panel
RADAR_MIN_IV_RANK           = 40          # IV rank ≥ 40        → dashboard panel
RADAR_DISCORD_REL_VOL       = 3.0         # 3× vol              → Discord candidate
RADAR_DISCORD_IV_RANK       = 75          # IV rank ≥ 75        → Discord candidate
RADAR_DISCORD_NET_PREM      = 10_000_000  # |net premium| ≥ $10M → Discord candidate
RADAR_MAX_DISCORD_DAY       = 5           # hard cap per calendar day
RADAR_DISCORD_COOLDOWN_SECS = 7200        # 2-hour per-ticker cooldown

# Outcome tracking for crypto spot signals — no option contract exists, so
# wins/losses are measured as a % move in the signal direction.
CRYPTO_TARGET_PCT = 10.0   # % price move in direction = win
CRYPTO_STOP_PCT   = 5.0    # % price move against direction = loss

# TradeOdds validation gate for the regular per-ticker stock signals
# (MOMENTUM BREAKOUT / BEARISH PUT) — checked as the LAST gate, only once
# score + volume + cooldown already passed, so it's at most one paid call
# per qualifying signal rather than one per evaluation.
STOCK_TRADEODDS_MIN_PROB    = 55.0  # absolute probability required in the signal's own direction
STOCK_TRADEODDS_MIN_SAMPLE  = 10    # minimum historical sample size to trust the read
STOCK_TRADEODDS_FORWARD     = "5d"  # swing horizon, vs. 0DTE's 1d
STOCK_TRADEODDS_REFERENCE   = "1d"
TRADEODDS_OUTAGE_GRACE_MINUTES = 10.0  # short outages still hard-block; sustained ones beyond this fire unconfirmed instead of going dark

# SPY 0DTE morning scanner — fires once per trading day off the opening
# 5-minute range, validated against TradeOdds before it's allowed to alert.
ZERO_DTE_TICKER          = "SPY"            # kept for logging/startup notice references
ZERO_DTE_TICKERS         = ["SPY", "QQQ"]  # all tickers the 0DTE scanner evaluates each morning
ZERO_DTE_FIRE_TIME       = dtime(9, 35)   # earliest time the opening range is complete
ZERO_DTE_FIRE_WINDOW_END = dtime(9, 50)   # miss this and the setup is stale — skip for the day
ZERO_DTE_CLOSE_TIME      = dtime(15, 30)  # reminder text only; scanner doesn't auto-close positions
ZERO_DTE_MIN_VOL_RATIO   = 1.3            # opening 5-min volume vs expected pace, to count as a "surge"
ZERO_DTE_MIN_EDGE        = 5.0            # TradeOdds probability-point edge required to confirm direction
ZERO_DTE_MIN_SAMPLE      = 5              # minimum TradeOdds historical sample size to trust the read
ZERO_DTE_THIN_SAMPLE     = 10            # below this count, ZERO_DTE_THIN_EDGE applies instead
ZERO_DTE_THIN_EDGE       = 10.0          # higher edge requirement for thin-sample (5-9 matches)

# Intraday position management for the day's already-fired 0DTE signal —
# each checked every 5-minute scan cycle during market hours, each fires at
# most once per day. CLOSE, TARGET, and REVERSAL are all measured from the
# entry price (SPY's price when the morning signal fired): REVERSAL at a
# pure sign-flip (any move against, however small), CLOSE once that move
# against reaches this %, TARGET once the move in favor reaches this %.
ZERO_DTE_CLOSE_PCT       = 0.5            # % move AGAINST the signal, from entry, to fire CLOSE POSITION
ZERO_DTE_TARGET_PCT      = 1.0            # % move IN FAVOR of the signal, from entry, to fire TARGET HIT

# Pre-market scanner — runs 8:00-9:33 ET, two one-shot-per-day alerts: a
# BRIEFING sharp at 9:00 and an OPENING BELL sharp at 9:30. Reuses the same
# WATCHLIST + fetch_all_snapshots() the regular scan already calls — no new
# continuous polling, just the existing batch snapshot at an earlier hour.
PREMARKET_START          = dtime(8, 0)    # window opens — before this, nothing to do
PREMARKET_EARLY_TIME     = dtime(8, 0)    # early price flash fire time
PREMARKET_EARLY_END      = dtime(8, 5)    # miss this and the early flash is stale — skip for today
PREMARKET_BRIEFING_TIME  = dtime(9, 0)    # briefing's earliest fire time
PREMARKET_BRIEFING_END   = dtime(9, 5)    # miss this and the briefing is stale — skip for today
PREMARKET_BELL_TIME      = dtime(9, 30)   # opening bell's earliest fire time
PREMARKET_BELL_END       = dtime(9, 33)   # miss this and the bell is stale — skip for today
PREMARKET_TOP_N          = 5              # tickers covered in the briefing
PREMARKET_BELL_TOP_N     = 3              # highest-conviction tickers covered in the opening bell
PREMARKET_GAP_MIN_PCT    = 0.5            # minimum |gap%| vs prior close to count as a "mover" at all
PREMARKET_EARLY_TOP_N    = 15             # stocks shown in the early price flash
PREMARKET_EARLY_MIN_PCT  = 0.2            # lower bar — show more tickers in early flash

# Daily summary — fires once at market close, same one-shot-per-day gate as the briefing/bell
DAILY_SUMMARY_TIME = dtime(16, 15)   # earliest fire time
DAILY_SUMMARY_END  = dtime(16, 30)   # miss this window → skip for today

# EOD 0DTE force-close reminder — fires once at 3:44 PM to catch expiring positions
EOD_CLOSER_TIME    = dtime(15, 44)
EOD_CLOSER_END     = dtime(15, 50)

LOG_DIR              = Path("logs")
DECISION_LOG         = LOG_DIR / "qvix_decisions.jsonl"   # one JSON object per line
RUNTIME_LOG          = LOG_DIR / "qvix_runtime.log"
LOCK_FILE            = LOG_DIR / "qvix.pid"               # single-instance guard
def _dte_state_file(ticker: str): return LOG_DIR / f"qvix_zero_dte_state_{ticker}.json"
COOLDOWN_STATE_FILE       = LOG_DIR / "qvix_cooldowns.json"         # persists per-(ticker,signal) cooldowns across restarts
PREMARKET_STATE_FILE      = LOG_DIR / "qvix_premarket_state.json"   # persists today's briefing/bell fire-state across restarts
ROBINHOOD_ENRICHMENT_FILE = LOG_DIR / "robinhood_enrichment.json"   # written by robinhood_bridge.py every 5 min
LIQUID_POSITIONING_FILE   = LOG_DIR / "liquid_positioning.json"     # written by liquid_bridge.py every 5 min
SIGNAL_LOG_FILE        = Path("signal_log.json")             # every fired signal, open/closed, for outcome tracking
CIRCUIT_BREAKER_FILE   = Path("circuit_breaker.json")        # {"starting_equity": <float>} — must be seeded manually, never fabricated
UNUSUAL_ACTIVITY_FILE  = Path("unusual_activity.json")       # live radar output for dashboard panel
CIRCUIT_BREAKER_MAX_DAILY_LOSS_PCT = 5.0
SIGNAL_EXPIRY_HOURS = 72   # crypto signals (tp=sl=None, no option contract) auto-expire after this long — they can never resolve via the premium check

# Tickers with a structural long bias — never fire BEARISH PUT on these.
# They tend to be high-volatility speculative names whose bear signals are
# extremely noisy and whose primary risk is to the upside, not the downside.
LONG_BIAS_TICKERS = {"ASTS", "RKLB", "LUNR", "RCAT", "IREN", "IONQ"}

# Data-driven BP exclusions (2026-07-27): 0% WR across all resolved signals.
# QQQ 0% WR -207%, NVDA 0% WR -113%, GOOGL 0% WR -40%.
# TradeOdds confirmed: NVDA is 2 of 3 confirmed-but-losing BP signals; removing
# it cleans the confirmed bucket to RIOT +53% / TSLA +55% — both wins.
BEARISH_PUT_BLOCKED: set = {"QQQ", "NVDA", "GOOGL"}


# ─── TECHNICAL INDICATORS ──────────────────────────────────────────────────────

def _ema(prices: np.ndarray, period: int) -> np.ndarray:
    """Exponential moving average (full series)."""
    k = 2.0 / (period + 1)
    out = np.empty_like(prices, dtype=float)
    out[0] = prices[0]
    for i in range(1, len(prices)):
        out[i] = prices[i] * k + out[i - 1] * (1 - k)
    return out


def calc_rsi(prices: np.ndarray, period: int = 14) -> float:
    """Wilder smoothed RSI. Returns 50.0 if not enough data."""
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices.astype(float))
    gains  = np.maximum(deltas, 0.0)
    losses = np.abs(np.minimum(deltas, 0.0))
    ag, al = np.mean(gains[:period]), np.mean(losses[:period])
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    return 100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al)


def calc_macd(prices: np.ndarray, fast=12, slow=26, signal=9) -> tuple:
    """Returns (macd_line, signal_line, histogram). Zeros if not enough data."""
    if len(prices) < slow + signal:
        return 0.0, 0.0, 0.0
    p = prices.astype(float)
    ml = _ema(p, fast) - _ema(p, slow)
    sl = _ema(ml, signal)
    return float(ml[-1]), float(sl[-1]), float(ml[-1] - sl[-1])


def calc_vwap(bars: list) -> float:
    """VWAP from a list of OHLCV bar dicts."""
    if not bars:
        return 0.0
    pv  = sum((b["h"] + b["l"] + b["c"]) / 3.0 * b["v"] for b in bars)
    vol = sum(b["v"] for b in bars)
    return pv / vol if vol else 0.0


# ─── POLYGON.IO DATA LAYER ──────────────────────────────────────────────────────

def _poly_get(url: str, params: dict) -> dict:
    params["apiKey"] = POLYGON_API_KEY
    for attempt in range(3):
        r = requests.get(url, params=params, timeout=12)
        if r.status_code == 429:
            delay = 2 ** attempt * 2  # 2s, 4s, 8s
            logging.warning(f"  Polygon 429 rate-limit (attempt {attempt + 1}/3) — retrying in {delay}s")
            time.sleep(delay)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


def fetch_daily(ticker: str, lookback_days: int = 70) -> list:
    """Fetch adjusted daily OHLCV bars (~65 trading days of history)."""
    end   = date.today()
    start = end - timedelta(days=lookback_days + 25)   # buffer for weekends/holidays
    data  = _poly_get(
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}",
        {"adjusted": "true", "sort": "asc", "limit": 150},
    )
    return data.get("results", [])


def _fetch_daily_with_retry(ticker: str, retries: int = 1, backoff_secs: float = 5.0) -> list:
    """
    fetch_daily with a couple of retries on transient failures (read timeouts,
    momentary network hiccups). The daily cache only refreshes once per
    calendar day, so without a retry here a single transient error permanently
    blanks that ticker out of every scan for the rest of the day even though
    a retry moments later would have succeeded.
    """
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return fetch_daily(ticker)
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff_secs)
    raise last_exc


# ─── DAILY HISTORY CACHE ───────────────────────────────────────────────────────
# Historical bars (RSI, MACD, 20D high/low, avg volume) are fetched once per
# trading day with REQUEST_DELAY_SECS between calls.  Every 5-minute scan then
# uses ONE batch snapshot call instead of 62 individual ticker calls.

_daily_cache: dict = {}
_cache_date: Optional[date] = None


def refresh_daily_cache():
    """Fetch 70-day daily bars for every watchlist ticker. Called once per trading day."""
    global _daily_cache, _cache_date
    logging.info(
        f"Refreshing daily history cache ({len(WATCHLIST)} tickers, "
        f"~{len(WATCHLIST) * REQUEST_DELAY_SECS / 60:.1f} min at free-tier rate)…"
    )
    new_cache: dict = {}
    for i, ticker in enumerate(WATCHLIST):
        try:
            new_cache[ticker] = _fetch_daily_with_retry(ticker)
        except Exception as exc:
            logging.error(f"  {ticker}: daily cache error: {exc}")
            new_cache[ticker] = _daily_cache.get(ticker, [])
        if i < len(WATCHLIST) - 1:
            time.sleep(REQUEST_DELAY_SECS)
    _daily_cache = new_cache
    _cache_date  = date.today()
    ok = sum(bool(v) for v in _daily_cache.values())
    logging.info(f"Daily cache ready — {ok}/{len(WATCHLIST)} tickers loaded\n")


def fetch_opening_range_bars(ticker: str, day: date) -> list:
    """
    The first five 1-minute bars of the regular session (9:30-9:34 ET
    inclusive, i.e. the 9:30:00-9:35:00 window) for `day`.

    Polygon's range-aggs endpoint accepts from/to as either a date string
    (which defaults to the start of that calendar day — 4:00 AM ET
    pre-market, not 9:30) or an explicit millisecond timestamp. Passing
    date-only bounds with sort=asc + a small limit was the actual bug here:
    with ~5.5 hours of pre-market minute bars ahead of the regular open, a
    limit of 15 only ever returned 4:00-4:14 AM and never reached 9:30 —
    confirmed live by comparing a date-bounded call (returns 4:00 AM bars)
    against an explicit-timestamp call for this exact window (returns the
    correct 9:30-9:35 bars immediately). Bounding the request itself to the
    opening window — rather than fetching the whole day and filtering after
    the fact — fixes it directly.
    """
    start_ms = int(datetime.combine(day, dtime(9, 30), tzinfo=ET).timestamp() * 1000)
    end_ms   = int(datetime.combine(day, dtime(9, 35), tzinfo=ET).timestamp() * 1000)
    data = _poly_get(
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start_ms}/{end_ms}",
        {"adjusted": "true", "sort": "asc", "limit": 15},
    )
    bars = data.get("results", [])
    out = []
    for b in bars:
        ts = datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc).astimezone(ET)
        if dtime(9, 30) <= ts.time() < dtime(9, 35):
            out.append(b)
    return out


def fetch_all_snapshots() -> dict:
    """ONE Polygon API call — batch snapshot for all watchlist tickers."""
    data = _poly_get(
        "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers",
        {"tickers": ",".join(WATCHLIST)},
    )
    return {item["ticker"]: item for item in data.get("tickers", [])}


def fetch_ticker_news(ticker: str) -> Optional[dict]:
    """
    Most recent news headline for a ticker, for the pre-market briefing's
    catalyst summary. Returns None on any failure (no results, network
    error, plan restriction) — the briefing must say "no catalyst found"
    rather than fabricate one.
    """
    try:
        data = _poly_get(
            "https://api.polygon.io/v2/reference/news",
            {"ticker": ticker, "limit": 1, "order": "desc", "sort": "published_utc"},
        )
        results = data.get("results", [])
        if not results:
            return None
        r = results[0]
        return {
            "title":     r.get("title"),
            "publisher": r.get("publisher", {}).get("name"),
            "url":       r.get("article_url"),
        }
    except Exception as exc:
        logging.warning(f"  {ticker}: news fetch failed: {exc}")
        return None


# ─── CRYPTO DATA LAYER ─────────────────────────────────────────────────────────

_crypto_daily_cache: dict = {}
_crypto_cache_date: Optional[date] = None


def _fetch_coinbase_candles(product_id: str, granularity: str, limit: int) -> list:
    """
    Fetch OHLCV candles from the Coinbase Advanced Trade v3 public API.
    No authentication required. Returns candles newest-first (index 0 is
    the still-forming current candle), each a dict with numeric fields:
      {"start": <unix_int>, "open": str, "high": str,
       "low": str, "close": str, "volume": str}

    Raises on HTTP error so callers can fall back to cached data — never
    swallows failures silently.
    """
    r = requests.get(
        f"{COINBASE_BASE_URL}/products/{product_id}/candles",
        params={"granularity": granularity, "limit": limit},
        timeout=12,
    )
    r.raise_for_status()
    return r.json().get("candles", [])


def _crypto_day_elapsed_fraction(candle_start_unix: Optional[int]) -> float:
    """
    Fraction of the still-forming UTC daily candle that has elapsed, clamped
    to [0.05, 1.0] — same convention as session_elapsed_fraction() for stocks.

    Coinbase's ONE_DAY candle always starts at UTC midnight (candle_start_unix
    is a Unix epoch integer). We compute elapsed time against the current UTC
    clock, which is more accurate than relying on the API to supply a separate
    "last updated" timestamp field.

    Without this scaling, current_vol is a partial day being compared to a
    full-day 20D average baseline, which reads as a volume drought by
    construction for most of the day — not weak conviction. Falls back to 1.0
    if the start timestamp is missing or unparseable, so a malformed response
    degrades gracefully rather than raising.
    """
    try:
        start_ts = int(candle_start_unix)
    except (TypeError, ValueError):
        return 1.0
    elapsed_secs = datetime.now(timezone.utc).timestamp() - start_ts
    if elapsed_secs <= 0:
        return 0.05
    return max(0.05, min(1.0, elapsed_secs / 86400.0))


def refresh_crypto_daily_cache():
    """
    Fetch 300 daily candles per crypto asset from the Coinbase Advanced Trade
    v3 public API (no auth required). Called once per calendar day.

    300 candles gives ~10 months of daily history, up from the previous 70-day
    UW feed — better RSI(14), MACD(12,26,9), and 20-day high/low accuracy.
    Candles come newest-first; reversed here to oldest-first so scoring
    functions can use standard slice/index conventions.
    """
    global _crypto_daily_cache, _crypto_cache_date

    logging.info(f"Refreshing crypto daily history cache ({len(CRYPTO_WATCHLIST)} assets, Coinbase)…")
    new_cache: dict = {}
    for sym in CRYPTO_WATCHLIST:
        product_id = f"{sym}-USD"
        try:
            candles = _fetch_coinbase_candles(product_id, granularity="ONE_DAY", limit=300)
            # Coinbase returns newest-first — reverse to chronological order
            # (oldest first) matching eval_momentum_breakout/eval_bearish_put.
            bars = [
                {
                    "o": float(c["open"]),  "h": float(c["high"]),
                    "l": float(c["low"]),   "c": float(c["close"]),
                    "v": float(c.get("volume") or 0),
                }
                for c in reversed(candles)
            ]
            new_cache[sym] = bars
        except Exception as exc:
            logging.error(f"  {sym}: crypto daily cache error: {exc}")
            new_cache[sym] = _crypto_daily_cache.get(sym, [])

    _crypto_daily_cache = new_cache
    _crypto_cache_date  = date.today()
    ok = sum(bool(v) for v in _crypto_daily_cache.values())
    logging.info(f"Crypto cache ready — {ok}/{len(CRYPTO_WATCHLIST)} assets loaded\n")


def fetch_crypto_snapshots() -> dict:
    """
    Live "today so far" OHLCV per crypto asset from the Coinbase Advanced
    Trade v3 public API (no auth required). Fetched fresh every 5-minute scan.

    The ONE_DAY candle with limit=1 returns the still-forming today bar:
    volume accumulates throughout the day, and close is the most recent trade
    price. The candle's "start" field (UTC midnight Unix timestamp) lets
    _crypto_day_elapsed_fraction() scale the volume comparison correctly so
    partial-day volume isn't compared against a full-day baseline.
    """
    snapshots: dict = {}
    today = datetime.now(ET).date()
    for sym in CRYPTO_WATCHLIST:
        product_id = f"{sym}-USD"
        try:
            candles = _fetch_coinbase_candles(product_id, granularity="ONE_DAY", limit=1)
            if not candles:
                continue
            bar = candles[0]
            snapshots[sym] = {
                "day": {
                    "o": float(bar["open"]),  "h": float(bar["high"]),
                    "l": float(bar["low"]),   "c": float(bar["close"]),
                    "v": float(bar.get("volume") or 0),
                },
                "lastTrade": {"p": float(bar["close"]), "asof": today},
                "day_elapsed_fraction": _crypto_day_elapsed_fraction(
                    bar.get("start")
                ),
            }
        except Exception as exc:
            logging.warning(f"  {sym}: Coinbase crypto snapshot fetch failed: {exc}")
    return snapshots


# ─── TRADEODDS VALIDATION ───────────────────────────────────────────────────────
# Independent historical check used to gate the SPY 0DTE scanner — a local
# opening-range signal only fires if TradeOdds' 10-year pattern history also
# favors that direction. Called once per trading day (only after the local
# direction/momentum/volume checks already pass) to keep compute-credit spend
# minimal.

# Tracks how long TradeOdds has been continuously failing — a 2-attempt/
# 2-second retry absorbs a brief blip, but a real ~46-minute outage (seen
# live: 09:04-09:50 ET one morning, 87 signals silently blocked) outlasts
# that completely. first_failure_time marks the start of the CURRENT
# unbroken streak of failures; a single success resets it.
_tradeodds_outage_state = {"first_failure_time": None}

# Unusual Whales net-impact cache — refreshed once per scan cycle, shared across
# all ticker evaluations so we make exactly one API call per 5-minute window.
_uw_net_impact_cache: dict = {"data": None, "ts": None}

# Unusual Whales market tide cache — same refresh pattern.
_market_tide_cache:  dict = {"data": None, "ts": None}
_vix_cache:          dict = {"level": None, "ts": None}
_sector_flow_cache:  dict = {"data": None, "ts": None}

# Unusual Activity Radar — Discord cooldowns, completely isolated from _cooldowns
# so radar pings never interact with signal-generation logic.
_radar_cooldowns: dict    = {}    # (ticker, date) → datetime of last Discord ping
_radar_discord_date: str  = ""    # tracks calendar day for daily cap reset
_radar_discord_count: int = 0     # pings fired today


def _note_tradeodds_failure():
    if _tradeodds_outage_state["first_failure_time"] is None:
        _tradeodds_outage_state["first_failure_time"] = datetime.now(ET)


def _note_tradeodds_recovery():
    """No-op unless an outage was actually being tracked. Announces recovery to Discord with the outage's real duration."""
    first_failure = _tradeodds_outage_state["first_failure_time"]
    if first_failure is None:
        return
    outage_minutes = (datetime.now(ET) - first_failure).total_seconds() / 60.0
    _tradeodds_outage_state["first_failure_time"] = None
    logging.info(f"  TradeOdds recovered after {outage_minutes:.0f} minutes")
    _post_embed({
        "title":       "✅  TradeOdds Recovered",
        "description": f"TradeOdds was unavailable for **{outage_minutes:.0f} minutes** — historical validation is back online.",
        "color":       0x00C851,
        "timestamp":   datetime.utcnow().isoformat() + "Z",
    })


def _tradeodds_outage_minutes() -> float:
    """0.0 if not currently in a tracked outage, else minutes since the first consecutive failure."""
    first_failure = _tradeodds_outage_state["first_failure_time"]
    if first_failure is None:
        return 0.0
    return (datetime.now(ET) - first_failure).total_seconds() / 60.0


def fetch_tradeodds_validation(symbol: str, conditions: dict, forward_period: str = "1d",
                                reference_period: str = "1d", lookback_years: str = "10y") -> Optional[dict]:
    """
    POST to TradeOdds /api/v1/analyze. Returns None on any failure (missing
    key, network error, non-2xx) rather than a fabricated result — callers
    must treat None as "could not validate," not as agreement or disagreement.

    Retries once after a short backoff before giving up — same pattern as
    _post_embed's Discord retry. A brief outage recovering within a couple
    seconds is exactly what one retry is for. Extended outages (beyond what
    a 2-second retry can absorb) are tracked separately via
    _note_tradeodds_failure/_note_tradeodds_recovery so callers can fall
    back to firing unconfirmed instead of going dark for the whole incident
    — see _stock_tradeodds_confirms.
    """
    if not TRADEODDS_API_KEY:
        logging.warning("  TRADEODDS_API_KEY not set — skipping historical validation")
        return None

    payload = {
        "symbol":           symbol,
        "reference_period": reference_period,
        "forward_period":   forward_period,
        "conditions":       conditions,
        "lookback_years":   lookback_years,
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
            _note_tradeodds_recovery()
            return r.json()
        except Exception as exc:
            if attempt == 0:
                logging.warning(f"  TradeOdds validation call failed, retrying once: {exc}")
                time.sleep(2)
                continue
            logging.warning(f"  TradeOdds validation call failed after retry: {exc}")
            _note_tradeodds_failure()
            return None


def _tradeodds_confirms(direction: str, result: Optional[dict]) -> tuple:
    """
    True only if TradeOdds' historical sample is large enough to trust AND
    favors `direction` by enough probability points.
    Two tiers: n >= ZERO_DTE_THIN_SAMPLE uses ZERO_DTE_MIN_EDGE (5pts);
    ZERO_DTE_MIN_SAMPLE <= n < ZERO_DTE_THIN_SAMPLE uses ZERO_DTE_THIN_EDGE (10pts).
    Returns (confirmed: bool, note: str).
    """
    if not result:
        return False, "TradeOdds unavailable — historical validation skipped, no alert fired"

    sample   = result.get("sample_size", 0)
    prob_up  = result.get("probability_up", 50.0)
    prob_dn  = result.get("probability_down", 50.0)

    if sample < ZERO_DTE_MIN_SAMPLE:
        return False, f"TradeOdds sample too small (n={sample}) — no alert fired"

    if direction == "CALL":
        edge = prob_up - prob_dn
        note = f"TradeOdds (n={sample}, 10y): {prob_up:.0f}% up vs {prob_dn:.0f}% down"
    else:
        edge = prob_dn - prob_up
        note = f"TradeOdds (n={sample}, 10y): {prob_dn:.0f}% down vs {prob_up:.0f}% up"

    required_edge = ZERO_DTE_MIN_EDGE if sample >= ZERO_DTE_THIN_SAMPLE else ZERO_DTE_THIN_EDGE
    if sample < ZERO_DTE_THIN_SAMPLE:
        note += " [thin-sample]"
    return edge >= required_edge, note


def _stock_tradeodds_confirms(direction: str, result: Optional[dict]) -> tuple:
    """
    Gate for the regular per-ticker stock signals — deliberately a different,
    simpler test than the 0DTE gate above. Confirms only if the probability
    in `direction`'s own favor (probability_up for CALL, probability_down for
    PUT) is itself >= STOCK_TRADEODDS_MIN_PROB, rather than an edge over the
    opposite side. Returns (should_fire: bool, info: dict) — info is always
    populated (even on failure) and always carries "tradeodds_confirmed",
    which is distinct from should_fire: during an extended outage
    should_fire can be True while tradeodds_confirmed is False, because
    TradeOdds itself never actually backed the signal.

    Outage fallback: a short TradeOdds outage (< TRADEODDS_OUTAGE_GRACE_MINUTES)
    still hard-blocks — the original, safe behavior. Once the SAME
    continuous outage has run that long, the signal fires anyway
    (unconfirmed) rather than going dark for the whole incident, the way 87
    real signals got silently blocked during a 46-minute outage on 2026-06-25.
    """
    if not result:
        outage_minutes = _tradeodds_outage_minutes()
        if outage_minutes >= TRADEODDS_OUTAGE_GRACE_MINUTES:
            note = f"⚠️ TradeOdds unavailable for {outage_minutes:.0f}min — firing unconfirmed"
            return True, {"checked": True, "available": False, "tradeodds_confirmed": False,
                          "outage_fallback": True, "note": note}
        return False, {
            "checked": True, "available": False, "tradeodds_confirmed": False,
            "outage_fallback": False, "note": "TradeOdds unavailable — no alert fired",
        }

    sample  = result.get("sample_size", 0)
    prob_up = result.get("probability_up", 50.0)
    prob_dn = result.get("probability_down", 50.0)
    # CALL: require prob_up >= 55% (stock likely to rise).
    # PUT/bearish: require prob_up < 45% — absence of upward conviction.
    # Using prob_up as the single reference metric in both branches keeps the
    # note readable and avoids the prior confusion where prob_dn >= 55% and
    # prob_up < 45% were treated as equivalent (they diverge when there is a
    # neutral probability mass that doesn't sum to 100).
    bearish_bar = 100.0 - STOCK_TRADEODDS_MIN_PROB  # 45.0 when min_prob = 55

    info = {
        "checked":          True,
        "available":        True,
        "sample_size":      sample,
        "probability_up":   prob_up,
        "probability_down": prob_dn,
        "direction":        direction,
    }

    if sample < STOCK_TRADEODDS_MIN_SAMPLE:
        info["tradeodds_confirmed"] = False
        info["outage_fallback"]     = False
        info["low_sample_fallback"] = True
        info["note"] = f"⚠️ TradeOdds sample too small (n={sample}) — firing unconfirmed"
        return True, info

    if direction == "CALL":
        confirmed = prob_up >= STOCK_TRADEODDS_MIN_PROB
        info["note"] = (
            f"TradeOdds (n={sample}): prob_up={prob_up:.1f}% "
            f"{'>=' if confirmed else '<'} {STOCK_TRADEODDS_MIN_PROB:.0f}% threshold"
        )
    else:
        confirmed = prob_up < bearish_bar
        info["note"] = (
            f"TradeOdds (n={sample}): prob_up={prob_up:.1f}% "
            f"{'<' if confirmed else '>='} {bearish_bar:.0f}% bearish threshold"
        )
    info["tradeodds_confirmed"] = confirmed
    info["outage_fallback"]     = False
    return confirmed, info


# ─── SIGNAL EVALUATORS ─────────────────────────────────────────────────────────
#
# Each evaluator returns:
#   {
#     "score":   int 0–100,
#     "reasons": list[str],   — human-readable explanation of every criterion
#     "metrics": dict,        — raw values for the decision log
#   }
#
# SCORING RUBRIC (same for both momentum and bearish):
#   20-day breakout/breakdown : 25 pts
#   RSI zone                  : 20 pts
#   Volume surge              : 25 pts
#   MACD alignment            : 20 pts
#   VWAP position (intraday)  : 10 pts
#                               ──────
#                               100 pts max

def eval_momentum_breakout(ticker: str, daily: list, intraday: list, session_fraction: float = 1.0) -> dict:
    """
    MOMENTUM BREAKOUT — bullish call setup.
    Criteria: price > 20D high, RSI 55-75, volume surge, MACD bullish, above VWAP.

    session_fraction: how much of today's trading session has elapsed (1.0 =
    full day). For both stocks mid-session and crypto's still-forming UTC
    daily candle, volume[-1] is
    CUMULATIVE volume since the open — comparing that directly to a full-day
    20D average would make vol_ratio read low all morning by construction,
    not because conviction is actually weak. Scaling the average down to the
    elapsed fraction keeps the comparison apples-to-apples at any time of day.
    """
    if len(daily) < 25:
        return {"score": 0, "reasons": ["Insufficient price history"], "metrics": {}}

    closes  = np.array([b["c"] for b in daily], dtype=float)
    volumes = np.array([b["v"] for b in daily], dtype=float)

    # Use latest intraday close as current price when available (more real-time)
    price      = float(intraday[-1]["c"]) if intraday else float(closes[-1])
    prev_close = float(closes[-2])
    day_chg    = (price - prev_close) / prev_close if prev_close else 0.0

    score   = 0
    reasons = []

    # ── 1. Price breakout above 20-day high (25 pts) ──────────────────────────
    high_20d = float(np.max(closes[-20:]))
    if price > high_20d:
        pct_above = (price - high_20d) / high_20d * 100
        score += 25
        reasons.append(f"Price ${price:.2f} breaks 20D high ${high_20d:.2f} (+{pct_above:.1f}%)")
    else:
        gap = (high_20d - price) / high_20d * 100
        reasons.append(f"Price ${price:.2f} is {gap:.1f}% below 20D high ${high_20d:.2f} — no breakout")

    # ── 2. RSI momentum zone (20 pts) ─────────────────────────────────────────
    rsi_val = calc_rsi(closes)
    if 55 <= rsi_val <= 75:
        score += 20
        reasons.append(f"RSI {rsi_val:.1f} — ideal momentum zone [55–75]")
    elif rsi_val > 75:
        score += 8
        reasons.append(f"RSI {rsi_val:.1f} — overbought territory, partial credit")
    elif 45 <= rsi_val < 55:
        score += 8
        reasons.append(f"RSI {rsi_val:.1f} — momentum building, not yet strong")
    else:
        reasons.append(f"RSI {rsi_val:.1f} — too weak for bullish momentum setup")

    # ── 3. Volume surge (25 pts) ───────────────────────────────────────────────
    # Baseline is yesterday's single-day volume (not a 20D average) — same
    # comparison the Railway version uses. Noisier and easier to trigger on a
    # moderate move than a smoothed 20-day average, by design.
    prior_day_vol = float(volumes[-2])
    expected_vol  = prior_day_vol * session_fraction
    vol_ratio     = float(volumes[-1] / expected_vol) if expected_vol else 0.0
    vol_label     = "prior-day vol" if session_fraction >= 0.999 else "expected pace"
    current_vol   = float(volumes[-1])
    avg_vol_20d   = float(np.mean(volumes[-21:-1]))
    if vol_ratio >= 2.0:
        score += 25
        reasons.append(f"Volume {vol_ratio:.1f}× {vol_label} — strong institutional conviction")
    elif vol_ratio >= 1.5:
        score += 15
        reasons.append(f"Volume {vol_ratio:.1f}× {vol_label} — elevated, good confirmation")
    elif vol_ratio >= 1.2:
        score += 8
        reasons.append(f"Volume {vol_ratio:.1f}× {vol_label} — slightly above average")
    else:
        reasons.append(f"Volume {vol_ratio:.1f}× {vol_label} — weak, breakout lacks conviction")

    # ── 4. MACD bullish alignment (20 pts) ────────────────────────────────────
    ml, sl, hist = calc_macd(closes)
    if hist > 0 and ml > sl:
        score += 20
        reasons.append(f"MACD bullish crossover — histogram {hist:+.4f}, momentum accelerating")
    elif ml > 0:
        score += 8
        reasons.append(f"MACD line positive ({ml:+.4f}), signal not yet crossed")
    else:
        reasons.append(f"MACD bearish ({ml:+.4f}) — trend not aligned with breakout")

    # ── 5. Above VWAP (10 pts) ────────────────────────────────────────────────
    if intraday:
        vwap_val = calc_vwap(intraday)
        if vwap_val > 0:
            if price > vwap_val:
                score += 10
                reasons.append(f"Trading above intraday VWAP ${vwap_val:.2f} — institutional bias bullish")
            else:
                reasons.append(f"Below VWAP ${vwap_val:.2f} — smart money not confirming today's move")
    else:
        reasons.append("No intraday data — VWAP check skipped")

    return {
        "score":   min(score, 100),
        "reasons": reasons,
        "metrics": {
            "price":       round(price, 2),
            "high_20d":    round(high_20d, 2),
            "rsi":         round(rsi_val, 1),
            "vol_ratio":   round(vol_ratio, 2),
            "current_vol": round(current_vol, 0),
            "avg_vol_20d": round(avg_vol_20d, 0),
            "day_chg_pct": round(day_chg * 100, 2),
            "macd_hist":   round(hist, 4),
        },
    }


def eval_bearish_put(ticker: str, daily: list, intraday: list, session_fraction: float = 1.0) -> dict:
    """
    BEARISH PUT — put option setup.
    Criteria: price < 20D low, RSI 25-45, high-vol selling, MACD bearish, below VWAP.

    session_fraction: see eval_momentum_breakout — scales the 20D volume
    average down to how much of today's session has elapsed so vol_ratio is
    comparable at any time of day, not just at the close.
    """
    if len(daily) < 25:
        return {"score": 0, "reasons": ["Insufficient price history"], "metrics": {}}

    closes  = np.array([b["c"] for b in daily], dtype=float)
    volumes = np.array([b["v"] for b in daily], dtype=float)

    price      = float(intraday[-1]["c"]) if intraday else float(closes[-1])
    prev_close = float(closes[-2])

    score   = 0
    reasons = []

    # ── 1. Price breakdown below 20-day low (25 pts) ──────────────────────────
    low_20d = float(np.min(closes[-20:]))
    if price < low_20d:
        pct_below = (low_20d - price) / low_20d * 100
        score += 25
        reasons.append(f"Price ${price:.2f} breaks 20D low ${low_20d:.2f} (-{pct_below:.1f}%)")
    else:
        gap = (price - low_20d) / low_20d * 100
        reasons.append(f"Price ${price:.2f} is {gap:.1f}% above 20D low ${low_20d:.2f} — no breakdown")

    # ── 2. RSI bearish zone (20 pts) ──────────────────────────────────────────
    rsi_val = calc_rsi(closes)
    if 25 <= rsi_val <= 45:
        score += 20
        reasons.append(f"RSI {rsi_val:.1f} — ideal bearish zone [25–45], momentum weak")
    elif rsi_val < 25:
        score += 8
        reasons.append(f"RSI {rsi_val:.1f} — oversold, bounce risk present, partial credit")
    elif 45 < rsi_val <= 55:
        score += 8
        reasons.append(f"RSI {rsi_val:.1f} — momentum weakening, approaching bearish zone")
    else:
        reasons.append(f"RSI {rsi_val:.1f} — too strong for bearish setup")

    # ── 3. Volume on selling pressure (25 pts) ────────────────────────────────
    # Baseline is yesterday's single-day volume (not a 20D average) — same
    # comparison the Railway version uses. Noisier and easier to trigger on a
    # moderate move than a smoothed 20-day average, by design.
    prior_day_vol = float(volumes[-2])
    expected_vol  = prior_day_vol * session_fraction
    vol_ratio     = float(volumes[-1] / expected_vol) if expected_vol else 0.0
    vol_label     = "prior-day vol" if session_fraction >= 0.999 else "expected pace"
    current_vol   = float(volumes[-1])
    avg_vol_20d   = float(np.mean(volumes[-21:-1]))
    day_chg      = (price - prev_close) / prev_close if prev_close else 0.0
    if vol_ratio >= 1.5 and day_chg < 0:
        score += 25
        reasons.append(f"Heavy selling {vol_ratio:.1f}× {vol_label} volume, down {day_chg*100:.1f}% — capitulation signal")
    elif vol_ratio >= 1.2 and day_chg < 0:
        score += 15
        reasons.append(f"Elevated selling {vol_ratio:.1f}× {vol_label}, down {day_chg*100:.1f}%")
    elif vol_ratio >= 1.5:
        score += 10
        reasons.append(f"Volume spike {vol_ratio:.1f}× {vol_label} but price direction mixed")
    else:
        reasons.append(f"Volume {vol_ratio:.1f}× {vol_label} — insufficient selling pressure")

    # ── 4. MACD bearish alignment (20 pts) ────────────────────────────────────
    ml, sl, hist = calc_macd(closes)
    if hist < 0 and ml < sl:
        score += 20
        reasons.append(f"MACD bearish crossover — histogram {hist:+.4f}, downtrend accelerating")
    elif ml < 0:
        score += 8
        reasons.append(f"MACD below zero ({ml:+.4f}), bearish but signal not yet crossed")
    else:
        reasons.append(f"MACD positive ({ml:+.4f}) — trend not confirming bearish thesis")

    # ── 5. Below VWAP (10 pts) ────────────────────────────────────────────────
    if intraday:
        vwap_val = calc_vwap(intraday)
        if vwap_val > 0:
            if price < vwap_val:
                score += 10
                reasons.append(f"Below intraday VWAP ${vwap_val:.2f} — sellers in control all session")
            else:
                reasons.append(f"Above VWAP ${vwap_val:.2f} — buyers absorbing selling today")
    else:
        reasons.append("No intraday data — VWAP check skipped")

    return {
        "score":   min(score, 100),
        "reasons": reasons,
        "metrics": {
            "price":      round(price, 2),
            "low_20d":    round(low_20d, 2),
            "rsi":        round(rsi_val, 1),
            "vol_ratio":  round(vol_ratio, 2),
            "current_vol": round(current_vol, 0),
            "avg_vol_20d": round(avg_vol_20d, 0),
            "day_chg_pct": round(day_chg * 100, 2),
            "macd_hist":  round(hist, 4),
        },
    }


def eval_big_drop_bounce(ticker: str, daily: list, intraday: list) -> dict:
    """
    BIG DROP BOUNCE — bullish call on a stock that has pulled back 5%+ from
    its 10-day high while smart money (call/put OI ratio) signals a bounce.
    Price technicals only here. The OI ratio gate (_big_drop_oi_ratio) runs
    separately and is the primary confirmation.

    Scoring (100 pts max):
      40 pts: Declined 5%+ from 10-day high
      20 pts: RSI 30–55 (oversold pullback, not breakdown)
      20 pts: Price above 50-day MA (long-term uptrend intact)
      20 pts: Selling was orderly — recent vol within 2.5× average
    """
    if len(daily) < 55:
        return {"score": 0, "reasons": ["Insufficient history for BIG DROP BOUNCE"], "metrics": {}}

    closes  = np.array([b["c"] for b in daily], dtype=float)
    volumes = np.array([b["v"] for b in daily], dtype=float)
    price   = float(intraday[-1]["c"]) if intraday else float(closes[-1])

    score   = 0
    reasons = []

    # 1. Decline from 10-day high (40 pts)
    high_10d = float(np.max(closes[-10:]))
    drop_pct = (high_10d - price) / high_10d * 100
    if drop_pct >= 7:
        score += 40
        reasons.append(f"Price ${price:.2f} is {drop_pct:.1f}% below 10D high ${high_10d:.2f} — deep pullback, prime bounce zone")
    elif drop_pct >= 5:
        score += 30
        reasons.append(f"Price ${price:.2f} is {drop_pct:.1f}% below 10D high ${high_10d:.2f} — solid pullback")
    elif drop_pct >= 3:
        score += 15
        reasons.append(f"Price ${price:.2f} is {drop_pct:.1f}% below 10D high — shallow pullback only")
    else:
        reasons.append(f"Price ${price:.2f} only {drop_pct:.1f}% below 10D high — no significant drop")

    # 2. RSI in oversold-but-not-crashing zone (20 pts)
    rsi_val = calc_rsi(closes)
    if 30 <= rsi_val <= 55:
        score += 20
        reasons.append(f"RSI {rsi_val:.1f} — oversold pullback zone [30–55], bounce setup confirmed")
    elif 25 <= rsi_val < 30:
        score += 10
        reasons.append(f"RSI {rsi_val:.1f} — very oversold, watch for trend break")
    elif rsi_val < 25:
        reasons.append(f"RSI {rsi_val:.1f} — extreme oversold, potential trend break not bounce")
    else:
        reasons.append(f"RSI {rsi_val:.1f} — not oversold enough for bounce play")

    # 3. Long-term uptrend intact: price above 50-day MA (20 pts)
    ma50 = float(np.mean(closes[-50:]))
    if price > ma50:
        score += 20
        reasons.append(f"Price ${price:.2f} above 50D MA ${ma50:.2f} — long-term uptrend intact, this is a pullback")
    else:
        pct_below = (ma50 - price) / ma50 * 100
        reasons.append(f"Price ${price:.2f} is {pct_below:.1f}% below 50D MA ${ma50:.2f} — potential breakdown, not pullback")

    # 4. Orderly selling — recent 3-day vol within 2.5× average (20 pts)
    avg_vol_20d = float(np.mean(volumes[-21:-1]))
    recent_vol  = float(np.mean(volumes[-3:]))
    vol_ratio   = recent_vol / avg_vol_20d if avg_vol_20d else 1.0
    if vol_ratio <= 1.5:
        score += 20
        reasons.append(f"Selling volume {vol_ratio:.1f}× average — orderly pullback, not panic")
    elif vol_ratio <= 2.5:
        score += 10
        reasons.append(f"Selling volume {vol_ratio:.1f}× average — elevated but manageable")
    else:
        reasons.append(f"Selling volume {vol_ratio:.1f}× average — heavy panic selling, recovery uncertain")

    return {
        "score":   min(score, 100),
        "reasons": reasons,
        "metrics": {
            "price":      round(price, 2),
            "high_10d":   round(high_10d, 2),
            "drop_pct":   round(drop_pct, 2),
            "rsi":        round(rsi_val, 1),
            "ma50":       round(ma50, 2),
            "vol_ratio":  round(vol_ratio, 2),
        },
    }


def eval_opening_range(bars: list) -> dict:
    """
    SPY 0DTE opening-range read: direction + momentum from the first five
    1-minute bars (9:30-9:34 ET). Volume is NOT scored here — it's checked by
    the caller against the ticker's own daily-volume cache, since this
    function only sees the 5 opening bars and has no baseline to compare to.
    """
    if len(bars) < 5:
        return {"valid": False, "reason": f"only {len(bars)}/5 opening bars available"}

    opens  = [b["o"] for b in bars]
    closes = [b["c"] for b in bars]
    vols   = [b["v"] for b in bars]

    open_px    = float(opens[0])
    last_close = float(closes[-1])
    chg_pct    = (last_close - open_px) / open_px * 100 if open_px else 0.0
    total_vol  = float(sum(vols))

    up_bars   = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
    down_bars = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i - 1])

    if chg_pct > 0:
        direction = "CALL"
    elif chg_pct < 0:
        direction = "PUT"
    else:
        direction = None

    # Momentum confirms when at least 3 of the 4 bar-to-bar moves agree with
    # the overall direction — a single counter-tick doesn't kill the setup,
    # a choppy/indecisive open does.
    momentum_confirms = (
        (direction == "CALL" and up_bars >= 3) or
        (direction == "PUT" and down_bars >= 3)
    )

    return {
        "valid":             True,
        "direction":         direction,
        "chg_pct":           chg_pct,
        "open_px":           open_px,
        "last_close":        last_close,
        "total_vol":         total_vol,
        "up_bars":           up_bars,
        "down_bars":         down_bars,
        "momentum_confirms": momentum_confirms,
    }


def eval_crash_alert(
    tick_results: dict,
    idx1_intra: list,
    idx2_intra: list,
    idx1_name: str = "SPY",
    idx2_name: str = "QQQ",
) -> dict:
    """
    MARKET CRASH ALERT — systemic breakdown detector.
    Triggers on: leading-index intraday drops + broad watchlist breadth deterioration.
    For stocks  : idx1=SPY, idx2=QQQ
    For crypto  : idx1=BTC, idx2=ETH

    SCORING:
      Index 1 intraday decline : 35 pts
      Index 2 intraday decline : 25 pts
      Watchlist breadth        : 40 pts
                                 ──────
                                 100 pts max  (alert fires at >= 60)
    """
    score   = 0
    reasons = []

    def intra_change_pct(bars: list) -> float:
        if len(bars) < 2:
            return 0.0
        return (bars[-1]["c"] - bars[0]["o"]) / bars[0]["o"] * 100.0

    # ── 1. Index 1 intraday decline (35 pts) ──────────────────────────────────
    chg1 = intra_change_pct(idx1_intra)
    if chg1 <= -2.5:
        score += 35
        reasons.append(f"{idx1_name} down {chg1:.2f}% intraday — severe market stress")
    elif chg1 <= -1.5:
        score += 25
        reasons.append(f"{idx1_name} down {chg1:.2f}% intraday — notable broad market weakness")
    elif chg1 <= -1.0:
        score += 12
        reasons.append(f"{idx1_name} down {chg1:.2f}% intraday — early warning signal")
    else:
        reasons.append(f"{idx1_name} {chg1:+.2f}% intraday — no critical market stress")

    # ── 2. Index 2 intraday decline (25 pts) ──────────────────────────────────
    chg2 = intra_change_pct(idx2_intra)
    if chg2 <= -2.5:
        score += 25
        reasons.append(f"{idx2_name} down {chg2:.2f}% intraday — leading index in freefall")
    elif chg2 <= -1.5:
        score += 15
        reasons.append(f"{idx2_name} down {chg2:.2f}% intraday — leading selloff")
    elif chg2 <= -1.0:
        score += 8
        reasons.append(f"{idx2_name} down {chg2:.2f}% intraday — under pressure")
    else:
        reasons.append(f"{idx2_name} {chg2:+.2f}% intraday — holding")

    # ── 3. Watchlist breadth (40 pts) ─────────────────────────────────────────
    n = len(tick_results)
    n_bearish = sum(
        1 for r in tick_results.values()
        if r.get("bearish_put", {}).get("score", 0) >= 60
    )
    pct = n_bearish / n * 100.0 if n else 0.0

    if pct >= 65:
        score += 40
        reasons.append(f"Breadth collapse: {n_bearish}/{n} tickers ({pct:.0f}%) showing bearish signals ≥60")
    elif pct >= 45:
        score += 25
        reasons.append(f"Broad weakness: {n_bearish}/{n} tickers ({pct:.0f}%) bearish")
    elif pct >= 30:
        score += 12
        reasons.append(f"Sector weakness: {n_bearish}/{n} tickers ({pct:.0f}%) bearish")
    else:
        reasons.append(f"Breadth intact: only {n_bearish}/{n} tickers ({pct:.0f}%) bearish")

    return {
        "score":   min(score, 100),
        "reasons": reasons,
        "metrics": {
            "idx1_chg_pct":  round(chg1, 2),
            "idx2_chg_pct":  round(chg2, 2),
            "breadth_pct":   round(pct, 1),
            "bearish_count": n_bearish,
            "total_tickers": n,
        },
    }


# ─── DISCORD NOTIFICATIONS ──────────────────────────────────────────────────────

_STYLE = {
    "MOMENTUM BREAKOUT":  {"color": 0x00C851, "emoji": "🚀"},
    "BEARISH PUT":        {"color": 0xFF4444, "emoji": "📉"},
    "BIG DROP BOUNCE":    {"color": 0x00BFFF, "emoji": "🏹"},
    "MARKET CRASH ALERT": {"color": 0xCC0000, "emoji": "🚨"},
    "0DTE SCALP":         {"color": 0xFFD700, "emoji": "⚡"},
}


def _send_rh_block_alert(ticker: str, signal: str, reason: str) -> None:
    """Send a Discord alert when an order is blocked by kill switch or circuit breaker."""
    embed = {
        "title": f"🛑 RH Auto-Execution BLOCKED — {ticker}",
        "color": 0xFF0000,
        "fields": [
            {"name": "Signal", "value": signal, "inline": True},
            {"name": "Reason", "value": reason, "inline": True},
        ],
        "footer": {"text": "QVIX | To re-enable: rm logs/rh_kill_switch  or  fix circuit breaker"},
    }
    _post_embed(embed)


def _post_embed(embed: dict, webhook_url: Optional[str] = None) -> bool:
    """
    Post one embed to the Discord webhook. Returns True/False so callers (and
    the runtime log) can tell definitively whether an alert was actually
    delivered — previously a successful post logged nothing at all, so the
    only way to confirm delivery was to test it manually after the fact.
    Retries once on a transient failure before giving up.
    Pass webhook_url to route to a non-default channel (e.g. DISCORD_CRYPTO_WEBHOOK_URL).
    """
    url = webhook_url or DISCORD_WEBHOOK_URL
    if not url:
        logging.warning("Discord post skipped — DISCORD_WEBHOOK_URL is not set")
        return False
    for attempt in range(2):
        try:
            r = requests.post(url, json={"username": "QVIX 5.1", "embeds": [embed]}, timeout=10)
            r.raise_for_status()
            logging.info(f"  Discord post OK ({r.status_code})")
            return True
        except Exception as exc:
            if attempt == 0:
                time.sleep(2)
                continue
            logging.error(f"Discord post failed after retry: {exc}")
            return False


def _strike_increment(price: float) -> float:
    """Standard listed-option strike spacing by underlying price band."""
    if price < 25:
        return 0.5
    if price < 100:
        return 1.0
    if price < 200:
        return 2.5
    return 5.0


def _next_friday(min_days_out: int) -> date:
    d = date.today() + timedelta(days=min_days_out)
    return d + timedelta(days=(4 - d.weekday()) % 7)   # Friday = weekday 4


def fetch_option_chain(
    ticker: str,
    option_type: str,
    strike_lo: float,
    strike_hi: float,
    expiry_hi: date,
) -> list:
    """
    Option contracts for a stock, via Polygon's options chain snapshot,
    filtered server-side to a strike band and expiry window. An unfiltered
    call to this endpoint caps out at 250 results ordered in a way that isn't
    proximity-to-current-price — for TSLA (~$387) an unfiltered request came
    back as only strikes $5-$190, all at a single near-term expiry, nowhere
    near the contract we actually wanted. Filtering by contract_type +
    strike_price.gte/.lte + expiration_date.lte ensures the relevant contract
    is actually in the page we get back.

    Raises requests.HTTPError on failure — in particular HTTP 403 if this
    account's plan doesn't include options data, which the caller must handle
    by falling back to no pricing.
    """
    data = _poly_get(
        f"https://api.polygon.io/v3/snapshot/options/{ticker}",
        {
            "contract_type":       option_type.lower(),
            "strike_price.gte":    strike_lo,
            "strike_price.lte":    strike_hi,
            "expiration_date.gte": date.today().isoformat(),
            "expiration_date.lte": expiry_hi.isoformat(),
            "limit":                250,
        },
    )
    return data.get("results", [])


def _pick_contract(contracts: list, target_strike: float, target_expiry: date) -> Optional[dict]:
    """Nearest-match contract by expiry first, then strike (contracts are
    already filtered to the right contract_type/strike band/expiry window by
    fetch_option_chain)."""
    if not contracts:
        return None

    def sort_key(c):
        d = c["details"]
        try:
            expiry_diff = abs((date.fromisoformat(d["expiration_date"]) - target_expiry).days)
        except (KeyError, ValueError):
            expiry_diff = 9999
        strike_diff = abs(d.get("strike_price", 0) - target_strike)
        return (expiry_diff, strike_diff)

    return min(contracts, key=sort_key)


def _real_option_price(contract: dict) -> Optional[tuple]:
    """
    (low, high) entry range for the contract. Waterfall of three sources:

    1. last_quote bid/ask — live spread, most accurate. Polygon free tier
       doesn't return this field, so it's almost never present in practice.
    2. last_quote midpoint — sometimes present even without full bid/ask.
    3. day OHLC — today's actual traded range. Previously required vol > 0
       but that blocked early-morning signals before the option has printed
       its first trade. Now accepts vol == 0 as long as open > 0 (the opening
       theoretical price is good enough to log as entry_premium).

    Returns None only if no price data exists at all for this contract.
    """
    q = contract.get("last_quote") or {}

    # 1. Live bid/ask spread
    bid, ask = q.get("bid"), q.get("ask")
    if bid and ask and bid > 0 and ask > 0 and ask >= bid:
        return float(bid), float(ask)

    # 2. Midpoint (sometimes present without full bid/ask on Polygon)
    mid = q.get("midpoint")
    if mid and mid > 0:
        return float(mid * 0.97), float(mid * 1.03)   # ±3% spread estimate

    # 3. Day OHLC — use even if no trades yet (vol == 0); opening price is valid
    day = contract.get("day") or {}
    low  = day.get("low")
    high = day.get("high")
    opn  = day.get("open")
    if low and high and low > 0 and high >= low:
        return float(low), float(high)
    if opn and opn > 0:
        return float(opn * 0.95), float(opn * 1.05)   # ±5% around open if only open available

    return None


def _big_drop_oi_ratio(ticker: str, price: float) -> dict:
    """
    Fetch call/put open interest AND same-day volume at the ATM strike for 10–17 DTE.
    Returns call_oi, put_oi, ratio, call_vol, put_vol, strike, and expiry.
    A ratio >= 2.0 means smart money is positioned 2:1 calls over puts — bounce signal.
    call_vol/put_vol are today's traded volume at the same strike — used by the BDB
    same-day flow check to catch cases where flow contradicts the OI thesis.
    Returns {"ratio": 0.0} on any API failure.
    """
    try:
        inc    = _strike_increment(price)
        strike = round(round(price / inc) * inc, 2)
        expiry = _next_friday(10)

        calls = fetch_option_chain(ticker, "call",
                                   strike_lo=round(strike - inc, 2),
                                   strike_hi=round(strike + inc, 2),
                                   expiry_hi=expiry + timedelta(days=7))
        puts  = fetch_option_chain(ticker, "put",
                                   strike_lo=round(strike - inc, 2),
                                   strike_hi=round(strike + inc, 2),
                                   expiry_hi=expiry + timedelta(days=7))

        call_c   = _pick_contract(calls, strike, expiry)
        put_c    = _pick_contract(puts,  strike, expiry)
        call_oi  = int(call_c["open_interest"]) if call_c and call_c.get("open_interest") else 0
        put_oi   = int(put_c["open_interest"])  if put_c  and put_c.get("open_interest")  else 0
        call_vol = int(call_c.get("day", {}).get("volume") or 0) if call_c else 0
        put_vol  = int(put_c.get("day", {}).get("volume")  or 0) if put_c  else 0
        ratio    = round(call_oi / put_oi, 2) if put_oi > 0 else 0.0
        return {"call_oi": call_oi, "put_oi": put_oi, "ratio": ratio,
                "call_vol": call_vol, "put_vol": put_vol,
                "strike": strike, "expiry": expiry}
    except Exception as exc:
        logging.warning(f"  [BDB] OI ratio fetch failed for {ticker}: {exc}")
        return {"ratio": 0.0}


def _option_plan(signal: str, score: int, price: float, ticker: str) -> Optional[dict]:
    """
    Pick a slightly-OTM strike/expiry for a MOMENTUM BREAKOUT (call) or
    BEARISH PUT (put) signal, then try to attach a REAL bid/ask quote from
    Polygon's options chain. If the chain endpoint 403s (account plan doesn't
    include options data), no matching contract is found, or the quote looks
    broken, "real_data" is False and no entry/target/stop prices are returned
    — only strike + expiry, so we never show a fabricated price next to a real
    ticker. Target/stop, when present, are a trading-plan projection off the
    REAL quote midpoint, not a quote themselves.
    """
    if signal not in ("MOMENTUM BREAKOUT", "BEARISH PUT", "BIG DROP BOUNCE") or not price:
        return None

    option_type = "CALL" if signal in ("MOMENTUM BREAKOUT", "BIG DROP BOUNCE") else "PUT"

    inc = _strike_increment(price)

    if signal == "BIG DROP BOUNCE":
        # ATM strike — highest delta, moves most with the stock on a bounce
        target_strike = round(round(price / inc) * inc, 2)
        target_expiry = _next_friday(10)   # 10–14 DTE: enough time, not too much theta
        style    = "Bounce"
        gain_pct = 0.40
        loss_pct = 0.35
    else:
        nearest = round(price / inc) * inc
        if option_type == "CALL" and nearest <= price:
            nearest += inc
        elif option_type == "PUT" and nearest >= price:
            nearest -= inc
        target_strike = round(nearest, 2)

        # HIGH-confidence setups are played for the immediate move (this week's
        # expiry); everything else gets a longer runway for the thesis to play out.
        if score >= 85:
            target_expiry, style = _next_friday(0), "Intraday"
        else:
            target_expiry, style = _next_friday(14), "Swing"

        gain_pct = 0.50 if style == "Intraday" else 0.35
        loss_pct = 0.30 if style == "Intraday" else 0.40

    plan = {
        "option_type": option_type,
        "style":       style,
        "real_data":   False,
        "strike":      target_strike,
        "expiry":      target_expiry,
    }

    try:
        contracts = fetch_option_chain(
            ticker, option_type,
            strike_lo=round(target_strike * 0.75, 2),
            strike_hi=round(target_strike * 1.25, 2),
            expiry_hi=target_expiry + timedelta(days=21),
        )
        contract = _pick_contract(contracts, target_strike, target_expiry)
        if contract:
            d = contract["details"]
            plan["strike"] = d.get("strike_price", target_strike)
            try:
                plan["expiry"] = date.fromisoformat(d["expiration_date"])
            except (KeyError, ValueError):
                pass

            # Already present on the same Polygon contract snapshot used for
            # strike/expiry/quote above — no extra API call needed.
            day_data = contract.get("day") or {}
            if contract.get("open_interest") is not None and day_data.get("volume") is not None:
                plan["volume"]        = int(day_data["volume"])
                plan["open_interest"] = int(contract["open_interest"])

            quote = _real_option_price(contract)
            if quote:
                low, high = quote
                mid = (low + high) / 2
                # Sanity-check: Polygon sometimes returns stale OHLC for
                # thinly-traded or illiquid strikes (prior-session prices, or
                # theoretical opens before the contract has printed a trade
                # at the current underlying level). For near-ATM options
                # (within 5% of stock price) with >20 DTE, a midpoint below
                # ~3% of the underlying per √(DTE/30) is implausible at any
                # realistic IV — reject it rather than broadcast a wrong price.
                days_out_check = (plan["expiry"] - date.today()).days
                moneyness      = abs(plan["strike"] - price) / price
                if moneyness < 0.05 and days_out_check > 20:
                    min_floor = price * 0.030 * (days_out_check / 30) ** 0.5
                    if mid < min_floor:
                        logging.warning(
                            f"  {ticker}: Polygon price ${mid:.2f} < "
                            f"floor ${min_floor:.2f} "
                            f"(near-ATM, {days_out_check}d) "
                            f"— data stale, omitting entry range from alert"
                        )
                        quote = None
            if quote:
                low, high = quote
                mid = (low + high) / 2
                plan.update({
                    "real_data":  True,
                    "entry_low":  round(low, 2),
                    "entry_high": round(high, 2),
                    "target":     round(mid * (1 + gain_pct), 2),
                    "stop":       round(mid * (1 - loss_pct), 2),
                    "target_pct": round(gain_pct * 100),
                    "stop_pct":   round(loss_pct * 100),
                })
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        logging.warning(f"  {ticker}: options chain fetch failed (HTTP {status}) — strike/expiry only, no price")
    except Exception as exc:
        logging.warning(f"  {ticker}: options chain fetch error: {exc} — strike/expiry only, no price")

    plan["days_out"] = (plan["expiry"] - date.today()).days
    return plan


def _zero_dte_option_plan(ticker: str, direction: str, price: float) -> dict:
    """
    ATM same-day-expiry contract for the 0DTE scanner. Targets the nearest $1
    strike to spot — SPY and QQQ both list $1-wide strikes at every price level
    — and today's expiry only. real_data stays False unless a real quote came
    back from the chain.
    """
    option_type = "CALL" if direction == "CALL" else "PUT"
    strike      = round(price)
    expiry      = date.today()

    plan = {
        "option_type": option_type,
        "real_data":   False,
        "strike":      strike,
        "expiry":      expiry,
    }

    try:
        contracts = fetch_option_chain(
            ticker, option_type,
            strike_lo=strike - 5,
            strike_hi=strike + 5,
            expiry_hi=expiry,
        )
        contract = _pick_contract(contracts, strike, expiry)
        if contract:
            d = contract["details"]
            plan["strike"] = d.get("strike_price", strike)
            try:
                plan["expiry"] = date.fromisoformat(d["expiration_date"])
            except (KeyError, ValueError):
                pass

            day_data = contract.get("day") or {}
            if contract.get("open_interest") is not None and day_data.get("volume") is not None:
                plan["volume"]        = int(day_data["volume"])
                plan["open_interest"] = int(contract["open_interest"])

            quote = _real_option_price(contract)
            if quote:
                low, high = quote
                mid = (low + high) / 2
                plan.update({
                    "real_data":  True,
                    "entry_low":  round(low, 2),
                    "entry_high": round(high, 2),
                    "target":     round(mid * 2.0, 2),   # +100%
                    "stop":       round(mid * 0.5, 2),    # -50%
                })
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        logging.warning(f"  {ticker} 0DTE: options chain fetch failed (HTTP {status}) — strike/expiry only, no price")
    except Exception as exc:
        logging.warning(f"  {ticker} 0DTE: options chain fetch error: {exc} — strike/expiry only, no price")

    return plan


def _fmt_price(price: float) -> str:
    """
    Smart price formatter for crypto: BTC/ETH/SOL-magnitude assets read fine
    at 2 decimals, but sub-$1 assets (DOGE, ADA, ...) lose real precision
    there, and sub-$0.01 assets lose it entirely at either 2 or 4 decimals.
    Scales precision to magnitude instead of a fixed format spec.
    """
    if price >= 1:
        return f"{price:.2f}"
    elif price >= 0.01:
        return f"{price:.4f}"
    else:
        return f"{price:.6f}"


def _trade_plan(signal: str, price: float) -> str:
    """Build entry/target/stop/R:R block for a given signal and price."""
    if signal == "MOMENTUM BREAKOUT":
        target = price * 1.02
        stop   = price * 0.99
    elif signal == "BEARISH PUT":
        target = price * 0.98
        stop   = price * 1.01
    else:
        return ""   # No price-based plan for crash alerts

    reward = abs(target - price)
    risk   = abs(stop   - price)
    rr     = reward / risk if risk else 2.0
    rr_str = f"1:{rr:.0f}" if rr == int(rr) else f"1:{rr:.1f}"

    return (
        f"🎯 Entry:  **${_fmt_price(price)}**\n"
        f"💰 Target: **${_fmt_price(target)}**\n"
        f"🛑 Stop:   **${_fmt_price(stop)}**\n"
        f"Risk/Reward: **{rr_str}**"
    )


# ─── SIGNAL LOG ─────────────────────────────────────────────────────────────────
# Every fired signal, open and (eventually) closed, for outcome tracking —
# separate from DECISION_LOG (every evaluation, fired or not) and from the
# Discord embeds themselves. Stored as a JSON array (not JSONL) specifically
# so _update_signal_outcome can find and mutate one record in place once an
# exit is known.

def _load_signal_log() -> list:
    """Best-effort load — missing/corrupt file returns an empty list, never crashes the caller."""
    try:
        return json.loads(SIGNAL_LOG_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _log_signal(ticker: str, signal: str, direction: str, score: int, price: float,
                 tp: Optional[float], sl: Optional[float], strike: Optional[float] = None,
                 expiry=None, entry_premium: Optional[float] = None,
                 tradeodds_confirmed: Optional[bool] = None,
                 signal_type: str = "stock_option",
                 low_sample_fallback: bool = False,
                 synthesis_verdict: Optional[str] = None,
                 synthesis_confidence: Optional[int] = None) -> None:
    """
    Append one open-signal record. Best-effort — a write failure logs a
    warning, never blocks the alert that already fired.

    signal_type distinguishes how _check_signal_outcomes should resolve this
    record: "stock_option" re-fetches the option contract premium, "crypto_spot"
    compares current Coinbase price to tp/sl as absolute price levels,
    "0dte_option" uses SPY spot vs. strike intrinsic value at end of day.

    tp/sl meaning by signal_type:
      stock_option — option premium targets (from _option_plan)
      crypto_spot  — absolute spot price levels (entry ± CRYPTO_TARGET/STOP_PCT)
      0dte_option  — option premium targets (mid × 2.0 / mid × 0.5 from _price_and_plan)

    low_sample_fallback: True when TradeOdds fired but n < STOCK_TRADEODDS_MIN_SAMPLE
    (the new soft-gate path added 2026-07-08). Used by the dashboard to separate
    "confirmed" vs "unconfirmed-outage" vs "unconfirmed-thin-sample" signals.
    """
    record = {
        "timestamp":      datetime.now(ET).isoformat(),
        "ticker":         ticker,
        "signal":         signal,
        "signal_type":    signal_type,
        "direction":      direction,
        "score":          score,
        "price":          price,
        "tp":             tp,
        "sl":             sl,
        "strike":         strike,
        "expiry":         expiry.isoformat() if hasattr(expiry, "isoformat") else expiry,
        "entry_premium":  entry_premium,
        "tradeodds_confirmed":  tradeodds_confirmed,
        "low_sample_fallback":  low_sample_fallback,
        "synthesis_verdict":    synthesis_verdict,
        "synthesis_confidence": synthesis_confidence,
        "status":         "open",
        "exit_price":     None,
        "exit_time":      None,
        "pnl":            None,
        "outcome":        None,
        "executed_liquid":     False,
        "executed_robinhood":  False,
        "liquid_trade_id":     None,
        "robinhood_order_id":  None,
    }
    records = _load_signal_log()
    records.append(record)
    try:
        SIGNAL_LOG_FILE.write_text(json.dumps(records, indent=2))
    except Exception as exc:
        logging.warning(f"  Signal log write failed: {exc}")


def _update_signal_outcome(ticker: str, timestamp: str, *, status=None, exit_price=None,
                            exit_time=None, pnl=None, outcome=None, executed_liquid=None,
                            executed_robinhood=None, liquid_trade_id=None, robinhood_order_id=None) -> bool:
    """Find the record matching (ticker, timestamp) exactly and update only the fields given. Returns whether a match was found."""
    records = _load_signal_log()
    updated = False
    for rec in records:
        if rec.get("ticker") == ticker and rec.get("timestamp") == timestamp:
            if status is not None:              rec["status"] = status
            if exit_price is not None:          rec["exit_price"] = exit_price
            if exit_time is not None:           rec["exit_time"] = exit_time
            if pnl is not None:                 rec["pnl"] = pnl
            if outcome is not None:             rec["outcome"] = outcome
            if executed_liquid is not None:     rec["executed_liquid"] = executed_liquid
            if executed_robinhood is not None:  rec["executed_robinhood"] = executed_robinhood
            if liquid_trade_id is not None:     rec["liquid_trade_id"] = liquid_trade_id
            if robinhood_order_id is not None:  rec["robinhood_order_id"] = robinhood_order_id
            updated = True
            break
    if updated:
        try:
            SIGNAL_LOG_FILE.write_text(json.dumps(records, indent=2))
        except Exception as exc:
            logging.warning(f"  Signal log update failed: {exc}")
            return False
    return updated


def _check_signal_outcomes(
    crypto_prices: Optional[dict] = None,
    spy_price: Optional[float] = None,
    spot_prices: Optional[dict] = None,
) -> None:
    """
    Resolve open signal records to win/loss/expired based on signal_type:

    "stock_option" (default for old records without the field):
        Re-fetches the exact option contract's live premium via Polygon and
        closes when premium hits tp (win) or sl (loss).

    "crypto_spot":
        Compares current Coinbase spot price (from crypto_prices dict passed
        by run_crypto_scan) against tp/sl stored as absolute price levels.
        Only checked when crypto_prices is provided — stock scan calls omit
        it so crypto records are skipped there and resolved in the crypto scan.

    "0dte_option":
        Uses SPY's current spot price to compute intrinsic value of the
        contract at or after 15:45 ET on expiry day (end-of-day resolution
        rather than intraday premium re-fetch, since Polygon 403s on option
        chain data for this account's plan). Only checked when spy_price is
        provided.

    Legacy records (no signal_type field) are treated as "stock_option".
    Records with tp=sl=None that still somehow reach the open_records list
    are auto-expired after SIGNAL_EXPIRY_HOURS as a safety net.
    """
    records = _load_signal_log()
    now     = datetime.now(ET)

    # ── Safety net: expire truly unresolvable legacy records (tp=sl=None) ────
    for rec in records:
        if rec.get("status") != "open":
            continue
        if rec.get("tp") is not None or rec.get("sl") is not None:
            continue
        try:
            logged_at = datetime.fromisoformat(rec["timestamp"])
        except (KeyError, ValueError):
            continue
        age_hours = (now - logged_at).total_seconds() / 3600.0
        if age_hours >= SIGNAL_EXPIRY_HOURS:
            _update_signal_outcome(rec["ticker"], rec["timestamp"], status="expired", outcome="expired")
            logging.info(f"  ⏳  EXPIRED  {rec['ticker']} {rec['signal']} — open {age_hours:.0f}h with no resolvable tp/sl")

    open_records = [
        r for r in records
        if r.get("status") == "open" and r.get("tp") is not None and r.get("sl") is not None
    ]
    if not open_records:
        return

    for rec in open_records:
        sig_type = rec.get("signal_type", "stock_option")
        ticker   = rec["ticker"]
        tp, sl   = rec["tp"], rec["sl"]

        # ── Crypto spot resolution ────────────────────────────────────────────
        if sig_type == "crypto_spot":
            if not crypto_prices:
                continue   # not called from crypto scan — skip until it is
            current = crypto_prices.get(ticker)
            if current is None:
                continue
            bullish    = "BREAKOUT" in rec.get("direction", "") or "MOMENTUM" in rec.get("direction", "")
            hit_target = (current >= tp) if bullish else (current <= tp)
            hit_stop   = (current <= sl) if bullish else (current >= sl)
            if not (hit_target or hit_stop):
                continue
            entry  = rec.get("price") or 0.0
            pnl    = ((current - entry) / entry * 100) if entry else None
            if pnl is not None and not bullish:
                pnl = -pnl   # falling price = profit for bearish
            outcome = "win" if hit_target else "loss"
            _update_signal_outcome(ticker, rec["timestamp"],
                status="closed", exit_price=round(current, 8),
                exit_time=now.isoformat(), pnl=round(pnl, 2) if pnl is not None else None,
                outcome=outcome)
            dir_tag = rec.get("direction", "?")
            logging.info(f"  📊  CRYPTO EXIT  {ticker} {dir_tag} — {outcome.upper()}  "
                         f"${current:.6f} (entry ${entry:.6f}, {pnl:+.1f}%)")
            continue

        # ── 0DTE intrinsic-value resolution (end-of-day, spot price vs strike) ─
        if sig_type == "0dte_option":
            # Use per-ticker spot price so QQQ signals use QQQ price, not SPY
            underlying_price = (spot_prices or {}).get(ticker) if spot_prices else spy_price
            if underlying_price is None:
                underlying_price = spy_price   # legacy fallback
            if underlying_price is None:
                continue   # no price available this cycle
            expiry_s = rec.get("expiry")
            strike   = rec.get("strike")
            if not expiry_s or strike is None:
                continue
            try:
                expiry_date = date.fromisoformat(expiry_s)
            except ValueError:
                continue
            if expiry_date != date.today():
                continue   # not expiry day yet — leave open
            if now.time() < dtime(15, 45):
                continue   # too early for end-of-day resolution
            direction     = rec.get("direction", "")
            entry_premium = rec.get("entry_premium")
            intrinsic = max(0.0, underlying_price - float(strike)) if direction == "CALL" \
                   else max(0.0, float(strike) - underlying_price)
            if entry_premium and entry_premium > 0:
                pnl     = (intrinsic - entry_premium) / entry_premium * 100
                outcome = "win" if intrinsic > entry_premium else "loss"
            else:
                pnl     = None
                outcome = "win" if intrinsic > 0 else "loss"
            _update_signal_outcome(ticker, rec["timestamp"],
                status="closed", exit_price=round(intrinsic, 4),
                exit_time=now.isoformat(), pnl=round(pnl, 2) if pnl is not None else None,
                outcome=outcome)
            ep_str = f"entry ${entry_premium:.2f}, " if entry_premium else ""
            logging.info(f"  📊  0DTE EXIT  {ticker} {direction} ${strike} spot=${underlying_price:.2f} — {outcome.upper()}  "
                         f"intrinsic ${intrinsic:.2f} ({ep_str}{pnl:+.1f}%)" if pnl is not None else
                         f"  📊  0DTE EXIT  {ticker} {direction} ${strike} spot=${underlying_price:.2f} — {outcome.upper()}  "
                         f"intrinsic ${intrinsic:.2f}")
            continue

        # ── Stock option resolution (default) — re-fetch Polygon premium ─────
        if not (rec.get("executed_liquid") or rec.get("executed_robinhood")):
            continue   # watchlist / manual-tracking signals — never auto-exit
        option_type   = rec.get("direction")
        strike        = rec.get("strike")
        expiry_s      = rec.get("expiry")
        entry_premium = rec.get("entry_premium")

        if option_type not in ("CALL", "PUT") or strike is None or expiry_s is None or entry_premium is None:
            continue   # can't re-identify the contract — leave open

        try:
            expiry = date.fromisoformat(expiry_s)
        except ValueError:
            continue

        if expiry < date.today():
            logging.warning(f"  Exit check: {ticker} {option_type} ${strike} [{expiry_s}] already expired — skipping")
            continue

        try:
            contracts = fetch_option_chain(
                ticker, option_type,
                strike_lo=round(strike * 0.75, 2),
                strike_hi=round(strike * 1.25, 2),
                expiry_hi=expiry + timedelta(days=3),
            )
            contract = _pick_contract(contracts, strike, expiry)
        except Exception as exc:
            logging.warning(f"  Exit check: {ticker} contract lookup failed: {exc}")
            continue

        if not contract:
            logging.warning(f"  Exit check: {ticker} {option_type} ${strike} [{expiry_s}] contract not found (likely expired) — leaving open")
            continue

        quote = _real_option_price(contract)
        if not quote:
            continue   # no real trade today on this contract — can't mark-to-market yet

        low, high = quote
        current_premium = (low + high) / 2

        hit_target = current_premium >= tp
        hit_stop   = current_premium <= sl
        if not (hit_target or hit_stop):
            continue

        outcome = "win" if hit_target else "loss"
        pnl     = (current_premium - entry_premium) / entry_premium * 100

        _update_signal_outcome(
            ticker, rec["timestamp"],
            status="closed",
            exit_price=round(current_premium, 4),
            exit_time=datetime.now(ET).isoformat(),
            pnl=round(pnl, 2),
            outcome=outcome,
        )
        logging.info(
            f"  📊  EXIT  {ticker} {option_type} ${strike} — {outcome.upper()}  "
            f"premium ${current_premium:.2f} (entry ${entry_premium:.2f}, {pnl:+.1f}%)"
        )


def _get_signal_stats() -> dict:
    """Read-only summary over the entire signal_log.json history. Never mutates the file."""
    records = _load_signal_log()
    wins   = sum(1 for r in records if r.get("outcome") == "win")
    losses = sum(1 for r in records if r.get("outcome") == "loss")
    decided = wins + losses
    pnls = [r["pnl"] for r in records if r.get("pnl") is not None]

    return {
        "total_signals":  len(records),
        "open_signals":   sum(1 for r in records if r.get("status") == "open"),
        "closed_signals": sum(1 for r in records if r.get("status") == "closed"),
        "wins":           wins,
        "losses":         losses,
        "win_rate":       round(wins / decided * 100, 1) if decided > 0 else 0.0,
        "total_pnl":      round(sum(pnls), 2) if pnls else 0.0,
    }


def _compute_daily_summary() -> dict:
    """
    Build the stats dict for the 4:15 PM Discord summary. Covers three scopes:
      • today — signals fired today and their exits (if already resolved)
      • all-time — cumulative win rate across the full history
      • fix attribution — post-2026-07-08 breakdown by TradeOdds confirmation path

    "Today" uses the signal's timestamp date for signals fired, and exit_time
    date for which day a resolution counted. Both are in ET.
    """
    records = _load_signal_log()
    today   = date.today().isoformat()

    today_sigs    = [r for r in records if r.get("timestamp", "")[:10] == today]
    all_closed    = [r for r in records if r.get("outcome") in ("win", "loss")]
    today_closed  = [r for r in all_closed if (r.get("exit_time") or "")[:10] == today]

    def _stats(recs: list) -> dict:
        w = sum(1 for r in recs if r.get("outcome") == "win")
        l = sum(1 for r in recs if r.get("outcome") == "loss")
        pnls = [r["pnl"] for r in recs if r.get("pnl") is not None]
        return {"count": len(recs), "wins": w, "losses": l,
                "win_rate": round(w / (w + l) * 100, 1) if (w + l) > 0 else None,
                "net_pnl":  round(sum(pnls), 2) if pnls else None}

    # Per-class breakdown (today's fired signals)
    by_class: dict = {}
    for sig_type, label in [("stock_option", "Stocks"), ("crypto_spot", "Crypto"), ("0dte_option", "0DTE")]:
        class_sigs    = [r for r in today_sigs if r.get("signal_type", "stock_option") == sig_type]
        class_closed  = [r for r in class_sigs if r.get("outcome") in ("win", "loss")]
        class_pnls    = [r["pnl"] for r in class_closed if r.get("pnl") is not None]
        by_class[label] = {
            "total":    len(class_sigs),
            "closed":   len(class_closed),
            "wins":     sum(1 for r in class_closed if r.get("outcome") == "win"),
            "losses":   sum(1 for r in class_closed if r.get("outcome") == "loss"),
            "net_pnl":  round(sum(class_pnls), 2) if class_pnls else None,
            "win_rate": None,
        }
        d = by_class[label]
        if (d["wins"] + d["losses"]) > 0:
            d["win_rate"] = round(d["wins"] / (d["wins"] + d["losses"]) * 100, 1)

    # Best / worst by PnL from today's closed trades
    resolved_today = sorted(today_closed, key=lambda r: r.get("pnl") or 0)
    best  = resolved_today[-1] if resolved_today else None
    worst = resolved_today[0]  if resolved_today else None

    # Fix attribution (since 2026-07-08 when the four fixes landed)
    FIX_DATE    = "2026-07-08"
    post_fix    = [r for r in records if r.get("timestamp", "")[:10] >= FIX_DATE]
    confirmed   = [r for r in post_fix if r.get("tradeodds_confirmed") is True
                                       and not r.get("low_sample_fallback")]
    thin_sample = [r for r in post_fix if r.get("low_sample_fallback")]
    outage_fb   = [r for r in post_fix if r.get("tradeodds_confirmed") is False
                                       and not r.get("low_sample_fallback")]
    crypto_sp   = [r for r in post_fix if r.get("signal_type") == "crypto_spot"]
    dte_tracked = [r for r in post_fix if r.get("signal_type") == "0dte_option"]

    all_decided = len(all_closed)
    all_wins    = sum(1 for r in all_closed if r.get("outcome") == "win")

    return {
        "today_signals":    len(today_sigs),
        "today_closed":     len(today_closed),
        "today_wins":       sum(1 for r in today_closed if r.get("outcome") == "win"),
        "today_losses":     sum(1 for r in today_closed if r.get("outcome") == "loss"),
        "today_pnl":        round(sum(r["pnl"] for r in today_closed if r.get("pnl") is not None), 2)
                            if today_closed else None,
        "by_class":         by_class,
        "best":             best,
        "worst":            worst,
        "all_time_wins":    all_wins,
        "all_time_decided": all_decided,
        "all_time_win_rate": round(all_wins / all_decided * 100, 1) if all_decided > 0 else None,
        "all_time_total":   len(records),
        # Fix attribution buckets
        "fix": {
            "confirmed":   _stats([r for r in confirmed if r.get("outcome") in ("win","loss")]),
            "thin_sample": _stats([r for r in thin_sample if r.get("outcome") in ("win","loss")]),
            "outage_fb":   _stats([r for r in outage_fb  if r.get("outcome") in ("win","loss")]),
            "crypto_spot": _stats([r for r in crypto_sp  if r.get("outcome") in ("win","loss")]),
            "dte_tracked": _stats([r for r in dte_tracked if r.get("outcome") in ("win","loss")]),
            # raw counts (including open) for the "signals fired" column
            "confirmed_n":   len(confirmed),
            "thin_sample_n": len(thin_sample),
            "outage_fb_n":   len(outage_fb),
            "crypto_spot_n": len(crypto_sp),
            "dte_tracked_n": len(dte_tracked),
        },
    }


_SILENCE_THRESHOLDS = {
    "BDB":   3,   # market days: BIG DROP BOUNCE
    "MB/BP": 4,   # market days: MOMENTUM BREAKOUT or BEARISH PUT
    "0DTE":  2,   # market days: 0dte_option signals
}


def _market_days_since(d: Optional[date]) -> Optional[int]:
    """Count Mon-Fri trading days from d (exclusive) up to and including today."""
    if d is None:
        return None
    count = 0
    cur = d + timedelta(days=1)
    today = date.today()
    while cur <= today:
        if cur.weekday() < 5:
            count += 1
        cur += timedelta(days=1)
    return count


def _silence_check() -> list:
    """
    Scan signal_log.json for the most recent signal per strategy arm.
    Returns a list of dicts for arms that have been silent longer than their
    threshold. Empty list means all arms are healthy.
    """
    records = _load_signal_log()

    def _last_date(pred) -> Optional[date]:
        ts_list = [r["timestamp"] for r in records if pred(r) and r.get("timestamp")]
        if not ts_list:
            return None
        return date.fromisoformat(max(ts_list)[:10])

    arms = [
        ("BDB",   lambda r: r.get("signal") == "BIG DROP BOUNCE"),
        ("MB/BP", lambda r: r.get("signal") in ("MOMENTUM BREAKOUT", "BEARISH PUT")),
        ("0DTE",  lambda r: r.get("signal_type") == "0dte_option"),
    ]

    alerts = []
    for name, pred in arms:
        last = _last_date(pred)
        days = _market_days_since(last)
        threshold = _SILENCE_THRESHOLDS[name]
        silent = (days is None) or (days > threshold)
        if silent:
            alerts.append({
                "name":      name,
                "last_date": last.strftime("%b %-d") if last else "never",
                "days":      days,
                "threshold": threshold,
            })
    return alerts


def send_daily_summary() -> None:
    """
    Build and post the 4:15 PM market-close signal summary to Discord.
    Called once per trading day from run_daily_summary_check().
    """
    s   = _compute_daily_summary()
    now = datetime.now(ET)

    # ── Top-line description ──────────────────────────────────────────────────
    if s["today_signals"] == 0:
        desc = "No signals fired today."
    else:
        wins   = s["today_wins"]
        losses = s["today_losses"]
        decided = wins + losses
        wr_str  = f"{wins}/{decided} = {round(wins/decided*100):d}%" if decided > 0 else "no closes yet"
        pnl_str = f"{s['today_pnl']:+.1f}%" if s["today_pnl"] is not None else "—"
        desc = (f"**{s['today_signals']}** signal{'s' if s['today_signals'] != 1 else ''} fired  ·  "
                f"**{s['today_closed']}** resolved  ·  {wr_str}  ·  net P&L **{pnl_str}**")

    # ── Asset class breakdown (code-block table) ──────────────────────────────
    rows = ["```", f"{'Class':<9} {'Fired':>5} {'Closed':>6} {'Win%':>5} {'Net P&L':>8}", "─" * 38]
    for label in ("Stocks", "Crypto", "0DTE"):
        d   = s["by_class"][label]
        wr  = f"{d['win_rate']:.0f}%" if d["win_rate"] is not None else "—"
        pnl = f"{d['net_pnl']:+.1f}%" if d["net_pnl"] is not None else "—"
        rows.append(f"{label:<9} {d['total']:>5} {d['closed']:>6} {wr:>5} {pnl:>8}")
    rows.append("```")
    fields = [{"name": "📊 By asset class", "value": "\n".join(rows), "inline": False}]

    # ── Best / worst ──────────────────────────────────────────────────────────
    if s["best"] and s["best"].get("pnl") is not None:
        b = s["best"]
        fields.append({"name": "🏆 Best today",
                        "value": f"`{b['ticker']}` {b['signal']}  **{b['pnl']:+.1f}%**",
                        "inline": True})
    if s["worst"] and s["worst"].get("pnl") is not None and s["worst"] is not s["best"]:
        w = s["worst"]
        fields.append({"name": "📉 Worst today",
                        "value": f"`{w['ticker']}` {w['signal']}  **{w['pnl']:+.1f}%**",
                        "inline": True})

    # ── All-time ──────────────────────────────────────────────────────────────
    if s["all_time_win_rate"] is not None:
        fields.append({"name": "📈 All-time",
                        "value": (f"{s['all_time_win_rate']:.1f}% win rate  "
                                  f"({s['all_time_wins']}W / {s['all_time_decided'] - s['all_time_wins']}L "
                                  f"from {s['all_time_total']} signals)"),
                        "inline": False})

    # ── Fix attribution (since 2026-07-08) ───────────────────────────────────
    fx = s["fix"]
    attr_lines = [
        f"✅ TradeOdds confirmed: **{fx['confirmed_n']}** signals"
        + (f" | {fx['confirmed']['wins']}W/{fx['confirmed']['losses']}L" if fx["confirmed"]["count"] else ""),
        f"⚠️  Thin-sample soft-gate (new): **{fx['thin_sample_n']}** signals"
        + (f" | {fx['thin_sample']['wins']}W/{fx['thin_sample']['losses']}L" if fx["thin_sample"]["count"] else ""),
        f"🔌 Outage fallback: **{fx['outage_fb_n']}** signals"
        + (f" | {fx['outage_fb']['wins']}W/{fx['outage_fb']['losses']}L" if fx["outage_fb"]["count"] else ""),
        f"🪙 Crypto spot-tracked (new): **{fx['crypto_spot_n']}** signals"
        + (f" | {fx['crypto_spot']['wins']}W/{fx['crypto_spot']['losses']}L" if fx["crypto_spot"]["count"] else ""),
        f"⚡ 0DTE tracked (new): **{fx['dte_tracked_n']}** signals"
        + (f" | {fx['dte_tracked']['wins']}W/{fx['dte_tracked']['losses']}L" if fx["dte_tracked"]["count"] else ""),
    ]
    fields.append({"name": "🔧 Fix attribution (since Jul 8)",
                    "value": "\n".join(attr_lines), "inline": False})

    # ── Strategy silence detection ────────────────────────────────────────────
    silence = _silence_check()
    if silence:
        slines = []
        for arm in silence:
            days_str = f"{arm['days']} market days" if arm["days"] is not None else "never"
            slines.append(
                f"🔇 **{arm['name']}** silent {days_str}"
                f" (last: {arm['last_date']}, threshold: {arm['threshold']}d)"
            )
        fields.append({"name": "⚠️ Strategy Silence Alerts", "value": "\n".join(slines), "inline": False})
        logging.warning(f"  Daily summary: strategy silence — {', '.join(a['name'] for a in silence)}")

    ok = _post_embed({
        "title":       f"📊 QVIX Daily Summary — {now.strftime('%B %-d, %Y')}",
        "description": desc,
        "color":       0x3FB950 if (s.get("today_pnl") or 0) >= 0 else 0xF85149,
        "fields":      fields,
        "footer":      {"text": f"QVIX · {now.strftime('%-I:%M %p ET')}  ·  http://localhost:8765/dashboard.html"},
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    })
    logging.info(f"  {'📊' if ok else '⚠️'}  Daily summary {'posted' if ok else 'FAILED'}  "
                 f"({s['today_signals']} signals today, {s['today_closed']} resolved)")


def run_daily_summary_check() -> None:
    """
    Gate for the daily 4:15 PM market-close summary. Called from the main
    loop every 5 minutes; does real work only once per trading day, during
    the 4:15-4:30 ET window. Marks fired before posting so a crash mid-post
    doesn't cause a duplicate on the next cycle.
    """
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return   # weekend
    if not (DAILY_SUMMARY_TIME <= now.time() <= DAILY_SUMMARY_END):
        return   # outside the window
    if _premarket_state.get("summary_fired"):
        return   # already sent today

    _premarket_state["summary_fired"] = True
    _save_premarket_state()
    try:
        send_daily_summary()
    except Exception as exc:
        logging.error(f"  Daily summary: {exc}")


# ─── EOD 0DTE FORCE-CLOSE REMINDER ───────────────────────────────────────────────
# Fires once per market day at 3:44 PM ET. Queries Robinhood for any open option
# positions expiring today and posts a Discord alert listing each one with current
# bid/ask. Pure alert — no orders placed. Prevents the "forgot to close" loss
# pattern where 0DTE positions expire worthless for $0 at 4:00 PM.


class _RobinhoodError(Exception):
    """Raised by fetch_robinhood_0dte_positions() when the API fails in a way
    that means the EOD close check could not run. Distinct from a non-fatal
    per-position lookup failure (which just skips that position)."""


def _rh_get(url: str, params: Optional[dict] = None) -> Optional[dict]:
    """
    Single Robinhood API GET.
    Raises _RobinhoodError on 401 (token expired — must surface to caller).
    Returns None on other transient errors (connection timeout, 5xx, etc.) so
    the caller can decide whether to skip or escalate.
    """
    try:
        r = requests.get(
            url, params=params,
            headers={"Authorization": f"Bearer {ROBINHOOD_ACCESS_TOKEN}", "Accept": "application/json"},
            timeout=10,
        )
        if r.status_code == 401:
            raise _RobinhoodError(
                "Robinhood 401 — ROBINHOOD_ACCESS_TOKEN is expired. "
                "Refresh it in .env and restart the daemon."
            )
        r.raise_for_status()
        return r.json()
    except _RobinhoodError:
        raise
    except Exception as exc:
        logging.warning(f"  EOD closer: Robinhood GET {url} failed: {exc}")
        return None


def fetch_robinhood_0dte_positions() -> list:
    """
    Return a list of open Robinhood option positions expiring today.
    Each entry: {ticker, option_type, strike, qty, avg_price, bid, ask}.

    Raises _RobinhoodError on unrecoverable failures (unset token, 401,
    positions endpoint unreachable). Per-position instrument/quote lookup
    failures are non-fatal — that position is skipped with a warning.
    """
    if not ROBINHOOD_ACCESS_TOKEN:
        raise _RobinhoodError(
            "ROBINHOOD_ACCESS_TOKEN is not set in .env — "
            "add it to enable the EOD position closer."
        )

    today = date.today().isoformat()
    results = []
    url: Optional[str] = f"{RH_BASE}/options/positions/"
    params: Optional[dict] = {"nonzero": "true"}

    while url:
        data = _rh_get(url, params)   # raises _RobinhoodError on 401
        if data is None:
            raise _RobinhoodError(
                "Robinhood /options/positions/ returned no data — "
                "network error or unexpected response."
            )

        for pos in data.get("results", []):
            try:
                qty = float(pos.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            if qty <= 0:
                continue

            instrument_url = pos.get("option")
            if not instrument_url:
                continue

            # Instrument lookup — non-fatal: skip position if unreachable
            instr = _rh_get(instrument_url)
            if not instr:
                logging.warning(f"  EOD closer: could not fetch instrument {instrument_url} — skipping position")
                continue
            if instr.get("expiration_date", "") != today:
                continue

            instrument_id = instrument_url.rstrip("/").split("/")[-1]

            # Quote lookup — non-fatal: show position without bid/ask if unreachable
            bid = ask = None
            quote_data = _rh_get(f"{RH_BASE}/marketdata/options/{instrument_id}/")
            if quote_data:
                try:
                    b = float(quote_data.get("bid_price") or 0)
                    a = float(quote_data.get("ask_price") or 0)
                    if b > 0:
                        bid = b
                    if a > 0:
                        ask = a
                except (TypeError, ValueError):
                    pass

            try:
                avg_price = float(pos.get("average_price") or 0) or None
                strike    = float(instr.get("strike_price") or 0) or None
            except (TypeError, ValueError):
                avg_price = strike = None

            results.append({
                "ticker":      instr.get("chain_symbol", "?"),
                "option_type": (instr.get("type") or "?").upper(),
                "strike":      strike,
                "qty":         qty,
                "avg_price":   avg_price,
                "bid":         bid,
                "ask":         ask,
            })

        url = data.get("next")
        params = None

    return results


def _auto_register_manual_positions(positions: list) -> int:
    """
    Cross-reference Robinhood positions against signal_log.json.
    For any position not already logged, create a 'manual_0dte' stub so it
    has a permanent accounting record.

    When bid/ask are None (zero quote at 3:48 PM) the option is almost
    certainly expiring worthless — close it immediately with pnl=-100%.
    When a live quote exists, log it as open (outcome checker resolves it).

    Mutates each position dict: adds p["tracked"] = True/False.
    Returns the count of newly registered entries.
    """
    today = date.today().isoformat()
    records = _load_signal_log()

    def _is_tracked(p: dict) -> bool:
        opt = p["option_type"].upper()
        for rec in records:
            if rec.get("ticker") != p["ticker"]:
                continue
            if opt not in (rec.get("direction") or "").upper():
                continue
            try:
                if abs(float(rec.get("strike") or 0) - float(p["strike"] or 0)) > 0.01:
                    continue
            except (TypeError, ValueError):
                continue
            if rec.get("status") not in ("open",):
                continue
            if not str(rec.get("expiry", "")).startswith(today):
                continue
            return True
        return False

    newly_registered = 0
    for p in positions:
        p["tracked"] = _is_tracked(p)
        if p["tracked"]:
            continue

        bid, ask = p.get("bid"), p.get("ask")
        avg      = p.get("avg_price") or 0
        worthless = bid is None or bid == 0  # bid=0 with ask>0 still expires worthless

        if worthless:
            record_status  = "closed"
            record_outcome = "expired-worthless"
            exit_price     = 0.0
            exit_time      = datetime.now(ET).isoformat()
            pnl            = -100.0 if avg else None
        else:
            record_status  = "open"
            record_outcome = None
            exit_price     = None
            exit_time      = None
            pnl            = None

        record = {
            "timestamp":            datetime.now(ET).isoformat(),
            "ticker":               p["ticker"],
            "signal":               f"MANUAL {p['option_type']}",
            "signal_type":          "manual_0dte",
            "direction":            f"MANUAL {p['option_type']}",
            "score":                0,
            "price":                None,
            "tp":                   None,
            "sl":                   None,
            "strike":               p["strike"],
            "expiry":               today,
            "entry_premium":        avg or None,
            "tradeodds_confirmed":  None,
            "low_sample_fallback":  False,
            "synthesis_verdict":    None,
            "synthesis_confidence": None,
            "status":               record_status,
            "exit_price":           exit_price,
            "exit_time":            exit_time,
            "pnl":                  pnl,
            "outcome":              record_outcome,
            "executed_liquid":      False,
            "executed_robinhood":   False,
            "liquid_trade_id":      None,
            "robinhood_order_id":   None,
            "source":               "manual",
            "qty":                  p.get("qty"),
        }
        records.append(record)
        newly_registered += 1
        status_tag = f"{record_status} / {record_outcome or 'tracking'}"
        logging.info(
            f"  EOD closer: auto-registered manual position — "
            f"{p['ticker']} {p['option_type']} ${p['strike']:.0f}"
            f" ×{p.get('qty', '?'):.0f}  →  {status_tag}"
        )

    if newly_registered:
        try:
            SIGNAL_LOG_FILE.write_text(json.dumps(records, indent=2))
        except Exception as exc:
            logging.warning(f"  EOD closer: signal_log write failed after auto-registering: {exc}")

    return newly_registered


def _send_sms_alert(body: str) -> bool:
    """
    Send an SMS via the Twilio REST API (raw HTTP — no twilio library needed).
    Requires TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_TO in .env.
    Logs but never raises on failure so it can't mask the primary Discord alert.
    """
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_TO]):
        logging.warning(
            "  SMS: TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_TO not set — "
            "add them to .env to enable SMS alerts"
        )
        return False
    try:
        r = requests.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            data={"From": TWILIO_FROM, "To": TWILIO_TO, "Body": body[:1600]},
            timeout=15,
        )
        r.raise_for_status()
        logging.info(f"  SMS: delivered to {TWILIO_TO}")
        return True
    except Exception as exc:
        logging.error(f"  SMS: FAILED ({exc}) — message was: {body[:120]}")
        return False


def _send_eod_closer_failure_alert(reason: str) -> None:
    """
    Cannot-miss alert for when the EOD 0DTE closer fails to run.
    Posts a visually distinct Discord message (different username, @here ping,
    pure red) AND sends an SMS via Twilio.
    This is the failure itself becoming the alert — not a log entry to check later.
    """
    now_str = datetime.now(ET).strftime('%I:%M %p ET')
    logging.error(f"  EOD CLOSER FAILED: {reason}")

    # Discord — deliberately different from normal signal embeds:
    # different username, @here ping, pure red (not the normal orange-red),
    # no fields/footer chrome — just the urgent message front and center.
    if DISCORD_WEBHOOK_URL:
        embed = {
            "title":       "🚨  EOD CLOSER FAILED — MANUAL ACTION REQUIRED",
            "description": (
                f"**The 3:44 PM 0DTE position check could not run.**\n\n"
                f"**Reason:** {reason}\n\n"
                f"**Manually check and close any 0DTE option positions before 4:00 PM ET.**"
            ),
            "color":     0xFF0000,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        for attempt in range(2):
            try:
                r = requests.post(
                    DISCORD_WEBHOOK_URL,
                    json={"username": "QVIX 🚨 CRITICAL", "content": "@here", "embeds": [embed]},
                    timeout=10,
                )
                r.raise_for_status()
                logging.info("  EOD closer failure alert: Discord @here delivered")
                break
            except Exception as exc:
                if attempt == 0:
                    time.sleep(2)
                    continue
                logging.error(f"  EOD closer failure alert: Discord ALSO FAILED: {exc}")

    # SMS — independent channel, fires even if Discord fails
    _send_sms_alert(
        f"QVIX EOD CLOSER FAILED at {now_str}: {reason}. "
        f"Check and close all 0DTE option positions before 4PM ET."
    )


def send_eod_0dte_close_alert(positions: list) -> bool:
    """Post EOD close reminder to Discord listing all open 0DTE positions."""
    lines = []
    for p in positions:
        bid, ask = p.get("bid"), p.get("ask")
        mid      = (bid + ask) / 2 if bid and ask else None
        cost     = p["avg_price"]
        pnl_str  = f"  ({(mid - cost) / cost * 100:+.0f}%)" if mid and cost else ""
        if bid and ask:
            quote = f"bid ${bid:.2f} / ask ${ask:.2f}"
        else:
            quote = "bid $0.00 — likely worthless"
        strike = f"${p['strike']:.0f}" if p["strike"] else "?"
        tag    = "  ⚡ manual" if not p.get("tracked", True) else ""
        lines.append(
            f"• {p['ticker']} {p['option_type']} {strike}  ×{p['qty']:.0f}"
            f"  avg ${cost:.2f}{pnl_str}  —  {quote}{tag}"
        )

    embed = {
        "title":       "⚠️  EOD CLOSE REMINDER — 0DTE Positions Open",
        "description": "**CLOSE ALL BEFORE 4:00 PM ET — 16 minutes remaining**\n\n" + "\n".join(lines),
        "color":       0xFF4444,
        "fields": [{
            "name":   "Action",
            "value":  "Market sell all 0DTE contracts now. Positions held to 4 PM expire worthless.",
            "inline": False,
        }],
        "footer":    {"text": f"QVIX 5.1  ·  EOD Closer  ·  {datetime.now(ET).strftime('%I:%M %p ET')}"},
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    ok = _post_embed(embed)
    logging.info(f"  EOD closer: Discord alert {'delivered' if ok else 'DELIVERY FAILED'}  ({len(positions)} position(s))")
    return ok


def run_eod_0dte_closer() -> None:
    """
    EOD 0DTE force-close reminder. Called from the main loop every 5 minutes;
    fires exactly once per trading day during the 3:44-3:50 ET window.

    Success path: queries Robinhood for open 0DTE positions → posts Discord alert.
    Failure path: fires a SEPARATE cannot-miss alert (distinct Discord @here +
    Twilio SMS) so a silent failure is impossible. The failure IS the alert.
    """
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return
    if not (EOD_CLOSER_TIME <= now.time() <= EOD_CLOSER_END):
        return
    if _premarket_state.get("eod_closer_fired"):
        return

    _premarket_state["eod_closer_fired"] = True
    _save_premarket_state()

    logging.info("  EOD closer: 3:44 PM check — querying Robinhood for open 0DTE positions…")
    try:
        positions = fetch_robinhood_0dte_positions()
        if not positions:
            logging.info("  EOD closer: no open 0DTE positions found — no alert needed")
            return
        logging.info(f"  EOD closer: {len(positions)} open 0DTE position(s) — posting Discord alert")
        registered = _auto_register_manual_positions(positions)
        if registered:
            logging.info(f"  EOD closer: auto-registered {registered} untracked manual position(s) in signal_log")
        send_eod_0dte_close_alert(positions)
    except _RobinhoodError as exc:
        _send_eod_closer_failure_alert(str(exc))
    except Exception as exc:
        _send_eod_closer_failure_alert(f"Unexpected error: {exc}")


# ─── PHASE 2: POSITION SIZING / CIRCUIT BREAKER / AUTO-EXECUTE DECISIONS ────────
# Pure calculation + decision-logging functions. None of these place trades —
# see the module docstring note above on why live execution isn't wired in.

def _calculate_position_size(score: int, account_equity: float, confirmed: bool = True,
                             vix_level: Optional[float] = None) -> float:
    """
    Position size in USD: 1% of equity for score 65-74, 2% for 75-84, 3% for
    85+, hard-capped at $500/trade regardless of equity size. Returns 0.0 for
    score < 65 (ALERT_MIN_SCORE) — there's no sane partial size below the
    threshold a signal needs to fire at all.

    confirmed=False (TradeOdds fired unconfirmed — outage or sample too
    small to trust) halves the size rather than blocking the trade outright.

    vix_level adjusts for vol regime: >30 → 75%, >35 → 50%. >40 should have
    been gated before this call for long-vol signals, but the multiplier
    provides a safety net for puts and other signals that still fire.
    """
    if score >= 85:
        pct = 0.03
    elif score >= 75:
        pct = 0.02
    elif score >= 65:
        pct = 0.01
    else:
        return 0.0
    if not confirmed:
        pct /= 2
    multiplier = 1.0
    if vix_level is not None:
        if vix_level > 35:
            multiplier = 0.50
        elif vix_level > 30:
            multiplier = 0.75
    return round(min(account_equity * pct * multiplier, 500.0), 2)


def _run_multi_agent_analysis(ticker: str) -> Optional[dict]:
    """
    Lazy-import and run the QVIX 6.0 agent pipeline for `ticker`.
    Agents/ dir is added to sys.path on first call; subsequent calls are fast.
    Returns the full analysis dict (analysts/bull/bear/synthesis/risk/portfolio)
    or None if the pipeline fails — callers must handle None as a fallback.
    Only fires the Discord portfolio card for BUY/SELL verdicts; HOLD is silent.
    """
    try:
        agents_dir = str(Path(__file__).parent / "agents")
        if agents_dir not in sys.path:
            sys.path.insert(0, agents_dir)
        from analyst_agents    import run_analyst_team      # noqa: PLC0415
        from researcher_agents import run_researcher_team   # noqa: PLC0415
        from synthesis_agent   import run_synthesis         # noqa: PLC0415
        from risk_agent        import run_risk_check        # noqa: PLC0415
        from portfolio_agent   import run_portfolio_manager # noqa: PLC0415
        import time as _time

        t0       = _time.time()
        analysts = run_analyst_team(ticker)

        researchers = run_researcher_team(ticker, analysts)
        bull = next((r for r in researchers if r.get("agent") == "bull_researcher"),
                    researchers[0] if researchers else {})
        bear = next((r for r in researchers if r.get("agent") == "bear_researcher"),
                    researchers[1] if len(researchers) > 1 else {})

        synthesis = run_synthesis(ticker, bull, bear, analysts)
        risk      = run_risk_check(ticker, synthesis)

        verdict  = synthesis.get("verdict", "HOLD")
        all_reports = {
            "analysts": analysts, "bull": bull, "bear": bear,
            "synthesis": synthesis, "risk": risk,
        }

        # Post enriched Discord card for actionable verdicts; HOLD is fully silent.
        portfolio = None
        if verdict in ("BUY", "SELL", "CALENDAR_SPREAD"):
            portfolio = run_portfolio_manager(ticker, all_reports)

        elapsed = round(_time.time() - t0, 1)
        logging.info(f"  🤖  Multi-agent complete: {verdict} / {risk.get('decision')} in {elapsed}s")
        return {**all_reports, "portfolio": portfolio}

    except Exception as exc:
        logging.warning(f"  Multi-agent pipeline failed for {ticker}: {exc}")
        return None


def _check_circuit_breaker() -> bool:
    """
    True = safe to trade, False = halt. Reads starting_equity from
    CIRCUIT_BREAKER_FILE (must be seeded manually — there is no live
    brokerage-balance feed anywhere in this codebase to auto-discover it;
    fabricating a number here would make the breaker meaningless) and sums
    today's CLOSED signals' pnl% from signal_log.json, filtered by exit_time
    (the day the loss was actually realized, not when the position opened).
    Halts if that sum is a loss of CIRCUIT_BREAKER_MAX_DAILY_LOSS_PCT or
    worse. Conservative by design: any missing/unreadable/zero starting_equity
    halts trading rather than assuming it's safe to proceed.

    Note: pnl is each trade's independent % return (see _get_signal_stats),
    so this sum is a simplification — not a true equity-curve drawdown
    unless every trade was sized identically — same convention already used
    for total_pnl in the stats function, kept consistent rather than
    introducing a different model here.
    """
    try:
        cb = json.loads(CIRCUIT_BREAKER_FILE.read_text())
        starting_equity = float(cb["starting_equity"])
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError) as exc:
        logging.warning(f"  Circuit breaker: starting_equity not configured in {CIRCUIT_BREAKER_FILE} ({exc}) — halting until it's seeded")
        return False

    if starting_equity <= 0:
        logging.warning("  Circuit breaker: starting_equity is zero/invalid — halting trading")
        return False

    today = date.today().isoformat()
    records = _load_signal_log()
    todays_pnl = sum(
        r["pnl"] for r in records
        if r.get("status") == "closed" and r.get("pnl") is not None
        and str(r.get("exit_time") or "").startswith(today)
        and (r.get("executed_liquid") or r.get("executed_robinhood"))
    )

    if -todays_pnl >= CIRCUIT_BREAKER_MAX_DAILY_LOSS_PCT:
        logging.warning(f"  🛑 Circuit breaker HALTED — today's closed PnL is {todays_pnl:+.2f}% (limit -{CIRCUIT_BREAKER_MAX_DAILY_LOSS_PCT:.0f}%)")
        return False
    return True


# Hard off by default. Liquid Co-Invest is only reachable via MCP tools bound
# to an interactive Claude session — this standalone process has no path to
# actually call it, unlike Polygon/TradeOdds which are real REST APIs with
# their own keys in .env. Flipping this to True does NOT make execution work;
# it would need a genuine Liquid REST API + credentials wired in first, the
# same way Polygon/TradeOdds were verified before being relied on. Until
# then this only ever logs what it would have done.
LIQUID_AUTO_EXECUTE_ENABLED = False

# ─── ROBINHOOD AUTO-EXECUTION (MCP queue) ─────────────────────────────────────
# qvix.py writes pending orders here; the QVIX MCP executor loop reads and
# places them via the Robinhood MCP connector (no bearer token needed — the
# MCP session is OAuth-authenticated permanently).
RH_AUTO_EXECUTE_ENABLED = True
RH_ORDER_BUDGET         = 100.0  # max dollars per options trade
RH_PENDING_ORDERS_FILE  = LOG_DIR / "rh_pending_orders.json"
_rh_queue_lock          = threading.Lock()


def _rh_queue_order(ticker: str, signal: str, plan: dict) -> str:
    """
    Write a pending order entry to rh_pending_orders.json for the MCP
    executor loop to pick up and place via the Robinhood MCP connector.
    Returns a local ref_id string (used to match the log entry later).
    Never raises — queue failure must not crash the scan cycle.
    """
    strike      = plan.get("strike")
    expiry      = plan.get("expiry")
    option_type = plan.get("option_type", "call")
    ref_id      = f"qvix-{ticker}-{int(time.time())}"

    entry = {
        "ref_id":      ref_id,
        "ticker":      ticker,
        "signal":      signal,
        "option_type": option_type,
        "strike":      strike,
        "expiry":      expiry,
        "budget":      RH_ORDER_BUDGET,
        "status":      "pending",
        "queued_at":   datetime.now(ZoneInfo("America/New_York")).isoformat(),
    }

    try:
        with _rh_queue_lock:
            existing = []
            if RH_PENDING_ORDERS_FILE.exists():
                try:
                    existing = json.loads(RH_PENDING_ORDERS_FILE.read_text())
                except (json.JSONDecodeError, ValueError):
                    existing = []
            existing.append(entry)
            RH_PENDING_ORDERS_FILE.write_text(json.dumps(existing, indent=2))
        logging.info(f"  📋  RH order queued: {ref_id}  {ticker} {option_type.upper()} {strike} {expiry}")
    except Exception as exc:
        logging.warning(f"  RH queue write failed for {ticker}: {exc}")

    return ref_id


def _liquid_trade_decision(signal: str, score: int) -> Optional[dict]:
    """
    What would be traded on Liquid Co-Invest for this signal — pure decision
    logic, places nothing. Long for MOMENTUM BREAKOUT, short for BEARISH PUT;
    3x leverage for score 65-79, 5x for score >= 80. Returns None for any
    other signal name (e.g. MARKET CRASH ALERT) or score below 65.
    """
    if signal == "MOMENTUM BREAKOUT":
        side = "long"
    elif signal == "BEARISH PUT":
        side = "short"
    else:
        return None

    if score >= 80:
        leverage = 5
    elif score >= 65:
        leverage = 3
    else:
        return None

    return {"side": side, "leverage": leverage}


def _log_liquid_dry_run(ticker: str, signal: str, score: int, tradeodds_confirmed: Optional[bool] = None,
                        vix_level: Optional[float] = None) -> None:
    """
    Computes and logs what Liquid auto-execution would do for this signal —
    size, side, leverage, and whether the circuit breaker would currently
    allow it — without placing anything. See LIQUID_AUTO_EXECUTE_ENABLED.

    Reads starting_equity from CIRCUIT_BREAKER_FILE (the same source of
    truth the breaker itself uses) rather than accepting it as a parameter —
    there's no live brokerage-balance feed anywhere in this codebase, so
    this must come from that one seeded value, not be guessed by a caller.

    tradeodds_confirmed=False halves the computed size (see
    _calculate_position_size) instead of sizing as if TradeOdds had backed
    it. None (e.g. crypto, which has no TradeOdds gate) sizes at full.
    """
    decision = _liquid_trade_decision(signal, score)
    if not decision:
        return

    try:
        cb = json.loads(CIRCUIT_BREAKER_FILE.read_text())
        account_equity = float(cb["starting_equity"])
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError):
        logging.info(
            f"  [DRY RUN] Liquid {decision['side'].upper()} {ticker} {decision['leverage']}x "
            f"— size unknown ({CIRCUIT_BREAKER_FILE} not seeded with starting_equity)"
        )
        return

    size = _calculate_position_size(score, account_equity, confirmed=(tradeodds_confirmed is not False),
                                    vix_level=vix_level)
    status = "WOULD EXECUTE" if _check_circuit_breaker() else "BLOCKED by circuit breaker"
    size_note = "" if tradeodds_confirmed is not False else " (halved — TradeOdds unconfirmed)"
    logging.info(
        f"  [DRY RUN] Liquid {decision['side'].upper()} {ticker} "
        f"{decision['leverage']}x  ·  ${size:.2f} notional{size_note}  ·  {status}"
    )


def send_alert(
    ticker: str,
    signal: str,
    score: int,
    reasons: list,
    price: Optional[float] = None,
    is_crypto: bool = False,
    metrics: Optional[dict] = None,
    price_asof: Optional[date] = None,
    opt: Optional[dict] = None,
    tradeodds_note: Optional[str] = None,
    synthesis: Optional[dict] = None,
    robinhood_data: Optional[dict] = None,
    liquid_data: Optional[dict] = None,
    uw_flow: Optional[dict] = None,
    uw_darkpool: Optional[dict] = None,
) -> bool:
    metrics = metrics or {}
    style   = _STYLE.get(signal, {"color": 0x888888, "emoji": "📊"})
    label   = f"[CRYPTO] {ticker}" if is_crypto else ticker

    # Options only apply to listed stocks, not crypto spot. Callers that
    # already computed the plan (e.g. to log tp/sl) pass it in via `opt` so
    # this doesn't re-fetch the options chain a second time — guarantees the
    # logged tp/sl match exactly what gets shown here, not a second,
    # possibly-drifted quote.
    if opt is None and not is_crypto:
        opt = _option_plan(signal, score, price, ticker)

    conf = "HIGH" if score >= 85 else "ELEVATED" if score >= 70 else "MEDIUM"

    if opt:
        # Shared clean signal card (discord_format.py) for every stock
        # CALL/PUT signal — same visual format across every signal type.
        direction = "BULLISH" if opt["option_type"] == "CALL" else "BEARISH"
        holding_period = "Intraday — close by end of day" if opt.get("style") == "Intraday" else "Swing trade — 2-3 weeks"

        uw_parts = []
        if uw_flow and uw_flow.get("confirmed"):
            uw_parts.append(uw_flow["note"])
        if uw_darkpool and uw_darkpool.get("confirmed"):
            uw_parts.append(uw_darkpool["note"])
        smart_money_note = " | ".join(uw_parts) if uw_parts else None

        embed = build_signal_embed(
            ticker=ticker,
            direction=direction,
            direction_label=signal,
            strike=opt.get("strike"),
            expiry=opt.get("expiry"),
            bid=opt.get("entry_low") if opt.get("real_data") else None,
            ask=opt.get("entry_high") if opt.get("real_data") else None,
            target=opt.get("target") if opt.get("real_data") else None,
            target_pct=opt.get("target_pct") if opt.get("real_data") else None,
            stop=opt.get("stop") if opt.get("real_data") else None,
            stop_pct=opt.get("stop_pct") if opt.get("real_data") else None,
            volume=opt.get("volume"),
            open_interest=opt.get("open_interest"),
            confidence=score,
            smart_money_note=smart_money_note,
            holding_period=holding_period,
            footer_text=f"QVIX 5.1  ·  {datetime.now(ET).strftime('%I:%M %p ET')}",
        )

        # Supplementary context beyond the core signal card — kept as
        # trailing fields rather than folded into the card itself, so the
        # card stays exactly the clean format while nothing existing is lost.
        fields = [
            {"name": "QVIX Analysis", "value": "\n".join(f"• {r}" for r in reasons[:6]) or "—", "inline": False},
        ]
        if synthesis:
            fields.append({"name": "AI Synthesis", "value": synthesis["reason"], "inline": False})
        if robinhood_data:
            rb_parts = []
            if robinhood_data.get("pe_ratio") is not None:
                rb_parts.append(f"📊 P/E: {robinhood_data['pe_ratio']:.1f}x")
            if robinhood_data.get("earnings_days") is not None:
                days  = robinhood_data["earnings_days"]
                warn  = " ⚠️" if days <= 7 else ""
                rb_parts.append(f"🗓️ Earnings in {days}d{warn}")
            if rb_parts:
                fields.append({"name": "Market Context", "value": "  |  ".join(rb_parts), "inline": False})
        if liquid_data and liquid_data.get("funding_rate") is not None:
            bias     = liquid_data.get("bias", "")
            confirms = "✅ Confirms" if "DOMINANT" in bias else "⚠️ Diverges"
            fields.append({
                "name":  "Positioning",
                "value": f"Funding: {liquid_data['funding_rate']:+.3f}%  |  Longs: {liquid_data.get('long_pct', 0):.0f}%  |  {confirms}",
                "inline": False,
            })
        if tradeodds_note:
            fields.append({"name": "Confirmation", "value": tradeodds_note, "inline": False})
        embed["fields"] = fields

        _wh = DISCORD_CRYPTO_WEBHOOK_URL if is_crypto else None
        ok = _post_embed(embed, webhook_url=_wh)
        logging.info(f"  Discord {'delivered' if ok else 'DELIVERY FAILED'}: {ticker} {signal}")
        return ok

    # Crypto signals and the market-wide crash alert have no strike/expiry —
    # keep the original fields-based embed for these.
    price_name = f"Price (as of {price_asof.isoformat()} close)" if price_asof else "Price"
    fields = [
        {"name": "Confidence", "value": f"**{score}/100** ({conf})", "inline": True},
        {"name": price_name,   "value": f"${_fmt_price(price)}" if price else "—", "inline": True},
    ]
    if price:
        plan = _trade_plan(signal, price)
        if plan:
            fields.append({"name": "Trade Plan", "value": plan, "inline": False})

    fields.append({
        "name":   "QVIX Analysis",
        "value":  "\n".join(f"• {r}" for r in reasons[:6]) or "—",
        "inline": False,
    })

    _wh = DISCORD_CRYPTO_WEBHOOK_URL if is_crypto else None
    ok = _post_embed({
        "title":     f"{style['emoji']}  {signal}  ·  {label}",
        "color":     style["color"],
        "fields":    fields,
        "footer":    {"text": f"QVIX 5.1  ·  {datetime.now(ET).strftime('%I:%M %p ET')}"},
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }, webhook_url=_wh)
    logging.info(f"  Discord {'delivered' if ok else 'DELIVERY FAILED'}: {label} {signal}")
    return ok


def send_zero_dte_alert(ticker: str, direction: str, plan: dict, price: float, opening: dict, tradeodds_note: str) -> bool:
    """
    0DTE scanner alert — same shared clean signal card as every other signal
    type, with opening-range read and TradeOdds confirmation appended.
    """
    embed = build_signal_embed(
        ticker=ticker,
        direction="BULLISH" if direction == "CALL" else "BEARISH",
        direction_label="0DTE SCALP",
        strike=plan.get("strike"),
        expiry=plan.get("expiry"),
        bid=plan.get("entry_low") if plan.get("real_data") else None,
        ask=plan.get("entry_high") if plan.get("real_data") else None,
        target=plan.get("target") if plan.get("real_data") else None,
        target_pct=100 if plan.get("real_data") else None,
        stop=plan.get("stop") if plan.get("real_data") else None,
        stop_pct=50 if plan.get("real_data") else None,
        volume=plan.get("volume"),
        open_interest=plan.get("open_interest"),
        confidence=100,   # this scanner only ever posts once TradeOdds has confirmed the setup
        smart_money_note=tradeodds_note,
        holding_period=f"0DTE — close before {ZERO_DTE_CLOSE_TIME.strftime('%-I:%M %p')} ET, theta decay accelerates fast",
        footer_text=f"QVIX 5.1  ·  {datetime.now(ET).strftime('%I:%M %p ET')}",
    )
    embed["fields"] = [{
        "name":  "Opening Range",
        "value": f"{opening['chg_pct']:+.2f}%  ·  Vol {opening['total_vol']:,.0f} ({opening['vol_ratio']:.1f}x expected pace)  ·  {ticker} ${price:.2f}",
        "inline": False,
    }]

    ok = _post_embed(embed)
    logging.info(f"  Discord {'delivered' if ok else 'DELIVERY FAILED'}: {ticker} 0DTE SCALP {direction}")
    return ok


def send_startup_notice():
    _post_embed({
        "title":       "⚙️  QVIX 5.1 Online",
        "color":       0x0099FF,
        "description": (
            f"**Stocks** ({len(WATCHLIST)} tickers) — market hours only\n"
            f"**Crypto** ({len(CRYPTO_WATCHLIST)} assets: {' · '.join(CRYPTO_WATCHLIST)}) — 24/7\n\n"
            "Signals: 🚀 MOMENTUM BREAKOUT  ·  📉 BEARISH PUT  ·  🚨 MARKET CRASH ALERT  ·  ⚡ 0DTE SCALP  ·  🌅 PRE-MARKET  ·  🔔 OPENING BELL\n"
            f"Alert threshold: **{ALERT_MIN_SCORE}/100**  ·  Cooldown: stocks {STOCK_COOLDOWN_SECS//3600}h+date / crypto {ALERT_COOLDOWN_SECS//3600}h  ·  Scan: every {SCAN_INTERVAL_SECS//60} min\n"
            f"0DTE (qvix): {ZERO_DTE_FIRE_TIME.strftime('%-I:%M')}–{ZERO_DTE_FIRE_WINDOW_END.strftime('%-I:%M %p')} ET, TradeOdds-gated · UW flow scanner: 9:30–11:00 AM ET, flow-only (no TradeOdds)\n"
            f"Pre-market: briefing {PREMARKET_BRIEFING_TIME.strftime('%-I:%M')} ET, opening bell {PREMARKET_BELL_TIME.strftime('%-I:%M %p')} ET"
        ),
        "footer":      {"text": f"Started {datetime.now(ET).strftime('%Y-%m-%d %I:%M %p ET')}  ·  code {_QVIX_MTIME}"},
    })


# ─── DECISION LOGGER ───────────────────────────────────────────────────────────

def log_decision(ticker: str, signal: str, result: dict, alerted: bool):
    """
    Append one JSON record per evaluation to qvix_decisions.jsonl.
    This is your audit trail — every scan, every score, every reason Claude used.
    """
    entry = {
        "ts":      datetime.now(ET).isoformat(),
        "ticker":  ticker,
        "signal":  signal,
        "score":   result["score"],
        "alerted": alerted,
        "reasons": result["reasons"],
        "metrics": result.get("metrics", {}),
    }
    with DECISION_LOG.open("a") as f:
        f.write(json.dumps(entry) + "\n")


# ─── ALERT COOLDOWN TRACKER ────────────────────────────────────────────────────
# Persisted to COOLDOWN_STATE_FILE — without this, a restart wipes the
# in-memory dict and lets anything that already fired today fire again,
# since _cooldown_ok has no other record of the prior alert.

def _parse_cooldown_records(raw: str, now: datetime) -> dict:
    """
    Parse one cooldowns JSON blob into the in-memory dict, isolating each
    record's own try/except so a single malformed entry (bad schema, bad
    timestamp) only drops that one record — not everything parsed after it.
    Previously one bad `ts` field raised ValueError out of the whole loop,
    which the caller's broad except swallowed, silently discarding every
    later record in the file (including same-day entries for tickers whose
    cooldown should still be active) and letting them re-fire.
    """
    _REQUIRED_KEYS = {"ticker", "signal", "ts", "score"}
    cooldowns: dict = {}
    records = json.loads(raw)
    for rec in records:
        try:
            if not isinstance(rec, dict) or not _REQUIRED_KEYS.issubset(rec):
                logging.warning(f"  Cooldown record failed schema check, skipping: {rec!r}")
                continue
            last_time = datetime.fromisoformat(rec["ts"])
            if (now - last_time).total_seconds() < ALERT_COOLDOWN_SECS:
                date_str = last_time.astimezone(ET).date().isoformat()
                cooldowns[(rec["ticker"], rec["signal"], date_str)] = (last_time, rec["score"])
        except (KeyError, ValueError) as exc:
            logging.warning(f"  Cooldown record unparseable, skipping (not discarding the rest): {rec!r} ({exc})")
            continue
    return cooldowns


def _load_cooldowns() -> dict:
    """
    Restore cooldown state from disk. Key is (ticker, signal, date_str) where
    date_str is the ET calendar date the alert fired — this is the primary
    same-day guard. Only records younger than ALERT_COOLDOWN_SECS (24h) are
    loaded so stale entries don't bloat memory across multiple days.

    Falls back to the .bak snapshot if the primary file is missing or fails
    to parse (e.g. truncated by a process kill mid-write) — without this,
    a single corrupt write silently resets EVERY ticker's cooldown to empty
    on the next restart, letting every signal that already fired today fire
    again. Atomic writes in _save_cooldowns() should prevent the corruption
    from happening in the first place; this is the second line of defense.
    """
    now = datetime.now(ET)
    bak = COOLDOWN_STATE_FILE.with_suffix(".json.bak")
    for path in (COOLDOWN_STATE_FILE, bak):
        try:
            return _parse_cooldown_records(path.read_text(), now)
        except FileNotFoundError:
            continue
        except (ValueError, json.JSONDecodeError) as exc:
            logging.warning(f"  {path.name} failed to parse ({exc}) — trying fallback")
            continue
    return {}


def _save_cooldowns():
    """
    Persist cooldown state. Writes to a temp file and renames it into place
    (os.replace is atomic on the same filesystem) so a process killed
    mid-save — the same 41x/day restart churn that motivated the O_CREAT|
    O_EXCL fix on the lock file — can never leave qvix_cooldowns.json
    truncated/corrupt. Previously a plain write_text() truncated the file
    before writing the new contents, so a kill in that window produced a
    broken JSON file that _load_cooldowns() would fail to parse and reset
    to empty, wiping every ticker's "already fired today" protection.
    """
    try:
        records = [
            {"ticker": t, "signal": s, "ts": last_time.isoformat(), "score": score}
            for (t, s, _d), (last_time, score) in _cooldowns.items()
        ]
        bak = COOLDOWN_STATE_FILE.with_suffix(".json.bak")
        if COOLDOWN_STATE_FILE.exists():
            bak.write_text(COOLDOWN_STATE_FILE.read_text())

        tmp = COOLDOWN_STATE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(records))
        os.replace(tmp, COOLDOWN_STATE_FILE)
    except Exception as exc:
        logging.warning(f"  Cooldown state persist failed: {exc}")


_cooldowns: dict = _load_cooldowns()   # key: (ticker, signal) → (datetime of last alert, score at last alert)


def _cooldown_ok(ticker: str, signal: str, score: int, cooldown_secs: int = ALERT_COOLDOWN_SECS) -> bool:
    """
    Two-layer cooldown gate:

    1. Calendar-day gate (date in key): blocks any same ET-date re-fire
       instantly, regardless of time elapsed. This is the primary guard for
       stocks — market hours never cross midnight so 12h elapsed is redundant,
       but a stale file or restart could bypass it without this check.

    2. Time-elapsed gate: blocks cross-midnight re-fires within cooldown_secs.
       Primarily for crypto, which runs 24/7 — a signal at 11:55 PM should
       still block at 12:05 AM even though that's a new calendar day.

    3. Crypto ticker-level daily cap: if ANY signal for this crypto ticker
       has fired today (regardless of signal name), block all further signals.
       Prevents the same asset from firing MOMENTUM BREAKOUT at 9 AM and then
       BEARISH PUT at 1 PM — two conflicting signals on the same crypto in one
       day is noise, not edge.
    """
    now      = datetime.now(ET)
    today    = now.date().isoformat()
    day_key  = (ticker, signal, today)

    if day_key in _cooldowns:
        return False

    # Crypto: block if any signal for this ticker fired today (signal-type isolation fix)
    if ticker.startswith("CRYPTO:"):
        for (t, _s, d) in _cooldowns:
            if t == ticker and d == today:
                logging.info(f"  Cooldown: {ticker} already fired a signal today ({_s}) — blocking {signal}")
                return False

    for (t, s, _d), (last_time, _score) in _cooldowns.items():
        if t == ticker and s == signal:
            if (now - last_time).total_seconds() < cooldown_secs:
                return False

    _cooldowns[day_key] = (now, score)
    _save_cooldowns()
    return True


def _cooldown_check(ticker: str, signal: str, score: int) -> bool:
    """
    Read-only cooldown check — returns True if the signal would pass cooldown,
    but does NOT stamp the cooldown entry. Use this for early gate checks where
    the signal may still be suppressed downstream (e.g. auction blackout, earnings,
    market tide). Only call _cooldown_ok (which writes) once all gates have passed
    and the signal is confirmed to fire.
    """
    now     = datetime.now(ET)
    today   = now.date().isoformat()
    day_key = (ticker, signal, today)

    if day_key in _cooldowns:
        return False

    if ticker.startswith("CRYPTO:"):
        for (t, _s, d) in _cooldowns:
            if t == ticker and d == today:
                return False

    cooldown_secs = ALERT_COOLDOWN_SECS if ticker.startswith("CRYPTO") else STOCK_COOLDOWN_SECS
    for (t, s, _d), (last_time, _score) in _cooldowns.items():
        if t == ticker and s == signal:
            if (now - last_time).total_seconds() < cooldown_secs:
                return False

    return True


def _should_alert(
    ticker: str,
    signal: str,
    score: int,
    min_score: int = ALERT_MIN_SCORE,
    current_vol: Optional[float] = None,
    avg_vol_20d: Optional[float] = None,
    session_fraction: float = 1.0,
) -> bool:
    """
    session_fraction scales avg_vol_20d (a full-day average) down to "expected
    volume so far today" before comparing — same fix as eval_momentum_breakout/
    eval_bearish_put's own vol_ratio. Without it, current_vol is cumulative
    volume since the open while avg_vol_20d is a full session's worth, so this
    gate could never pass until very late in the day regardless of score —
    it was silently killing nearly every otherwise-qualifying stock signal
    before the close. Crypto's still-forming UTC daily candle has the exact
    same problem — see _crypto_day_elapsed_fraction / run_crypto_scan, which
    now pass a real session_fraction here instead of leaving it at 1.0.
    """
    logging.info(f"ENTERCHECK ticker={ticker} signal={signal} score={score} min_score={min_score} current_vol={current_vol} avg_vol_20d={avg_vol_20d}")
    if score < min_score:
        return False
    if current_vol is not None and avg_vol_20d is not None:
        expected_vol_so_far = avg_vol_20d * session_fraction
        vol_ratio = current_vol / expected_vol_so_far if expected_vol_so_far > 0 else 1.0
        if vol_ratio < 0.70:
            logging.info(f"VOLCHECK {ticker} {signal} ratio={vol_ratio:.2f} current={current_vol:.0f} expected={expected_vol_so_far:.0f}")
            return False
    # Read-only check — cooldown is only stamped when signal actually fires
    return _cooldown_check(ticker, signal, score)


# ─── MARKET HOURS CHECK ────────────────────────────────────────────────────────

def is_market_open() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:   # Saturday=5, Sunday=6
        return False
    open_t  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_t = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return open_t <= now < close_t


def session_elapsed_fraction() -> float:
    """
    Fraction of today's 9:30-4:00 ET session that has elapsed, clamped to
    [0.05, 1.0]. Used to scale the 20D average volume down to an
    apples-to-apples "expected volume so far" baseline, since a stock's
    cumulative volume-since-open is naturally much smaller than a full day's
    average early in the session — that's a time-of-day effect, not weak
    conviction. The 0.05 floor avoids wild ratios in the first minute or two.
    """
    now     = datetime.now(ET)
    open_t  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_t = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    if now <= open_t:
        return 0.05
    if now >= close_t:
        return 1.0
    elapsed_mins = (now - open_t).total_seconds() / 60.0
    total_mins   = (close_t - open_t).total_seconds() / 60.0
    return max(0.05, min(1.0, elapsed_mins / total_mins))


# ─── MULTI-SOURCE ENRICHMENT + CLAUDE SYNTHESIS ────────────────────────────────

def fetch_robinhood_enrichment(ticker: str, plan: Optional[dict]) -> dict:
    """
    Read Robinhood enrichment written by robinhood_bridge.py. Returns {} if the
    bridge file doesn't exist yet or this ticker has no entry — never blocks a
    signal.
    """
    try:
        data = json.loads(ROBINHOOD_ENRICHMENT_FILE.read_text())
        return data.get(ticker, {})
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def fetch_liquid_positioning(symbol: str) -> Optional[dict]:
    """
    Read Liquid Co-Invest positioning written by liquid_bridge.py. Returns None
    if the bridge file doesn't exist yet or this symbol has no entry.
    """
    try:
        data = json.loads(LIQUID_POSITIONING_FILE.read_text())
        return data.get(symbol)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def fetch_unusual_whales_flow(ticker: str, direction: str) -> dict:
    """
    Pull recent unusual options flow alerts for a ticker from Unusual Whales and
    check whether smart-money confirms our signal direction.

    Correct endpoint: /api/option-trades/flow-alerts/?ticker={ticker}
    Filters to alerts created in the last 2 hours, matching direction.
    Metric: total_premium (dollars), richer than raw contract count.
    Returns {"confirmed": bool, "premium": float, "sweeps": int, "note": str}.
    Falls back gracefully — never blocks a signal, only adds confidence.
    """
    _FALLBACK = {"confirmed": False, "premium": 0.0, "sweeps": 0, "contrary_premium": 0.0, "note": "Unusual Whales unavailable"}

    if not UNUSUAL_WHALES_API_KEY:
        return _FALLBACK

    try:
        r = requests.get(
            "https://api.unusualwhales.com/api/option-trades/flow-alerts/",
            headers={"Authorization": f"Bearer {UNUSUAL_WHALES_API_KEY}"},
            params={"ticker": ticker, "limit": 50},
            timeout=10,
        )
        if r.status_code == 404:
            return {"confirmed": False, "premium": 0.0, "sweeps": 0, "contrary_premium": 0.0, "note": f"No UW flow data for {ticker}"}
        r.raise_for_status()

        alerts = r.json().get("data", [])
        cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
        dir_lc = direction.lower()              # UW returns "call" / "put"
        opp_lc = "put" if dir_lc == "call" else "call"

        total_premium    = 0.0
        contrary_premium = 0.0
        sweep_count      = 0

        for alert in alerts:
            alert_type = (alert.get("type") or "").lower()

            # created_at is an ISO string: "2026-07-04T17:41:04.831101Z"
            ts_raw = alert.get("created_at")
            if not ts_raw:
                continue
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if ts < cutoff:
                continue

            prem = float(alert.get("total_premium") or 0)
            if alert_type == dir_lc:
                total_premium += prem
                if alert.get("has_sweep"):
                    sweep_count += 1
            elif alert_type == opp_lc:
                contrary_premium += prem

        confirmed = total_premium > 0
        if confirmed:
            sweep_str = f", {sweep_count} sweep{'s' if sweep_count != 1 else ''}" if sweep_count else ""
            note = f"UW {direction} flow: ${total_premium:,.0f} premium in last 2h{sweep_str} ✅"
        else:
            note = f"No unusual {direction} flow alerts in last 2h"
        if contrary_premium > 0:
            note += f"  (contrary {opp_lc}: ${contrary_premium:,.0f})"

        logging.info(f"  UW flow {ticker} {direction}: {note}")
        return {
            "confirmed": confirmed, "premium": total_premium,
            "sweeps": sweep_count, "contrary_premium": contrary_premium, "note": note,
        }

    except Exception as exc:
        logging.warning(f"  Unusual Whales flow fetch failed for {ticker}: {exc}")
        return _FALLBACK


def fetch_unusual_whales_darkpool(ticker: str, direction: str) -> dict:
    """
    Fetch recent dark pool prints for a ticker and check for institutional
    conviction in our signal direction.

    Correct endpoint: /api/darkpool/{ticker} — returns last 500 prints.
    Direction is inferred by comparing print price to the NBBO midpoint:
      price > mid → buyer-initiated  → confirms CALL
      price < mid → seller-initiated → confirms PUT
    Confirm threshold: >$500K in qualifying prints in the last 2 hours.
    Returns {"confirmed": bool, "total_premium": float, "prints": int, "note": str}.
    Falls back gracefully — never blocks a signal.
    """
    _FALLBACK = {"confirmed": False, "total_premium": 0.0, "prints": 0, "note": "Dark pool unavailable"}
    _DP_MIN_PREMIUM = 500_000  # $500K qualifying threshold

    if not UNUSUAL_WHALES_API_KEY:
        return _FALLBACK

    try:
        r = requests.get(
            f"https://api.unusualwhales.com/api/darkpool/{ticker}",
            headers={"Authorization": f"Bearer {UNUSUAL_WHALES_API_KEY}"},
            timeout=12,
        )
        if r.status_code == 404:
            return {"confirmed": False, "total_premium": 0.0, "prints": 0, "note": f"No dark pool data for {ticker}"}
        r.raise_for_status()

        prints = r.json().get("data", [])
        cutoff = datetime.now(timezone.utc) - timedelta(hours=2)

        total_premium = 0.0
        qualifying    = 0

        for print_ in prints:
            if print_.get("canceled"):
                continue

            ts_raw = print_.get("executed_at")
            if not ts_raw:
                continue
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue
            if ts < cutoff:
                continue

            # Infer direction from price vs NBBO midpoint
            try:
                bid  = float(print_.get("nbbo_bid") or 0)
                ask  = float(print_.get("nbbo_ask") or 0)
                mid  = (bid + ask) / 2 if bid and ask else 0
                px   = float(print_.get("price") or 0)
            except (TypeError, ValueError):
                continue
            if mid == 0:
                continue

            buyer_initiated = px >= mid
            if direction == "CALL" and not buyer_initiated:
                continue
            if direction == "PUT" and buyer_initiated:
                continue

            prem = float(print_.get("premium") or 0)
            total_premium += prem
            qualifying    += 1

        confirmed = total_premium >= _DP_MIN_PREMIUM
        if confirmed:
            note = f"Dark pool: {qualifying} prints · ${total_premium:,.0f} {direction}-side in last 2h 🐋"
        elif qualifying > 0:
            note = f"Dark pool: {qualifying} prints · ${total_premium:,.0f} (below conviction threshold)"
        else:
            note = f"No directional dark pool prints in last 2h"

        logging.info(f"  UW darkpool {ticker} {direction}: {note}")
        return {"confirmed": confirmed, "total_premium": total_premium, "prints": qualifying, "note": note}

    except Exception as exc:
        logging.warning(f"  Unusual Whales dark pool fetch failed for {ticker}: {exc}")
        return _FALLBACK


def fetch_market_net_impact() -> dict:
    """
    Fetch market-wide net premium leaderboard from Unusual Whales.
    Called once per scan cycle and cached for 5 minutes so all per-ticker
    lookups in the same cycle share one API call.

    Returns {"bullish": {ticker: net_premium}, "bearish": {ticker: net_premium}}.
    """
    global _uw_net_impact_cache

    now = datetime.now(timezone.utc)
    cached_ts = _uw_net_impact_cache.get("ts")
    if cached_ts and (now - cached_ts).total_seconds() < 300 and _uw_net_impact_cache.get("data"):
        return _uw_net_impact_cache["data"]

    _EMPTY = {"bullish": {}, "bearish": {}}

    if not UNUSUAL_WHALES_API_KEY:
        return _EMPTY

    try:
        r = requests.get(
            "https://api.unusualwhales.com/api/market/top-net-impact",
            headers={"Authorization": f"Bearer {UNUSUAL_WHALES_API_KEY}"},
            params={"limit": 20},
            timeout=10,
        )
        r.raise_for_status()
        items = r.json().get("data", [])

        bullish: dict = {}
        bearish: dict = {}
        for item in items:
            t   = item.get("ticker", "")
            np_ = float(item.get("net_premium") or 0)
            if np_ > 0:
                bullish[t] = np_
            elif np_ < 0:
                bearish[t] = np_

        result = {"bullish": bullish, "bearish": bearish}
        _uw_net_impact_cache = {"data": result, "ts": now}
        logging.info(f"  UW net-impact: {len(bullish)} bullish, {len(bearish)} bearish tickers")
        return result

    except Exception as exc:
        logging.warning(f"  Unusual Whales net-impact fetch failed: {exc}")
        return _EMPTY


def _uw_net_impact_for_ticker(ticker: str, direction: str, net_impact: dict) -> dict:
    """
    Look up a ticker in the pre-fetched net-impact dict and return a
    confirmation result for the given signal direction.
    """
    _MISS = {"confirmed": False, "net_premium": 0.0, "note": ""}

    if direction == "CALL":
        np_ = net_impact.get("bullish", {}).get(ticker, 0.0)
        if np_ > 0:
            return {"confirmed": True, "net_premium": np_, "note": f"Top net call buying: +${np_/1e6:.1f}M ✅"}
    elif direction == "PUT":
        np_ = net_impact.get("bearish", {}).get(ticker, 0.0)
        if np_ < 0:
            return {"confirmed": True, "net_premium": np_, "note": f"Top net put buying: ${np_/1e6:.1f}M ✅"}

    return _MISS


def fetch_market_tide() -> dict:
    """
    Fetch today's cumulative directional flow proxy from /api/market/sector-etfs.

    The original /api/market/tide endpoint returns 404 at the current UW
    subscription tier (confirmed Jul 16 2026 — all other UW endpoints are 200).
    This function uses SPY's bullish/bearish premium split as the proxy:

      call_net = SPY bullish_premium − SPY bearish_premium
        > 0  → bullish trades dominating (bullish tape)
        < 0  → bearish trades dominating (bearish tape)

    put_net is set equal to call_net (in the original UW API the two metrics
    co-move: both positive in bull tapes, both negative in bear tapes).
    This is a stopgap — not a true independent put-flow signal.  If UW exposes
    a put-specific endpoint at this tier in future, replace it here.

    Scale note: this proxy runs at ~25% of the original endpoint's absolute
    values (SPY ETF flow only, not full market).  BDB Tier-1 (−$5M) fires
    correctly.  BDB Tier-2 (−$75M) will not fire until recalibrated —
    that is step 3 of the Jul-16 fix plan, after data from multiple market
    conditions is collected.

    Returns {"call_net": float, "put_net": float, "prev_call_net": float|None,
             "note": str}.
    Falls back to _NEUTRAL on any failure so fail-closed paths downstream
    catch it correctly.
    """
    global _market_tide_cache

    _NEUTRAL = {"call_net": 0.0, "put_net": 0.0,
                "note": "Market tide unavailable — gate skipped"}

    now = datetime.now(timezone.utc)
    cached_ts = _market_tide_cache.get("ts")
    if cached_ts and (now - cached_ts).total_seconds() < 300 and _market_tide_cache.get("data"):
        return _market_tide_cache["data"]

    if not UNUSUAL_WHALES_API_KEY:
        return _NEUTRAL

    try:
        r = requests.get(
            "https://api.unusualwhales.com/api/market/sector-etfs",
            headers={"Authorization": f"Bearer {UNUSUAL_WHALES_API_KEY}"},
            timeout=10,
        )
        r.raise_for_status()

        body = r.json()
        if not isinstance(body, dict):
            return _NEUTRAL
        data = body.get("data", [])
        spy  = next((d for d in data if d and d.get("ticker") == "SPY"), None)
        if not spy:
            return _NEUTRAL

        bull     = float(spy.get("bullish_premium") or 0)
        bear     = float(spy.get("bearish_premium") or 0)
        call_net = bull - bear
        put_net  = call_net  # co-directional in original UW API; same proxy here

        # prev_call_net from cache (5-min cache interval ≈ original API's bar interval)
        prev_call_net = _market_tide_cache.get("data", {}).get("call_net")

        note = (
            f"Market tide (sector-etfs proxy): {call_net/1e6:+.1f}M"
            f"  ({'BULL' if call_net > 0 else 'BEAR'})"
        )
        result = {"call_net": call_net, "put_net": put_net,
                  "prev_call_net": prev_call_net, "note": note}
        _market_tide_cache = {"data": result, "ts": now}
        logging.info(f"  🌊  {note}")
        return result

    except Exception as exc:
        logging.warning(f"  Market tide fetch failed: {exc}")
        return _NEUTRAL


def fetch_vix_level() -> Optional[float]:
    """
    Fetch VIX from Polygon's previous-close endpoint (I:VIX) with a 5-min cache.
    Used for regime-based sizing and the VIX panic-gate (no long-vol when VIX > 40).
    Previous close is precise enough — VIX regime classification doesn't need
    tick-level data, and this avoids a second Polygon indices call during scans.
    Returns None on failure so all VIX-dependent gates fail open (skip, don't block).
    """
    global _vix_cache
    now = datetime.now(timezone.utc)
    cached_ts = _vix_cache.get("ts")
    if cached_ts and (now - cached_ts).total_seconds() < 300 and _vix_cache.get("level") is not None:
        return _vix_cache["level"]
    if not POLYGON_API_KEY:
        return None
    try:
        r = requests.get(
            "https://api.polygon.io/v2/aggs/ticker/I:VIX/prev",
            params={"adjusted": "true", "apiKey": POLYGON_API_KEY},
            timeout=8,
        )
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            return None
        vix = float(results[0].get("c", 0))
        if vix > 0:
            _vix_cache = {"level": vix, "ts": now}
            logging.info(f"  📊  VIX: {vix:.1f}")
            return vix
        return None
    except Exception as exc:
        logging.warning(f"  VIX fetch failed: {exc}")
        return None


def fetch_sector_flow() -> dict:
    """
    Fetch SPDR sector ETF options data from UW (/api/market/sector-etfs).
    Returns per-sector bearish/bullish premium ratios and SPY-derived market
    sentiment for use in the sector flow gate and Claude synthesis context.

    bear_bull_ratio = bearish_premium / bullish_premium (UW pre-computed):
      > 1.3 → sector BEAR (suppress MOMENTUM BREAKOUT calls in this sector)
      < 0.5 → sector BULL (suppress BEARISH PUT in this sector)
      else  → NEUTRAL (no block)

    Returns an empty sentinel if UW is unavailable — gate always fails open.
    """
    global _sector_flow_cache
    _NEUTRAL = {"market_ratio": 1.0, "market_bias": "NEUTRAL", "sectors": {},
                "note": "Sector flow unavailable — gate skipped"}
    now = datetime.now(timezone.utc)
    cached_ts = _sector_flow_cache.get("ts")
    if cached_ts and (now - cached_ts).total_seconds() < 300 and _sector_flow_cache.get("data"):
        return _sector_flow_cache["data"]
    if not UNUSUAL_WHALES_API_KEY:
        return _NEUTRAL
    try:
        r = requests.get(
            "https://api.unusualwhales.com/api/market/sector-etfs",
            headers={"Authorization": f"Bearer {UNUSUAL_WHALES_API_KEY}"},
            timeout=10,
        )
        if r.status_code == 404:
            return _NEUTRAL
        r.raise_for_status()
        rows = r.json().get("data", [])
        if not rows:
            return _NEUTRAL

        sectors: dict = {}
        market_ratio = 1.0
        for row in rows:
            etf = row.get("ticker", "")
            bear = float(row.get("bearish_premium") or 0)
            bull = float(row.get("bullish_premium") or 0)
            call = float(row.get("call_premium") or 0)
            put  = float(row.get("put_premium")  or 0)
            if bull < 1:
                continue
            ratio = bear / bull
            # Only index meaningful flow — skip ETFs with tiny premium (<$500K combined)
            if bear + bull < 500_000:
                continue
            bias = "BEAR" if ratio > 1.3 else ("BULL" if ratio < 0.5 else "NEUTRAL")
            sectors[etf] = {
                "ratio": round(ratio, 3), "bias": bias,
                "bear": bear, "bull": bull, "call": call, "put": put,
                "name": row.get("full_name", etf),
            }
            if etf == "SPY":
                market_ratio = ratio

        parts = [f"SPY bear/bull={market_ratio:.2f}"]
        for etf, s in sectors.items():
            if etf != "SPY" and s["bias"] != "NEUTRAL":
                parts.append(f"{etf}:{s['bias']}({s['ratio']:.2f})")
        note = "Sector flow: " + " | ".join(parts[:6])
        market_bias = "BEAR" if market_ratio > 1.3 else ("BULL" if market_ratio < 0.5 else "NEUTRAL")
        result = {"market_ratio": round(market_ratio, 3), "market_bias": market_bias,
                  "sectors": sectors, "note": note}
        _sector_flow_cache = {"data": result, "ts": now}
        logging.info(f"  🏭  {note}")
        return result

    except Exception as exc:
        logging.warning(f"  Sector flow fetch failed: {exc}")
        return _NEUTRAL


def claude_synthesis(
    ticker: str,
    signal: str,
    score: int,
    reasons: list,
    tradeodds_info: Optional[dict],
    robinhood_data: dict,
    liquid_data: Optional[dict],
    uw_flow: Optional[dict] = None,
    uw_darkpool: Optional[dict] = None,
    net_impact: Optional[dict] = None,
    sector_flow: Optional[dict] = None,
) -> dict:
    """
    Call claude-sonnet-4-6 to synthesize all available data sources and return
    a structured verdict. Falls back to NEUTRAL on any failure so a Claude
    outage never silently kills signals.

    Verdict rules enforced here (not in the prompt) so the LLM can't bypass them:
      STRONG + confidence >= 70  → fires clean
      STRONG + confidence <  70  → downgraded to NEUTRAL
      NEUTRAL                    → fires with warning
      WEAK                       → suppressed, suppression notice posted
    """
    _FALLBACK = {"verdict": "NEUTRAL", "confidence": 50, "reason": "Claude synthesis unavailable — firing unconfirmed"}

    if not ANTHROPIC_API_KEY:
        logging.warning("  ANTHROPIC_API_KEY not set — Claude synthesis skipped, treating as NEUTRAL")
        return _FALLBACK

    td_text = tradeodds_info.get("note", "unavailable") if tradeodds_info else "unavailable"

    rb_parts = []
    if robinhood_data.get("pe_ratio") is not None:
        rb_parts.append(f"P/E {robinhood_data['pe_ratio']:.1f}x")
    if robinhood_data.get("earnings_days") is not None:
        rb_parts.append(f"earnings in {robinhood_data['earnings_days']}d")
    if robinhood_data.get("bid") and robinhood_data.get("ask"):
        rb_parts.append(f"options spread {robinhood_data.get('spread_pct', 0):.1f}%")
    rb_text = ", ".join(rb_parts) if rb_parts else "unavailable"

    if liquid_data:
        lq_text = (
            f"funding {liquid_data.get('funding_rate', 0):+.3f}%, "
            f"longs {liquid_data.get('long_pct', 0):.0f}%, "
            f"shorts {liquid_data.get('short_pct', 0):.0f}%"
        )
    else:
        lq_text = "unavailable"

    prompt = (
        f"Analyze this trading signal and give a final verdict.\n\n"
        f"Signal: {ticker} {signal}\n"
        f"QVIX Score: {score}/100\n"
        f"QVIX Reasons:\n" + "\n".join(f"- {r}" for r in reasons[:6]) + "\n"
        f"TradeOdds (10y historical): {td_text}\n"
        f"Unusual Whales options flow: {uw_flow['note'] if uw_flow else 'unavailable'}\n"
        f"Unusual Whales dark pool: {uw_darkpool['note'] if uw_darkpool and uw_darkpool.get('note') else 'unavailable'}\n"
        f"Market net impact: {net_impact['note'] if net_impact and net_impact.get('note') else 'unavailable'}\n"
        f"Sector flow: {sector_flow['note'] if sector_flow and sector_flow.get('note') else 'unavailable'}\n"
        f"Robinhood: {rb_text}\n"
        f"Liquid Positioning: {lq_text}\n\n"
        f"Respond in exactly this format — nothing else:\n"
        f"VERDICT: STRONG|NEUTRAL|WEAK\n"
        f"CONFIDENCE: <integer 0-100>\n"
        f"REASON: <one sentence, max 120 chars>"
    )

    for attempt in range(2):
        try:
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      "claude-sonnet-4-6",
                    "max_tokens": 100,
                    "messages":   [{"role": "user", "content": prompt}],
                },
                timeout=15,
            )
            r.raise_for_status()
            text = r.json()["content"][0]["text"].strip()

            verdict = confidence = reason = None
            for line in text.splitlines():
                if line.startswith("VERDICT:"):
                    verdict = line.split(":", 1)[1].strip()
                elif line.startswith("CONFIDENCE:"):
                    try:
                        confidence = int(line.split(":", 1)[1].strip())
                    except ValueError:
                        pass
                elif line.startswith("REASON:"):
                    reason = line.split(":", 1)[1].strip()

            if verdict not in ("STRONG", "NEUTRAL", "WEAK") or confidence is None or not reason:
                raise ValueError(f"Unexpected response format: {text!r}")

            if verdict == "STRONG" and confidence < 70:
                verdict = "NEUTRAL"

            logging.info(f"  Claude synthesis: {verdict} ({confidence}) — {reason}")
            return {"verdict": verdict, "confidence": confidence, "reason": reason}

        except Exception as exc:
            if attempt == 0:
                logging.warning(f"  Claude synthesis failed, retrying: {exc}")
                time.sleep(2)
                continue
            logging.warning(f"  Claude synthesis failed after retry: {exc} — treating as NEUTRAL")
            return _FALLBACK


def _send_suppression_notice(ticker: str, signal: str, score: int, synthesis: dict, crypto: bool = False):
    _wh = DISCORD_CRYPTO_WEBHOOK_URL if crypto else None
    _post_embed({
        "title":       f"🚫 Signal Suppressed — {ticker} {signal}",
        "color":       0x888888,
        "description": f"Scored **{score}/100** but Claude rated **WEAK** — not fired.\n> {synthesis['reason']}",
        "footer":      {"text": f"QVIX 5.1  ·  {datetime.now(ET).strftime('%I:%M %p ET')}"},
        "timestamp":   datetime.utcnow().isoformat() + "Z",
    }, webhook_url=_wh)


# ─── MAIN SCAN ─────────────────────────────────────────────────────────────────

def run_scan():
    now = datetime.now(ET)

    # ── Daily history: load once per trading day, BEFORE the market-open gate.
    # This refresh is synchronous and takes ~13s/ticker (free-tier rate limit)
    # — for 54 tickers that's ~12 minutes. Gating it behind is_market_open()
    # meant it only ever started right at 9:30:00, freezing the entire loop
    # (crypto scans, market-open logging, the 0DTE scanner) for the first ~12
    # minutes of every trading day. Running it here instead lets it complete
    # during pre-market, on the first weekday loop tick before 9:30.
    if now.weekday() < 5 and _cache_date != date.today():
        refresh_daily_cache()
        now = datetime.now(ET)   # refresh can take several minutes — re-read the clock

    if not is_market_open():
        logging.info(f"Market closed ({now.strftime('%A %H:%M ET')}) — stocks paused, crypto active")
        return

    now = datetime.now(ET)
    logging.info(f"━━━  Scan  {now.strftime('%H:%M:%S ET')}  ━━━")
    session_fraction = session_elapsed_fraction()

    # ── Unusual Whales market-wide data: one call each, shared across tickers ───
    uw_net_impact = fetch_market_net_impact()
    market_tide   = fetch_market_tide()
    vix_level     = fetch_vix_level()
    sector_flow   = fetch_sector_flow()

    # ── ONE API call for every ticker's current data ──────────────────────────
    try:
        snapshots = fetch_all_snapshots()
        logging.info(f"  Snapshot: {len(snapshots)}/{len(WATCHLIST)} tickers")
    except requests.HTTPError as exc:
        logging.error(f"  Snapshot fetch failed: HTTP {exc.response.status_code}  {exc}")
        return
    except Exception as exc:
        logging.error(f"  Snapshot fetch failed: {exc}")
        return

    tick_results: dict = {}

    for ticker in WATCHLIST:
        try:
            snap = snapshots.get(ticker)
            if not snap:
                logging.warning(f"  {ticker}: missing from snapshot — skipping")
                continue

            daily = list(_daily_cache.get(ticker, []))
            if not daily:
                logging.warning(f"  {ticker}: no daily history in cache — skipping")
                continue

            # Splice today's live bar into the tail of the daily history so that
            # RSI / MACD / volume calculations reflect the current session.
            day_data = snap.get("day", {})
            if day_data.get("c"):
                today_bar = {
                    "o": day_data.get("o", day_data["c"]),
                    "h": day_data.get("h", day_data["c"]),
                    "l": day_data.get("l", day_data["c"]),
                    "c": day_data["c"],
                    "v": day_data.get("v", 0),
                }
                daily = daily[:-1] + [today_bar]

            # Build a 2-bar synthetic intraday list from the snapshot so that:
            #   • intra_change_pct  → (close - open) / open  (crash alert)
            #   • calc_vwap         → typical price from today's H/L/C
            #   • evaluators        → intraday[-1]["c"] for current price
            # NOTE: use `or`, not dict.get(key, default) — Polygon returns explicit
            # 0.0 fields (not missing keys) for "day" before the session has any
            # trades, and .get(key, default) only falls back on a MISSING key, so
            # a present-but-zero "c" was passing straight through as price $0.00.
            # Prefer the live last-trade price (refreshed on every snapshot call)
            # over the minute aggregate, which only updates once per minute bar
            # and can otherwise make the price look frozen between scans.
            last_trade = snap.get("lastTrade", {})
            min_bar    = snap.get("min", {})
            open_px    = day_data.get("o") or daily[-1]["c"]
            close_px   = last_trade.get("p") or min_bar.get("c") or day_data.get("c") or open_px
            intraday = [
                {"o": open_px,  "h": open_px,                          "l": open_px,                          "c": open_px,  "v": 0},
                {"o": open_px,  "h": day_data.get("h") or close_px,    "l": day_data.get("l") or close_px,    "c": close_px, "v": day_data.get("v", 0)},
            ]

            price = float(close_px or daily[-1]["c"])

            mb  = eval_momentum_breakout(ticker, daily, intraday, session_fraction)
            bp  = eval_bearish_put(ticker, daily, intraday, session_fraction)
            bdb = eval_big_drop_bounce(ticker, daily, intraday) if ticker not in BDB_EXCLUDED_TICKERS else {"score": 0, "reasons": ["ticker excluded from BDB"], "metrics": {}}

            tick_results[ticker] = {
                "momentum_breakout": mb,
                "bearish_put":       bp,
                "big_drop_bounce":   bdb,
                "intraday":          intraday,
                "price":             price,
            }

            for sig_name, result in [("MOMENTUM BREAKOUT", mb), ("BEARISH PUT", bp)]:
                # Long-bias tickers never trigger a BEARISH PUT — their put
                # signals are structurally noisy and primary risk is to upside.
                if sig_name == "BEARISH PUT" and ticker in LONG_BIAS_TICKERS:
                    continue

                if sig_name == "BEARISH PUT" and ticker in BEARISH_PUT_BLOCKED:
                    continue

                metrics = result.get("metrics", {})

                # MOMENTUM BREAKOUT uses a floor (78) and a ceiling (85).
                # Floor: below 78, breakout signal is too weak.
                # Ceiling: above 85, setup is overbought not high-conviction
                # (90–100 score bucket: 20% WR, -197% net across 5 signals).
                sig_min_score = MOMENTUM_BREAKOUT_MIN_SCORE if sig_name == "MOMENTUM BREAKOUT" else ALERT_MIN_SCORE
                fire = _should_alert(
                    ticker, sig_name, result["score"],
                    min_score=sig_min_score,
                    current_vol=metrics.get("current_vol"),
                    avg_vol_20d=metrics.get("avg_vol_20d"),
                    session_fraction=session_fraction,
                )

                if fire and sig_name == "MOMENTUM BREAKOUT" and result["score"] > MOMENTUM_BREAKOUT_MAX_SCORE:
                    logging.info(
                        f"  📊  SCORE CEILING  {ticker} MOMENTUM BREAKOUT: "
                        f"score {result['score']} > {MOMENTUM_BREAKOUT_MAX_SCORE} — overbought, suppressed"
                    )
                    fire = False

                if fire and sig_name == "MOMENTUM BREAKOUT" and now.time() >= dtime(10, 0):
                    logging.info(
                        f"  ⏰  WINDOW CLOSE  {ticker} MOMENTUM BREAKOUT: "
                        f"after 10:00 ET — late-session breakouts 17% WR historically, suppressed"
                    )
                    fire = False

                # ── Bull-trend block for BEARISH PUT ─────────────────────────
                # If the stock is trading more than 10% above its 50-day MA it
                # is in a confirmed bull trend. Shorting it via puts fights the
                # primary trend and has a losing track record (NVDA Jul 3 & 8).
                if fire and sig_name == "BEARISH PUT":
                    _closes_arr = np.array([b["c"] for b in daily], dtype=float)
                    if len(_closes_arr) >= 50:
                        _ma50     = float(np.mean(_closes_arr[-50:]))
                        _pct_abv  = (price - _ma50) / _ma50 * 100
                        if _pct_abv > 10:
                            logging.info(
                                f"  📈  BULL TREND BLOCK  {ticker} BEARISH PUT: "
                                f"price {_pct_abv:.1f}% above 50D MA — fighting the trend"
                            )
                            fire = False

                # ── Opening auction blackout (9:30–9:45 ET) ──────────────────
                # The first 15 minutes are dominated by gap fills, order imbalances,
                # and price discovery noise — directional signals here are coin flips.
                if fire and now.time() < dtime(9, 45):
                    logging.info(
                        f"  🔔  AUCTION BLOCK  {ticker} {sig_name}: "
                        f"opening auction window before 9:45 AM — signal suppressed"
                    )
                    fire = False

                # ── Earnings blackout (≤3 days to ER) ───────────────────────
                # IV crush after an earnings report destroys directional option P&L
                # regardless of the underlying move. Block new entries within 3 days
                # of earnings. Robinhood enrichment file is a local JSON read — no
                # extra network call.
                if fire:
                    _early_rb   = fetch_robinhood_enrichment(ticker, None)
                    _earn_days  = _early_rb.get("earnings_days")
                    if _earn_days is not None and 0 <= _earn_days <= 3:
                        logging.info(
                            f"  📅  EARNINGS BLOCK  {ticker} {sig_name}: "
                            f"earnings in {_earn_days}d — IV crush risk, signal suppressed"
                        )
                        fire = False

                # ── VIX regime gate ──────────────────────────────────────────
                # Buying calls when VIX > 40 means buying at peak implied vol —
                # the premium is astronomically inflated and any rally gets crushed
                # by vol contraction. Puts in panic are fine (you're selling risk
                # into a demand spike), so the hard block is calls-only above 40.
                # VIX 30–40: gate skips but position size is halved (see
                # _calculate_position_size). VIX unavailable → fail open.
                if fire and vix_level is not None and vix_level > 40 and sig_name == "MOMENTUM BREAKOUT":
                    logging.info(
                        f"  📊  VIX BLOCK  {ticker} {sig_name}: "
                        f"VIX {vix_level:.1f} > 40 — long-vol suppressed in panic regime"
                    )
                    fire = False

                # ── Sector flow gate ─────────────────────────────────────────
                # One UW call covers all tickers: block bullish calls in sectors
                # where institutions are buying puts (bear_bull_ratio > 1.3) and
                # block bearish puts in sectors where institutions are buying calls
                # (bear_bull_ratio < 0.5). SPY ratio is the market-wide proxy.
                # Min combined premium $500K to avoid thin-flow false signals.
                # Tickers not in TICKER_SECTOR (indexes, ETFs) skip this gate.
                if fire:
                    _sector_etf = TICKER_SECTOR.get(ticker)
                    if _sector_etf:
                        _sec      = sector_flow.get("sectors", {}).get(_sector_etf, {})
                        _sec_ratio = _sec.get("ratio", 1.0)
                        _mkt_ratio = sector_flow.get("market_ratio", 1.0)
                        _sec_bias  = _sec.get("bias", "NEUTRAL")
                        _mkt_bias  = sector_flow.get("market_bias", "NEUTRAL")

                        if sig_name == "MOMENTUM BREAKOUT" and _sec_bias == "BEAR":
                            logging.info(
                                f"  🏭  SECTOR BLOCK  {ticker} MOMENTUM BREAKOUT: "
                                f"{_sector_etf} bear/bull={_sec_ratio:.2f} — sector flow bearish"
                            )
                            fire = False
                        elif sig_name == "MOMENTUM BREAKOUT" and _mkt_bias == "BEAR" and _sec_bias != "BULL":
                            logging.info(
                                f"  🏭  MARKET FLOW BLOCK  {ticker} MOMENTUM BREAKOUT: "
                                f"SPY bear/bull={_mkt_ratio:.2f} — market-wide flow bearish"
                            )
                            fire = False
                        elif sig_name == "BEARISH PUT" and _sec_bias == "BULL":
                            logging.info(
                                f"  🏭  SECTOR BLOCK  {ticker} BEARISH PUT: "
                                f"{_sector_etf} bear/bull={_sec_ratio:.2f} — sector flow strongly bullish"
                            )
                            fire = False
                        elif sig_name == "BEARISH PUT" and _mkt_bias == "BULL" and _sec_bias != "BEAR":
                            logging.info(
                                f"  🏭  MARKET FLOW BLOCK  {ticker} BEARISH PUT: "
                                f"SPY bear/bull={_mkt_ratio:.2f} — market-wide flow strongly bullish"
                            )
                            fire = False

                tradeodds_info = None
                if fire:
                    direction = "CALL" if sig_name == "MOMENTUM BREAKOUT" else "PUT"
                    tradeodds_result = fetch_tradeodds_validation(
                        ticker,
                        conditions={"daily_change": True, "vix_level": True, "regime": True, "rel_vol": True},
                        forward_period=STOCK_TRADEODDS_FORWARD,
                        reference_period=STOCK_TRADEODDS_REFERENCE,
                    )
                    fire, tradeodds_info = _stock_tradeodds_confirms(direction, tradeodds_result)
                    metrics["tradeodds"] = tradeodds_info

                    # Belt-and-suspenders: hard block any signal where TradeOdds
                    # returned unconfirmed and this is NOT the extended-outage
                    # fallback. _stock_tradeodds_confirms already does this, but
                    # an explicit check here makes the invariant unbreakable.
                    if (fire and tradeodds_info
                            and not tradeodds_info.get("tradeodds_confirmed")
                            and not tradeodds_info.get("outage_fallback")
                            and not tradeodds_info.get("low_sample_fallback")):
                        fire = False
                        logging.warning(f"  {ticker} {sig_name}: TradeOdds unconfirmed (non-outage) — hard blocked")

                # ── Market Tide Gate ─────────────────────────────────────────
                # Block signals that fight today's established institutional flow.
                # The cumulative market tide shows where real money is actually
                # going, not where technicals suggest it should go.
                #
                # Guard: skip before 10:00 AM ET — the opening auction creates
                # noisy early imbalances that resolve within 30 minutes.
                # Threshold: ±$5M minimum before blocking to ignore trivial noise.
                if fire and now.time() >= dtime(10, 0):
                    call_net = market_tide.get("call_net", 0.0)
                    if sig_name == "MOMENTUM BREAKOUT" and call_net < -5_000_000:
                        logging.info(
                            f"  🌊  TIDE BLOCK  {ticker} MOMENTUM BREAKOUT: "
                            f"net call flow {call_net/1e6:+.1f}M — bulls fighting the tape"
                        )
                        fire = False
                    elif sig_name == "BEARISH PUT":
                        _bp_note = market_tide.get("note", "")
                        if call_net == 0.0 and "unavailable" in _bp_note:
                            # Fail-closed: API down — suppress low-confidence signals
                            if result["score"] < 85:
                                logging.warning(
                                    f"  🌊  TIDE UNAVAILABLE  {ticker} BEARISH PUT: "
                                    f"API down — score {result['score']} < 85, suppressing"
                                )
                                fire = False
                            else:
                                logging.warning(
                                    f"  🌊  TIDE UNAVAILABLE  {ticker} BEARISH PUT: "
                                    f"API down — score {result['score']} ≥ 85, allowing through"
                                )
                        elif call_net > 5_000_000:
                            # Bullish tape — bears fighting the flow
                            # PROVISIONAL threshold: calibrate from bull-tape data (step 3)
                            logging.info(
                                f"  🌊  TIDE BLOCK  {ticker} BEARISH PUT: "
                                f"call net {call_net/1e6:+.1f}M — bullish tape, "
                                f"bears fighting the flow"
                            )
                            fire = False

                # ── Multi-source enrichment + Claude synthesis gate ─────────
                # Order: TradeOdds → Market Tide → UW flow (+10/-12) → UW darkpool (+5) →
                #        net-impact (+5) → Claude STRONG/NEUTRAL/WEAK.
                # effective_score drives synthesis and Discord display;
                # result["score"] (raw QVIX) is preserved for audit logs.
                synthesis:       Optional[dict] = None
                plan:            Optional[dict] = None
                robinhood_data:  dict           = {}
                liquid_data:     Optional[dict] = None
                uw_flow:         Optional[dict] = None
                uw_darkpool:     Optional[dict] = None
                ni_result:       Optional[dict] = None
                effective_score: int            = result["score"]
                if fire:
                    direction      = "CALL" if sig_name == "MOMENTUM BREAKOUT" else "PUT"
                    plan           = _option_plan(sig_name, result["score"], price, ticker)

                    # Reject same-day expiry — 0DTE theta decay makes these
                    # unreliable for swing signals; the dedicated 0DTE scanner
                    # handles intentional same-day plays with its own logic.
                    if plan and plan.get("days_out", 1) == 0:
                        logging.info(f"  {ticker} {sig_name}: same-day expiry rejected ({plan['expiry']} is today) — skipping")
                        fire = False

                    if fire:
                        robinhood_data = fetch_robinhood_enrichment(ticker, plan)
                        liquid_data    = fetch_liquid_positioning(ticker)
                        uw_flow        = fetch_unusual_whales_flow(ticker, direction)
                        uw_darkpool    = fetch_unusual_whales_darkpool(ticker, direction)
                        ni_result      = _uw_net_impact_for_ticker(ticker, direction, uw_net_impact)

                        # Contrary flow hard block: if opposite-direction institutional
                        # premium in the last 2h is > 2× confirming premium AND > $500K,
                        # the smart money is clearly betting against this signal.
                        contrary = uw_flow.get("contrary_premium", 0.0)
                        confirming = uw_flow.get("premium", 0.0)
                        if contrary > confirming * 2 and contrary > 500_000:
                            logging.info(
                                f"  🚫  FLOW BLOCK  {ticker} {sig_name}: "
                                f"contrary UW flow ${contrary/1e6:.1f}M >> confirming ${confirming/1e6:.1f}M"
                            )
                            fire = False

                        boost = 0
                        if fire and uw_flow.get("confirmed"):    boost += 10
                        if fire and uw_darkpool.get("confirmed"): boost += 5
                        if fire and ni_result.get("confirmed"):   boost += 5
                        if boost:
                            effective_score = min(100, result["score"] + boost)
                            logging.info(f"  UW boost +{boost}: {result['score']} → {effective_score} ({ticker} {sig_name})")

                        metrics["uw_flow"]     = uw_flow
                        metrics["uw_darkpool"] = uw_darkpool
                        metrics["net_impact"]  = ni_result
                        synthesis = claude_synthesis(
                            ticker, sig_name, effective_score, result["reasons"],
                            tradeodds_info, robinhood_data, liquid_data,
                            uw_flow=uw_flow, uw_darkpool=uw_darkpool, net_impact=ni_result,
                            sector_flow=sector_flow,
                        )
                        metrics["synthesis"] = synthesis
                        if synthesis["verdict"] == "WEAK":
                            fire = False

                log_decision(ticker, sig_name, result, fire)
                if fire:
                    unconfirmed = tradeodds_info is not None and tradeodds_info.get("tradeodds_confirmed") is False
                    use_standard_alert = True

                    if effective_score >= MULTI_AGENT_SCORE_THRESHOLD:
                        logging.info(f"  🤖  Score {effective_score}/100 ≥ {MULTI_AGENT_SCORE_THRESHOLD} — running multi-agent pipeline for {ticker}…")
                        ma = _run_multi_agent_analysis(ticker)
                        if ma is not None:
                            ma_verdict  = ma.get("synthesis", {}).get("verdict", "HOLD")
                            ma_decision = ma.get("risk", {}).get("decision", "REJECTED")
                            if ma_verdict in ("BUY", "SELL", "CALENDAR_SPREAD"):
                                # Enriched card was posted by portfolio_agent — skip basic card
                                logging.info(f"  ✅  ALERT  {ticker}  {sig_name}  {effective_score}/100  — multi-agent {ma_verdict} {ma_decision}")
                            else:
                                # HOLD — suppress entirely, no Discord card
                                logging.info(f"  🚫  SUPPRESSED  {ticker}  {sig_name}  — multi-agent HOLD: {ma.get('synthesis', {}).get('reasoning', '')[:80]}")
                                fire = False
                            use_standard_alert = False
                        else:
                            # Pipeline failed — fall back to standard alert for this signal
                            logging.warning(f"  ⚠️  Multi-agent failed — falling back to standard alert for {ticker}")

                    if use_standard_alert:
                        logging.info(f"  🔔  ALERT  {ticker}  {sig_name}  {effective_score}/100  — {synthesis['verdict']} ({synthesis['confidence']})")
                        send_alert(ticker, sig_name, effective_score, result["reasons"], price,
                                   metrics=result.get("metrics", {}), opt=plan,
                                   tradeodds_note=tradeodds_info.get("note") if unconfirmed else None,
                                   synthesis=synthesis, robinhood_data=robinhood_data, liquid_data=liquid_data,
                                   uw_flow=uw_flow, uw_darkpool=uw_darkpool)

                    if fire:
                        # Stamp cooldown only now — all gates cleared
                        _cooldown_ok(ticker, sig_name, result["score"], STOCK_COOLDOWN_SECS)
                        entry_premium = (
                            (plan["entry_low"] + plan["entry_high"]) / 2
                            if plan and plan.get("real_data") else None
                        )
                        if entry_premium is None:
                            logging.warning(
                                f"  ⚠️  {ticker} {sig_name}: no real option quote available — "
                                f"signal logged but tp/sl/entry_premium will be null; "
                                f"record auto-expires in {SIGNAL_EXPIRY_HOURS}h if unresolved"
                            )
                        _log_signal(ticker, sig_name, direction, result["score"], price,
                                    plan.get("target") if plan else None, plan.get("stop") if plan else None,
                                    strike=plan.get("strike") if plan else None,
                                    expiry=plan.get("expiry") if plan else None,
                                    entry_premium=entry_premium,
                                    tradeodds_confirmed=tradeodds_info.get("tradeodds_confirmed") if tradeodds_info else None,
                                    signal_type="stock_option",
                                    low_sample_fallback=bool(tradeodds_info.get("low_sample_fallback")) if tradeodds_info else False,
                                    synthesis_verdict=synthesis.get("verdict") if synthesis else None,
                                    synthesis_confidence=synthesis.get("confidence") if synthesis else None)
                        if LIQUID_AUTO_EXECUTE_ENABLED:
                            logging.warning("  LIQUID_AUTO_EXECUTE_ENABLED is True but no execution path is wired in — see qvix.py comments. Logging dry-run only.")
                        _log_liquid_dry_run(ticker, sig_name, result["score"],
                                             tradeodds_confirmed=tradeodds_info.get("tradeodds_confirmed") if tradeodds_info else None,
                                             vix_level=vix_level)
                        if RH_AUTO_EXECUTE_ENABLED and plan and plan.get("strike") and plan.get("expiry"):
                            if Path("logs/rh_kill_switch").exists():
                                logging.warning(f"  🛑  RH kill switch active — {ticker} {sig_name} NOT queued")
                                _send_rh_block_alert(ticker, sig_name, "Kill switch active (logs/rh_kill_switch exists)")
                            elif not _check_circuit_breaker():
                                logging.warning(f"  🛑  Circuit breaker halted RH queue — {ticker} {sig_name} NOT queued")
                                _send_rh_block_alert(ticker, sig_name, "Circuit breaker tripped — daily loss limit hit")
                            else:
                                rh_ref = _rh_queue_order(ticker, sig_name, plan)
                                _update_signal(ticker, sig_name, robinhood_order_id=rh_ref)
                elif synthesis and synthesis["verdict"] == "WEAK":
                    logging.info(f"  🚫  SUPPRESSED  {ticker}  {sig_name}  — Claude WEAK: {synthesis['reason']}")
                    _send_suppression_notice(ticker, sig_name, effective_score, synthesis)
                else:
                    tag = "↑" if sig_name == "MOMENTUM BREAKOUT" else "↓"
                    suffix = f"  ({tradeodds_info['note']})" if tradeodds_info else ""
                    logging.info(f"  {tag}  {ticker:<6}  {sig_name:<22}  {result['score']:>3}/100{suffix}")

            # ── BIG DROP BOUNCE ──────────────────────────────────────────────
            # Stock has pulled back 5%+ from 10-day high. If call/put open
            # interest at the ATM strike is ≥ 2:1, smart money is positioned
            # for a bounce — fire a bullish call signal.
            if bdb["score"] >= 45:
                oi_data  = _big_drop_oi_ratio(ticker, price)
                oi_ratio = oi_data.get("ratio", 0.0)
                logging.info(f"  [BDB] {ticker}  score={bdb['score']}/100  OI ratio={oi_ratio:.2f}  (threshold 1.2)")

                if oi_ratio >= 1.2:
                    # OI ≥ 5.0 → 92 clears the ≥ 90 Tide-unavailable override threshold
                    if oi_ratio >= 5.0:
                        bdb_score = 92
                    elif oi_ratio >= 3.0:
                        bdb_score = 88
                    else:
                        bdb_score = 82
                    fire_bdb  = True

                    # ── Suspension gate ───────────────────────────────────────
                    # BDB is suspended pending 5 clean signals under the new
                    # logic (Market Tide + same-day flow checks). Flip
                    # BDB_SUSPENDED = False once those conditions are met.
                    if BDB_SUSPENDED:
                        logging.info(f"  [BDB] {ticker}: suspended — re-enable after 5 clean signals")
                        fire_bdb = False

                    # ── Cooldown gate ─────────────────────────────────────────
                    # Routes through _should_alert / _cooldown_ok exactly like
                    # MOMENTUM BREAKOUT — one Discord alert per ticker per
                    # calendar day max. Previously BDB bypassed this entirely,
                    # causing the same signal to log and fire every scan cycle.
                    if fire_bdb and not _should_alert(ticker, "BIG DROP BOUNCE", bdb_score):
                        logging.info(f"  [BDB] {ticker}: cooldown active — suppressed")
                        fire_bdb = False

                    if fire_bdb and now.time() < dtime(10, 30):
                        logging.info(f"  [BDB] {ticker}: pre-10:30 window — suppressed")
                        fire_bdb = False

                    if fire_bdb and vix_level is not None and vix_level > 40:
                        logging.info(f"  [BDB] {ticker}: VIX {vix_level:.1f} > 40 — suppressed")
                        fire_bdb = False

                    if fire_bdb:
                        _bdb_rb    = fetch_robinhood_enrichment(ticker, None)
                        _earn_days = _bdb_rb.get("earnings_days")
                        if _earn_days is not None and 0 <= _earn_days <= 3:
                            logging.info(f"  [BDB] {ticker}: earnings in {_earn_days}d — suppressed")
                            fire_bdb = False

                    if fire_bdb:
                        bdb_plan = _option_plan("BIG DROP BOUNCE", bdb_score, price, ticker)
                        if bdb_plan and bdb_plan.get("days_out", 1) == 0:
                            logging.info(f"  [BDB] {ticker}: same-day expiry rejected")
                            fire_bdb = False

                    # ── Market Tide gate — two-tier ───────────────────────────
                    # Tier 1 (mild):   call_net < -$5M → suppress (tape weak)
                    # Tier 2 (severe): call_net < -$75M AND still falling
                    #   → suppress unless score ≥ 90 (high-conviction override)
                    # Covers the full session including 9:30–10 AM — the opening
                    # auction noise rationale was removed after TSLA/TER fired
                    # directly into a risk-off open (Jul 15). On API failure
                    # (call_net = 0.0) the gate logs a warning and allows through.
                    if fire_bdb:
                        _bdb_call_net = market_tide.get("call_net", 0.0)
                        _bdb_prev_net = market_tide.get("prev_call_net")
                        _tide_falling = (
                            _bdb_prev_net is not None
                            and _bdb_call_net < _bdb_prev_net
                        )
                        if _bdb_call_net == 0.0 and "unavailable" in market_tide.get("note", ""):
                            if bdb_score < 90:
                                logging.warning(
                                    f"  🌊  TIDE UNAVAILABLE  {ticker} BIG DROP BOUNCE: "
                                    f"API down — score {bdb_score} < 90 minimum, suppressing"
                                )
                                fire_bdb = False
                            else:
                                logging.warning(
                                    f"  🌊  TIDE UNAVAILABLE  {ticker} BIG DROP BOUNCE: "
                                    f"API down — score {bdb_score} ≥ 90, allowing through"
                                )
                        elif _bdb_call_net < -75_000_000 and _tide_falling:
                            if bdb_score < 90:
                                logging.info(
                                    f"  🌊  TIDE BLOCK  {ticker} BIG DROP BOUNCE: "
                                    f"severe risk-off {_bdb_call_net/1e6:+.1f}M "
                                    f"(prev {_bdb_prev_net/1e6:+.1f}M, falling) — "
                                    f"score {bdb_score} < 90 minimum"
                                )
                                fire_bdb = False
                            else:
                                logging.info(
                                    f"  🌊  TIDE WARN  {ticker} BIG DROP BOUNCE: "
                                    f"severe risk-off {_bdb_call_net/1e6:+.1f}M — "
                                    f"score {bdb_score} ≥ 90, allowing through"
                                )
                        elif _bdb_call_net < -5_000_000:
                            logging.info(
                                f"  🌊  TIDE BLOCK  {ticker} BIG DROP BOUNCE: "
                                f"net call flow {_bdb_call_net/1e6:+.1f}M — bulls fighting the tape"
                            )
                            fire_bdb = False

                    # ── Same-day flow check at target strike ──────────────────
                    # OI reflects accumulated positioning; same-day volume at the
                    # target strike reveals what smart money is doing *today*.
                    # Suppress if put volume > call volume at that strike.
                    # Guard: only apply when either side has traded — avoids
                    # suppressing legitimate pre-market setups before the option
                    # has printed its first trade of the session.
                    if fire_bdb:
                        _call_vol = oi_data.get("call_vol", 0)
                        _put_vol  = oi_data.get("put_vol",  0)
                        if _call_vol + _put_vol > 0 and _put_vol > _call_vol:
                            logging.info(
                                f"  📉  FLOW BLOCK  {ticker} BIG DROP BOUNCE: "
                                f"same-day put vol {_put_vol:,} > call vol {_call_vol:,} "
                                f"at ${oi_data.get('strike')} strike — flow contradicts bounce"
                            )
                            fire_bdb = False
                        else:
                            logging.info(
                                f"  [BDB] {ticker}: strike flow OK  "
                                f"call_vol={_call_vol:,}  put_vol={_put_vol:,}"
                            )

                    # ── TradeOdds gate — BDB is always a CALL (bounce) ───────
                    if fire_bdb:
                        bdb_to_result = fetch_tradeodds_validation(
                            ticker,
                            conditions={"daily_change": True, "vix_level": True, "regime": True, "rel_vol": True},
                            forward_period=STOCK_TRADEODDS_FORWARD,
                            reference_period=STOCK_TRADEODDS_REFERENCE,
                        )
                        bdb_to_fire, bdb_to_info = _stock_tradeodds_confirms("CALL", bdb_to_result)
                        if not bdb_to_fire:
                            logging.info(f"  [BDB] {ticker}: TradeOdds blocked — {bdb_to_info.get('note','')}")
                            fire_bdb = False
                        elif not bdb_to_info.get("tradeodds_confirmed"):
                            logging.info(f"  [BDB] {ticker}: TradeOdds fallback — {bdb_to_info.get('note','')}")
                        else:
                            logging.info(f"  [BDB] {ticker}: TradeOdds confirmed — {bdb_to_info.get('note','')}")

                    if fire_bdb:
                        # Stamp cooldown only now — signal has cleared all gates
                        cooldown_secs = STOCK_COOLDOWN_SECS
                        _cooldown_ok(ticker, "BIG DROP BOUNCE", bdb_score, cooldown_secs)
                        bdb_reasons = bdb["reasons"] + [
                            f"Call/Put OI {oi_ratio:.1f}× at ${oi_data.get('strike')} strike "
                            f"({oi_data.get('call_oi', 0):,} calls vs {oi_data.get('put_oi', 0):,} puts) "
                            f"— smart money positioned for bounce"
                        ]
                        entry_premium = (
                            (bdb_plan["entry_low"] + bdb_plan["entry_high"]) / 2
                            if bdb_plan and bdb_plan.get("real_data") else None
                        )
                        send_alert(ticker, "BIG DROP BOUNCE", bdb_score, bdb_reasons, price,
                                   metrics={**bdb["metrics"], "oi_ratio": oi_ratio},
                                   opt=bdb_plan)
                        _log_signal(ticker, "BIG DROP BOUNCE", "CALL", bdb_score, price,
                                    bdb_plan.get("target") if bdb_plan else None,
                                    bdb_plan.get("stop") if bdb_plan else None,
                                    strike=bdb_plan.get("strike") if bdb_plan else oi_data.get("strike"),
                                    expiry=bdb_plan.get("expiry") if bdb_plan else oi_data.get("expiry"),
                                    entry_premium=entry_premium,
                                    tradeodds_confirmed=None,
                                    signal_type="stock_option",
                                    synthesis_verdict=None,
                                    synthesis_confidence=None)
                        logging.info(f"  🏹  BIG DROP BOUNCE  {ticker}  {bdb_score}/100  OI={oi_ratio:.1f}×")
                        if RH_AUTO_EXECUTE_ENABLED and bdb_plan and bdb_plan.get("strike") and bdb_plan.get("expiry"):
                            if Path("logs/rh_kill_switch").exists():
                                logging.warning(f"  🛑  RH kill switch active — {ticker} BIG DROP BOUNCE NOT queued")
                                _send_rh_block_alert(ticker, "BIG DROP BOUNCE", "Kill switch active (logs/rh_kill_switch exists)")
                            elif not _check_circuit_breaker():
                                logging.warning(f"  🛑  Circuit breaker halted RH queue — {ticker} BIG DROP BOUNCE NOT queued")
                                _send_rh_block_alert(ticker, "BIG DROP BOUNCE", "Circuit breaker tripped — daily loss limit hit")
                            else:
                                rh_ref = _rh_queue_order(ticker, "BIG DROP BOUNCE", bdb_plan)
                                _update_signal(ticker, "BIG DROP BOUNCE", robinhood_order_id=rh_ref)

        except Exception as exc:
            logging.error(f"  {ticker}: {exc}")

    # ── 0DTE INTRADAY MANAGEMENT (TARGET/CLOSE/REVERSAL) ───────────────────────
    # Runs every cycle this function runs (i.e. every 5 min, market hours
    # only — run_scan() already returns early when closed), reusing SPY's
    # snapshot from the loop above. No-ops if no 0DTE signal fired today.
    try:
        check_zero_dte_position(tick_results)
    except Exception as exc:
        logging.error(f"  0DTE position check: {exc}")

    # ── MARKET CRASH ALERT ────────────────────────────────────────────────────
    spy_intra = tick_results.get("SPY", {}).get("intraday", [])
    qqq_intra = tick_results.get("QQQ", {}).get("intraday", [])

    crash = eval_crash_alert(tick_results, spy_intra, qqq_intra)
    fire_crash = _should_alert("MARKET", "MARKET CRASH ALERT", crash["score"], CRASH_MIN_SCORE)
    log_decision("MARKET", "MARKET CRASH ALERT", crash, fire_crash)

    if fire_crash:
        logging.info(f"  🚨  CRASH ALERT  score={crash['score']}/100")
        send_alert("MARKET", "MARKET CRASH ALERT", crash["score"], crash["reasons"])
        _cooldown_ok("MARKET", "MARKET CRASH ALERT", crash["score"], STOCK_COOLDOWN_SECS)
    else:
        logging.info(f"  🌡️  Crash score: {crash['score']}/100  (threshold {CRASH_MIN_SCORE})")

    # ── EXIT TRACKER — check open signals for tp/sl hits ───────────────────────
    try:
        _0dte_spot_prices = {
            t: tick_results[t]["price"]
            for t in ZERO_DTE_TICKERS
            if t in tick_results and tick_results[t].get("price")
        }
        _check_signal_outcomes(
            spy_price=tick_results.get("SPY", {}).get("price"),
            spot_prices=_0dte_spot_prices,
        )
    except Exception as exc:
        logging.error(f"  Signal outcome check: {exc}")

    logging.info(f"  Scan complete — {len(tick_results)}/{len(WATCHLIST)} tickers evaluated\n")


# ─── PRE-MARKET SCANNER ─────────────────────────────────────────────────────────
# Runs 8:00-9:33 ET. Two one-shot-per-day alerts, each with its own fire
# window and background retry thread (same pattern as the 0DTE scanner):
#   - BRIEFING   sharp at 9:00 — market bias, top movers, key levels,
#                TradeOdds, news catalyst.
#   - OPENING BELL  sharp at 9:30 — confirmed SPY direction at the open, plus
#                the highest-conviction picks from the briefing re-checked
#                against the actual open print.
# State (including the briefing's picks, so the bell can reuse them without
# re-fetching TradeOdds/news) persists to PREMARKET_STATE_FILE.

def _spy_premarket_bias(snapshots: dict) -> dict:
    """SPY's pre-market gap vs prior close. {"available": False} if the data isn't there yet."""
    spy = snapshots.get(ZERO_DTE_TICKER, {})
    prev_close = spy.get("prevDay", {}).get("c")
    last_trade = spy.get("lastTrade", {})
    min_bar    = spy.get("min", {})
    day_data   = spy.get("day", {})
    price = last_trade.get("p") or min_bar.get("c") or day_data.get("c")
    if not prev_close or not price:
        return {"available": False}

    gap_pct = (price - prev_close) / prev_close * 100.0
    if gap_pct > 0.1:
        bias = "BULLISH"
    elif gap_pct < -0.1:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"
    return {"available": True, "price": float(price), "prev_close": float(prev_close), "gap_pct": gap_pct, "bias": bias}


def _premarket_movers(snapshots: dict) -> list:
    """
    Every WATCHLIST stock with a pre-market price, ranked by |gap%| vs prior
    close, descending. Volume is whatever Polygon's snapshot reports at the
    time of the call (day.v if the session has any pre-market prints
    accumulated into it, else the minute bar's accumulated volume) — best
    available, not a fabricated number.
    """
    movers = []
    for ticker, snap in snapshots.items():
        prev_close = snap.get("prevDay", {}).get("c")
        if not prev_close:
            continue
        last_trade = snap.get("lastTrade", {})
        min_bar    = snap.get("min", {})
        day_data   = snap.get("day", {})
        price = last_trade.get("p") or min_bar.get("c") or day_data.get("c")
        if not price:
            continue
        gap_pct = (price - prev_close) / prev_close * 100.0
        volume  = day_data.get("v") or min_bar.get("av") or 0
        movers.append({
            "ticker":     ticker,
            "price":      float(price),
            "prev_close": float(prev_close),
            "prev_high":  snap.get("prevDay", {}).get("h"),
            "prev_low":   snap.get("prevDay", {}).get("l"),
            "gap_pct":    gap_pct,
            "volume":     float(volume),
        })
    movers.sort(key=lambda m: abs(m["gap_pct"]), reverse=True)
    return movers


def _build_premarket_picks(snapshots: dict) -> list:
    """Top PREMARKET_TOP_N movers (min |gap%| = PREMARKET_GAP_MIN_PCT), each enriched with direction, 20D levels, TradeOdds, and news."""
    movers = [m for m in _premarket_movers(snapshots) if abs(m["gap_pct"]) >= PREMARKET_GAP_MIN_PCT]

    picks = []
    for m in movers[:PREMARKET_TOP_N]:
        direction = "CALL" if m["gap_pct"] > 0 else "PUT"
        daily = _daily_cache.get(m["ticker"], [])
        high_20d = float(np.max([b["c"] for b in daily[-20:]])) if len(daily) >= 20 else None
        low_20d  = float(np.min([b["c"] for b in daily[-20:]])) if len(daily) >= 20 else None

        tradeodds_result = fetch_tradeodds_validation(
            m["ticker"],
            conditions={"daily_change": True, "vix_level": True, "regime": True, "rel_vol": True},
            forward_period="1d",
            reference_period="1d",
        )
        news = fetch_ticker_news(m["ticker"])

        picks.append({
            **m,
            "direction": direction,
            "high_20d":  high_20d,
            "low_20d":   low_20d,
            "tradeodds": tradeodds_result,
            "news":      news,
        })
    return picks


def _rank_opening_picks(picks: list, snapshots: dict) -> list:
    """
    Re-checks each briefing pick against the actual opening print. "Confirmed"
    (still moving the same direction it gapped pre-market) sorts first;
    within each group, ranked by |move| descending. No new TradeOdds/news
    calls — this only re-reads the snapshot already fetched for the bell.
    """
    enriched = []
    for p in picks:
        snap = snapshots.get(p["ticker"])
        if not snap:
            continue
        last_trade = snap.get("lastTrade", {})
        day_data   = snap.get("day", {})
        price = last_trade.get("p") or day_data.get("o") or day_data.get("c") or p["price"]
        prev_close = p["prev_close"]
        chg_pct = (price - prev_close) / prev_close * 100.0 if prev_close else 0.0
        confirmed = (p["direction"] == "CALL" and chg_pct > 0) or (p["direction"] == "PUT" and chg_pct < 0)
        enriched.append({**p, "open_price": float(price), "open_chg_pct": chg_pct, "confirmed": confirmed})
    enriched.sort(key=lambda x: (not x["confirmed"], -abs(x["open_chg_pct"])))
    return enriched


def send_premarket_early_prices(movers: list) -> bool:
    """8 AM price flash — compact table of where watchlist stocks are trading pre-market."""
    if not movers:
        lines = ["No pre-market activity detected yet — markets may not have opened pre-market trading."]
    else:
        lines = ["```"]
        lines.append(f"{'TICKER':<8} {'PRE-MKT':>8}  {'CHG%':>7}  {'vs CLOSE':>9}  VOL")
        lines.append("─" * 52)
        for m in movers:
            arrow = "▲" if m["gap_pct"] > 0 else "▼"
            lines.append(
                f"{m['ticker']:<8} ${m['price']:>7.2f}  {arrow}{abs(m['gap_pct']):>5.2f}%  "
                f"${m['prev_close']:>8.2f}  {_fmt_vol(m['volume'])}"
            )
        lines.append("```")
        lines.append("*Use limit orders only during pre-market (4 AM – 9:30 AM ET)*")

    ok = _post_embed({
        "title":     "🌄  PRE-MARKET PRICES  ·  8:00 AM ET",
        "color":     0x5865F2,
        "description": "\n".join(lines),
        "footer":    {"text": f"QVIX  ·  Pre-market data via Polygon  ·  {datetime.now(ET).strftime('%I:%M %p ET')}"},
        "timestamp": datetime.utcnow().isoformat() + "Z",
    })
    logging.info(f"  Discord {'delivered' if ok else 'DELIVERY FAILED'}: EARLY PRE-MARKET PRICES ({len(movers)} movers)")
    return ok


def _fmt_vol(vol: float) -> str:
    if vol >= 1_000_000:
        return f"{vol/1_000_000:.1f}M"
    if vol >= 1_000:
        return f"{vol/1_000:.0f}K"
    return str(int(vol))


def send_premarket_briefing(bias: dict, picks: list, stats: dict) -> bool:
    bias_emoji = "🟢" if bias["bias"] == "BULLISH" else "🔴" if bias["bias"] == "BEARISH" else "⚪"
    fields = [
        {
            "name":   "Market Bias",
            "value":  (
                f"{bias_emoji} **{bias['bias']}** — SPY {bias['gap_pct']:+.2f}% pre-market "
                f"(${bias['price']:.2f} vs prior close ${bias['prev_close']:.2f})"
            ),
            "inline": False,
        },
    ]

    if stats["total_signals"] == 0:
        track_record = "No signals tracked yet."
    else:
        track_record = (
            f"**{stats['win_rate']}%** win rate ({stats['wins']}W / {stats['losses']}L)  ·  "
            f"{stats['closed_signals']} closed, {stats['open_signals']} open  ·  "
            f"Cumulative PnL: **{stats['total_pnl']:+.2f}%**"
        )
    fields.append({"name": "📊 Track Record", "value": track_record, "inline": False})

    if not picks:
        fields.append({"name": "Top Movers", "value": "No tickers showing significant pre-market movement today.", "inline": False})
    else:
        for i, p in enumerate(picks, 1):
            dir_emoji = "🟢" if p["direction"] == "CALL" else "🔴"
            lines = [f"{dir_emoji} {p['direction']} bias — Gap {p['gap_pct']:+.2f}%  ·  Vol {p['volume']:,.0f}"]

            levels = []
            if p.get("prev_high") and p.get("prev_low"):
                levels.append(f"Prior day H/L: ${p['prev_high']:.2f} / ${p['prev_low']:.2f}")
            if p.get("high_20d") and p.get("low_20d"):
                levels.append(f"20D H/L: ${p['high_20d']:.2f} / ${p['low_20d']:.2f}")
            if levels:
                lines.append(" · ".join(levels))

            to = p.get("tradeodds")
            if to:
                side = "up" if p["direction"] == "CALL" else "down"
                prob = to.get("probability_up") if p["direction"] == "CALL" else to.get("probability_down")
                if prob is not None:
                    lines.append(f"TradeOdds: {prob:.0f}% probability {side} (n={to.get('sample_size', 0)})")
            else:
                lines.append("TradeOdds: unavailable")

            news = p.get("news")
            lines.append(f"📰 {news['title']} ({news.get('publisher', '')})" if news else "📰 No recent catalyst found")

            fields.append({"name": f"{i}. {p['ticker']} — ${p['price']:.2f}", "value": "\n".join(lines), "inline": False})

    ok = _post_embed({
        "title":     "🌅  PRE-MARKET BRIEFING",
        "color":     0x3498DB,
        "fields":    fields,
        "footer":    {"text": f"QVIX 5.1  ·  {datetime.now(ET).strftime('%I:%M %p ET')}"},
        "timestamp": datetime.utcnow().isoformat() + "Z",
    })
    logging.info(f"  Discord {'delivered' if ok else 'DELIVERY FAILED'}: PRE-MARKET BRIEFING")
    return ok


def send_opening_bell(direction: str, price: float, prev_close: float, chg_pct: float, top: list) -> bool:
    dir_emoji = "🟢" if direction == "CALL" else "🔴"
    fields = [
        {
            "name":   "SPY at Open",
            "value":  f"{dir_emoji} **{direction}** confirmed — ${price:.2f} ({chg_pct:+.2f}% vs prior close ${prev_close:.2f})",
            "inline": False,
        },
    ]

    if not top:
        fields.append({"name": "Top Trades", "value": "No pre-market picks survived to the open.", "inline": False})
    else:
        for i, p in enumerate(top, 1):
            tag = "✅ confirmed" if p.get("confirmed") else "⚠️ not confirmed at open"
            entry_side = "above" if p["direction"] == "CALL" else "below"
            fields.append({
                "name":  f"{i}. {p['ticker']} — {p['direction']} ({tag})",
                "value": (
                    f"Open: ${p['open_price']:.2f} ({p['open_chg_pct']:+.2f}%)\n"
                    f"Watch entry {entry_side} ${p['open_price']:.2f} in the first 10 minutes"
                ),
                "inline": False,
            })

    ok = _post_embed({
        "title":     "🔔  OPENING BELL",
        "color":     0x00C851 if direction == "CALL" else 0xFF4444,
        "fields":    fields,
        "footer":    {"text": f"QVIX 5.1  ·  {datetime.now(ET).strftime('%I:%M %p ET')}"},
        "timestamp": datetime.utcnow().isoformat() + "Z",
    })
    logging.info(f"  Discord {'delivered' if ok else 'DELIVERY FAILED'}: OPENING BELL")
    return ok


_PREMARKET_STATE_DEFAULTS = {
    "date": None,
    "early_fired": False, "early_thread_running": False,
    "briefing_fired": False, "briefing_thread_running": False,
    "bell_fired": False, "bell_thread_running": False,
    "summary_fired": False,
    "eod_closer_fired": False,
    "picks": [],
}


def _load_premarket_state() -> dict:
    """Restore today's briefing/bell/summary fire-state (and picks) from disk, if saved today."""
    try:
        data = json.loads(PREMARKET_STATE_FILE.read_text())
        if data.get("date") and date.fromisoformat(data["date"]) == date.today():
            return {
                "date":                    date.today(),
                "early_fired":             bool(data.get("early_fired", False)),
                "early_thread_running":    False,
                "briefing_fired":          bool(data.get("briefing_fired", False)),
                "briefing_thread_running": False,
                "bell_fired":              bool(data.get("bell_fired", False)),
                "bell_thread_running":     False,
                "summary_fired":           bool(data.get("summary_fired", False)),
                "eod_closer_fired":        bool(data.get("eod_closer_fired", False)),
                "picks":                   data.get("picks", []),
            }
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError):
        pass
    return dict(_PREMARKET_STATE_DEFAULTS)


def _save_premarket_state():
    """Best-effort persist — a failed write risks one extra same-day attempt on restart, never a lost one."""
    try:
        PREMARKET_STATE_FILE.write_text(json.dumps({
            "date":           _premarket_state["date"].isoformat() if _premarket_state["date"] else None,
            "early_fired":    _premarket_state.get("early_fired", False),
            "briefing_fired": _premarket_state["briefing_fired"],
            "bell_fired":     _premarket_state["bell_fired"],
            "summary_fired":    _premarket_state.get("summary_fired", False),
            "eod_closer_fired": _premarket_state.get("eod_closer_fired", False),
            "picks":            _premarket_state.get("picks", []),
        }))
    except Exception as exc:
        logging.warning(f"  Pre-market state persist failed: {exc}")


_premarket_state = _load_premarket_state()


def _premarket_early_attempt() -> bool:
    """One attempt. Returns True only if snapshot data isn't available yet (worth retrying)."""
    logging.info("━━━  Early Pre-Market Price Flash  ━━━")
    try:
        snapshots = fetch_all_snapshots()
    except Exception as exc:
        logging.error(f"  Early pre-market: snapshot fetch failed: {exc}")
        return True

    all_movers = _premarket_movers(snapshots)
    movers = [m for m in all_movers if abs(m["gap_pct"]) >= PREMARKET_EARLY_MIN_PCT]
    if not movers and not all_movers:
        logging.info("  Early pre-market: no snapshot data yet — retrying in 60s")
        return True

    send_premarket_early_prices(movers[:PREMARKET_EARLY_TOP_N])
    logging.info(f"  📊  EARLY PRE-MARKET PRICES sent — {len(movers)} movers")
    return False


def _premarket_early_retry_loop():
    try:
        while True:
            should_retry = _premarket_early_attempt()
            if not should_retry:
                break
            if datetime.now(ET).time() >= PREMARKET_EARLY_END:
                logging.warning(f"Early pre-market: data never became available before {PREMARKET_EARLY_END.strftime('%H:%M')} ET — skipping for today")
                break
            time.sleep(60)
    finally:
        _premarket_state["early_fired"]          = True
        _premarket_state["early_thread_running"] = False
        _save_premarket_state()


def _premarket_briefing_attempt() -> bool:
    """One attempt. Returns True only if pre-market snapshot data isn't ready yet (worth retrying)."""
    logging.info("━━━  Pre-Market Briefing  ━━━")
    try:
        snapshots = fetch_all_snapshots()
    except Exception as exc:
        logging.error(f"  Pre-market briefing: snapshot fetch failed: {exc}")
        return True

    bias = _spy_premarket_bias(snapshots)
    if not bias.get("available"):
        logging.info("  Pre-market briefing: SPY pre-market data not yet available — retrying in 30s")
        return True

    picks = _build_premarket_picks(snapshots)
    _premarket_state["picks"] = picks
    stats = _get_signal_stats()
    send_premarket_briefing(bias, picks, stats)
    logging.info(f"  📰  PRE-MARKET BRIEFING sent — bias={bias['bias']}, {len(picks)} picks, track record {stats['win_rate']}% ({stats['wins']}W/{stats['losses']}L)")
    return False


def _premarket_bell_attempt() -> bool:
    """One attempt. Returns True only if SPY's opening print isn't there yet (worth retrying)."""
    logging.info("━━━  Opening Bell  ━━━")
    try:
        snapshots = fetch_all_snapshots()
    except Exception as exc:
        logging.error(f"  Opening bell: snapshot fetch failed: {exc}")
        return True

    spy = snapshots.get(ZERO_DTE_TICKER, {})
    day_data = spy.get("day", {})
    price = day_data.get("o") or day_data.get("c")
    prev_close = spy.get("prevDay", {}).get("c")
    if not price or not prev_close:
        logging.info("  Opening bell: SPY opening print not yet available — retrying in 15s")
        return True

    chg_pct   = (price - prev_close) / prev_close * 100.0
    direction = "CALL" if chg_pct >= 0 else "PUT"

    ranked = _rank_opening_picks(_premarket_state.get("picks") or [], snapshots)
    top    = ranked[:PREMARKET_BELL_TOP_N]

    send_opening_bell(direction, float(price), float(prev_close), chg_pct, top)
    logging.info(f"  🔔  OPENING BELL sent — SPY {direction} {chg_pct:+.2f}%")
    return False


def _premarket_briefing_retry_loop():
    try:
        while True:
            should_retry = _premarket_briefing_attempt()
            if not should_retry:
                break
            if datetime.now(ET).time() >= PREMARKET_BRIEFING_END:
                logging.warning(f"Pre-market briefing: data never became available before {PREMARKET_BRIEFING_END.strftime('%H:%M')} ET — skipping for today")
                break
            time.sleep(30)
    finally:
        _premarket_state["briefing_fired"]          = True
        _premarket_state["briefing_thread_running"] = False
        _save_premarket_state()


def _premarket_bell_retry_loop():
    try:
        while True:
            should_retry = _premarket_bell_attempt()
            if not should_retry:
                break
            if datetime.now(ET).time() >= PREMARKET_BELL_END:
                logging.warning(f"Opening bell: SPY open print never became available before {PREMARKET_BELL_END.strftime('%H:%M')} ET — skipping for today")
                break
            time.sleep(15)
    finally:
        _premarket_state["bell_fired"]          = True
        _premarket_state["bell_thread_running"] = False
        _save_premarket_state()


def run_premarket_scan():
    """
    Self-gates on weekday + the 8:00-9:33 ET window; only ever does real work
    three times per trading day (early price flash ~8:00, briefing ~9:00, bell
    ~9:30), each handed off to its own background thread so a slow retry
    sequence can't block crypto/stock scanning. Stops automatically once 9:33
    passes — the regular run_scan() takes over at 9:30 on its own schedule.
    """
    now   = datetime.now(ET)
    today = now.date()

    if _premarket_state["date"] != today:
        _premarket_state.update({
            "date": today,
            "early_fired": False, "early_thread_running": False,
            "briefing_fired": False, "briefing_thread_running": False,
            "bell_fired": False, "bell_thread_running": False,
            "picks": [],
        })
        _save_premarket_state()

    if now.weekday() >= 5:                                          # weekend — nothing to do
        return
    if now.time() < PREMARKET_START or now.time() > PREMARKET_BELL_END:
        return                                                       # outside the whole window

    # Early price flash phase — fires anytime 8:00 AM up to 8:59 AM (catchup: fires
    # even if process started late, as long as the briefing hasn't taken over yet).
    if not _premarket_state.get("early_fired") and not _premarket_state.get("early_thread_running"):
        if PREMARKET_EARLY_TIME <= now.time() < PREMARKET_BRIEFING_TIME:
            _premarket_state["early_thread_running"] = True
            threading.Thread(target=_premarket_early_retry_loop, daemon=True).start()
        elif now.time() >= PREMARKET_BRIEFING_TIME:
            logging.warning("Early pre-market: process started after 9:00 ET — skipping early flash for today")
            _premarket_state["early_fired"] = True
            _save_premarket_state()

    # Briefing phase — fires anytime 9:00 AM up to 9:29 AM (catchup: a late
    # start at e.g. 9:15 still sends a briefing before the bell).
    if not _premarket_state["briefing_fired"] and not _premarket_state["briefing_thread_running"]:
        if PREMARKET_BRIEFING_TIME <= now.time() < PREMARKET_BELL_TIME:
            _premarket_state["briefing_thread_running"] = True
            threading.Thread(target=_premarket_briefing_retry_loop, daemon=True).start()
        elif now.time() >= PREMARKET_BELL_TIME:
            logging.warning("Pre-market briefing: process started after 9:30 ET — skipping briefing for today")
            _premarket_state["briefing_fired"] = True
            _save_premarket_state()

    # Bell phase (9:30 AM)
    if not _premarket_state["bell_fired"] and not _premarket_state["bell_thread_running"]:
        if PREMARKET_BELL_TIME <= now.time() <= PREMARKET_BELL_END:
            _premarket_state["bell_thread_running"] = True
            threading.Thread(target=_premarket_bell_retry_loop, daemon=True).start()
        elif now.time() > PREMARKET_BELL_END:
            logging.warning(f"Opening bell: missed the {PREMARKET_BELL_TIME.strftime('%H:%M')}-{PREMARKET_BELL_END.strftime('%H:%M')} ET fire window — skipping for today")
            _premarket_state["bell_fired"] = True
            _save_premarket_state()


# ─── SPY 0DTE MORNING SCANNER ───────────────────────────────────────────────────
# Fires once per trading day, in the 9:35-9:50 ET window, off the first five
# 1-minute bars of the session. Gated by THREE independent checks, in order
# of cost (cheapest first) so a weak setup never reaches the paid TradeOdds
# call: (1) momentum — opening direction confirmed across the 5 bars,
# (2) volume — opening 5-min volume vs. this ticker's own recent daily-volume
# pace, (3) TradeOdds — 10y historical pattern read must also favor that
# direction by ZERO_DTE_MIN_EDGE points. All three must pass to alert.
#
# "fired" persists to ZERO_DTE_STATE_FILE so a restart mid-window (e.g. for a
# code deploy) doesn't hand the day a second, unintended shot. If the opening
# bars come back empty (a transient Polygon data-availability gap, not a
# real rejection), a background thread retries every 60s until either bars
# show up or the 9:50 cutoff passes — this runs off the main loop so it never
# blocks crypto/stock scanning the way the old single-shot check could.

_ZERO_DTE_STATE_DEFAULTS = {
    "date": None, "fired": False, "thread_running": False,
    "direction": None, "entry_price": None,
    "close_fired": False, "reversal_fired": False, "target_fired": False,
}


def _load_zero_dte_state(ticker: str) -> dict:
    """Restore today's fire-state (and entry_price/direction for position management) from disk, if saved today."""
    try:
        data = json.loads(_dte_state_file(ticker).read_text())
        if data.get("date") and date.fromisoformat(data["date"]) == date.today():
            return {
                "date":           date.today(),
                "fired":          bool(data.get("fired", False)),
                "thread_running": False,
                "direction":      data.get("direction"),
                "entry_price":    data.get("entry_price"),
                "close_fired":    bool(data.get("close_fired", False)),
                "reversal_fired": bool(data.get("reversal_fired", False)),
                "target_fired":   bool(data.get("target_fired", False)),
            }
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError):
        pass
    return dict(_ZERO_DTE_STATE_DEFAULTS)


def _save_zero_dte_state(ticker: str):
    """Best-effort persist — a failed write just means a restart could get an extra shot, never a lost one."""
    state = _zero_dte_states[ticker]
    try:
        _dte_state_file(ticker).write_text(json.dumps({
            "date":           state["date"].isoformat() if state["date"] else None,
            "fired":          state["fired"],
            "direction":      state.get("direction"),
            "entry_price":    state.get("entry_price"),
            "close_fired":    state.get("close_fired", False),
            "reversal_fired": state.get("reversal_fired", False),
            "target_fired":   state.get("target_fired", False),
        }))
    except Exception as exc:
        logging.warning(f"  0DTE state persist failed ({ticker}): {exc}")


_zero_dte_states = {t: _load_zero_dte_state(t) for t in ZERO_DTE_TICKERS}


def _zero_dte_attempt(today: date, ticker: str) -> bool:
    """
    One evaluation pass for `ticker`. Returns True only when the opening-range
    bars came back empty/incomplete — the one failure mode worth retrying.
    Every other outcome (fired, rejected, hard error) returns False.
    """
    state = _zero_dte_states[ticker]
    logging.info(f"━━━  {ticker} 0DTE Morning Scan  ━━━")

    try:
        bars = fetch_opening_range_bars(ticker, today)
    except Exception as exc:
        logging.error(f"  0DTE scan ({ticker}): failed to fetch opening range bars: {exc}")
        return False

    opening = eval_opening_range(bars)
    if not opening["valid"]:
        logging.info(f"  0DTE scan ({ticker}): {opening['reason']} — retrying in 60s")
        return True

    direction = opening["direction"]
    if not direction:
        logging.info(f"  0DTE scan ({ticker}): flat at the open — no clear direction, no alert")
        return False
    if not opening["momentum_confirms"]:
        logging.info(f"  0DTE scan ({ticker}): {direction} bias but opening bars are choppy/unconfirmed — no alert")
        return False

    # Volume confirmation reuses the daily-bar cache already populated for the
    # regular scan (no extra API call): scale the recent average full-day volume
    # down to a 5-minute slice of a 390-minute session.
    daily = _daily_cache.get(ticker, [])
    if len(daily) >= 10:
        avg_daily_vol     = float(np.mean([b["v"] for b in daily[-10:]]))
        expected_5min_vol = avg_daily_vol * (5.0 / 390.0)
        vol_ratio          = opening["total_vol"] / expected_5min_vol if expected_5min_vol else 0.0
    else:
        vol_ratio = 0.0
    opening["vol_ratio"] = vol_ratio

    if vol_ratio < ZERO_DTE_MIN_VOL_RATIO:
        logging.info(f"  0DTE scan ({ticker}): {direction} bias but volume only {vol_ratio:.1f}x expected — too weak, no alert")
        return False

    # TradeOdds historical validation — only called once the two free local
    # checks already passed, to keep compute-credit spend down.
    tradeodds_result = fetch_tradeodds_validation(
        ticker,
        conditions={"daily_change": True, "vix_level": True, "regime": True, "rel_vol": True},
    )
    confirmed, note = _tradeodds_confirms(direction, tradeodds_result)
    if not confirmed:
        logging.info(f"  0DTE scan ({ticker}): local {direction} bias not confirmed — {note} — no alert")
        return False

    price = float(opening["last_close"])
    plan  = _zero_dte_option_plan(ticker, direction, price)

    # Record entry for intraday position management.
    state["direction"]   = direction
    state["entry_price"] = price

    logging.info(f"  🔔  0DTE ALERT  {ticker}  {direction}  strike {plan['strike']}  — {note}")
    send_zero_dte_alert(ticker, direction, plan, price, opening, note)
    log_decision(
        ticker, "0DTE SCALP",
        {"score": 100, "reasons": [note], "metrics": opening},
        alerted=True,
    )
    entry_premium = (
        (plan["entry_low"] + plan["entry_high"]) / 2
        if plan.get("real_data") else None
    )
    _log_signal(
        ticker, "0DTE SCALP",
        "CALL" if direction == "CALL" else "PUT",
        100, price,
        plan.get("target"), plan.get("stop"),
        strike=plan.get("strike"),
        expiry=plan.get("expiry"),
        entry_premium=entry_premium,
        tradeodds_confirmed=True,
        signal_type="0dte_option",
    )
    return False


def _zero_dte_retry_loop(today: date, ticker: str):
    """Background thread: re-attempt every 60s while bars are empty, until a definitive outcome or the 9:50 cutoff."""
    try:
        while True:
            should_retry = _zero_dte_attempt(today, ticker)
            if not should_retry:
                break
            if datetime.now(ET).time() >= ZERO_DTE_FIRE_WINDOW_END:
                logging.warning(
                    f"0DTE scan ({ticker}): opening bars never became available before "
                    f"{ZERO_DTE_FIRE_WINDOW_END.strftime('%H:%M')} ET — giving up for today"
                )
                break
            time.sleep(60)
    finally:
        _zero_dte_states[ticker]["fired"]          = True
        _zero_dte_states[ticker]["thread_running"] = False
        _save_zero_dte_state(ticker)


def run_zero_dte_scan():
    now   = datetime.now(ET)
    today = now.date()

    if now.weekday() >= 5:   # weekend — nothing to do
        return

    for ticker in ZERO_DTE_TICKERS:
        state = _zero_dte_states[ticker]

        if state["date"] != today:
            state.update({
                "date": today, "fired": False, "thread_running": False,
                "direction": None, "entry_price": None,
                "close_fired": False, "reversal_fired": False, "target_fired": False,
            })
            _save_zero_dte_state(ticker)

        if state["fired"] or state["thread_running"]:
            continue                                          # already attempted (or retrying) today
        if now.time() < ZERO_DTE_FIRE_TIME:
            continue                                          # opening range not complete yet
        if now.time() > ZERO_DTE_FIRE_WINDOW_END:
            logging.warning(
                f"0DTE scan ({ticker}): missed the {ZERO_DTE_FIRE_TIME.strftime('%H:%M')}-"
                f"{ZERO_DTE_FIRE_WINDOW_END.strftime('%H:%M')} ET fire window — skipping for today"
            )
            state["fired"] = True
            _save_zero_dte_state(ticker)
            continue

        state["thread_running"] = True
        threading.Thread(target=_zero_dte_retry_loop, args=(today, ticker), daemon=True).start()


# ─── 0DTE INTRADAY POSITION MANAGEMENT ──────────────────────────────────────────
# Runs every 5-minute scan cycle during market hours (called from run_scan(),
# which already only runs while the market is open) — not just in the
# 9:35-9:50 morning window. Monitors the day's already-fired 0DTE signal (if
# any) for three triggers, each firing at most once per day. Reuses SPY's
# snapshot already fetched for the regular per-ticker scan — no extra API call.

_ZERO_DTE_ALERT_STYLE = {
    "TARGET":   {"emoji": "✅", "color": 0x00C851, "title": "0DTE TARGET HIT",
                 "action": "Price has moved 1% in the signal's favor — consider taking profits."},
    "CLOSE":    {"emoji": "🔔", "color": 0xFF4444, "title": "0DTE CLOSE POSITION",
                 "action": "Price has moved against the signal — consider closing."},
    "REVERSAL": {"emoji": "⚠️", "color": 0xFFA500, "title": "0DTE REVERSAL DETECTED",
                 "action": "Direction has reversed from the morning signal — consider closing."},
}


def send_zero_dte_management_alert(ticker: str, kind: str, direction: str, entry_price: float, price: float, pct_move: float) -> bool:
    """Post one of the three intraday 0DTE management alerts (TARGET/CLOSE/REVERSAL)."""
    style = _ZERO_DTE_ALERT_STYLE[kind]
    fields = [
        {"name": "Original Signal", "value": f"{direction} from ${entry_price:.2f}", "inline": True},
        {"name": "Current Price",   "value": f"${price:.2f} ({pct_move:+.2f}% vs entry)", "inline": True},
        {"name": "Action",          "value": f"**{style['title']}** — {style['action']}", "inline": False},
    ]
    ok = _post_embed({
        "title":     f"{style['emoji']}  {style['title']}  ·  {ticker}",
        "color":     style["color"],
        "fields":    fields,
        "footer":    {"text": f"QVIX 5.1  ·  {datetime.now(ET).strftime('%I:%M %p ET')}"},
        "timestamp": datetime.utcnow().isoformat() + "Z",
    })
    logging.info(f"  Discord {'delivered' if ok else 'DELIVERY FAILED'}: 0DTE {kind} {ticker}")
    return ok


def check_zero_dte_position(tick_results: dict):
    """
    Runs for each 0DTE ticker independently. No-ops for a ticker unless its
    signal fired today (direction/entry_price set). All three triggers
    (REVERSAL, CLOSE, TARGET) are measured from the entry price and fire at
    most once per ticker per day.
    """
    for ticker in ZERO_DTE_TICKERS:
        state       = _zero_dte_states[ticker]
        direction   = state.get("direction")
        entry_price = state.get("entry_price")
        if not direction or not entry_price:
            continue

        snap = tick_results.get(ticker)
        if not snap:
            continue

        price             = snap["price"]
        pct_from_entry    = (price - entry_price) / entry_price * 100.0
        move_in_direction = pct_from_entry if direction == "CALL" else -pct_from_entry

        if not state.get("target_fired") and move_in_direction >= ZERO_DTE_TARGET_PCT:
            logging.info(f"  🎯  0DTE TARGET HIT  {ticker}  {direction}  {move_in_direction:+.2f}% vs entry")
            send_zero_dte_management_alert(ticker, "TARGET", direction, entry_price, price, move_in_direction)
            state["target_fired"] = True
            _save_zero_dte_state(ticker)

        if not state.get("reversal_fired") and move_in_direction < 0:
            logging.info(f"  ⚠️  0DTE REVERSAL  {ticker}  {direction}  {move_in_direction:+.2f}% vs entry")
            send_zero_dte_management_alert(ticker, "REVERSAL", direction, entry_price, price, move_in_direction)
            state["reversal_fired"] = True
            _save_zero_dte_state(ticker)

        if not state.get("close_fired") and move_in_direction <= -ZERO_DTE_CLOSE_PCT:
            logging.info(f"  🔔  0DTE CLOSE SIGNAL  {ticker}  {direction}  {move_in_direction:+.2f}% vs entry")
            send_zero_dte_management_alert(ticker, "CLOSE", direction, entry_price, price, move_in_direction)
            state["close_fired"] = True
            _save_zero_dte_state(ticker)


# ─── CRYPTO SCAN ───────────────────────────────────────────────────────────────

def run_crypto_scan():
    # ── Daily history: load once per calendar day ─────────────────────────────
    if _crypto_cache_date != date.today():
        refresh_crypto_daily_cache()

    now = datetime.now(ET)
    logging.info(f"━━━  Crypto Scan  {now.strftime('%H:%M:%S ET')}  ━━━")

    # ── ONE API call for all crypto assets' current data ──────────────────────
    try:
        snapshots = fetch_crypto_snapshots()
        logging.info(f"  Snapshot: {len(snapshots)}/{len(CRYPTO_WATCHLIST)} assets")
    except requests.HTTPError as exc:
        logging.error(f"  Crypto snapshot failed: HTTP {exc.response.status_code}  {exc}")
        return
    except Exception as exc:
        logging.error(f"  Crypto snapshot failed: {exc}")
        return

    tick_results: dict = {}

    for sym in CRYPTO_WATCHLIST:
        try:
            snap = snapshots.get(sym)
            if not snap:
                logging.warning(f"  {sym}: missing from crypto snapshot — skipping")
                continue

            daily = list(_crypto_daily_cache.get(sym, []))
            if not daily:
                logging.warning(f"  {sym}: no daily history in crypto cache — skipping")
                continue

            day_data = snap.get("day", {})
            if day_data.get("c"):
                today_bar = {
                    "o": day_data.get("o", day_data["c"]),
                    "h": day_data.get("h", day_data["c"]),
                    "l": day_data.get("l", day_data["c"]),
                    "c": day_data["c"],
                    "v": day_data.get("v", 0),
                }
                daily = daily[:-1] + [today_bar]

            # Same `or`-not-.get-default fix as the stock scan — a present-but-zero
            # field from Polygon must not be treated as "use the real value 0".
            open_px  = day_data.get("o") or daily[-1]["c"]
            close_px = day_data.get("c") or open_px
            intraday = [
                {"o": open_px, "h": open_px,                       "l": open_px,                       "c": open_px,  "v": 0},
                {"o": open_px, "h": day_data.get("h") or close_px, "l": day_data.get("l") or close_px, "c": close_px, "v": day_data.get("v", 0)},
            ]

            # Crypto snapshot uses lastTrade.p for the most recent price —
            # genuinely live (see fetch_crypto_snapshots), unlike the old
            # Polygon /prev call this replaced. "asof" still carries the
            # date through for the alert text.
            last_trade  = snap.get("lastTrade", {})
            price       = float(last_trade.get("p") or close_px or daily[-1]["c"])
            price_asof  = last_trade.get("asof")
            day_frac    = snap.get("day_elapsed_fraction", 1.0)

            mb = eval_momentum_breakout(sym, daily, intraday, session_fraction=day_frac)
            bp = eval_bearish_put(sym, daily, intraday, session_fraction=day_frac)

            tick_results[sym] = {
                "momentum_breakout": mb,
                "bearish_put":       bp,
                "intraday":          intraday,
                "price":             price,
            }

            for sig_name, result in [("MOMENTUM BREAKOUT", mb), ("BEARISH PUT", bp)]:
                metrics = result.get("metrics", {})
                fire = _should_alert(
                    f"CRYPTO:{sym}", sig_name, result["score"],
                    min_score=CRYPTO_ALERT_MIN_SCORE,
                    current_vol=metrics.get("current_vol"),
                    avg_vol_20d=metrics.get("avg_vol_20d"),
                    session_fraction=day_frac,
                )

                synthesis:   Optional[dict] = None
                liquid_data: Optional[dict] = None
                if fire:
                    liquid_data = fetch_liquid_positioning(sym)
                    synthesis   = claude_synthesis(
                        sym, sig_name, result["score"], result["reasons"],
                        None, {}, liquid_data,
                    )
                    metrics["synthesis"] = synthesis
                    if synthesis["verdict"] == "WEAK":
                        fire = False

                log_decision(f"CRYPTO:{sym}", sig_name, result, fire)
                if fire:
                    _cooldown_ok(f"CRYPTO:{sym}", sig_name, result["score"], ALERT_COOLDOWN_SECS)
                    logging.info(f"  🔔  CRYPTO ALERT  {sym}  {sig_name}  {result['score']}/100  — {synthesis['verdict']} ({synthesis['confidence']})")
                    send_alert(sym, sig_name, result["score"], result["reasons"], price, is_crypto=True,
                               metrics=result.get("metrics", {}), price_asof=price_asof,
                               synthesis=synthesis, liquid_data=liquid_data)
                    # Spot-price tp/sl: CRYPTO_TARGET_PCT in direction = win,
                    # CRYPTO_STOP_PCT against = loss. Stored as absolute levels
                    # so _check_signal_outcomes can compare without knowing direction.
                    if sig_name == "MOMENTUM BREAKOUT":
                        _tp = round(price * (1 + CRYPTO_TARGET_PCT / 100), 8)
                        _sl = round(price * (1 - CRYPTO_STOP_PCT  / 100), 8)
                    else:
                        _tp = round(price * (1 - CRYPTO_TARGET_PCT / 100), 8)
                        _sl = round(price * (1 + CRYPTO_STOP_PCT  / 100), 8)
                    _log_signal(sym, sig_name, sig_name, result["score"], price,
                                _tp, _sl, signal_type="crypto_spot")
                    if LIQUID_AUTO_EXECUTE_ENABLED:
                        logging.warning("  LIQUID_AUTO_EXECUTE_ENABLED is True but no execution path is wired in — see qvix.py comments. Logging dry-run only.")
                    _log_liquid_dry_run(sym, sig_name, result["score"])
                elif synthesis and synthesis["verdict"] == "WEAK":
                    logging.info(f"  🚫  SUPPRESSED  {sym}  {sig_name}  — Claude WEAK: {synthesis['reason']}")
                    _send_suppression_notice(sym, sig_name, result["score"], synthesis, crypto=True)
                else:
                    tag = "↑" if sig_name == "MOMENTUM BREAKOUT" else "↓"
                    logging.info(f"  {tag}  {sym:<6}  {sig_name:<22}  {result['score']:>3}/100")

        except Exception as exc:
            logging.error(f"  {sym}: {exc}")

    # ── CRYPTO CRASH ALERT (BTC = market index, ETH = alt index) ─────────────
    btc_intra = tick_results.get("BTC", {}).get("intraday", [])
    eth_intra = tick_results.get("ETH", {}).get("intraday", [])

    crash = eval_crash_alert(tick_results, btc_intra, eth_intra, idx1_name="BTC", idx2_name="ETH")
    fire_crash = _should_alert("CRYPTO:MARKET", "MARKET CRASH ALERT", crash["score"], CRASH_MIN_SCORE)
    log_decision("CRYPTO:MARKET", "MARKET CRASH ALERT", crash, fire_crash)

    if fire_crash:
        logging.info(f"  🚨  CRYPTO CRASH ALERT  score={crash['score']}/100")
        send_alert("CRYPTO MARKET", "MARKET CRASH ALERT", crash["score"], crash["reasons"], is_crypto=True)
        _cooldown_ok("CRYPTO:MARKET", "MARKET CRASH ALERT", crash["score"], ALERT_COOLDOWN_SECS)
    else:
        logging.info(f"  🌡️  Crypto crash score: {crash['score']}/100  (threshold {CRASH_MIN_SCORE})")

    # ── EXIT TRACKER — check open crypto spot signals against live prices ────
    crypto_prices = {
        sym: float(snapshots[sym]["lastTrade"]["p"])
        for sym in CRYPTO_WATCHLIST
        if sym in snapshots and snapshots[sym].get("lastTrade", {}).get("p")
    }
    try:
        _check_signal_outcomes(crypto_prices=crypto_prices)
    except Exception as exc:
        logging.error(f"  Crypto signal outcome check: {exc}")

    logging.info(f"  Crypto scan complete — {len(tick_results)}/{len(CRYPTO_WATCHLIST)} assets evaluated\n")


# ─── SINGLE-INSTANCE LOCK ──────────────────────────────────────────────────────
# A second QVIX process running concurrently (e.g. started twice via nohup) was
# the cause of every log line and Discord alert appearing twice. This lock file
# stops a second instance from starting while one is already running.
#
# The old read-then-write pattern had a race: launchctl's KeepAlive relaunches
# QVIX within ThrottleInterval (10s), so a new process could read a stale PID
# (old process just exited), treat it as a ghost, and steal the lock before the
# OS finished reaping the old PID. Fix: open with O_CREAT|O_EXCL — the kernel
# makes the existence-check + create atomic, so only one process can win.

def _acquire_lock():
    try:
        fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        # Lock file already exists — check if the owning process is still alive.
        try:
            pid = int(LOCK_FILE.read_text().strip())
            os.kill(pid, 0)   # raises ProcessLookupError if process is gone
            print(f"QVIX is already running (PID {pid}). Exiting.")
            sys.exit(1)
        except (ValueError, ProcessLookupError):
            # Stale lock (process died without cleanup). Remove and re-acquire.
            LOCK_FILE.unlink(missing_ok=True)
            _acquire_lock()


def _release_lock():
    try:
        if LOCK_FILE.exists() and LOCK_FILE.read_text().strip() == str(os.getpid()):
            LOCK_FILE.unlink()
    except Exception:
        pass


# ─── ENTRY POINT ───────────────────────────────────────────────────────────────

def _wait_for_network(host: str = "api.polygon.io", timeout: int = 45, interval: int = 2) -> bool:
    """Block until DNS resolves or timeout elapses. Absorbs the Mac boot/wake
    race where launchd starts the daemon before the network stack is ready."""
    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        try:
            socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
            if attempt > 0:
                logging.info(f"  Network ready after ~{attempt * interval}s")
            return True
        except OSError:
            if attempt == 0:
                logging.info(f"  DNS not ready — waiting up to {timeout}s for network…")
            attempt += 1
            time.sleep(interval)
    logging.warning(f"  Network still unavailable after {timeout}s — proceeding anyway")
    return False


def _check_polygon_api_key():
    try:
        r = requests.get(
            "https://api.polygon.io/v1/marketstatus/now",
            params={"apiKey": POLYGON_API_KEY},
            timeout=10,
        )
        if r.status_code == 200:
            logging.info("  Polygon API key verified OK")
        elif r.status_code in (401, 403):
            logging.critical(f"  Polygon API key invalid or expired (HTTP {r.status_code}) — QVIX cannot fetch market data")
            _post_embed({
                "title": "⚠️ Polygon API Key Invalid",
                "description": "⚠️ Polygon API key invalid or expired — QVIX cannot fetch market data",
                "color": 0xFF0000,
            })
        else:
            logging.warning(f"  Polygon API health check returned unexpected status {r.status_code}")
    except Exception as exc:
        logging.warning(f"  Polygon API health check failed: {exc}")


def _run_unusual_radar() -> None:
    """
    Market-wide unusual activity radar. Discovery tool only — NOT a trade signal.
    Calls UW stock screener for tickers with elevated rel-vol + IV rank and writes
    unusual_activity.json for the dashboard panel. Discord pings only on extreme
    outliers (3×/IV75/$10M), capped to RADAR_MAX_DISCORD_DAY per day.
    """
    global _radar_discord_date, _radar_discord_count

    if not UNUSUAL_WHALES_API_KEY or not is_market_open():
        return

    today = datetime.now(ET).date().isoformat()
    if _radar_discord_date != today:
        _radar_discord_date  = today
        _radar_discord_count = 0
        for k in list(_radar_cooldowns.keys()):
            if k[1] != today:
                del _radar_cooldowns[k]

    try:
        r = requests.get(
            "https://api.unusualwhales.com/api/screener/stocks",
            headers={"Authorization": f"Bearer {UNUSUAL_WHALES_API_KEY}"},
            params={
                "order":                            "net_premium",
                "order_direction":                  "desc",
                "min_stock_volume_vs_avg30_volume": str(RADAR_MIN_REL_VOL),
                "min_iv_rank":                      str(RADAR_MIN_IV_RANK),
                "issue_types[]":                    "Common Stock",
                "hide_index_etf":                   "true",
                "limit":                            "15",
            },
            timeout=12,
        )
        r.raise_for_status()
        rows = r.json().get("data", [])
    except Exception as exc:
        logging.warning(f"  [Radar] UW screener fetch failed: {exc}")
        return

    tickers_out = []
    now = datetime.now(ET)

    for row in rows:
        ticker     = row.get("ticker", "")
        rel_vol    = float(row.get("stock_volume_vs_avg30_volume") or 0)
        iv_rank    = float(row.get("iv_rank") or 0)
        pct_change = float(row.get("perc_change") or 0) * 100   # UW returns decimal
        net_call   = float(row.get("net_call_premium") or 0)
        net_put    = float(row.get("net_put_premium")  or 0)
        net_prem   = float(row.get("net_premium")      or 0)
        sentiment  = "bullish" if net_prem > 0 else ("bearish" if net_prem < 0 else "neutral")

        tickers_out.append({
            "ticker":           ticker,
            "pct_change":       round(pct_change, 2),
            "rel_vol":          round(rel_vol, 2),
            "iv_rank":          round(iv_rank, 1),
            "net_call_premium": round(net_call),
            "net_put_premium":  round(net_put),
            "net_premium":      round(net_prem),
            "sentiment":        sentiment,
        })

        # Discord: extreme outliers only — all three conditions must fire together
        if (
            rel_vol          >= RADAR_DISCORD_REL_VOL
            and iv_rank      >= RADAR_DISCORD_IV_RANK
            and abs(net_prem) >= RADAR_DISCORD_NET_PREM
            and _radar_discord_count < RADAR_MAX_DISCORD_DAY
            and (ticker, today) not in _radar_cooldowns
        ):
            _radar_cooldowns[(ticker, today)] = now
            _radar_discord_count += 1
            direction = "🟢" if net_prem > 0 else "🔴"
            net_str   = f"${abs(net_prem)/1_000_000:.1f}M net {'calls' if net_prem > 0 else 'puts'}"
            _post_embed({
                "title":       f"🔍 RADAR — {ticker}  ({direction} unusual activity)",
                "description": (
                    f"**Not a trade signal — manual review only**\n\n"
                    f"Move today: **{pct_change:+.1f}%**  ·  "
                    f"Rel Vol: **{rel_vol:.1f}×**  ·  "
                    f"IV Rank: **{iv_rank:.0f}**  ·  "
                    f"Net premium: **{net_str}**"
                ),
                "color":     0x3fb950 if net_prem > 0 else 0xf85149,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "footer":    {"text": f"QVIX Radar · {_radar_discord_count}/{RADAR_MAX_DISCORD_DAY} today"},
            })
            logging.info(f"  [Radar] Discord ping: {ticker} {rel_vol:.1f}× IVR={iv_rank:.0f} {net_str}")

    try:
        UNUSUAL_ACTIVITY_FILE.write_text(json.dumps(
            {"timestamp": now.isoformat(), "tickers": tickers_out}, indent=2
        ))
    except Exception as exc:
        logging.warning(f"  [Radar] Failed to write {UNUSUAL_ACTIVITY_FILE}: {exc}")

    logging.info(f"  [Radar] {len(tickers_out)} tickers · {_radar_discord_count} Discord pings today")


def main():
    LOG_DIR.mkdir(exist_ok=True)
    _acquire_lock()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(RUNTIME_LOG),
        ],
    )

    try:
        from health import startup_validate
        if not startup_validate("QVIX"):
            sys.exit(1)

        logging.info("QVIX 5.1 — Autonomous Trading Intelligence System")
        logging.info(f"Code version (mtime): {_QVIX_MTIME}")
        logging.info(f"Stocks  : {len(WATCHLIST)} tickers — market hours only")
        logging.info(f"Crypto  : {' · '.join(CRYPTO_WATCHLIST)} — 24/7")
        logging.info(f"Alert threshold: {ALERT_MIN_SCORE}/100  |  Cooldown: stocks {STOCK_COOLDOWN_SECS//3600}h+date / crypto {ALERT_COOLDOWN_SECS//3600}h  |  Scan: every {SCAN_INTERVAL_SECS//60} min")
        logging.info(f"Decisions logged to: {DECISION_LOG}\n")

        _wait_for_network()
        send_startup_notice()
        _check_polygon_api_key()

        while True:
            try:
                # Crypto runs 24/7 every cycle
                run_crypto_scan()

                # Self-guards on weekday + the 8:00-9:33 ET window; fires the
                # briefing (~9:00) and opening bell (~9:30), each at most once
                # per trading day.
                run_premarket_scan()

                # Stocks self-guard on market hours inside run_scan()
                run_scan()

                # Market-wide unusual activity radar (discovery only — not a signal)
                _run_unusual_radar()

                # Once per trading day at 4:15 PM ET — self-guards internally
                run_daily_summary_check()

                # Once per trading day at 3:44 PM ET — query Robinhood for open 0DTE
                # positions and post a Discord close reminder. Self-guards internally.
                run_eod_0dte_closer()

                # DISABLED: Polygon's 15-min data delay makes this scanner
                # unreliable for a 0DTE setup. Superseded by spy_0dte_uw.py,
                # which reads Unusual Whales flow alerts instead — run that
                # as its own process. Left in place (not deleted) because
                # check_zero_dte_position()'s TARGET/CLOSE/REVERSAL checks
                # below still reference _zero_dte_state; with this call
                # disabled that state never gets set, so those checks just
                # no-op instead of erroring.
                # run_zero_dte_scan()

                time.sleep(SCAN_INTERVAL_SECS)
            except KeyboardInterrupt:
                logging.info("QVIX 5.1 stopped by user.")
                break
            except Exception as exc:
                logging.error(f"Unexpected error: {exc}")
                time.sleep(30)
    finally:
        _release_lock()


if __name__ == "__main__":
    main()


def _send_heartbeat() -> None:
    """Post a silent heartbeat to Discord every hour so subscribers and operators know QVIX is alive."""
    now = datetime.now(ET)
    _post_embed({
        "title": "💓  QVIX 5.1 — Online",
        "description": (
            f"Bot is running normally · {now.strftime('%I:%M %p ET')}\n"
            f"Scanning 54 stocks + 8 crypto · Score threshold 65/100"
        ),
        "color": 0x1D9E75,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    })
    logging.info(f"  💓  Heartbeat sent — {now.strftime('%H:%M ET')}")
