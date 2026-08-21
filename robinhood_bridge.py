#!/usr/bin/env python3
"""
robinhood_bridge.py — Robinhood enrichment bridge for QVIX.

Runs every 5 minutes (via launchctl StartInterval). Fetches fundamentals
(P/E ratio, next earnings date) for every stock in WATCHLIST in a single
batch call, then writes logs/robinhood_enrichment.json for qvix.py to read
at signal evaluation time.

Options bid/ask is NOT pre-fetched here because the relevant strike is only
known after _option_plan() runs at signal time inside qvix.py. The bridge
handles the slower/heavier fundamentals batch; qvix.py's Layer 1 stub
(fetch_robinhood_enrichment) adds the bid/ask from this file.

Add to .env:
  ROBINHOOD_ACCESS_TOKEN=<your bearer token>

How to get your token:
  1. Open browser DevTools → Network tab
  2. Log into robinhood.com
  3. Filter requests for "api.robinhood.com"
  4. Copy the "Authorization: Bearer ..." header value from any request
  5. Paste the token (without "Bearer ") into .env
  Token typically expires after 24h — refresh as needed.
"""

import json
import logging
import os
import socket
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

from rh_token_manager import run_refresh, token_hours_remaining

# Auto-refresh if access token is expired or within 2h of expiry.
run_refresh()

ROBINHOOD_ACCESS_TOKEN = os.getenv("ROBINHOOD_ACCESS_TOKEN", "")
RH_BASE                = "https://api.robinhood.com"

LOG_DIR                  = Path("logs")
ROBINHOOD_ENRICHMENT_FILE = LOG_DIR / "robinhood_enrichment.json"
BRIDGE_LOG               = LOG_DIR / "robinhood_bridge.log"

WATCHLIST = [
    "SPY",  "QQQ",  "IWM",  "DIA",  "AAPL", "NVDA", "TSLA", "MSFT", "META",
    "AMZN", "GOOGL", "AMD", "PLTR", "COIN", "MSTR", "HOOD", "SOFI", "NIO",
    "RIVN", "LCID", "SMCI", "IONQ", "RKLB", "MARA", "RIOT", "HUT",  "CLSK",
    "BTBT", "WULF", "APLD", "CORZ", "IREN", "CEG",  "VST",  "BE",   "CRWV",
    "SMH",  "AVGO", "MU",   "TSM",  "GLD",  "SLV",  "COPX", "URA",  "USO",
    "FCX",  "MP",   "ON",   "NXP",  "TER",  "AMBA", "CGNX", "AMKR", "SPCX",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BRIDGE_LOG),
    ],
)


def _rh_headers() -> dict:
    return {
        "Authorization": f"Bearer {ROBINHOOD_ACCESS_TOKEN}",
        "Accept":        "application/json",
    }


def _days_until(date_str: str) -> int:
    """Return calendar days until an ISO date string, or 999 if unparseable."""
    try:
        target = date.fromisoformat(date_str[:10])
        return max(0, (target - date.today()).days)
    except (ValueError, TypeError):
        return 999


def fetch_fundamentals_batch(tickers: list) -> dict:
    """
    Single GET /fundamentals/?symbols=A,B,C returns fundamentals for all
    tickers at once — Robinhood supports up to ~75 in one call.
    """
    r = requests.get(
        f"{RH_BASE}/fundamentals/",
        params={"symbols": ",".join(tickers)},
        headers=_rh_headers(),
        timeout=15,
    )
    r.raise_for_status()
    raw = r.json()
    results = raw.get("results", []) or []
    if results and not results[0]:
        logging.debug(f"  fundamentals: first result is null — positional matching active")
    elif results and not results[0].get("symbol"):
        logging.debug(f"  fundamentals: no symbol field in results — positional matching active")

    enrichment = {}
    for item, sym in zip(results, tickers):
        if not item:
            continue
        sym = item.get("symbol") or item.get("ticker") or sym
        if not sym:
            continue
        pe_str       = item.get("pe_ratio")
        earnings_str = item.get("next_earnings_date") or item.get("earnings_date")
        pe_ratio     = float(pe_str) if pe_str else None
        earnings_days = _days_until(earnings_str) if earnings_str else None
        enrichment[sym] = {
            "pe_ratio":     round(pe_ratio, 1) if pe_ratio else None,
            "earnings_days": earnings_days if earnings_days is not None and earnings_days < 365 else None,
            "ts":           datetime.now(timezone.utc).isoformat(),
        }
    return enrichment


def _get_with_retry(url: str, params: Optional[dict] = None, retries: int = 1) -> Optional[dict]:
    """
    Shared GET-with-retry for the options-chain lookups below — one 2s
    backoff retry, same pattern the fundamentals batch already uses in
    main(). Returns None (never raises) so one bad ticker/ID can't take
    down the rest of the cycle's enrichment.
    """
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=_rh_headers(), timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt < retries:
                time.sleep(2)
                continue
            logging.warning(f"  GET {url} failed: {exc}")
            return None


def fetch_quotes_batch(tickers: list) -> dict:
    """
    Single GET /quotes/?symbols=A,B,C for live last-trade price. Nothing
    else in this bridge fetches a live stock price today — it's needed here
    to pick a near-the-money strike per ticker before the options lookup.
    """
    data = _get_with_retry(f"{RH_BASE}/quotes/", {"symbols": ",".join(tickers)})
    if not data:
        return {}
    prices = {}
    for item in data.get("results", []) or []:
        sym       = item.get("symbol")
        price_str = item.get("last_trade_price") or item.get("last_extended_hours_trade_price")
        if not sym or not price_str:
            continue
        try:
            prices[sym] = float(price_str)
        except (TypeError, ValueError):
            continue
    return prices


def get_equity_instrument_id(ticker: str) -> Optional[str]:
    data = _get_with_retry(f"{RH_BASE}/instruments/", {"symbol": ticker})
    if not data:
        return None
    results = data.get("results", [])
    return results[0]["id"] if results else None


def get_chain_id(ticker: str) -> Optional[str]:
    """
    Resolve a fresh options chain_id for this ticker. Never cached across
    tickers — chain_id is underlying-specific, so each ticker needs its own
    instrument-id lookup + chain lookup every cycle.
    """
    instrument_id = get_equity_instrument_id(ticker)
    if not instrument_id:
        return None
    data = _get_with_retry(f"{RH_BASE}/options/chains/", {"equity_instrument_ids": instrument_id})
    if not data:
        return None
    results = data.get("results", [])
    return results[0]["id"] if results else None


def _atm_strike_increment(price: float) -> float:
    """Standard listed-option strike spacing by underlying price band (same convention as qvix.py's _strike_increment)."""
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


def get_atm_option_instrument(chain_id: str, expiration_date: str, strike: float) -> Optional[dict]:
    data = _get_with_retry(f"{RH_BASE}/options/instruments/", {
        "chain_id":          chain_id,
        "expiration_dates":  expiration_date,
        "type":              "call",
        "state":             "active",
    })
    if not data:
        return None
    results = data.get("results", [])
    if not results:
        return None
    return min(results, key=lambda c: abs(float(c.get("strike_price") or 0) - strike))


def get_option_quote(instrument_id: str) -> Optional[dict]:
    data = _get_with_retry(f"{RH_BASE}/marketdata/options/{instrument_id}/")
    if not data:
        return None
    try:
        bid, ask = float(data.get("bid_price")), float(data.get("ask_price"))
    except (TypeError, ValueError):
        return None
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    return {
        "bid":           bid,
        "ask":           ask,
        "volume":        int(data.get("volume") or 0),
        "open_interest": int(data.get("open_interest") or 0),
    }


def fetch_atm_option_quote(ticker: str, current_price: Optional[float]) -> Optional[dict]:
    """
    Live bid/ask/volume/open-interest for a representative near-the-money
    CALL, picked fresh each cycle: nearest listed strike to current_price,
    nearest Friday expiry >= 7 days out. This is real, tradable-contract
    data — not the exact contract a future signal will trade (that's
    decided at signal time in qvix.py from the signal's own direction/
    score); it's market color for the "Market Context" Discord field.
    Returns None on any failure — never raises, never blocks the batch.
    """
    if not current_price:
        return None

    chain_id = get_chain_id(ticker)
    if not chain_id:
        return None

    inc    = _atm_strike_increment(current_price)
    strike = round(round(current_price / inc) * inc, 2)
    expiry = _next_friday(7)

    instrument = get_atm_option_instrument(chain_id, expiry.isoformat(), strike)
    if not instrument:
        return None

    quote = get_option_quote(instrument.get("id"))
    if not quote:
        return None

    mid        = (quote["bid"] + quote["ask"]) / 2
    spread_pct = round((quote["ask"] - quote["bid"]) / mid * 100, 2) if mid else None

    return {
        "bid":           quote["bid"],
        "ask":           quote["ask"],
        "spread_pct":    spread_pct,
        "volume":        quote["volume"],
        "open_interest": quote["open_interest"],
        "strike":        float(instrument.get("strike_price") or strike),
        "expiry":        instrument.get("expiration_date") or expiry.isoformat(),
        "option_type":   "call",
    }


def _wait_for_dns(host: str = "api.robinhood.com", retries: int = 30, delay: float = 1.0) -> None:
    for attempt in range(retries):
        try:
            socket.getaddrinfo(host, 443)
            if attempt:
                logging.info(f"DNS ready after {attempt}s")
            return
        except socket.gaierror:
            if attempt == 0:
                logging.info("DNS not yet ready — waiting (boot delay?)…")
            time.sleep(delay)
    logging.warning(f"DNS still not ready after {retries}s — proceeding anyway")


def main():
    LOG_DIR.mkdir(exist_ok=True)
    _wait_for_dns()

    if not ROBINHOOD_ACCESS_TOKEN:
        logging.warning("ROBINHOOD_ACCESS_TOKEN not set — writing empty enrichment file")
        ROBINHOOD_ENRICHMENT_FILE.write_text(json.dumps({}))
        return

    enrichment = {}
    # Robinhood fundamentals endpoint handles all tickers in one call.
    # Batch in chunks of 50 to stay well within any undocumented limits.
    chunk_size = 50
    for i in range(0, len(WATCHLIST), chunk_size):
        batch = WATCHLIST[i: i + chunk_size]
        for attempt in range(2):
            try:
                chunk_result = fetch_fundamentals_batch(batch)
                enrichment.update(chunk_result)
                logging.info(f"  Batch {i//chunk_size + 1}: fetched {len(chunk_result)} tickers")
                break
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 401:
                    logging.error("Robinhood 401 — ROBINHOOD_ACCESS_TOKEN may be expired. Refresh it in .env.")
                    ROBINHOOD_ENRICHMENT_FILE.write_text(json.dumps({}))
                    return
                if attempt == 0:
                    time.sleep(2)
                    continue
                logging.warning(f"  Batch {i//chunk_size + 1} failed after retry: {exc}")
            except Exception as exc:
                if attempt == 0:
                    time.sleep(2)
                    continue
                logging.warning(f"  Batch {i//chunk_size + 1} error: {exc}")

    # ── Live ATM options quotes: one batched price lookup, then a per-ticker
    # chain/instrument/quote chain. If the price batch comes back empty
    # (e.g. an expired token), every ticker below just no-ops — no wasted
    # calls chasing a lookup that can't succeed.
    prices = fetch_quotes_batch(WATCHLIST)
    logging.info(f"  Live quotes: {len(prices)}/{len(WATCHLIST)} tickers")

    for ticker in WATCHLIST:
        if ticker not in enrichment:
            continue
        try:
            opt_quote = fetch_atm_option_quote(ticker, prices.get(ticker))
        except Exception as exc:
            logging.warning(f"  {ticker}: options quote error: {exc}")
            opt_quote = None

        if opt_quote:
            enrichment[ticker].update(opt_quote)
        else:
            enrichment[ticker].setdefault("bid", None)
            enrichment[ticker].setdefault("ask", None)
            enrichment[ticker].setdefault("spread_pct", None)
            enrichment[ticker].setdefault("volume", None)
            enrichment[ticker].setdefault("open_interest", None)
        time.sleep(0.5)   # ~4 extra calls/ticker — avoid hammering Robinhood's unofficial API

    bak = ROBINHOOD_ENRICHMENT_FILE.with_suffix(".json.bak")
    if ROBINHOOD_ENRICHMENT_FILE.exists():
        bak.write_text(ROBINHOOD_ENRICHMENT_FILE.read_text())
    ROBINHOOD_ENRICHMENT_FILE.write_text(json.dumps(enrichment, indent=2))
    logging.info(f"Robinhood bridge: wrote {len(enrichment)} tickers → {ROBINHOOD_ENRICHMENT_FILE}")


if __name__ == "__main__":
    main()
