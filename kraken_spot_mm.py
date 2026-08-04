#!/usr/bin/env python3
"""
kraken_spot_mm.py — Kraken Spot market-making bot
==================================================
Starting capital : $10,000
Pairs            : SOL/USD · ETH/USD · XBT/USD
Strategy         : Post tight limit bid+ask (post-only, always 0% maker fee)
                   Cancel-on-move guard cuts adverse selection
                   Inventory skew prevents runaway positions

Suggested capital split to fund your Kraken account before running:
  $3,400  USD  (buying power)
  $3,300  SOL  (~44 SOL at current prices)
  $2,100  ETH  (~0.63 ETH)
  $1,200  BTC  (~0.011 BTC)

Uses existing .env keys:
  KRAKEN_API_KEY
  KRAKEN_API_SECRET
  DISCORD_WEBHOOK_URL  (optional — hourly reports)

Run: python3 kraken_spot_mm.py
"""

import asyncio
import base64
from collections import deque
import hashlib
import hmac
import json
import logging
import math
import os
import signal
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, Optional, Tuple

import requests
import websockets
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────

SPOT_REST = "https://api.kraken.com"
SPOT_WS   = "wss://ws.kraken.com/v2"

API_KEY     = os.getenv("KRAKEN_API_KEY", "")
API_SECRET  = os.getenv("KRAKEN_API_SECRET", "")
DISCORD_WH  = os.getenv("DISCORD_WEBHOOK_URL", "")

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Instruments: REST pair → config
# base_asset: key used in Kraken balance response
INSTRUMENTS: Dict[str, dict] = {
    "SOLUSD": {
        "ws_pair":        "SOL/USD",
        "base_asset":     "SOL",
        "min_qty":        0.5,
        "qty_step":       0.5,
        "price_decimals": 2,
        "target_usd":     250,
    },
    "ETHUSD": {
        "ws_pair":        "ETH/USD",
        "base_asset":     "XETH",
        "min_qty":        0.01,
        "qty_step":       0.01,
        "price_decimals": 2,
        "target_usd":     250,
    },
    "XBTUSD": {
        "ws_pair":        "XBT/USD",
        "base_asset":     "XXBT",
        "min_qty":        0.0001,
        "qty_step":       0.0001,
        "price_decimals": 1,
        "target_usd":     200,
    },
}

SPREAD_BPS       = 6       # ±0.06% each side → 0.12% round-trip
MAX_INV_USD      = 2_000   # max net $ exposure per instrument before skew
SKEW_FACTOR      = 2.5
QUOTE_REFRESH_S  = 10
MID_DRIFT_BPS    = 3
MAX_DRAWDOWN_USD = 1_000   # session stop: -$1K (10% of $10K capital)
FILLS_POLL_S     = 15
BALANCE_SYNC_S   = 60
STATS_LOG_S      = 60
DISCORD_REPORT_S = 3_600
RECONCILE_S      = 30

CANCEL_MOVE_BPS  = 8
CANCEL_MOVE_S    = 2.0
COOLDOWN_S       = 4.0
GUARD_POLL_S     = 0.5

# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("smm")

# ──────────────────────────────────────────────────────────────
# KRAKEN SPOT API
# ──────────────────────────────────────────────────────────────

class KrakenSpotAPI:
    """Thin wrapper for Kraken Spot REST API."""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self._sess      = requests.Session()
        self._sess.headers.update({"Accept": "application/json"})

    # ── auth ───────────────────────────────────────────────

    def _sign(self, urlpath: str, data: dict, nonce: str) -> str:
        post_data = urllib.parse.urlencode(data)
        encoded   = (nonce + post_data).encode()
        message   = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac       = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode()

    def _post(self, path: str, data: Optional[dict] = None) -> dict:
        data  = dict(data) if data else {}
        nonce = str(int(time.time() * 1_000_000))   # microseconds — always increasing
        data["nonce"] = nonce
        sig = self._sign(path, data, nonce)
        r   = self._sess.post(
            SPOT_REST + path,
            data=data,
            headers={"API-Key": self.api_key, "API-Sign": sig},
            timeout=10,
        )
        r.raise_for_status()
        resp = r.json()
        if resp.get("error"):
            raise ValueError(f"Kraken: {resp['error']}")
        return resp.get("result", {})

    # ── account ────────────────────────────────────────────

    def get_balance(self) -> dict:
        return self._post("/0/private/Balance")

    def get_open_orders(self) -> dict:
        return self._post("/0/private/OpenOrders")

    def get_trades_history(self, start: Optional[float] = None) -> dict:
        data = {}
        if start:
            data["start"] = str(int(start))
        return self._post("/0/private/TradesHistory", data)

    # ── orders ─────────────────────────────────────────────

    def add_order(
        self,
        pair:  str,
        side:  str,    # "buy" | "sell"
        qty:   float,
        price: float,
    ) -> dict:
        return self._post("/0/private/AddOrder", {
            "pair":      pair,
            "type":      side,
            "ordertype": "limit",
            "price":     str(price),
            "volume":    str(qty),
            "oflags":    "post",   # post-only: rejected if it would cross spread
        })

    def cancel_order(self, txid: str) -> dict:
        return self._post("/0/private/CancelOrder", {"txid": txid})

    def cancel_all(self) -> dict:
        return self._post("/0/private/CancelAll")


# ──────────────────────────────────────────────────────────────
# ORDER BOOK  (Kraken WS v2 format)
# ──────────────────────────────────────────────────────────────

class OrderBook:
    def __init__(self, symbol: str):
        self.symbol  = symbol
        self.bids:   Dict[float, float] = {}
        self.asks:   Dict[float, float] = {}
        self._ready  = False
        self.updated = 0.0

    def apply_snapshot(self, bids: list, asks: list):
        self.bids = {float(e["price"]): float(e["qty"]) for e in bids}
        self.asks = {float(e["price"]): float(e["qty"]) for e in asks}
        self._ready  = True
        self.updated = time.time()

    def apply_update(self, bids: list, asks: list):
        for e in bids:
            p, q = float(e["price"]), float(e["qty"])
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        for e in asks:
            p, q = float(e["price"]), float(e["qty"])
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q
        self.updated = time.time()

    @property
    def best_bid(self) -> Optional[float]:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return min(self.asks) if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        b, a = self.best_bid, self.best_ask
        return (b + a) / 2 if b and a else None

    @property
    def ready(self) -> bool:
        return self._ready and (time.time() - self.updated < 5)


# ──────────────────────────────────────────────────────────────
# BALANCE TRACKER
# ──────────────────────────────────────────────────────────────

class BalanceTracker:
    """Soft-tracks USD and base-asset balances; resyncs from exchange periodically."""

    def __init__(self):
        self._bal: Dict[str, float] = {}
        self.last_sync = 0.0

    def sync(self, api: KrakenSpotAPI):
        try:
            raw = api.get_balance()
            self._bal = {k: float(v) for k, v in raw.items()}
            self.last_sync = time.time()
            log.info(
                f"Balance sync: USD={self.usd:.2f}  "
                + "  ".join(f"{k}={v:.4f}" for k, v in self._bal.items() if k != "ZUSD")
            )
        except Exception as e:
            log.warning(f"Balance sync failed: {e}")

    @property
    def usd(self) -> float:
        return self._bal.get("ZUSD", 0.0)

    def asset(self, key: str) -> float:
        return self._bal.get(key, 0.0)

    def can_buy(self, usd_needed: float) -> bool:
        return self.usd >= usd_needed * 1.01   # 1% headroom

    def can_sell(self, asset_key: str, qty: float) -> bool:
        return self.asset(asset_key) >= qty * 1.01

    def apply_fill(self, side: str, asset_key: str, qty: float, price: float, fee: float):
        cost = qty * price
        if side == "buy":
            self._bal["ZUSD"]    = self.usd - cost - fee
            self._bal[asset_key] = self.asset(asset_key) + qty
        else:
            self._bal["ZUSD"]    = self.usd + cost - fee
            self._bal[asset_key] = max(0.0, self.asset(asset_key) - qty)


# ──────────────────────────────────────────────────────────────
# PER-INSTRUMENT MARKET MAKER
# ──────────────────────────────────────────────────────────────

@dataclass
class ActiveQuote:
    txid:      str
    side:      str
    price:     float
    qty:       float
    placed_at: float = field(default_factory=time.time)


@dataclass
class Fill:
    txid:       str
    pair:       str
    side:       str
    price:      float
    qty:        float
    fee:        float
    ts:         float


class InstrumentMM:
    """Quotes a single spot pair."""

    def __init__(self, pair: str, cfg: dict, api: KrakenSpotAPI, balances: BalanceTracker):
        self.pair     = pair
        self.cfg      = cfg
        self.api      = api
        self.balances = balances
        self.book     = OrderBook(cfg["ws_pair"])

        self.bid_quote: Optional[ActiveQuote] = None
        self.ask_quote: Optional[ActiveQuote] = None

        self._last_mid:     Optional[float] = None
        self._last_quote_t: float           = 0.0

        self.net_pos:   float = 0.0   # base asset net (+ = long)
        self.avg_entry: float = 0.0

        self.realized_pnl: float = 0.0
        self.volume_usd:   float = 0.0
        self.fill_count:   int   = 0
        self.guard_cancels: int  = 0

        self._mid_history:    Deque[Tuple[float, float]] = deque(maxlen=40)
        self._cooldown_until: float = 0.0

    # ── cancel-on-move guard ───────────────────────────────

    def record_mid(self, mid: float):
        self._mid_history.append((time.time(), mid))

    def in_cooldown(self) -> bool:
        return time.time() < self._cooldown_until

    def check_and_fire_guard(self) -> bool:
        if not self._mid_history:
            return False
        now    = time.time()
        cutoff = now - CANCEL_MOVE_S
        window = [(t, p) for t, p in self._mid_history if t >= cutoff]
        if len(window) < 2:
            return False
        oldest_p = window[0][1]
        newest_p = window[-1][1]
        if oldest_p <= 0:
            return False
        move_bps = abs(newest_p - oldest_p) / oldest_p * 10_000
        if move_bps < CANCEL_MOVE_BPS:
            return False

        direction = "UP" if newest_p > oldest_p else "DOWN"
        log.info(
            f"[{self.pair}] GUARD {direction} {move_bps:.1f} bps in {CANCEL_MOVE_S}s"
            f" — pulling quotes ({COOLDOWN_S}s cooldown)"
        )
        self._cancel_side("bid")
        self._cancel_side("ask")
        self._cooldown_until = now + COOLDOWN_S
        self.guard_cancels  += 1
        return True

    # ── quoting ────────────────────────────────────────────

    def _order_qty(self, mid: float) -> float:
        step = self.cfg["qty_step"]
        raw  = self.cfg["target_usd"] / mid
        qty  = max(self.cfg["min_qty"], math.floor(raw / step) * step)
        return round(qty, 8)

    def _compute_prices(self, mid: float) -> Tuple[float, float]:
        half    = SPREAD_BPS / 10_000
        inv_usd = self.net_pos * mid

        if abs(inv_usd) > MAX_INV_USD * 0.4:
            skew = min(abs(inv_usd) / MAX_INV_USD, 1.0) * half * (SKEW_FACTOR - 1)
            if inv_usd > 0:
                half_bid = half * 0.6
                half_ask = half + skew
            else:
                half_bid = half + skew
                half_ask = half * 0.6
        else:
            half_bid = half_ask = half

        pd  = self.cfg["price_decimals"]
        bid = round(mid * (1 - half_bid), pd)
        ask = round(mid * (1 + half_ask), pd)
        return bid, ask

    def _should_requote(self, mid: float) -> bool:
        if self.bid_quote is None or self.ask_quote is None:
            return True
        if time.time() - self._last_quote_t > QUOTE_REFRESH_S:
            return True
        if self._last_mid:
            drift = abs(mid - self._last_mid) / self._last_mid * 10_000
            if drift > MID_DRIFT_BPS:
                return True
        return False

    def requote(self):
        if self.in_cooldown():
            return
        if not self.book.ready:
            return
        mid = self.book.mid
        if mid is None:
            return
        if not self._should_requote(mid):
            return
        if abs(self.net_pos * mid) >= MAX_INV_USD * 1.8:
            log.warning(f"[{self.pair}] inventory ${self.net_pos * mid:+.0f} at hard cap — skipping")
            return

        self._cancel_side("bid")
        self._cancel_side("ask")

        bid_p, ask_p = self._compute_prices(mid)
        qty          = self._order_qty(mid)

        # Buy side — need USD
        if self.balances.can_buy(bid_p * qty):
            try:
                r    = self.api.add_order(self.pair, "buy", qty, bid_p)
                txids = r.get("txids", [])
                if txids:
                    self.bid_quote = ActiveQuote(txid=txids[0], side="buy", price=bid_p, qty=qty)
            except ValueError as e:
                if "post" in str(e).lower():
                    pass   # post-only rejection — price crossed spread, skip
                else:
                    log.error(f"[{self.pair}] bid order error: {e}")
            except Exception as e:
                log.error(f"[{self.pair}] bid order error: {e}")
        else:
            log.debug(f"[{self.pair}] insufficient USD for bid ({bid_p * qty:.2f} needed)")

        # Sell side — need base asset
        if self.balances.can_sell(self.cfg["base_asset"], qty):
            try:
                r    = self.api.add_order(self.pair, "sell", qty, ask_p)
                txids = r.get("txids", [])
                if txids:
                    self.ask_quote = ActiveQuote(txid=txids[0], side="sell", price=ask_p, qty=qty)
            except ValueError as e:
                if "post" in str(e).lower():
                    pass
                else:
                    log.error(f"[{self.pair}] ask order error: {e}")
            except Exception as e:
                log.error(f"[{self.pair}] ask order error: {e}")
        else:
            log.debug(f"[{self.pair}] insufficient {self.cfg['base_asset']} for ask ({qty} needed)")

        self._last_mid     = mid
        self._last_quote_t = time.time()

        bid_txt = f"bid={bid_p}" if self.bid_quote else "bid=SKIP(no USD)"
        ask_txt = f"ask={ask_p}" if self.ask_quote else "ask=SKIP(no asset)"
        log.info(
            f"[{self.pair}] quoted  {bid_txt}  {ask_txt}  qty={qty}"
            f"  mid={mid:.{self.cfg['price_decimals']}f}"
            f"  inv={self.net_pos:+.4f} (${self.net_pos * mid:+.0f})"
        )

    def _cancel_side(self, side: str):
        q = self.bid_quote if side == "bid" else self.ask_quote
        if q:
            try:
                self.api.cancel_order(q.txid)
            except Exception:
                pass
        if side == "bid":
            self.bid_quote = None
        else:
            self.ask_quote = None

    def cancel_all(self):
        self._cancel_side("bid")
        self._cancel_side("ask")

    # ── fill handler ───────────────────────────────────────

    def on_fill(self, fill: Fill):
        qty = fill.qty
        p   = fill.price
        ak  = self.cfg["base_asset"]

        if fill.side == "buy":
            if self.net_pos >= 0:
                total_cost     = self.net_pos * self.avg_entry + qty * p
                self.net_pos  += qty
                self.avg_entry = total_cost / self.net_pos if self.net_pos else p
            else:
                covered = min(qty, abs(self.net_pos))
                self.realized_pnl += (self.avg_entry - p) * covered
                self.net_pos  += qty
                if self.net_pos > 0:
                    self.avg_entry = p
            self.bid_quote = None
        else:
            if self.net_pos <= 0:
                total_cost     = abs(self.net_pos) * self.avg_entry + qty * p
                self.net_pos  -= qty
                self.avg_entry = total_cost / abs(self.net_pos) if self.net_pos else p
            else:
                closed = min(qty, self.net_pos)
                self.realized_pnl += (p - self.avg_entry) * closed
                self.net_pos  -= qty
                if self.net_pos < 0:
                    self.avg_entry = p
            self.ask_quote = None

        self.realized_pnl -= fill.fee
        self.volume_usd   += qty * p
        self.fill_count   += 1
        self.balances.apply_fill(fill.side, ak, qty, p, fill.fee)

        log.info(
            f"[{self.pair}] FILL {fill.side.upper():4s}  {qty}@{p}"
            f"  fee=${fill.fee:.4f}  net_pos={self.net_pos:+.4f}"
            f"  rpnl=${self.realized_pnl:.2f}"
        )
        self.requote()

    # ── reconcile ──────────────────────────────────────────

    def reconcile(self, live_txids: set):
        if self.bid_quote and self.bid_quote.txid not in live_txids:
            log.debug(f"[{self.pair}] bid txid {self.bid_quote.txid} gone — clearing")
            self.bid_quote = None
        if self.ask_quote and self.ask_quote.txid not in live_txids:
            log.debug(f"[{self.pair}] ask txid {self.ask_quote.txid} gone — clearing")
            self.ask_quote = None

    # ── stats ──────────────────────────────────────────────

    def summary(self) -> str:
        mid  = self.book.mid or 0
        upnl = (mid - self.avg_entry) * self.net_pos if mid and self.avg_entry else 0
        return (
            f"[{self.pair}]  fills={self.fill_count:5d}"
            f"  vol=${self.volume_usd:>10,.0f}"
            f"  pos={self.net_pos:+.4f}"
            f"  rpnl=${self.realized_pnl:+.2f}"
            f"  upnl=${upnl:+.2f}"
            f"  guards={self.guard_cancels}"
        )


# ──────────────────────────────────────────────────────────────
# VOLUME TRACKER
# ──────────────────────────────────────────────────────────────

class VolumeTracker:
    def __init__(self):
        self._start   = time.time()
        self.session  = 0.0
        self.daily    = 0.0
        self.monthly  = 0.0
        self._day     = datetime.now(timezone.utc).date()
        self._month   = datetime.now(timezone.utc).month

    def add(self, usd: float):
        now = datetime.now(timezone.utc)
        if now.date() != self._day:
            log.info(f"Day rollover — daily volume ${self.daily:,.0f}")
            self.daily = 0.0
            self._day  = now.date()
        if now.month != self._month:
            log.info(f"Month rollover — monthly volume ${self.monthly:,.0f}")
            self.monthly = 0.0
            self._month  = now.month
        self.session += usd
        self.daily   += usd
        self.monthly += usd

    def summary(self) -> str:
        elapsed_h = max((time.time() - self._start) / 3_600, 0.001)
        run_rate  = self.session / elapsed_h * 24 * 365
        # fee tier: 0% at $10M+/month 30-day rolling
        tier = "0.00% ✓" if self.monthly >= 10_000_000 else f"building (${self.monthly:,.0f}/$10M)"
        return (
            f"Volume — session: ${self.session:>10,.0f}"
            f"  daily: ${self.daily:>10,.0f}"
            f"  monthly: ${self.monthly:>10,.0f}"
            f"  run-rate/yr: ${run_rate:>12,.0f}"
            f"  fee tier: {tier}"
        )


# ──────────────────────────────────────────────────────────────
# DISCORD
# ──────────────────────────────────────────────────────────────

def _discord(text: str, color: int = 1940085):
    if not DISCORD_WH:
        return
    try:
        requests.post(DISCORD_WH, json={"embeds": [{
            "title":       "📈 QVIX Spot MM",
            "description": text,
            "color":       color,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }]}, timeout=5)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ──────────────────────────────────────────────────────────────

class SpotMarketMaker:

    def __init__(self):
        self.api      = KrakenSpotAPI(API_KEY, API_SECRET)
        self.balances = BalanceTracker()
        self.makers   = {
            pair: InstrumentMM(pair, cfg, self.api, self.balances)
            for pair, cfg in INSTRUMENTS.items()
        }
        self.tracker  = VolumeTracker()
        self._running = True

        self._seen_fills:     set            = set()
        self._fills_since:    float          = time.time() - 3600   # last hour on startup
        self._last_stats_t    = time.time()
        self._last_discord_t  = time.time()
        self._last_reconcile  = time.time()
        self._last_bal_sync   = 0.0

    # ── risk ───────────────────────────────────────────────

    def _session_pnl(self) -> float:
        return sum(m.realized_pnl for m in self.makers.values())

    def _check_risk(self) -> bool:
        pnl = self._session_pnl()
        if pnl < -MAX_DRAWDOWN_USD:
            log.critical(f"EMERGENCY STOP — drawdown ${pnl:.2f}")
            _discord(f"🚨 **EMERGENCY STOP** — drawdown ${pnl:.2f}", color=15158332)
            return False
        return True

    # ── fills ──────────────────────────────────────────────

    def _poll_fills(self):
        try:
            result = self.api.get_trades_history(start=self._fills_since)
            trades = result.get("trades", {})
        except Exception as e:
            log.warning(f"TradesHistory error: {e}")
            return

        for txid, t in trades.items():
            if txid in self._seen_fills:
                continue
            self._seen_fills.add(txid)

            pair  = t.get("pair", "")
            side  = t.get("type", "")
            qty   = float(t.get("vol", 0))
            price = float(t.get("price", 0))
            fee   = float(t.get("fee", 0))
            ts    = float(t.get("time", 0))

            self._fills_since = max(self._fills_since, ts)

            mm = self.makers.get(pair)
            if mm and qty > 0:
                fill = Fill(txid=txid, pair=pair, side=side,
                            price=price, qty=qty, fee=fee, ts=ts)
                mm.on_fill(fill)
                self.tracker.add(qty * price)

    # ── reconcile ──────────────────────────────────────────

    def _reconcile(self):
        try:
            result = self.api.get_open_orders()
            open_orders = result.get("open", {})
            live_txids  = set(open_orders.keys())
        except Exception as e:
            log.warning(f"reconcile error: {e}")
            return
        for mm in self.makers.values():
            mm.reconcile(live_txids)

    # ── loops ──────────────────────────────────────────────

    async def _run_fast_guard(self):
        while self._running:
            for mm in self.makers.values():
                if mm.bid_quote is None and mm.ask_quote is None:
                    continue
                try:
                    mm.check_and_fire_guard()
                except Exception as e:
                    log.debug(f"[{mm.pair}] guard error: {e}")
            await asyncio.sleep(GUARD_POLL_S)

    async def _run_quotes(self):
        while self._running:
            if not self._check_risk():
                self._running = False
                break

            now = time.time()

            if now - self._last_bal_sync > BALANCE_SYNC_S:
                self.balances.sync(self.api)
                self._last_bal_sync = now

            if now - self._last_reconcile > RECONCILE_S:
                self._reconcile()
                self._last_reconcile = now

            for mm in self.makers.values():
                try:
                    mm.requote()
                except Exception as e:
                    log.error(f"[{mm.pair}] requote error: {e}")

            await asyncio.sleep(QUOTE_REFRESH_S)

    async def _run_fills(self):
        while self._running:
            self._poll_fills()
            await asyncio.sleep(FILLS_POLL_S)

    async def _run_reporting(self):
        while self._running:
            now = time.time()
            if now - self._last_stats_t >= STATS_LOG_S:
                log.info(self.tracker.summary())
                for mm in self.makers.values():
                    log.info(mm.summary())
                log.info(f"Session PnL: ${self._session_pnl():+.2f}  "
                         f"Balance USD: ${self.balances.usd:.2f}")
                self._last_stats_t = now

            if now - self._last_discord_t >= DISCORD_REPORT_S:
                lines = ["**Hourly Report**", "", self.tracker.summary(), ""]
                for mm in self.makers.values():
                    lines.append(mm.summary())
                lines.append(f"\nSession PnL: **${self._session_pnl():+.2f}**")
                lines.append(f"USD balance: **${self.balances.usd:.2f}**")
                _discord("\n".join(lines))
                self._last_discord_t = now

            await asyncio.sleep(5)

    # ── orderbook WebSocket ─────────────────────────────────

    async def _run_ws(self):
        ws_pairs = [cfg["ws_pair"] for cfg in INSTRUMENTS.values()]
        sub_msg  = json.dumps({
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol":  ws_pairs,
                "depth":   25,
            }
        })
        # Build reverse lookup: ws_pair → InstrumentMM
        pair_to_mm = {cfg["ws_pair"]: mm for mm in self.makers.values()
                      for pair, cfg in INSTRUMENTS.items() if cfg == mm.cfg}

        while self._running:
            try:
                async with websockets.connect(SPOT_WS, ping_interval=20, open_timeout=15) as ws:
                    await ws.send(sub_msg)
                    log.info(f"WS connected — subscribed to {ws_pairs}")

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            if not isinstance(msg, dict):
                                continue
                            channel = msg.get("channel", "")
                            if channel != "book":
                                continue
                            msg_type = msg.get("type", "")
                            for entry in msg.get("data", []):
                                sym = entry.get("symbol", "")
                                mm  = pair_to_mm.get(sym)
                                if not mm:
                                    continue
                                if msg_type == "snapshot":
                                    mm.book.apply_snapshot(
                                        entry.get("bids", []),
                                        entry.get("asks", []),
                                    )
                                elif msg_type == "update":
                                    mm.book.apply_update(
                                        entry.get("bids", []),
                                        entry.get("asks", []),
                                    )
                                mid = mm.book.mid
                                if mid:
                                    mm.record_mid(mid)
                        except Exception as e:
                            log.debug(f"WS parse error: {e}")

            except Exception as e:
                if self._running:
                    log.warning(f"WS disconnected ({e}) — reconnecting in 3s")
                    await asyncio.sleep(3)

    # ── shutdown ───────────────────────────────────────────

    def _halt(self):
        self._running = False
        log.info("Cancelling all open orders...")
        try:
            self.api.cancel_all()
            log.info("All orders cancelled.")
        except Exception as e:
            log.error(f"cancel_all error: {e}")
        _discord(
            f"⛔ Spot MM stopped\n{self.tracker.summary()}"
            f"\nFinal PnL: **${self._session_pnl():+.2f}**",
            color=15158332,
        )

    # ── entry ──────────────────────────────────────────────

    async def run(self):
        log.info("=" * 60)
        log.info("QVIX Kraken Spot Market Maker")
        log.info(f"Pairs   : {list(INSTRUMENTS.keys())}")
        log.info(f"Spread  : ±{SPREAD_BPS} bps ({SPREAD_BPS * 2} bps round-trip)")
        log.info(f"Orders  : ${min(c['target_usd'] for c in INSTRUMENTS.values())}–"
                 f"${max(c['target_usd'] for c in INSTRUMENTS.values())} per side")
        log.info(f"Max inv : ${MAX_INV_USD:,} per pair")
        log.info(f"Stop    : −${MAX_DRAWDOWN_USD:,} session drawdown")
        log.info(f"Guard   : >{CANCEL_MOVE_BPS} bps in {CANCEL_MOVE_S}s → {COOLDOWN_S}s cooldown")
        log.info("=" * 60)

        if not API_KEY or not API_SECRET:
            log.error("KRAKEN_API_KEY / KRAKEN_API_SECRET not set in .env")
            sys.exit(1)

        # Account check
        self.balances.sync(self.api)
        if self.balances.usd < 100:
            log.warning(f"USD balance ${self.balances.usd:.2f} is very low — "
                        "fund your Kraken account before running live")

        _discord(
            f"✅ **Spot MM started**\n"
            f"Pairs: {', '.join(INSTRUMENTS.keys())}\n"
            f"Spread: ±{SPREAD_BPS} bps  |  Stop: −${MAX_DRAWDOWN_USD:,}\n"
            f"USD available: ${self.balances.usd:.2f}"
        )

        try:
            await asyncio.gather(
                self._run_ws(),
                self._run_fast_guard(),
                self._run_quotes(),
                self._run_fills(),
                self._run_reporting(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            self._halt()


# ──────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from health import startup_validate
    if not startup_validate("kraken_spot_mm"):
        sys.exit(1)

    bot = SpotMarketMaker()

    def _on_signal(sig, _frame):
        log.info(f"Signal {sig} — shutting down")
        bot._running = False

    signal.signal(signal.SIGINT,  _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    asyncio.run(bot.run())
