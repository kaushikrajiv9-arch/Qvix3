#!/usr/bin/env python3
"""
kraken_mm.py — Kraken Futures market-making bot
================================================
Target : $1B/year volume → -0.02% maker rebate → ~$200K/year
Strategy: Post tight limit bid+ask on PF_SOLUSD, PF_XBTUSD, PF_ETHUSD
          Inventory skew prevents runaway positions.
          Emergency stop if session drawdown > MAX_DRAWDOWN_USD.

Required .env keys:
  KRAKEN_FUTURES_API_KEY=...
  KRAKEN_FUTURES_API_SECRET=...
  (optional) DISCORD_WEBHOOK_URL=...   ← existing key reused

Run:
  python3 kraken_mm.py

Requires (already installed):
  pip install requests websockets python-dotenv
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
from typing import Deque, Dict, List, Optional, Tuple

import requests
import websockets
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────

FUTURES_REST = "https://futures.kraken.com/derivatives/api/v3"
FUTURES_WS   = "wss://futures.kraken.com/ws/v1"

API_KEY    = os.getenv("KRAKEN_FUTURES_API_KEY", "")
API_SECRET = os.getenv("KRAKEN_FUTURES_API_SECRET", "")

from discord_format import get_discord_webhook_url
DISCORD_WEBHOOK = get_discord_webhook_url()

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Instruments: symbol → quoting config
INSTRUMENTS: Dict[str, dict] = {
    "PF_SOLUSD": {
        "min_qty":        1.0,     # minimum order size (1 SOL)
        "qty_step":       1.0,     # quantity granularity
        "price_decimals": 3,       # price tick precision
        "target_usd":     1_000,   # target ~$1K notional per side
    },
    "PF_XBTUSD": {
        "min_qty":        0.001,
        "qty_step":       0.001,
        "price_decimals": 1,
        "target_usd":     2_000,
    },
    "PF_ETHUSD": {
        "min_qty":        0.01,
        "qty_step":       0.01,
        "price_decimals": 2,
        "target_usd":     1_500,
    },
}

SPREAD_BPS        = 5      # half-spread each side in bps (0.05% each → 0.10% round trip)
MAX_INV_USD       = 5_000  # max net $ exposure per instrument before skewing
SKEW_FACTOR       = 2.5    # how aggressively to skew spread when inventory-heavy
QUOTE_REFRESH_S   = 10     # force requote every N seconds
MID_DRIFT_BPS     = 3      # requote if mid drifts more than this since last quote
MAX_DRAWDOWN_USD  = 3_000  # emergency halt if session PnL falls below -$3K
FILLS_POLL_S      = 2      # poll the fills endpoint this often
STATS_LOG_S       = 60     # log a stats line this often
DISCORD_REPORT_S  = 3_600  # send a Discord summary this often
RECONCILE_S       = 30     # resync open orders with exchange this often

# Cancel-on-move guard
CANCEL_MOVE_BPS   = 8      # cancel quotes if mid moves this many bps...
CANCEL_MOVE_S     = 2.0    # ...within this many seconds
COOLDOWN_S        = 4.0    # pause requoting this long after a guard trigger
GUARD_POLL_S      = 0.5    # how often the guard checks price velocity

# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "kraken_mm.log"),
    ],
)
log = logging.getLogger("mm")

# ──────────────────────────────────────────────────────────────
# KRAKEN FUTURES API CLIENT
# ──────────────────────────────────────────────────────────────

class KrakenFuturesAPI:
    """Minimal REST wrapper for Kraken Futures v3."""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self._sess      = requests.Session()
        self._sess.headers.update({"Accept": "application/json"})

    # ── auth ───────────────────────────────────────────────

    def _sign(self, endpoint: str, post_data: str, nonce: str) -> str:
        # Kraken Futures signature:
        #   SHA256(postData + nonce + endpoint) → HMAC-SHA512(b64decode(secret)) → b64encode
        message     = (post_data + nonce + endpoint).encode("utf-8")
        sha256_hash = hashlib.sha256(message).digest()
        secret      = base64.b64decode(self.api_secret)
        mac         = hmac.new(secret, sha256_hash, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _headers(self, endpoint: str, post_data: str = "") -> dict:
        nonce = str(int(time.time() * 1000))
        return {
            "APIKey":  self.api_key,
            "Nonce":   nonce,
            "Authent": self._sign(endpoint, post_data, nonce),
        }

    # ── public ─────────────────────────────────────────────

    def get_tickers(self) -> dict:
        return self._get("/tickers")

    def get_orderbook(self, symbol: str) -> dict:
        return self._get("/orderbook", {"symbol": symbol})

    # ── private ────────────────────────────────────────────

    def get_accounts(self) -> dict:
        return self._get("/accounts")

    def get_open_positions(self) -> dict:
        return self._get("/openpositions")

    def get_open_orders(self) -> dict:
        return self._get("/openorders")

    def get_fills(self, last_fill_time: Optional[str] = None) -> dict:
        params = {}
        if last_fill_time:
            params["lastFillTime"] = last_fill_time
        return self._get("/fills", params)

    def send_order(
        self,
        symbol:    str,
        side:      str,    # "buy" | "sell"
        qty:       float,
        price:     float,
        cli_ord_id: Optional[str] = None,
    ) -> dict:
        data: dict = {
            "orderType":  "lmt",
            "symbol":     symbol,
            "side":       side,
            "size":       qty,
            "limitPrice": price,
        }
        if cli_ord_id:
            data["cliOrdId"] = cli_ord_id
        return self._post("/sendorder", data)

    def cancel_order(self, order_id: str) -> dict:
        return self._post("/cancelorder", {"order_id": order_id})

    def cancel_all(self, symbol: Optional[str] = None) -> dict:
        data: dict = {}
        if symbol:
            data["symbol"] = symbol
        return self._post("/cancelallorders", data)

    # ── internals ──────────────────────────────────────────

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        endpoint = path
        hdrs     = self._headers(endpoint)
        r = self._sess.get(
            FUTURES_REST + path,
            params=params,
            headers=hdrs,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, data: dict) -> dict:
        endpoint  = path
        post_data = urllib.parse.urlencode(sorted(data.items()))
        hdrs      = self._headers(endpoint, post_data)
        hdrs["Content-Type"] = "application/x-www-form-urlencoded"
        r = self._sess.post(
            FUTURES_REST + path,
            data=data,
            headers=hdrs,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()


# ──────────────────────────────────────────────────────────────
# ORDER BOOK (maintained from WebSocket)
# ──────────────────────────────────────────────────────────────

class OrderBook:
    """Level-2 order book updated from WS snapshots and deltas."""

    def __init__(self, symbol: str):
        self.symbol  = symbol
        self.bids: Dict[float, float] = {}   # price → qty
        self.asks: Dict[float, float] = {}
        self._ready  = False
        self.updated = 0.0

    def apply_snapshot(self, bids: list, asks: list):
        self.bids = {float(p): float(q) for p, q in bids}
        self.asks = {float(p): float(q) for p, q in asks}
        self._ready  = True
        self.updated = time.time()

    def apply_delta(self, side: str, price: float, qty: float):
        book = self.bids if side == "buy" else self.asks
        if qty == 0:
            book.pop(price, None)
        else:
            book[price] = qty
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
    def spread_bps(self) -> Optional[float]:
        b, a = self.best_bid, self.best_ask
        if b and a and b > 0:
            return (a - b) / b * 10_000
        return None

    @property
    def ready(self) -> bool:
        return self._ready and (time.time() - self.updated < 5)


# ──────────────────────────────────────────────────────────────
# PER-INSTRUMENT MARKET MAKER
# ──────────────────────────────────────────────────────────────

@dataclass
class ActiveQuote:
    order_id:  str
    side:      str
    price:     float
    qty:       float
    placed_at: float = field(default_factory=time.time)


@dataclass
class Fill:
    order_id: str
    symbol:   str
    side:     str
    price:    float
    qty:      float
    fee:      float
    ts:       str


class InstrumentMM:
    """Quotes a single perpetual contract."""

    def __init__(self, symbol: str, cfg: dict, api: KrakenFuturesAPI):
        self.symbol = symbol
        self.cfg    = cfg
        self.api    = api
        self.book   = OrderBook(symbol)

        self.bid_quote: Optional[ActiveQuote] = None
        self.ask_quote: Optional[ActiveQuote] = None

        self._last_mid:     Optional[float] = None
        self._last_quote_t: float           = 0.0

        # net position in base asset (positive = long, negative = short)
        self.net_pos:   float = 0.0
        self.avg_entry: float = 0.0

        # accounting
        self.realized_pnl:    float = 0.0
        self.volume_usd:      float = 0.0
        self.fill_count:      int   = 0
        self.maker_rebate:    float = 0.0   # estimated at -0.02%
        self.guard_cancels:   int   = 0     # cancel-on-move trigger count

        # cancel-on-move: ring buffer of (timestamp, mid_price)
        self._mid_history:    Deque[Tuple[float, float]] = deque(maxlen=40)
        self._cooldown_until: float = 0.0

    # ── cancel-on-move guard ───────────────────────────────

    def record_mid(self, mid: float):
        self._mid_history.append((time.time(), mid))

    def in_cooldown(self) -> bool:
        return time.time() < self._cooldown_until

    def check_and_fire_guard(self) -> bool:
        """
        Returns True and cancels quotes if mid has moved >= CANCEL_MOVE_BPS
        within the last CANCEL_MOVE_S seconds (fast directional move = adverse fill risk).
        """
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

        # Fast move detected — pull quotes before we get adversely filled
        direction = "UP" if newest_p > oldest_p else "DOWN"
        log.info(
            f"[{self.symbol}] GUARD  {direction} {move_bps:.1f} bps in {CANCEL_MOVE_S}s "
            f"— cancelling quotes (cooldown {COOLDOWN_S}s)"
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
        return round(qty, 6)

    def _compute_prices(self, mid: float) -> Tuple[float, float]:
        """Bid and ask prices with inventory-based skew."""
        half = SPREAD_BPS / 10_000

        inv_usd = self.net_pos * mid
        if abs(inv_usd) > MAX_INV_USD * 0.4:
            skew = min(abs(inv_usd) / MAX_INV_USD, 1.0) * half * (SKEW_FACTOR - 1)
            if inv_usd > 0:       # too long → widen ask, tighten bid
                half_bid = half * 0.6
                half_ask = half + skew
            else:                 # too short → widen bid, tighten ask
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

        # Hard inventory cap — pause quoting, don't widen further
        if abs(self.net_pos * mid) >= MAX_INV_USD * 1.8:
            log.warning(f"[{self.symbol}] inventory ${self.net_pos * mid:+.0f} at hard cap — skipping quote")
            return

        self._cancel_side("bid")
        self._cancel_side("ask")

        bid_p, ask_p = self._compute_prices(mid)
        qty          = self._order_qty(mid)
        ts_ms        = int(time.time() * 1000)

        # symbol short for CLI order ID (must be ≤ 36 chars, alphanumeric+_-)
        sym_short = self.symbol.replace("PF_", "").replace("USD", "").lower()

        for side, price, label in (
            ("buy",  bid_p, f"mmb{sym_short}{ts_ms}"),
            ("sell", ask_p, f"mms{sym_short}{ts_ms + 1}"),
        ):
            try:
                r      = self.api.send_order(self.symbol, side, qty, price, label)
                status = r.get("sendStatus", {})
                if status.get("status") == "placed":
                    oid = status.get("order_id", "")
                    q   = ActiveQuote(order_id=oid, side=side, price=price, qty=qty)
                    if side == "buy":
                        self.bid_quote = q
                    else:
                        self.ask_quote = q
                else:
                    log.warning(f"[{self.symbol}] {side} order not placed: {status}")
            except Exception as e:
                log.error(f"[{self.symbol}] {side} send_order failed: {e}")

        self._last_mid    = mid
        self._last_quote_t = time.time()
        log.info(
            f"[{self.symbol}] quoted  bid={bid_p}  ask={ask_p}  qty={qty}  "
            f"mid={mid:.{self.cfg['price_decimals']}f}  "
            f"inv={self.net_pos:+.3f} (${self.net_pos * mid:+.0f})"
        )

    def _cancel_side(self, side: str):
        q = self.bid_quote if side == "bid" else self.ask_quote
        if q:
            try:
                self.api.cancel_order(q.order_id)
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

        if fill.side == "buy":
            if self.net_pos >= 0:
                total_cost    = self.net_pos * self.avg_entry + qty * p
                self.net_pos += qty
                self.avg_entry = total_cost / self.net_pos if self.net_pos else p
            else:
                covered        = min(qty, abs(self.net_pos))
                self.realized_pnl += (self.avg_entry - p) * covered
                self.net_pos  += qty
                if self.net_pos > 0:
                    self.avg_entry = p
            self.bid_quote = None

        else:  # sell
            if self.net_pos <= 0:
                total_cost    = abs(self.net_pos) * self.avg_entry + qty * p
                self.net_pos -= qty
                self.avg_entry = total_cost / abs(self.net_pos) if self.net_pos else p
            else:
                closed         = min(qty, self.net_pos)
                self.realized_pnl += (p - self.avg_entry) * closed
                self.net_pos  -= qty
                if self.net_pos < 0:
                    self.avg_entry = p
            self.ask_quote = None

        self.realized_pnl -= fill.fee
        rebate             = fill.qty * fill.price * 0.0002
        self.maker_rebate += rebate
        self.volume_usd   += fill.qty * fill.price
        self.fill_count   += 1

        log.info(
            f"[{self.symbol}] FILL {fill.side.upper():4s}  {qty}@{p}  "
            f"fee=${fill.fee:.4f}  rebate≈${rebate:.4f}  "
            f"net_pos={self.net_pos:+.4f}  rpnl=${self.realized_pnl:.2f}"
        )

        # Immediately requote the side that was just consumed
        self.requote()

    # ── reconcile ──────────────────────────────────────────

    def reconcile(self, exchange_orders: list):
        """Drop our quote references if the exchange no longer has them."""
        live_ids = {o.get("order_id") for o in exchange_orders}
        if self.bid_quote and self.bid_quote.order_id not in live_ids:
            log.debug(f"[{self.symbol}] bid_quote {self.bid_quote.order_id} gone from exchange — clearing")
            self.bid_quote = None
        if self.ask_quote and self.ask_quote.order_id not in live_ids:
            log.debug(f"[{self.symbol}] ask_quote {self.ask_quote.order_id} gone from exchange — clearing")
            self.ask_quote = None

    # ── stats ──────────────────────────────────────────────

    def summary(self) -> str:
        mid  = self.book.mid or 0
        upnl = (mid - self.avg_entry) * self.net_pos if mid and self.avg_entry else 0
        return (
            f"[{self.symbol}]  fills={self.fill_count:5d}  "
            f"vol=${self.volume_usd:>12,.0f}  "
            f"pos={self.net_pos:+.4f}  "
            f"rpnl=${self.realized_pnl:+.2f}  "
            f"upnl=${upnl:+.2f}  "
            f"rebate≈${self.maker_rebate:.2f}  "
            f"guards={self.guard_cancels}"
        )


# ──────────────────────────────────────────────────────────────
# VOLUME / REBATE TRACKER
# ──────────────────────────────────────────────────────────────

class VolumeTracker:
    def __init__(self):
        self._start     = time.time()
        self.session    = 0.0
        self.daily      = 0.0
        self.monthly    = 0.0
        self._day       = datetime.now(timezone.utc).date()
        self._month     = datetime.now(timezone.utc).month

    def add(self, usd: float):
        now = datetime.now(timezone.utc)
        if now.date() != self._day:
            log.info(f"Day rollover — daily volume was ${self.daily:,.0f}")
            self.daily = 0.0
            self._day  = now.date()
        if now.month != self._month:
            log.info(f"Month rollover — monthly volume was ${self.monthly:,.0f}")
            self.monthly = 0.0
            self._month  = now.month
        self.session += usd
        self.daily   += usd
        self.monthly += usd

    def summary(self) -> str:
        elapsed_h  = max((time.time() - self._start) / 3_600, 0.001)
        run_rate   = self.session / elapsed_h * 24 * 365
        est_rebate = self.monthly * 0.0002  # -0.02% maker rebate at target tier
        return (
            f"Volume — session: ${self.session:>12,.0f}  "
            f"daily: ${self.daily:>12,.0f}  "
            f"monthly: ${self.monthly:>12,.0f}  "
            f"run-rate/yr: ${run_rate:>14,.0f}  "
            f"est. monthly rebate: ${est_rebate:,.2f}"
        )


# ──────────────────────────────────────────────────────────────
# DISCORD
# ──────────────────────────────────────────────────────────────

def _discord(text: str, color: int = 1940085):
    if not DISCORD_WEBHOOK:
        return
    try:
        requests.post(DISCORD_WEBHOOK, json={
            "embeds": [{
                "title": "📊 QVIX Market Maker",
                "description": text,
                "color": color,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        }, timeout=5)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ──────────────────────────────────────────────────────────────

class MarketMakingBot:

    def __init__(self):
        self.api      = KrakenFuturesAPI(API_KEY, API_SECRET)
        self.makers   = {sym: InstrumentMM(sym, cfg, self.api) for sym, cfg in INSTRUMENTS.items()}
        self.tracker  = VolumeTracker()
        self._running = True

        self._last_fill_time: Optional[str] = None
        self._seen_fills:     set            = set()
        self._last_stats_t    = time.time()
        self._last_discord_t  = time.time()
        self._last_reconcile  = time.time()

    # ── risk ───────────────────────────────────────────────

    def _session_pnl(self) -> float:
        return sum(m.realized_pnl for m in self.makers.values())

    def _check_risk(self) -> bool:
        pnl = self._session_pnl()
        if pnl < -MAX_DRAWDOWN_USD:
            log.critical(f"EMERGENCY STOP — drawdown ${pnl:.2f} exceeds limit ${MAX_DRAWDOWN_USD}")
            _discord(f"🚨 **EMERGENCY STOP** — drawdown ${pnl:.2f}", color=15158332)
            return False
        return True

    # ── fills ──────────────────────────────────────────────

    def _poll_fills(self):
        try:
            data  = self.api.get_fills(self._last_fill_time)
            fills = data.get("fills", [])
        except Exception as e:
            log.warning(f"fills poll error: {e}")
            return

        for f in fills:
            fid = f.get("fill_id") or f.get("order_id", "") + str(f.get("fillTime", ""))
            if fid in self._seen_fills:
                continue
            self._seen_fills.add(fid)

            sym  = f.get("symbol", "")
            side = f.get("side", "")
            qty  = float(f.get("size", 0))
            px   = float(f.get("price", 0))
            fee  = float(f.get("fee_paid", 0))
            ts   = f.get("fillTime", "")

            if ts:
                self._last_fill_time = ts

            mm = self.makers.get(sym)
            if mm and qty > 0:
                fill = Fill(
                    order_id=f.get("order_id", ""),
                    symbol=sym, side=side,
                    price=px, qty=qty, fee=fee, ts=ts,
                )
                mm.on_fill(fill)
                self.tracker.add(qty * px)

    # ── reconcile ──────────────────────────────────────────

    def _reconcile(self):
        try:
            data   = self.api.get_open_orders()
            orders = data.get("openOrders", [])
        except Exception as e:
            log.warning(f"reconcile error: {e}")
            return
        for mm in self.makers.values():
            sym_orders = [o for o in orders if o.get("symbol") == mm.symbol]
            mm.reconcile(sym_orders)

    # ── quote loop ─────────────────────────────────────────

    async def _run_quotes(self):
        while self._running:
            if not self._check_risk():
                self._running = False
                break

            # Reconcile open orders periodically
            if time.time() - self._last_reconcile > RECONCILE_S:
                self._reconcile()
                self._last_reconcile = time.time()

            for mm in self.makers.values():
                try:
                    mm.requote()
                except Exception as e:
                    log.error(f"[{mm.symbol}] requote error: {e}")

            await asyncio.sleep(QUOTE_REFRESH_S)

    # ── fill polling loop ───────────────────────────────────

    async def _run_fills(self):
        while self._running:
            self._poll_fills()
            await asyncio.sleep(FILLS_POLL_S)

    # ── stats + discord loop ────────────────────────────────

    async def _run_reporting(self):
        while self._running:
            now = time.time()

            if now - self._last_stats_t >= STATS_LOG_S:
                log.info(self.tracker.summary())
                for mm in self.makers.values():
                    log.info(mm.summary())
                log.info(f"Session PnL: ${self._session_pnl():+.2f}")
                self._last_stats_t = now

            if now - self._last_discord_t >= DISCORD_REPORT_S:
                lines = ["**Hourly Report**", "", self.tracker.summary(), ""]
                for mm in self.makers.values():
                    lines.append(mm.summary())
                lines.append(f"\nSession PnL: **${self._session_pnl():+.2f}**")
                _discord("\n".join(lines))
                self._last_discord_t = now

            await asyncio.sleep(5)

    # ── cancel-on-move guard loop ───────────────────────────

    async def _run_fast_guard(self):
        """
        Runs every GUARD_POLL_S (0.5s) — much faster than the quote loop.
        Cancels resting quotes the moment price moves too fast in one direction,
        cutting adverse selection before a fill can happen.
        """
        while self._running:
            for mm in self.makers.values():
                if mm.bid_quote is None and mm.ask_quote is None:
                    continue
                try:
                    mm.check_and_fire_guard()
                except Exception as e:
                    log.debug(f"[{mm.symbol}] guard check error: {e}")
            await asyncio.sleep(GUARD_POLL_S)

    # ── orderbook WebSocket ─────────────────────────────────

    async def _run_ws(self):
        symbols = list(self.makers.keys())
        sub_msg = json.dumps({"event": "subscribe", "feed": "book", "product_ids": symbols})

        while self._running:
            try:
                async with websockets.connect(FUTURES_WS, ping_interval=20, open_timeout=15) as ws:
                    await ws.send(sub_msg)
                    log.info(f"WS connected — subscribed to {symbols}")

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg  = json.loads(raw)
                            feed = msg.get("feed", "")
                            sym  = msg.get("product_id", "")
                            mm   = self.makers.get(sym)
                            if not mm:
                                continue

                            if feed == "book_snapshot":
                                bids = [(e["price"], e["qty"]) for e in msg.get("bids", [])]
                                asks = [(e["price"], e["qty"]) for e in msg.get("asks", [])]
                                mm.book.apply_snapshot(bids, asks)
                                mid = mm.book.mid
                                if mid:
                                    mm.record_mid(mid)

                            elif feed == "book":
                                mm.book.apply_delta(
                                    msg.get("side", ""),
                                    float(msg.get("price", 0)),
                                    float(msg.get("qty",   0)),
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
            f"⛔ MM stopped\n{self.tracker.summary()}\nFinal PnL: **${self._session_pnl():+.2f}**",
            color=15158332,
        )

    # ── entry ──────────────────────────────────────────────

    async def run(self):
        log.info("=" * 60)
        log.info("QVIX Kraken Futures Market Maker starting")
        log.info(f"Instruments : {list(INSTRUMENTS.keys())}")
        log.info(f"Spread      : ±{SPREAD_BPS} bps ({SPREAD_BPS * 2} bps round-trip)")
        log.info(f"Max inv/sym : ${MAX_INV_USD:,}")
        log.info(f"Stop loss   : ${MAX_DRAWDOWN_USD:,}")
        log.info(f"Guard       : cancel if mid moves >{CANCEL_MOVE_BPS} bps in {CANCEL_MOVE_S}s → {COOLDOWN_S}s cooldown")
        log.info("=" * 60)

        if not API_KEY or not API_SECRET:
            log.error("KRAKEN_FUTURES_API_KEY / KRAKEN_FUTURES_API_SECRET not set in .env")
            sys.exit(1)

        # Account health check
        try:
            acc = self.api.get_accounts()
            accs = acc.get("accounts", {})
            log.info(f"Account OK — {len(accs)} sub-account(s) found")
        except Exception as e:
            log.error(f"Account check failed — check API keys: {e}")
            sys.exit(1)

        _discord(
            f"✅ **MM started**\n"
            f"Instruments: {', '.join(INSTRUMENTS.keys())}\n"
            f"Spread: ±{SPREAD_BPS} bps  |  Stop: -${MAX_DRAWDOWN_USD:,}"
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
    bot = MarketMakingBot()

    def _on_signal(sig, _frame):
        log.info(f"Signal {sig} — shutting down")
        bot._running = False

    signal.signal(signal.SIGINT,  _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    asyncio.run(bot.run())
