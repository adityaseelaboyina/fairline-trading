"""LIVE broker — REAL MONEY order placement on Kalshi (V2).

This is no longer a stub, but it is GATED HARD:
  * only constructed when paper_mode is false AND the run is explicitly armed,
  * two things are UNVERIFIED until a human runs the test order (see verify_order.py):
      1. POST signing  — Kalshi's documented scheme signs timestamp+METHOD+path
         (body not signed); if the V2 order endpoint needs body-signing it will
         401, and we switch. Do NOT trust it with money until the test order 200s.
      2. NO-side mapping — V2 orders use side bid/ask on the market. Buying/selling
         the NO leg's exact price/side convention MUST be confirmed by the test
         order before real trading. The dry run prints the exact body to inspect.

Sizing is bankroll-aware: each clip is a small fraction of the REAL account
balance, so a $20 account places ~$1 bets, not $10 ones.
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from typing import Optional

from data.orderbook import OrderBook


def size_for_balance(balance_cents: int, price_cents: int, fraction: float,
                     min_contracts: float = 1.0) -> float:
    """Contracts to buy so the clip is ~`fraction` of the account at `price`.
    e.g. $20 balance, 5% fraction, 25c price -> $1.00 / 0.25 = 4 contracts."""
    if price_cents <= 0:
        return 0.0
    budget_cents = balance_cents * fraction
    contracts = budget_cents / price_cents
    return max(min_contracts, math.floor(contracts * 100) / 100)  # 0.01 granularity


def to_yes_order(side: str, action: str, price_cents: int) -> tuple[str, int]:
    """V2 orders are YES-perspective only: bid=buy YES, ask=sell YES. A NO trade
    is the economic inverse on the YES leg. `price_cents` is the touch price in
    THIS side's own terms. Returns (api_side, yes_price_cents).

      (yes, buy)  -> bid @ yes_price
      (yes, sell) -> ask @ yes_price
      (no,  buy)  -> ask @ (100 - no_price)   # buying NO == selling YES
      (no,  sell) -> bid @ (100 - no_price)   # selling NO == buying YES
    """
    if side == "yes":
        return ("bid" if action == "buy" else "ask", price_cents)
    return ("ask" if action == "buy" else "bid", 100 - price_cents)


def build_v2_order(ticker: str, side: str, action: str, price_cents: int,
                   count: float) -> dict:
    """Build the V2 order body (YES-perspective). Returns the exact JSON we would
    POST, so the dry run can print it for inspection."""
    api_side, yes_price = to_yes_order(side, action, price_cents)
    return {
        "ticker": ticker,
        "side": api_side,
        "count": f"{count:.2f}",
        "price": f"{yes_price / 100.0:.4f}",
        "time_in_force": "immediate_or_cancel",   # taker at the touch; never rests
        "self_trade_prevention_type": "taker_at_cross",
        "client_order_id": uuid.uuid4().hex,
    }


@dataclass
class LiveFill:
    ticker: str
    side: str
    action: str
    requested_size: float
    filled_size: float
    decision_price_cents: Optional[int]
    sim_fill_price_cents: Optional[float]
    fee_cents: int
    levels: list
    accepted: bool
    reason: str
    ts_monotonic: float
    ts_wall: float


class LiveBroker:
    """Places REAL Kalshi orders. Constructed only when armed."""

    def __init__(self, cfg: dict, rest, run_dir: str, balance_cents: int):
        if cfg.get("paper_mode", True):
            raise NotImplementedError(
                "Refusing to construct LiveBroker while paper_mode is true.")
        live = cfg.get("live", {})
        if not live.get("armed", False):
            raise NotImplementedError(
                "LiveBroker requires live.armed=true (and the --arm flag). Refusing.")
        self.rest = rest
        self.cfg = cfg
        self.fraction = float(live.get("bankroll_fraction", 0.05))
        self.max_clip_dollars = float(live.get("max_clip_dollars", 2.0))
        self.start_balance_cents = balance_cents
        self.balance_cents = balance_cents

    def sized_count(self, price_cents: int) -> float:
        c = size_for_balance(self.balance_cents, price_cents, self.fraction)
        # never exceed max_clip_dollars
        if c * price_cents > self.max_clip_dollars * 100:
            c = math.floor((self.max_clip_dollars * 100 / price_cents) * 100) / 100
        return max(0.0, c)

    async def place(self, book: OrderBook, side: str, action: str,
                    price_cents: int, count: float) -> dict:
        """Submit a real order. side='yes'|'no', action='buy'|'sell'. Converts to
        the YES-perspective bid/ask + price. Raises on API error (caller logs)."""
        api_side, yes_price = to_yes_order(side, action, price_cents)
        return await self.rest.place_order_v2(
            ticker=book.market_ticker, side=api_side, count=count,
            price_dollars=yes_price / 100.0)
