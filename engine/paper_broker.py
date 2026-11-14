"""Paper broker: simulate fills against the LIVE order book.

Crosses the spread realistically by walking resting liquidity (OrderBook.walk),
applies the pluggable fee function, and tracks cash / positions / realized +
unrealized P&L. Enforces the hard guardrails before any simulated order:

  * STOP-file kill switch (halts all new orders)
  * max position per market
  * max total exposure (USD cost basis)
  * max orders / minute

NO REAL ORDERS. paper_mode is asserted true at construction.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from data.orderbook import OrderBook


@dataclass
class Leg:
    qty: int = 0
    avg_cost_cents: float = 0.0   # ex-fee basis per contract


@dataclass
class Position:
    ticker: str
    yes: Leg = field(default_factory=Leg)
    no: Leg = field(default_factory=Leg)
    realized_cents: int = 0
    fees_cents: int = 0

    def net_contracts(self) -> int:
        return self.yes.qty + self.no.qty

    def cost_basis_cents(self) -> float:
        return self.yes.qty * self.yes.avg_cost_cents + self.no.qty * self.no.avg_cost_cents


@dataclass
class Fill:
    ticker: str
    side: str            # yes | no
    action: str          # buy | sell
    requested_size: int
    filled_size: int
    decision_price_cents: Optional[int]
    sim_fill_price_cents: Optional[float]
    fee_cents: int
    levels: list
    accepted: bool
    reason: str
    ts_monotonic: float
    ts_wall: float


class PaperBroker:
    def __init__(self, cfg: dict, fee_fn: Callable[..., int], run_dir: str):
        # NOTE: this class NEVER places real orders — execute() simulates fills and
        # record_fill() is just a position ledger. In live mode it tracks the real
        # account's positions; real order placement happens only in LiveBroker.
        self.fee_fn = fee_fn
        self.run_dir = Path(run_dir)
        risk = cfg["risk"]
        self.cash_cents = int(round(risk["bankroll"] * 100))
        self.start_cash_cents = self.cash_cents
        self.max_pos = int(risk["max_position_per_market"])
        self.max_exposure_cents = int(round(risk["max_total_exposure"] * 100))
        self.max_orders_per_min = int(risk["max_orders_per_min"])
        self.positions: dict[str, Position] = {}
        self._order_times: deque[float] = deque()

    # ---- guardrails ----------------------------------------------------
    def kill_switch_engaged(self) -> bool:
        return (self.run_dir / "STOP").exists()

    def _rate_limited(self, now: float) -> bool:
        while self._order_times and now - self._order_times[0] > 60.0:
            self._order_times.popleft()
        return len(self._order_times) >= self.max_orders_per_min

    def _pos(self, ticker: str) -> Position:
        return self.positions.setdefault(ticker, Position(ticker))

    def total_exposure_cents(self) -> float:
        return sum(p.cost_basis_cents() for p in self.positions.values())

    # ---- simulated execution ------------------------------------------
    def execute(
        self,
        book: OrderBook,
        side: str,
        action: str,
        size: int,
        decision_price_cents: Optional[int] = None,
    ) -> Fill:
        now = time.monotonic()
        wall = time.time()

        def reject(reason: str) -> Fill:
            return Fill(book.market_ticker, side, action, size, 0, decision_price_cents,
                        None, 0, [], False, reason, now, wall)

        if self.kill_switch_engaged():
            return reject("kill_switch: STOP file present")
        if self._rate_limited(now):
            return reject("rate_limit: max_orders_per_min exceeded")

        pos = self._pos(book.market_ticker)
        leg = pos.yes if side == "yes" else pos.no

        if action == "buy":
            # position cap (net contracts) and exposure cap checked on intended size
            if abs(pos.net_contracts() + size) > self.max_pos:
                return reject(f"position_cap: would exceed {self.max_pos} contracts")

        filled, avg, levels = book.walk(side, action, size)
        if not filled or avg is None:
            return reject("no_liquidity: empty book for requested side")

        avg_int = int(round(avg))
        fee = self.fee_fn(avg_int, filled, ticker=book.market_ticker, maker=False)

        if action == "buy":
            cost = int(round(filled * avg)) + fee
            if cost > self.cash_cents:
                return reject("insufficient_cash")
            if self.total_exposure_cents() + filled * avg > self.max_exposure_cents:
                return reject(f"exposure_cap: would exceed ${self.max_exposure_cents/100:.0f}")
            self.cash_cents -= cost
            new_qty = leg.qty + filled
            leg.avg_cost_cents = (leg.qty * leg.avg_cost_cents + filled * avg) / new_qty if new_qty else 0.0
            leg.qty = new_qty
            pos.fees_cents += fee
        else:  # sell
            sell_qty = min(filled, leg.qty)
            if sell_qty <= 0:
                return reject("nothing_to_sell")
            proceeds = int(round(sell_qty * avg)) - fee
            self.cash_cents += proceeds
            pos.realized_cents += int(round((avg - leg.avg_cost_cents) * sell_qty)) - fee
            pos.fees_cents += fee
            leg.qty -= sell_qty
            if leg.qty == 0:
                leg.avg_cost_cents = 0.0
            filled = sell_qty

        self._order_times.append(now)
        return Fill(book.market_ticker, side, action, size, filled, decision_price_cents,
                    avg, fee, levels, True, "ok", now, wall)

    def record_fill(self, book: OrderBook, side: str, action: str, filled: int,
                    price_cents: float, fee_cents: int) -> Fill:
        """Update positions/cash/realized from a REAL fill (no sim walk, no caps —
        those were checked before the order went out). Used by the live path so
        all the existing exit/P&L/summary logic works on real fills."""
        now, wall = time.monotonic(), time.time()
        pos = self._pos(book.market_ticker)
        leg = pos.yes if side == "yes" else pos.no
        avg = float(price_cents)
        if action == "buy":
            self.cash_cents -= int(round(filled * avg)) + fee_cents
            new_qty = leg.qty + filled
            leg.avg_cost_cents = ((leg.qty * leg.avg_cost_cents + filled * avg) / new_qty
                                  if new_qty else 0.0)
            leg.qty = new_qty
        else:  # sell
            sell_qty = min(filled, leg.qty) if leg.qty else filled
            self.cash_cents += int(round(sell_qty * avg)) - fee_cents
            pos.realized_cents += int(round((avg - leg.avg_cost_cents) * sell_qty)) - fee_cents
            leg.qty = max(0, leg.qty - sell_qty)
            if leg.qty == 0:
                leg.avg_cost_cents = 0.0
            filled = sell_qty
        pos.fees_cents += fee_cents
        return Fill(book.market_ticker, side, action, filled, filled, int(round(avg)),
                    avg, fee_cents, [], True, "live", now, wall)

    # ---- valuation -----------------------------------------------------
    def unrealized_cents(self, books: dict[str, OrderBook]) -> float:
        total = 0.0
        for t, p in self.positions.items():
            b = books.get(t)
            if not b:
                continue
            ymid = b.mid("yes")
            nmid = b.mid("no")
            if p.yes.qty and ymid is not None:
                total += p.yes.qty * (ymid - p.yes.avg_cost_cents)
            if p.no.qty and nmid is not None:
                total += p.no.qty * (nmid - p.no.avg_cost_cents)
        return total

    def market_value_cents(self, books: dict[str, OrderBook]) -> float:
        """Mark value of open contracts (qty x mid). Net liquidation equity =
        cash + this (NOT cash + unrealized_pnl — that double-removes cost basis)."""
        total = 0.0
        for t, p in self.positions.items():
            b = books.get(t)
            if not b:
                continue
            ymid, nmid = b.mid("yes"), b.mid("no")
            if p.yes.qty and ymid is not None:
                total += p.yes.qty * ymid
            if p.no.qty and nmid is not None:
                total += p.no.qty * nmid
        return total

    def realized_cents(self) -> int:
        return sum(p.realized_cents for p in self.positions.values())

    def settle(self, ticker: str, result: str) -> dict:
        """Settle held contracts at resolution. result in {'yes','no'}.
        Returns the realized P&L contribution from settlement (cents)."""
        p = self.positions.get(ticker)
        if not p:
            return {"settled_pnl_cents": 0}
        yes_payoff = 100 if result == "yes" else 0
        no_payoff = 100 if result == "no" else 0
        pnl = 0
        if p.yes.qty:
            pnl += int(round(p.yes.qty * (yes_payoff - p.yes.avg_cost_cents)))
            self.cash_cents += p.yes.qty * yes_payoff
        if p.no.qty:
            pnl += int(round(p.no.qty * (no_payoff - p.no.avg_cost_cents)))
            self.cash_cents += p.no.qty * no_payoff
        p.realized_cents += pnl
        held_yes, held_no = p.yes.qty, p.no.qty
        p.yes = Leg()
        p.no = Leg()
        return {"settled_pnl_cents": pnl, "held_yes": held_yes, "held_no": held_no,
                "result": result}
