"""Local order book reconstructed from Kalshi snapshot + deltas.

Kalshi returns resting BIDS only, on both the `yes` and `no` sides, prices in
cents (YES + NO = 100). A YES *offer* is a resting NO bid converted:
a NO bid at price q is someone willing to sell YES at (100 - q). So:

    best_yes_bid = max(yes prices)
    best_yes_ask = 100 - max(no prices)     # to BUY yes you lift NO bids
    best_no_bid  = max(no prices)
    best_no_ask  = 100 - max(yes prices)

The book also supports walking liquidity for a paper fill (see walk()).
All quantities/prices are normalized to integers (cents, contracts), tolerating
both the legacy int encoding and the newer `_dollars`/fixed-point string encoding.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


def _to_cents(price) -> int:
    """Accept 42, '42', 0.42, '0.4200' -> 42 cents."""
    if isinstance(price, str):
        price = float(price) if ("." in price) else int(price)
    if isinstance(price, float):
        # dollars (0..1) -> cents
        return int(round(price * 100))
    return int(price)


def _to_count(qty) -> int:
    """Accept 13, '13', '13.00' (fixed point) -> 13 contracts."""
    if isinstance(qty, str):
        qty = float(qty)
    return int(round(float(qty)))


@dataclass
class OrderBook:
    market_ticker: str
    # side -> {price_cents: contracts}.  Both sides hold resting bids.
    yes: dict = field(default_factory=dict)
    no: dict = field(default_factory=dict)
    seq: Optional[int] = None
    last_recv_monotonic: float = 0.0
    # Feed integrity: True while this book may be STALE after a WS seq
    # gap, until a full snapshot (WS or REST recovery) rebuilds it. Deltas do NOT
    # clear it — they're built on the suspect state. Trade paths must refuse a
    # dirty book: trading off a desynced book means chasing prices that aren't there.
    dirty: bool = False

    # ---- ingest --------------------------------------------------------
    def apply_snapshot(self, msg: dict, recv_monotonic: float) -> None:
        self.yes = {}
        self.no = {}
        self.dirty = False        # a full snapshot is authoritative -> clean
        # Field naming has drifted across API versions; the LIVE format (verified
        # 2026-06-23) is `yes_dollars_fp`/`no_dollars_fp` with [price_dollars,
        # size_fp] string pairs. Earlier `yes`/`no` (int cents) and `yes_dollars`
        # are also accepted.
        for raw_key in ("yes", "yes_dollars", "yes_dollars_fp"):
            for level in msg.get(raw_key, []) or []:
                p, q = level[0], level[1]
                self.yes[_to_cents(p)] = _to_count(q)
        for raw_key in ("no", "no_dollars", "no_dollars_fp"):
            for level in msg.get(raw_key, []) or []:
                p, q = level[0], level[1]
                self.no[_to_cents(p)] = _to_count(q)
        self.seq = msg.get("seq")
        self.last_recv_monotonic = recv_monotonic

    def apply_delta(self, msg: dict, recv_monotonic: float, seq: Optional[int] = None) -> bool:
        """Apply one delta level. Sequence-gap detection lives at the
        connection level in KalshiWS (Kalshi's `seq` is per-connection, not
        per-market), so this always applies and returns True."""
        side = self.yes if msg.get("side") == "yes" else self.no
        # LIVE delta (verified 2026-06-23): price_dollars + delta_fp. Legacy:
        # price (int cents) + delta (int contracts).
        price = _to_cents(msg.get("price", msg.get("price_dollars")))
        delta = _to_count(msg.get("delta", msg.get("delta_fp", msg.get("delta_dollars", 0))))
        side[price] = side.get(price, 0) + delta
        if side[price] <= 0:
            side.pop(price, None)
        if seq is not None:
            self.seq = seq
        self.last_recv_monotonic = recv_monotonic
        return True

    # ---- top of book ---------------------------------------------------
    @property
    def best_yes_bid(self) -> Optional[int]:
        return max(self.yes) if self.yes else None

    @property
    def best_no_bid(self) -> Optional[int]:
        return max(self.no) if self.no else None

    @property
    def best_yes_ask(self) -> Optional[int]:
        return 100 - max(self.no) if self.no else None

    @property
    def best_no_ask(self) -> Optional[int]:
        return 100 - max(self.yes) if self.yes else None

    def mid(self, side: str = "yes") -> Optional[float]:
        if side == "yes":
            b, a = self.best_yes_bid, self.best_yes_ask
        else:
            b, a = self.best_no_bid, self.best_no_ask
        if b is None or a is None:
            return float(b if b is not None else a) if (b or a) else None
        return (b + a) / 2.0

    # ---- liquidity walk for paper fills --------------------------------
    def walk(self, side: str, action: str, size: int):
        """Walk resting liquidity to fill `size` contracts of `side`
        ('yes'|'no') for `action` ('buy'|'sell'). Crosses the spread
        realistically.

        Returns (filled_qty, avg_price_cents, levels) where `levels` is a list
        of (price_cents, qty) consumed. avg_price_cents is None if nothing filled.
        """
        # Determine which resting bids we consume and at what effective price.
        # BUY yes  -> lift yes-asks  = NO bids, effective yes price = 100 - no_price
        # SELL yes -> hit yes-bids   = YES bids, effective yes price = yes_price
        # BUY no   -> lift no-asks    = YES bids, effective no price  = 100 - yes_price
        # SELL no  -> hit no-bids     = NO bids, effective no price   = no_price
        if (side, action) == ("yes", "buy"):
            resting, convert = self.no, lambda p: 100 - p
        elif (side, action) == ("yes", "sell"):
            resting, convert = self.yes, lambda p: p
        elif (side, action) == ("no", "buy"):
            resting, convert = self.yes, lambda p: 100 - p
        elif (side, action) == ("no", "sell"):
            resting, convert = self.no, lambda p: p
        else:
            raise ValueError(f"bad side/action {side}/{action}")

        # Best price first: for buys we want the lowest effective price, which
        # corresponds to the highest resting bid; for sells we want the highest
        # effective price -> highest resting bid. Either way: resting bids high->low.
        levels_sorted = sorted(resting.items(), key=lambda kv: kv[0], reverse=True)
        remaining = size
        consumed = []
        cost = 0
        for resting_price, avail in levels_sorted:
            if remaining <= 0:
                break
            take = min(avail, remaining)
            eff = convert(resting_price)
            consumed.append((eff, take))
            cost += eff * take
            remaining -= take
        filled = size - remaining
        avg = (cost / filled) if filled else None
        return filled, avg, consumed

    def sellable_price(self, side: str) -> Optional[int]:
        """Best price you can SELL the held `side` at right now (hit the bid).
        Selling yes hits yes bids; selling no hits no bids."""
        return self.best_yes_bid if side == "yes" else self.best_no_bid

    def fillable_size(self, side: str, action: str, price_limit: int) -> int:
        """Total resting contracts fillable for (side, action) at an effective
        price no worse than `price_limit` (<= for buys, >= for sells). Used by
        the liquidity guard to size up a candidate before surfacing it."""
        if (side, action) == ("yes", "buy"):
            resting, convert = self.no, lambda p: 100 - p
        elif (side, action) == ("yes", "sell"):
            resting, convert = self.yes, lambda p: p
        elif (side, action) == ("no", "buy"):
            resting, convert = self.yes, lambda p: 100 - p
        elif (side, action) == ("no", "sell"):
            resting, convert = self.no, lambda p: p
        else:
            raise ValueError(f"bad side/action {side}/{action}")
        total = 0
        for resting_price, avail in resting.items():
            eff = convert(resting_price)
            if action == "buy" and eff <= price_limit:
                total += avail
            elif action == "sell" and eff >= price_limit:
                total += avail
        return total
