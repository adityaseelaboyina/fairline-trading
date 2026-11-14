"""Pluggable Kalshi fee schedule.

From kalshi.com/fee-schedule (read 2026-06-23, see docs/API_NOTES.md):

    taker fee (USD) = ceil(rate_taker * C * P * (1 - P))   rounded UP to the cent
    maker fee (USD) = ceil(rate_maker * C * P * (1 - P))

C = contracts, P = price in dollars (0..1). rate_taker defaults to 0.07,
rate_maker to 0.0175. Rates (incl. per-series overrides) come from config so a
changed schedule is a config edit, not a code change. Paper fills cross the
spread, so they are taker by default.

`fee_cents(...)` returns the fee in integer cents.
"""
from __future__ import annotations

import math
from typing import Callable


def make_fee_fn(cfg: dict) -> Callable[..., int]:
    fc = cfg.get("fees", {})
    base_taker = float(fc.get("rate_taker", 0.07))
    base_maker = float(fc.get("rate_maker", 0.0175))
    overrides = fc.get("series_overrides", {}) or {}

    def _series(ticker: str) -> str:
        # Kalshi tickers look like SERIES-...; series is the first segment.
        return ticker.split("-", 1)[0] if ticker else ""

    def fee_cents(price_cents: int, contracts: int, *, ticker: str = "", maker: bool = False) -> int:
        ov = overrides.get(_series(ticker), {})
        rate = float(ov.get("rate_maker", base_maker)) if maker else float(ov.get("rate_taker", base_taker))
        p = price_cents / 100.0
        fee_dollars = rate * contracts * p * (1.0 - p)
        return int(math.ceil(fee_dollars * 100.0 - 1e-9))  # round up to the cent

    return fee_cents
