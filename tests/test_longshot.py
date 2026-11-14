"""Deterministic tests for the longshot lane. No network, no third-party deps.

Run: python tests/test_longshot.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.fees import make_fee_fn
from engine.longshot import (apply_exposure_caps, gate_report, in_entry_window,
                             scan_market, settle_pnl_cents)

FEE = make_fee_fn({"fees": {"rate_taker": 0.07}})
CFG = {"clip_dollars": 3.0, "max_yes_mid_cents": 10, "max_spread_cents": 5,
       "max_no_ask_cents": 97, "min_net_win_cents": 6,
       "min_hours_to_close": 12, "max_hours_to_close": 36,
       "max_per_event": 2, "max_new_per_scan": 6, "max_open_cost_dollars": 60.0}


class FakeBook:
    """Minimal OrderBook stand-in: fixed touch + infinite depth at the touch."""
    def __init__(self, yes_bid, yes_ask, depth=10_000):
        self.best_yes_bid, self.best_yes_ask = yes_bid, yes_ask
        self.best_no_ask = (100 - yes_bid) if yes_bid is not None else None
        self.depth = depth

    def walk(self, side, action, size):
        px = self.best_no_ask if side == "no" else self.best_yes_ask
        n = min(size, self.depth)
        return n, float(px), ([(px, n)] if n else [])


def scan(book, hours=20.0, cfg=CFG):
    return scan_market(ticker="KXHIGHNY-26JUL15-B95.5",
                       event_ticker="KXHIGHNY-26JUL15", book=book,
                       hours_to_close=hours, fee_fn=FEE, cfg=cfg)


def test_entry_window():
    assert in_entry_window(20, 12, 36)
    assert not in_entry_window(5, 12, 36), "post-outcome entry must be blocked"
    assert not in_entry_window(48, 12, 36)
    assert scan(FakeBook(4, 6), hours=5.0) is None


def test_no_bid_no_trade():
    # yes_bid=0 -> NO ask is 100c -> selling the longshot is impossible.
    assert scan(FakeBook(0, 1)) is None
    assert scan(FakeBook(None, None)) is None


def test_mid_cap_blocks_the_may27_case():
    # yes 12/13 (mid 12.5) — the bucket that HIT on 2026-05-27. Must be skipped.
    assert scan(FakeBook(12, 13)) is None
    # yes 8/9 (mid 8.5) passes the cap.
    c = scan(FakeBook(8, 9))
    assert c is not None and c.no_ask == 92 and c.side == "no"


def test_spread_cap():
    assert scan(FakeBook(2, 9)) is None, "7c spread is not a price"


def test_fee_floor():
    # yes_bid 2 -> NO at 98c: blocked by max_no_ask (97) — cushion too thin.
    assert scan(FakeBook(2, 3)) is None
    # yes_bid 4 -> NO at 96c, 3 contracts: gross 12c, fee ceil(0.07*3*.96*.04*100)=1c
    c = scan(FakeBook(4, 5))
    assert c is not None
    assert c.contracts == 3 and c.fee_cents == 1 and c.net_win_cents == 11
    assert c.net_loss_cents == 96 * 3 + 1


def test_insufficient_depth_is_skipped():
    assert scan(FakeBook(5, 6, depth=1)) is None, "3-contract clip, 1 available"


def test_settle_pnl():
    # NO position, YES misses -> win.
    assert settle_pnl_cents("no", "no", 94, 3, 1) == (100 - 94) * 3 - 1
    # NO position, YES hits -> lose entry + fee.
    assert settle_pnl_cents("no", "yes", 94, 3, 1) == -(94 * 3 + 1)


def _cand(ticker, event, no_ask, contracts=3, fee=1):
    from engine.longshot import LongshotCandidate
    return LongshotCandidate(ticker=ticker, event_ticker=event, series="KXHIGHNY",
                             side="no", yes_bid=100 - no_ask - 1, yes_ask=100 - no_ask + 1,
                             no_ask=no_ask, contracts=contracts, fee_cents=fee,
                             net_win_cents=(100 - no_ask) * contracts - fee,
                             net_loss_cents=no_ask * contracts + fee,
                             hours_to_close=20.0)


def test_exposure_caps():
    cands = [_cand(f"T{i}", f"EV{i % 3}", 90 + i % 5) for i in range(12)]
    picked = apply_exposure_caps(cands, 0, {}, CFG)
    assert len(picked) <= CFG["max_new_per_scan"]
    per_ev = {}
    for c in picked:
        per_ev[c.event_ticker] = per_ev.get(c.event_ticker, 0) + 1
    assert all(v <= CFG["max_per_event"] for v in per_ev.values())
    # budget: open cost 5900c + one ~271-280c clip stays under 6000c; a second won't fit
    picked2 = apply_exposure_caps(cands, 5700, {}, CFG)
    assert len(picked2) == 1
    # pre-existing per-event exposure is respected
    picked3 = apply_exposure_caps(cands, 0, {"EV0": 2}, CFG)
    assert all(c.event_ticker != "EV0" for c in picked3)


def test_gate_report():
    rows = []
    for i in range(160):
        rows.append({"settled": True, "pnl_cents": 15 if i % 10 else -280,
                     "event_ticker": f"EV{i}", "series": f"S{i % 3}",
                     "day": f"2026-07-{(i % 20) + 1:02d}"})
    rep = gate_report(rows, CFG | {"gates": {"min_settled": 150, "min_events": 40,
                                             "max_worst_day_frac_of_gross": 0.35,
                                             "min_positive_series": 3}})
    assert rep["P1_sample"]["pass"]
    assert rep["P2_net_positive"]["pass"] == (sum(r["pnl_cents"] for r in rows) > 0)
    # unsettled rows are excluded entirely
    rep2 = gate_report([{"settled": False}], CFG | {"gates": {}})
    assert not rep2["all_pass"]


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} tests passed")
