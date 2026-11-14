"""Regression tests for bugs found while auditing execution logs against the
exchange's own records. Each test reproduces the original failure condition.

    python tests/test_audit.py
"""
import asyncio
import os
import sys
import tempfile
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

import data.kalshi_auth as kalshi_auth
import data.kalshi_rest as kalshi_rest
import ledger
from data.kalshi_rest import KalshiREST
from engine.fees import make_fee_fn
from engine.paper_broker import PaperBroker
from engine.rules import RulesEngine
from main import Harness
from tradelog.recorder import Recorder


def _fill(side, count, yes_price):
    return {"side": side, "count_fp": f"{count:.2f}",
            "yes_price_dollars": f"{yes_price:.4f}",
            "no_price_dollars": f"{1 - yes_price:.4f}", "fee_cost": "0"}


def _run_live_resolve(fills, results, keys):
    """Run ledger.live_resolve against a fake exchange (no network, no secrets)."""
    class FakeRest:
        def __init__(self, cfg, signer): pass
        async def get_balance_cents(self): return 5000
        async def get_positions(self): return []            # everything is flat
        async def get_fills(self, t): return fills.get(t, [])
        async def get_settlement(self, t):
            return {"settled": True, "result": results[t]}
        async def aclose(self): pass

    real_rest, real_signer = kalshi_rest.KalshiREST, kalshi_auth.KalshiSigner
    cwd = os.getcwd()
    tmp = tempfile.mkdtemp()
    open(os.path.join(tmp, "config.yaml"), "w").write("{}\n")
    try:
        kalshi_rest.KalshiREST = FakeRest
        kalshi_auth.KalshiSigner = NS(from_config=lambda cfg: None)
        os.chdir(tmp)
        return asyncio.run(ledger.live_resolve(keys))
    finally:
        kalshi_rest.KalshiREST, kalshi_auth.KalshiSigner = real_rest, real_signer
        os.chdir(cwd)


def test_exit_not_double_counted_with_settlement():
    """Bug: a position sold early was ALSO credited its full settlement payout."""
    partial, full, unknown = "KXT-PARTIAL-1", "KXT-FULL-1", "KXT-NOHIST-1"
    fills = {
        # bought 10 YES @40c, sold 6 @70c, held 4 into a YES settlement
        partial: [_fill("yes", 10, 0.40), _fill("no", 6, 0.70)],
        # bought 5 YES, sold all 5 @90c before a YES settlement
        full: [_fill("yes", 5, 0.40), _fill("no", 5, 0.90)],
    }
    results = {partial: "yes", full: "yes", unknown: "yes"}
    keys = [(partial, "yes"), (full, "yes"), (unknown, "yes")]
    _bal, _settled, resolved = _run_live_resolve(fills, results, keys)
    out_partial, _ = resolved[(partial, "yes")]
    assert out_partial == 6 * 70 + 4 * 100, out_partial        # not 10 * 100 + 420
    out_full, label = resolved[(full, "yes")]
    assert out_full == 5 * 90 and label == "sold", (out_full, label)   # no payout added
    print("ok: sold contracts are never also paid out at settlement")


def test_missing_history_is_not_booked_as_a_loss():
    """Bug: when the exchange returned no fill history, the ledger graded a $0 exit."""
    t = "KXT-NOHIST-1"
    _bal, _settled, resolved = _run_live_resolve({}, {t: "yes"}, [(t, "yes")])
    assert (t, "yes") not in resolved, resolved   # keeps the bot's own DB record
    print("ok: no exchange history leaves the logged result untouched")


def test_archive_fallbacks():
    """Bug: records past the exchange's archive cutoff vanished from the live
    endpoints, so fills came back empty and settled markets returned 404."""
    rest = KalshiREST.__new__(KalshiREST)
    calls = []

    async def fake_get(path, params=None):
        calls.append(path)
        if path == "/portfolio/fills":
            return {"fills": []}
        if path == "/historical/fills":
            return {"fills": [_fill("yes", 3, 0.5)]}
        if path.startswith("/historical/markets/"):
            return {"market": {"status": "finalized", "result": "no"}}
        raise AssertionError(path)

    async def gone(ticker):
        req = httpx.Request("GET", "https://example.test/markets/x")
        raise httpx.HTTPStatusError("404", request=req,
                                    response=httpx.Response(404, request=req))
    rest._get = fake_get
    rest.get_market = gone
    fills = asyncio.run(rest.get_fills("KXT-OLD-1"))
    assert len(fills) == 1 and "/historical/fills" in calls, calls
    s = asyncio.run(rest.get_settlement("KXT-OLD-1"))
    assert s["settled"] and s["result"] == "no", s
    print("ok: fills and settlements fall back to the exchange archive")


def test_settlements_deduplicated():
    """Bug: reconnects/restarts re-logged the same settlement, double-counting it."""
    rec = Recorder(tempfile.mkdtemp())
    for pnl in (100, 100, 100):
        rec.log_settlement(ticker="KXT-A", result="yes", settled_pnl_cents=pnl,
                           realized_pnl_cents=pnl, hold_to_resolution_pnl_cents=pnl)
    rec.log_settlement(ticker="KXT-B", result="no", settled_pnl_cents=-50,
                       realized_pnl_cents=-50, hold_to_resolution_pnl_cents=-50)
    rows = rec.settlements_deduped()
    assert sorted(r[0] for r in rows) == ["KXT-A", "KXT-B"], rows
    assert sum(r[2] for r in rows) == 50, rows      # 100 - 50, not 300 - 50
    print("ok: one settlement row per market")


def test_restart_reconciles_open_positions():
    """Bug: restarting mid-game forgot open positions, leaving them unmanaged."""
    cfg = {"engine": {"default_order_size": 10, "min_edge_cents": 9},
           "position_manager": {"min_hold_ticks": 1},
           "fees": {"rate_taker": 0.07, "rate_maker": 0.0175},
           "risk": {"bankroll": 1000.0, "max_position_per_market": 500,
                    "max_total_exposure": 500.0, "max_orders_per_min": 60}}
    fee = make_fee_fn(cfg)
    held, foreign = "KXMLSTOTAL-26JUL22MIACHI-3", "KXMLSTOTAL-26JUL22OTHER-3"

    class Rest:
        async def get_positions(self):
            return [{"ticker": held, "position_fp": "-12.00", "total_traded_dollars": "3.60"},
                    {"ticker": foreign, "position_fp": "5.00", "total_traded_dollars": "2.00"},
                    {"ticker": "KXMLSTOTAL-26JUL22FLAT-2", "position_fp": "0.00"}]
    s = NS(rest=Rest(), tickers=[held],
           broker=PaperBroker(cfg, fee, tempfile.mkdtemp()),
           rules=RulesEngine(cfg, lambda r, b: None, fee))
    asyncio.run(Harness._reconcile_positions.__get__(s)())
    pos = s.rules.positions[held]
    assert (pos.side, pos.qty, pos.entry_price_cents) == ("no", 12, 30), pos
    assert s.broker.positions[held].no.qty == 12
    assert foreign not in s.rules.positions        # not on this board: not managed
    print("ok: restart re-adopts open positions (side, size, entry) from the exchange")


def test_exposure_cap_uses_live_positions_and_fails_closed():
    """Bug: the per-game cap read in-memory positions that had gone stale, letting an
    oversized order through. Now it fetches live positions and fails closed."""
    from data.orderbook import OrderBook
    tk = "KXMLSTOTAL-26JUL22MIACHI-3"
    venue = OrderBook(tk); venue.yes = {38: 50}; venue.no = {60: 50}     # yes ask 40c
    placed, fetched = [], []

    async def fake_place(book, side, action, px, n):
        placed.append((px, n)); return {"fill_count": n, "order_id": "x"}

    class Rest:
        def __init__(self, positions):
            self.positions = positions
        async def get_orderbook(self, t): return venue
        async def get_positions(self):
            fetched.append(1)
            if self.positions is None:
                raise TimeoutError("exchange unreachable")
            return self.positions
        async def get_balance_cents(self): return 10_000           # $100 balance

    def harness(rest):
        s = NS(rest=rest, ws=NS(books={tk: venue}), rest_verify_orders=True,
               block_on_dirty=True, live_broker=NS(place=fake_place, balance_cents=0),
               broker=NS(kill_switch_engaged=lambda: False, realized_cents=lambda: 0,
                         positions={}),                  # in-memory: nothing open
               daily_loss_limit_cents=800, _is_blind=lambda t: False,
               _entry_gates=lambda req, t: None,
               _score_market_contradiction=lambda t: None,
               _maker_eligible=lambda lane, trig: False, cross_buffer_cents=2,
               reaction_clip_dollars=3.0, max_game_fraction=0.35,
               gate_max_contracts=150, _live_count=lambda px, clip: 100, _loop=None)
        s._game_cost_basis_cents = Harness._game_cost_basis_cents.__get__(s)
        s._marketable_price = Harness._marketable_price.__get__(s)
        return Harness._place_live.__get__(s)

    req = NS(action="buy", side="yes", size=100, decision_price_cents=45,
             thesis=NS(lane="edge", trigger="event:goal", limit_price_cents=45,
                       thesis_id="t", llm_start_wall=None, llm_end_wall=None,
                       llm_latency_ms=None, fair_source="ladder"),
             is_exit=False, trigger_label="", reason="", event_monotonic=0,
             event_wall=0, decision_monotonic=0, decision_wall=0, sell_fraction=None,
             entry_price_cents=None, reference_price_cents=None, seconds_held=0,
             score_event_recv_monotonic=None)

    # The exchange already holds $40 in this game; cap is 35% of $100 = $35.
    exposed = [{"ticker": "KXMLSGAME-26JUL22MIACHI-MIA", "total_traded_dollars": "40.00"}]
    asyncio.run(harness(Rest(exposed))(req, venue, 100))
    assert fetched and placed == [], placed        # live exposure blocks it
    # Exchange unreachable: block rather than trade blind.
    fetched.clear()
    asyncio.run(harness(Rest(None))(req, venue, 100))
    assert fetched and placed == [], placed
    print("ok: exposure cap reads LIVE positions and fails closed when it can't")


if __name__ == "__main__":
    test_exit_not_double_counted_with_settlement()
    test_missing_history_is_not_booked_as_a_loss()
    test_archive_fallbacks()
    test_settlements_deduplicated()
    test_restart_reconciles_open_positions()
    test_exposure_cap_uses_live_positions_and_fails_closed()
    print("\nAll audit regression tests passed.")
