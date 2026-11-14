"""Tests for sharp-feed fairs, asymmetry tagging, and fee-aware routing.

    python tests/test_research.py
"""
import asyncio
import math
import os
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from types import SimpleNamespace as NS

from data.orderbook import OrderBook
from data.score_feed import ScoreEvent, ScoreState
from data.sharp_feed import SharpFeed, devig, map_group_to_kalshi
from main import Harness
from tradelog.recorder import Recorder


def _book(yes_levels, no_levels, ticker="T"):
    b = OrderBook(ticker)
    b.yes = dict(yes_levels)
    b.no = dict(no_levels)
    return b


# ---------------- (a) de-vig correctness ----------------
def test_a_devig():
    # bookmaker decimal odds 1.50 / 4.50 / 6.00 -> implied sum > 1 (the vig)
    implied = {"home": 1 / 1.50, "draw": 1 / 4.50, "away": 1 / 6.00}
    assert sum(implied.values()) > 1.0
    fair = devig(implied)
    assert abs(sum(fair.values()) - 1.0) < 1e-9, fair
    # proportionality preserved
    assert abs(fair["home"] / fair["draw"] - (implied["home"] / implied["draw"])) < 1e-9
    # mapping lands on the right Kalshi tickers
    m = map_group_to_kalshi("26JUL02SUIDZA", "SUI", "DZA", "1x2", fair)
    assert set(m) == {"KXWCGAME-26JUL02SUIDZA-SUI", "KXWCGAME-26JUL02SUIDZA-TIE",
                      "KXWCGAME-26JUL02SUIDZA-DZA"}, m
    t = map_group_to_kalshi("26JUL02SUIDZA", "SUI", "DZA", "total",
                            {"over": 0.6, "under": 0.4}, line=2.5)
    assert t == {"KXWCTOTAL-26JUL02SUIDZA-3": 0.6}, t
    print("ok: (a) de-vig sums to 1, proportional, maps to Kalshi tickers")


# ---------------- (b) fair-resolution order + stale fallback ----------------
def _fair_stub(sharp=None, ladder=None):
    s = NS(sharp=sharp)
    s._ladder_fair_yes = lambda ticker: ladder
    return Harness._resolve_fair.__get__(s)


def test_b_fair_resolution():
    cfg = {"sharp_feed": {"provider": "none", "fresh_seconds": 10}}
    feed = SharpFeed(cfg, {})
    feed.update({"T": 0.62}, wall=time.time())            # fresh sharp
    f, src = _fair_stub(sharp=feed, ladder=55.0)("T", "yes", analyst_fair=40)
    assert (round(f), src) == (62, "sharp"), (f, src)
    # NO side converts
    f, src = _fair_stub(sharp=feed, ladder=55.0)("T", "no", analyst_fair=40)
    assert (round(f), src) == (38, "sharp"), (f, src)
    # stale sharp (>10s) -> ladder fallback
    feed.update({"T": 0.62}, wall=time.time() - 11)
    f, src = _fair_stub(sharp=feed, ladder=55.0)("T", "yes", analyst_fair=40)
    assert (f, src) == (55.0, "ladder"), (f, src)
    # no sharp, no ladder -> analyst-claimed
    f, src = _fair_stub(sharp=feed, ladder=None)("T", "yes", analyst_fair=40)
    assert (f, src) == (40.0, "analyst"), (f, src)
    # nothing at all -> (None, 'none')
    f, src = _fair_stub(sharp=None, ladder=None)("T", "yes")
    assert (f, src) == (None, "none")
    print("ok: (b) resolution order sharp->ladder->analyst incl. stale-sharp fallback")


# ---------------- (c) scorer_was_favourite uses PRE-goal fair ----------------
def test_c_pre_goal_favourite_label():
    tmp = tempfile.mkdtemp()
    s = NS(event_to_markets={"760498": ["KXWCTOTAL-26JUL02SUIDZA-2"]},
           _seen_goal_keys=set(), _last_spike_wall={}, _goal_ids={}, _goal_game={},
           recorder=Recorder(tmp), _loop=None, snipe_shadow=True, snipe_enabled=False,
           _game_meta={"26JUL02SUIDZA": ("SUI", "DZA", ["Swiss", "Algeria"])},
           # PRE-goal snapshot: SUI was the favourite (0.70)...
           _ml_fair_snap={"26JUL02SUIDZA": (time.time() - 30, {"SUI": 0.70,
                                                               "DZA": 0.10})})
    s._handle_goal = Harness._handle_goal.__get__(s)
    # ...even though POST-goal books (not consulted) would now flip the label,
    # the tag must come from the snapshot taken BEFORE detect_wall.
    st = ScoreState(event_id="760498", home="Swiss", away="Algeria",
                    home_score=1, away_score=0, state="in", period=2, clock="70'")
    s._handle_goal(ScoreEvent("goal", "760498", "Swiss", st, 1.0, time.time()))
    fav, bucket = s.recorder.db.execute(
        "SELECT scorer_was_favourite,minute_bucket FROM goal_events").fetchone()
    assert fav == 1, fav
    assert bucket == "61-90+", bucket
    # longshot case: Algeria scores -> favourite=0
    st2 = ScoreState(event_id="760498", home="Swiss", away="Algeria",
                     home_score=1, away_score=1, state="in", period=2, clock="80'")
    s._handle_goal(ScoreEvent("goal", "760498", "Algeria", st2, 2.0, time.time()))
    fav2 = s.recorder.db.execute(
        "SELECT scorer_was_favourite FROM goal_events ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]
    assert fav2 == 0, fav2
    print("ok: (c) scorer_was_favourite from PRE-goal snapshot + minute buckets")


# ---------------- (e) fee router: post-then-cross; snipes never post ----------
def test_e_fee_router():
    # eligibility predicate: only edge/reaction on slow triggers; snipes never
    s = NS(maker_enabled=True)
    elig = Harness._maker_eligible.__get__(s)
    assert elig("edge", "pregame") and elig("reaction", "refresh")
    assert not elig("edge", "event:goal")            # urgent trigger -> take
    assert not elig("snipe_a", "pregame") and not elig("snipe_b", "refresh")
    assert not elig("reaction", "snipe:a")
    s.maker_enabled = False
    assert not elig("edge", "pregame")               # master switch off -> take

    # full lifecycle: post -> never fills -> timeout -> CANCEL -> re-gate -> cross
    calls = {"placed": [], "cancelled": [], "polled": 0}

    class StubRest:
        async def place_order_v2(self, **kw):
            calls["placed"].append(kw)
            return {"order_id": "oid1", "fill_count": 0}
        async def get_order(self, oid):
            calls["polled"] += 1
            return {"fill_count": 0, "status": "resting"}
        async def cancel_order(self, oid):
            calls["cancelled"].append(oid)
            return {}

    h = NS(rest=StubRest(), maker_patience_s=0.15, gate_min_price=15,
           _entry_gates=lambda req, tk: None,
           _score_market_contradiction=lambda tk: None)
    mtc = Harness._maker_then_cross.__get__(h)
    book = _book({46: 500}, {50: 500}, "TK")          # yes bid 46 to join
    req = NS(side="yes", thesis=None, decision_price_cents=50)
    resp, style = asyncio.run(
        mtc(req, book, "TK", 5))
    assert resp is None and style == "cross", (resp, style)
    assert calls["cancelled"] == ["oid1"], calls       # no orphaned resting order
    p = calls["placed"][0]
    # time_in_force is REQUIRED — a maker order is GTC + post_only
    # (the old tif=None omitted a required field and 400'd every time).
    assert p.get("post_only") is True and p.get("tif") == "good_till_canceled", p
    assert p.get("stp") == "maker", p
    assert abs(p["price_dollars"] - 0.46) < 1e-9, p    # joined the touch, not crossed
    print("ok: (e) router posts GTC+post_only at touch, cancels at timeout, crosses")


# ---------------- (f) ledger fee lines reconcile ----------------
def test_f_ledger_fees():
    import ledger
    tmp = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp, "run"), exist_ok=True)
    r = Recorder(os.path.join(tmp, "run"))
    fill = NS(ticker="KXWCTOTAL-26JUL02SUIDZA-3", side="yes", action="buy",
              requested_size=10, filled_size=10, decision_price_cents=50,
              sim_fill_price_cents=50.0, fee_cents=18, accepted=True, reason="live")
    r.log_decision(thesis_id="t", trigger="x", fill=fill, event_monotonic=0,
                   event_wall=0, decision_monotonic=0, decision_wall=time.time(),
                   llm_start_wall=None, llm_end_wall=None, llm_latency_ms=None,
                   lane="edge", exec_style="taker")
    r.close()
    cwd = os.getcwd()
    try:
        os.chdir(tmp)
        _b, _s, _res, _ts, _lanes, fees = ledger.collect()
    finally:
        os.chdir(cwd)
    assert fees["paid"] == 18, fees
    # all-taker counterfactual at 50c x10: ceil(0.07*10*0.25*100) = 18
    assert fees["all_taker"] == math.ceil(0.07 * 10 * 0.25 * 100), fees
    print("ok: (f) ledger fee lines reconcile with per-fill fee fields")


if __name__ == "__main__":
    test_a_devig()
    test_b_fair_resolution()
    test_c_pre_goal_favourite_label()
    test_e_fee_router()
    test_f_ledger_fees()
    print("\nAll research-feature tests passed.")
