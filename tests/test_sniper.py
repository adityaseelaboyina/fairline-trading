"""Sniper-lane tests.

    python tests/test_sniper.py
"""
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from types import SimpleNamespace as NS

from data.orderbook import OrderBook
from data.score_feed import ScoreEvent, ScoreState
from engine.fees import make_fee_fn
from engine.rules import OpenPosition, RulesEngine
from engine.sniper import (SpikeDetector, class_a_scan, class_b_scan,
                           class_b_fair, decided_yes, depth_fill)
from main import Harness
from tradelog.recorder import Recorder


def _book(yes_levels, no_levels, ticker="T"):
    b = OrderBook(ticker)
    b.yes = dict(yes_levels)
    b.no = dict(no_levels)
    return b


def _state(home="Swiss", away="Algeria", hs=1, aw=0, period=2, clock="60'"):
    return ScoreState(event_id="760498", home=home, away=away, home_score=hs,
                      away_score=aw, state="in", period=period, clock=clock)


def _goal_stub(run_dir):
    """Minimal Harness stub for _handle_goal / _handle_correction."""
    s = NS(event_to_markets={"760498": ["KXWCTOTAL-26JUL02SUIDZA-2"]},
           _seen_goal_keys=set(), _last_spike_wall={}, _goal_ids={}, _goal_game={},
           _snipe_blacklist={}, snipe_blacklist_s=120.0,
           _snipe_positions={}, recorder=Recorder(run_dir),
           _loop=None, snipe_shadow=True, snipe_enabled=False,
           _game_meta={}, _ml_fair_snap={}, _snipe_tickers=set())
    s._handle_goal = Harness._handle_goal.__get__(s)
    s._handle_correction = Harness._handle_correction.__get__(s)
    s._flatten_snipes = Harness._flatten_snipes.__get__(s)
    return s


def test_a_goal_dedup():
    tmp = tempfile.mkdtemp()
    s = _goal_stub(tmp)
    ev = ScoreEvent("goal", "760498", "Swiss", _state(hs=1, aw=0), 1.0, time.time())
    s._handle_goal(ev)
    s._handle_goal(ev)                      # poll + spike double-detection
    n = s.recorder.db.execute("SELECT COUNT(*) FROM goal_events").fetchone()[0]
    assert n == 1, n
    # a DIFFERENT goal (2-0) is not deduped
    s._handle_goal(ScoreEvent("goal", "760498", "Swiss", _state(hs=2, aw=0),
                              2.0, time.time()))
    n = s.recorder.db.execute("SELECT COUNT(*) FROM goal_events").fetchone()[0]
    assert n == 2, n
    print("ok: (a) goal_event dedup across double-detection; new goals still land")


def test_b_var_reversion_flatten_and_blacklist():
    tmp = tempfile.mkdtemp()
    s = _goal_stub(tmp)
    ev = ScoreEvent("goal", "760498", "Swiss", _state(hs=1, aw=0), 1.0, time.time())
    s._handle_goal(ev)
    gid = s._goal_ids[("760498", "Swiss")]
    # an open sniper position on that goal + a rules engine that can flatten it
    fee = make_fee_fn({"fees": {"rate_taker": 0.07, "rate_maker": 0.0175}})
    fired = []
    rules = RulesEngine({"engine": {"default_order_size": 10, "min_edge_cents": 9},
                         "position_manager": {"min_hold_ticks": 1}},
                        lambda req, book: fired.append(req), fee)
    tk = "KXWCTOTAL-26JUL02SUIDZA-2"
    rules.positions[tk] = OpenPosition(ticker=tk, side="yes", entry_price_cents=90,
                                       qty=3, entry_tick=0, entry_wall=time.time(),
                                       exit_rules=[], thesis_id="snipe")
    s.rules = rules
    s.ws = NS(books={tk: _book({80: 100}, {15: 100}, tk)})
    s._snipe_positions = {gid: [(tk, "yes")]}
    s._handle_correction(ScoreEvent("correction", "760498", "Swiss",
                                    _state(hs=0, aw=0), 2.0, time.time(),
                                    "home score 1->0"))
    rev = s.recorder.db.execute("SELECT reverted FROM goal_events WHERE id=?",
                                (gid,)).fetchone()[0]
    assert rev == 1, "goal not marked reverted"
    assert any(r.action == "sell" and r.trigger_label == "var_reversion"
               for r in fired), fired
    assert s._snipe_blacklist.get("26JUL02SUIDZA", 0) > time.time(), "not blacklisted"
    print("ok: (b) VAR reversion marks goal reverted, flattens position, blacklists game")


def test_c_decided_truth_table():
    # KXWCTOTAL suffix 3 = Over 2.5: at exactly 2 goals NOT decided; at 3 decided
    assert not decided_yes("KXWCTOTAL-26JUL02SUIDZA-3", 1, 1)
    assert decided_yes("KXWCTOTAL-26JUL02SUIDZA-3", 2, 1)
    # team total: SUI over 1.5 (suffix SUI2): SUI is home in SUIDZA
    assert not decided_yes("KXWCTEAMTOTAL-26JUL02SUIDZA-SUI2", 1, 5)
    assert decided_yes("KXWCTEAMTOTAL-26JUL02SUIDZA-SUI2", 2, 0)
    # integer-line semantics: total == line is NOT decided (strictly greater only)
    from engine.markets import parse_market
    assert parse_market("KXWCTOTAL-26JUL02SUIDZA-3").line == 2.5
    # spreads / moneylines NEVER decided, even at huge leads
    assert not decided_yes("KXWCSPREAD-26JUL02SUIDZA-SUI2", 5, 0)
    assert not decided_yes("KXWCGAME-26JUL02SUIDZA-SUI", 9, 0)
    assert not decided_yes("KXWCGAME-26JUL02SUIDZA-TIE", 1, 1)
    print("ok: (c) decided truth table (totals only, strict-greater, no spreads/ML)")


def test_d_class_a_gates():
    tk = "KXWCTOTAL-26JUL02SUIDZA-2"       # Over 1.5; score 2-1 -> decided
    clip = 300
    # ask 92 with depth -> allowed
    b92 = _book({80: 1000}, {8: 1000}, tk)           # yes_ask = 92
    out = class_a_scan({tk: b92}, [tk], 2, 1, 93, clip, 3)
    assert len(out) == 1 and out[0]["ask"] == 92 and out[0]["order_px"] == 93, out
    # ask 95 -> blocked by the 93c cap
    b95 = _book({80: 1000}, {5: 1000}, tk)           # yes_ask = 95
    assert class_a_scan({tk: b95}, [tk], 2, 1, 93, clip, 3) == []
    # depth-insufficient -> blocked (clip needs 3 contracts, only 1 resting)
    bthin = _book({80: 1000}, {8: 1}, tk)
    assert class_a_scan({tk: bthin}, [tk], 2, 1, 93, clip, 3) == []
    # NOT decided (total == line's integer) -> never a candidate even at 50c
    bcheap = _book({40: 1000}, {50: 1000}, tk)
    assert class_a_scan({tk: bcheap}, [tk], 1, 0, 93, clip, 3) == []
    print("ok: (d) Class A allowed at 92c, blocked at 95c / thin depth / undecided")


def test_e_class_b_gap_and_exit():
    tk = "KXWCTOTAL-26JUL02SUIDZA-3"       # Over 2.5, score 1-0 -> undecided
    # lam 1.0/1.0 full-match, minute 45 -> fair_yes ~26.4c, fair_no ~73.6c
    fair = class_b_fair(tk, 1, 0, 45.0, 1.0, 1.0)
    assert fair is not None and 25 < fair < 28, fair
    def mk(no_ask):                        # buy-NO candidate: no_ask = 100 - max(yes)
        return _book({100 - no_ask: 2000}, {24: 2000}, tk)
    # gap ~8.6c (< 10) -> no trade
    out = class_b_scan({tk: mk(65)}, [tk], 1, 0, 45.0, 1.0, 1.0, 10, 300, 2)
    assert out == [], out
    # gap ~11.6c (>= 10) -> trade, with the MANDATORY decay exit minute attached
    out = class_b_scan({tk: mk(62)}, [tk], 1, 0, 45.0, 1.0, 1.0, 10, 300, 2)
    assert len(out) == 1 and out[0]["side"] == "no", out
    assert out[0]["exit_minute"] == 47, out[0]        # entry minute 45 + 2
    assert out[0]["order_px"] <= int(out[0]["fair"] - 4), out[0]   # fair-4 cap
    print("ok: (e) Class B gap 9c no-trade, 11c trades with decay exit registered")


def test_f_latency_budget_sim():
    tks = [f"KXWCTOTAL-26JUL02SUIDZA-{k}" for k in range(1, 7)]
    books = {t: _book({80: 1000}, {8: 1000}, t) for t in tks}
    t0 = time.monotonic()
    class_a_scan(books, tks, 3, 1, 93, 300, 3)
    class_b_scan(books, tks, 1, 0, 50.0, 1.2, 0.8, 10, 300, 2)
    ms = (time.monotonic() - t0) * 1000
    assert ms < 1500, f"scan took {ms:.0f}ms > 1500ms budget"
    print(f"ok: (f) full snipe scan sim in {ms:.1f}ms (< 1500ms budget)")


def test_g_shadow_depth_priced():
    # two-level book: worst level (not avg, not mid) must price the fill
    tk = "KXWCTOTAL-26JUL02SUIDZA-2"
    b = _book({80: 1000}, {12: 2, 10: 2}, tk)   # yes asks: 2 @ 88, then 2 @ 90
    f = depth_fill(b, "yes", 300)               # clip 300c @ ask 88 -> 3 contracts
    assert f is not None
    contracts, worst, ask = f
    assert (contracts, ask) == (3, 88) and worst == 90, f   # WORST level = 90
    # and the shadow row round-trips through the recorder
    tmp = tempfile.mkdtemp()
    r = Recorder(tmp)
    sid = r.log_shadow_snipe(goal_event_id=1, klass="a", ticker=tk, side="yes",
                             would_price_cents=worst, ask_cents=ask,
                             contracts=contracts, detect_ms=12.0, calc_ms=3.0,
                             gates_ms=1.0)
    r.update_shadow_book(sid, 60, 97.0)
    row = r.db.execute("SELECT would_price_cents,book_60s,detect_ms FROM shadow_snipes "
                       "WHERE id=?", (sid,)).fetchone()
    assert row == (90.0, 97.0, 12.0), row
    print("ok: (g) shadow rows depth-priced at the WORST fill level + book tracking")


def test_per_class_arming():
    """P1/P2 are separate gates: class_a_enabled fires live snipes for A only;
    Class B stays shadow (logged, armed=0, never placed)."""
    import asyncio
    tmp = tempfile.mkdtemp()
    tk_a = "KXWCTOTAL-26JUL02SUIDZA-2"        # decided at 2-1 -> Class A candidate
    tk_b = "KXWCTOTAL-26JUL02SUIDZA-4"        # undecided -> Class B candidate
    books = {tk_a: _book({80: 1000}, {8: 1000}, tk_a),      # ask 92
             tk_b: _book({40: 2000}, {24: 2000}, tk_b)}     # no_ask 60
    placed = []

    async def fake_place(c, klass, gid, dw, sid=None):
        placed.append(klass)

    s = NS(recorder=Recorder(tmp), ws=NS(books=books),
           event_to_markets={"760498": [tk_a, tk_b]},
           snipe_a_enabled=True, snipe_b_enabled=False,
           snipe_a_max_price=93, snipe_a_max_markets=3, snipe_clip_cents=300,
           snipe_b_min_gap=10, snipe_b_max=2,
           _lambda_snap={"26JUL02SUIDZA": (time.time(), 1.5, 1.0)}, sharp=None,
           fee_fn=make_fee_fn({"fees": {"rate_taker": 0.07, "rate_maker": 0.0175}}),
           _snipe_gates=lambda *a, **k: None, _loop=None,
           _place_snipe=fake_place)
    s._snipe_on_goal = Harness._snipe_on_goal.__get__(s)
    ev = ScoreEvent("goal", "760498", "Swiss", _state(hs=2, aw=1), 1.0, time.time())
    asyncio.run(
        s._snipe_on_goal(1, ev, "26JUL02SUIDZA", time.time()))
    assert placed and set(placed) == {"a"}, placed      # A fired live, B never did
    armed = dict(s.recorder.db.execute(
        "SELECT klass, MAX(armed) FROM shadow_snipes GROUP BY klass"))
    assert armed.get("a") == 1, armed                   # A logged as armed
    assert armed.get("b", 0) == 0, armed                # B logged shadow-only
    print("ok: per-class arming — A fires live, B stays shadow (P1/P2 independent)")


def test_snipe_slot_budget():
    """Snipes get their OWN slot budget — reaction/edge positions no
    longer starve the sniper, and snipe positions don't crowd out reaction entries."""
    from engine.paper_broker import Position
    code = "26JUL05MEXENG"
    # two REACTION positions open in the game
    p1 = Position(f"KXWCTOTAL-{code}-2"); p1.yes.qty = 5
    p2 = Position(f"KXWCTOTAL-{code}-3"); p2.yes.qty = 5
    positions = {p1.ticker: p1, p2.ticker: p2}
    s = NS(_snipe_blacklist={}, broker=NS(positions=positions,
                                          kill_switch_engaged=lambda: False,
                                          realized_cents=lambda: 0),
           daily_loss_limit_cents=800, gate_min_price=15, gate_max_price=90,
           snipe_a_max_price=93, gate_max_minute=75, snipe_max_per_game=2,
           _snipe_tickers=set(), gate_max_pos_per_game=2,
           _score_market_contradiction=lambda tk: None)
    g = Harness._snipe_gates.__get__(s)
    # reaction positions do NOT block a snipe anymore
    assert g(f"KXWCTOTAL-{code}-4", "a", 88, code, 60.0) is None
    # but two SNIPE positions do
    ps1 = Position(f"KXWCTEAMTOTAL-{code}-ENG1"); ps1.yes.qty = 3
    ps2 = Position(f"KXWCTEAMTOTAL-{code}-ENG2"); ps2.yes.qty = 3
    positions.update({ps1.ticker: ps1, ps2.ticker: ps2})
    s._snipe_tickers = {ps1.ticker, ps2.ticker}
    r = g(f"KXWCTOTAL-{code}-4", "a", 88, code, 60.0)
    assert r is not None and "snipe max_per_game" in r, r
    # and snipe positions don't count against a REACTION entry's per-game cap
    e = NS(gate_min_price=15, gate_max_price=90, gate_max_minute=75,
           gate_max_pos_per_game=2, reaction_max_per_game=2, _reaction_opens={},
           latest_score={}, market_to_event={}, _snipe_tickers=s._snipe_tickers,
           broker=NS(positions=positions))
    eg = Harness._entry_gates.__get__(e)
    req = NS(decision_price_cents=40, thesis=None)
    # 2 reaction + 2 snipe open; only the 2 reaction count -> at cap -> blocked
    r2 = eg(req, f"KXWCGAME-{code}-TIE")
    assert r2 is not None and "max_positions_per_game: 2" in r2, r2
    # with one reaction position gone, entry passes despite 2 snipe positions open
    p2.yes.qty = 0
    assert eg(req, f"KXWCGAME-{code}-TIE") is None
    print("ok: snipe slot budget — lanes no longer compete for the same 2 slots")


def test_var_reversion_clears_dedup():
    """After a VAR reversion, a legitimately re-scored goal at
    the same total must produce a NEW goal event (dedup key cleared)."""
    tmp = tempfile.mkdtemp()
    s = _goal_stub(tmp)
    s.rules = NS(positions={})
    s.ws = NS(books={})
    ev = ScoreEvent("goal", "760498", "Swiss", _state(hs=1, aw=0), 1.0, time.time())
    s._handle_goal(ev)                                        # goal 1-0
    s._handle_correction(ScoreEvent("correction", "760498", "Swiss",
                                    _state(hs=0, aw=0), 2.0, time.time(),
                                    "home 1->0"))             # VAR: back to 0-0
    s._handle_goal(ScoreEvent("goal", "760498", "Swiss",
                              _state(hs=1, aw=0), 3.0, time.time()))  # re-scored 1-0
    n = s.recorder.db.execute("SELECT COUNT(*) FROM goal_events").fetchone()[0]
    assert n == 2, f"re-scored goal was deduped (rows={n})"
    print("ok: VAR reversion clears dedup — re-scored goal snipes again")


def test_spike_detector():
    d = SpikeDetector(n_markets=3, move_cents=8, window_s=2.0)
    w = 100.0
    assert d.on_tick("T1", "G", 50, w) is None
    assert d.on_tick("T2", "G", 50, w) is None
    assert d.on_tick("T3", "G", 50, w) is None
    assert d.on_tick("T1", "G", 59, w + 0.5) is None      # 1 mover
    assert d.on_tick("T2", "G", 59, w + 0.8) is None      # 2 movers
    assert d.on_tick("T3", "G", 59, w + 1.0) == "G"       # 3 same-dir movers -> fire
    # opposite-direction moves don't count together
    d2 = SpikeDetector(n_markets=2, move_cents=8, window_s=2.0)
    d2.on_tick("A", "G", 50, w); d2.on_tick("B", "G", 50, w)
    d2.on_tick("A", "G", 60, w + 0.5)
    assert d2.on_tick("B", "G", 40, w + 0.8) is None
    print("ok: spike detector fires on 3 same-direction movers, not mixed")


if __name__ == "__main__":
    test_a_goal_dedup()
    test_b_var_reversion_flatten_and_blacklist()
    test_c_decided_truth_table()
    test_d_class_a_gates()
    test_e_class_b_gap_and_exit()
    test_f_latency_budget_sim()
    test_g_shadow_depth_priced()
    test_per_class_arming()
    test_snipe_slot_budget()
    test_var_reversion_clears_dedup()
    test_spike_detector()
    print("\nAll sniper tests passed.")
