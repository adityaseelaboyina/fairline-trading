"""Gate + safety tests. Uses unbound Harness
methods with stub `self` objects so no network/account is needed.

    python tests/test_gates.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from types import SimpleNamespace as NS

from data.orderbook import OrderBook
from engine.fees import make_fee_fn
from engine.paper_broker import Position
from main import Harness, estimate_minute


def _book(yes_mid):
    b = OrderBook("x")
    b.yes = {yes_mid: 1000}
    b.no = {100 - yes_mid: 1000}   # so mid("yes") == yes_mid
    return b


# ---------------- estimate_minute (extra time) ----------------
def test_estimate_minute():
    def st(period, clock):
        return NS(period=period, clock=clock)
    assert estimate_minute(st(1, "37'")) == 37
    assert estimate_minute(st(2, "67'")) == 67
    assert estimate_minute(st(2, "22'")) == 67       # 22 < 45 -> 45 + 22
    assert estimate_minute(st(3, "91'")) == 91
    assert estimate_minute(st(4, "106'")) == 106
    assert estimate_minute(st(4, "3'")) == 108       # 3 < 105 -> 105 + 3
    assert estimate_minute(st(5, "")) == 120         # shootout
    assert estimate_minute(st(0, "")) is None        # pregame
    print("ok: estimate_minute handles halves / extra time / shootout / pregame")


# ---------------- mega-edge veto ----------------
def test_mega_edge_veto():
    fee = make_fee_fn({"fees": {"rate_taker": 0.07, "rate_maker": 0.0175}})
    s = NS(max_plausible_edge=20, decided_moneyline=95, min_edge_after_fees=4,
           fee_fn=fee, ws=NS(books={}))
    g = Harness._apply_no_trade_gate.__get__(s)
    p = g({"best_trade": {"market_ticker": "KXWCGAME-26JUN29BRAJPN-BRA",
                          "side": "no", "limit_price_cents": 10}, "edge_cents": 68})
    assert p["best_trade"] is None and "implausible" in p["_guard_note"], p
    p2 = g({"best_trade": {"market_ticker": "KXWCGAME-26JUN29BRAJPN-BRA",
                           "side": "no", "limit_price_cents": 45}, "edge_cents": 12})
    assert p2["best_trade"] is not None, p2
    print("ok: mega-edge veto blocks 68c, allows 12c")


# ---------------- ladder gate (totals / team-totals) ----------------
def _ladder_stub(books):
    s = NS(ladder_enabled=True, ladder_edge_mid=9.0, ladder_edge_extreme=18.0,
           ws=NS(books=books))
    s._pois_sf = Harness._pois_sf
    s._lam_from_prob = Harness._lam_from_prob.__get__(s)
    s._edge_bar = Harness._edge_bar.__get__(s)
    s._ladder_spec = Harness._ladder_spec
    return Harness._apply_ladder_gate.__get__(s)


def test_ladder_gate_blocks_team_total_fade():
    g = _ladder_stub({
        "KXWCTEAMTOTAL-26JUN30FRASWE-FRA1": _book(94),
        "KXWCTEAMTOTAL-26JUN30FRASWE-FRA2": _book(70),
        "KXWCTEAMTOTAL-26JUN30FRASWE-FRA3": _book(40),
    })
    p = g({"best_trade": {"market_ticker": "KXWCTEAMTOTAL-26JUN30FRASWE-FRA1",
                          "side": "no", "limit_price_cents": 6}})
    assert p["best_trade"] is None and "ladder" in p["_guard_note"], p
    print("ok: ladder gate blocks the France team-total fade")


# ---------------- fresh-positions cost basis ----------------
def test_game_cost_basis_from_fresh_positions():
    f = Harness._game_cost_basis_cents.__get__(NS())
    positions = [
        {"ticker": "KXWCTOTAL-26JUL01BELSEN-3", "total_traded_dollars": "10.81"},
        {"ticker": "KXWCTEAMTOTAL-26JUL01BELSEN-BEL1", "total_traded_dollars": "7.98"},
        {"ticker": "KXWCGAME-26JUL01USABIH-USA", "total_traded_dollars": "5.00"},
        {"ticker": "KXWCSPREAD-26JUL01BELSEN-SEN2", "market_exposure_dollars": "2.00"},
    ]
    assert f("26JUL01BELSEN", positions) == round((10.81 + 7.98 + 2.00) * 100)  # other-game excluded
    assert f("26JUL01USABIH", positions) == 500
    print("ok: per-game cost basis from FRESH positions (total_traded or market_exposure)")


# ---------------- entry gates ----------------
def _entry_stub(latest_score=None, positions=None):
    s = NS(gate_min_price=15, gate_max_price=90, gate_max_minute=75,
           gate_max_pos_per_game=2, latest_score=latest_score or {},
           market_to_event={"KXWCTOTAL-26JUL01BELSEN-3": "760493"},
           broker=NS(positions=positions or {}), _snipe_tickers=set(),
           _game_root={})
    s._root = Harness._root.__get__(s)
    return Harness._entry_gates.__get__(s)


def test_entry_gate_price_floor_ceiling():
    g = _entry_stub()
    tk = "KXWCTOTAL-26JUL01BELSEN-3"
    assert "min_entry_price" in g(NS(decision_price_cents=10), tk)   # 10c blocked (the SEN +1.5 case)
    assert "max_entry_price" in g(NS(decision_price_cents=95), tk)   # 95c blocked
    assert g(NS(decision_price_cents=40), tk) is None               # 40c allowed
    print("ok: entry gate blocks <15c and >90c, allows 40c")


def test_entry_gate_minute():
    tk = "KXWCTOTAL-26JUL01BELSEN-3"
    late = {"760493": NS(state="in", period=2, clock="80'", home_score=1, away_score=0)}
    assert "max_entry_minute" in _entry_stub(latest_score=late)(NS(decision_price_cents=40), tk)
    early = {"760493": NS(state="in", period=1, clock="30'", home_score=0, away_score=0)}
    assert _entry_stub(latest_score=early)(NS(decision_price_cents=40), tk) is None
    print("ok: entry gate blocks in-play entries after minute 75")


def test_entry_gate_positions_per_game():
    p1 = Position("KXWCTOTAL-26JUL01BELSEN-3"); p1.no.qty = 10
    p2 = Position("KXWCTEAMTOTAL-26JUL01BELSEN-BEL1"); p2.yes.qty = 5
    g = _entry_stub(positions={p1.ticker: p1, p2.ticker: p2})
    r = g(NS(decision_price_cents=40), "KXWCSPREAD-26JUL01BELSEN-SEN2")   # 3rd leg on the game
    assert r is not None and "max_positions_per_game" in r, r
    print("ok: entry gate blocks a 3rd position in one game (anti-stacking)")


# ---------------- score/market contradiction ----------------
def test_score_market_contradiction():
    st = {"760493": NS(state="in", period=2, clock="70'", home_score=2, away_score=0)}
    books = {"KXWCGAME-26JUL01BELSEN-BEL": _book(15)}   # BEL is home (code BELSEN) leading 2-0 but ML 15c
    s = NS(latest_score=st, market_to_event={"KXWCTOTAL-26JUL01BELSEN-3": "760493"},
           ws=NS(books=books), _game_root={})
    s._root = Harness._root.__get__(s)
    r = Harness._score_market_contradiction.__get__(s)("KXWCTOTAL-26JUL01BELSEN-3")
    assert r is not None and "leads by 2" in r, r
    print("ok: contradiction breaker fires on 2-0 vs 15c ML (frozen feed)")


# ---------------- spread/moneyline Skellam gate ----------------
def _diff_stub(books, latest_score, code):
    s = NS(ladder_enabled=True, ladder_edge_mid=9.0, ladder_edge_extreme=18.0,
           ws=NS(books=books), latest_score=latest_score, _game_root={},
           market_to_event={f"KXWCSPREAD-{code}-SEN2": "760493",
                            f"KXWCGAME-{code}-SEN": "760493",
                            f"KXWCGAME-{code}-TIE": "760493"})
    s._root = Harness._root.__get__(s)
    s._pois_sf = Harness._pois_sf
    s._lam_from_prob = Harness._lam_from_prob.__get__(s)
    s._edge_bar = Harness._edge_bar.__get__(s)
    s._ml_mid = Harness._ml_mid.__get__(s)
    s._team_lambda = Harness._team_lambda.__get__(s)
    s._total_lambda = Harness._total_lambda.__get__(s)
    s._margin_prob = Harness._margin_prob
    return Harness._apply_diff_gate.__get__(s)


def test_diff_gate_blocks_longshot_spread():
    code = "26JUL01BELSEN"      # home=BEL, away=SEN
    books = {
        f"KXWCTEAMTOTAL-{code}-BEL1": _book(90), f"KXWCTEAMTOTAL-{code}-BEL2": _book(60),
        f"KXWCTEAMTOTAL-{code}-BEL3": _book(30),
        f"KXWCTEAMTOTAL-{code}-SEN1": _book(50), f"KXWCTEAMTOTAL-{code}-SEN2": _book(15),
    }
    st = {"760493": NS(state="in", period=1, clock="20'", home_score=0, away_score=0)}
    g = _diff_stub(books, st, code)
    # "Senegal wins by > 1.5" (underdog blowout) bought at 20c -> Skellam fair ~2c -> blocked
    p = g({"best_trade": {"market_ticker": f"KXWCSPREAD-{code}-SEN2",
                          "side": "yes", "limit_price_cents": 20}})
    assert p["best_trade"] is None and "diff-gate" in p["_guard_note"], p
    print("ok: diff-gate blocks a longshot underdog spread (the SEN +1.5 class)")


def _diff_fixture():
    """Shared fixture: 0-0 at 20', BEL strong ladder, SEN weak ladder. Returns the
    gate plus the model's own fair prices for TIE-no and SEN-ML-yes."""
    code = "26JUL01BELSEN"
    books = {
        f"KXWCTEAMTOTAL-{code}-BEL1": _book(90), f"KXWCTEAMTOTAL-{code}-BEL2": _book(60),
        f"KXWCTEAMTOTAL-{code}-BEL3": _book(30),
        f"KXWCTEAMTOTAL-{code}-SEN1": _book(50), f"KXWCTEAMTOTAL-{code}-SEN2": _book(15),
    }
    st = {"760493": NS(state="in", period=1, clock="20'", home_score=0, away_score=0)}
    g = _diff_stub(books, st, code)
    # reproduce the gate's own lambdas/fair so the tests aren't hardcoded
    s = NS(ws=NS(books=books), _game_root={})
    s._root = Harness._root.__get__(s)
    s._pois_sf = Harness._pois_sf
    s._lam_from_prob = Harness._lam_from_prob.__get__(s)
    lam_h = Harness._team_lambda.__get__(s)(code, "BEL")
    lam_a = Harness._team_lambda.__get__(s)(code, "SEN")
    rem = (90.0 - 20.0) / 90.0
    p_tie = Harness._margin_prob(0, lam_h * rem, lam_a * rem, lambda d: d == 0)
    p_sen = Harness._margin_prob(0, lam_a * rem, lam_h * rem, lambda d: d > 0)
    return g, code, (1.0 - p_tie) * 100.0, p_sen * 100.0


def test_tie_no_overpay_rule():
    g, code, fair_tie_no, _ = _diff_fixture()
    tie = f"KXWCGAME-{code}-TIE"
    # tie-NO at ~fair -> PASSES (the proven reaction lane stays open)
    p = g({"best_trade": {"market_ticker": tie, "side": "no",
                          "limit_price_cents": int(round(fair_tie_no))}})
    assert p["best_trade"] is not None, (fair_tie_no, p)
    # tie-NO at fair+10c -> BLOCKED (overpay)
    p2 = g({"best_trade": {"market_ticker": tie, "side": "no",
                           "limit_price_cents": int(round(fair_tie_no)) + 10}})
    assert p2["best_trade"] is None and "overpays" in p2["_guard_note"], p2
    print("ok: tie-NO at fair passes, at fair+10c blocks (no-overpay rule)")


def test_team_ml_at_fair_blocks():
    g, code, _, fair_sen_yes = _diff_fixture()
    # team moneyline bought AT fair has ~0 edge -> edge bar blocks it
    p = g({"best_trade": {"market_ticker": f"KXWCGAME-{code}-SEN", "side": "yes",
                          "limit_price_cents": int(round(fair_sen_yes))}})
    assert p["best_trade"] is None and "diff-gate" in p["_guard_note"], (fair_sen_yes, p)
    print("ok: team-ML at fair blocks (edge bar still applies to non-TIE moneylines)")


# ---------------- reaction lane (tests a-f) ----------------
def _lane_stub():
    s = NS(min_edge_entry=9, _REACTION_EVENT_RE=Harness._REACTION_EVENT_RE,
           sharp=None)                                  # no sharp feed in tests
    s._ladder_fair_yes = lambda ticker: None            # -> analyst fair fallback
    s._resolve_fair = Harness._resolve_fair.__get__(s)
    s._reaction_eligible = Harness._reaction_eligible.__get__(s)
    return Harness._determine_lane.__get__(s)


def _tie_pick(limit=46, fair=45, exit_rules=None, edge=0,
              ticker="KXWCGAME-26JUL02SUIDZA-TIE"):
    return {"best_trade": {"market_ticker": ticker, "side": "no",
                           "limit_price_cents": limit},
            "edge_cents": edge, "fair_value_cents": fair,
            "exit_rules": exit_rules if exit_rules is not None else
            [{"event": "goal_for:Switzerland", "action": "sell_all", "reason": "lock"}]}


def test_reaction_a_tie_at_fair_enters():
    from engine.rules import RulesEngine, Thesis
    lane_of = _lane_stub()
    pick = _tie_pick()                                   # tie-NO @46, fair 45, goal_for exit
    assert lane_of(pick) == "reaction", pick
    # a reaction thesis with ZERO edge enters; the same thesis in the edge lane does not
    fee = make_fee_fn({"fees": {"rate_taker": 0.07, "rate_maker": 0.0175}})
    for lane, should_enter in (("reaction", True), ("edge", False)):
        fired = []
        eng = RulesEngine({"engine": {"default_order_size": 10, "min_edge_cents": 9},
                           "position_manager": {"min_hold_ticks": 1}},
                          lambda req, book: fired.append(req), fee)
        eng.set_thesis(Thesis(
            thesis_id="t", market_ticker="x", side="no", limit_price_cents=46,
            edge_cents=0, exit_rules=pick["exit_rules"], llm_start_wall=0,
            llm_end_wall=0, llm_latency_ms=1, trigger_event_monotonic=0,
            trigger_event_wall=0, trigger="event:goal", lane=lane))
        b = _book(60)               # no_ask = 40 <= limit 46
        b.market_ticker = "x"
        eng.on_tick(b, 1.0)
        assert bool(fired) is should_enter, (lane, fired)
    print("ok: (a) tie-NO at fair + goal_for exit enters via reaction lane (edge lane doesn't)")


def test_reaction_b_overpay_blocked():
    lane_of = _lane_stub()
    p = _tie_pick(limit=55, fair=45)                     # fair+10 -> overpay
    assert lane_of(p) == "edge" and "overpays" in p["_reaction_reject"], p
    print("ok: (b) same pick at fair+10c rejected from reaction lane (no-overpay)")


def test_reaction_c_no_event_exit_blocked():
    lane_of = _lane_stub()
    p = _tie_pick(exit_rules=[{"event": "price<=30", "action": "sell_all"}])
    assert lane_of(p) == "edge" and "event-keyed" in p["_reaction_reject"], p
    print("ok: (c) no event-keyed exit rule -> not a reaction setup")


def test_reaction_d_spread_blocked():
    lane_of = _lane_stub()
    p = _tie_pick(ticker="KXWCSPREAD-26JUL02SUIDZA-SUI2")
    assert lane_of(p) == "edge" and "market type" in p["_reaction_reject"], p
    print("ok: (d) spread never reaction-eligible (stays edge-gated)")


def test_reaction_e_third_setup_blocked():
    s = NS(gate_min_price=15, gate_max_price=90, gate_max_minute=75,
           gate_max_pos_per_game=5,                       # loosened so ONLY the reaction cap trips
           reaction_max_per_game=2, _reaction_opens={"26JUL02SUIDZA": 2},
           latest_score={}, market_to_event={}, broker=NS(positions={}))
    g = Harness._entry_gates.__get__(s)
    req = NS(decision_price_cents=46, thesis=NS(lane="reaction"))
    r = g(req, "KXWCGAME-26JUL02SUIDZA-TIE")
    assert r is not None and "reaction max_per_game" in r, r
    print("ok: (e) 3rd reaction setup in one game blocked")


def test_reaction_f_edge_lane_precedence():
    lane_of = _lane_stub()
    p = _tie_pick(edge=12)                               # qualifies for BOTH lanes
    assert lane_of(p) == "edge", p
    print("ok: (f) pick qualifying for both lanes tags edge (edge precedence)")


if __name__ == "__main__":
    test_estimate_minute()
    test_mega_edge_veto()
    test_ladder_gate_blocks_team_total_fade()
    test_game_cost_basis_from_fresh_positions()
    test_entry_gate_price_floor_ceiling()
    test_entry_gate_minute()
    test_entry_gate_positions_per_game()
    test_score_market_contradiction()
    test_diff_gate_blocks_longshot_spread()
    test_tie_no_overpay_rule()
    test_team_ml_at_fair_blocks()
    test_reaction_a_tie_at_fair_enters()
    test_reaction_b_overpay_blocked()
    test_reaction_c_no_event_exit_blocked()
    test_reaction_d_spread_blocked()
    test_reaction_e_third_setup_blocked()
    test_reaction_f_edge_lane_precedence()
    print("\nAll gate + safety tests passed.")
