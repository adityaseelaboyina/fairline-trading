"""Club-soccer support: generic series parsing + the REST-verify
staleness fix. Run: python tests/test_clubsoccer.py"""
import asyncio
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from types import SimpleNamespace as NS

from data.orderbook import OrderBook
from engine.markets import parse_market, series_kind, series_root
from main import Harness
from tradelog.recorder import Recorder


def test_generic_series_parsing():
    # kind classification, any sport root
    assert series_kind("KXUCLGAME") == "GAME" and series_root("KXUCLGAME") == "KXUCL"
    assert series_kind("KXMLSTEAMTOTAL") == "TEAMTOTAL" and series_root("KXMLSTEAMTOTAL") == "KXMLS"
    assert series_kind("KXWCTOTAL") == "TOTAL"          # WC still parses (archives/tests)
    assert series_kind("KXMLSSPREAD") == "SPREAD"
    # full parse on real club tickers
    m = parse_market("KXUCLGAME-26JUL22EGRCEL-TIE")
    assert (m.kind, m.is_tie, m.home_code, m.away_code) == ("moneyline", True, "EGR", "CEL")
    m = parse_market("KXUCLTOTAL-26JUL22OMOALM-5")
    assert (m.kind, m.line) == ("game_total", 4.5)
    m = parse_market("KXMLSTEAMTOTAL-26JUL25HOUATX-HOU3")
    assert (m.kind, m.team_code, m.line) == ("team_total", "HOU", 2.5)
    m = parse_market("KXMLSSPREAD-26JUL25HOUATX-HOU3")
    assert (m.kind, m.team_code, m.line) == ("spread", "HOU", 2.5)
    print("ok: generic parsing — KXUCL/KXMLS/KXWC all share one grammar")


def test_reaction_lane_accepts_club_tie():
    s = NS(min_edge_entry=9, _REACTION_EVENT_RE=Harness._REACTION_EVENT_RE, sharp=None)
    s._ladder_fair_yes = lambda ticker: None
    s._resolve_fair = Harness._resolve_fair.__get__(s)
    s._reaction_eligible = Harness._reaction_eligible.__get__(s)
    lane_of = Harness._determine_lane.__get__(s)
    pick = {"best_trade": {"market_ticker": "KXMLSGAME-26JUL25SJLAG-TIE", "side": "no",
                           "limit_price_cents": 46},
            "edge_cents": 0, "fair_value_cents": 45,
            "exit_rules": [{"event": "goal_for:LA Galaxy", "action": "sell_all"}]}
    assert lane_of(pick) == "reaction", pick
    # spreads still never reaction-eligible, club or not
    pick2 = dict(pick, best_trade={"market_ticker": "KXMLSSPREAD-26JUL25SJLAG-SJL2",
                                   "side": "no", "limit_price_cents": 46})
    assert lane_of(pick2) == "edge"
    print("ok: reaction lane accepts club-soccer TIE, still rejects spreads")


def test_ladder_spec_generic():
    spec = Harness._ladder_spec("KXUCLTOTAL-26JUL22OMOALM-4")
    assert spec == ("KXUCLTOTAL-26JUL22OMOALM-", 4, None), spec
    spec = Harness._ladder_spec("KXMLSTEAMTOTAL-26JUL25HOUATX-HOU2")
    assert spec == ("KXMLSTEAMTOTAL-26JUL25HOUATX-HOU", 2, "HOU"), spec
    print("ok: ladder gate builds sibling prefixes from the ticker's own series")


def test_rest_verify_order_path():
    """The staleness fix: buys price off the venue REST book; stale WS asks can no
    longer produce phantom entries; buys fail CLOSED when REST is down."""
    tk = "KXUCLTOTAL-26JUL22OMOALM-4"
    # WS mirror claims a cheap 42c ask (stale); venue truth is 99c
    stale_ws = OrderBook(tk); stale_ws.yes = {30: 5}; stale_ws.no = {58: 5}   # ws yes_ask = 42
    venue = OrderBook(tk); venue.yes = {98: 50}; venue.no = {1: 50}   # real ask = 99

    class GoodRest:
        async def get_orderbook(self, t): return venue
    class DeadRest:
        async def get_orderbook(self, t): raise TimeoutError()

    placed = []
    async def fake_place(book, side, action, px, n):
        placed.append(px); return {"fill_count": 0, "order_id": "x"}

    def mk(rest):
        s = NS(rest=rest, ws=NS(books={tk: stale_ws}), rest_verify_orders=True,
               block_on_dirty=True,
               live_broker=NS(place=fake_place, balance_cents=5000),
               broker=NS(kill_switch_engaged=lambda: False, realized_cents=lambda: 0),
               daily_loss_limit_cents=800, _is_blind=lambda t: False,
               _entry_gates=lambda req, t: None,
               _score_market_contradiction=lambda t: None,
               _maker_eligible=lambda lane, trig: False,
               cross_buffer_cents=2, _loop=None)
        s._marketable_price = Harness._marketable_price.__get__(s)
        s._place_live = Harness._place_live.__get__(s)
        return s

    # buy with limit 45: WS mirror says ask 42 (would fire); venue says 99 -> the
    # marketable pricer now sees 99 > limit and SKIPS. Phantom entry eliminated.
    req = NS(action="buy", side="yes", size=5, decision_price_cents=45,
             thesis=NS(lane="reaction", trigger="event:goal", limit_price_cents=45,
                       thesis_id="t", llm_start_wall=None, llm_end_wall=None,
                       llm_latency_ms=None, fair_source="ladder"),
             is_exit=False, trigger_label="", reason="",
             event_monotonic=0, event_wall=0, decision_monotonic=0, decision_wall=0,
             sell_fraction=None, entry_price_cents=None, reference_price_cents=None,
             seconds_held=0, score_event_recv_monotonic=None)
    asyncio.run(
        mk(GoodRest())._place_live(req, stale_ws, 5))
    assert placed == [], f"phantom order fired: {placed}"
    # and with REST down, the buy fails CLOSED (no order)
    asyncio.run(
        mk(DeadRest())._place_live(req, stale_ws, 5))
    assert placed == [], placed
    print("ok: REST-verify — stale 42c mirror can't fire (venue 99c); REST-down buys blocked")


if __name__ == "__main__":
    test_generic_series_parsing()
    test_reaction_lane_accepts_club_tie()
    test_ladder_spec_generic()
    test_rest_verify_order_path()
    print("\nAll club-soccer port tests passed.")
