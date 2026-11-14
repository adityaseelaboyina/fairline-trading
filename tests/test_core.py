"""Deterministic-core tests. No network, no third-party deps (stdlib only).

    python tests/test_core.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.orderbook import OrderBook
from engine.fees import make_fee_fn
from engine.paper_broker import PaperBroker
from engine.rules import RulesEngine, Thesis


def _book(yes=None, no=None, ticker="KXWC-TEST"):
    b = OrderBook(ticker)
    b.apply_snapshot({"yes": yes or [], "no": no or []}, recv_monotonic=1.0)
    return b


def test_orderbook_top_and_normalization():
    # legacy int cents
    b = _book(yes=[[40, 100], [42, 50]], no=[[55, 70], [56, 30]])
    assert b.best_yes_bid == 42
    assert b.best_no_bid == 56
    assert b.best_yes_ask == 100 - 56 == 44
    assert b.best_no_ask == 100 - 42 == 58
    # newer dollars/fixed-point encoding normalizes to same cents
    b2 = OrderBook("X")
    b2.apply_snapshot({"yes_dollars": [["0.4200", "50.00"]],
                       "no_dollars": [["0.5600", "30.00"]]}, 1.0)
    assert b2.best_yes_bid == 42 and b2.yes[42] == 50
    assert b2.best_no_bid == 56 and b2.no[56] == 30
    print("ok: orderbook top-of-book + normalization")


def test_orderbook_delta():
    b = _book(yes=[[40, 100]], no=[[56, 30]])
    assert b.apply_delta({"side": "yes", "price": 41, "delta": 20}, 2.0, seq=6)
    assert b.yes[41] == 20
    # consuming all liquidity removes the level
    assert b.apply_delta({"side": "yes", "price": 41, "delta": -20}, 2.0, seq=7)
    assert 41 not in b.yes
    # deltas always apply; connection-level seq-gap handling lives in KalshiWS
    assert b.apply_delta({"side": "no", "price": 55, "delta": 10}, 2.0, seq=99)
    assert b.no[55] == 10
    print("ok: delta apply (connection-level seq handling is in KalshiWS)")


def test_walk_crosses_spread():
    # to BUY yes we lift NO bids: no bid 56 -> yes offer 44, no bid 55 -> yes offer 45
    b = _book(yes=[[40, 100]], no=[[55, 10], [56, 5]])
    filled, avg, levels = b.walk("yes", "buy", 8)
    assert filled == 8
    # 5 @ 44 (best), 3 @ 45  -> avg = (5*44 + 3*45)/8
    assert abs(avg - (5 * 44 + 3 * 45) / 8) < 1e-9
    assert levels[0] == (44, 5)
    print("ok: book walk crosses spread correctly")


def test_fees_formula():
    fee = make_fee_fn({"fees": {"rate_taker": 0.07, "rate_maker": 0.0175}})
    # max fee at 50c: 0.07*1*0.5*0.5 = 0.0175 -> 1.75c -> ceil = 2c
    assert fee(50, 1) == 2
    # 100 contracts at 50c: 0.07*100*0.25 = 1.75 -> 175c
    assert fee(50, 100) == 175
    # maker is 25% of taker
    assert fee(50, 100, maker=True) == 44  # ceil(1.75*100*0.25=43.75)
    # symmetric around 50
    assert fee(20, 100) == fee(80, 100)
    print("ok: fee schedule (taker/maker/rounding/symmetry)")


def _cfg(tmp):
    return {
        "paper_mode": True,
        "fees": {"rate_taker": 0.07, "rate_maker": 0.0175},
        "risk": {"bankroll": 100.0, "max_position_per_market": 50,
                 "max_total_exposure": 100.0, "max_orders_per_min": 100},
        "engine": {"default_order_size": 10, "min_edge_cents": 1},
    }


def test_paper_broker_fill_and_caps():
    tmp = tempfile.mkdtemp()
    cfg = _cfg(tmp)
    fee = make_fee_fn(cfg)
    pb = PaperBroker(cfg, fee, tmp)
    b = _book(yes=[[40, 100]], no=[[56, 100]])  # yes ask = 44
    f = pb.execute(b, "yes", "buy", 10)
    assert f.accepted and f.filled_size == 10 and f.sim_fill_price_cents == 44
    pos = pb.positions["KXWC-TEST"]
    assert pos.yes.qty == 10
    # cash debited cost + fee
    assert pb.cash_cents == 10000 - (10 * 44) - f.fee_cents

    # position cap
    big = pb.execute(b, "yes", "buy", 60)
    assert not big.accepted and "position_cap" in big.reason

    # kill switch
    open(os.path.join(tmp, "STOP"), "w").close()
    killed = pb.execute(b, "yes", "buy", 1)
    assert not killed.accepted and "kill_switch" in killed.reason
    os.remove(os.path.join(tmp, "STOP"))
    print("ok: paper broker fill + position cap + kill switch")


def test_paper_broker_settlement():
    tmp = tempfile.mkdtemp()
    cfg = _cfg(tmp)
    pb = PaperBroker(cfg, make_fee_fn(cfg), tmp)
    b = _book(yes=[[40, 100]], no=[[56, 100]])  # buy yes @44
    pb.execute(b, "yes", "buy", 10)
    info = pb.settle("KXWC-TEST", "yes")
    # 10 contracts pay 100 each, bought at 44 -> +56c each = 560c gross of entry fee
    assert info["settled_pnl_cents"] == 10 * (100 - 44)
    print("ok: settlement payoff")


def _pm_cfg(min_edge=0, price_exits=True, take_profit_enabled=False):
    return {"engine": {"default_order_size": 10, "min_edge_cents": min_edge},
            "position_manager": {"take_profit_cents": 8, "stop_loss_cents": 12,
                                 "min_hold_ticks": 1, "price_exits_enabled": price_exits,
                                 "take_profit_enabled": take_profit_enabled}}


def _wired_engine(min_edge=0, price_exits=True, take_profit_enabled=False):
    """RulesEngine whose sink simulates main's flow: records requests AND feeds
    the actual fill back via on_fill (entry price = decision/sellable price)."""
    fee = make_fee_fn({"fees": {"rate_taker": 0.07, "rate_maker": 0.0175}})
    fired = []
    holder = {}

    def sink(req, book):
        fired.append(req)
        e = holder["eng"]
        if req.action == "buy":
            e.on_fill(book.market_ticker, req.side, "buy",
                      float(req.decision_price_cents), req.size)
        else:
            pos = e.positions.get(book.market_ticker)
            held = pos.qty if pos else 0
            sz = max(0, int(round(held * (req.sell_fraction or 1.0))))
            e.on_fill(book.market_ticker, req.side, "sell",
                      float(book.sellable_price(req.side)), sz)

    eng = RulesEngine(_pm_cfg(min_edge, price_exits, take_profit_enabled), sink, fee)
    holder["eng"] = eng
    return eng, fired


def _thesis(limit, edge, rules):
    return Thesis(thesis_id="t", market_ticker="KXWC-TEST", side="yes",
                  limit_price_cents=limit, edge_cents=edge, exit_rules=rules,
                  llm_start_wall=0, llm_end_wall=0, llm_latency_ms=100,
                  trigger_event_monotonic=0, trigger_event_wall=0, trigger="pregame")


def test_rules_entry_then_take_profit():
    eng, fired = _wired_engine(min_edge=2)
    eng.set_thesis(_thesis(45, 5, []))
    b = _book(yes=[[40, 100]], no=[[56, 100]])     # yes ask = 44
    eng.on_tick(b, 1.0)                            # entry @44, entry_tick=1
    assert fired and fired[0].action == "buy" and "KXWC-TEST" in eng.positions
    b2 = _book(yes=[[60, 100]], no=[[38, 100]])    # yes bid 60 -> +16c, clears fees
    eng.on_tick(b2, 2.0)
    assert any(r.action == "sell" and r.is_exit and r.trigger_label == "take_profit"
               for r in fired)
    print("ok: entry then fee-cleared take_profit")


def test_take_profit_fires_with_stoploss_off():
    """Reaction-capture: take-profit fires on a spike even with price_exits
    (stop-loss) OFF — and a price DIP still does NOT sell."""
    eng, fired = _wired_engine(min_edge=2, price_exits=False, take_profit_enabled=True)
    eng.set_thesis(_thesis(45, 5, []))
    b = _book(yes=[[40, 100]], no=[[56, 100]])     # yes ask 44 -> entry @44
    eng.on_tick(b, 1.0)
    assert "KXWC-TEST" in eng.positions
    b2 = _book(yes=[[60, 100]], no=[[38, 100]])    # yes bid 60 -> +16c, clears fees
    eng.on_tick(b2, 2.0)
    assert any(r.action == "sell" and r.is_exit and r.trigger_label == "take_profit"
               for r in fired), "take-profit must fire on a spike even with stop-loss off"
    print("ok: take-profit fires on a spike with stop-loss OFF")


def test_take_profit_on_dip_does_not_sell():
    """With take-profit ON but stop-loss OFF, a price crater must NOT sell (no
    accidental stop-loss)."""
    eng, fired = _wired_engine(min_edge=2, price_exits=False, take_profit_enabled=True)
    eng.set_thesis(_thesis(45, 5, []))
    b = _book(yes=[[40, 100]], no=[[56, 100]])     # entry @44
    eng.on_tick(b, 1.0)
    b2 = _book(yes=[[30, 100]], no=[[40, 100]])    # sellable craters to 30 (−14c)
    eng.on_tick(b2, 2.0)
    eng.on_tick(b2, 3.0)
    assert not [r for r in fired if r.action == "sell"], "take-profit on must not add a stop-loss"
    print("ok: take-profit on, a dip still does not sell (no stop-loss)")


def test_no_same_tick_roundtrip_and_fee_gate():
    eng, fired = _wired_engine(min_edge=0)
    eng.set_thesis(_thesis(77, 5, [{"event": "price<=76", "action": "sell_all", "reason": "x"}]))
    b = _book(yes=[[70, 100]], no=[[23, 100]])     # yes ask = 100-23 = 77
    eng.on_tick(b, 1.0)                             # entry @77 — NO exit same tick
    assert [r for r in fired if r.action == "buy"] and not [r for r in fired if r.action == "sell"]
    b2 = _book(yes=[[76, 100]], no=[[23, 100]])     # sellable 76: 1c adverse vs entry 77
    eng.on_tick(b2, 2.0)
    assert not [r for r in fired if r.action == "sell"], "1c adverse move must NOT sell"
    b3 = _book(yes=[[64, 100]], no=[[23, 100]])     # sellable 64: 13c adverse >= stop 12
    eng.on_tick(b3, 3.0)
    assert any(r.action == "sell" and r.trigger_label == "stop_loss" for r in fired)
    print("ok: no same-tick round trip + 1c blocked + real stop fires")


def test_rules_event_exit_red_card():
    eng, fired = _wired_engine(min_edge=0)
    eng.set_thesis(_thesis(99, 5,
                   [{"event": "red_card:Brazil", "action": "sell_half", "reason": "risk"}]))
    b = _book(yes=[[40, 100]], no=[[56, 100]])
    eng.on_tick(b, 1.0)                             # entry, entry_tick=1
    eng.on_tick(b, 2.0)                             # advance tick (clears min-hold)
    eng.on_score_event("red_card", "Brazil", 30.0, {"KXWC-TEST": b}, 2.0, 2.0)
    assert any(r.action == "sell" and r.sell_fraction == 0.5 and r.is_exit for r in fired)
    print("ok: event-keyed exit (red_card) fires from score feed")


def test_market_parse_and_thesis_dead():
    from engine.markets import parse_market, thesis_dead, ScoreView
    # team total: NO Croatia over 1.5 (CRO is away in PANCRO)
    spec = parse_market("KXWCTEAMTOTAL-26JUN23PANCRO-CRO2")
    assert spec.kind == "team_total" and spec.team_code == "CRO" and spec.line == 1.5
    assert spec.home_code == "PAN" and spec.away_code == "CRO"
    # at 0-0 the NO under is alive; a price dip is irrelevant (no score arg = no death)
    assert thesis_dead("no", spec, ScoreView(0, 0, 30, "live")) is None
    # Croatia scores its 2nd (away_goals=2 > 1.5) -> NO is dead
    assert thesis_dead("no", spec, ScoreView(0, 2, 70, "live")) is not None
    # game total NO Over 2.5 dies the moment total crosses 3
    gt = parse_market("KXWCTOTAL-26JUN23PANCRO-3")
    assert gt.kind == "game_total" and gt.line == 2.5
    assert thesis_dead("no", gt, ScoreView(2, 0, 60, "live")) is None
    assert thesis_dead("no", gt, ScoreView(2, 1, 61, "live")) is not None
    print("ok: market parsing + thesis_dead (score-state, not price)")


def test_price_dip_does_not_sell_winner():
    """The PORUZB-5 bug: YES Over 5.5 dips in price but the scoreline is unchanged.
    With price exits OFF (the default), NOTHING sells — we hold the winner."""
    eng, fired = _wired_engine(min_edge=0, price_exits=False)
    eng.set_thesis(_thesis(70, 5, []))            # buy yes if ask<=70
    b = _book(yes=[[60, 100]], no=[[37, 100]])    # yes ask = 100-37 = 63 <= 70 -> entry @63
    eng.on_tick(b, 1.0)
    assert "KXWC-TEST" in eng.positions
    # price craters to 51 (sellable) — a 12c "drawdown" — but score unchanged
    b2 = _book(yes=[[51, 100]], no=[[40, 100]])
    eng.on_tick(b2, 2.0)
    eng.on_tick(b2, 3.0)
    assert not [r for r in fired if r.action == "sell"], "price dip must NOT sell a binary winner"
    print("ok: price dip with unchanged score does NOT sell (PORUZB-5 fixed)")


def test_score_state_exit_fires():
    eng, fired = _wired_engine(min_edge=0, price_exits=False)
    # NO Croatia over 1.5 in PANCRO
    th = Thesis(thesis_id="t", market_ticker="KXWCTEAMTOTAL-26JUN23PANCRO-CRO2",
                side="no", limit_price_cents=80, edge_cents=5, exit_rules=[],
                llm_start_wall=0, llm_end_wall=0, llm_latency_ms=100,
                trigger_event_monotonic=0, trigger_event_wall=0, trigger="pregame")
    eng.set_thesis(th)
    tk = "KXWCTEAMTOTAL-26JUN23PANCRO-CRO2"
    b = _book(yes=[[28, 500]], no=[[72, 500]], ticker=tk)   # no ask = 100-28 = 72
    eng.on_tick(b, 1.0)                                     # entry NO @72
    eng.on_tick(b, 2.0)                                     # advance past min-hold
    assert tk in eng.positions
    from engine.markets import ScoreView
    # Croatia scores its 2nd -> away_goals=2 > 1.5 -> thesis dead -> exit fires
    eng.check_thesis_death({tk: ScoreView(0, 2, 70, "live")}, {tk: b}, 3.0, 3.0)
    assert any(r.action == "sell" and r.is_exit and "thesis_dead" in r.trigger_label
               for r in fired)
    print("ok: score-state exit fires when the line is actually crossed")


def test_blind_gate_blocks_entry():
    eng, fired = _wired_engine(min_edge=0, price_exits=False)
    eng.blind_check = lambda ticker: True          # simulate WS down / stale score
    eng.set_thesis(_thesis(80, 5, []))
    b = _book(yes=[[40, 100]], no=[[56, 100]])     # would normally enter
    eng.on_tick(b, 1.0)
    assert not eng.positions and not fired, "blind gate must block entries"
    print("ok: blind gate blocks entries when state is unseeable")


def _no_total_thesis(tk):
    return Thesis(thesis_id="t", market_ticker=tk, side="no", limit_price_cents=80,
                 edge_cents=5, exit_rules=[{"event": "clock>=80", "action": "sell_all",
                                            "reason": "lock"}],
                 llm_start_wall=0, llm_end_wall=0, llm_latency_ms=100,
                 trigger_event_monotonic=0, trigger_event_wall=0, trigger="pregame")


def test_clear_on_exit_no_double_fire():
    """EGYIRI-2 bug: a position re-fired its exit 8x. Clearing on first exit means
    a second rule / thesis_dead / re-check can never re-sell it."""
    from engine.markets import ScoreView
    eng, fired = _wired_engine(min_edge=0, price_exits=False)
    tk = "KXWCTOTAL-26JUN26EGYIRI-2"                       # NO Over 1.5
    eng.set_thesis(_no_total_thesis(tk))
    b = _book(yes=[[20, 500]], no=[[80, 500]], ticker=tk)  # no ask = 100-20 = 80
    eng.on_tick(b, 1.0)                                    # entry NO @80
    eng.on_tick(b, 2.0)                                    # past min-hold
    assert tk in eng.positions
    sv = ScoreView(1, 1, 85, "live")                       # total 2 > 1.5 AND clock>=80
    eng.on_score_event("goal", "Iran", 85.0, {tk: b}, 3.0, 3.0)   # clock>=80 fires -> pops
    eng.check_thesis_death({tk: sv}, {tk: b}, 3.0, 3.0)          # position gone -> no 2nd sell
    eng.check_thesis_death({tk: sv}, {tk: b}, 4.0, 4.0)
    sells = [r for r in fired if r.action == "sell"]
    assert len(sells) == 1, f"expected exactly 1 exit, got {len(sells)}"
    assert tk not in eng.positions
    print("ok: clear-on-exit — exactly one exit, no re-fire")


def test_final_status_skips_exit():
    """Don't sim-sell into a settling book (the NO-TIE @96c phantom). On a final
    state the position is held for the settlement poll to pay at true 0/100."""
    from engine.markets import ScoreView
    eng, fired = _wired_engine(min_edge=0, price_exits=False)
    tk = "KXWCTOTAL-26JUN26EGYIRI-2"
    eng.set_thesis(_no_total_thesis(tk))
    b = _book(yes=[[20, 500]], no=[[80, 500]], ticker=tk)
    eng.on_tick(b, 1.0); eng.on_tick(b, 2.0)
    sv = ScoreView(1, 1, 90, "final")                     # thesis is dead but game is FINAL
    eng.check_thesis_death({tk: sv}, {tk: b}, 3.0, 3.0)
    assert not [r for r in fired if r.action == "sell"], "must not sim-sell on final"
    assert tk in eng.positions, "held for settlement, not sold into stale book"
    print("ok: final status skips sim-sell (settlement pays out)")


def test_live_sizing_and_order_builder():
    from engine.live_broker import size_for_balance, build_v2_order
    # $20 balance, 5% per clip, 25c price -> $1.00 / 0.25 = 4 contracts
    assert size_for_balance(2000, 25, 0.05) == 4.0
    # $20, 5%, 80c -> $1 / 0.80 = 1.25 contracts
    assert size_for_balance(2000, 80, 0.05) == 1.25
    # floor at min 1 contract
    assert size_for_balance(2000, 99, 0.01) == 1.0
    # YES buy -> bid at the yes price
    b = build_v2_order("KXWCTOTAL-26JUN27CROGHA-2", "yes", "buy", 23, 4.0)
    assert b["side"] == "bid" and b["price"] == "0.2300" and b["count"] == "4.00"
    assert b["time_in_force"] == "immediate_or_cancel"
    # YES sell -> ask at the yes price
    s = build_v2_order("KXWCTOTAL-26JUN27CROGHA-2", "yes", "sell", 99, 4.0)
    assert s["side"] == "ask" and s["price"] == "0.9900"
    # NO buy at 30c -> SELL YES at 70c (the critical mapping)
    nb = build_v2_order("KXWCTOTAL-26JUN27CROGHA-2", "no", "buy", 30, 4.0)
    assert nb["side"] == "ask" and nb["price"] == "0.7000", nb
    # NO sell at 96c -> BUY YES at 4c
    ns = build_v2_order("KXWCTOTAL-26JUN27CROGHA-2", "no", "sell", 96, 4.0)
    assert ns["side"] == "bid" and ns["price"] == "0.0400", ns
    print("ok: live sizing + V2 order builder (YES + NO->inverse-YES mapping)")


if __name__ == "__main__":
    test_orderbook_top_and_normalization()
    test_orderbook_delta()
    test_walk_crosses_spread()
    test_fees_formula()
    test_paper_broker_fill_and_caps()
    test_paper_broker_settlement()
    test_rules_entry_then_take_profit()
    test_take_profit_fires_with_stoploss_off()
    test_take_profit_on_dip_does_not_sell()
    test_no_same_tick_roundtrip_and_fee_gate()
    test_rules_event_exit_red_card()
    test_market_parse_and_thesis_dead()
    test_price_dip_does_not_sell_winner()
    test_score_state_exit_fires()
    test_blind_gate_blocks_entry()
    test_live_sizing_and_order_builder()
    test_clear_on_exit_no_double_fire()
    test_final_status_skips_exit()
    print("\nAll core tests passed.")
