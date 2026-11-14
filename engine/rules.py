"""Deterministic rules engine + position manager — the millisecond hot path.

NO LLM here. It consumes the thesis + machine-readable exit_rules the analyst
already produced, opens an entry when the thesis is actionable, and then ACTIVELY
MANAGES every open position on each tick + verified-score event:

  * standing config risk rules: take_profit_cents / stop_loss_cents (hard),
  * the analyst's per-trade exit_rules (price>=/<=, goal/red/clock),

with these guarantees (the bugs the last live run exposed):

  1. NO same-tick round trip. A position cannot be evaluated for exit until at
     least `min_hold_ticks` after the tick it was entered on.
  2. Triggers compare against the ACTUAL recorded entry fill price (set via
     on_fill), never a stale snapshot.
  3. take_profit / profit exits only fire when sellable - entry clears the
     round-trip fee. A 1-cent adverse move can NOT trigger a sell.
  4. goal/red exits fire from the verified score feed on detection; the score
     event's receive-stamp rides along so the recorder can log the sell gap.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from data.orderbook import OrderBook
from engine.markets import ScoreView, parse_market, thesis_dead


@dataclass
class Thesis:
    thesis_id: str
    market_ticker: str
    side: str                  # yes | no  (side we'd hold)
    limit_price_cents: int
    edge_cents: int
    exit_rules: list           # [{event, action, reason}]
    llm_start_wall: Optional[float]
    llm_end_wall: Optional[float]
    llm_latency_ms: Optional[float]
    trigger_event_monotonic: float
    trigger_event_wall: float
    trigger: str
    entered: bool = False
    lane: str = "edge"        # 'edge' (needs edge >= min_edge) | 'reaction'
                              # (fair-priced setup with event-keyed exits; no edge required)
    fair_source: str = "analyst"   # sharp | ladder | analyst


@dataclass
class OpenPosition:
    ticker: str
    side: str
    entry_price_cents: int     # ACTUAL fill price (from on_fill)
    qty: int
    entry_tick: int            # tick counter at entry (min-hold guard)
    entry_wall: float
    exit_rules: list
    thesis_id: str
    fired_events: set = field(default_factory=set)


@dataclass
class DecisionRequest:
    thesis: Optional[Thesis]
    side: str
    action: str                # buy | sell
    size: int
    decision_price_cents: Optional[int]
    reason: str
    event_monotonic: float
    event_wall: float
    decision_monotonic: float
    decision_wall: float
    sell_fraction: Optional[float] = None
    # exit bookkeeping (None for entries)
    is_exit: bool = False
    trigger_label: Optional[str] = None       # e.g. stop_loss | take_profit | rule:price<=40 | rule:red_card
    reference_price_cents: Optional[int] = None
    entry_price_cents: Optional[int] = None
    seconds_held: Optional[float] = None
    score_event_recv_monotonic: Optional[float] = None  # for sell-gap logging


class RulesEngine:
    def __init__(self, cfg: dict, decision_sink: Callable[["DecisionRequest", OrderBook], None],
                 fee_fn: Optional[Callable[..., int]] = None):
        self.sink = decision_sink
        self.fee_fn = fee_fn or (lambda p, c, **k: 0)
        eng = cfg["engine"]
        self.default_size = int(eng["default_order_size"])
        self.min_edge = int(eng.get("min_edge_cents", 0))
        pm = cfg.get("position_manager", {})
        self.take_profit = int(pm.get("take_profit_cents", 8))
        self.stop_loss = int(pm.get("stop_loss_cents", 12))
        self.min_hold_ticks = int(pm.get("min_hold_ticks", 1))
        # Price-based stops are STRUCTURALLY WRONG on binary event contracts that
        # settle on the final score (a price wobble is not new info). Off by
        # default; downside protection is the score-state exit (check_thesis_death)
        # plus position sizing. Set true only for non-binary/continuous markets.
        self.price_exits_enabled = bool(pm.get("price_exits_enabled", False))
        # Take-profit (locking a WINNER into a spike / longshot reversion) is a valid
        # price exit and fires on its own flag — the stop-loss does NOT (a dip is noise
        # on a binary). Back-compat: price_exits_enabled also enables take-profit.
        self.take_profit_enabled = bool(pm.get("take_profit_enabled", False))
        self.late_minute = int(pm.get("moneyline_late_minute", 80))
        # Blind gate: do nothing (no entries, no price exits) when we can't see state.
        self.blind_check: Callable[[str], bool] = lambda ticker: False
        self.thesis_by_ticker: dict[str, Thesis] = {}
        self.positions: dict[str, OpenPosition] = {}
        self.tick = 0

    # ---- thesis + fill feedback ---------------------------------------
    def set_thesis(self, thesis: Thesis) -> None:
        self.thesis_by_ticker[thesis.market_ticker] = thesis

    def on_fill(self, ticker: str, side: str, action: str, fill_price: float,
                qty: int) -> None:
        """Called by the sink with the ACTUAL fill so the manager tracks the real
        entry price and entry tick."""
        if qty <= 0:
            return
        if action == "buy":
            th = self.thesis_by_ticker.get(ticker)
            pos = self.positions.get(ticker)
            if pos and pos.side == side:
                # average in
                new_qty = pos.qty + qty
                pos.entry_price_cents = int(round(
                    (pos.entry_price_cents * pos.qty + fill_price * qty) / new_qty))
                pos.qty = new_qty
            else:
                self.positions[ticker] = OpenPosition(
                    ticker=ticker, side=side, entry_price_cents=int(round(fill_price)),
                    qty=qty, entry_tick=self.tick, entry_wall=time.time(),
                    exit_rules=(th.exit_rules if th else []),
                    thesis_id=(th.thesis_id if th else ""))
        else:  # sell
            pos = self.positions.get(ticker)
            if pos:
                pos.qty -= qty
                if pos.qty <= 0:
                    self.positions.pop(ticker, None)

    # ---- fee-aware exit gate ------------------------------------------
    def _round_trip_fee(self, entry: int, sellable: int, qty: int, ticker: str) -> int:
        return (self.fee_fn(entry, qty, ticker=ticker)
                + self.fee_fn(sellable, qty, ticker=ticker))

    def _clears_fees(self, entry: int, sellable: int, qty: int, ticker: str) -> bool:
        return (sellable - entry) * qty > self._round_trip_fee(entry, sellable, qty, ticker)

    # ---- hot path: per tick -------------------------------------------
    def on_tick(self, book: OrderBook, recv_monotonic: float) -> None:
        self.tick += 1
        ticker = book.market_ticker

        # 1) ENTRY (once) — exits are NOT considered on the entry tick.
        # Edge lane needs edge >= min_edge; the reaction lane enters on its
        # own criteria (fair price + event-keyed exits), verified upstream.
        th = self.thesis_by_ticker.get(ticker)
        if th and not th.entered and (th.lane == "reaction"
                                      or th.edge_cents >= self.min_edge):
            if self.blind_check(ticker):
                return  # blind: no entries while we can't see state
            ask = book.best_yes_ask if th.side == "yes" else book.best_no_ask
            if ask is not None and ask <= th.limit_price_cents:
                th.entered = True
                self._emit_entry(th, book, ask, recv_monotonic)
                return

        # 2) Active per-tick management. Downside protection is the SCORE-STATE exit
        # (check_thesis_death), never a price dip. But TAKE-PROFIT — locking a winner
        # into a spike / longshot reversion — is a valid price exit and runs on its
        # own flag (the reaction-capture). The STOP-LOSS stays behind
        # price_exits_enabled because a dip on a binary is noise, not information.
        pos = self.positions.get(ticker)
        if pos is None:
            return
        if self.tick - pos.entry_tick < self.min_hold_ticks:
            return  # min-hold: no same-tick (or too-soon) round trip
        if self.blind_check(ticker):
            return
        sellable = book.sellable_price(pos.side)
        if sellable is None:
            return
        entry = pos.entry_price_cents

        # TAKE-PROFIT: sell the winner once the gain clears the round-trip fee.
        if ((self.take_profit_enabled or self.price_exits_enabled)
                and sellable - entry >= self.take_profit
                and self._clears_fees(entry, sellable, pos.qty, ticker)):
            self._emit_exit(pos, book, 1.0, "take_profit", sellable, recv_monotonic)
            return

        # Legacy STOP-LOSS + analyst price-keyed rules: OFF unless explicitly enabled.
        if not self.price_exits_enabled:
            return
        if entry - sellable >= self.stop_loss:
            self._emit_exit(pos, book, 1.0, "stop_loss", sellable, recv_monotonic)
            return
        # analyst price-keyed rules
        for rule in pos.exit_rules:
            ev = rule.get("event", "")
            m = re.match(r"price(>=|<=)(\d+)", ev)
            if not m:
                continue
            op, val = m.group(1), int(m.group(2))
            if op == ">=" and sellable >= val:
                if self._clears_fees(entry, sellable, pos.qty, ticker):  # profit side
                    self._fire_rule(pos, book, rule, sellable, recv_monotonic, ev)
                    return
            elif op == "<=" and sellable <= val:
                # loss side: only honor it as a real stop, never a fee-noise blip
                if entry - sellable >= self.stop_loss:
                    self._fire_rule(pos, book, rule, sellable, recv_monotonic, ev)
                    return

    # ---- score-state exits (the real downside protection) -------------
    def check_thesis_death(self, score_by_ticker: dict, books: dict,
                           recv_monotonic: float, wall: float) -> None:
        """For every open position, exit ONLY if the verified score state has
        invalidated the thesis (the under crossed, team losing late, final
        against us). Called whenever the verified score updates — a fresh score
        is required by construction, so this is never 'blind'."""
        for ticker, pos in list(self.positions.items()):
            book = books.get(ticker)
            s: ScoreView = score_by_ticker.get(ticker)
            if book is None or s is None:
                continue
            if s.status == "final":
                # Game is over: do NOT sim-sell into a stale/settling book (that's
                # how a doomed contract "sold" at 96c). Let the settlement poll pay
                # it out at true 0/100.
                continue
            reason = thesis_dead(pos.side, parse_market(ticker), s, self.late_minute)
            if reason:
                sellable = book.sellable_price(pos.side)
                self._emit_exit(pos, book, 1.0, f"thesis_dead:{reason}", sellable,
                                recv_monotonic, score_recv=recv_monotonic, score_wall=wall)

    # ---- verified score events (fast, off the WS tick path) -----------
    def on_score_event(self, kind: str, team: Optional[str], minute: Optional[float],
                       books: dict[str, OrderBook], recv_monotonic: float,
                       wall: float) -> None:
        if kind == "final":
            return  # game over: no sim-sells into a settling book; settlement pays out
        for ticker, pos in list(self.positions.items()):
            book = books.get(ticker)
            if book is None:
                continue
            if self.tick - pos.entry_tick < self.min_hold_ticks:
                continue
            sellable = book.sellable_price(pos.side)
            for rule in pos.exit_rules:
                ev = rule.get("event", "")
                if ev in pos.fired_events:
                    continue
                if self._event_matches(ev, kind, team, minute):
                    pos.fired_events.add(ev)
                    self._fire_rule(pos, book, rule, sellable, recv_monotonic, ev,
                                    score_recv=recv_monotonic, score_wall=wall)
                    break

    @staticmethod
    def _event_matches(rule_event: str, kind: str, team: Optional[str],
                       minute: Optional[float]) -> bool:
        def team_match(spec: str) -> bool:
            if team is None:
                return False
            spec, t = spec.strip().lower(), team.lower()
            return spec in t or t in spec
        if rule_event.startswith("goal_for:") and kind == "goal":
            return team_match(rule_event.split(":", 1)[1])
        if rule_event.startswith("goal_against:") and kind == "goal":
            return team_match(rule_event.split(":", 1)[1])
        if rule_event.startswith("red_card:") and kind == "red_card":
            return team_match(rule_event.split(":", 1)[1])
        m = re.match(r"clock>=(\d+)", rule_event)
        if m and minute is not None:
            return minute >= int(m.group(1))
        return False

    # ---- emission ------------------------------------------------------
    def _emit_entry(self, th: Thesis, book: OrderBook, ask: int,
                    recv_monotonic: float) -> None:
        self.sink(DecisionRequest(
            thesis=th, side=th.side, action="buy", size=self.default_size,
            decision_price_cents=ask,
            reason=f"entry @ ask {ask} <= limit {th.limit_price_cents}",
            event_monotonic=th.trigger_event_monotonic, event_wall=th.trigger_event_wall,
            decision_monotonic=time.monotonic(), decision_wall=time.time(),
        ), book)

    def _fire_rule(self, pos: OpenPosition, book: OrderBook, rule: dict,
                   sellable: Optional[int], recv_monotonic: float, label: str,
                   score_recv: Optional[float] = None,
                   score_wall: Optional[float] = None) -> None:
        action = rule.get("action")
        if action == "hold":
            return
        if action == "add":
            self.sink(DecisionRequest(
                thesis=self.thesis_by_ticker.get(pos.ticker), side=pos.side, action="buy",
                size=self.default_size, decision_price_cents=sellable,
                reason=f"rule:{label} add", event_monotonic=recv_monotonic,
                event_wall=(score_wall or time.time()),
                decision_monotonic=time.monotonic(), decision_wall=time.time(),
            ), book)
            return
        frac = 1.0 if action == "sell_all" else 0.5
        self._emit_exit(pos, book, frac, f"rule:{label}", sellable, recv_monotonic,
                        score_recv=score_recv, score_wall=score_wall)

    def _emit_exit(self, pos: OpenPosition, book: OrderBook, frac: float, label: str,
                   reference_price: Optional[int], recv_monotonic: float,
                   score_recv: Optional[float] = None,
                   score_wall: Optional[float] = None) -> None:
        # Clear the position from the manager on the FIRST exit so a second rule
        # (or thesis_dead, or a partial fill that left contracts behind) can never
        # re-fire on it. The sink still sells the real broker quantity.
        self.positions.pop(pos.ticker, None)
        self.sink(DecisionRequest(
            thesis=self.thesis_by_ticker.get(pos.ticker), side=pos.side, action="sell",
            size=0, sell_fraction=frac, decision_price_cents=reference_price,
            reason=label, event_monotonic=(score_recv or recv_monotonic),
            event_wall=(score_wall or time.time()),
            decision_monotonic=time.monotonic(), decision_wall=time.time(),
            is_exit=True, trigger_label=label, reference_price_cents=reference_price,
            entry_price_cents=pos.entry_price_cents,
            seconds_held=time.time() - pos.entry_wall,
            score_event_recv_monotonic=score_recv,
        ), book)
