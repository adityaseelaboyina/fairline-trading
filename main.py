"""Live Kalshi paper-trading harness for World Cup soccer markets.

    python main.py --config config.yaml

paper_mode is the hard default — NO real orders are placed anywhere in this
build. The LLM analyst runs OFF the hot path (pre-game + verified state changes
+ slow dead-time refresh); a deterministic rules engine evaluates triggers on
every WS tick; the paper broker simulates fills against the live book; and the
SQLite recorder captures the full timing chain for every decision.

Wiring:
    WS ticks ---------> RulesEngine.on_tick  ---> sink ---> PaperBroker + Recorder
    score events -----> RulesEngine.on_score_event + analyst triggers
    analyst (async) --> installs Thesis into RulesEngine
    REST poll --------> settlement -> PaperBroker.settle + Recorder
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import signal
import time
import uuid
from pathlib import Path

import yaml

from data.discover import discover
from data.kalshi_auth import KalshiSigner
from data.kalshi_rest import KalshiREST
from data.kalshi_ws import KalshiWS
from data.orderbook import OrderBook
from data.score_feed import ScoreEvent, ScoreFeed
from data.sharp_feed import SharpFeed
from engine.analyst import Analyst
from engine.fees import make_fee_fn
from engine.live_broker import build_v2_order, size_for_balance
from engine.paper_broker import PaperBroker
from engine.rules import DecisionRequest, RulesEngine, Thesis
from engine.sniper import SpikeDetector, class_a_scan, class_b_scan
from tradelog.recorder import Recorder


def estimate_minute(state) -> float | None:
    """Best-effort match minute from ESPN period + displayClock. Handles extra time:
    P1 base 0, P2 base 45, P3(ET1) base 90, P4(ET2) base 105, P5+ (shootout) -> 120.
    Returns None for pregame/unknown period. Capped at 130 (fixes the old
    (period-1)*45 bug that mapped ET clocks to minute 240+)."""
    p = state.period or 0
    if p >= 5:
        return 120.0
    base = {1: 0, 2: 45, 3: 90, 4: 105}.get(p)
    if base is None:            # pregame / unknown period
        return None
    nums = re.findall(r"\d+", state.clock or "")
    if not nums:
        return float(base)
    mins = int(nums[0])
    # trust an absolute clock (>= base); otherwise it's minutes-into-period
    return min(130.0, float(mins if mins >= base else base + mins))


SAFETY_GATE_KEYS = ("max_plausible_edge_cents", "min_entry_price_cents",
                    "max_entry_price_cents", "max_contracts_per_clip",
                    "max_entry_minute", "max_positions_per_game")


def require_safety_gates(cfg: dict) -> None:
    """Startup contract: live mode REFUSES to start if any safety gate is
    missing or disabled (<=0). Raises SystemExit naming the offending gate."""
    sg = cfg.get("safety_gates", {}) or {}
    for k in SAFETY_GATE_KEYS:
        v = sg.get(k)
        if v is None or (isinstance(v, (int, float)) and v <= 0):
            raise SystemExit(
                f"[SAFETY] live mode refuses to start: safety gate '{k}' is missing or "
                f"disabled in config.yaml `safety_gates`. These are safety gates, not "
                f"tunables — set it (>0) and restart.")


class Harness:
    def __init__(self, cfg: dict, dry_run: bool = False, live: bool = False):
        self.cfg = cfg
        self.run_dir = cfg.get("run_dir", "./run")
        Path(self.run_dir).mkdir(parents=True, exist_ok=True)
        # dry-run: print the exact V2 order body we WOULD send (sized off the real
        # balance), but execute paper fills and send nothing.
        # live: place REAL orders (gated by config + --arm).
        self.dry_run = dry_run
        self.live = live
        self.live_broker = None
        livecfg = cfg.get("live", {})
        self.live_fraction = float(livecfg.get("bankroll_fraction", 0.05))
        self.max_clip_dollars = float(livecfg.get("max_clip_dollars", 2.0))
        self.cross_buffer_cents = int(livecfg.get("cross_buffer_cents", 2))
        self.max_game_fraction = float(livecfg.get("max_game_fraction", 0.0))
        # Reaction lane: smaller clip, capped setups per game
        self.reaction_clip_dollars = float(livecfg.get("reaction_clip_dollars", 3.0))
        self.reaction_max_per_game = int(livecfg.get("reaction_max_per_game", 2))
        self._reaction_opens: dict[str, int] = {}   # game code -> reaction entries filled
        # ---- Sniper lane, SHADOW-FIRST ----
        sn = cfg.get("sniper", {}) or {}
        self.snipe_enabled = bool(sn.get("enabled", False))     # NEVER default-on
        # per-class arming: P1 (Class A) and P2 (Class B) are separate phase gates,
        # so each class arms independently; legacy `enabled` arms both.
        self.snipe_a_enabled = bool(sn.get("class_a_enabled", self.snipe_enabled))
        self.snipe_b_enabled = bool(sn.get("class_b_enabled", self.snipe_enabled))
        self.snipe_shadow = bool(sn.get("shadow", True))        # shadow logs regardless
        self.snipe_clip_cents = int(round(float(sn.get("clip_dollars", 3.0)) * 100))
        self.snipe_a_max_price = int(sn.get("class_a_max_price", 93))
        self.snipe_a_max_markets = int(sn.get("class_a_max_markets_per_goal", 3))
        self.snipe_b_min_gap = int(sn.get("class_b_min_gap_cents", 10))
        self.snipe_b_max = int(sn.get("class_b_max_per_goal", 2))
        self.snipe_blacklist_s = float(sn.get("blacklist_seconds", 120))
        self.snipe_max_per_game = int(sn.get("max_per_game", 2))
        self._snipe_tickers: set = set()             # tickers currently held by the snipe lane
        self.spike_detector = SpikeDetector(
            n_markets=int(sn.get("spike_markets", 3)),
            move_cents=int(sn.get("spike_move_cents", 8)),
            window_s=float(sn.get("spike_window_s", 2.0)))
        self._seen_goal_keys: set = set()            # (event_id, team, new_total) dedup
        self._snipe_blacklist: dict[str, float] = {} # game code -> blacklist-until wall
        self._last_spike_wall: dict[str, float] = {} # game code -> last spike wall
        self._lambda_snap: dict[str, tuple] = {}     # game -> (wall, lam_home, lam_away)
        self._snipe_positions: dict[int, list] = {}  # goal_event_id -> [(ticker, side)]
        self._goal_ids: dict[tuple, int] = {}        # (event_id, team) -> latest goal id
        self._goal_game: dict[int, str] = {}         # goal_event_id -> game code
        # Pre-goal moneyline fairs (asymmetry tagging must use the PRE-goal fair,
        # never post-goal books). Refreshed with the lambda snapshots.
        self._ml_fair_snap: dict[str, tuple] = {}    # game -> (wall, {team_code: prob})
        # Execution: REST re-verify, dirty-book guard, maker routing (non-urgent only)
        ex = cfg.get("execution", {}) or {}
        self.rest_verify_orders = bool(ex.get("rest_verify_orders", True))
        self.maker_enabled = bool(ex.get("maker_enabled", True))
        self.maker_patience_s = float(ex.get("maker_patience_seconds", 45))
        self.block_on_dirty = bool(ex.get("block_on_dirty_book", True))  # feed-integrity
        # Sharp-book fair values (provider "none" -> the ladder model stays primary)
        self._game_meta: dict[str, tuple] = {}       # game -> (home, away, [names])
        self._game_root: dict[str, str] = {}         # game -> series root (KXWC/KXUCL/KXMLS)
        from engine.markets import parse_market, series_root
        for t in cfg.get("market_tickers", []):
            code = t.split("-")[1]
            self._game_root.setdefault(code, series_root(t.split("-")[0]))
            if code not in self._game_meta:
                try:
                    sp = parse_market(t)
                    self._game_meta[code] = (sp.home_code, sp.away_code, ["", ""])
                except Exception:  # noqa: BLE001
                    pass
        self.sharp = SharpFeed(cfg, self._game_meta, on_var=self._on_var_review)
        self.daily_loss_limit_cents = int(round(float(
            livecfg.get("daily_loss_limit_dollars", 10.0)) * 100))
        self.live_balance_cents = 0

        self.signer = KalshiSigner.from_config(cfg)
        self.rest = KalshiREST(cfg, self.signer)
        self.analyst = Analyst(cfg)
        self.fee_fn = make_fee_fn(cfg)
        self.broker = PaperBroker(cfg, self.fee_fn, self.run_dir)
        self.recorder = Recorder(self.run_dir)
        self.rules = RulesEngine(cfg, self._sink, self.fee_fn)
        self.ws = KalshiWS(cfg, self.signer, self._on_orderbook, self._on_ticker,
                           rest=self.rest)
        self.score = ScoreFeed(cfg, self._on_score_event)
        eng = cfg["engine"]
        self.min_edge_after_fees = int(eng.get("min_edge_after_fees_cents", 0))
        self.decided_moneyline = int(eng.get("decided_moneyline_cents", 95))
        self.min_edge_entry = int(eng.get("min_edge_cents", 9))   # edge-lane bar (unchanged)
        # ---- SAFETY GATES: live mode REFUSES to start without them ----
        if self.live:
            require_safety_gates(cfg)
        sg = cfg.get("safety_gates", {}) or {}
        self.max_plausible_edge = int(sg.get("max_plausible_edge_cents", 25))  # never 0/off
        self.gate_min_price = int(sg.get("min_entry_price_cents", 15))
        self.gate_max_price = int(sg.get("max_entry_price_cents", 90))
        self.gate_max_contracts = int(sg.get("max_contracts_per_clip", 150))
        self.gate_max_minute = int(sg.get("max_entry_minute", 75))
        self.gate_max_pos_per_game = int(sg.get("max_positions_per_game", 2))
        self.rules.blind_check = self._is_blind
        saf = cfg.get("safety", {})
        self.max_score_age_s = float(saf.get("max_score_age_s", 45))
        self.context = cfg.get("context", {}) or {}
        tl = cfg.get("totals_ladder", {}) or {}
        self.ladder_enabled = bool(tl.get("enabled", False))
        self.ladder_edge_mid = float(tl.get("edge_at_mid_cents", 9))
        self.ladder_edge_extreme = float(tl.get("edge_at_extreme_cents", 18))

        self.tickers = list(cfg["market_tickers"])
        # market_ticker -> espn event id  (and reverse)
        self.market_to_event = dict(cfg["score_feed"].get("event_map", {}))
        self.event_to_markets: dict[str, list[str]] = {}
        for m, e in self.market_to_event.items():
            self.event_to_markets.setdefault(str(e), []).append(m)

        self.latest_ticker: dict[str, dict] = {}
        self.latest_score: dict[str, object] = {}     # event_id -> ScoreState
        self.settled: set[str] = self.recorder.settled_tickers()  # seed from DB
        self.pregame_done: set[str] = set()            # event ids that have had a pregame scan
        self.last_analyst_latency_ms: float | None = None
        self.last_markets_scanned: int = 0
        self.ticks: int = 0                            # WS book updates processed
        self.decisions: int = 0                        # accepted simulated orders
        self.min_liquidity = int(cfg.get("min_liquidity_contracts", 50))
        self.heartbeat_seconds = int(cfg.get("heartbeat_seconds", 10))
        self._analyst_lock = asyncio.Lock()
        self._last_analyst_call: dict[str, float] = {}
        self._stop = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    # ===== hot path callbacks =========================================
    def _on_orderbook(self, book: OrderBook, recv_monotonic: float) -> None:
        # synchronous, microseconds — pure arithmetic in the rules engine
        self.ticks += 1
        self.rules.on_tick(book, recv_monotonic)
        # Book-spike pre-trigger. A repricing shock (>=3 markets of one
        # game jumping >=8c together) collapses score-poll latency to ~0 by forcing
        # an immediate ESPN fetch. It NEVER authorizes a trade by itself.
        mid = book.mid("yes")
        if mid is not None:
            parts = book.market_ticker.split("-")
            game = parts[1] if len(parts) > 1 else ""
            fired = self.spike_detector.on_tick(book.market_ticker, game, mid, time.time())
            if fired and self._loop:
                self._last_spike_wall[fired] = time.time()
                print(f"[spike] repricing shock in {fired} -> forced score fetch")
                self._loop.create_task(self.score.poll_once())

    def _on_ticker(self, msg: dict, recv_monotonic: float) -> None:
        t = msg.get("market_ticker") or msg.get("market_id")
        if t:
            self.latest_ticker[t] = msg  # logged only; NEVER used as score truth

    # ===== decision sink (still off-network; sqlite write is local) ====
    def _sink(self, req: DecisionRequest, book: OrderBook) -> None:
        ticker = book.market_ticker
        if req.action == "sell":
            pos = self.broker.positions.get(ticker)
            held = 0
            if pos:
                held = pos.yes.qty if req.side == "yes" else pos.no.qty
            size = max(0, int(round(held * (req.sell_fraction or 1.0))))
            if size <= 0:
                return
        else:
            size = req.size
            if (self.dry_run or self.live) and req.decision_price_cents:
                # size the clip off the REAL balance (so it behaves like the funded
                # account, not the $1000 paper bankroll)
                lc = size_for_balance(self.live_balance_cents, req.decision_price_cents,
                                      self.live_fraction)
                size = max(1, int(lc))

        if self.live:
            # REAL MONEY: place asynchronously (sink is the sync hot path). Entries
            # and exits are already de-duped synchronously upstream, so scheduling
            # is safe. All position/P&L bookkeeping happens on the real fill.
            self._loop.create_task(self._place_live(req, book, size))
            return

        fill = self.broker.execute(book, req.side, req.action, size, req.decision_price_cents)
        th = req.thesis
        llm_start = th.llm_start_wall if th else None
        llm_end = th.llm_end_wall if th else None
        llm_lat = th.llm_latency_ms if th else None
        thesis_id = th.thesis_id if th else (req.trigger_label or "")
        decision_id = self.recorder.log_decision(
            thesis_id=thesis_id, trigger=req.reason, fill=fill,
            event_monotonic=req.event_monotonic, event_wall=req.event_wall,
            decision_monotonic=req.decision_monotonic, decision_wall=req.decision_wall,
            llm_start_wall=llm_start, llm_end_wall=llm_end, llm_latency_ms=llm_lat,
            lane=(th.lane if th else "edge"),
        )
        if fill.accepted:
            self.decisions += 1
            # tell the position manager the ACTUAL fill so exits compare to real entry
            self.rules.on_fill(ticker, req.side, req.action,
                               fill.sim_fill_price_cents, fill.filled_size)
            if req.is_exit:
                gap_ms = None
                if req.score_event_recv_monotonic is not None:
                    gap_ms = (req.decision_monotonic - req.score_event_recv_monotonic) * 1000.0
                entry = req.entry_price_cents or 0
                realized = int(round((fill.sim_fill_price_cents - entry) * fill.filled_size)) \
                    - fill.fee_cents
                self.recorder.log_exit(
                    ticker=ticker, thesis_id=thesis_id, entry_price_cents=entry,
                    trigger=req.trigger_label, reference_price_compared=req.reference_price_cents,
                    sim_fill_price_cents=fill.sim_fill_price_cents, size=fill.filled_size,
                    realized_pnl_cents=realized, seconds_held=req.seconds_held,
                    score_event_to_sell_gap_ms=gap_ms)
        tag = "OK " if fill.accepted else "REJ"
        extra = ""
        if req.is_exit and fill.accepted:
            extra = (f" entry={req.entry_price_cents} held={req.seconds_held:.0f}s"
                     + (f" gap={ (req.decision_monotonic - req.score_event_recv_monotonic)*1000:.0f}ms"
                        if req.score_event_recv_monotonic is not None else ""))
        print(f"[{'exit ' if req.is_exit else 'trade'}] {tag} {req.action.upper()} {req.side} "
              f"{ticker} req={size} fill={fill.filled_size} @ {fill.sim_fill_price_cents} "
              f"fee={fill.fee_cents}c :: {req.trigger_label or req.reason}{extra}")
        if self.dry_run and fill.accepted:
            px = int(req.decision_price_cents or fill.sim_fill_price_cents or 0)
            body = build_v2_order(ticker, req.side, req.action, px, float(fill.filled_size))
            print(f"   [DRY-RUN] WOULD PLACE: {req.action} {req.side} -> api_side={body['side']} "
                  f"{ticker} count={body['count']} price={body['price']} "
                  f"(~${fill.filled_size * px / 100:.2f}) tif={body['time_in_force']}")

        # price_drift sampler (entries with a measured LLM latency only)
        if fill.accepted and not req.is_exit and llm_lat and self._loop:
            self._loop.create_task(
                self._sample_drift(decision_id, ticker, req.side, llm_lat))

    async def _place_live(self, req: DecisionRequest, book: OrderBook, size: int) -> None:
        """REAL ORDER. Guards, place, then book the real fill into the same
        position model the exits/P&L use."""
        ticker = book.market_ticker
        # ---- hard guards (checked at placement time) ----
        if self.broker.kill_switch_engaged():
            print(f"[LIVE] BLOCKED {ticker}: STOP file present"); return
        if req.action == "buy" and self._is_blind(ticker):
            print(f"[LIVE] BLOCKED {ticker}: blind (WS down / stale score)"); return
        if self.broker.realized_cents() <= -self.daily_loss_limit_cents:
            print(f"[LIVE] BLOCKED {ticker}: daily loss limit "
                  f"${self.daily_loss_limit_cents/100:.2f} hit"); return
        # ---- Entry gates + contradiction breaker (BUYS only; exits exempt) ----
        if req.action == "buy":
            gate = self._entry_gates(req, ticker)
            if gate:
                print(f"[GATE] blocked buy {req.side} {ticker}: {gate}"); return
            contra = self._score_market_contradiction(ticker)
            if contra:
                print(f"[GATE] score/market contradiction {ticker}: {contra}")
                if self._loop:
                    self._loop.create_task(self.score.poll_once())  # force a fresh score fetch
                return
        # ---- feed-integrity: never trade off a stale (post-gap) book ----
        book = self.ws.books.get(ticker, book)
        if self.block_on_dirty and getattr(book, "dirty", False):
            print(f"[GATE] BLOCKED {req.action} {req.side} {ticker}: book dirty (WS resyncing)")
            return
        # ---- FRESH, MARKETABLE pricing ----
        # The local WebSocket book can drift from the exchange WITHOUT a sequence gap
        # (observed live: local asks of 13-42c while the exchange sat at 99c). So
        # every live order is priced off a fresh REST snapshot, not the local mirror.
        # Buys fail CLOSED if that fetch fails; sells fall back to the local book,
        # because refusing an exit is worse than imperfect exit pricing.
        if self.rest_verify_orders:
            try:
                fresh = await asyncio.wait_for(self.rest.get_orderbook(ticker),
                                               timeout=2.0)
                book.yes, book.no = fresh.yes, fresh.no
                book.dirty = False
                book.last_recv_monotonic = time.monotonic()
            except Exception as exc:  # noqa: BLE001
                if req.action == "buy":
                    print(f"[GATE] blocked buy {ticker}: REST verify failed ({exc!r})")
                    return
                print(f"[LIVE] REST verify failed on SELL {ticker} ({exc!r}) — "
                      f"proceeding on WS book (best-effort exit)")
        px = self._marketable_price(book, req.side, req.action, req)
        if px is None:
            kind = "ask" if req.action == "buy" else "bid"
            print(f"[LIVE] skip {req.action} {req.side} {ticker}: no crossable {kind} "
                  f"/ price ran past limit")
            return
        if req.action == "buy":
            # Verify against FRESH exchange state and FAIL CLOSED. The in-memory view
            # once went stale and let an oversized order past the concentration cap.
            try:
                positions = await asyncio.wait_for(self.rest.get_positions(), timeout=2.0)
                bal = await asyncio.wait_for(self.rest.get_balance_cents(), timeout=2.0)
            except Exception as exc:  # noqa: BLE001
                print(f"[GATE] blocked {ticker}: could not verify positions/balance "
                      f"({exc!r})"); return
            self.live_balance_cents = bal
            self.live_broker.balance_cents = bal
            lane = req.thesis.lane if req.thesis else "edge"
            clip = self.reaction_clip_dollars if lane == "reaction" else None
            size = self._live_count(px, clip)                # FRESH balance + lane clip
            code = ticker.split("-")[1] if "-" in ticker else ""
            cap = bal * self.max_game_fraction
            existing = self._game_cost_basis_cents(code, positions)
            room = cap - existing
            if room <= 0:
                print(f"[GATE] blocked {ticker}: per-game cap ${cap/100:.2f} for {code} "
                      f"full (fresh exposure ${existing/100:.2f})"); return
            size = min(size, int(room / px))
            size = min(size, self.gate_max_contracts)        # hard unit cap
        if size <= 0:
            return
        # ---- Fee-aware routing. Non-urgent entries post as maker first;
        # everything else (exits, urgent lanes) takes immediately.
        th0 = req.thesis
        lane0 = th0.lane if th0 else "edge"
        exec_style = "taker"
        resp = None
        if req.action == "buy" and self._maker_eligible(lane0, th0.trigger if th0 else ""):
            mresp, mstyle = await self._maker_then_cross(req, book, ticker, size)
            if mstyle == "abort":
                return
            if mresp is not None:
                resp, exec_style = mresp, "maker"
            else:
                exec_style = "maker_timeout_cross"
        if resp is None:
            try:
                resp = await self.live_broker.place(book, req.side, req.action, px,
                                                    float(size))
            except Exception as exc:  # noqa: BLE001
                print(f"[LIVE] ORDER FAILED {req.action} {req.side} {ticker}: {exc!r}")
                return
        th_lane = req.thesis.lane if req.thesis else "edge"
        self.recorder.log_order_response(
            lane=th_lane, ticker=ticker, side=req.side, order_px_cents=px,
            requested=size, resp=resp, book_was_dirty=getattr(book, "dirty", False))
        filled = float(resp.get("fill_count") or 0)
        order_id = resp.get("order_id")
        if filled <= 0:
            print(f"[LIVE] no fill {req.action} {req.side} {ticker} "
                  f"(IOC @{px}c, nothing at touch) order_id={order_id}")
            return
        filled_int = int(round(filled))
        yes_fill = float(resp.get("average_fill_price") or 0) * 100.0   # YES-perspective
        side_price = (100.0 - yes_fill) if req.side == "no" else yes_fill
        fee_cents = int(round(float(resp.get("average_fee_paid") or 0) * 100.0 * filled))
        # book the real fill into the position model so exits/P&L/settlement work
        fill = self.broker.record_fill(book, req.side, req.action, filled_int,
                                       side_price, fee_cents)
        self.rules.on_fill(ticker, req.side, req.action, side_price, filled_int)
        self.decisions += 1
        th = req.thesis
        thesis_id = th.thesis_id if th else (req.trigger_label or "")
        lane = th.lane if th else "edge"
        if req.action == "buy" and lane == "reaction":
            code = ticker.split("-")[1] if "-" in ticker else ""
            self._reaction_opens[code] = self._reaction_opens.get(code, 0) + 1
        decision_id = self.recorder.log_decision(
            thesis_id=thesis_id, trigger=req.reason, fill=fill,
            event_monotonic=req.event_monotonic, event_wall=req.event_wall,
            decision_monotonic=req.decision_monotonic, decision_wall=req.decision_wall,
            llm_start_wall=(th.llm_start_wall if th else None),
            llm_end_wall=(th.llm_end_wall if th else None),
            llm_latency_ms=(th.llm_latency_ms if th else None), lane=lane,
            fair_source=(th.fair_source if th else None), exec_style=exec_style)
        # Price-drift sampler: measure what analyst latency costs on live entries.
        llm_lat = th.llm_latency_ms if th else None
        if not req.is_exit and llm_lat and self._loop:
            self._loop.create_task(self._sample_drift(decision_id, ticker, req.side, llm_lat))
        if req.is_exit:
            entry = req.entry_price_cents or 0
            realized = int(round((side_price - entry) * filled_int)) - fee_cents
            self.recorder.log_exit(
                ticker=ticker, thesis_id=thesis_id, entry_price_cents=entry,
                trigger=req.trigger_label, reference_price_compared=req.reference_price_cents,
                sim_fill_price_cents=side_price, size=filled_int,
                realized_pnl_cents=realized, seconds_held=req.seconds_held,
                score_event_to_sell_gap_ms=None)
        # refresh balance for the next clip's sizing
        try:
            self.live_balance_cents = await self.rest.get_balance_cents()
            self.live_broker.balance_cents = self.live_balance_cents
        except Exception:  # noqa: BLE001
            pass
        print(f"[LIVE] FILLED [{lane}] {req.action} {req.side} {ticker} x{filled_int} @ "
              f"{side_price:.1f}c fee={fee_cents}c order_id={order_id} :: "
              f"{req.trigger_label or req.reason} | bal ${self.live_balance_cents/100:.2f}")

    def _maker_eligible(self, lane: str, trigger: str) -> bool:
        """Fee-aware routing for NON-urgent entries ONLY — edge/reaction
        lane on slow triggers (pregame / refresh). Snipes and event exits ALWAYS
        take: latency is the product, never risk the fill."""
        return (self.maker_enabled and lane in ("edge", "reaction")
                and trigger in ("pregame", "refresh"))

    async def _maker_then_cross(self, req: DecisionRequest, book: OrderBook,
                                ticker: str, size: int):
        """Post at the touch as maker (post-only GTC), wait maker_patience_seconds,
        then cancel and (if gates still pass) signal a taker cross. Returns
        (resp|None, style): style 'maker' = filled resting; 'cross' = timed out,
        caller crosses IOC; 'abort' = do nothing further (failed cancel / re-gate)."""
        from engine.live_broker import to_yes_order
        side = req.side
        join = book.best_yes_bid if side == "yes" else book.best_no_bid
        if join is None or join < self.gate_min_price:
            return None, "cross"                    # nothing sane to join
        api_side, yes_price = to_yes_order(side, "buy", join)
        try:
            # Per docs.kalshi.com/api-reference/orders/create-order-v2, time_in_force
            # is REQUIRED; a resting maker order is good_till_canceled + post_only.
            resp = await self.rest.place_order_v2(
                ticker=ticker, side=api_side, count=float(size),
                price_dollars=yes_price / 100.0, tif="good_till_canceled",
                stp="maker", post_only=True)
        except Exception as exc:  # noqa: BLE001 — maker unsupported/rejected -> taker
            print(f"[exec] maker post failed ({exc!r}) -> taker cross")
            return None, "cross"
        oid = resp.get("order_id")
        filled = float(resp.get("fill_count") or 0)
        if filled >= size:
            return resp, "maker"
        deadline = time.monotonic() + self.maker_patience_s
        poll = max(0.05, min(5.0, self.maker_patience_s / 5))
        avg = yes_price / 100.0
        fee = 0.0
        while time.monotonic() < deadline:
            await asyncio.sleep(poll)
            try:
                o = await self.rest.get_order(oid)
            except Exception:  # noqa: BLE001
                continue
            filled = float(o.get("fill_count") or 0)
            avg = float(o.get("average_fill_price") or avg)
            fee = float(o.get("average_fee_paid") or 0)
            if filled >= size or str(o.get("status", "")).lower() in (
                    "executed", "filled"):
                return {"order_id": oid, "fill_count": filled,
                        "average_fill_price": avg, "average_fee_paid": fee}, "maker"
        # timeout: cancel FIRST — an orphaned resting order is unacceptable
        try:
            await self.rest.cancel_order(oid)
        except Exception as exc:  # noqa: BLE001
            print(f"[exec] CANCEL FAILED {oid} ({exc!r}) — NOT crossing (double-fill "
                  f"risk); check the order manually")
            return None, "abort"
        if filled > 0:                              # partial while resting: keep it
            return {"order_id": oid, "fill_count": filled,
                    "average_fill_price": avg, "average_fee_paid": fee}, "maker"
        # staleness discipline: the 45s wait ages the setup — re-run the gates
        gate = self._entry_gates(req, ticker)
        if gate:
            print(f"[exec] maker timeout; cross blocked by gate: {gate}")
            return None, "abort"
        contra = self._score_market_contradiction(ticker)
        if contra:
            print(f"[exec] maker timeout; cross blocked: {contra}")
            return None, "abort"
        return None, "cross"

    def _marketable_price(self, book: OrderBook, side: str, action: str,
                          req: DecisionRequest) -> int | None:
        """Price an IOC to actually CROSS the current touch, in `side`'s own terms.
          buy : lift the ask (+buffer), CAPPED at the analyst limit so we never pay
                past our edge. Returns None if there's no ask or it ran past limit.
          sell: hit the bid (-buffer), floored at 1c. Returns None if no bid.
        """
        buf = self.cross_buffer_cents
        if action == "buy":
            ask = book.best_yes_ask if side == "yes" else book.best_no_ask
            if ask is None:
                return None
            limit = req.thesis.limit_price_cents if req.thesis else \
                int(req.decision_price_cents or ask)
            if ask > limit:
                return None  # price ran past our edge during the delay — skip
            return min(ask + buf, limit)
        bid = book.sellable_price(side)
        if bid is None:
            return None
        return max(1, bid - buf)

    def _live_count(self, price_cents: int, clip_dollars: float | None = None) -> int:
        """Whole contracts for one clip: bankroll_fraction of the real balance,
        hard-capped at the lane's clip (edge $5 / reaction $3), off the FRESH price."""
        if price_cents <= 0 or price_cents > self.live_balance_cents:
            return 0
        clip = self.max_clip_dollars if clip_dollars is None else clip_dollars
        budget = min(self.live_balance_cents * self.live_fraction, clip * 100.0)
        return max(1, int(budget / price_cents))

    def _game_cost_basis_cents(self, code: str, positions: list) -> int:
        """Total cost basis (cents) across all FRESH Kalshi positions in one game.
        Uses total_traded_dollars (= position_fp x avg), falling back to
        market_exposure_dollars. The concentration cap computes exposure from REAL
        positions, never from in-memory state, which can go stale."""
        total = 0.0
        for r in positions:
            if r.get("ticker", "").split("-")[1:2] != [code]:
                continue
            total += abs(float(r.get("total_traded_dollars")
                               or r.get("market_exposure_dollars") or 0))
        return round(total * 100)

    def _entry_gates(self, req: DecisionRequest, ticker: str) -> str | None:
        """Safety gates for a NEW buy. Returns a block-reason or None. Exits are
        never passed here. Price uses the analyst's intended entry (decision_price);
        minute uses the ticker's mapped-event verified score."""
        px = int(req.decision_price_cents or 0)
        if px < self.gate_min_price:
            return f"min_entry_price {px}c < {self.gate_min_price}c"
        if px > self.gate_max_price:
            return f"max_entry_price {px}c > {self.gate_max_price}c"
        # minute: block NEW in-play entries past max_entry_minute (or unknown minute
        # while in-play). Pregame / no-score -> allowed (nothing to be late for).
        st = self.latest_score.get(str(self.market_to_event.get(ticker)))
        if st is not None and st.state == "in":
            m = estimate_minute(st)
            if m is None or m > self.gate_max_minute:
                return f"max_entry_minute: in-play minute {m} > {self.gate_max_minute}"
        # positions-per-game cap (prevents stacking correlated bets on one game).
        # Snipe-lane positions are excluded — they carry their own budget (snipe
        # max_per_game) and are certainties, not directional stack risk.
        code = ticker.split("-")[1] if "-" in ticker else ""
        n = sum(1 for t, p in self.broker.positions.items()
                if t.split("-")[1:2] == [code] and p.net_contracts() != 0
                and t not in self._snipe_tickers)
        if n >= self.gate_max_pos_per_game:
            return f"max_positions_per_game: {n} already open in {code}"
        # Reaction-lane extras: tighter 20-80c band + per-game reaction cap
        # (counted WITHIN max_positions_per_game, not additional to it)
        th = getattr(req, "thesis", None)
        lane = th.lane if th else "edge"
        if lane == "reaction":
            if not 20 <= px <= 80:
                return f"reaction band: {px}c outside 20-80c"
            if self._reaction_opens.get(code, 0) >= self.reaction_max_per_game:
                return (f"reaction max_per_game: {self._reaction_opens[code]} "
                        f"already opened in {code}")
        return None

    def _score_market_contradiction(self, ticker: str) -> str | None:
        """Frozen-feed detector. The freshness gate can't catch a feed that was
        ALREADY stuck (before == now == wrong); the market can. Returns a block-reason
        if the verified score grossly contradicts live ML/total pricing. Conservative
        thresholds on purpose -> fires on frozen feeds, not on disagreement noise."""
        from engine.markets import parse_market
        eid = self.market_to_event.get(ticker)
        st = self.latest_score.get(str(eid)) if eid is not None else None
        if st is None or st.state != "in":
            return None
        minute = estimate_minute(st) or 0.0
        code = ticker.split("-")[1] if "-" in ticker else ""
        spec = parse_market(ticker)
        hcode, acode = spec.home_code, spec.away_code

        def ml_mid(team):
            b = self.ws.books.get(f"{self._root(code)}GAME-{code}-{team}")
            return b.mid("yes") if b else None

        diff = st.home_score - st.away_score
        # (a) a team leads by 2+ at >=60' but its moneyline yes_mid < 40c
        if minute >= 60 and abs(diff) >= 2:
            leader = hcode if diff > 0 else acode
            m = ml_mid(leader)
            if m is not None and m < 40:
                return f"{leader} leads by {abs(diff)} at {minute:.0f}' but ML mid {m:.0f}c<40c"
        # (b) level at >=60' but a team's moneyline yes_mid > 85c
        if minute >= 60 and diff == 0:
            for team in (hcode, acode):
                m = ml_mid(team)
                if m is not None and m > 85:
                    return f"level at {minute:.0f}' but {team} ML mid {m:.0f}c>85c"
        # (c) score says N total goals but the Over (N-0.5) market says < 10c
        total = st.home_score + st.away_score
        if total >= 1:
            b = self.ws.books.get(f"{self._root(code)}TOTAL-{code}-{total}")
            m = b.mid("yes") if b else None
            if m is not None and m < 10:
                return f"total {total} goals but Over {total-0.5} mid {m:.0f}c<10c"
        return None

    # ===== Sniper lane: deterministic, the LLM never touches this =====
    def _handle_goal(self, ev: ScoreEvent) -> None:
        """Confirmed score increase -> dedup'd goal_event row, then
        schedule the overshoot sampler (always) and the snipe scan (shadow always,
        live only if armed). Synchronous and cheap — heavy work is scheduled."""
        detect_wall = time.time()
        tickers = self.event_to_markets.get(ev.event_id, [])
        if not tickers:
            return
        game = tickers[0].split("-")[1]
        st = ev.state
        team_new = st.home_score if ev.team == st.home else st.away_score
        key = (ev.event_id, ev.team, team_new)
        if key in self._seen_goal_keys:
            return                      # poll + spike double-detection dedup
        self._seen_goal_keys.add(key)
        old_home = st.home_score - (1 if ev.team == st.home else 0)
        old_away = st.away_score - (1 if ev.team == st.away else 0)
        spike = self._last_spike_wall.get(game)
        if spike is not None and detect_wall - spike > 10:
            spike = None                # stale spike; not this goal's pre-trigger
        # Asymmetry shadow tags, from the PRE-goal ML fair snapshot (taken before
        # detect_wall by construction); post-goal books would flip the label.
        minute = estimate_minute(st)
        fav = None
        meta = self._game_meta.get(game)
        snap = self._ml_fair_snap.get(game)
        if meta and snap and snap[0] < detect_wall:
            scorer_code = meta[0] if ev.team == st.home else meta[1]
            p = snap[1].get(scorer_code)
            if p is not None:
                fav = int(p > 0.5)
        m = minute or 0.0
        bucket = "0-30" if m <= 30 else ("31-60" if m <= 60 else "61-90+")
        goal_id = self.recorder.log_goal_event(
            event_id=ev.event_id, team=ev.team, minute=minute,
            old_home=old_home, old_away=old_away,
            new_home=st.home_score, new_away=st.away_score,
            espn_wall=ev.wall_ts, detect_wall=detect_wall, spike_wall=spike,
            scorer_was_favourite=fav, minute_bucket=bucket)
        self._goal_ids[(ev.event_id, ev.team)] = goal_id
        self._goal_game[goal_id] = game
        if self._loop:
            self._loop.create_task(self._overshoot_sample(goal_id, game))
            if (self.snipe_shadow or self.snipe_a_enabled or self.snipe_b_enabled):
                self._loop.create_task(
                    self._snipe_on_goal(goal_id, ev, game, detect_wall))

    def _handle_correction(self, ev: ScoreEvent) -> None:
        """VAR / attribution fix. Mark the goal reverted, flatten any
        sniper position opened on it, blacklist the game for 120s."""
        tickers = self.event_to_markets.get(ev.event_id, [])
        game = tickers[0].split("-")[1] if tickers else ""
        self._snipe_blacklist[game] = time.time() + self.snipe_blacklist_s
        # Clear the dedup key so a legitimately RE-scored goal
        # at the same total isn't invisible to the sniper afterwards.
        st = ev.state
        team_now = st.home_score if ev.team == st.home else st.away_score
        self._seen_goal_keys.discard((ev.event_id, ev.team, team_now + 1))
        gid = self._goal_ids.get((ev.event_id, ev.team))
        if gid is not None:
            self.recorder.mark_goal_reverted(gid)
            self._flatten_snipes(gid)
        print(f"[VAR] correction: {ev.detail} — sniping {game} blacklisted "
              f"{self.snipe_blacklist_s:.0f}s")

    async def _on_var_review(self, game: str) -> None:
        """Sharp feed flags a VAR review -> pre-emptively flatten sniper
        positions from that game's goals and blacklist, instead of waiting for the
        score reversion to show up in ESPN."""
        self._snipe_blacklist[game] = time.time() + self.snipe_blacklist_s
        for gid, g in list(self._goal_game.items()):
            if g == game and gid in self._snipe_positions:
                self._flatten_snipes(gid)
        print(f"[VAR] sharp-feed review flag for {game} — pre-emptive flatten + "
              f"{self.snipe_blacklist_s:.0f}s blacklist")

    def _flatten_snipes(self, goal_id: int) -> None:
        """Marketable IOC out of every sniper position opened on a reverted goal."""
        for ticker, _side in self._snipe_positions.pop(goal_id, []):
            self._snipe_tickers.discard(ticker)
            pos = self.rules.positions.get(ticker)
            book = self.ws.books.get(ticker)
            if pos is None or book is None:
                continue
            self.rules._emit_exit(pos, book, 1.0, "var_reversion",
                                  book.sellable_price(pos.side), time.monotonic())

    def _snipe_gates(self, ticker: str, klass: str, ask: int, game: str,
                     minute: float) -> str | None:
        """Gate variant for snipes. Class A is EXEMPT from minute-75 (a decided
        market at 80' is still decided) and uses the 93c ceiling; everything else
        binds for both classes, incl. the contradiction breaker."""
        if time.time() < self._snipe_blacklist.get(game, 0.0):
            return "game blacklisted (VAR correction)"
        if self.broker.kill_switch_engaged():
            return "STOP file present"
        if self.broker.realized_cents() <= -self.daily_loss_limit_cents:
            return "daily loss limit hit"
        if ask < self.gate_min_price:
            return f"min_entry_price {ask}c < {self.gate_min_price}c"
        ceiling = self.snipe_a_max_price if klass == "a" else self.gate_max_price
        if ask > ceiling:
            return f"price {ask}c > class-{klass} ceiling {ceiling}c"
        if klass == "b" and minute > self.gate_max_minute:
            return f"max_entry_minute: {minute:.0f} > {self.gate_max_minute}"
        # Snipes have their OWN slot budget: they don't compete with
        # reaction/edge positions for max_positions_per_game — a decided market is a
        # settlement-in-waiting, not a correlated directional bet. The $-concentration
        # cap (35%/game, fresh-checked in _place_snipe) still bounds total exposure.
        code = ticker.split("-")[1] if "-" in ticker else ""
        n = sum(1 for t in self._snipe_tickers
                if t.split("-")[1:2] == [code]
                and (p := self.broker.positions.get(t)) and p.net_contracts() != 0)
        if n >= self.snipe_max_per_game:
            return f"snipe max_per_game: {n} snipe positions already open"
        contra = self._score_market_contradiction(ticker)
        if contra:
            return f"score/market contradiction: {contra}"
        return None

    async def _snipe_on_goal(self, goal_id: int, ev: ScoreEvent, game: str,
                             detect_wall: float) -> None:
        """Scan both snipe classes on a confirmed goal. SHADOW-logs every qualifying
        candidate (depth-priced, worst level); fires live only if sniper.enabled."""
        t0 = time.monotonic()
        st = ev.state
        hs, as_ = st.home_score, st.away_score
        minute = estimate_minute(st) or 0.0
        tickers = self.event_to_markets.get(ev.event_id, [])
        books = self.ws.books
        cand_a = class_a_scan(books, tickers, hs, as_, self.snipe_a_max_price,
                              self.snipe_clip_cents, self.snipe_a_max_markets)
        lam = self._lambda_snap.get(game)
        # A fresh sharp-book fair overrides the ladder fair -> lag-arbitrage;
        # sharp-anchored triggers must clear gap + round-trip fees.
        sharp_fairs = {}
        if self.sharp and self.sharp.enabled():
            for t in tickers:
                sf = self.sharp.get_fair_cents(t)
                if sf is not None:
                    sharp_fairs[t] = sf
        rt_fee = 2 * self.fee_fn(50, 1)          # peak round-trip taker fee/contract
        cand_b = (class_b_scan(books, tickers, hs, as_, minute, lam[1], lam[2],
                               self.snipe_b_min_gap, self.snipe_clip_cents,
                               self.snipe_b_max, fair_override=sharp_fairs,
                               override_extra_gap=rt_fee) if lam else [])
        calc_ms = (time.monotonic() - t0) * 1000.0
        detect_ms = max(0.0, (detect_wall - ev.wall_ts) * 1000.0)
        for klass, cands in (("a", cand_a), ("b", cand_b)):
            live = self.snipe_a_enabled if klass == "a" else self.snipe_b_enabled
            for c in cands:
                tg = time.monotonic()
                blocked = self._snipe_gates(c["ticker"], klass, c["ask"], game, minute)
                gates_ms = (time.monotonic() - tg) * 1000.0
                sid = self.recorder.log_shadow_snipe(
                    goal_event_id=goal_id, klass=klass, ticker=c["ticker"],
                    side=c["side"], would_price_cents=c["worst"], ask_cents=c["ask"],
                    contracts=c["contracts"], fair_cents=c.get("fair"),
                    gap_cents=c.get("gap"), detect_ms=detect_ms, calc_ms=calc_ms,
                    gates_ms=gates_ms, blocked_reason=blocked,
                    armed=int(live and not blocked))
                print(f"[snipe:{klass}] {'LIVE' if live else 'SHADOW'} "
                      f"{c['side']} {c['ticker']} ask={c['ask']}c worst={c['worst']}c "
                      f"x{c['contracts']}"
                      + (f" fair={c['fair']:.0f}c({c.get('fair_source','ladder')}) "
                         f"gap={c['gap']:+.0f}c" if klass == "b" else "")
                      + (f" BLOCKED: {blocked}" if blocked else "")
                      + f" | lat d/c/g {detect_ms:.0f}/{calc_ms:.0f}/{gates_ms:.0f}ms")
                if self._loop:
                    self._loop.create_task(
                        self._shadow_track(sid, c["ticker"], c["side"]))
                    # venue-truth crosscheck: does the REST book agree with our WS book?
                    b = books.get(c["ticker"])
                    self._loop.create_task(self._snipe_rest_crosscheck(
                        sid, c["ticker"], c["side"], getattr(b, "dirty", False)))
                if live and not blocked:
                    await self._place_snipe(c, klass, goal_id, detect_wall, sid)

    async def _place_snipe(self, c: dict, klass: str, goal_id: int,
                           detect_wall: float, sid: int = None) -> None:
        """Real snipe order, parallel to _place_live. Fresh-state fetch runs
        here (fair calc already done concurrently upstream). IOC, lane snipe_a/b,
        registers a thesis so reconciliation + VAR flatten own the position. Every
        exit records its outcome to shadow_snipes[sid] so we're not blind on 0-fills."""
        def _out(outcome, wire=None):
            if sid is not None:
                self.recorder.set_snipe_outcome(sid, outcome, wire)
        ticker, side = c["ticker"], c["side"]
        book = self.ws.books.get(ticker)
        if book is None or self.live_broker is None:
            return _out("no_size")
        # feed-integrity: a stale (post-gap) book is why snipes chased phantom asks
        if self.block_on_dirty and getattr(book, "dirty", False):
            print(f"[GATE] snipe blocked {ticker}: book dirty (WS resyncing)")
            return _out("dirty_book")
        try:
            positions, bal = await asyncio.wait_for(
                asyncio.gather(self.rest.get_positions(),
                               self.rest.get_balance_cents()), timeout=2.0)
        except Exception as exc:  # noqa: BLE001  — FAIL CLOSED
            print(f"[GATE] snipe blocked {ticker}: could not verify "
                  f"positions/balance ({exc!r})")
            return _out("verify_failed", (time.time() - detect_wall) * 1000.0)
        self.live_balance_cents = bal
        self.live_broker.balance_cents = bal
        code = ticker.split("-")[1] if "-" in ticker else ""
        room = bal * self.max_game_fraction - self._game_cost_basis_cents(code, positions)
        if room <= 0:
            print(f"[GATE] snipe blocked {ticker}: per-game cap full")
            return _out("cap_full")
        px = c["order_px"]
        contracts = min(c["contracts"], int(room / px), self.gate_max_contracts)
        if contracts <= 0:
            return _out("no_size")
        try:
            resp = await self.live_broker.place(book, side, "buy", px, float(contracts))
        except Exception as exc:  # noqa: BLE001
            print(f"[SNIPE] ORDER FAILED {side} {ticker}: {exc!r}")
            return _out("order_failed", (time.time() - detect_wall) * 1000.0)
        self.recorder.log_order_response(
            lane=f"snipe_{klass}", ticker=ticker, side=side, order_px_cents=px,
            requested=contracts, resp=resp, book_was_dirty=getattr(book, "dirty", False))
        wire_ms = (time.time() - detect_wall) * 1000.0
        filled = float(resp.get("fill_count") or 0)
        if filled <= 0:
            print(f"[SNIPE] no fill {side} {ticker} (IOC @{px}c) "
                  f"| detect->wire {wire_ms:.0f}ms")
            return _out("no_fill", wire_ms)
        filled_int = int(round(filled))
        yes_fill = float(resp.get("average_fill_price") or 0) * 100.0
        side_price = (100.0 - yes_fill) if side == "no" else yes_fill
        fee_cents = int(round(float(resp.get("average_fee_paid") or 0) * 100.0 * filled))
        lane = f"snipe_{klass}"
        # thesis FIRST (rules.on_fill copies its exit rules into the position):
        # Class A holds to settlement (no rules); Class B gets the mandatory decay
        # sell — thesis_dead already covers every open position automatically.
        exit_rules = ([] if klass == "a" else
                      [{"event": f"clock>={c['exit_minute']}", "action": "sell_all",
                        "reason": "class-B decay exit"}])
        self.rules.set_thesis(Thesis(
            thesis_id=f"snipe{goal_id}", market_ticker=ticker, side=side,
            limit_price_cents=px, edge_cents=0, exit_rules=exit_rules,
            llm_start_wall=None, llm_end_wall=None, llm_latency_ms=None,
            trigger_event_monotonic=time.monotonic(), trigger_event_wall=detect_wall,
            trigger=f"snipe:{klass}", entered=True, lane=lane))
        fill = self.broker.record_fill(book, side, "buy", filled_int,
                                       side_price, fee_cents)
        self.rules.on_fill(ticker, side, "buy", side_price, filled_int)
        self._snipe_positions.setdefault(goal_id, []).append((ticker, side))
        self._snipe_tickers.add(ticker)
        self.decisions += 1
        self.recorder.log_decision(
            thesis_id=f"snipe{goal_id}", trigger=f"snipe:{klass} goal#{goal_id}",
            fill=fill, event_monotonic=time.monotonic(), event_wall=detect_wall,
            decision_monotonic=time.monotonic(), decision_wall=time.time(),
            llm_start_wall=None, llm_end_wall=None, llm_latency_ms=None, lane=lane,
            fair_source=c.get("fair_source"), exec_style="taker")  # snipes always take
        _out("filled", wire_ms)
        budget = " OVER-BUDGET" if wire_ms > 1500 else ""
        print(f"[SNIPE] FILLED [{lane}] buy {side} {ticker} x{filled_int} @ "
              f"{side_price:.1f}c | detect->wire {wire_ms:.0f}ms{budget}")

    async def _snipe_rest_crosscheck(self, sid: int, ticker: str, side: str,
                                     ws_dirty: bool) -> None:
        """Fetch the venue's REST orderbook right after a snipe candidate and record
        its ask next to our WS ask. The decisive stale-book test: if WS said 42c and
        the venue REST says 99c, the '+38 shadow edge' was scored off a desynced book."""
        try:
            book = await asyncio.wait_for(self.rest.get_orderbook(ticker), timeout=2.0)
        except Exception:  # noqa: BLE001
            return
        rest_ask = book.best_yes_ask if side == "yes" else book.best_no_ask
        self.recorder.set_snipe_rest_ask(sid, rest_ask, ws_dirty)

    async def _shadow_track(self, snipe_id: int, ticker: str, side: str) -> None:
        """Book state at +30/+60/+120s for a shadow snipe (what the market did next)."""
        last = 0
        for off in (30, 60, 120):
            await asyncio.sleep(off - last)
            last = off
            b = self.ws.books.get(ticker)
            if b is None:
                continue
            ask = b.best_yes_ask if side == "yes" else b.best_no_ask
            if ask is not None:
                self.recorder.update_shadow_book(snipe_id, off, ask)

    async def _overshoot_sample(self, goal_id: int, game: str) -> None:
        """Snapshot every mapped book of the game at t0..+120s."""
        tickers = [t for t in self.tickers if t.split("-")[1:2] == [game]]
        last = 0
        for off in (0, 5, 10, 20, 30, 45, 60, 90, 120):
            await asyncio.sleep(off - last)
            last = off
            for t in tickers:
                b = self.ws.books.get(t)
                if b is None:
                    continue
                ya, na = b.best_yes_ask, b.best_no_ask
                self.recorder.log_overshoot(
                    goal_event_id=goal_id, ticker=t, offset_s=off,
                    yes_bid=b.best_yes_bid, yes_ask=ya,
                    no_bid=b.best_no_bid, no_ask=na,
                    yes_ask_depth=(b.fillable_size("yes", "buy", ya) if ya else 0),
                    no_ask_depth=(b.fillable_size("no", "buy", na) if na else 0))

    def _snapshot_lambdas(self) -> None:
        """Continuously captured PRE-goal ladder lambdas, so a
        pre-event value always exists (post-goal books are circular)."""
        from engine.markets import parse_market
        games: dict[str, tuple] = {}
        for t in self.tickers:
            code = t.split("-")[1]
            if code not in games:
                try:
                    spec = parse_market(t)
                    games[code] = (spec.home_code, spec.away_code)
                except Exception:  # noqa: BLE001
                    pass
        for code, (h, a) in games.items():
            lh, la = self._team_lambda(code, h), self._team_lambda(code, a)
            if lh is None or la is None:
                lt = self._total_lambda(code)
                if lt is None:
                    continue
                mh, ma = self._ml_mid(code, h), self._ml_mid(code, a)
                sh = (mh / (mh + ma)) if (mh and ma and mh + ma > 0) else 0.5
                lh, la = lt * sh, lt * (1 - sh)
            self._lambda_snap[code] = (time.time(), lh, la)
            # PRE-goal ML fairs for the asymmetry tags. Sharp feed if
            # fresh, else de-vigged ML mids. Snapshot cadence guarantees the value
            # used at goal time predates the goal.
            fairs = {}
            for team in (h, a):
                sf = (self.sharp.get_fair_cents(f"{self._root(code)}GAME-{code}-{team}")
                      if self.sharp else None)
                if sf is not None:
                    fairs[team] = sf / 100.0
            if not fairs:
                mh, ma = self._ml_mid(code, h), self._ml_mid(code, a)
                mt = self._ml_mid(code, "TIE")
                tot = sum(x for x in (mh, ma, mt) if x)
                if mh and tot:
                    fairs[h] = mh / tot
                if ma and tot:
                    fairs[a] = ma / tot
            if fairs:
                self._ml_fair_snap[code] = (time.time(), fairs)

    async def _lambda_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._snapshot_lambdas()
            except Exception as exc:  # noqa: BLE001
                print(f"[lambda] snapshot error: {exc!r}")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    async def _sample_drift(self, decision_id: int, ticker: str, side: str,
                            latency_ms: float) -> None:
        await asyncio.sleep(latency_ms / 1000.0)
        book = self.ws.books.get(ticker)
        if not book:
            return
        mid = book.mid(side)
        if mid is not None:
            self.recorder.set_price_drift(decision_id, mid)

    # ===== score events ================================================
    async def _on_score_event(self, ev: ScoreEvent) -> None:
        self.latest_score[ev.event_id] = ev.state
        kalshi_lastplay = None
        for m in self.event_to_markets.get(ev.event_id, []):
            if m in self.latest_ticker:
                kalshi_lastplay = json.dumps(self.latest_ticker[m])[:500]
                break
        self.recorder.log_score_event(ev, kalshi_lastplay=kalshi_lastplay)
        print(f"[score] {ev.kind} {ev.detail} {ev.state.home} {ev.state.home_score}-"
              f"{ev.state.away_score} {ev.state.away} (P{ev.state.period} {ev.state.clock})")

        # Keep display names current so the sharp provider can match games
        _tk = self.event_to_markets.get(ev.event_id, [])
        if _tk:
            _g = _tk[0].split("-")[1]
            meta = self._game_meta.get(_g)
            if meta and ev.state.home:
                self._game_meta[_g] = (meta[0], meta[1],
                                       [ev.state.home, ev.state.away])

        # ---- Sniper lane hooks: goal pipeline + VAR handling ----
        if ev.kind == "correction":
            self._handle_correction(ev)
        elif ev.kind == "goal":
            self._handle_goal(ev)

        # feed the deterministic engine's event-keyed exit rules (fast, no LLM)
        minute = estimate_minute(ev.state)
        relevant = self.event_to_markets.get(ev.event_id) or self.tickers
        books = {t: self.ws.books[t] for t in relevant if t in self.ws.books}
        self.rules.on_score_event(ev.kind, ev.team, minute, books, ev.recv_monotonic, ev.wall_ts)

        # SCORE-STATE exits (the real downside protection): exit only when the
        # verified score has invalidated the thesis — never on a price wobble.
        score_by_ticker = {}
        for t in self.rules.positions:
            sv = self._score_view(self.market_to_event.get(t))
            if sv is not None:
                score_by_ticker[t] = sv
        if score_by_ticker:
            all_books = {t: self.ws.books[t] for t in self.rules.positions if t in self.ws.books}
            self.rules.check_thesis_death(score_by_ticker, all_books,
                                          ev.recv_monotonic, ev.wall_ts)

        # Major verified state changes trigger ONE board-wide analyst scan.
        # First call for a game = pregame; thereafter only goal / red / half.
        reason = None
        if ev.kind == "kickoff":
            if ev.event_id not in self.pregame_done:
                self.pregame_done.add(ev.event_id)
                reason = "pregame"
        elif ev.kind == "goal":
            reason = "event:goal"
        elif ev.kind == "red_card":
            reason = "event:red"
        elif ev.kind == "period":
            reason = "event:half"
        if reason:
            self._loop.create_task(self._run_analyst(reason, ev.recv_monotonic, ev.wall_ts))

    # ===== analyst orchestration (OFF hot path) ========================
    def _quote(self, b: OrderBook) -> dict:
        ya, na = b.best_yes_ask, b.best_no_ask
        return {
            "yes_bid": b.best_yes_bid, "yes_ask": ya,
            "no_bid": b.best_no_bid, "no_ask": na,
            "yes_mid": b.mid("yes"),
            # resting size at the touch -> lets the analyst avoid thin books
            "yes_fillable_at_ask": b.fillable_size("yes", "buy", ya) if ya is not None else 0,
            "no_fillable_at_ask": b.fillable_size("no", "buy", na) if na is not None else 0,
            "yes_depth": sorted(b.yes.items(), reverse=True)[:4],
            "no_depth": sorted(b.no.items(), reverse=True)[:4],
        }

    def _board_snapshot(self) -> tuple[dict, int]:
        """Board grouped by game (ESPN event id) -> series -> market, so the
        analyst can run its per-game cross-market consistency check. Returns
        (snapshot, markets_scanned)."""
        games: dict[str, dict] = {}
        n = 0
        for t, b in self.ws.books.items():
            eid = str(self.market_to_event.get(t, "unknown"))
            series = t.split("-", 1)[0]
            games.setdefault(eid, {}).setdefault(series, {})[t] = self._quote(b)
            n += 1
        out = {}
        for eid, series_map in games.items():
            out[eid] = {
                "verified_score": self._score_context_by_event(eid),
                "markets_by_series": series_map,
            }
        return out, n

    def _score_context_by_event(self, eid) -> dict:
        st = self.latest_score.get(str(eid))
        if not st:
            return {"note": "no verified score yet for this game"}
        return {
            "event_id": st.event_id, "home": st.home, "away": st.away,
            "home_score": st.home_score, "away_score": st.away_score,
            "home_reds": st.home_reds, "away_reds": st.away_reds,
            "state": st.state, "period": st.period, "clock": st.clock,
            "estimated_minute": estimate_minute(st),
        }

    def _score_view(self, eid):
        from engine.markets import ScoreView
        st = self.latest_score.get(str(eid)) if eid is not None else None
        if st is None:
            return None
        status = {"pre": "pre", "in": "live", "post": "final"}.get(st.state, "live")
        return ScoreView(home_goals=st.home_score, away_goals=st.away_score,
                         minute=estimate_minute(st) or 0.0, status=status)

    def _is_blind(self, ticker: str) -> bool:
        """No entries / no price exits while we can't see state: WS down or the
        score feed has gone stale."""
        if not self.ws.connected:
            return True
        if time.time() - self.score.last_poll_wall > self.max_score_age_s:
            return True
        return False

    async def _run_analyst(self, reason: str, event_monotonic: float,
                           event_wall: float) -> None:
        # serialize calls; coalesce bursts of the same reason. NEVER a tight loop.
        now = time.monotonic()
        if now - self._last_analyst_call.get(reason, 0) < 2.0:
            return
        self._last_analyst_call[reason] = now
        async with self._analyst_lock:
            board, markets_scanned = self._board_snapshot()
            scores = {eid: self._score_context_by_event(eid) for eid in self.event_to_markets}
            res = await self.analyst.analyze(
                board_snapshot=board, score_context=scores, trigger=reason,
                context=self.context)
            self.last_analyst_latency_ms = res.latency_ms
            self.last_markets_scanned = markets_scanned
            thesis_id = uuid.uuid4().hex[:12]
            self.recorder.log_analyst_call(
                reason=reason, markets_scanned=markets_scanned,
                start_wall=res.start_wall, end_wall=res.end_wall,
                start_monotonic=res.start_monotonic, end_monotonic=res.end_monotonic,
                parsed_ok=res.parsed_ok, raw_json=res.raw_text, thesis_id=thesis_id,
            )
            if not res.parsed_ok:
                print(f"[analyst] {reason} (scanned {markets_scanned}) unparseable: {res.error}")
                return
            try:
                self._snapshot_lambdas()   # refresh pre-goal λ on every scan
            except Exception:  # noqa: BLE001
                pass
            parsed = res.parsed
            parsed["_lane"] = self._determine_lane(parsed)   # edge | reaction
            parsed = self._apply_liquidity_guard(parsed)
            parsed = self._apply_no_trade_gate(parsed)
            try:
                parsed = self._apply_ladder_gate(parsed)     # totals + team-totals
                parsed = self._apply_diff_gate(parsed)        # spread + moneyline (Skellam)
            except Exception as exc:  # noqa: BLE001  — never let the gate break trading
                print(f"[gate] gate error (skipped): {exc!r}")
            parsed = self._apply_freshness_gate(parsed, scores)
            bt = parsed.get("best_trade")
            if not bt:
                print(f"[analyst] {reason} scanned={markets_scanned} "
                      f"({res.latency_ms:.0f}ms) -> NO TRADE "
                      f"({parsed.get('_guard_note','no edge')})")
                return
            self._install_thesis(parsed, res, thesis_id, reason, event_monotonic, event_wall)
            print(f"[analyst] {reason} scanned={markets_scanned} ({res.latency_ms:.0f}ms) "
                  f"-> BEST [{parsed.get('_lane','edge')}] {bt['side']} {bt['market_ticker']} "
                  f"@{bt['limit_price_cents']}c "
                  f"edge={parsed.get('edge_cents')} conf={parsed.get('confidence')}")
            for a in (parsed.get("ranked_alternatives") or [])[:3]:
                flag = " THIN" if a.get("thin_book") else ""
                print(f"           alt: {a.get('side')} {a.get('market_ticker')} "
                      f"@{a.get('price_cents')}c edge={a.get('edge_cents')} "
                      f"fillable={a.get('fillable','?')}{flag}")

    def _candidate_liquidity(self, ticker: str, side: str, price) -> int:
        b = self.ws.books.get(ticker)
        if not b or side not in ("yes", "no") or price is None:
            return 0
        return b.fillable_size(side, "buy", int(price))

    def _apply_liquidity_guard(self, parsed: dict) -> dict:
        """Never surface a best_trade we can't fill at meaningful size. Demote a
        thin best_trade into alternatives (flagged) and promote the best liquid
        alternative. Tag every alternative with its fillable size."""
        bt = parsed.get("best_trade") or {}
        alts = parsed.setdefault("ranked_alternatives", [])
        bt_liq = self._candidate_liquidity(
            bt.get("market_ticker"), bt.get("side"), bt.get("limit_price_cents"))
        if bt and bt_liq < self.min_liquidity:
            alts.insert(0, {
                "market_ticker": bt.get("market_ticker"), "side": bt.get("side"),
                "price_cents": bt.get("limit_price_cents"),
                "edge_cents": parsed.get("edge_cents"),
                "note": f"LOW-LIQUIDITY: {bt_liq} contracts < min {self.min_liquidity}",
                "thin_book": True, "fillable": bt_liq,
            })
            promoted = None
            for a in alts:
                liq = self._candidate_liquidity(
                    a.get("market_ticker"), a.get("side"), a.get("price_cents"))
                if liq >= self.min_liquidity:
                    promoted = a
                    break
            if promoted:
                parsed["best_trade"] = {
                    "market_ticker": promoted["market_ticker"], "side": promoted["side"],
                    "limit_price_cents": promoted["price_cents"]}
                parsed["edge_cents"] = promoted.get("edge_cents", parsed.get("edge_cents"))
                parsed["_guard_note"] = "thin best demoted; promoted liquid alternative"
            else:
                parsed["best_trade"] = None
                parsed["_guard_note"] = (
                    f"all candidates below min_liquidity {self.min_liquidity}")
        # annotate every alternative with fillable size + thin flag
        for a in alts:
            liq = self._candidate_liquidity(
                a.get("market_ticker"), a.get("side"), a.get("price_cents"))
            a["fillable"] = liq
            a["thin_book"] = liq < self.min_liquidity
        return parsed

    # ---- Fair-value resolution (sharp -> ladder -> analyst) --------

    def _root(self, code: str) -> str:
        """Series root (KXWC/KXUCL/KXMLS) for a game code — club-soccer port."""
        return self._game_root.get(code, "KXWC")

    def _ladder_fair_yes(self, ticker: str):
        """Market-implied YES fair from the ticker's sibling ladder lines (same
        machinery as the ladder gate), or None."""
        spec = self._ladder_spec(ticker)
        if spec is None:
            return None
        prefix, k_bet, team = spec
        lams = []
        for t, b in self.ws.books.items():
            if not t.startswith(prefix):
                continue
            sib = t.split("-")[2]
            kp = sib[3:] if team else sib
            try:
                k = int(kp)
            except ValueError:
                continue
            if k == k_bet:
                continue
            mid = b.mid("yes")
            if mid is None:
                continue
            p = mid / 100.0
            if 0.03 < p < 0.97:
                lams.append(self._lam_from_prob(k, p))
        if not lams:
            return None
        return self._pois_sf(sum(lams) / len(lams), k_bet) * 100.0

    def _resolve_fair(self, ticker: str, side: str, analyst_fair=None):
        """Resolution order: sharp feed (if fresher than 10s) -> ladder ->
        analyst-claimed. Returns (fair_in_side_terms | None, source)."""
        sharp = self.sharp.get_fair_cents(ticker) if self.sharp else None
        if sharp is not None:
            return (sharp if side == "yes" else 100.0 - sharp), "sharp"
        lf = self._ladder_fair_yes(ticker)
        if lf is not None:
            return (lf if side == "yes" else 100.0 - lf), "ladder"
        if analyst_fair is not None:
            return float(analyst_fair), "analyst"
        return None, "none"

    # ---- Reaction lane (a second entry class, NOT a lower bar) ------
    _REACTION_EVENT_RE = re.compile(r"^(goal_for:|goal_against:|clock>=)")

    def _reaction_eligible(self, parsed: dict) -> str | None:
        """Why this pick does NOT qualify for the reaction lane, else None.
        Criteria (all): TIE-ML / game-total / team-total only; entry 20-80c;
        carries an event-keyed exit rule; doesn't overpay the analyst's own fair
        by >3c (market fair re-checked by the ladder/diff gates downstream)."""
        bt = parsed.get("best_trade") or {}
        ticker = bt.get("market_ticker", "")
        parts = ticker.split("-")
        series = parts[0] if parts else ""
        suffix = parts[2] if len(parts) > 2 else ""
        from engine.markets import series_kind
        kind = series_kind(series)
        if not (kind in ("TOTAL", "TEAMTOTAL")
                or (kind == "GAME" and suffix == "TIE")):
            return "market type (TIE/totals/team-totals only)"
        px = int(bt.get("limit_price_cents", 0) or 0)
        if not 20 <= px <= 80:
            return f"price {px}c outside 20-80c reaction band"
        rules = parsed.get("exit_rules") or []
        if not any(self._REACTION_EVENT_RE.match(str(r.get("event", "")))
                   for r in rules):
            return "no event-keyed exit rule (goal_for/goal_against/clock>=)"
        # No-overpay vs the RESOLVED fair (sharp -> ladder -> analyst)
        fair, src = self._resolve_fair(
            ticker, bt.get("side", "yes"),
            parsed.get("fair_value_cents") or bt.get("fair_value_cents"))
        parsed["_fair_source"] = src
        if fair is not None and px > float(fair) + 3:
            return f"limit {px}c overpays {src} fair {fair:.0f}c by more than 3c"
        return None

    def _determine_lane(self, parsed: dict) -> str:
        """'edge' if the pick clears the edge-lane bar (edge lane takes precedence);
        else 'reaction' if it qualifies; else 'edge' (where it will die at the bar)."""
        if int(parsed.get("edge_cents", 0) or 0) >= self.min_edge_entry:
            return "edge"
        why_not = self._reaction_eligible(parsed)
        if why_not is None:
            return "reaction"
        parsed["_reaction_reject"] = why_not
        bt = parsed.get("best_trade") or {}
        if bt.get("market_ticker"):
            print(f"[GATE] reaction-lane reject {bt.get('market_ticker')}: {why_not}")
        return "edge"

    def _apply_no_trade_gate(self, parsed: dict) -> dict:
        """Decline dead/efficient boards. (1) game decided: any moneyline leg for
        the candidate's game >= decided_moneyline_cents. (2) edge after round-trip
        fees < min_edge_after_fees_cents. Either => open nothing."""
        bt = parsed.get("best_trade")
        if not bt:
            return parsed
        # (0) implausible edge => data/model error, not opportunity (the 68c Brazil
        # pick on a stale score; the fade-the-favorite mega-edges). Liquid markets
        # are not off by 30c+.
        edge0 = int(parsed.get("edge_cents", 0) or 0)
        if self.max_plausible_edge and edge0 > self.max_plausible_edge:
            parsed["best_trade"] = None
            parsed["_guard_note"] = (f"implausible edge {edge0}c > {self.max_plausible_edge}c "
                                     f"— stale/mispriced data, not real edge")
            return parsed
        ticker = bt.get("market_ticker", "")
        # (1) decided board — check moneyline legs of the same game
        parts = ticker.split("-")
        if len(parts) >= 2:
            code = parts[1]
            for t, b in self.ws.books.items():
                if t.startswith(f"{self._root(code)}GAME-{code}-"):
                    ask = b.best_yes_ask
                    if ask is not None and ask >= self.decided_moneyline:
                        parsed["best_trade"] = None
                        parsed["_guard_note"] = (
                            f"game decided: moneyline leg {t} ask {ask}c "
                            f">= {self.decided_moneyline}c")
                        return parsed
        # (2) edge after fees — EDGE LANE ONLY. The reaction lane enters on
        # fair price + event exits, not edge; its own criteria were checked upstream.
        if parsed.get("_lane") == "reaction":
            return parsed
        edge = int(parsed.get("edge_cents", 0) or 0)
        limit = int(bt.get("limit_price_cents", 50))
        rt_fee = self.fee_fn(limit, 1, ticker=ticker) + self.fee_fn(limit, 1, ticker=ticker)
        if edge - rt_fee < self.min_edge_after_fees:
            parsed["best_trade"] = None
            parsed["_guard_note"] = (
                f"edge {edge}c - round-trip fee {rt_fee}c < min "
                f"{self.min_edge_after_fees}c after fees")
        return parsed

    # ---- Totals-ladder coherence gate ------------------------
    @staticmethod
    def _pois_sf(lam: float, k: int) -> float:
        """P(total >= k) for Poisson(lam). k is the suffix N (Over N-0.5 => total>=N)."""
        if k <= 0:
            return 1.0
        import math
        cdf, term = 0.0, math.exp(-lam)
        for i in range(0, k):
            if i > 0:
                term *= lam / i
            cdf += term
        return max(0.0, min(1.0, 1.0 - cdf))

    def _lam_from_prob(self, k: int, p: float) -> float:
        """Invert P(total>=k)=p for the Poisson mean (bisection; sf is increasing in lam)."""
        lo, hi = 1e-3, 12.0
        for _ in range(40):
            mid = (lo + hi) / 2.0
            if self._pois_sf(mid, k) < p:
                lo = mid
            else:
                hi = mid
        return (lo + hi) / 2.0

    def _edge_bar(self, price: int) -> float:
        """Required edge (cents) vs market-fair, scaled by price: edge_mid at 50c
        rising linearly to edge_extreme at 0/100c. Cheap longshots must clear a
        higher bar."""
        frac = abs(50 - price) / 50.0
        return self.ladder_edge_mid + (self.ladder_edge_extreme - self.ladder_edge_mid) * frac

    @staticmethod
    def _ladder_spec(ticker: str):
        """For a totals / team-total ticker return (prefix, k_bet, team): `prefix`
        matches the sibling lines that share the same goal distribution, `k_bet` is
        this line's threshold N (Over N-0.5 => total >= N), and `team` is the 3-letter
        team code for team-totals (None for game totals). None if not a totals market."""
        parts = ticker.split("-")
        if len(parts) < 3:
            return None
        series, code, suffix = parts[0], parts[1], parts[2]
        from engine.markets import series_kind
        kind = series_kind(series)
        if kind == "TOTAL":                       # game total: siblings = other lines
            try:
                return f"{series}-{code}-", int(suffix), None
            except ValueError:
                return None
        if kind == "TEAMTOTAL":                   # team total: siblings = same team's lines
            team = suffix[:3]
            try:
                return f"{series}-{code}-{team}", int(suffix[3:]), team
            except ValueError:
                return None
        return None

    def _apply_ladder_gate(self, parsed: dict) -> dict:
        """Re-price a GAME-TOTAL or TEAM-TOTAL bet using the goal expectation the
        MARKET implies from the sibling lines of the same ladder (a non-LLM signal),
        then require a price-scaled edge vs THAT fair. Kills phantom Under/Over edge
        that only existed because the LLM's lambda disagreed with the market (e.g.
        fading a heavy favourite's team total). Fail-open: if the ladder can't establish a
        lambda, it does nothing."""
        if not self.ladder_enabled:
            return parsed
        bt = parsed.get("best_trade")
        if not bt:
            return parsed
        ticker = bt.get("market_ticker", "")
        side = bt.get("side", "")
        spec = self._ladder_spec(ticker)
        if spec is None:
            return parsed
        prefix, k_bet, team = spec
        # market lambda from the OTHER lines of this ladder
        lams = []
        for t, b in self.ws.books.items():
            if not t.startswith(prefix):
                continue
            sib = t.split("-")[2]
            kp = sib[3:] if team else sib         # strip the team code for team totals
            try:
                k = int(kp)
            except ValueError:
                continue
            if k == k_bet:
                continue
            mid = b.mid("yes")
            if mid is None:
                continue
            p = mid / 100.0
            if p <= 0.03 or p >= 0.97:   # uninformative at the rails
                continue
            lams.append(self._lam_from_prob(k, p))
        if not lams:
            return parsed   # fail-open: no market lambda available
        lam_mkt = sum(lams) / len(lams)
        p_over = self._pois_sf(lam_mkt, k_bet)
        fair = (p_over if side == "yes" else 1.0 - p_over) * 100.0
        limit = int(bt.get("limit_price_cents", 50) or 50)
        # Reaction lane: no edge required — just never overpay market fair
        # by more than 3c (same no-overpay logic as the TIE carve-out, generalized).
        if parsed.get("_lane") == "reaction":
            if limit > fair + 3:
                parsed["best_trade"] = None
                parsed["_guard_note"] = (
                    f"reaction: limit {limit}c overpays ladder fair {fair:.0f}c by >3c "
                    f"(mkt λ≈{lam_mkt:.2f})")
            return parsed
        edge_vs_mkt = fair - limit
        bar = self._edge_bar(limit)
        if edge_vs_mkt < bar:
            parsed["best_trade"] = None
            parsed["_guard_note"] = (
                f"ladder: mkt λ≈{lam_mkt:.2f} -> {side} fair {fair:.0f}c vs limit {limit}c, "
                f"edge {edge_vs_mkt:+.0f}c < bar {bar:.0f}c")
        return parsed

    # ---- Spread / moneyline coherence via a Skellam of per-team rates -----
    def _ml_mid(self, code: str, team: str):
        b = self.ws.books.get(f"{self._root(code)}GAME-{code}-{team}")
        return b.mid("yes") if b else None

    def _team_lambda(self, code: str, team: str):
        """Full-match goal lambda for `team` implied by its TEAMTOTAL ladder, or None."""
        prefix = f"{self._root(code)}TEAMTOTAL-{code}-{team}"
        lams = []
        for t, b in self.ws.books.items():
            if not t.startswith(prefix):
                continue
            try:
                k = int(t.split("-")[2][3:])
            except (ValueError, IndexError):
                continue
            mid = b.mid("yes")
            if mid is None:
                continue
            p = mid / 100.0
            if 0.03 < p < 0.97:
                lams.append(self._lam_from_prob(k, p))
        return sum(lams) / len(lams) if lams else None

    def _total_lambda(self, code: str):
        """Full-match TOTAL goal lambda implied by the game's totals ladder, or None."""
        prefix = f"{self._root(code)}TOTAL-{code}-"
        lams = []
        for t, b in self.ws.books.items():
            if not t.startswith(prefix):
                continue
            try:
                k = int(t.split("-")[2])
            except ValueError:
                continue
            mid = b.mid("yes")
            if mid is None:
                continue
            p = mid / 100.0
            if 0.03 < p < 0.97:
                lams.append(self._lam_from_prob(k, p))
        return sum(lams) / len(lams) if lams else None

    @staticmethod
    def _margin_prob(cur_margin: int, lam_a: float, lam_b: float, cmp_fn) -> float:
        """P(cur_margin + Xa - Xb satisfies cmp_fn), Xa~Poisson(lam_a), Xb~Poisson(lam_b),
        truncated at 10 goals each side (double-Poisson Skellam; cheap, exact enough)."""
        import math
        pa = [math.exp(-lam_a) * lam_a ** k / math.factorial(k) for k in range(11)]
        pb = [math.exp(-lam_b) * lam_b ** k / math.factorial(k) for k in range(11)]
        return sum(pa[a] * pb[b] for a in range(11) for b in range(11)
                   if cmp_fn(cur_margin + a - b))

    def _apply_diff_gate(self, parsed: dict) -> dict:
        """Coherence gate for KXWCSPREAD (team wins reg by >N) and KXWCGAME (moneyline).
        Builds market-implied per-team REMAINING goal rates (team-total ladders, else
        split the totals lambda by the moneyline mid), conditions on the CURRENT verified
        score + time left, and prices the goal-difference via a Skellam — then requires
        the same price-scaled edge vs that fair. Fail-open. Closes the spread/moneyline
        gap that admitted the MEX +2.5 and SEN +1.5 longshots."""
        if not self.ladder_enabled:
            return parsed
        bt = parsed.get("best_trade")
        if not bt:
            return parsed
        ticker = bt.get("market_ticker", "")
        side = bt.get("side", "")
        parts = ticker.split("-")
        if len(parts) < 3:
            return parsed
        series, code, suffix = parts[0], parts[1], parts[2]
        from engine.markets import series_kind
        s_kind = series_kind(series)
        if s_kind not in ("SPREAD", "GAME"):
            return parsed
        from engine.markets import parse_market
        spec = parse_market(ticker)
        hcode, acode = spec.home_code, spec.away_code
        lam_h, lam_a = self._team_lambda(code, hcode), self._team_lambda(code, acode)
        if lam_h is None or lam_a is None:                 # fall back: split totals by ML
            lam_tot = self._total_lambda(code)
            if lam_tot is None:
                return parsed                              # fail-open
            mh, ma = self._ml_mid(code, hcode), self._ml_mid(code, acode)
            sh = (mh / (mh + ma)) if (mh and ma and mh + ma > 0) else 0.5
            lam_h, lam_a = lam_tot * sh, lam_tot * (1 - sh)
        st = self.latest_score.get(str(self.market_to_event.get(ticker)))
        if st is None:
            return parsed                                  # no verified score -> fail-open
        minute = estimate_minute(st)
        if minute is None:
            minute = 45.0 if st.state == "in" else 0.0
        rem = max(0.0, (90.0 - minute) / 90.0)
        lh, la = lam_h * rem, lam_a * rem
        ch, ca = st.home_score, st.away_score
        if s_kind == "SPREAD":
            team = suffix[:3]
            try:
                n = int(suffix[3:])
            except ValueError:
                return parsed
            if team == hcode:
                cur, lt, lo = ch - ca, lh, la
            elif team == acode:
                cur, lt, lo = ca - ch, la, lh
            else:
                return parsed
            p_yes = self._margin_prob(cur, lt, lo, lambda d: d >= n)   # wins reg by >= n
        else:  # KXWCGAME moneyline
            if suffix == "TIE":
                p_yes = self._margin_prob(ch - ca, lh, la, lambda d: d == 0)
            elif suffix == hcode:
                p_yes = self._margin_prob(ch - ca, lh, la, lambda d: d > 0)
            elif suffix == acode:
                p_yes = self._margin_prob(ca - ch, la, lh, lambda d: d > 0)
            else:
                return parsed
        fair = (p_yes if side == "yes" else 1.0 - p_yes) * 100.0
        limit = int(bt.get("limit_price_cents", 50) or 50)
        # TIE is the proven reaction lane (tie-NO entries won via event exits with ~no
        # model-edge at entry). Requiring the full edge bar would block them, so TIE
        # only gets a NO-OVERPAY check: never pay more than ~fair + 3c. Spreads and
        # team moneylines (the demonstrated loss class) keep the full edge bar.
        if s_kind == "GAME" and suffix == "TIE":
            if limit > fair + 3:
                parsed["best_trade"] = None
                parsed["_guard_note"] = (f"diff-gate(tie): limit {limit}c overpays "
                                         f"fair {fair:.0f}c by more than 3c")
            return parsed
        edge = fair - limit
        bar = self._edge_bar(limit)
        if edge < bar:
            parsed["best_trade"] = None
            parsed["_guard_note"] = (f"diff-gate: fair {fair:.0f}c vs limit {limit}c, "
                                     f"edge {edge:+.0f}c < bar {bar:.0f}c")
        return parsed

    def _apply_freshness_gate(self, parsed: dict, scores_at_call: dict) -> dict:
        """The analyst takes ~30s. Never act on a snapshot that aged out: drop the
        trade if the candidate's game went final or its score moved during the
        think."""
        bt = parsed.get("best_trade")
        if not bt:
            return parsed
        eid = str(self.market_to_event.get(bt.get("market_ticker"), ""))
        before = scores_at_call.get(eid, {})
        now = self._score_context_by_event(eid)
        if now.get("state") == "final" or now.get("state") == "post":
            parsed["best_trade"] = None
            parsed["_guard_note"] = "stale: game went final during analyst latency"
            return parsed

        def key(d):
            return (d.get("home_score"), d.get("away_score"), d.get("state"))
        if before and key(before) != key(now):
            parsed["best_trade"] = None
            parsed["_guard_note"] = "stale: score moved during analyst latency — re-decide"
        return parsed

    def _install_thesis(self, parsed, res, thesis_id, reason, event_monotonic,
                        event_wall) -> None:
        bt = parsed["best_trade"]
        target = bt["market_ticker"]
        if target not in self.ws.books and target not in self.tickers:
            print(f"[analyst] ignoring thesis for unwatched market {target}")
            return
        th = Thesis(
            thesis_id=thesis_id, market_ticker=target, side=bt["side"],
            limit_price_cents=int(bt["limit_price_cents"]),
            edge_cents=int(parsed.get("edge_cents", 0) or 0),
            exit_rules=parsed.get("exit_rules", []),
            llm_start_wall=res.start_wall, llm_end_wall=res.end_wall,
            llm_latency_ms=res.latency_ms,
            trigger_event_monotonic=event_monotonic, trigger_event_wall=event_wall,
            trigger=reason,
            lane=parsed.get("_lane", "edge"),
            fair_source=parsed.get("_fair_source", "analyst"),
        )
        self.rules.set_thesis(th)

    # ===== settlement polling ==========================================
    async def _settlement_loop(self) -> None:
        interval = self.cfg["kalshi"].get("rest_poll_seconds", 15)
        while not self._stop.is_set():
            for t in self.tickers:
                if t in self.settled:
                    continue
                try:
                    s = await self.rest.get_settlement(t)
                except Exception as exc:  # noqa: BLE001
                    print(f"[rest] settlement poll {t} failed: {exc!r}")
                    continue
                if s["settled"] and s["result"] in ("yes", "no"):
                    self._on_settled(t, s["result"])
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    def _on_settled(self, ticker: str, result: str) -> None:
        info = self.broker.settle(ticker, result)
        realized = self.broker.positions.get(ticker)
        realized_cents = realized.realized_cents if realized else 0
        # hold-to-resolution counterfactual: never sold, held every entry to settle
        htr = 0
        for side, qty, price, fee in self.recorder.accepted_buys_for(ticker):
            if price is None:
                continue
            payoff = 100 if side == result else 0
            htr += int(round(qty * (payoff - price))) - int(fee or 0)
        self.recorder.log_settlement(
            ticker=ticker, result=result, settled_pnl_cents=info["settled_pnl_cents"],
            realized_pnl_cents=realized_cents, hold_to_resolution_pnl_cents=htr,
        )
        self.settled.add(ticker)
        self._snipe_tickers.discard(ticker)      # frees the snipe lane's slot budget
        self.rules.thesis_by_ticker.pop(ticker, None)
        self.rules.positions.pop(ticker, None)   # manager no longer tracks a settled market
        print(f"[settle] {ticker} -> {result.upper()} | realized "
              f"{realized_cents/100:+.2f} vs hold-to-resolution {htr/100:+.2f}")

    # ===== Startup position reconciliation =====================
    async def _reconcile_positions(self) -> None:
        """Seed the P&L ledger AND the exit manager from positions already open on
        Kalshi, so a restart mid-slate doesn't orphan live contracts (they'd go
        unmanaged — no score-state exits). Reconciled positions carry no analyst
        exit_rules, so they're protected by check_thesis_death (score state) only."""
        from engine.rules import OpenPosition
        try:
            rows = await self.rest.get_positions()
        except Exception as exc:  # noqa: BLE001
            print(f"[reconcile] could not fetch positions ({exc!r}); starting flat.")
            return
        seeded = 0
        for r in rows:
            ticker = r.get("ticker")
            # Kalshi shape (verified live 2026-06-27): position_fp is a signed
            # fixed-point STRING (+ = long YES, - = long NO); amounts are *_dollars
            # strings. (NOT `position`/`total_traded` ints.)
            pos = int(round(float(r.get("position_fp", 0) or 0)))
            if not ticker or pos == 0:
                continue
            if ticker not in self.tickers:
                print(f"[reconcile] holding {pos:+d} {ticker} — NOT in this board, "
                      f"won't be managed. Close it manually if unwanted.")
                continue
            side = "yes" if pos > 0 else "no"
            qty = abs(pos)
            traded_cents = round(float(r.get("total_traded_dollars",
                                 r.get("market_exposure_dollars", 0)) or 0) * 100)
            entry = min(99, max(1, round(traded_cents / qty))) if (traded_cents and qty) else 50
            # P&L ledger
            p = self.broker._pos(ticker)
            leg = p.yes if side == "yes" else p.no
            leg.qty, leg.avg_cost_cents = qty, float(entry)
            self.broker.cash_cents -= qty * entry   # keep the ledger internally consistent
            # exit manager
            self.rules.positions[ticker] = OpenPosition(
                ticker=ticker, side=side, entry_price_cents=entry, qty=qty,
                entry_tick=0, entry_wall=time.time(), exit_rules=[],
                thesis_id="reconciled")
            seeded += 1
            print(f"[reconcile] resumed {side} {ticker} x{qty} @ ~{entry}c "
                  f"(score-state exit only)")
        if seeded:
            print(f"[reconcile] seeded {seeded} open position(s) from Kalshi.")

    # ===== heartbeat (clean line every N seconds) ======================
    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            books = self.ws.books
            connected = sum(1 for b in books.values() if b.seq is not None or b.yes or b.no)
            unreal = self.broker.unrealized_cents(books)
            real = self.broker.realized_cents()
            equity = self.broker.cash_cents + self.broker.market_value_cents(books)
            lat = f"{self.last_analyst_latency_ms:.0f}ms" if self.last_analyst_latency_ms else "n/a"
            kill = " [STOP]" if self.broker.kill_switch_engaged() else ""
            npos = sum(1 for p in self.broker.positions.values() if p.net_contracts())
            print(f"[hb] markets={connected}/{len(books)} conn_seq={self.ws._expected_seq} "
                  f"ticks={self.ticks} decisions={self.decisions} pos={npos} "
                  f"cash=${self.broker.cash_cents/100:.2f} realized=${real/100:+.2f} "
                  f"unreal=${unreal/100:+.2f} equity=${equity/100:.2f} "
                  f"analyst={lat} scanned={self.last_markets_scanned}{kill}")
            seqs = " ".join(f"{t.replace('KXWCGAME-','').replace('KXWC','')}:{b.seq}"
                            for t, b in books.items())
            print(f"     last_seq: {seqs}")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.heartbeat_seconds)
            except asyncio.TimeoutError:
                pass

    async def _analyst_scheduler(self) -> None:
        """Pregame board scan at startup, then ONLY the slow refresh timer in
        dead time. Event-driven calls come from _on_score_event, never a loop."""
        interval = self.cfg["analyst"].get("llm_refresh_seconds", 180)
        await asyncio.sleep(4)  # let the first WS snapshots land
        for eid in self.event_to_markets:
            self.pregame_done.add(eid)
        await self._run_analyst("pregame", time.monotonic(), time.time())
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                await self._run_analyst("refresh", time.monotonic(), time.time())

    # ===== lifecycle ===================================================
    async def run(self) -> None:
        self._loop = asyncio.get_running_loop()
        if self.dry_run or self.live:
            try:
                self.live_balance_cents = await self.rest.get_balance_cents()
            except Exception as exc:  # noqa: BLE001
                self.live_balance_cents = 2000
                print(f"[warn] balance fetch failed ({exc!r}); assuming $20.00")
        if self.dry_run:
            print(f"[dry-run] REAL account balance ${self.live_balance_cents/100:.2f} "
                  f"— clips sized at {self.live_fraction*100:.0f}% of it, NOTHING is sent.")
        if self.live:
            from engine.live_broker import LiveBroker
            self.live_broker = LiveBroker(self.cfg, self.rest, self.run_dir,
                                          self.live_balance_cents)
            print(f"[LIVE] *** REAL MONEY *** balance ${self.live_balance_cents/100:.2f} | "
                  f"clip {self.live_fraction*100:.0f}% (cap ${self.max_clip_dollars}) | "
                  f"daily-loss-stop ${self.daily_loss_limit_cents/100:.2f} | "
                  f"STOP file halts orders.")
        if self.live or self.dry_run:
            await self._reconcile_positions()
        if self.broker.kill_switch_engaged():
            print(f"[warn] STOP file present in {self.run_dir} — no orders will be simulated.")
        tasks = [
            asyncio.create_task(self.ws.run()),
            asyncio.create_task(self.score.run()),
            asyncio.create_task(self._settlement_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._analyst_scheduler()),
            asyncio.create_task(self._lambda_loop()),   # pre-goal λ capture
            asyncio.create_task(self.sharp.run()),      # sharp feed (no-op if "none")
        ]
        await self._stop.wait()
        self.ws.stop(); self.score.stop(); self.sharp.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def request_stop(self, *_):
        if self._loop:
            self._loop.call_soon_threadsafe(self._stop.set)

    async def aclose(self) -> None:
        await self.rest.aclose()
        try:
            await self.score.provider.aclose()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    # ===== session summary =============================================
    def print_summary(self) -> None:
        print("\n" + "=" * 64)
        mode = ("LIVE — REAL orders were placed (ledger $ below is the paper bankroll, "
                "NOT your Kalshi cash)" if self.live
                else "paper_mode — no real orders were placed")
        print(f"SESSION SUMMARY  ({mode})")
        total, accepted = self.recorder.all_decisions_count()
        mean_lat, p95_lat = self.recorder.analyst_latency_stats()
        mean_scanned = self.recorder.mean_markets_scanned()
        drift = self.recorder.mean_price_drift()
        unreal = self.broker.unrealized_cents(self.ws.books)
        real = self.broker.realized_cents()
        equity = self.broker.cash_cents + self.broker.market_value_cents(self.ws.books)
        # single authoritative realized = broker (cash-based, net of fees). The
        # exits table is a per-exit diagnostic, now de-duped (clear-on-exit).
        start = self.broker.start_cash_cents
        print(f"  trades (decisions): {accepted} accepted / {total} total")
        # Score the lanes separately (the validation sample needs this split)
        lane_counts = self.recorder.lane_entry_counts()
        lane_of = self.recorder.lane_of_ticker()
        lane_exit_pnl: dict[str, int] = {}
        for (tk, _e, _t, _r, _f, _s, rpnl, _h, _g) in self.recorder.exit_rows():
            ln = lane_of.get(tk, "edge")
            lane_exit_pnl[ln] = lane_exit_pnl.get(ln, 0) + (rpnl or 0)
        for ln in sorted(set(lane_counts) | set(lane_exit_pnl)):
            print(f"  lane[{ln}]: entries={lane_counts.get(ln, 0)}  "
                  f"exit-realized ${lane_exit_pnl.get(ln, 0)/100:+.2f}")
        print(f"  sim P&L:   realized ${real/100:+.2f}  unrealized ${unreal/100:+.2f}  "
              f"equity ${equity/100:.2f} (start ${start/100:.2f})")
        print(f"  reconcile: start + realized + unrealized = "
              f"${(start + real + unreal)/100:.2f}  vs equity ${equity/100:.2f}  "
              f"(residual ${(equity - start - real - unreal)/100:+.2f} = open-entry fees)")
        print(f"  analyst latency: mean "
              f"{(f'{mean_lat:.0f}ms' if mean_lat else 'n/a')}  "
              f"95p {(f'{p95_lat:.0f}ms' if p95_lat else 'n/a')}")
        print(f"  mean markets_scanned per call: "
              f"{(f'{mean_scanned:.1f}' if mean_scanned is not None else 'n/a')}")
        print(f"  mean price_drift during analyst latency: "
              f"{(f'{drift:+.2f}c' if drift is not None else 'n/a')}")
        # per-position exit lifecycle
        exits = self.recorder.exit_rows()
        if exits:
            print("  exits (entry -> trigger | held | realized | score->sell gap):")
            for (tk, entry, trig, ref, fillp, sz, rpnl, held, gap) in exits:
                gap_s = f" gap={gap:.0f}ms" if gap is not None else ""
                print(f"    {tk} {entry}c -> {trig} @{fillp} x{sz} | "
                      f"{held:.0f}s | ${rpnl/100:+.2f}{gap_s}")
        else:
            print("  exits: none fired")
        # active-exit P&L vs hold-to-settlement (where markets settled)
        active = self.recorder.active_exit_pnl_cents()
        settle_rows = self.recorder.settlements_deduped()   # one row per ticker
        if settle_rows:
            print("  settled markets (realized vs hold-to-resolution):")
            for tk, rs, rp, hp in settle_rows:
                print(f"    {tk} -> {rs}: ${rp/100:+.2f} vs ${hp/100:+.2f} "
                      f"(diff ${ (rp-hp)/100:+.2f})")
        hold_cf = sum(hp for _, _, _, hp in settle_rows)
        print(f"  active-exit realized ${active/100:+.2f}"
              + (f"  vs hold-to-settlement ${hold_cf/100:+.2f}" if settle_rows
                 else "  (no markets settled this session)"))
        print("=" * 64)


async def _amain(cfg: dict, max_seconds: float | None = None,
                 dry_run: bool = False, live: bool = False) -> None:
    h = Harness(cfg, dry_run=dry_run, live=live)
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, h.request_stop)
        except NotImplementedError:
            pass
    if max_seconds:
        async def _auto_stop():
            await asyncio.sleep(max_seconds)
            print(f"\n[main] --max-seconds {max_seconds:.0f} elapsed; shutting down.")
            h.request_stop()
        loop.create_task(_auto_stop())
    try:
        await h.run()
    finally:
        h.print_summary()
        await h.aclose()
        h.recorder.close()


def _load_dotenv() -> None:
    """Load a local .env (gitignored) if python-dotenv is available, so secrets
    never need to be exported by hand or pasted into a terminal/chat."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


async def _run_discover(cfg: dict, games: list[str]) -> None:
    signer = KalshiSigner.from_config(cfg)
    rest = KalshiREST(cfg, signer)
    try:
        await discover(cfg, rest, games)
    finally:
        await rest.aclose()


def main() -> None:
    ap = argparse.ArgumentParser(description="Kalshi World Cup paper-trading harness")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--max-seconds", type=float, default=None,
                    help="auto-shutdown after N seconds (for time-boxed/demo runs)")
    ap.add_argument("--discover", nargs="+", metavar="GAME",
                    help="list CORE markets for one or more game codes/event tickers "
                         "(e.g. --discover 26JUN23PORUZB KXWCGAME-26JUN23ENGGHA) and exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="real prices + real-balance sizing, prints the order it WOULD "
                         "place, sends NOTHING (last check before live)")
    ap.add_argument("--live", action="store_true",
                    help="place REAL orders (requires paper_mode:false, live.armed:true, --arm)")
    ap.add_argument("--arm", action="store_true",
                    help="explicit confirmation that you are placing REAL money orders")
    args = ap.parse_args()
    _load_dotenv()
    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)
    if args.discover:
        asyncio.run(_run_discover(cfg, args.discover))
        return
    if args.live:
        # Safety-gate contract FIRST: refuse (naming the gate) before anything
        # else, even without --arm, so a disabled safety gate can never reach live.
        require_safety_gates(cfg)
        # TRIPLE GATE for real money.
        if cfg.get("paper_mode", True):
            raise SystemExit("--live requires `paper_mode: false` in config.yaml.")
        if not cfg.get("live", {}).get("armed", False):
            raise SystemExit("--live requires `live.armed: true` in config.yaml.")
        if not args.arm:
            raise SystemExit("--live requires the --arm flag (confirms REAL orders).")
        print("*** LIVE REAL-MONEY MODE — real orders will be placed on your Kalshi "
              "account. Ctrl-C to stop; create a STOP file to halt new orders. ***")
    elif not cfg.get("paper_mode", True):
        raise SystemExit("paper_mode is false. Pass --live --arm to trade real money, "
                         "or set paper_mode: true (use --dry-run to preview safely).")
    asyncio.run(_amain(cfg, max_seconds=args.max_seconds, dry_run=args.dry_run,
                       live=args.live))


if __name__ == "__main__":
    main()
