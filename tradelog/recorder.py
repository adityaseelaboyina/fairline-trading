"""SQLite recorder — the point of the project.

NOTE: this package is named `tradelog`, not `logging`, on purpose: a top-level
`logging/` directory would shadow Python's stdlib `logging` module.

Every decision is logged with the full timing chain so we can measure how much
the market moved while the model was thinking:

    event_ts            receive-stamp (monotonic + wall) of the triggering tick/event
    decision_ts         when the deterministic engine decided
    llm_call_start_ts   } present only if the analyst was involved in this thesis
    llm_call_end_ts     }
    decision_price      best/mid at decision time
    sim_fill_price      what walking the live book actually gave
    price_drift         DERIVED: mid at (decision_ts + measured_llm_latency) - decision_price
                        i.e. the slippage you'd eat if the decision were delayed by
                        one model-latency. Filled in later by a deferred sampler.

On settlement we log resolution + realized P&L per position, alongside the
hold-to-resolution counterfactual.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS analyst_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reason TEXT,                -- pregame | event:goal | event:red | event:half | refresh
    markets_scanned INTEGER,    -- board width at call time (latency-vs-width analysis)
    start_wall REAL, end_wall REAL,
    start_monotonic REAL, end_monotonic REAL,
    latency_ms REAL,
    parsed_ok INTEGER,
    raw_json TEXT,
    thesis_id TEXT
);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thesis_id TEXT,
    trigger TEXT,
    ticker TEXT,
    side TEXT, action TEXT,
    requested_size INTEGER, filled_size INTEGER,
    event_wall REAL, event_monotonic REAL,
    decision_wall REAL, decision_monotonic REAL,
    llm_call_start_wall REAL, llm_call_end_wall REAL,
    llm_latency_ms REAL,
    decision_price_cents INTEGER,
    sim_fill_price_cents REAL,
    fee_cents INTEGER,
    accepted INTEGER,
    reason TEXT,
    -- derived; NULL until the deferred sampler fills it
    price_drift_cents REAL,
    lane TEXT DEFAULT 'edge',   -- 'edge' | 'reaction' | 'snipe_a' | 'snipe_b'
    fair_source TEXT,           -- sharp | ladder | analyst (fair-resolution order)
    exec_style TEXT             -- taker | maker | maker_timeout_cross
);

CREATE TABLE IF NOT EXISTS exits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT,
    thesis_id TEXT,
    entry_price_cents INTEGER,
    trigger TEXT,                       -- stop_loss | take_profit | rule:price<=NN | rule:red_card:TEAM ...
    reference_price_compared INTEGER,   -- the price the trigger compared against
    sim_fill_price_cents REAL,          -- what walking the book actually gave
    size INTEGER,
    realized_pnl_cents INTEGER,
    seconds_held REAL,
    score_event_to_sell_gap_ms REAL,    -- detection -> sell decision (event-driven exits)
    ts_wall REAL
);

CREATE TABLE IF NOT EXISTS settlements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT,
    result TEXT,
    settled_pnl_cents INTEGER,
    realized_pnl_cents INTEGER,
    hold_to_resolution_pnl_cents INTEGER,
    ts_wall REAL
);

-- every confirmed score change, with the full latency chain
CREATE TABLE IF NOT EXISTS goal_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT, team TEXT, minute REAL,
    old_home INTEGER, old_away INTEGER, new_home INTEGER, new_away INTEGER,
    espn_wall REAL,          -- when ESPN's payload carried it
    detect_wall REAL,        -- when we detected it
    spike_wall REAL,         -- book-spike pre-trigger time, if one fired
    reverted INTEGER DEFAULT 0,  -- VAR / correction
    scorer_was_favourite INTEGER,  -- from PRE-goal fair, never post-goal books
    minute_bucket TEXT             -- 0-30 | 31-60 | 61-90+
);

-- Tasks 11/12 shadow mode: every qualifying snipe, depth-priced (never mids)
CREATE TABLE IF NOT EXISTS shadow_snipes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    goal_event_id INTEGER, klass TEXT,        -- 'a' | 'b'
    ticker TEXT, side TEXT,
    would_price_cents REAL,                   -- WORST level needed to fill the clip
    ask_cents REAL, contracts INTEGER,
    fair_cents REAL, gap_cents REAL,          -- Class B only
    detect_ms REAL, calc_ms REAL, gates_ms REAL,   -- latency split
    book_30s REAL, book_60s REAL, book_120s REAL,  -- side ask at +30/60/120s
    blocked_reason TEXT,                      -- gate that would have blocked it, if any
    armed INTEGER DEFAULT 0,                  -- 1 if this snipe also fired live
    outcome TEXT,                             -- armed snipes: filled|no_fill|cap_full|
                                              --   verify_failed|no_size|order_failed
    wire_ms REAL,                             -- detect->order-on-wire latency (armed only)
    rest_ask_cents REAL,                      -- VENUE-truth ask (REST) vs our WS
    ws_dirty INTEGER,                         --   ask, to prove stale-book vs genuine-thin
    wall REAL
);

-- book path after EVERY goal (traded or not)
CREATE TABLE IF NOT EXISTS overshoot_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    goal_event_id INTEGER, ticker TEXT, offset_s INTEGER,
    yes_bid REAL, yes_ask REAL, no_bid REAL, no_ask REAL,
    yes_ask_depth INTEGER, no_ask_depth INTEGER, wall REAL
);

-- Order-response forensics: persist the FULL venue reply to every live
-- order, so a no-fill is diagnosable (400 reject vs accepted-then-expired IOC vs
-- instant-expiry) instead of a discarded fill_count=0. Turns every future no-fill
-- into data.
CREATE TABLE IF NOT EXISTS order_responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lane TEXT, ticker TEXT, side TEXT, order_px_cents INTEGER,
    requested INTEGER, fill_count REAL, book_was_dirty INTEGER,
    status TEXT, raw_json TEXT, wall REAL
);

CREATE TABLE IF NOT EXISTS score_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT, event_id TEXT, team TEXT,
    home TEXT, away TEXT, home_score INTEGER, away_score INTEGER,
    period INTEGER, clock TEXT,
    recv_monotonic REAL, wall REAL, detail TEXT,
    -- for the "never trust Kalshi last-play" audit: what Kalshi claimed, if anything
    kalshi_lastplay TEXT
);
"""


class Recorder:
    def __init__(self, run_dir: str, db_name: str = "harness.sqlite"):
        Path(run_dir).mkdir(parents=True, exist_ok=True)
        self.path = str(Path(run_dir) / db_name)
        self.db = sqlite3.connect(self.path)
        self.db.executescript(SCHEMA)
        # migrations for DBs created before newer columns existed
        for stmt in (
            "ALTER TABLE decisions ADD COLUMN lane TEXT DEFAULT 'edge'",
            "ALTER TABLE decisions ADD COLUMN fair_source TEXT",
            "ALTER TABLE decisions ADD COLUMN exec_style TEXT",
            "ALTER TABLE goal_events ADD COLUMN scorer_was_favourite INTEGER",
            "ALTER TABLE goal_events ADD COLUMN minute_bucket TEXT",
            "ALTER TABLE shadow_snipes ADD COLUMN outcome TEXT",
            "ALTER TABLE shadow_snipes ADD COLUMN wire_ms REAL",
            "ALTER TABLE shadow_snipes ADD COLUMN rest_ask_cents REAL",
            "ALTER TABLE shadow_snipes ADD COLUMN ws_dirty INTEGER",
        ):
            try:
                self.db.execute(stmt)
            except sqlite3.OperationalError:
                pass
        self.db.commit()

    def close(self) -> None:
        self.db.commit()
        self.db.close()

    # ---- analyst -------------------------------------------------------
    def log_analyst_call(self, *, reason, markets_scanned, start_wall, end_wall,
                         start_monotonic, end_monotonic, parsed_ok, raw_json,
                         thesis_id) -> int:
        latency_ms = (end_monotonic - start_monotonic) * 1000.0
        cur = self.db.execute(
            """INSERT INTO analyst_calls(reason,markets_scanned,start_wall,end_wall,
               start_monotonic,end_monotonic,latency_ms,parsed_ok,raw_json,thesis_id)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (reason, markets_scanned, start_wall, end_wall, start_monotonic,
             end_monotonic, latency_ms, int(parsed_ok), raw_json, thesis_id),
        )
        self.db.commit()
        return cur.lastrowid

    def analyst_latency_stats(self):
        rows = [r[0] for r in self.db.execute(
            "SELECT latency_ms FROM analyst_calls WHERE latency_ms IS NOT NULL")]
        return _mean_p95(rows)

    def mean_markets_scanned(self):
        rows = [r[0] for r in self.db.execute(
            "SELECT markets_scanned FROM analyst_calls WHERE markets_scanned IS NOT NULL")]
        return (sum(rows) / len(rows)) if rows else None

    # ---- decisions -----------------------------------------------------
    def log_decision(self, *, thesis_id, trigger, fill, event_monotonic, event_wall,
                     decision_monotonic, decision_wall, llm_start_wall, llm_end_wall,
                     llm_latency_ms, lane: str = "edge", fair_source: str = None,
                     exec_style: str = None) -> int:
        cur = self.db.execute(
            """INSERT INTO decisions(thesis_id,trigger,ticker,side,action,
               requested_size,filled_size,event_wall,event_monotonic,decision_wall,
               decision_monotonic,llm_call_start_wall,llm_call_end_wall,llm_latency_ms,
               decision_price_cents,sim_fill_price_cents,fee_cents,accepted,reason,
               price_drift_cents,lane,fair_source,exec_style)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,?,?)""",
            (thesis_id, trigger, fill.ticker, fill.side, fill.action,
             fill.requested_size, fill.filled_size, event_wall, event_monotonic,
             decision_wall, decision_monotonic, llm_start_wall, llm_end_wall,
             llm_latency_ms, fill.decision_price_cents, fill.sim_fill_price_cents,
             fill.fee_cents, int(fill.accepted), fill.reason, lane, fair_source,
             exec_style),
        )
        self.db.commit()
        return cur.lastrowid

    # ---- per-lane reporting ------------------------------------
    def lane_of_ticker(self) -> dict:
        """ticker -> lane of its most recent accepted BUY (for scoring exits/settles)."""
        return dict(self.db.execute(
            """SELECT ticker, COALESCE(lane,'edge') FROM decisions WHERE id IN
                 (SELECT MAX(id) FROM decisions
                  WHERE accepted=1 AND action='buy' GROUP BY ticker)"""))

    def lane_entry_counts(self) -> dict:
        return dict(self.db.execute(
            "SELECT COALESCE(lane,'edge'), COUNT(*) FROM decisions "
            "WHERE accepted=1 AND action='buy' GROUP BY 1"))

    def set_price_drift(self, decision_id: int, mid_after_latency: float) -> None:
        row = self.db.execute(
            "SELECT decision_price_cents FROM decisions WHERE id=?", (decision_id,)
        ).fetchone()
        if not row or row[0] is None:
            return
        drift = mid_after_latency - row[0]
        self.db.execute("UPDATE decisions SET price_drift_cents=? WHERE id=?",
                        (drift, decision_id))
        self.db.commit()

    # ---- exits ---------------------------------------------------------
    def log_exit(self, *, ticker, thesis_id, entry_price_cents, trigger,
                 reference_price_compared, sim_fill_price_cents, size,
                 realized_pnl_cents, seconds_held, score_event_to_sell_gap_ms) -> int:
        cur = self.db.execute(
            """INSERT INTO exits(ticker,thesis_id,entry_price_cents,trigger,
               reference_price_compared,sim_fill_price_cents,size,realized_pnl_cents,
               seconds_held,score_event_to_sell_gap_ms,ts_wall)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (ticker, thesis_id, entry_price_cents, trigger, reference_price_compared,
             sim_fill_price_cents, size, realized_pnl_cents, seconds_held,
             score_event_to_sell_gap_ms, time.time()),
        )
        self.db.commit()
        return cur.lastrowid

    def exit_rows(self):
        return self.db.execute(
            """SELECT ticker,entry_price_cents,trigger,reference_price_compared,
               sim_fill_price_cents,size,realized_pnl_cents,seconds_held,
               score_event_to_sell_gap_ms FROM exits ORDER BY id""").fetchall()

    def active_exit_pnl_cents(self):
        rows = [r[0] for r in self.db.execute("SELECT realized_pnl_cents FROM exits")]
        return sum(rows) if rows else 0

    # ---- settlement ----------------------------------------------------
    def log_settlement(self, *, ticker, result, settled_pnl_cents,
                       realized_pnl_cents, hold_to_resolution_pnl_cents) -> None:
        self.db.execute(
            """INSERT INTO settlements(ticker,result,settled_pnl_cents,
               realized_pnl_cents,hold_to_resolution_pnl_cents,ts_wall)
               VALUES(?,?,?,?,?,?)""",
            (ticker, result, settled_pnl_cents, realized_pnl_cents,
             hold_to_resolution_pnl_cents, time.time()),
        )
        self.db.commit()

    # ---- order-response forensics --------------------------
    def log_order_response(self, *, lane, ticker, side, order_px_cents, requested,
                           resp, book_was_dirty=False) -> None:
        import json as _json
        resp = resp or {}
        self.db.execute(
            """INSERT INTO order_responses(lane,ticker,side,order_px_cents,requested,
               fill_count,book_was_dirty,status,raw_json,wall)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (lane, ticker, side, order_px_cents, requested,
             float(resp.get("fill_count") or 0), int(book_was_dirty),
             str(resp.get("status") or ""), _json.dumps(resp)[:1000], time.time()))
        self.db.commit()

    # ---- sniper lane --------------------------------------
    def log_goal_event(self, *, event_id, team, minute, old_home, old_away,
                       new_home, new_away, espn_wall, detect_wall,
                       spike_wall=None, scorer_was_favourite=None,
                       minute_bucket=None) -> int:
        cur = self.db.execute(
            """INSERT INTO goal_events(event_id,team,minute,old_home,old_away,
               new_home,new_away,espn_wall,detect_wall,spike_wall,
               scorer_was_favourite,minute_bucket)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (event_id, team, minute, old_home, old_away, new_home, new_away,
             espn_wall, detect_wall, spike_wall, scorer_was_favourite,
             minute_bucket))
        self.db.commit()
        return cur.lastrowid

    def mark_goal_reverted(self, goal_event_id: int) -> None:
        self.db.execute("UPDATE goal_events SET reverted=1 WHERE id=?", (goal_event_id,))
        self.db.commit()

    def log_shadow_snipe(self, *, goal_event_id, klass, ticker, side,
                         would_price_cents, ask_cents, contracts, fair_cents=None,
                         gap_cents=None, detect_ms=None, calc_ms=None, gates_ms=None,
                         blocked_reason=None, armed=0) -> int:
        cur = self.db.execute(
            """INSERT INTO shadow_snipes(goal_event_id,klass,ticker,side,
               would_price_cents,ask_cents,contracts,fair_cents,gap_cents,
               detect_ms,calc_ms,gates_ms,blocked_reason,armed,wall)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (goal_event_id, klass, ticker, side, would_price_cents, ask_cents,
             contracts, fair_cents, gap_cents, detect_ms, calc_ms, gates_ms,
             blocked_reason, armed, time.time()))
        self.db.commit()
        return cur.lastrowid

    def set_snipe_outcome(self, snipe_id: int, outcome: str,
                          wire_ms: float = None) -> None:
        """Record what actually happened when an ARMED snipe tried to fire live
        (filled / no_fill / cap_full / verify_failed / no_size / order_failed) —
        so we can distinguish 'no edge' from 'blocked' without reading the console."""
        self.db.execute("UPDATE shadow_snipes SET outcome=?, wire_ms=? WHERE id=?",
                        (outcome, wire_ms, snipe_id))
        self.db.commit()

    def set_snipe_rest_ask(self, snipe_id: int, rest_ask, ws_dirty: bool) -> None:
        """Venue-truth crosscheck: the REST orderbook ask at snipe time
        vs what our WS book claimed. WS 42c + REST 99c = stale book, proven."""
        self.db.execute("UPDATE shadow_snipes SET rest_ask_cents=?, ws_dirty=? WHERE id=?",
                        (rest_ask, int(ws_dirty), snipe_id))
        self.db.commit()

    def update_shadow_book(self, snipe_id: int, offset_s: int, ask: float) -> None:
        col = {30: "book_30s", 60: "book_60s", 120: "book_120s"}.get(offset_s)
        if col:
            self.db.execute(f"UPDATE shadow_snipes SET {col}=? WHERE id=?",
                            (ask, snipe_id))
            self.db.commit()

    def log_overshoot(self, *, goal_event_id, ticker, offset_s, yes_bid, yes_ask,
                      no_bid, no_ask, yes_ask_depth, no_ask_depth) -> None:
        self.db.execute(
            """INSERT INTO overshoot_paths(goal_event_id,ticker,offset_s,yes_bid,
               yes_ask,no_bid,no_ask,yes_ask_depth,no_ask_depth,wall)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (goal_event_id, ticker, offset_s, yes_bid, yes_ask, no_bid, no_ask,
             yes_ask_depth, no_ask_depth, time.time()))
        self.db.commit()

    # ---- score ---------------------------------------------------------
    def log_score_event(self, ev, kalshi_lastplay: Optional[str] = None) -> None:
        s = ev.state
        self.db.execute(
            """INSERT INTO score_events(kind,event_id,team,home,away,home_score,
               away_score,period,clock,recv_monotonic,wall,detail,kalshi_lastplay)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ev.kind, ev.event_id, ev.team, s.home, s.away, s.home_score,
             s.away_score, s.period, s.clock, ev.recv_monotonic, ev.wall_ts,
             ev.detail, kalshi_lastplay),
        )
        self.db.commit()

    # ---- summary helpers ----------------------------------------------
    def latency_stats(self):
        rows = [r[0] for r in self.db.execute(
            "SELECT llm_latency_ms FROM decisions WHERE accepted=1 AND llm_latency_ms IS NOT NULL")]
        return _mean_p95(rows)

    def mean_price_drift(self):
        rows = [r[0] for r in self.db.execute(
            "SELECT price_drift_cents FROM decisions WHERE price_drift_cents IS NOT NULL")]
        return (sum(rows) / len(rows)) if rows else None

    def accepted_buys_for(self, ticker: str):
        return self.db.execute(
            """SELECT side, filled_size, sim_fill_price_cents, fee_cents
               FROM decisions WHERE ticker=? AND accepted=1 AND action='buy'""",
            (ticker,)).fetchall()

    def all_decisions_count(self):
        total = self.db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        accepted = self.db.execute("SELECT COUNT(*) FROM decisions WHERE accepted=1").fetchone()[0]
        return total, accepted

    def settled_tickers(self) -> set:
        """Tickers already logged as settled — seeds Harness.settled on startup so a
        restart does not re-register (and re-double-count) prior settlements."""
        return {r[0] for r in self.db.execute("SELECT DISTINCT ticker FROM settlements")}

    def settlements_deduped(self):
        """One row per ticker (latest by id), so the summary counterfactual isn't
        double/triple-counted across reconnects/restarts."""
        return self.db.execute(
            """SELECT ticker,result,realized_pnl_cents,hold_to_resolution_pnl_cents
               FROM settlements WHERE id IN
                   (SELECT MAX(id) FROM settlements GROUP BY ticker)
               ORDER BY ticker""").fetchall()


def _mean_p95(values):
    if not values:
        return None, None
    s = sorted(values)
    mean = sum(s) / len(s)
    idx = max(0, int(round(0.95 * (len(s) - 1))))
    return mean, s[idx]
