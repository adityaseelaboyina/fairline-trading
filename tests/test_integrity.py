"""Feed integrity: dirty-book guard + order-response forensics.
The post-mortem found Class A's 0-for-13 was a STALE book (WS seq-gap, no guard),
not a missing edge. These tests lock the instrument repair.

    python tests/test_integrity.py
"""
import asyncio
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from types import SimpleNamespace as NS

from data.orderbook import OrderBook
from tradelog.recorder import Recorder
from main import Harness


def test_dirty_flag_lifecycle():
    b = OrderBook("T")
    assert b.dirty is False
    # a delta must NOT clear a dirty flag (built on suspect state)
    b.dirty = True
    b.apply_delta({"price_dollars": "0.50", "delta_fp": "1000000", "side": "yes"},
                  1.0, seq=5)
    assert b.dirty is True, "delta wrongly cleared dirty"
    # a full snapshot IS authoritative -> clean
    b.apply_snapshot({"seq": 6, "yes": [[50, 3]], "no": [[48, 3]]}, 1.0)
    assert b.dirty is False, "snapshot did not clear dirty"
    print("ok: dirty set stays through deltas, cleared only by a full snapshot")


def test_ws_gap_marks_all_books_dirty():
    from data.kalshi_ws import KalshiWS
    cfg = {"market_tickers": ["A", "B", "C"],
           "kalshi": {"ws_url": "wss://x", "ws_sign_path": "/x"}}
    ws = KalshiWS.__new__(KalshiWS)          # skip __init__ network setup
    ws.books = {t: OrderBook(t) for t in cfg["market_tickers"]}
    ws._recovering = False
    ws._last_resub_monotonic = -100.0        # ensure debounce doesn't suppress
    ws._resub_debounce_s = 5.0
    ws._expected_seq = 100
    ws.rest = None

    async def noop(*a, **k):
        pass
    ws._recover = noop

    async def go():
        ws._maybe_resubscribe(ws, 105)       # a gap (expected 101, got 105)
        await asyncio.sleep(0)               # let the (noop) recovery task run
    asyncio.run(go())
    assert all(b.dirty for b in ws.books.values()), "gap didn't mark all books dirty"
    print("ok: a connection-level seq gap marks EVERY book dirty")


def _snipe_stub(rec, book):
    s = NS(recorder=rec, ws=NS(books={"KXWCTOTAL-26JUL09FRAMAR-2": book}),
           live_broker=NS(balance_cents=9000), block_on_dirty=True,
           rest=None, max_game_fraction=0.35, gate_max_contracts=150,
           _game_cost_basis_cents=lambda c, p: 0)
    s._place_snipe = Harness._place_snipe.__get__(s)
    return s


def test_snipe_refuses_dirty_book():
    tmp = tempfile.mkdtemp()
    rec = Recorder(tmp)
    tk = "KXWCTOTAL-26JUL09FRAMAR-2"
    book = OrderBook(tk); book.yes = {42: 2}; book.no = {56: 2}; book.dirty = True
    sid = rec.log_shadow_snipe(goal_event_id=1, klass="a", ticker=tk, side="yes",
                               would_price_cents=43, ask_cents=42, contracts=3, armed=1)
    cand = {"ticker": tk, "side": "yes", "ask": 42, "worst": 43, "contracts": 3,
            "order_px": 43}
    asyncio.run(
        _snipe_stub(rec, book)._place_snipe(cand, "a", 1, time.time(), sid))
    outcome = rec.db.execute("SELECT outcome FROM shadow_snipes WHERE id=?",
                             (sid,)).fetchone()[0]
    assert outcome == "dirty_book", outcome
    # and NO order response row (nothing was ever sent)
    assert rec.db.execute("SELECT COUNT(*) FROM order_responses").fetchone()[0] == 0
    print("ok: snipe refuses a dirty book (outcome=dirty_book, no order sent)")


def test_order_response_persisted():
    tmp = tempfile.mkdtemp()
    r = Recorder(tmp)
    r.log_order_response(lane="snipe_a", ticker="KXWCTOTAL-26JUL09FRAMAR-2",
                         side="yes", order_px_cents=43, requested=3,
                         resp={"fill_count": 0, "status": "canceled",
                               "order_id": "abc"}, book_was_dirty=False)
    row = r.db.execute("SELECT lane,ticker,fill_count,status,book_was_dirty,raw_json "
                       "FROM order_responses").fetchone()
    assert row[0] == "snipe_a" and row[2] == 0.0 and row[3] == "canceled", row
    assert '"order_id": "abc"' in row[5], "raw response not persisted"
    print("ok: full order response persisted (no-fills are now diagnosable)")


def test_rest_crosscheck_records_venue_truth():
    tmp = tempfile.mkdtemp()
    rec = Recorder(tmp)
    tk = "KXWCTOTAL-26JUL09FRAMAR-2"
    # our WS book claims a cheap 42c ask; the venue REST book says ~99c (decided)
    ws_book = OrderBook(tk); ws_book.yes = {42: 2}; ws_book.no = {56: 2}; ws_book.dirty = True
    rest_book = OrderBook(tk); rest_book.yes = {99: 50}; rest_book.no = {1: 50}

    class Rest:
        async def get_orderbook(self, t):
            return rest_book
    s = NS(rest=Rest(), recorder=rec)
    s._snipe_rest_crosscheck = Harness._snipe_rest_crosscheck.__get__(s)
    sid = rec.log_shadow_snipe(goal_event_id=1, klass="a", ticker=tk, side="yes",
                               would_price_cents=43, ask_cents=42, contracts=3, armed=0)
    asyncio.run(
        s._snipe_rest_crosscheck(sid, tk, "yes", ws_book.dirty))
    wsa, rest_ask, dty = rec.db.execute(
        "SELECT ask_cents, rest_ask_cents, ws_dirty FROM shadow_snipes WHERE id=?",
        (sid,)).fetchone()
    assert (wsa, rest_ask, dty) == (42, 99, 1), (wsa, rest_ask, dty)
    print("ok: REST crosscheck records venue ask (WS 42c vs venue 99c) + dirty flag")


if __name__ == "__main__":
    test_dirty_flag_lifecycle()
    test_ws_gap_marks_all_books_dirty()
    test_snipe_refuses_dirty_book()
    test_order_response_persisted()
    test_rest_crosscheck_records_venue_truth()
    print("\nAll feed-integrity tests passed.")
