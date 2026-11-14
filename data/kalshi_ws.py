"""Authenticated Kalshi WebSocket client.

Subscribes to orderbook_delta + ticker for the configured tickers, maintains a
local OrderBook per market, and stamps every inbound message with a *monotonic*
receive timestamp the instant it arrives. Reconnects with exponential backoff
and resnapshots on sequence gaps.

The hot path is deliberately dumb: on each message we update the book and invoke
a synchronous callback. No LLM, no network, no awaits in the callback path.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Awaitable, Callable, Optional

import websockets

from .kalshi_auth import KalshiSigner
from .orderbook import OrderBook, _to_cents


class KalshiWS:
    def __init__(
        self,
        cfg: dict,
        signer: KalshiSigner,
        on_orderbook: Callable[[OrderBook, float], None],
        on_ticker: Optional[Callable[[dict, float], None]] = None,
        rest=None,
    ):
        self.cfg = cfg
        self.signer = signer
        self.rest = rest                      # KalshiREST, for fresh snapshots on gap
        self.url = cfg["kalshi"]["ws_url"]
        self.sign_path = cfg["kalshi"]["ws_sign_path"]
        self.tickers = list(cfg["market_tickers"])
        self.on_orderbook = on_orderbook
        self.on_ticker = on_ticker
        self.books: dict[str, OrderBook] = {t: OrderBook(t) for t in self.tickers}
        self._cmd_id = 0
        self._stop = asyncio.Event()
        # Kalshi `seq` is a CONNECTION-level counter (monotonic across all
        # messages on the socket), not per-market. Track it here; a break in
        # continuity means we dropped messages and must resync.
        self._expected_seq: Optional[int] = None
        self._last_resub_monotonic = 0.0
        self._resub_debounce_s = 5.0
        self._recovering = False
        self.recovery_fails: dict[str, int] = {}   # per-market failed REST recoveries
        self.degraded: set[str] = set()             # markets dropped after 3 failures
        self.connected = False                      # for the blind gate

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_once()
                backoff = 1.0  # reset on clean exit
            except Exception as exc:  # noqa: BLE001 — log + retry, never die
                print(f"[ws] disconnected: {exc!r}; reconnecting in {backoff:.0f}s")
                await asyncio.wait([asyncio.create_task(self._stop.wait())], timeout=backoff)
                backoff = min(backoff * 2, 60.0)

    async def _connect_once(self) -> None:
        headers = self.signer.ws_headers(self.sign_path)
        # websockets uses `additional_headers` (>=12) / `extra_headers` (older).
        try:
            ws_cm = websockets.connect(self.url, additional_headers=headers, ping_interval=10)
        except TypeError:
            ws_cm = websockets.connect(self.url, extra_headers=headers, ping_interval=10)
        async with ws_cm as ws:
            print(f"[ws] connected {self.url}")
            self._expected_seq = None  # fresh connection -> re-anchor seq
            self.connected = True
            try:
                await self._subscribe(ws)
                async for raw in ws:
                    recv_ts = time.monotonic()  # stamp the instant it lands
                    if self._stop.is_set():
                        break
                    self._handle(json.loads(raw), recv_ts, ws)
            finally:
                self.connected = False

    async def _subscribe(self, ws) -> None:
        self._cmd_id += 1
        await ws.send(json.dumps({
            "id": self._cmd_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta", "ticker"],
                "market_tickers": [t for t in self.tickers if t not in self.degraded],
            },
        }))

    def _handle(self, data: dict, recv_ts: float, ws) -> None:
        mtype = data.get("type")
        msg = data.get("msg", data)
        seq = data.get("seq")

        # Connection-level sequence continuity: a real gap means we dropped
        # messages on the socket -> resync (debounced, logged once).
        if seq is not None and mtype in ("orderbook_snapshot", "orderbook_delta"):
            if self._expected_seq is not None and seq != self._expected_seq + 1:
                self._maybe_resubscribe(ws, seq)
            self._expected_seq = seq

        if mtype == "orderbook_snapshot":
            book = self._book(msg)
            book.apply_snapshot({**msg, "seq": seq}, recv_ts)
            self.on_orderbook(book, recv_ts)
        elif mtype == "orderbook_delta":
            book = self._book(msg)
            book.apply_delta(msg, recv_ts, seq=seq)
            self.on_orderbook(book, recv_ts)
        elif mtype == "ticker":
            if self.on_ticker:
                self.on_ticker({**msg, "_recv_monotonic": recv_ts}, recv_ts)
        elif mtype == "error":
            print(f"[ws] server error: {msg}")
        # subscribed/ok acks ignored

    def _maybe_resubscribe(self, ws, seq: int) -> None:
        now = time.monotonic()
        if self._recovering or now - self._last_resub_monotonic < self._resub_debounce_s:
            return  # debounce: never a tight resubscribe loop
        self._last_resub_monotonic = now
        self._recovering = True
        print(f"[ws] sequence gap (got {seq}, expected "
              f"{self._expected_seq + 1 if self._expected_seq is not None else '?'}); recovering")
        self._expected_seq = None  # re-anchor on the next message
        # The gap is CONNECTION-level, so EVERY book is now suspect
        # until its fresh snapshot lands. Mark all dirty; _recover clears each.
        for b in self.books.values():
            b.dirty = True
        asyncio.create_task(self._recover(ws))

    async def _recover(self, ws) -> None:
        """On a gap: refetch a fresh REST snapshot per market (backoff + degrade
        after 3 failures, keeping the rest of the board live), then resubscribe
        ONCE. No tight loop."""
        try:
            if self.rest is not None:
                for t in self.tickers:
                    if t in self.degraded:
                        continue
                    try:
                        fresh = await self.rest.get_orderbook(t)
                        book = self.books.setdefault(t, OrderBook(t))
                        book.yes, book.no = fresh.yes, fresh.no
                        book.last_recv_monotonic = time.monotonic()
                        book.dirty = False       # fresh REST snapshot -> trustworthy again
                        self.recovery_fails[t] = 0
                        self.on_orderbook(book, book.last_recv_monotonic)
                    except Exception as exc:  # noqa: BLE001
                        self.recovery_fails[t] = self.recovery_fails.get(t, 0) + 1
                        if self.recovery_fails[t] >= 3:
                            self.degraded.add(t)
                            print(f"[ws] {t} DEGRADED after 3 failed recoveries: {exc!r}")
                        await asyncio.sleep(min(2 ** self.recovery_fails[t], 10))
            await self._subscribe(ws)
        except Exception as exc:  # noqa: BLE001
            print(f"[ws] recovery failed: {exc!r}")
        finally:
            self._recovering = False

    def _book(self, msg: dict) -> OrderBook:
        t = msg.get("market_ticker") or msg.get("market_id")
        if t not in self.books:
            self.books[t] = OrderBook(t)
        return self.books[t]
