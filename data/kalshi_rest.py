"""Kalshi REST client (read-only): market metadata + settlement polling.

Only GETs are implemented. There is intentionally no order-placement method here
(that lives, unimplemented, in engine/live_broker.py). Public market-data calls
are sent signed anyway — Kalshi accepts the auth headers on public endpoints.
"""
from __future__ import annotations

from typing import Optional

import httpx

from .kalshi_auth import KalshiSigner
from .orderbook import OrderBook


class KalshiREST:
    def __init__(self, cfg: dict, signer: KalshiSigner):
        self.signer = signer
        kc = cfg["kalshi"]
        self.base = kc["rest_base"].rstrip("/")
        self.prefix = kc["rest_prefix"]
        self._client = httpx.AsyncClient(timeout=10.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        full_path = self.prefix + path
        headers = self.signer.headers("GET", full_path)
        resp = await self._client.get(self.base + full_path, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def get_market(self, ticker: str) -> dict:
        data = await self._get(f"/markets/{ticker}")
        return data.get("market", data)

    async def list_markets(self, **params) -> list[dict]:
        data = await self._get("/markets", params=params or None)
        return data.get("markets", [])

    async def get_orderbook(self, ticker: str, depth: int = 32) -> OrderBook:
        data = await self._get(f"/markets/{ticker}/orderbook", params={"depth": depth})
        ob = data.get("orderbook") or data.get("orderbook_fp") or data
        book = OrderBook(ticker)
        book.apply_snapshot(ob, recv_monotonic=0.0)
        return book

    async def get_settlement(self, ticker: str) -> dict:
        """Return {status, result, settled} for a market.

        Settlement is read off the market object: status reaches
        determined/settled/finalized and `result` is populated.
        """
        try:
            m = await self.get_market(ticker)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            # Settled markets past Kalshi's historical cutoff 404 here and are
            # served only by /historical/markets/{ticker} (docs: historical data).
            d = await self._get(f"/historical/markets/{ticker}")
            m = d.get("market", d)
        status = (m.get("status") or "").lower()
        result = m.get("result")
        settled = status in ("settled", "finalized", "determined") and bool(result)
        return {"status": status, "result": result, "settled": settled, "market": m}

    # ---- writes (REAL MONEY) — used only by LiveBroker when armed -------
    async def _post(self, path: str, body: dict) -> dict:
        """Signed POST. NOTE: Kalshi's documented signature is timestamp+METHOD+
        path (body NOT signed); this matches our verified GET scheme. If the V2
        order endpoint requires body-signing, the test order will 401 and we
        switch — verify before trusting with real money."""
        full_path = self.prefix + path
        headers = self.signer.headers("POST", full_path)
        headers["Content-Type"] = "application/json"
        resp = await self._client.post(self.base + full_path, json=body, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def get_balance_cents(self) -> int:
        """Account cash balance in integer cents."""
        d = await self._get("/portfolio/balance")
        return int(d.get("balance", 0))   # Kalshi returns cents (e.g. 3715 == $37.15)

    async def get_positions(self) -> list[dict]:
        """Open market positions for reconciliation on startup. Kalshi returns
        `market_positions` with a signed `position` (+ = long YES, - = long NO),
        `market_exposure`/`total_traded` in cents. Field names are read
        defensively since the shape has drifted across API versions."""
        d = await self._get("/portfolio/positions")
        return d.get("market_positions") or d.get("positions") or []

    async def get_fills(self, ticker: str, limit: int = 200) -> list[dict]:
        """All fills (bot + manual app trades) for a ticker. Each: action
        (buy/sell), count_fp, no_price_dollars/yes_price_dollars, fee_cost."""
        d = await self._get("/portfolio/fills", params={"ticker": ticker, "limit": limit})
        fills = d.get("fills") or []
        if not fills:
            # Fills older than Kalshi's historical cutoff are served ONLY by
            # /historical/fills (docs.kalshi.com/getting_started/historical_data).
            d = await self._get("/historical/fills",
                                params={"ticker": ticker, "limit": 1000})
            fills = d.get("fills") or []
        return fills

    async def place_order_v2(self, *, ticker: str, side: str, count: float,
                             price_dollars: float, tif: str = "immediate_or_cancel",
                             stp: str = "taker_at_cross",
                             client_order_id: Optional[str] = None,
                             post_only: bool = False) -> dict:
        """POST a V2 order. side='bid'(buy)|'ask'(sell). count/price are
        fixed-point strings. time_in_force is required by the API: use
        immediate_or_cancel for takers, good_till_canceled + post_only for a resting
        maker order."""
        body = {
            "ticker": ticker, "side": side,
            "count": f"{count:.2f}", "price": f"{price_dollars:.4f}",
            "self_trade_prevention_type": stp,
        }
        if tif:
            body["time_in_force"] = tif
        if post_only:
            body["post_only"] = True
        if client_order_id:
            body["client_order_id"] = client_order_id
        return await self._post("/portfolio/events/orders", body)

    async def get_order(self, order_id: str) -> dict:
        """Status of one order (fill_count / remaining_count / status)."""
        d = await self._get(f"/portfolio/orders/{order_id}")
        return d.get("order", d)

    async def cancel_order(self, order_id: str) -> dict:
        """Cancel a resting order. Orphaned resting orders are unacceptable —
        callers must treat a failed cancel as an error, not a shrug."""
        full_path = self.prefix + f"/portfolio/orders/{order_id}"
        headers = self.signer.headers("DELETE", full_path)
        resp = await self._client.delete(self.base + full_path, headers=headers)
        resp.raise_for_status()
        return resp.json()
