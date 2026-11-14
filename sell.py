"""Manually close (sell) a specific position at a marketable price — your call.

List what you currently hold:
    python sell.py

Sell a position (real money — requires --arm):
    python sell.py KXWCGAME-26JUN29NEDMAR-NED --arm           # sell the whole thing
    python sell.py KXWCGAME-26JUN29NEDMAR-NED --fraction 0.5 --arm   # sell half

It reads your ACTUAL Kalshi position (side + size), prices an IOC to cross the
current bid, and sells. If the bot is running, halt it first (`touch run/STOP`)
so it doesn't try to manage a position you just closed. After selling, run
`python ledger.py --live` to refresh your records.
"""
from __future__ import annotations

import argparse
import asyncio

import yaml

from data.kalshi_auth import KalshiSigner
from data.kalshi_rest import KalshiREST
from engine.live_broker import to_yes_order


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def _held(rows: list, ticker: str) -> int:
    for r in rows:
        if r.get("ticker") == ticker:
            return int(round(float(r.get("position_fp", 0) or 0)))
    return 0


async def _list(rest: KalshiREST) -> None:
    rows = await rest.get_positions()
    held = [r for r in rows if int(round(float(r.get("position_fp", 0) or 0))) != 0]
    if not held:
        print("No open positions.")
        return
    print("Your open positions (entry ~ / current bid):\n")
    for r in held:
        t = r.get("ticker")
        pos = int(round(float(r.get("position_fp", 0) or 0)))
        side, qty = ("yes" if pos > 0 else "no"), abs(pos)
        traded = float(r.get("total_traded_dollars", 0) or 0)
        entry = round(traded / qty * 100) if qty else 0
        try:
            ob = await rest.get_orderbook(t, depth=4)
            bid = ob.sellable_price(side)
        except Exception:  # noqa: BLE001
            bid = None
        bidtxt = f"{bid}c (~${qty*bid/100:.2f})" if bid is not None else "no bid"
        pl = f"{bid-entry:+d}c/contract" if bid is not None else ""
        print(f"  {t:42} {side.upper():3} x{qty:<4} entry ~{entry}c  sell now {bidtxt}  {pl}")
    print("\nTo sell one:  python sell.py <TICKER> --arm")


async def amain(cfg: dict, ticker: str | None, fraction: float, buf: int) -> None:
    rest = KalshiREST(cfg, KalshiSigner.from_config(cfg))
    try:
        if not ticker:
            await _list(rest)
            return
        pos = _held(await rest.get_positions(), ticker)
        if pos == 0:
            print(f"No open position in {ticker} — nothing to sell.")
            return
        side, qty = ("yes" if pos > 0 else "no"), abs(pos)
        count = round(qty * fraction, 2)
        if count < 0.01:
            print("Requested size rounds to 0 — nothing sent.")
            return
        ob = await rest.get_orderbook(ticker, depth=8)
        bid = ob.sellable_price(side)
        if bid is None:
            print(f"No bid to sell {side.upper()} {ticker} into (empty book). Try again later.")
            return
        px = max(1, bid - buf)                      # cross THROUGH the bid so it fills
        api_side, yes_price = to_yes_order(side, "sell", px)
        print(f"Selling {count} {side.upper()} of {ticker}: book bid {bid}c -> "
              f"IOC @ {px}c ...")
        resp = await rest.place_order_v2(ticker=ticker, side=api_side, count=count,
                                         price_dollars=yes_price / 100.0)
        filled = float(resp.get("fill_count") or 0)
        if filled <= 0:
            print(f"NO FILL — nothing at the touch right now. order_id={resp.get('order_id')}")
            return
        yes_fill = float(resp.get("average_fill_price") or 0) * 100.0
        side_price = (100.0 - yes_fill) if side == "no" else yes_fill
        print(f"SOLD {filled:g} {side.upper()} @ {side_price:.0f}c = ~${filled*side_price/100:.2f} "
              f"order_id={resp.get('order_id')}")
        bal = await rest.get_balance_cents()
        print(f"cash balance now ${bal/100:.2f}  (run `python ledger.py --live` to update records)")
    finally:
        await rest.aclose()


def main() -> None:
    ap = argparse.ArgumentParser(description="Manually sell a specific position")
    ap.add_argument("ticker", nargs="?", help="market ticker to sell; omit to list positions")
    ap.add_argument("--fraction", type=float, default=1.0, help="portion to sell (default all)")
    ap.add_argument("--buffer", type=int, default=1, help="cents to cross below the bid (default 1)")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--arm", action="store_true", help="required to place the REAL sell order")
    args = ap.parse_args()
    if args.ticker and not args.arm:
        raise SystemExit("Refusing: pass --arm to place the real sell order "
                         "(or run with no ticker to just list positions).")
    _load_dotenv()
    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)
    asyncio.run(amain(cfg, args.ticker, args.fraction, args.buffer))


if __name__ == "__main__":
    main()
