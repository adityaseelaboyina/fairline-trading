"""Verify the LIVE order path + POST signing with ~zero risk.

Places ONE far-from-market immediate-or-cancel bid (1c on a real market). At 1c
it fills nothing, so no contracts are bought and no money is spent — but it
exercises the real signed POST so we learn:
  * does POST signing work (200) or need body-signing (401)?
  * is the V2 endpoint/body shape correct?

This DOES hit your real account API. It is gated behind an explicit flag.

    python verify_order.py --config config.yaml --i-understand-real-order
"""
from __future__ import annotations

import argparse
import asyncio

import yaml

from data.kalshi_auth import KalshiSigner
from data.kalshi_rest import KalshiREST


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


async def amain(cfg: dict) -> None:
    signer = KalshiSigner.from_config(cfg)
    rest = KalshiREST(cfg, signer)
    try:
        bal = await rest.get_balance_cents()
        print(f"account balance: ${bal/100:.2f}")
        ticker = cfg["market_tickers"][0]
        # Two non-filling probes: a bid (buy YES @1c) and an ask (sell YES @99c,
        # i.e. the NO path). Both are far from any real market so they fill nothing.
        probes = [("bid (buy YES)", "bid", 0.01),
                  ("ask (sell YES = buy NO)", "ask", 0.99)]
        ok = True
        for label, api_side, price in probes:
            print(f"\nprobe: {label} 1x IOC @ ${price:.2f} on {ticker} ...")
            try:
                resp = await rest.place_order_v2(
                    ticker=ticker, side=api_side, count=1.0, price_dollars=price)
                fc = resp.get("fill_count")
                print(f"  ACCEPTED  order_id={resp.get('order_id')} fill_count={fc} "
                      f"remaining={resp.get('remaining_count')}")
                if str(fc) not in ("0", "0.00", "0.0"):
                    print("  WARNING: it actually FILLED — close it in the Kalshi app.")
            except Exception as exc:  # noqa: BLE001
                ok = False
                print(f"  REJECTED: {exc!r}")
        if ok:
            print("\nVERIFIED: both order directions (bid + ask) work. Ready to wire/arm live.")
        else:
            print("\nA probe was rejected — do NOT arm live; paste me the error.")
    finally:
        await rest.aclose()


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify the live Kalshi order path (~$0 risk)")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--i-understand-real-order", action="store_true",
                    help="required: this sends a real (non-filling) order to your account")
    args = ap.parse_args()
    if not args.i_understand_real_order:
        raise SystemExit("Refusing: pass --i-understand-real-order to send the test order.")
    _load_dotenv()
    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)
    asyncio.run(amain(cfg))


if __name__ == "__main__":
    main()
