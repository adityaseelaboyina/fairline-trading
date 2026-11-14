"""Longshot lane runner — SHADOW ONLY. This program cannot place orders.

Slow-lane counterpart to the soccer harness: scans configured retail-heavy
series (weather, politics) once per run, logs would-be NO fills on overpriced
YES longshots (priced off walked depth, never mids), then settles and reports.
It never imports engine.live_broker and has no order-placement path at all.
Arming (a future, manual, user-only step) requires the pre-registered gates in
engine/longshot.py to pass first.

Usage (cron it):
    python longshot_scan.py --scan      # find + log shadow entries
    python longshot_scan.py --settle    # resolve settled shadow positions
    python longshot_scan.py --report    # calibration + P&L + arming-gate status

Kill switch: a file named STOP in run_dir halts --scan (same as the harness).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
import time
from datetime import datetime, timezone

import yaml

from data.kalshi_auth import KalshiSigner
from data.kalshi_rest import KalshiREST
from engine.fees import make_fee_fn
from engine.longshot import (LongshotCandidate, apply_exposure_caps, gate_report,
                             scan_market, settle_pnl_cents)

SCHEMA = """
CREATE TABLE IF NOT EXISTS longshot_shadow (
  id INTEGER PRIMARY KEY,
  ts_wall REAL NOT NULL,
  day TEXT NOT NULL,
  ticker TEXT NOT NULL,
  event_ticker TEXT NOT NULL,
  series TEXT NOT NULL,
  side TEXT NOT NULL,
  yes_bid INTEGER, yes_ask INTEGER, no_ask INTEGER,
  contracts INTEGER, fee_cents INTEGER,
  net_win_cents INTEGER, hours_to_close REAL,
  settled INTEGER DEFAULT 0, result TEXT, pnl_cents INTEGER, settle_ts REAL,
  UNIQUE(ticker, day)
);
"""


def _db(cfg: dict) -> sqlite3.Connection:
    run_dir = cfg.get("run_dir", "./run")
    os.makedirs(run_dir, exist_ok=True)
    conn = sqlite3.connect(os.path.join(run_dir, "longshot.sqlite"))
    conn.execute(SCHEMA)
    return conn


def _stopped(cfg: dict) -> bool:
    return os.path.exists(os.path.join(cfg.get("run_dir", "./run"), "STOP"))


def _iso_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def _yes_prices_cents(m: dict):
    """Defensive read: cents ints (yes_bid) or dollar strings (yes_bid_dollars)."""
    def cents(key):
        if m.get(key) is not None:
            return int(m[key])
        d = m.get(key + "_dollars")
        return int(round(float(d) * 100)) if d is not None else None
    return cents("yes_bid"), cents("yes_ask")


async def do_scan(cfg: dict) -> None:
    lane = cfg["longshot"]
    if _stopped(cfg):
        print("STOP file present — no new shadow entries.")
        return
    signer = KalshiSigner.from_config(cfg)
    rest = KalshiREST(cfg, signer)
    fee_fn = make_fee_fn(cfg)
    conn = _db(cfg)
    now = time.time()
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    open_rows = conn.execute(
        "SELECT event_ticker, no_ask*contracts + fee_cents FROM longshot_shadow "
        "WHERE settled=0").fetchall()
    open_cost = sum(r[1] for r in open_rows)
    open_by_event: dict = {}
    for ev, _c in open_rows:
        open_by_event[ev] = open_by_event.get(ev, 0) + 1

    candidates: list[LongshotCandidate] = []
    scanned = 0
    try:
        for series in lane.get("series", []):
            markets = await rest.list_markets(series_ticker=series, status="open",
                                              limit=200)
            for m in markets:
                scanned += 1
                yb, ya = _yes_prices_cents(m)
                if ya is None or ya > int(lane.get("max_yes_mid_cents", 10)) + 3:
                    continue                      # cheap prefilter before book fetch
                close_ts = _iso_ts(m["close_time"]) if m.get("close_time") else None
                if not close_ts:
                    continue
                hours = (close_ts - now) / 3600.0
                book = await rest.get_orderbook(m["ticker"], depth=8)
                c = scan_market(ticker=m["ticker"],
                                event_ticker=m.get("event_ticker", ""),
                                book=book, hours_to_close=hours,
                                fee_fn=fee_fn, cfg=lane)
                if c:
                    candidates.append(c)
    finally:
        await rest.aclose()

    picked = apply_exposure_caps(candidates, open_cost, open_by_event, lane)
    for c in picked:
        try:
            conn.execute(
                "INSERT INTO longshot_shadow (ts_wall, day, ticker, event_ticker,"
                " series, side, yes_bid, yes_ask, no_ask, contracts, fee_cents,"
                " net_win_cents, hours_to_close) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (now, day, c.ticker, c.event_ticker, c.series, c.side, c.yes_bid,
                 c.yes_ask, c.no_ask, c.contracts, c.fee_cents, c.net_win_cents,
                 c.hours_to_close))
            print(f"SHADOW SELL {c.ticker}: buy NO {c.contracts}x @{c.no_ask}c "
                  f"(yes {c.yes_bid}/{c.yes_ask}, fee {c.fee_cents}c, "
                  f"win +{c.net_win_cents}c / lose -{c.net_loss_cents}c, "
                  f"T-{c.hours_to_close}h)")
        except sqlite3.IntegrityError:
            pass                                  # already entered today
    conn.commit()
    print(f"scanned={scanned} candidates={len(candidates)} entered={len(picked)} "
          f"open_cost=${open_cost/100:.2f}")


async def do_settle(cfg: dict) -> None:
    signer = KalshiSigner.from_config(cfg)
    rest = KalshiREST(cfg, signer)
    conn = _db(cfg)
    rows = conn.execute("SELECT id, ticker, side, no_ask, contracts, fee_cents "
                        "FROM longshot_shadow WHERE settled=0").fetchall()
    n = 0
    try:
        for rid, ticker, side, no_ask, contracts, fee in rows:
            s = await rest.get_settlement(ticker)
            if not s["settled"]:
                continue
            pnl = settle_pnl_cents(side, s["result"], no_ask, contracts, fee)
            conn.execute("UPDATE longshot_shadow SET settled=1, result=?, "
                         "pnl_cents=?, settle_ts=? WHERE id=?",
                         (s["result"], pnl, time.time(), rid))
            n += 1
            print(f"SETTLED {ticker}: {s['result']} -> {pnl:+d}c")
    finally:
        await rest.aclose()
    conn.commit()
    print(f"settled {n} of {len(rows)} open shadow positions")


def do_report(cfg: dict) -> None:
    lane = cfg["longshot"]
    conn = _db(cfg)
    rows = [dict(zip(("settled", "pnl_cents", "event_ticker", "series", "day",
                      "yes_bid", "yes_ask", "result"), r))
            for r in conn.execute(
                "SELECT settled, pnl_cents, event_ticker, series, day,"
                " yes_bid, yes_ask, result FROM longshot_shadow").fetchall()]
    settled = [r for r in rows if r["settled"]]
    print(f"shadow entries: {len(rows)} total, {len(settled)} settled")
    if settled:
        net = sum(r["pnl_cents"] for r in settled)
        wins = sum(1 for r in settled if r["pnl_cents"] > 0)
        print(f"net: {net:+d}c over {len(settled)} settles ({wins} wins)")
        print("\ncalibration (yes mid at entry vs YES hit rate):")
        for lo, hi in ((0, 3), (3, 6), (6, 9), (9, 13)):
            b = [r for r in settled if lo <= (r["yes_bid"] + r["yes_ask"]) / 2 < hi]
            if b:
                hits = sum(1 for r in b if r["result"] == "yes")
                imp = sum((r["yes_bid"] + r["yes_ask"]) / 2 for r in b) / len(b)
                print(f"  {lo}-{hi}c: n={len(b)} implied={imp:.1f}% "
                      f"actual={100 * hits / len(b):.1f}%")
    print("\nARMING GATES (pre-registered; arming is a manual user step):")
    rep = gate_report(rows, lane)
    for k, v in rep.items():
        print(f"  {k}: {v}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Longshot lane — shadow only")
    ap.add_argument("--config", default="config.yaml")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--scan", action="store_true")
    g.add_argument("--settle", action="store_true")
    g.add_argument("--report", action="store_true")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)
    if "longshot" not in cfg:
        raise SystemExit("config.yaml has no `longshot:` section — see README")
    if args.scan:
        asyncio.run(do_scan(cfg))
    elif args.settle:
        asyncio.run(do_settle(cfg))
    else:
        do_report(cfg)


if __name__ == "__main__":
    main()
