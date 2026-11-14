"""One-shot live pick.

Pulls the current order book (REST) for the configured markets plus the
independently-verified score (ESPN), asks the analyst for its read, and prints
the full pick — best trade, fair value, edge, ranked alternatives, exit rules,
and what it verified vs assumed. No orders, paper only.

    python pick.py --config config.yaml [--trigger live]
"""
from __future__ import annotations

import argparse
import asyncio
import json

import yaml

from data.kalshi_auth import KalshiSigner
from data.kalshi_rest import KalshiREST
from data.score_feed import ESPNScoreProvider
from engine.analyst import Analyst


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


async def build_board(rest: KalshiREST, tickers: list[str]) -> dict:
    board = {}
    for t in tickers:
        try:
            m = await rest.get_market(t)
            ob = await rest.get_orderbook(t, depth=8)
            board[t] = {
                "title": m.get("yes_sub_title") or m.get("title"),
                "status": m.get("status"),
                "best_yes_bid": ob.best_yes_bid, "best_yes_ask": ob.best_yes_ask,
                "best_no_bid": ob.best_no_bid, "best_no_ask": ob.best_no_ask,
                "yes_mid": ob.mid("yes"),
                "yes_depth": sorted(ob.yes.items(), reverse=True)[:5],
                "no_depth": sorted(ob.no.items(), reverse=True)[:5],
            }
        except Exception as exc:  # noqa: BLE001
            board[t] = {"error": repr(exc)}
    return board


async def build_score(cfg: dict) -> dict:
    sc = cfg["score_feed"]
    event_ids = {str(v) for v in sc.get("event_map", {}).values()}
    if not event_ids:
        return {"note": "no event_map configured"}
    prov = ESPNScoreProvider(sc.get("espn_league", "fifa.world"))
    try:
        states = await prov.fetch()
    finally:
        await prov.aclose()
    out = {}
    for eid in event_ids:
        s = states.get(eid)
        if s:
            out[eid] = {"home": s.home, "away": s.away,
                        "home_score": s.home_score, "away_score": s.away_score,
                        "home_reds": s.home_reds, "away_reds": s.away_reds,
                        "state": s.state, "period": s.period, "clock": s.clock}
    return out or {"note": "configured events not found on the live scoreboard"}


def print_pick(parsed: dict, latency_ms: float) -> None:
    bt = parsed["best_trade"]
    print("\n" + "=" * 64)
    print(f"BEST PICK   {bt['side'].upper()} {bt['market_ticker']}  "
          f"limit {bt['limit_price_cents']}c")
    print(f"  fair value {parsed['fair_value_cents']}c | edge {parsed['edge_cents']}c | "
          f"confidence {parsed['confidence']} | analyst {latency_ms:.0f}ms")
    print(f"  self-audit: {parsed.get('self_audit','')}")
    alts = parsed.get("ranked_alternatives") or []
    if alts:
        print("  ranked alternatives:")
        for a in alts:
            print(f"    - {a.get('side','?')} {a.get('market_ticker','?')} @ "
                  f"{a.get('price_cents','?')}c  edge {a.get('edge_cents','?')}c  "
                  f"({a.get('note','')})")
    rules = parsed.get("exit_rules") or []
    if rules:
        print("  exit rules:")
        for r in rules:
            print(f"    - on {r.get('event')}: {r.get('action')}  ({r.get('reason','')})")
    if parsed.get("inputs_verified"):
        print(f"  verified: {parsed['inputs_verified']}")
    if parsed.get("inputs_assumed"):
        print(f"  assumed:  {parsed['inputs_assumed']}")
    print("=" * 64)


async def amain(cfg: dict, trigger: str) -> None:
    signer = KalshiSigner.from_config(cfg)
    rest = KalshiREST(cfg, signer)
    try:
        board = await build_board(rest, cfg["market_tickers"])
        score = await build_score(cfg)
        print("LIVE BOARD:")
        for t, b in board.items():
            if "error" in b:
                print(f"  {t}: {b['error']}")
            else:
                print(f"  {t}: yes {b['best_yes_bid']}/{b['best_yes_ask']}  "
                      f"no {b['best_no_bid']}/{b['best_no_ask']}  ({b['status']})")
        print(f"VERIFIED SCORE: {json.dumps(score)}")
        analyst = Analyst(cfg)
        res = await analyst.analyze(board_snapshot=board, score_context=score, trigger=trigger)
        if not res.parsed_ok:
            print(f"\n[analyst] could not parse a pick: {res.error}\nraw:\n{res.raw_text[:1500]}")
            return
        print_pick(res.parsed, res.latency_ms)
    finally:
        await rest.aclose()


def main() -> None:
    ap = argparse.ArgumentParser(description="One-shot live pick from the analyst")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--trigger", default="live")
    args = ap.parse_args()
    _load_dotenv()
    with open(args.config) as fh:
        cfg = yaml.safe_load(fh)
    asyncio.run(amain(cfg, args.trigger))


if __name__ == "__main__":
    main()
