"""Real-money trade ledger across ALL live runs.

Reads every run/*.sqlite, groups real orders (reason='live') by position
(ticker + side), and shows cash IN (what you paid) vs cash OUT (sell proceeds +
settlement payout), whether the bot sold it or let it ride to settlement.

    python ledger.py            # fast, offline (DB only)
    python ledger.py --live     # also hit Kalshi: resolve unlogged settlements
                                # and print the authoritative account net

Run it after every session to keep a running tally. It merges across runs, so a
position bought one run and settled the next is stitched together correctly.
With --live it self-reconciles: any position the DB still shows "open" is checked
against Kalshi, so a settlement the bot missed (run ended early) is resolved.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import glob
import json
import os
import sqlite3
from collections import defaultdict

from engine.markets import parse_market

START_CENTS = 0  # record your initial deposit with `python ledger.py --deposit <USD>`
CASHFLOW_PATH = os.path.join("run", "cashflows.json")  # later deposits/withdrawals


def load_cashflows() -> list:
    try:
        with open(CASHFLOW_PATH) as fh:
            return json.load(fh)
    except (FileNotFoundError, ValueError):
        return []


def record_cashflow(kind: str, amount_cents: int) -> None:
    flows = load_cashflows()
    flows.append({"type": kind, "amount_cents": int(amount_cents),
                  "ts": datetime.datetime.now().isoformat(timespec="seconds")})
    os.makedirs("run", exist_ok=True)
    with open(CASHFLOW_PATH, "w") as fh:
        json.dump(flows, fh, indent=2)


def cashflow_totals() -> tuple[int, int, int]:
    """(extra_deposits, withdrawals, manual_pnl) in cents beyond the START deposit.
    manual_pnl = P&L from trades placed by hand on the same account. It shares the
    balance with the bot, so it is subtracted back out to keep the headline net
    measuring the bot alone."""
    dep = wd = man = 0
    for f in load_cashflows():
        amt = int(f.get("amount_cents", 0))
        if f.get("type") == "withdraw":
            wd += amt
        elif f.get("type") == "deposit":
            dep += amt
        elif f.get("type") == "manual_pnl":
            man += amt
    return dep, wd, man


def decode(t: str, side: str) -> str:
    try:
        s = parse_market(t)
    except Exception:  # noqa: BLE001
        return f"{t} ({side})"
    code = t.split("-")[1]
    if s.kind == "game_total":
        return f"{code} total {'Over' if side=='yes' else 'Under'} {s.line}"
    if s.kind == "team_total":
        return f"{code} {s.team_code} {'Over' if side=='yes' else 'Under'} {s.line}"
    if s.kind == "moneyline":
        who = "Tie" if s.is_tie else s.team_code
        return f"{code} ML {who} {'YES' if side=='yes' else 'NO'}"
    if s.kind == "spread":
        return f"{code} {s.team_code} +{s.line} ({side})"
    return f"{t} ({side})"


def collect():
    buys = defaultdict(list)   # (ticker,side) -> [(qty,price,fee)]
    sells = defaultdict(list)
    result: dict[str, str] = {}
    first_ts: dict[tuple, float] = {}
    lanes: dict[tuple, str] = {}          # (ticker,side) -> lane of its buy
    fees = {"paid": 0, "all_taker": 0, "rebates": 0}   # fee drag, measured
    for path in sorted(glob.glob("run/*.sqlite")):
        db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                          "AND name='decisions'").fetchone():
            db.close()      # e.g. run/longshot.sqlite: shadow-only, no real trades
            continue
        try:
            q = ("SELECT decision_wall,ticker,side,action,filled_size,"
                 "sim_fill_price_cents,fee_cents,{lane} FROM decisions "
                 "WHERE reason='live' AND accepted=1")
            try:
                rows = list(db.execute(q.format(lane="COALESCE(lane,'edge')")))
            except sqlite3.OperationalError:      # DB predates the lane column
                rows = list(db.execute(q.format(lane="'edge'")))
            for w, t, side, act, qty, px, fee, lane in rows:
                key = (t, side)
                rec = (qty or 0, px or 0, fee or 0)
                fees["paid"] += fee or 0
                if qty and px:
                    import math
                    p = max(0.01, min(0.99, (px or 50) / 100.0))
                    fees["all_taker"] += math.ceil(0.07 * qty * p * (1 - p) * 100)
                if act == "buy":
                    buys[key].append(rec)
                    first_ts[key] = min(first_ts.get(key, 9e18), w or 9e18)
                    lanes[key] = lane or "edge"
                else:
                    sells[key].append(rec)
            try:
                for t, res in db.execute(
                    "SELECT ticker,result FROM settlements WHERE result IN ('yes','no')"):
                    result[t] = res
            except sqlite3.OperationalError:
                pass
        finally:
            db.close()
    return buys, sells, result, first_ts, lanes, fees


def build(buys, sells, result, first_ts, manual=None, lanes=None):
    manual = manual or {}
    lanes = lanes or {}
    closed, openpos = [], []
    real_in = real_out = 0
    lane_tot = {}          # lane -> [in_cents, out_cents]
    for key in sorted(buys, key=lambda k: first_ts.get(k, 0)):
        t, side = key
        bq = sum(q for q, _, _ in buys[key])
        cost = sum(q * p for q, p, _ in buys[key]) + sum(f for _, _, f in buys[key])
        sq = sum(q for q, _, _ in sells.get(key, []))
        proceeds = (sum(q * p for q, p, _ in sells.get(key, []))
                    - sum(f for _, _, f in sells.get(key, [])))
        leftover = bq - sq
        res = result.get(t)
        avg = cost / bq if bq else 0
        when = datetime.datetime.fromtimestamp(first_ts.get(key, 0)).strftime("%m-%d %H:%M")
        # flat on Kalshi — resolved from fills via --live (sells + settled remainder)
        if key in manual:
            out, how = manual[key]
            closed.append((when, decode(t, side), int(bq), avg, cost, out, out - cost, how))
            real_in += cost
            real_out += out
            lt = lane_tot.setdefault(lanes.get(key, "edge"), [0, 0])
            lt[0] += cost; lt[1] += out
            continue
        if leftover > 0 and res is None:
            openpos.append((when, decode(t, side), int(bq), avg, cost))
            continue
        settle = (leftover * 100 if side == res else 0) if leftover > 0 else 0
        out = proceeds + settle
        how = ("sold+settled" if sq > 0 and leftover > 0 else
               "sold" if sq > 0 else
               "settled WON" if settle > 0 else "settled lost")
        closed.append((when, decode(t, side), int(bq), avg, cost, out, out - cost, how))
        real_in += cost
        real_out += out
        lt = lane_tot.setdefault(lanes.get(key, "edge"), [0, 0])
        lt[0] += cost; lt[1] += out
    return closed, openpos, real_in, real_out, lane_tot


def report(closed, openpos, real_in, real_out, balance_cents=None,
           extra_dep=0, withdrawn=0, lane_tot=None, fees=None, manual=0):
    print("=" * 92)
    print("  REAL-MONEY TRADE LEDGER  (all live runs)")
    print("=" * 92)
    print(f"  {'date':11} {'position':30} {'qty':>4} {'@avg':>6} {'IN':>8} "
          f"{'OUT':>8} {'NET':>8}  how")
    print("  " + "-" * 88)
    for when, name, qty, avg, cin, cout, net, how in closed:
        print(f"  {when:11} {name:30} {qty:>4} {avg:>5.0f}c ${cin/100:>6.2f} "
              f"${cout/100:>6.2f} ${net/100:>+6.2f}  {how}")
    print("  " + "-" * 88)
    print(f"  {'CLOSED TOTAL':52} ${real_in/100:>6.2f} ${real_out/100:>6.2f} "
          f"${(real_out-real_in)/100:>+6.2f}")
    for ln, (cin, cout) in sorted((lane_tot or {}).items()):
        print(f"  {'  lane['+ln+']':52} ${cin/100:>6.2f} ${cout/100:>6.2f} "
              f"${(cout-cin)/100:>+6.2f}")
    if openpos:
        print("\n  OPEN (still riding — not yet sold or settled):")
        for when, name, qty, avg, cost in openpos:
            print(f"  {when:11} {name:30} {qty:>4} {avg:>5.0f}c  ${cost/100:.2f} at risk")
    if fees:
        saved = fees["all_taker"] - fees["paid"]
        print(f"\n  fees: paid ${fees['paid']/100:.2f} | if-all-taker "
              f"${fees['all_taker']/100:.2f} (maker savings ${saved/100:+.2f}) | "
              f"rebates ${fees['rebates']/100:.2f}")
    print("\n" + "=" * 92)
    print(f"  realized net (from trade log): ${(real_out-real_in)/100:+.2f}")
    if balance_cents is not None:
        deposited = START_CENTS + extra_dep
        # true BOT P&L = in-account + withdrawn - deposited - manual (hand-placed)
        # trading P&L (which lives in the same balance but is NOT the bot's record)
        net = balance_cents + withdrawn - deposited - manual
        if withdrawn or extra_dep or manual:
            print(f"  LIVE balance ${balance_cents/100:.2f} + withdrawn "
                  f"${withdrawn/100:.2f}  -  deposited ${deposited/100:.2f}"
                  + (f"  -  manual P&L ${manual/100:+.2f}" if manual else ""))
        else:
            print(f"  LIVE account balance: ${balance_cents/100:.2f}  "
                  f"(deposited ${deposited/100:.2f})")
        print(f"  ->  AUTHORITATIVE BOT NET ${net/100:+.2f}"
              + (f"   (account incl. manual: ${(net+manual)/100:+.2f})" if manual else ""))
    else:
        print(f"  (deposited ${START_CENTS/100:.2f}; run with --live for the "
              f"authoritative account net)")
    print("=" * 92)


async def live_resolve(openpos_keys):
    """Hit Kalshi to resolve every position the DB still thinks is open:
      - genuinely still open on Kalshi -> leave as open
      - settled -> record the result
      - flat on Kalshi but not settled (closed in the app) -> pull sell proceeds
        from fills so the manual close shows up with real P&L.
    Returns (balance_cents, {ticker:result}, {(ticker,side):manual_proceeds_cents})."""
    import yaml
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
    except ImportError:
        pass
    from data.kalshi_auth import KalshiSigner
    from data.kalshi_rest import KalshiREST
    cfg = yaml.safe_load(open("config.yaml"))
    rest = KalshiREST(cfg, KalshiSigner.from_config(cfg))
    settled, manual = {}, {}
    try:
        bal = await rest.get_balance_cents()
        poss = await rest.get_positions()
        truly_open = set()
        for r in poss:
            p = int(round(float(r.get("position_fp", 0) or 0)))
            if p != 0:
                truly_open.add((r.get("ticker"), "yes" if p > 0 else "no"))
        for key in openpos_keys:
            t, side = key
            if key in truly_open:
                continue  # really still open on Kalshi -> leave as open
            # Flat on Kalshi => closed (sold and/or settled). The true OUT is
            # sell proceeds from FILLS, plus settlement only on whatever was still
            # HELD at settlement (bought - sold). This avoids crediting a full
            # settlement payout to contracts you already sold.
            try:
                fills = await rest.get_fills(t)
            except Exception:  # noqa: BLE001
                fills = []
            # A fill is an EXIT of our position when its book `side` differs from
            # the side we hold (selling NO crosses to the yes side, and vice versa);
            # fills on our own side are the entries. Price for our P&L is always our
            # side's price.
            bought = sold = proceeds = 0.0
            for f in fills:
                cnt = float(f.get("count_fp", 0) or 0)
                px = float((f.get("no_price_dollars") if side == "no"
                            else f.get("yes_price_dollars")) or 0)
                fee = float(f.get("fee_cost", 0) or 0)
                # `side` is deprecated; `outcome_side` is its replacement.
                fside = f.get("side") or f.get("outcome_side")
                if fside != side:                   # exit (sold our side)
                    sold += cnt
                    proceeds += cnt * px * 100 - fee * 100
                else:                               # entry (bought our side)
                    bought += cnt
            if bought <= 0.0001:
                # No fill history found: we know NOTHING from Kalshi, so keep the
                # bot's own DB record. Never book missing data as a loss.
                continue
            leftover = bought - sold
            out = proceeds
            if leftover > 0.0001:  # held a remainder into settlement
                try:
                    s = await rest.get_settlement(t)
                    if s["settled"] and s["result"] == side:
                        out += leftover * 100
                except Exception:  # noqa: BLE001
                    pass
            label = "sold" if sold > 0.0001 else (
                "settled WON" if out > 0 else "settled lost")
            manual[key] = (round(out), label)
    finally:
        await rest.aclose()
    return bal, settled, manual


def main() -> None:
    ap = argparse.ArgumentParser(description="Real-money trade ledger")
    ap.add_argument("--live", action="store_true",
                    help="hit Kalshi to resolve unlogged settlements + show account net")
    ap.add_argument("--withdraw", type=float, metavar="USD",
                    help="record a withdrawal (keeps the authoritative net correct) and exit")
    ap.add_argument("--manual", type=float, metavar="USD",
                    help="record YOUR OWN hand-placed bet's net P&L on this account "
                         "(positive=won, negative=lost) so the bot's net stays clean")
    ap.add_argument("--deposit", type=float, metavar="USD",
                    help="record an added deposit and exit")
    args = ap.parse_args()

    if args.withdraw is not None:
        record_cashflow("withdraw", round(args.withdraw * 100))
        print(f"recorded withdrawal of ${args.withdraw:.2f}")
        return
    if args.manual:
        record_cashflow("manual_pnl", round(args.manual * 100))
        print(f"recorded manual P&L ${args.manual:+.2f} (kept OUT of the bot's net)")
    if args.deposit is not None:
        record_cashflow("deposit", round(args.deposit * 100))
        print(f"recorded deposit of ${args.deposit:.2f}")
        return

    buys, sells, result, first_ts, lanes, fees = collect()
    balance, manual = None, {}
    if args.live:
        # Resolve positions against Kalshi reality (fills) so manual app-closes
        # don't get mis-credited the full settlement payout. ONLY for single-sided
        # tickers: if we traded BOTH yes and no on a ticker, fills can't tell a
        # "buy NO" from a "sell YES", so fall back to the DB settlement for those.
        sides = defaultdict(set)
        for (t, s) in buys:
            sides[t].add(s)
        single = [k for k in buys if len(sides[k[0]]) == 1]
        balance, extra, manual = asyncio.run(live_resolve(single))
        result.update(extra)
    closed, openpos, real_in, real_out, lane_tot = build(buys, sells, result, first_ts,
                                                         manual, lanes)
    extra_dep, withdrawn, manual = cashflow_totals()
    report(closed, openpos, real_in, real_out, balance_cents=balance,
           extra_dep=extra_dep, withdrawn=withdrawn, lane_tot=lane_tot, fees=fees,
           manual=manual)


if __name__ == "__main__":
    main()
