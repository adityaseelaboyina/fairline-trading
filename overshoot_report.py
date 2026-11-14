"""Post-slate report: what actually happens to books after a goal.

Reads run/harness.sqlite (goal_events, overshoot_paths, shadow_snipes, settlements)
and answers, with data: how big is the post-goal overshoot, how fast must we be, how
often did the Class A window (decided market still quoted <= 93c) even exist, and
where should Class B's exit timer sit.

    python overshoot_report.py [--db run/harness.sqlite]
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict

from engine.sniper import decided_yes

OFFSETS = (0, 5, 10, 20, 30, 45, 60, 90, 120)


def series_of(ticker: str) -> str:
    return ticker.split("-")[0]


def main() -> None:
    ap = argparse.ArgumentParser(description="Post-goal overshoot report (Task 14)")
    ap.add_argument("--db", default="run/harness.sqlite")
    ap.add_argument("--class-a-cap", type=int, default=93)
    args = ap.parse_args()
    db = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)

    goals = {gid: (eid, team, nh, na, rev, dw, sw, fav, bucket)
             for gid, eid, team, nh, na, rev, dw, sw, fav, bucket
             in db.execute("SELECT id,event_id,team,new_home,new_away,reverted,"
                           "detect_wall,spike_wall,scorer_was_favourite,"
                           "minute_bucket FROM goal_events")}
    settled = dict(db.execute(
        "SELECT ticker,result FROM settlements WHERE result IN ('yes','no')"))
    if not goals:
        print("No goal_events logged yet — run a slate first.")
        return

    print(f"{len(goals)} goal events "
          f"({sum(1 for g in goals.values() if g[4])} reverted/VAR)")
    spikes = [(g[5] - g[6]) * 1000 for g in goals.values() if g[6]]
    if spikes:
        print(f"spike pre-trigger led detection by: "
              f"{sorted(spikes)[len(spikes)//2]:.0f}ms median ({len(spikes)} goals)")

    # ---- overshoot path vs settled value, per market series ----------------
    paths = defaultdict(lambda: defaultdict(list))   # series -> offset -> [dev cents]
    windows = []                                     # Class A window durations
    for gid, tk, off, ya in db.execute(
            "SELECT goal_event_id,ticker,offset_s,yes_ask FROM overshoot_paths"):
        g = goals.get(gid)
        res = settled.get(tk)
        if g is None or res is None or ya is None:
            continue
        settle_val = 100.0 if res == "yes" else 0.0
        paths[series_of(tk)][off].append(ya - settle_val)

    for series, by_off in sorted(paths.items()):
        print(f"\n{series}: yes_ask deviation from settled value (mean, n)")
        peak, peak_off = 0.0, 0
        for off in OFFSETS:
            vals = by_off.get(off, [])
            if not vals:
                continue
            m = sum(vals) / len(vals)
            if abs(m) > abs(peak):
                peak, peak_off = m, off
            print(f"  +{off:>3}s  {m:+7.1f}c   (n={len(vals)})")
        if peak:
            # decay half-life: first offset after the peak where |dev| <= |peak|/2
            half = next((o for o in OFFSETS if o > peak_off
                         and by_off.get(o)
                         and abs(sum(by_off[o]) / len(by_off[o])) <= abs(peak) / 2),
                        None)
            print(f"  peak {peak:+.1f}c at +{peak_off}s"
                  + (f", half-life ~{half - peak_off}s" if half else ", no decay seen"))

    # ---- Asymmetry breakdown by scorer_was_favourite x bucket ----
    # Pre-registered hypothesis: overreaction when a favourite scores (fade),
    # underreaction when a longshot scores late (buy). Rules arm per-cell ONLY at
    # >=10 events with the confirming sign — our data, not the 2013 Betfair paper's.
    cells = defaultdict(lambda: defaultdict(list))   # (fav,bucket) -> offset -> [dev]
    for gid, tk, off, ya in db.execute(
            "SELECT goal_event_id,ticker,offset_s,yes_ask FROM overshoot_paths"):
        g = goals.get(gid)
        res = settled.get(tk)
        if g is None or res is None or ya is None or g[7] is None:
            continue
        settle_val = 100.0 if res == "yes" else 0.0
        cells[(g[7], g[8])][off].append(ya - settle_val)
    if cells:
        print("\nAsymmetry cells (R3 check — arm per-cell only at n>=10, right sign):")
        for (fav, bucket), by_off in sorted(cells.items()):
            peak, peak_off = 0.0, 0
            for off in OFFSETS:
                vals = by_off.get(off, [])
                if vals:
                    m = sum(vals) / len(vals)
                    if abs(m) > abs(peak):
                        peak, peak_off = m, off
            label = "favourite" if fav else "longshot "
            print(f"  {label} scored, {bucket:>6}: n={len(by_off.get(0, []))} "
                  f"peak dev {peak:+.1f}c at +{peak_off}s")
    else:
        print("\nAsymmetry cells: no tagged goals with settlements yet")

    # ---- Class A window: decided markets still quoted <= cap ---------------
    for gid, tk, off, ya in db.execute(
            "SELECT goal_event_id,ticker,offset_s,yes_ask FROM overshoot_paths "
            "ORDER BY goal_event_id,ticker,offset_s"):
        g = goals.get(gid)
        if g is None or ya is None or g[4]:
            continue
        if decided_yes(tk, g[2], g[3]) and ya <= args.class_a_cap:
            windows.append((gid, tk, off))
    if windows:
        by_pos = defaultdict(list)
        for gid, tk, off in windows:
            by_pos[(gid, tk)].append(off)
        durations = [max(offs) for offs in by_pos.values()]
        durations.sort()
        print(f"\nClass A window (decided & ask<= {args.class_a_cap}c): "
              f"existed on {len(by_pos)} goal-market pairs; "
              f"still open at +{durations[len(durations)//2]}s median, "
              f"+{durations[-1]}s max")
    else:
        print(f"\nClass A window: never observed (no decided market quoted "
              f"<= {args.class_a_cap}c after a goal)")

    # ---- shadow snipe P&L (depth-priced entries only) -----------------------
    # Armed-snipe LIVE outcomes: distinguishes a real
    # 0-fill edge problem (no_fill) from gate blocks or execution failures.
    try:
        outc = list(db.execute(
            "SELECT klass, COALESCE(outcome,'(shadow/unfired)'), COUNT(*), AVG(wire_ms) "
            "FROM shadow_snipes WHERE armed=1 GROUP BY 1,2 ORDER BY 1,2"))
        if outc:
            print("\nArmed-snipe LIVE outcomes (what actually happened when it fired):")
            for k, o, n, w in outc:
                print(f"  class {k}: {o:14} x{n}"
                      + (f"  (avg wire {w:.0f}ms)" if w else ""))
    except sqlite3.OperationalError:
        pass

    # Venue-truth crosscheck: WS-claimed ask vs REST ask at snipe time.
    # Large gaps prove the '0-fill' was a STALE local book, not a missing edge.
    try:
        xc = list(db.execute(
            "SELECT ticker, ask_cents, rest_ask_cents, ws_dirty FROM shadow_snipes "
            "WHERE rest_ask_cents IS NOT NULL ORDER BY id DESC LIMIT 12"))
        if xc:
            print("\nWS-book vs VENUE (REST) ask at snipe time — stale-book proof:")
            print(f"  {'market':24} {'WS':>4} {'REST':>5} {'gap':>5} dirty")
            for tk, wsa, ra, dty in xc:
                gap = (ra - wsa) if (ra is not None and wsa is not None) else None
                print(f"  {tk.split('-',1)[1]:24} {wsa:>3.0f}c {ra:>4.0f}c "
                      f"{gap:>+4.0f}c {'YES' if dty else 'no'}")
    except sqlite3.OperationalError:
        pass

    print("\nShadow snipes (depth-priced; entries at WORST fill level):")
    for klass in ("a", "b"):
        rows = list(db.execute(
            "SELECT ticker,side,would_price_cents,contracts,book_60s,blocked_reason "
            "FROM shadow_snipes WHERE klass=?", (klass,)))
        if not rows:
            print(f"  class {klass}: none logged")
            continue
        net = wins = graded = 0
        for tk, side, px, n, b60, blocked in rows:
            res = settled.get(tk)
            if res is None or px is None:
                continue
            graded += 1
            payoff = 100.0 if res == side else 0.0
            pnl = (payoff - px) * (n or 0)
            net += pnl
            wins += payoff > px
        blocked_n = sum(1 for r in rows if r[5])
        print(f"  class {klass}: {len(rows)} logged ({blocked_n} gate-blocked), "
              f"{graded} settled -> net {net/100:+.2f}$ ({wins}/{graded} wins)")
    print("\nPhase gates: P1 arms Class A at >=15 shadowed opps, net>0, VAR<25% "
          "gross, window > 2x detect latency. P2 arms Class B at >=25 shadows, "
          "net>0 ex-best. Do not renegotiate.")


if __name__ == "__main__":
    main()
