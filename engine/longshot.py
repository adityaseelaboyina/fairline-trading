"""Longshot lane (slow lane) — pure, testable logic. NO LLM, NO live orders.

Strategy under test (favorite-longshot bias): on slow, retail-heavy series
(weather, politics), YES contracts priced in the low single cents win LESS often
than their price implies. Selling them (buying NO at the ask) should therefore
carry positive expectancy IF the bias exceeds spread + taker fees.

Evidence so far (one research pass: 6 NYC weather events, 34 markets):
directionally confirmed (9/9 shadow-equivalent sells won, +2.1c/contract net;
0-10c bucket implied 2.2% vs 0% actual) but a 12.5c bucket DID hit — widening
the sell cap to 15c would have flipped the sample negative. Hence the hard
max_yes_mid_cents cap below and the shadow-first gates.

This module mirrors engine/sniper.py: pure functions only. All I/O (REST,
sqlite, scheduling) lives in longshot_scan.py. Nothing here can place an order.

PRE-REGISTERED ARMING GATES (config: longshot.gates) — the lane may only go
live after ALL of these hold on SHADOW data, and arming is a deliberate manual
step, never automatic:
  P1: >= 150 settled shadow entries across >= 40 distinct events
  P2: net P&L > 0 counting ONLY unblocked, depth-fillable entries
  P3: worst single UTC-day loss < 35% of gross winnings
  P4: >= 3 distinct series each individually net >= 0 (no single-series carry)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class LongshotCandidate:
    ticker: str
    event_ticker: str
    series: str
    side: str                 # always "no" in this lane (sell the YES longshot)
    yes_bid: int              # cents
    yes_ask: int              # cents
    no_ask: int               # cents actually payable (walked worst level)
    contracts: int            # fillable at <= no_ask against logged depth
    fee_cents: int            # taker fee for the whole clip at no_ask
    net_win_cents: int        # clip profit if YES misses (after fee)
    net_loss_cents: int       # clip loss if YES hits (after fee)
    hours_to_close: float


def series_of(ticker: str) -> str:
    return ticker.split("-", 1)[0] if ticker else ""


def in_entry_window(hours_to_close: float, min_h: float, max_h: float) -> bool:
    """Entry allowed only inside [min_h, max_h] hours before close.

    min_h guards against trading AFTER the outcome is effectively known
    (daily weather markets trade until midnight but the day's high is set by
    late afternoon — entering there would backtest great and mean nothing).
    max_h avoids stale books right after listing."""
    return min_h <= hours_to_close <= max_h


def scan_market(*, ticker: str, event_ticker: str, book, hours_to_close: float,
                fee_fn: Callable[..., int], cfg: dict) -> Optional[LongshotCandidate]:
    """Evaluate ONE open market for a shadow longshot-sell entry.

    Deterministic filters, every one fail-closed:
      1. entry window (per-series override, else lane default)
      2. yes mid <= max_yes_mid_cents (the 10c cap; 12.5c hits are the lesson)
      3. a REAL yes bid exists (yes_bid >= 1) — otherwise NO cannot be bought
         below 100c and the "edge" is fictional
      4. spread <= max_spread_cents (wide spread = no price, skip)
      5. depth-walked NO fill for clip_dollars at worst level <= max_no_ask
      6. net win after taker fee >= min_net_win_cents for the clip
    """
    series = series_of(ticker)
    windows = cfg.get("entry_windows", {}) or {}
    w = windows.get(series, {})
    min_h = float(w.get("min_hours", cfg.get("min_hours_to_close", 12)))
    max_h = float(w.get("max_hours", cfg.get("max_hours_to_close", 36)))
    if not in_entry_window(hours_to_close, min_h, max_h):
        return None

    yes_bid, yes_ask = book.best_yes_bid, book.best_yes_ask
    if yes_bid is None or yes_ask is None or yes_bid < 1:
        return None                                    # rule 3: no real bid
    mid = (yes_bid + yes_ask) / 2.0
    if mid > float(cfg.get("max_yes_mid_cents", 10)):
        return None                                    # rule 2
    if yes_ask - yes_bid > int(cfg.get("max_spread_cents", 5)):
        return None                                    # rule 4

    clip_cents = int(round(float(cfg.get("clip_dollars", 3.0)) * 100))
    no_touch = book.best_no_ask
    if no_touch is None or no_touch <= 0 or no_touch >= 100:
        return None
    contracts = max(1, clip_cents // no_touch)
    filled, _avg, levels = book.walk("no", "buy", contracts)
    if filled < contracts or not levels:
        return None                                    # rule 5: no fantasy fills
    worst = max(px for px, _q in levels)
    if worst > int(cfg.get("max_no_ask_cents", 97)):
        return None

    fee = fee_fn(worst, contracts, ticker=ticker)
    net_win = (100 - worst) * contracts - fee
    net_loss = worst * contracts + fee
    if net_win < int(cfg.get("min_net_win_cents", 6)):
        return None                                    # rule 6: fee floor

    return LongshotCandidate(
        ticker=ticker, event_ticker=event_ticker, series=series, side="no",
        yes_bid=yes_bid, yes_ask=yes_ask, no_ask=worst, contracts=contracts,
        fee_cents=fee, net_win_cents=net_win, net_loss_cents=net_loss,
        hours_to_close=round(hours_to_close, 2),
    )


def apply_exposure_caps(candidates: list, open_cost_cents: int,
                        open_by_event: dict, cfg: dict) -> list:
    """Portfolio gates, applied AFTER per-market filters:

      * max_per_event: NO entries on strikes of one event are mutually
        exclusive on the hit side (at most one strike can settle YES), but we
        still cap them — a settlement-source error hits the whole event.
      * max_new_per_scan: rate cap per scan run.
      * max_open_cost_dollars: total shadow cost basis across the lane. THE
        correlation cap — one weird day (heat wave, news shock) is the only
        way this lane loses big, so total same-day exposure is what's bounded.

    Candidates are ranked cheapest no_ask first (widest cushion per dollar)."""
    out = []
    max_event = int(cfg.get("max_per_event", 2))
    max_new = int(cfg.get("max_new_per_scan", 6))
    budget = int(round(float(cfg.get("max_open_cost_dollars", 60.0)) * 100))
    spent = open_cost_cents
    per_event = dict(open_by_event)
    for c in sorted(candidates, key=lambda c: c.no_ask):
        if len(out) >= max_new:
            break
        if per_event.get(c.event_ticker, 0) >= max_event:
            continue
        cost = c.no_ask * c.contracts + c.fee_cents
        if spent + cost > budget:
            continue
        out.append(c)
        spent += cost
        per_event[c.event_ticker] = per_event.get(c.event_ticker, 0) + 1
    return out


def settle_pnl_cents(side: str, result: str, entry_price_cents: int,
                     contracts: int, fee_cents: int) -> int:
    """Realized P&L for a settled shadow position (fee charged on entry only;
    settlement is free on Kalshi)."""
    won = (result == side)
    if won:
        return (100 - entry_price_cents) * contracts - fee_cents
    return -(entry_price_cents * contracts + fee_cents)


def gate_report(rows: list, cfg: dict) -> dict:
    """Evaluate the PRE-REGISTERED arming gates against settled shadow rows.
    rows: dicts with keys settled(bool), pnl_cents, event_ticker, series,
    day(str, UTC date of entry). Returns per-gate pass/fail + numbers.
    This function only reports; arming remains a manual config edit."""
    g = cfg.get("gates", {})
    need_n = int(g.get("min_settled", 150))
    need_events = int(g.get("min_events", 40))
    max_day_frac = float(g.get("max_worst_day_frac_of_gross", 0.35))
    need_series = int(g.get("min_positive_series", 3))

    settled = [r for r in rows if r.get("settled")]
    net = sum(r["pnl_cents"] for r in settled)
    gross_win = sum(r["pnl_cents"] for r in settled if r["pnl_cents"] > 0)
    by_day, by_series = {}, {}
    for r in settled:
        by_day[r["day"]] = by_day.get(r["day"], 0) + r["pnl_cents"]
        by_series[r["series"]] = by_series.get(r["series"], 0) + r["pnl_cents"]
    worst_day = min(by_day.values()) if by_day else 0
    pos_series = sum(1 for v in by_series.values() if v >= 0)

    p1 = len(settled) >= need_n and len({r["event_ticker"] for r in settled}) >= need_events
    p2 = net > 0
    p3 = gross_win > 0 and (-worst_day) < max_day_frac * gross_win
    p4 = len(by_series) >= need_series and pos_series >= need_series
    return {
        "P1_sample": {"pass": p1, "settled": len(settled),
                      "events": len({r["event_ticker"] for r in settled}),
                      "need": (need_n, need_events)},
        "P2_net_positive": {"pass": p2, "net_cents": net},
        "P3_tail": {"pass": p3, "worst_day_cents": worst_day,
                    "gross_win_cents": gross_win, "max_frac": max_day_frac},
        "P4_breadth": {"pass": p4, "series_pnl_cents": by_series},
        "all_pass": p1 and p2 and p3 and p4,
    }
