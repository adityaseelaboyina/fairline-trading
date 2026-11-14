"""Sniper lane: deterministic post-goal logic. NO LLM here, ever.

The bot's one demonstrated edge is reacting to verified goals faster than retail.
This module holds the pure, testable pieces:

  * decided_yes()      — Class A truth table: which markets a goal makes CERTAIN.
                         Only totals/team-totals; Overs only (goals are irreversible,
                         modulo VAR, which the correction handler covers); a lead is NEVER certainty
                         so spreads/moneylines are excluded by construction.
  * depth_fill()       — counterfactual fills priced at the WORST level needed to fill
                         the clip against logged depth. Never mids: mid-priced fills overstate P&L.
  * class_a_scan()     — decided markets still quoted below certainty.
  * class_b_fair() / class_b_scan()
                       — repriced-but-uncertain markets: settle-state + remaining
                         Poisson mass from the PRE-GOAL ladder lambda (post-goal books
                         are the thing that's wrong, so never derive fair from them).
  * SpikeDetector      — book-shock pre-trigger: >=3 markets of one game moving >=8c
                         the same way within 2s. Only ever triggers a score FETCH,
                         never a trade.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from data.orderbook import OrderBook
from engine.markets import parse_market


# ---------- Class A: mathematically decided -------------------------------

def decided_yes(ticker: str, home_goals: int, away_goals: int) -> bool:
    """True iff this market's YES is CERTAIN given the current score.
    Overs only: total/team goals STRICTLY above the line (at exactly the line's
    integer part it is NOT decided). Unders are never decided before the whistle.
    Spreads and moneylines are never Class A — a lead is not certainty."""
    try:
        spec = parse_market(ticker)
    except Exception:  # noqa: BLE001
        return False
    if spec.line is None:
        return False
    if spec.kind == "game_total":
        return (home_goals + away_goals) > spec.line
    if spec.kind == "team_total":
        tg = home_goals if spec.team_code == spec.home_code else away_goals
        return tg > spec.line
    return False


def depth_fill(book: OrderBook, side: str, clip_cents: int):
    """Counterfactual BUY fill for ~clip_cents, priced at the WORST level consumed
    against logged depth (never a mid, never the touch alone).
    Returns (contracts, worst_price_cents, ask) or None if depth can't fill the clip."""
    ask = book.best_yes_ask if side == "yes" else book.best_no_ask
    if ask is None or ask <= 0:
        return None
    contracts = max(1, int(clip_cents / ask))
    filled, _avg, levels = book.walk(side, "buy", contracts)
    if filled < contracts or not levels:
        return None                     # insufficient logged depth -> no fantasy fill
    worst = max(px for px, _q in levels)
    return contracts, worst, ask


def class_a_scan(books: dict, game_tickers: list, home_goals: int, away_goals: int,
                 max_price: int, clip_cents: int, max_markets: int) -> list:
    """Decided-YES markets still quoted at ask <= max_price with depth for the clip.
    Sorted lowest ask first (most room), capped at max_markets."""
    out = []
    for t in game_tickers:
        b = books.get(t)
        if b is None or not decided_yes(t, home_goals, away_goals):
            continue
        f = depth_fill(b, "yes", clip_cents)
        if f is None:
            continue
        contracts, worst, ask = f
        if ask > max_price or worst > max_price + 2:
            continue
        out.append({"ticker": t, "side": "yes", "ask": ask, "worst": worst,
                    "contracts": contracts,
                    "order_px": min(ask + 1, 95)})   # marketable IOC ask+1, hard 95c cap
    out.sort(key=lambda c: c["ask"])
    return out[:max_markets]


# ---------- Class B: repriced-but-uncertain --------------------------------

def _pois_sf(lam: float, k: int) -> float:
    """P(X >= k), X ~ Poisson(lam)."""
    if k <= 0:
        return 1.0
    cdf, term = 0.0, math.exp(-lam)
    for i in range(0, k):
        if i > 0:
            term *= lam / i
        cdf += term
    return max(0.0, min(1.0, 1.0 - cdf))


def _margin_prob(cur: int, lam_a: float, lam_b: float, cmp_fn) -> float:
    pa = [math.exp(-lam_a) * lam_a ** k / math.factorial(k) for k in range(11)]
    pb = [math.exp(-lam_b) * lam_b ** k / math.factorial(k) for k in range(11)]
    return sum(pa[a] * pb[b] for a in range(11) for b in range(11)
               if cmp_fn(cur + a - b))


def class_b_fair(ticker: str, home_goals: int, away_goals: int, minute: float,
                 lam_home: float, lam_away: float) -> Optional[float]:
    """Post-goal fair (YES, cents) for an UNDECIDED total/team-total/TIE market:
    current settle-state + remaining Poisson mass. Lambdas are FULL-MATCH pre-goal
    values (from the last pre-event ladder snapshot), scaled by time remaining here.
    Returns None for markets this class doesn't touch (spreads, team-ML, decided)."""
    try:
        spec = parse_market(ticker)
    except Exception:  # noqa: BLE001
        return None
    rem = max(0.0, (90.0 - minute) / 90.0)
    lh, la = lam_home * rem, lam_away * rem
    if spec.kind == "game_total" and spec.line is not None:
        total = home_goals + away_goals
        if total > spec.line:
            return None                                   # decided -> Class A's job
        need = math.ceil(spec.line - total)               # more goals needed
        return _pois_sf(lh + la, need) * 100.0
    if spec.kind == "team_total" and spec.line is not None:
        tg = home_goals if spec.team_code == spec.home_code else away_goals
        lt = lh if spec.team_code == spec.home_code else la
        if tg > spec.line:
            return None
        return _pois_sf(lt, math.ceil(spec.line - tg)) * 100.0
    if spec.kind == "moneyline" and spec.is_tie:
        return _margin_prob(home_goals - away_goals, lh, la, lambda d: d == 0) * 100.0
    return None


def class_b_scan(books: dict, game_tickers: list, home_goals: int, away_goals: int,
                 minute: float, lam_home: float, lam_away: float,
                 min_gap: int, clip_cents: int, max_snipes: int,
                 fair_override: dict | None = None,
                 override_extra_gap: int = 0) -> list:
    """Undecided TOTAL/TEAMTOTAL/TIE markets whose price sits >= min_gap from the
    post-goal fair, within 20-80c, with depth. Order capped at fair - 4c (never pay
    through more than the gap justifies). Sorted widest gap first, max max_snipes.
    Every candidate carries its MANDATORY exit minute (entry minute + 2).

    `fair_override` ({ticker: fair_yes_cents} from the de-vigged sharp
    feed) replaces the ladder fair where present, turning this into lag-arbitrage;
    overridden fairs demand min_gap + override_extra_gap (round-trip fees)."""
    fair_override = fair_override or {}
    out = []
    for t in game_tickers:
        b = books.get(t)
        if b is None:
            continue
        model_fair = class_b_fair(t, home_goals, away_goals, minute, lam_home, lam_away)
        sharp = fair_override.get(t)
        if sharp is not None and model_fair is not None:
            fair_yes, need_gap, src = sharp, min_gap + override_extra_gap, "sharp"
        elif model_fair is not None:
            fair_yes, need_gap, src = model_fair, min_gap, "ladder"
        else:
            continue
        for side, fair in (("yes", fair_yes), ("no", 100.0 - fair_yes)):
            f = depth_fill(b, side, clip_cents)
            if f is None:
                continue
            contracts, worst, ask = f
            gap = fair - ask
            if gap < need_gap or not 20 <= ask <= 80:
                continue
            cap = int(fair - 4)
            if worst > cap:
                continue
            out.append({"ticker": t, "side": side, "ask": ask, "worst": worst,
                        "contracts": contracts, "fair": fair, "gap": gap,
                        "order_px": min(ask + 1, cap),
                        "exit_minute": int(minute) + 2,
                        "fair_source": src})
    out.sort(key=lambda c: -c["gap"])
    return out[:max_snipes]


# ---------- Book-spike pre-trigger -------------------------------

@dataclass
class SpikeDetector:
    """>=`n_markets` markets of ONE game moving their yes-touch >=`move_cents` in the
    SAME direction within `window_s` -> repricing shock. Fires a callback that only
    triggers an out-of-cycle score fetch — a spike never authorizes a trade. Thin-book
    false positives are harmless by design (they just cause a fetch)."""
    n_markets: int = 3
    move_cents: int = 8
    window_s: float = 2.0
    cooldown_s: float = 5.0
    _hist: dict = field(default_factory=dict)     # ticker -> deque[(wall, mid)]
    _moves: deque = field(default_factory=deque)  # (wall, game, ticker, direction)
    _last_fire: dict = field(default_factory=dict)

    def on_tick(self, ticker: str, game: str, mid: float, wall: float):
        """Feed a touch update. Returns the game code when a spike fires, else None."""
        h = self._hist.setdefault(ticker, deque())
        h.append((wall, mid))
        while h and wall - h[0][0] > self.window_s:
            h.popleft()
        move = mid - h[0][1]
        if abs(move) < self.move_cents:
            return None                      # only evaluate on a qualifying move
        d = 1 if move > 0 else -1
        self._moves.append((wall, game, ticker, d))
        h.clear()
        h.append((wall, mid))
        while self._moves and wall - self._moves[0][0] > self.window_s:
            self._moves.popleft()
        movers = {tk for _w, g, tk, dd in self._moves if g == game and dd == d}
        if (len(movers) >= self.n_markets
                and wall - self._last_fire.get(game, 0.0) >= self.cooldown_s):
            self._last_fire[game] = wall
            return game
        return None
