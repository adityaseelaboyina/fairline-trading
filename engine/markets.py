"""Event-market semantics + thesis-invalidation exits.

On a Kalshi event contract the price is a temporary opinion about a binary that
settles to $1 or $0 on the FINAL score. A mid-game price dip with the scoreline
unchanged is noise, not information. So downside protection is keyed to the
SCORE STATE, never to price movement:

  thesis_dead(side, market, score) -> True only when the score has made this
  position a near-certain loser (the under crossed its line, your team is losing
  late, the game finished against you). It NEVER returns True on price movement.

parse_market() turns a Kalshi ticker into (kind, team_code, line) using the game
code embedded in the ticker (e.g. 26JUN23PANCRO -> home=PAN, away=CRO), so no
external name matching is needed.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class MarketSpec:
    kind: str            # moneyline | game_total | team_total | spread | unknown
    home_code: str
    away_code: str
    team_code: Optional[str] = None   # the team the leg is about (moneyline/team_total/spread)
    is_tie: bool = False              # moneyline tie leg
    line: Optional[float] = None      # over/under or spread line (X.5)


@dataclass
class ScoreView:
    home_goals: int
    away_goals: int
    minute: float
    status: str          # pre | live | ht | final


def series_kind(series: str) -> Optional[str]:
    """Classify a soccer series by its NAME SUFFIX, sport-agnostically.
    KXWCGAME / KXUCLGAME / KXMLSGAME (World Cup, Champions League, MLS) share one
    grammar — only the root differs. Order matters (TEAMTOTAL before TOTAL)."""
    for k in ("TEAMTOTAL", "TOTAL", "SPREAD", "GAME"):
        if series.endswith(k):
            return k
    return None


def series_root(series: str) -> str:
    """'KXUCLTOTAL' -> 'KXUCL'; the sport prefix used to build sibling tickers."""
    k = series_kind(series)
    return series[: -len(k)] if k else series


def parse_market(ticker: str) -> MarketSpec:
    parts = ticker.split("-")
    series = parts[0]
    code = parts[1] if len(parts) > 1 else ""
    suffix = parts[2] if len(parts) > 2 else ""
    home_code, away_code = code[-6:-3], code[-3:]

    def _line_from_n(n_str: str) -> Optional[float]:
        try:
            return int(n_str) - 0.5    # suffix N == "Over (N-0.5)" / "by more than (N-0.5)"
        except ValueError:
            return None

    kind = series_kind(series)
    if kind == "GAME":
        if suffix == "TIE":
            return MarketSpec("moneyline", home_code, away_code, is_tie=True)
        return MarketSpec("moneyline", home_code, away_code, team_code=suffix)
    if kind == "TOTAL":
        return MarketSpec("game_total", home_code, away_code, line=_line_from_n(suffix))
    if kind == "TEAMTOTAL":
        return MarketSpec("team_total", home_code, away_code,
                          team_code=suffix[:3], line=_line_from_n(suffix[3:]))
    if kind == "SPREAD":
        return MarketSpec("spread", home_code, away_code,
                          team_code=suffix[:3], line=_line_from_n(suffix[3:]))
    return MarketSpec("unknown", home_code, away_code)


def _team_goals(spec: MarketSpec, s: ScoreView) -> int:
    return s.home_goals if spec.team_code == spec.home_code else s.away_goals


def _opp_goals(spec: MarketSpec, s: ScoreView) -> int:
    return s.away_goals if spec.team_code == spec.home_code else s.home_goals


def thesis_dead(side: str, spec: MarketSpec, s: ScoreView,
                late_minute: int = 80) -> Optional[str]:
    """Return a short reason string if the SCORE STATE has invalidated this
    position, else None. Never fires on price."""
    final = s.status == "final"
    if spec.kind == "moneyline":
        if spec.is_tie:
            if side == "yes":   # betting a draw
                if final and s.home_goals != s.away_goals:
                    return "final not a draw"
            else:               # betting NOT a draw
                if final and s.home_goals == s.away_goals:
                    return "final is a draw"
            return None
        my, opp = _team_goals(spec, s), _opp_goals(spec, s)
        if side == "yes":       # need the team to win
            if final and my <= opp:
                return "final: team did not win"
            if my < opp and s.minute >= late_minute:
                return f"losing {my}-{opp} at {s.minute:.0f}'"
        else:                   # team does NOT win (draw/lose ok)
            if final and my > opp:
                return "final: team won"
        return None
    if spec.line is None:
        return None
    if spec.kind == "game_total":
        total = s.home_goals + s.away_goals
        if side == "no" and total > spec.line:
            return f"total {total} crossed {spec.line}"
        if side == "yes" and final and total < spec.line:
            return f"final total {total} < {spec.line}"
        return None
    if spec.kind == "team_total":
        tg = _team_goals(spec, s)
        if side == "no" and tg > spec.line:
            return f"team {tg} crossed {spec.line}"
        if side == "yes" and final and tg < spec.line:
            return f"final team {tg} < {spec.line}"
        return None
    if spec.kind == "spread":
        margin = _team_goals(spec, s) - _opp_goals(spec, s)
        if side == "no" and margin > spec.line:
            return f"margin {margin} covered {spec.line}"
        if side == "yes" and final and margin <= spec.line:
            return f"final margin {margin} <= {spec.line}"
        return None
    return None
