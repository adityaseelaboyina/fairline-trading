"""Sharp-book fair values.

A de-vigged live sharp line (bet365 via Goalserve, ~1s updates) is a better fair
value than our Poisson ladder: it replaces model risk with sharp consensus, and
converts Class B into lag-arbitrage (enter when Kalshi deviates from sharp fair
by more than gap+fees, either direction).

Provider is CONFIG, not code: `sharp_feed.provider: none` (ladder stays primary,
today's behavior) or `goalserve`. The interface is deliberately tiny so feeds can
be swapped/removed without touching consumers. A paid feed is only worth keeping if
sharp-anchored triggers beat ladder-anchored ones by more than the subscription.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Awaitable, Callable, Optional

import httpx

from engine.markets import parse_market


def devig(implied: dict) -> dict:
    """Proportional de-vig within one market group: probabilities scaled to sum 1.
    Input: {outcome: implied_prob (1/decimal_odds)}. Same method as the pregame
    de-vig workflow."""
    s = sum(v for v in implied.values() if v and v > 0)
    if s <= 0:
        return {}
    return {k: v / s for k, v in implied.items() if v and v > 0}


def map_group_to_kalshi(game_code: str, home_code: str, away_code: str,
                        group: str, fairs: dict, line: float | None = None) -> dict:
    """Map one de-vigged bookmaker market group to Kalshi tickers -> fair prob.
    group: '1x2' (fairs: home/draw/away), 'total' (fairs: over/under + line),
    'team_total_home'/'team_total_away' (fairs: over/under + line)."""
    out = {}
    if group == "1x2":
        if "home" in fairs:
            out[f"KXWCGAME-{game_code}-{home_code}"] = fairs["home"]
        if "away" in fairs:
            out[f"KXWCGAME-{game_code}-{away_code}"] = fairs["away"]
        if "draw" in fairs:
            out[f"KXWCGAME-{game_code}-TIE"] = fairs["draw"]
    elif group == "total" and line is not None and "over" in fairs:
        k = int(line + 0.5)                       # bookmaker Over 2.5 -> suffix 3
        if abs((k - 0.5) - line) < 1e-9:          # only .5 lines map cleanly
            out[f"KXWCTOTAL-{game_code}-{k}"] = fairs["over"]
    elif group in ("team_total_home", "team_total_away") and line is not None \
            and "over" in fairs:
        k = int(line + 0.5)
        if abs((k - 0.5) - line) < 1e-9:
            team = home_code if group.endswith("home") else away_code
            out[f"KXWCTEAMTOTAL-{game_code}-{team}{k}"] = fairs["over"]
    return out


class SharpProvider:
    """Interface: fetch live de-viggable odds + VAR events for the mapped games."""

    async def fetch(self) -> tuple[dict, list]:
        """Returns ({kalshi_ticker: fair_prob}, [var_event_game_codes])."""
        raise NotImplementedError

    async def aclose(self) -> None:
        pass


class GoalserveProvider(SharpProvider):
    """Goalserve in-play soccer odds (bet365 feed). Parses defensively — the
    exact payload shape is verified during the trial; anything unparseable is
    skipped rather than guessed at."""
    URL = "http://www.goalserve.com/getfeed/{key}/getodds/soccer?cat=soccer_10&json=1"

    def __init__(self, cfg: dict, game_meta: dict):
        sf = cfg.get("sharp_feed", {})
        self.key = os.environ.get(sf.get("api_key_env", "GOALSERVE_API_KEY"), "")
        self.game_meta = game_meta          # game_code -> (home_code, away_code, names)
        self._client = httpx.AsyncClient(timeout=5.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch(self) -> tuple[dict, list]:
        if not self.key:
            return {}, []
        resp = await self._client.get(self.URL.format(key=self.key))
        resp.raise_for_status()
        data = resp.json()
        fairs: dict = {}
        var_games: list = []
        for match in (data.get("scores", {}) or {}).get("categories", []) or []:
            # defensive walk; skip anything that doesn't match a mapped game
            code = self._match_game(match)
            if code is None:
                continue
            home, away, _ = self.game_meta[code]
            if str(match.get("status", "")).upper().startswith("VAR"):
                var_games.append(code)
            for group, g_fairs, line in self._extract_groups(match):
                fairs.update(map_group_to_kalshi(code, home, away, group,
                                                 devig(g_fairs), line))
        return fairs, var_games

    def _match_game(self, match: dict) -> Optional[str]:
        h = str(match.get("localteam", {}).get("name", "")).lower()
        a = str(match.get("visitorteam", {}).get("name", "")).lower()
        for code, (_hc, _ac, names) in self.game_meta.items():
            if names and names[0].lower() in h and names[1].lower() in a:
                return code
        return None

    @staticmethod
    def _extract_groups(match: dict):
        """Yield (group, {outcome: implied_prob}, line) from a Goalserve match blob."""
        for odds_type in match.get("odds", {}).get("type", []) or []:
            name = str(odds_type.get("value", "")).lower()
            books = odds_type.get("bookmaker", []) or []
            b365 = next((b for b in books
                         if str(b.get("name", "")).lower() == "bet365"), None)
            if b365 is None:
                continue
            odds = {str(o.get("name", "")).lower(): o.get("value")
                    for o in b365.get("odd", []) or []}

            def imp(k):
                try:
                    v = float(odds.get(k, 0) or 0)
                    return 1.0 / v if v > 1.0 else None
                except (TypeError, ValueError):
                    return None
            if name in ("match winner", "1x2", "fulltime result"):
                yield "1x2", {"home": imp("home") or imp("1"),
                              "draw": imp("draw") or imp("x"),
                              "away": imp("away") or imp("2")}, None
            elif name.startswith("over/under"):
                try:
                    line = float(name.split()[-1])
                except ValueError:
                    continue
                yield "total", {"over": imp(f"over {line}") or imp("over"),
                                "under": imp(f"under {line}") or imp("under")}, line


class SharpFeed:
    """Freshness-disciplined cache + poll loop. get_fair_cents() returns None when
    stale (>fresh_seconds), so consumers fall back to the ladder AND log it."""

    def __init__(self, cfg: dict, game_meta: dict,
                 on_var: Callable[[str], Awaitable[None]] | None = None):
        sf = cfg.get("sharp_feed", {}) or {}
        self.provider_name = sf.get("provider", "none")
        self.poll_seconds = float(sf.get("poll_seconds", 1))
        self.fresh_seconds = float(sf.get("fresh_seconds", 10))
        self.on_var = on_var
        self.provider: SharpProvider | None = (
            GoalserveProvider(cfg, game_meta) if self.provider_name == "goalserve"
            else None)
        self._fairs: dict = {}       # ticker -> (prob, wall)
        self._stale_logged = 0.0
        self._stop = asyncio.Event()

    def enabled(self) -> bool:
        return self.provider is not None

    def stop(self) -> None:
        self._stop.set()

    def update(self, fairs: dict, wall: float | None = None) -> None:
        w = wall if wall is not None else time.time()
        for t, p in fairs.items():
            self._fairs[t] = (p, w)

    def get_fair_cents(self, ticker: str, now: float | None = None):
        """De-vigged sharp fair in cents, or None if unknown/stale."""
        rec = self._fairs.get(ticker)
        if rec is None:
            return None
        prob, wall = rec
        now = now if now is not None else time.time()
        if now - wall > self.fresh_seconds:
            if now - self._stale_logged > 30:
                self._stale_logged = now
                print(f"[sharp] stale >{self.fresh_seconds:.0f}s — ladder fallback")
            return None
        return prob * 100.0

    async def run(self) -> None:
        if self.provider is None:
            return
        while not self._stop.is_set():
            try:
                fairs, var_games = await self.provider.fetch()
                if fairs:
                    self.update(fairs)
                if self.on_var:
                    for g in var_games:
                        await self.on_var(g)
            except Exception as exc:  # noqa: BLE001
                print(f"[sharp] poll error: {exc!r}")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_seconds)
            except asyncio.TimeoutError:
                pass
