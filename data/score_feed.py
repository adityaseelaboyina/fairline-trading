"""Independent live score feed.

This is the authoritative score source. Kalshi's "last play" / ticker fields are
NEVER trusted as ground truth for the score — they may be logged for comparison,
but goal/red-card/period decisions come from here.

Default provider polls ESPN's public soccer scoreboard JSON (~10s). The provider
is pluggable: subclass ScoreProvider for FIFA's feed or any other source.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import httpx


@dataclass
class ScoreState:
    event_id: str
    home: str = ""
    away: str = ""
    home_score: int = 0
    away_score: int = 0
    home_reds: int = 0
    away_reds: int = 0
    state: str = "pre"          # pre | in | post
    period: int = 0
    clock: str = ""
    raw: dict = field(default_factory=dict)


@dataclass
class ScoreEvent:
    kind: str                   # goal | red_card | period | status | kickoff | final
    event_id: str
    team: Optional[str]         # team display name the event is "for"
    state: ScoreState
    recv_monotonic: float
    wall_ts: float
    detail: str = ""


class ScoreProvider:
    """Interface for a pluggable score source."""

    async def fetch(self) -> dict[str, ScoreState]:
        raise NotImplementedError


class ESPNScoreProvider(ScoreProvider):
    URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/scoreboard"

    def __init__(self, league: str):
        self.league = league
        self._client = httpx.AsyncClient(timeout=8.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch(self) -> dict[str, ScoreState]:
        resp = await self._client.get(self.URL.format(league=self.league))
        resp.raise_for_status()
        data = resp.json()
        out: dict[str, ScoreState] = {}
        for ev in data.get("events", []):
            comp = (ev.get("competitions") or [{}])[0]
            st = comp.get("status", ev.get("status", {})) or {}
            stype = st.get("type", {}) or {}
            state = ScoreState(
                event_id=str(ev.get("id")),
                state=stype.get("state", "pre"),
                period=st.get("period", 0),
                clock=st.get("displayClock", ""),
                raw=ev,
            )
            for c in comp.get("competitors", []):
                name = (c.get("team", {}) or {}).get("displayName", "")
                score = int(float(c.get("score", 0) or 0))
                reds = 0
                # ESPN sometimes carries red cards as a statistic; tolerate absence.
                for s in c.get("statistics", []) or []:
                    if s.get("name") in ("redCards", "redcards"):
                        reds = int(float(s.get("displayValue", 0) or 0))
                if c.get("homeAway") == "home":
                    state.home, state.home_score, state.home_reds = name, score, reds
                else:
                    state.away, state.away_score, state.away_reds = name, score, reds
            out[state.event_id] = state
        return out


class ScoreFeed:
    def __init__(self, cfg: dict, on_event: Callable[[ScoreEvent], Awaitable[None]]):
        sc = cfg["score_feed"]
        self.poll_seconds = sc.get("poll_seconds", 10)
        # Tighter cadence while any mapped game is live; failures back
        # off exponentially to 15s max.
        self.live_poll_seconds = sc.get("live_poll_seconds", 3)
        self._any_live = False
        self._backoff: float | None = None
        self.on_event = on_event
        if sc.get("provider", "espn") == "espn":
            self.provider: ScoreProvider = ESPNScoreProvider(sc.get("espn_league", "fifa.world"))
        else:
            raise ValueError(f"unknown score provider {sc.get('provider')!r}")
        # only track events we care about, if a map is supplied
        self._watch_event_ids = set(str(v) for v in sc.get("event_map", {}).values())
        self._prev: dict[str, ScoreState] = {}
        self.last_poll_wall = 0.0      # updated every successful poll (for the blind gate)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                states = await self.provider.fetch()
                self.last_poll_wall = time.time()   # fresh score => not blind
                self._backoff = None
                self._any_live = any(
                    s.state == "in" for eid, s in states.items()
                    if not self._watch_event_ids or eid in self._watch_event_ids)
                await self._diff(states)
            except Exception as exc:  # noqa: BLE001
                # Exponential backoff to 15s max on feed failures
                self._backoff = min((self._backoff or self.live_poll_seconds) * 2, 15.0)
                print(f"[score] poll error: {exc!r} (backoff {self._backoff:.0f}s)")
            interval = (self._backoff if self._backoff is not None else
                        (self.live_poll_seconds if self._any_live else self.poll_seconds))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    async def poll_once(self) -> None:
        """Force a single immediate fetch+diff (used by the contradiction breaker to
        un-freeze a stuck feed). Same body as one run() iteration."""
        try:
            states = await self.provider.fetch()
            self.last_poll_wall = time.time()
            await self._diff(states)
        except Exception as exc:  # noqa: BLE001
            print(f"[score] forced poll error: {exc!r}")

    async def _emit(self, ev: ScoreEvent) -> None:
        await self.on_event(ev)

    async def _diff(self, states: dict[str, ScoreState]) -> None:
        recv = time.monotonic()
        wall = time.time()
        for eid, cur in states.items():
            if self._watch_event_ids and eid not in self._watch_event_ids:
                continue
            prev = self._prev.get(eid)
            if prev is None:
                # first sighting; if already live emit a status so the analyst gets context
                if cur.state == "in":
                    await self._emit(ScoreEvent("status", eid, None, cur, recv, wall, "first-seen live"))
                self._prev[eid] = cur
                continue

            if prev.state != "in" and cur.state == "in":
                await self._emit(ScoreEvent("kickoff", eid, None, cur, recv, wall))
            if cur.state == "post" and prev.state != "post":
                await self._emit(ScoreEvent("final", eid, None, cur, recv, wall, "fulltime"))
            if cur.period != prev.period and cur.period:
                await self._emit(ScoreEvent("period", eid, None, cur, recv, wall, f"period {cur.period}"))

            # VAR / corrections — a score DECREASE is a disallowed goal
            # (or a team-attribution fix, which shows as one down + one up).
            if cur.home_score < prev.home_score:
                await self._emit(ScoreEvent("correction", eid, cur.home, cur, recv,
                                            wall, f"home score {prev.home_score}->{cur.home_score}"))
            if cur.away_score < prev.away_score:
                await self._emit(ScoreEvent("correction", eid, cur.away, cur, recv,
                                            wall, f"away score {prev.away_score}->{cur.away_score}"))
            if cur.home_score > prev.home_score:
                await self._emit(ScoreEvent("goal", eid, cur.home, cur, recv, wall, "home goal"))
            if cur.away_score > prev.away_score:
                await self._emit(ScoreEvent("goal", eid, cur.away, cur, recv, wall, "away goal"))
            if cur.home_reds > prev.home_reds:
                await self._emit(ScoreEvent("red_card", eid, cur.home, cur, recv, wall, "home red"))
            if cur.away_reds > prev.away_reds:
                await self._emit(ScoreEvent("red_card", eid, cur.away, cur, recv, wall, "away red"))

            self._prev[eid] = cur
