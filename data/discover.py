"""Per-game CORE market discovery.

Given a game code (e.g. 26JUN23PORUZB) or its moneyline event ticker, query the
whitelisted CORE series and print config-ready `market_tickers` + `event_map`.

CORE = moneyline (KXWCGAME), goal spread (KXWCSPREAD), game total goals
(KXWCTOTAL), team totals (KXWCTEAMTOTAL). Everything else (1st/2nd-half, BTTS,
player/game props) is excluded simply by not being on the whitelist.

The ESPN event id for `event_map` is auto-matched from the two team codes in the
game code against the live fifa.world scoreboard; if no match, a placeholder is
emitted for you to fill in.
"""
from __future__ import annotations

import httpx

from .kalshi_rest import KalshiREST

_LABELS = {
    "KXWCGAME": "moneyline",
    "KXWCSPREAD": "spread",
    "KXWCTOTAL": "total",
    "KXWCTEAMTOTAL": "team-total",
}
DEFAULT_CORE_SERIES = list(_LABELS)
_ACTIVE = {"active", "open", "initialized", "unopened"}


def _code_of(arg: str) -> str:
    """Accept a bare code (26JUN23PORUZB) or a full ticker/event and return the code."""
    if arg.upper().startswith("KX") and "-" in arg:
        return arg.split("-", 1)[1].rsplit("-", 1)[0] if arg.count("-") > 1 else arg.split("-", 1)[1]
    return arg


async def _espn_index(league: str) -> list[tuple[set, str, str]]:
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/scoreboard"
    out = []
    try:
        async with httpx.AsyncClient(timeout=8.0) as c:
            data = (await c.get(url)).json()
        for ev in data.get("events", []):
            comp = (ev.get("competitions") or [{}])[0]
            abbrs, names = set(), []
            for t in comp.get("competitors", []):
                team = t.get("team", {}) or {}
                if team.get("abbreviation"):
                    abbrs.add(team["abbreviation"].upper())
                names.append(team.get("displayName", ""))
            out.append((abbrs, str(ev.get("id")), " vs ".join(names)))
    except Exception as exc:  # noqa: BLE001
        print(f"# [discover] ESPN lookup failed: {exc!r}")
    return out


def _match_espn(code: str, index: list[tuple[set, str, str]]):
    teams = {code[-6:-3].upper(), code[-3:].upper()}  # last two 3-letter codes
    for abbrs, eid, label in index:
        if teams & abbrs == teams or teams <= abbrs:
            return eid, label
    # loose: any overlap of both codes against abbreviations as substrings
    for abbrs, eid, label in index:
        joined = "".join(abbrs)
        if all(t in joined for t in teams):
            return eid, label
    return None, None


async def discover(cfg: dict, rest: KalshiREST, games: list[str]) -> None:
    core = cfg.get("discover", {}).get("core_series", DEFAULT_CORE_SERIES)
    league = cfg.get("score_feed", {}).get("espn_league", "fifa.world")
    index = await _espn_index(league)

    tickers: list[str] = []
    event_map: dict[str, str] = {}
    for arg in games:
        code = _code_of(arg)
        espn_id, espn_label = _match_espn(code, index)
        found = []
        for series in core:
            try:
                ms = await rest.list_markets(event_ticker=f"{series}-{code}")
            except Exception:  # noqa: BLE001
                ms = []
            for m in ms:
                if (m.get("status") or "").lower() in _ACTIVE:
                    found.append((series, m))
        print(f"\n# === {code} === ESPN event {espn_id or 'UNKNOWN — fill in'}"
              f"{f' ({espn_label})' if espn_label else ''} | {len(found)} core markets")
        for series, m in found:
            t = m["ticker"]
            tickers.append(t)
            event_map[t] = espn_id or "<ESPN_EVENT_ID>"
            print(f"  {t:42} [{_LABELS.get(series, series)}] {m.get('yes_sub_title') or ''}")

    print("\n# ---- paste into config.yaml ----")
    print("market_tickers:")
    for t in tickers:
        print(f'  - "{t}"')
    print("score_feed:")
    print("  event_map:")
    for t, e in event_map.items():
        print(f'    "{t}": "{e}"')
