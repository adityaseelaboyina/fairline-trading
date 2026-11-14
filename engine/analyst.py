"""LLM analyst — runs OFF the millisecond hot path.

Called PRE-GAME and on major verified state changes (goal, red card, halftime,
fulltime) and on a slow dead-time refresh — NEVER per WS tick. It receives the
current board snapshot plus the independently-verified score context and returns
STRICT JSON (schema below) describing the best trade and machine-readable
exit_rules that the deterministic engine in rules.py will execute.

All timing is captured (start/end monotonic + wall) so the recorder can measure
decision latency and price drift during "thinking" time.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Optional

import anthropic

# Verbatim analyst system prompt, with the JSON contract appended.
SYSTEM_PROMPT = """You are a prediction market analyst for Kalshi scanning a live World Cup board. Pick the single best trade now.
Factor in current score, game state, time remaining, team form, base rates, and cross-market pricing inefficiencies
(de-vig the moneyline, back out Poisson lambdas, flag derivatives that disagree with the sharp line).
Before finalizing: run a self-audit. List which inputs you VERIFIED vs ASSUMED. If research tools are available and key
inputs are unverified, verify them. Your exit_rules will be executed by a deterministic engine with no judgment, so they
must be unambiguous and event-keyed. You are called outside the live hot path — be thorough, not fast. Output ONLY the JSON.

Output ONLY a single JSON object matching exactly this schema (no prose, no markdown fences):
{
  "best_trade": {"market_ticker": str, "side": "yes"|"no", "limit_price_cents": int},
  "fair_value_cents": int,
  "edge_cents": int,
  "confidence": "low"|"medium"|"high",
  "ranked_alternatives": [{"market_ticker": str, "side": str, "price_cents": int, "edge_cents": int, "note": str}],
  "exit_rules": [{"event": "goal_for:TEAM"|"goal_against:TEAM"|"red_card:TEAM"|"clock>=MIN"|"price>=CENTS"|"price<=CENTS", "action": "sell_all"|"sell_half"|"hold"|"add", "reason": str}],
  "inputs_verified": [str],
  "inputs_assumed": [str],
  "self_audit": str
}"""

_REQUIRED_KEYS = {
    "best_trade", "fair_value_cents", "edge_cents", "confidence",
    "ranked_alternatives", "exit_rules", "inputs_verified", "inputs_assumed",
    "self_audit",
}


@dataclass
class AnalystResult:
    parsed: Optional[dict]
    raw_text: str
    parsed_ok: bool
    error: Optional[str]
    start_wall: float
    end_wall: float
    start_monotonic: float
    end_monotonic: float

    @property
    def latency_ms(self) -> float:
        return (self.end_monotonic - self.start_monotonic) * 1000.0


class Analyst:
    def __init__(self, cfg: dict):
        ac = cfg["anthropic"]
        key = os.environ.get(ac["api_key_env"])
        if not key:
            raise RuntimeError(f"Set {ac['api_key_env']} in the environment.")
        self.client = anthropic.AsyncAnthropic(api_key=key)
        self.model = ac["model"]
        self.max_tokens = ac.get("max_tokens", 4096)

    async def analyze(self, *, board_snapshot: dict, score_context: dict,
                      trigger: str, context: Optional[dict] = None) -> AnalystResult:
        user_payload = {
            "trigger": trigger,
            "verified_score_context": score_context,   # authoritative independent feed
            "sharp_context": context or {},             # optional crowd-vs-model lines
            "board_snapshot": board_snapshot,           # grouped by game (event id) -> series -> market
            "instructions": (
                "score_context is the authoritative score; Kalshi last-play/ticker fields are "
                "reference only and must NOT be trusted as the score. board_snapshot is grouped "
                "by game (ESPN event id) then by series (KXWCGAME=moneyline, KXWCSPREAD=goal "
                "spread, KXWCTOTAL=game total goals, KXWCTEAMTOTAL=team total). Run the "
                "cross-market consistency check PER GAME: de-vig the moneyline, back out Poisson "
                "lambdas, reprice the spread/totals/team-totals off that anchor, and flag any "
                "derivative that disagrees with the sharp line. Return the SINGLE best edge across "
                "ALL markets and ALL games — not just moneyline. Each quote includes "
                "yes_fillable_at_ask / no_fillable_at_ask (resting contracts at the touch): do NOT "
                "make best_trade a market you cannot fill at meaningful size; put thin-book ideas "
                "in ranked_alternatives instead. NO-TRADE GATE: if a game is effectively decided "
                "(any moneyline leg >= 95c) or your edge does not clearly clear round-trip fees, "
                "do NOT force a trade — set best_trade to a clear pass with low confidence and "
                "edge_cents <= 0, and explain in self_audit. A scan running is not a reason to trade. "
                "CONTEXT: do not run a vanilla Poisson in a vacuum. Adjust goal-expectancy for game "
                "state — a must-win/desperation situation RAISES goal rate, so NO-overs edges are "
                "systematically OVERSTATED there; widen your lambda band and haircut those edges. If "
                "sharp_context has a model/sharp line (e.g. moneyline_sharp_prob), compare it to "
                "Kalshi pricing and PREFER a crowd-vs-model moneyline divergence over a thin totals "
                "grind in an efficient corner of the book."
            ),
        }
        start_wall, start_mono = time.time(), time.monotonic()
        payload_str = json.dumps(user_payload)
        text, err = await self._call(payload_str)
        parsed, perr = (None, err) if err else self._parse(text)
        # one strict retry if the model returned unparseable output
        if not err and parsed is None:
            text2, err2 = await self._call(
                payload_str + "\n\nREMINDER: reply with ONLY the JSON object, no prose, "
                "no markdown fences.")
            if not err2:
                p2, perr2 = self._parse(text2)
                if p2 is not None:
                    text, parsed, perr = text2, p2, None
        end_wall, end_mono = time.time(), time.monotonic()
        ok = parsed is not None
        return AnalystResult(parsed, text, ok, (None if ok else perr),
                             start_wall, end_wall, start_mono, end_mono)

    async def _call(self, content: str):
        try:
            resp = await self.client.messages.create(
                model=self.model, max_tokens=self.max_tokens, system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": content}],
            )
            return "".join(b.text for b in resp.content
                           if getattr(b, "type", "") == "text"), None
        except Exception as exc:  # noqa: BLE001
            return "", f"api_error: {exc!r}"

    @staticmethod
    def _extract_json(text: str):
        """Robust JSON extraction: strip fences, try whole, else scan the first
        BALANCED {...}. Never raises."""
        if not text:
            return None
        import re as _re
        t = _re.sub(r"```(?:json)?", "", text).strip()
        try:
            return json.loads(t)
        except Exception:  # noqa: BLE001
            pass
        start = t.find("{")
        if start == -1:
            return None
        depth = 0
        for i in range(start, len(t)):
            if t[i] == "{":
                depth += 1
            elif t[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(t[start:i + 1])
                    except Exception:  # noqa: BLE001
                        return None
        return None

    @classmethod
    def _parse(cls, text: str):
        """Validate required keys. Returns (obj, error)."""
        obj = cls._extract_json(text)
        if obj is None:
            return None, "no_json_object"
        missing = _REQUIRED_KEYS - set(obj)
        if missing:
            return None, f"missing_keys: {sorted(missing)}"
        bt = obj.get("best_trade") or {}
        # a pass (no trade) is allowed: best_trade may be null
        if bt and not all(k in bt for k in ("market_ticker", "side", "limit_price_cents")):
            return None, "best_trade_incomplete"
        if bt and bt.get("side") not in ("yes", "no"):
            return None, "best_trade_bad_side"
        return obj, None
