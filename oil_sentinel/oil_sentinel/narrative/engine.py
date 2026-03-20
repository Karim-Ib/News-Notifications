"""
Narrative trend tracking engine.

Computes a rolling 48-hour weighted sentiment state from scored alerts,
detects state transitions, tracks momentum across consecutive 12h windows,
and identifies the top-3 key drivers for the current state.

State ladder (most bearish → most bullish):
  strong_de_escalation → de_escalation → stable → escalation → strong_escalation

Weighted score formula (per article):
  direction_sign × magnitude × confidence × tier_weight
  Averaged across all articles in the window.
  Range: roughly −12 to +12 (tier1 article, mag 10, conf 1.0)

State thresholds:
  score ≥  3.0  → strong_escalation
  score ≥  1.0  → escalation
  score ≤ −3.0  → strong_de_escalation
  score ≤ −1.0  → de_escalation
  else          → stable

Momentum: compare mean score of previous 12h window to current 12h window.
  Δ > +0.5 → strengthening
  Δ < −0.5 → weakening
  else      → stable
"""

import json
import logging
import sqlite3
from typing import Optional

from oil_sentinel.db import (
    get_connection,
    get_latest_narrative_state,
    insert_narrative_state,
    transaction,
)

logger = logging.getLogger(__name__)

# ── State definitions ──────────────────────────────────────────────────────

STATE_EMOJI = {
    "strong_escalation":    "🔴",
    "escalation":           "🟠",
    "stable":               "🟡",
    "de_escalation":        "🟢",
    "strong_de_escalation": "💚",
}

STATE_LABELS = {
    "strong_escalation":    "STRONG ESCALATION",
    "escalation":           "ESCALATION",
    "stable":               "STABLE",
    "de_escalation":        "DE-ESCALATION",
    "strong_de_escalation": "STRONG DE-ESCALATION",
}

_DIRECTION_SIGN = {"bullish": 1, "bearish": -1, "neutral": 0}
_STRONG_THRESHOLD = 3.0
_MILD_THRESHOLD   = 1.0
_MOMENTUM_THRESHOLD = 0.5


# ── DB helpers ─────────────────────────────────────────────────────────────

def _fetch_alerts(conn: sqlite3.Connection, within_hours: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT al.id, al.direction, al.magnitude, al.confidence,
               al.narrative_key, al.summary, al.created_at,
               ar.source_name, ar.url AS article_url
        FROM alerts al
        LEFT JOIN articles ar ON al.article_id = ar.id
        WHERE al.created_at >= datetime('now', ? || ' hours')
        ORDER BY al.created_at ASC
        """,
        (f"-{within_hours}",),
    ).fetchall()
    return [dict(r) for r in rows]


def _fetch_alerts_between(
    conn: sqlite3.Connection, from_hours: int, to_hours: int
) -> list[dict]:
    """Alerts created between `from_hours` ago and `to_hours` ago."""
    rows = conn.execute(
        """
        SELECT al.id, al.direction, al.magnitude, al.confidence,
               al.narrative_key, al.summary, al.created_at,
               ar.source_name, ar.url AS article_url
        FROM alerts al
        LEFT JOIN articles ar ON al.article_id = ar.id
        WHERE al.created_at >= datetime('now', ? || ' hours')
          AND al.created_at <  datetime('now', ? || ' hours')
        ORDER BY al.created_at ASC
        """,
        (f"-{from_hours}", f"-{to_hours}"),
    ).fetchall()
    return [dict(r) for r in rows]


# ── Computation helpers ────────────────────────────────────────────────────

def _tier_weight(source_name: Optional[str], tier1_domains: set) -> float:
    if not source_name or not tier1_domains:
        return 1.0
    src = source_name.lower()
    return 1.2 if any(d.lower() in src for d in tier1_domains) else 1.0


def _weighted_score(alerts: list[dict], tier1_domains: set) -> float:
    """Mean signed sentiment contribution across all alerts in the list."""
    if not alerts:
        return 0.0
    total = sum(
        _DIRECTION_SIGN.get(a.get("direction") or "neutral", 0)
        * (a.get("magnitude") or 0)
        * (a.get("confidence") or 0.0)
        * _tier_weight(a.get("source_name"), tier1_domains)
        for a in alerts
    )
    return total / len(alerts)


def _derive_state(score: float) -> str:
    if score >= _STRONG_THRESHOLD:
        return "strong_escalation"
    if score >= _MILD_THRESHOLD:
        return "escalation"
    if score <= -_STRONG_THRESHOLD:
        return "strong_de_escalation"
    if score <= -_MILD_THRESHOLD:
        return "de_escalation"
    return "stable"


def _compute_momentum(curr_score: float, prev_score: float) -> str:
    delta = curr_score - prev_score
    if delta > _MOMENTUM_THRESHOLD:
        return "strengthening"
    if delta < -_MOMENTUM_THRESHOLD:
        return "weakening"
    return "stable"


def _key_drivers(alerts: list[dict], state: str) -> list[dict]:
    """
    Top 3 articles most relevant to the current state:
    - Escalation  → highest magnitude bullish
    - De-escalation → highest magnitude bearish
    - Stable       → highest magnitude regardless of direction
    """
    if state in ("escalation", "strong_escalation"):
        pool = [a for a in alerts if a.get("direction") == "bullish"]
    elif state in ("de_escalation", "strong_de_escalation"):
        pool = [a for a in alerts if a.get("direction") == "bearish"]
    else:
        pool = list(alerts)
    pool.sort(key=lambda a: a.get("magnitude") or 0, reverse=True)
    return pool[:3]


# ── Public API ─────────────────────────────────────────────────────────────

def evaluate_narrative(db_path: str, tier1_domains: set) -> dict:
    """
    Recompute the narrative state from the rolling 48-hour alert window.
    Persists the result to DB. Returns a result dict:
    {
        state_id        : int,
        state           : str,
        previous_state  : str | None,
        is_transition   : bool,
        weighted_score  : float,
        momentum        : str,
        bull_count      : int,
        bear_count      : int,
        neutral_count   : int,
        avg_bull_mag    : float | None,
        avg_bear_mag    : float | None,
        key_drivers     : list[dict],   # up to 3 alert dicts
        alerts_48h      : list[dict],
    }
    Returns {"state": "stable", "is_transition": False, "alerts_48h": []}
    if there are no alerts in the window yet.
    """
    conn = get_connection(db_path)
    try:
        alerts_48h = _fetch_alerts(conn, within_hours=48)

        if not alerts_48h:
            return {"state": "stable", "is_transition": False, "alerts_48h": []}

        # Full-window score → state
        current_score = _weighted_score(alerts_48h, tier1_domains)
        current_state = _derive_state(current_score)

        # Momentum: previous 12-24h window vs current 0-12h window
        alerts_prev12 = _fetch_alerts_between(conn, from_hours=24, to_hours=12)
        alerts_curr12 = _fetch_alerts(conn, within_hours=12)
        prev_score    = _weighted_score(alerts_prev12, tier1_domains)
        curr_score12  = _weighted_score(alerts_curr12, tier1_domains)
        momentum = _compute_momentum(curr_score12, prev_score)

        # Directional counts and magnitudes
        bulls    = [a for a in alerts_48h if a.get("direction") == "bullish"]
        bears    = [a for a in alerts_48h if a.get("direction") == "bearish"]
        neutrals = [a for a in alerts_48h if a.get("direction") == "neutral"]

        avg_bull_mag = (
            sum(a.get("magnitude") or 0 for a in bulls) / len(bulls)
            if bulls else None
        )
        avg_bear_mag = (
            sum(a.get("magnitude") or 0 for a in bears) / len(bears)
            if bears else None
        )

        drivers    = _key_drivers(alerts_48h, current_state)
        driver_ids = json.dumps([a["id"] for a in drivers])

        # Previous state
        prev_row       = get_latest_narrative_state(conn)
        previous_state = prev_row["state"] if prev_row else None
        is_transition  = previous_state is not None and current_state != previous_state

        with transaction(conn):
            state_id = insert_narrative_state(
                conn,
                state=current_state,
                previous_state=previous_state,
                weighted_score=current_score,
                momentum=momentum,
                bull_count=len(bulls),
                bear_count=len(bears),
                neutral_count=len(neutrals),
                avg_bull_mag=avg_bull_mag,
                avg_bear_mag=avg_bear_mag,
                key_driver_ids=driver_ids,
            )

        logger.info(
            "Narrative: %s (score=%.2f, momentum=%s, transition=%s, prev=%s)",
            current_state, current_score, momentum, is_transition, previous_state,
        )

        return {
            "state_id":       state_id,
            "state":          current_state,
            "previous_state": previous_state,
            "is_transition":  is_transition,
            "weighted_score": current_score,
            "momentum":       momentum,
            "bull_count":     len(bulls),
            "bear_count":     len(bears),
            "neutral_count":  len(neutrals),
            "avg_bull_mag":   avg_bull_mag,
            "avg_bear_mag":   avg_bear_mag,
            "key_drivers":    drivers,
            "alerts_48h":     alerts_48h,
        }
    finally:
        conn.close()
