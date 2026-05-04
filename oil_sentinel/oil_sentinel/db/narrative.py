"""Narrative states table CRUD."""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def insert_narrative_state(
    conn: sqlite3.Connection,
    *,
    state: str,
    previous_state: Optional[str],
    weighted_score: float,
    momentum: str,
    bull_count: int,
    bear_count: int,
    neutral_count: int,
    avg_bull_mag: Optional[float],
    avg_bear_mag: Optional[float],
    key_driver_ids: str,  # JSON array string
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO narrative_states
            (state, previous_state, weighted_score, momentum,
             bull_count, bear_count, neutral_count,
             avg_bull_mag, avg_bear_mag, key_driver_ids)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            state, previous_state, weighted_score, momentum,
            bull_count, bear_count, neutral_count,
            avg_bull_mag, avg_bear_mag, key_driver_ids,
        ),
    )
    return cursor.lastrowid


def get_narrative_history(
    conn: sqlite3.Connection,
    hours: int = 168,
) -> list[tuple[datetime, float, str]]:
    """Return (utc_datetime, weighted_score, state) tuples for the last N hours, oldest first."""
    rows = conn.execute(
        """
        SELECT computed_at, weighted_score, state FROM narrative_states
        WHERE computed_at >= datetime('now', ? || ' hours')
        ORDER BY computed_at ASC
        """,
        (f"-{hours}",),
    ).fetchall()
    result = []
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["computed_at"]).replace(tzinfo=timezone.utc)
            result.append((ts, float(r["weighted_score"]), str(r["state"])))
        except (ValueError, TypeError):
            pass
    return result


def get_latest_narrative_state(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM narrative_states ORDER BY computed_at DESC LIMIT 1"
    ).fetchone()


def mark_narrative_transition_alerted(conn: sqlite3.Connection, state_id: int) -> None:
    conn.execute(
        "UPDATE narrative_states SET transition_alerted = 1 WHERE id = ?",
        (state_id,),
    )


def prune_old_narrative_states(conn: sqlite3.Connection, keep_days: int = 30) -> int:
    """Delete narrative_states rows older than keep_days. Returns count deleted."""
    cursor = conn.execute(
        "DELETE FROM narrative_states WHERE computed_at < datetime('now', ? || ' days')",
        (f"-{keep_days}",),
    )
    return cursor.rowcount
