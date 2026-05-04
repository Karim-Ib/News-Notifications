"""Daily accuracy scores table CRUD."""

import sqlite3
from typing import Optional


def insert_daily_score(
    conn: sqlite3.Connection,
    *,
    date: str,
    narrative_state: Optional[str],
    weighted_score: Optional[float],
    net_direction: Optional[str],
    bull_count: int,
    bear_count: int,
    neutral_count: int,
    avg_magnitude: Optional[float],
    wti_open: Optional[float],
    wti_close: Optional[float],
    wti_change_pct: Optional[float],
    prediction_correct: Optional[int],
    skip_reason: Optional[str],
) -> bool:
    """Insert a daily score row. Returns True if inserted, False if date already exists."""
    try:
        conn.execute(
            """
            INSERT INTO daily_scores
                (date, narrative_state, weighted_score, net_direction,
                 bull_count, bear_count, neutral_count, avg_magnitude,
                 wti_open, wti_close, wti_change_pct,
                 prediction_correct, skip_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                date, narrative_state, weighted_score, net_direction,
                bull_count, bear_count, neutral_count, avg_magnitude,
                wti_open, wti_close, wti_change_pct,
                prediction_correct, skip_reason,
            ),
        )
        return True
    except sqlite3.IntegrityError:
        return False  # date already evaluated


def daily_score_exists(conn: sqlite3.Connection, date: str) -> bool:
    """Return True if a daily_scores row already exists for the given date."""
    row = conn.execute(
        "SELECT 1 FROM daily_scores WHERE date = ?", (date,)
    ).fetchone()
    return row is not None


def get_daily_scores(
    conn: sqlite3.Connection,
    days: Optional[int] = 30,
) -> list[sqlite3.Row]:
    """Return daily_scores rows newest-first. days=None returns all rows."""
    if days is not None:
        return conn.execute(
            """
            SELECT * FROM daily_scores
            WHERE date >= date('now', ? || ' days')
            ORDER BY date DESC
            """,
            (f"-{days}",),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM daily_scores ORDER BY date DESC"
    ).fetchall()
