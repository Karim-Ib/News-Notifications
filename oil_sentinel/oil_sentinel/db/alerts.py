"""Alerts table CRUD."""

import sqlite3
from typing import Optional


def insert_alert(
    conn: sqlite3.Connection,
    *,
    narrative_key: str,
    article_id: Optional[int] = None,
    event_type: Optional[str] = None,
    direction: Optional[str] = None,
    magnitude: Optional[int] = None,
    confidence: Optional[float] = None,
    market_anomaly: bool = False,
    composite_score: Optional[float] = None,
    summary: Optional[str] = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO alerts
            (article_id, narrative_key, event_type, direction,
             magnitude, confidence, market_anomaly, composite_score, summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article_id, narrative_key, event_type, direction,
            magnitude, confidence, int(market_anomaly),
            composite_score, summary,
        ),
    )
    return cursor.lastrowid


def mark_alert_sent(
    conn: sqlite3.Connection, alert_id: int, telegram_msg_id: Optional[int] = None
) -> None:
    conn.execute(
        "UPDATE alerts SET sent_at = datetime('now'), telegram_msg_id = ? WHERE id = ?",
        (telegram_msg_id, alert_id),
    )


def get_recent_narrative_keys(
    conn: sqlite3.Connection, within_hours: int = 12
) -> list[str]:
    """Return distinct narrative_keys created within the last N hours, most recent first."""
    rows = conn.execute(
        """
        SELECT DISTINCT narrative_key FROM alerts
        WHERE created_at >= datetime('now', ? || ' hours')
        ORDER BY created_at DESC
        """,
        (f"-{within_hours}",),
    ).fetchall()
    return [r["narrative_key"] for r in rows]


def _narrative_jaccard(a: str, b: str) -> float:
    """Word-overlap similarity between two snake_case narrative keys (0.0–1.0)."""
    wa = set(a.split("_"))
    wb = set(b.split("_"))
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def _latest_direction_for_narrative(
    conn: sqlite3.Connection, narrative_key: str
) -> Optional[str]:
    """Return the direction of the most recent alert for this narrative key."""
    row = conn.execute(
        """
        SELECT direction FROM alerts
        WHERE narrative_key = ?
        ORDER BY created_at DESC LIMIT 1
        """,
        (narrative_key,),
    ).fetchone()
    return row["direction"] if row else None


def narrative_exists_recent(
    conn: sqlite3.Connection,
    narrative_key: str,
    within_hours: int = 12,
    similarity_threshold: float = 0.75,
    incoming_direction: Optional[str] = None,
) -> Optional[str]:
    """
    Return the matching existing narrative_key if one already exists within the window,
    either as an exact match or with Jaccard word-overlap >= similarity_threshold.
    Returns None if no match found.

    Direction bypass: if incoming_direction differs from the matched narrative's most
    recent direction (e.g. bearish article on a bullish narrative thread), returns None
    to let the alert through — a signal reversal on the same topic is always newsworthy.
    Neutral articles are never bypassed (no directional signal to compare).
    """
    recent = get_recent_narrative_keys(conn, within_hours=within_hours)
    if not recent:
        return None

    # Find matching key — exact first, then fuzzy
    matched_key = None
    if narrative_key in recent:
        matched_key = narrative_key
    else:
        for existing in recent:
            if _narrative_jaccard(narrative_key, existing) >= similarity_threshold:
                matched_key = existing
                break

    if matched_key is None:
        return None

    # Direction bypass: opposing signal on the same narrative thread → let it through
    if incoming_direction and incoming_direction != "neutral":
        existing_direction = _latest_direction_for_narrative(conn, matched_key)
        if existing_direction and existing_direction != incoming_direction:
            return None  # direction flip — not a duplicate

    return matched_key


def last_sent_for_narrative(
    conn: sqlite3.Connection, narrative_key: str
) -> Optional[str]:
    """Return ISO-8601 sent_at of the most recent sent alert for this narrative."""
    row = conn.execute(
        """
        SELECT sent_at FROM alerts
        WHERE narrative_key = ? AND sent_at IS NOT NULL
        ORDER BY sent_at DESC LIMIT 1
        """,
        (narrative_key,),
    ).fetchone()
    return row["sent_at"] if row else None


def get_recently_sent_alerts(
    conn: sqlite3.Connection, hours: int = 24
) -> list[sqlite3.Row]:
    """Return alerts dispatched within the last N hours, oldest first."""
    return conn.execute(
        """
        SELECT direction, sent_at FROM alerts
        WHERE sent_at IS NOT NULL
          AND sent_at >= datetime('now', ? || ' hours')
        ORDER BY sent_at ASC
        """,
        (f"-{hours}",),
    ).fetchall()


def get_unsent_alerts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT al.*,
               ar.published_at  AS article_published_at,
               ar.source_name   AS article_source,
               ar.url           AS article_url
        FROM alerts al
        LEFT JOIN articles ar ON al.article_id = ar.id
        WHERE al.sent_at IS NULL
        ORDER BY al.created_at ASC
        """
    ).fetchall()
