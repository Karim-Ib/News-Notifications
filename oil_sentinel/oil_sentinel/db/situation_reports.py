"""Situation reports table CRUD."""

import sqlite3
from typing import Optional


def get_current_sitrep(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """Return the latest situation report row (highest version), or None."""
    return conn.execute(
        "SELECT * FROM situation_reports ORDER BY version DESC LIMIT 1"
    ).fetchone()


def insert_sitrep_row(
    conn: sqlite3.Connection,
    *,
    content: str,
    previous_id: Optional[int] = None,
    compacted_from: Optional[int] = None,
) -> int:
    """Insert a new situation report version. Returns new row id."""
    token_estimate = len(content) // 4
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS v FROM situation_reports"
    ).fetchone()
    version = (row["v"] or 0) + 1
    cursor = conn.execute(
        """
        INSERT INTO situation_reports
            (content, token_estimate, version, previous_id, compacted_from)
        VALUES (?, ?, ?, ?, ?)
        """,
        (content, token_estimate, version, previous_id, compacted_from),
    )
    return cursor.lastrowid
