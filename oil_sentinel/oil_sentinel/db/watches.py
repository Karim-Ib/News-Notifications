"""Price watches table CRUD."""

import sqlite3
from typing import Optional


def insert_watch(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    direction: str,
    target_price: float,
    label: Optional[str] = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO price_watches (ticker, direction, target_price, label)
        VALUES (?, ?, ?, ?)
        """,
        (ticker, direction, target_price, label or None),
    )
    return cursor.lastrowid


def get_active_watches(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return all active watches, ordered by creation time."""
    return conn.execute(
        "SELECT * FROM price_watches WHERE active = 1 ORDER BY created_at ASC"
    ).fetchall()


def get_active_watches_for_ticker(
    conn: sqlite3.Connection, ticker: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM price_watches WHERE active = 1 AND ticker = ?",
        (ticker,),
    ).fetchall()


def get_watch_by_id(
    conn: sqlite3.Connection, watch_id: int
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM price_watches WHERE id = ?", (watch_id,)
    ).fetchone()


def trigger_watch(conn: sqlite3.Connection, watch_id: int) -> None:
    """Mark a watch as fired: set triggered_at and deactivate."""
    conn.execute(
        "UPDATE price_watches SET active = 0, triggered_at = datetime('now') WHERE id = ?",
        (watch_id,),
    )


def deactivate_watch(conn: sqlite3.Connection, watch_id: int) -> None:
    """Manually deactivate a watch (user /unwatch) without setting triggered_at."""
    conn.execute(
        "UPDATE price_watches SET active = 0 WHERE id = ?",
        (watch_id,),
    )


def deactivate_all_watches(conn: sqlite3.Connection) -> int:
    """Deactivate all active watches. Returns count removed."""
    cursor = conn.execute("UPDATE price_watches SET active = 0 WHERE active = 1")
    return cursor.rowcount


def update_watch_price(
    conn: sqlite3.Connection, watch_id: int, new_price: float
) -> None:
    conn.execute(
        "UPDATE price_watches SET target_price = ? WHERE id = ?",
        (new_price, watch_id),
    )
