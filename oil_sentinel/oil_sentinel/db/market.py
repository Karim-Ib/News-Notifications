"""Market data table CRUD."""

import sqlite3
from datetime import datetime, timezone
from typing import Optional


def insert_market_sample(
    conn: sqlite3.Connection,
    *,
    ticker: str,
    price: float,
    change_pct: Optional[float] = None,
    zscore: Optional[float] = None,
    is_anomaly: bool = False,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO market_data (ticker, price, change_pct, zscore, is_anomaly)
        VALUES (?, ?, ?, ?, ?)
        """,
        (ticker, price, change_pct, zscore, int(is_anomaly)),
    )
    return cursor.lastrowid


def get_recent_prices(
    conn: sqlite3.Connection, ticker: str, limit: int = 288
) -> list[float]:
    rows = conn.execute(
        """
        SELECT price FROM market_data
        WHERE ticker = ?
        ORDER BY sampled_at DESC
        LIMIT ?
        """,
        (ticker, limit),
    ).fetchall()
    return [r["price"] for r in reversed(rows)]


def get_price_history(
    conn: sqlite3.Connection,
    ticker: str,
    hours: int = 24,
) -> list[tuple[datetime, float]]:
    """Return (utc_datetime, price) pairs for the last N hours, oldest first."""
    rows = conn.execute(
        """
        SELECT sampled_at, price FROM market_data
        WHERE ticker = ?
          AND sampled_at >= datetime('now', ? || ' hours')
        ORDER BY sampled_at ASC
        """,
        (ticker, f"-{hours}"),
    ).fetchall()
    result = []
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["sampled_at"]).replace(tzinfo=timezone.utc)
            result.append((ts, float(r["price"])))
        except (ValueError, TypeError):
            pass
    return result


def latest_market_sample(
    conn: sqlite3.Connection, ticker: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM market_data WHERE ticker = ? ORDER BY sampled_at DESC LIMIT 1",
        (ticker,),
    ).fetchone()
