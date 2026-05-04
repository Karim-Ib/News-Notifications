"""Portfolios, transactions, and portfolio snapshots CRUD."""

import sqlite3
from typing import Optional


def insert_portfolio(
    conn: sqlite3.Connection,
    *,
    name: str,
    ticker: str,
    product: str,
    currency: str = "EUR",
) -> Optional[int]:
    """Insert a new portfolio. Returns new id, or None if name already exists."""
    try:
        cursor = conn.execute(
            """
            INSERT INTO portfolios (name, ticker, product, currency)
            VALUES (?, ?, ?, ?)
            """,
            (name, ticker, product, currency),
        )
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return None


def get_portfolio_by_name(
    conn: sqlite3.Connection, name: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM portfolios WHERE name = ?", (name,)
    ).fetchone()


def get_portfolio_by_id(
    conn: sqlite3.Connection, portfolio_id: int
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM portfolios WHERE id = ?", (portfolio_id,)
    ).fetchone()


def get_active_portfolios(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM portfolios WHERE active = 1 ORDER BY created_at ASC"
    ).fetchall()


def deactivate_portfolio(conn: sqlite3.Connection, portfolio_id: int) -> None:
    conn.execute(
        "UPDATE portfolios SET active = 0 WHERE id = ?", (portfolio_id,)
    )


def insert_transaction(
    conn: sqlite3.Connection,
    *,
    portfolio_id: int,
    action: str,
    amount_eur: float,
    price_per_unit: float,
    units: float,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO transactions (portfolio_id, action, amount_eur, price_per_unit, units)
        VALUES (?, ?, ?, ?, ?)
        """,
        (portfolio_id, action, amount_eur, price_per_unit, units),
    )
    return cursor.lastrowid


def get_transactions(
    conn: sqlite3.Connection, portfolio_id: int
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM transactions WHERE portfolio_id = ? ORDER BY timestamp ASC",
        (portfolio_id,),
    ).fetchall()


def insert_portfolio_snapshot(
    conn: sqlite3.Connection,
    *,
    portfolio_id: int,
    unit_price: float,
    total_units: float,
    total_value: float,
    total_invested: float,
    pnl_eur: float,
    pnl_pct: float,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO portfolio_snapshots
            (portfolio_id, unit_price, total_units, total_value, total_invested, pnl_eur, pnl_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (portfolio_id, unit_price, total_units, total_value, total_invested, pnl_eur, pnl_pct),
    )
    return cursor.lastrowid


def get_portfolio_snapshots(
    conn: sqlite3.Connection,
    portfolio_id: int,
    hours: Optional[int] = None,
) -> list[sqlite3.Row]:
    if hours is not None:
        return conn.execute(
            """
            SELECT * FROM portfolio_snapshots
            WHERE portfolio_id = ?
              AND timestamp >= datetime('now', ? || ' hours')
            ORDER BY timestamp ASC
            """,
            (portfolio_id, f"-{hours}"),
        ).fetchall()
    return conn.execute(
        "SELECT * FROM portfolio_snapshots WHERE portfolio_id = ? ORDER BY timestamp ASC",
        (portfolio_id,),
    ).fetchall()


def get_last_portfolio_snapshot(
    conn: sqlite3.Connection, portfolio_id: int
) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM portfolio_snapshots
        WHERE portfolio_id = ?
        ORDER BY timestamp DESC LIMIT 1
        """,
        (portfolio_id,),
    ).fetchone()
