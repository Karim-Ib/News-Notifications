"""Articles table CRUD."""

import hashlib
import sqlite3
from typing import Optional


def url_hash(url: str) -> str:
    return hashlib.sha256(url.strip().encode()).hexdigest()


def title_hash_exists(conn: sqlite3.Connection, title_hash: str, within_hours: int = 24) -> bool:
    """Return True if a same-title article was stored within the last N hours."""
    row = conn.execute(
        """
        SELECT 1 FROM articles
        WHERE title_hash = ?
          AND fetched_at >= datetime('now', ? || ' hours')
        LIMIT 1
        """,
        (title_hash, f"-{within_hours}"),
    ).fetchone()
    return row is not None


def article_exists(conn: sqlite3.Connection, url: str) -> bool:
    h = url_hash(url)
    row = conn.execute(
        "SELECT 1 FROM articles WHERE url_hash = ?", (h,)
    ).fetchone()
    return row is not None


def insert_article(
    conn: sqlite3.Connection,
    *,
    url: str,
    title: Optional[str] = None,
    title_hash: Optional[str] = None,
    source_name: Optional[str] = None,
    published_at: Optional[str] = None,
    gdelt_tone: Optional[float] = None,
    gdelt_themes: Optional[str] = None,   # JSON string
    actors: Optional[str] = None,          # JSON string
    raw_json: Optional[str] = None,
) -> Optional[int]:
    """Insert article; returns new row id, or None if URL already exists."""
    h = url_hash(url)
    try:
        cursor = conn.execute(
            """
            INSERT INTO articles
                (url_hash, url, title, title_hash, source_name, published_at,
                 gdelt_tone, gdelt_themes, actors, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (h, url, title, title_hash, source_name, published_at,
             gdelt_tone, gdelt_themes, actors, raw_json),
        )
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return None  # duplicate


def update_article_body(conn: sqlite3.Connection, article_id: int, body_text: str) -> None:
    """Store extracted body text for an article."""
    conn.execute(
        "UPDATE articles SET body_text = ? WHERE id = ?", (body_text, article_id)
    )


def get_unscored_articles(
    conn: sqlite3.Connection, limit: int = 10
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM articles WHERE scored = 0 ORDER BY fetched_at ASC LIMIT ?",
        (limit,),
    ).fetchall()


def mark_article_scored(
    conn: sqlite3.Connection, article_id: int, skipped: bool = False
) -> None:
    status = 2 if skipped else 1
    conn.execute(
        "UPDATE articles SET scored = ? WHERE id = ?", (status, article_id)
    )
