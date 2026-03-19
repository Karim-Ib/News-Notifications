"""
SQLite schema and CRUD for oil_sentinel.
Tables: articles, market_data, alerts
"""

import hashlib
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Connection / context
# ---------------------------------------------------------------------------

def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    url_hash      TEXT    NOT NULL UNIQUE,          -- SHA-256 of canonical URL
    url           TEXT    NOT NULL,
    title         TEXT,
    title_hash    TEXT,                             -- hash of normalised first-8-word title
    source_name   TEXT,
    published_at  TEXT,                             -- ISO-8601 / GDELT seendate
    fetched_at    TEXT    NOT NULL DEFAULT (datetime('now')),
    gdelt_tone    REAL,                             -- GDELT tone score (negative = negative)
    gdelt_themes  TEXT,                             -- JSON array of theme codes
    actors        TEXT,                             -- JSON array of matched actors
    raw_json      TEXT,                             -- full GDELT record as JSON
    scored        INTEGER NOT NULL DEFAULT 0        -- 0=pending, 1=scored, 2=skipped
);

CREATE INDEX IF NOT EXISTS idx_articles_fetched     ON articles(fetched_at);
CREATE INDEX IF NOT EXISTS idx_articles_scored      ON articles(scored);
CREATE INDEX IF NOT EXISTS idx_articles_title_hash  ON articles(title_hash);

CREATE TABLE IF NOT EXISTS market_data (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT    NOT NULL,
    price        REAL    NOT NULL,
    change_pct   REAL,                              -- % change from previous sample
    zscore       REAL,                              -- rolling z-score at capture time
    sampled_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    is_anomaly   INTEGER NOT NULL DEFAULT 0         -- 1 if |zscore| >= threshold
);

CREATE INDEX IF NOT EXISTS idx_market_ticker_time ON market_data(ticker, sampled_at);

CREATE TABLE IF NOT EXISTS alerts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id       INTEGER REFERENCES articles(id),
    narrative_key    TEXT    NOT NULL,              -- dedup / cooldown key
    event_type       TEXT,
    direction        TEXT    CHECK(direction IN ('bullish','bearish','neutral')),
    magnitude        INTEGER CHECK(magnitude BETWEEN 0 AND 10),
    confidence       REAL    CHECK(confidence BETWEEN 0.0 AND 1.0),
    market_anomaly   INTEGER NOT NULL DEFAULT 0,    -- 1 if coincident market spike
    composite_score  REAL,
    summary          TEXT,
    sent_at          TEXT,                          -- NULL until actually sent
    telegram_msg_id  INTEGER,
    created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_alerts_narrative  ON alerts(narrative_key, created_at);
CREATE INDEX IF NOT EXISTS idx_alerts_sent       ON alerts(sent_at);
"""


def init_db(db_path: str) -> None:
    """Create all tables and apply any pending column migrations."""
    conn = get_connection(db_path)
    with transaction(conn):
        conn.executescript(SCHEMA)

        # Migration: add title_hash column to articles if absent
        article_cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)")}
        if "title_hash" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN title_hash TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_articles_title_hash ON articles(title_hash)"
            )

        # Migration: widen magnitude constraint from 1-5 to 0-10.
        # SQLite cannot ALTER CHECK constraints, so recreate the alerts table.
        alert_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='alerts'"
        ).fetchone()
        if alert_sql and "BETWEEN 1 AND 5" in (alert_sql[0] or ""):
            conn.executescript("""
                ALTER TABLE alerts RENAME TO alerts_old;

                CREATE TABLE alerts (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    article_id       INTEGER REFERENCES articles(id),
                    narrative_key    TEXT    NOT NULL,
                    event_type       TEXT,
                    direction        TEXT    CHECK(direction IN ('bullish','bearish','neutral')),
                    magnitude        INTEGER CHECK(magnitude BETWEEN 0 AND 10),
                    confidence       REAL    CHECK(confidence BETWEEN 0.0 AND 1.0),
                    market_anomaly   INTEGER NOT NULL DEFAULT 0,
                    composite_score  REAL,
                    summary          TEXT,
                    sent_at          TEXT,
                    telegram_msg_id  INTEGER,
                    created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
                );

                INSERT INTO alerts SELECT * FROM alerts_old;
                DROP TABLE alerts_old;

                CREATE INDEX IF NOT EXISTS idx_alerts_narrative ON alerts(narrative_key, created_at);
                CREATE INDEX IF NOT EXISTS idx_alerts_sent      ON alerts(sent_at);
            """)
    conn.close()


# ---------------------------------------------------------------------------
# Articles CRUD
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Market data CRUD
# ---------------------------------------------------------------------------

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


def latest_market_sample(
    conn: sqlite3.Connection, ticker: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM market_data WHERE ticker = ? ORDER BY sampled_at DESC LIMIT 1",
        (ticker,),
    ).fetchone()


# ---------------------------------------------------------------------------
# Alerts CRUD
# ---------------------------------------------------------------------------

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


def get_unsent_alerts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT al.*,
               ar.published_at  AS article_published_at,
               ar.source_name   AS article_source
        FROM alerts al
        LEFT JOIN articles ar ON al.article_id = ar.id
        WHERE al.sent_at IS NULL
        ORDER BY al.created_at ASC
        """
    ).fetchall()
