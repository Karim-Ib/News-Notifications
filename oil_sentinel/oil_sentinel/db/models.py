"""
SQLite schema and CRUD for oil_sentinel.
Tables: articles, market_data, alerts
"""

import hashlib
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
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
CREATE TABLE IF NOT EXISTS narrative_states (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    state            TEXT    NOT NULL,   -- strong_escalation | escalation | stable | de_escalation | strong_de_escalation
    previous_state   TEXT,               -- NULL on first record
    weighted_score   REAL    NOT NULL,   -- signed mean sentiment score
    momentum         TEXT    NOT NULL,   -- strengthening | weakening | stable
    bull_count       INTEGER NOT NULL DEFAULT 0,
    bear_count       INTEGER NOT NULL DEFAULT 0,
    neutral_count    INTEGER NOT NULL DEFAULT 0,
    avg_bull_mag     REAL,
    avg_bear_mag     REAL,
    key_driver_ids   TEXT,               -- JSON array of alert IDs (top 3)
    computed_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    transition_alerted INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_narrative_computed ON narrative_states(computed_at);

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
    body_text     TEXT,                             -- extracted full article text (trafilatura)
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

CREATE TABLE IF NOT EXISTS price_watches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker       TEXT    NOT NULL,
    direction    TEXT    NOT NULL CHECK(direction IN ('above', 'below')),
    target_price REAL    NOT NULL,
    label        TEXT,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    triggered_at TEXT,
    active       INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_watches_active ON price_watches(active, ticker);
"""


def init_db(db_path: str) -> None:
    """Create all tables and apply any pending column migrations."""
    conn = get_connection(db_path)
    with transaction(conn):
        conn.executescript(SCHEMA)
        conn.executescript(PORTFOLIO_SCHEMA)

        # Migration: add title_hash column to articles if absent
        article_cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)")}
        if "title_hash" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN title_hash TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_articles_title_hash ON articles(title_hash)"
            )

        # Migration: add body_text column to articles if absent
        article_cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)")}
        if "body_text" not in article_cols:
            conn.execute("ALTER TABLE articles ADD COLUMN body_text TEXT")

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


# ---------------------------------------------------------------------------
# Narrative state CRUD
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Price watches CRUD
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Portfolio tracking schema (appended via migration in init_db)
# ---------------------------------------------------------------------------

PORTFOLIO_SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolios (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    UNIQUE NOT NULL,
    ticker     TEXT    NOT NULL,
    product    TEXT    NOT NULL CHECK(product IN ('long', 'short')),
    currency   TEXT    NOT NULL DEFAULT 'EUR',
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    active     INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS transactions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id   INTEGER NOT NULL REFERENCES portfolios(id),
    action         TEXT    NOT NULL CHECK(action IN ('buy', 'sell')),
    amount_eur     REAL    NOT NULL,
    price_per_unit REAL    NOT NULL,
    units          REAL    NOT NULL,
    timestamp      TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id   INTEGER NOT NULL REFERENCES portfolios(id),
    timestamp      TEXT    NOT NULL DEFAULT (datetime('now')),
    unit_price     REAL    NOT NULL,
    total_units    REAL    NOT NULL,
    total_value    REAL    NOT NULL,
    total_invested REAL    NOT NULL,
    pnl_eur        REAL    NOT NULL,
    pnl_pct        REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_portfolios_active      ON portfolios(active, name);
CREATE INDEX IF NOT EXISTS idx_transactions_portfolio ON transactions(portfolio_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshots_portfolio    ON portfolio_snapshots(portfolio_id, timestamp);
"""


# ---------------------------------------------------------------------------
# Portfolios CRUD
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Transactions CRUD
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Portfolio snapshots CRUD
# ---------------------------------------------------------------------------

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
