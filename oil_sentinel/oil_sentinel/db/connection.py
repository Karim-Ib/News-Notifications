"""
Database connection, transaction context manager, schema constants, and init_db.
"""

import sqlite3
from contextlib import contextmanager


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

ACCURACY_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_scores (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    date               TEXT    UNIQUE NOT NULL,
    narrative_state    TEXT,
    weighted_score     REAL,
    net_direction      TEXT,
    bull_count         INTEGER NOT NULL DEFAULT 0,
    bear_count         INTEGER NOT NULL DEFAULT 0,
    neutral_count      INTEGER NOT NULL DEFAULT 0,
    avg_magnitude      REAL,
    wti_open           REAL,
    wti_close          REAL,
    wti_change_pct     REAL,
    prediction_correct INTEGER,
    skip_reason        TEXT,
    computed_at        TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_daily_scores_date ON daily_scores(date);
"""

SITREP_SCHEMA = """
CREATE TABLE IF NOT EXISTS situation_reports (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    content          TEXT    NOT NULL,
    token_estimate   INTEGER NOT NULL DEFAULT 0,
    version          INTEGER NOT NULL DEFAULT 1,
    previous_id      INTEGER REFERENCES situation_reports(id),
    created_at       TEXT    NOT NULL DEFAULT (datetime('now')),
    compacted_from   INTEGER             -- NULL normally; set when created by compaction
);

CREATE INDEX IF NOT EXISTS idx_sitrep_version ON situation_reports(version);
"""

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
# Schema initialisation and migrations
# ---------------------------------------------------------------------------

def init_db(db_path: str) -> None:
    """Create all tables and apply any pending column migrations."""
    conn = get_connection(db_path)
    with transaction(conn):
        conn.executescript(SCHEMA)
        conn.executescript(PORTFOLIO_SCHEMA)
        conn.executescript(SITREP_SCHEMA)
        conn.executescript(ACCURACY_SCHEMA)

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
