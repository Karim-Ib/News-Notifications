"""
Async orchestrator for oil_sentinel.

Three independent polling loops:
  - news_loop    : GDELT poll every 15 min
  - market_loop  : yfinance poll every 5 min (sync via thread executor)
  - scoring_loop : Gemini scoring + Telegram dispatch every 2 min

Usage:
    cp config.ini.example config.ini   # fill in your keys
    python main.py
"""

import asyncio
import configparser
import logging
import logging.handlers
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp

import market as market_mod
from db import init_db
from gdelt import poll_and_store as gdelt_poll
from scorer import make_gemini_client, score_pending_articles
from telegram_bot import dispatch_alerts, send_market_alert

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config.ini"


def load_config() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if not CONFIG_PATH.exists():
        print(
            f"ERROR: {CONFIG_PATH} not found.\n"
            "Copy config.ini.example -> config.ini and fill in your keys.",
            file=sys.stderr,
        )
        sys.exit(1)
    cfg.read(CONFIG_PATH)
    return cfg


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(level: str, log_file: str) -> None:
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    if log_file:
        fh = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

class State:
    market_anomaly: bool = False
    last_market_alert: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Polling loops
# ---------------------------------------------------------------------------

async def news_loop(cfg: configparser.ConfigParser, session: aiohttp.ClientSession) -> None:
    """Poll GDELT every N minutes."""
    interval = cfg.getint("gdelt", "poll_interval_minutes", fallback=15) * 60
    db_path = cfg.get("database", "path", fallback="oil_sentinel.db")
    tone_threshold = cfg.getfloat("gdelt", "tone_threshold", fallback=2.5)
    min_relevance = cfg.getint("gdelt", "min_relevance", fallback=60)
    logger = logging.getLogger("news_loop")

    logger.info("News loop started (interval=%ds)", interval)
    while True:
        try:
            n = await gdelt_poll(
                db_path,
                session,
                tone_threshold=tone_threshold,
                min_relevance=min_relevance,
                timespan=f"{cfg.getint('gdelt', 'poll_interval_minutes', fallback=15)}m",
            )
            logger.info("GDELT poll done: %d new articles", n)
        except Exception as exc:
            logger.exception("News loop error: %s", exc)
        await asyncio.sleep(interval)


async def market_loop(
    cfg: configparser.ConfigParser,
    session: aiohttp.ClientSession,
    state: State,
) -> None:
    """Poll yfinance every N minutes (blocking call -> thread pool)."""
    interval = cfg.getint("market", "poll_interval_minutes", fallback=5) * 60
    db_path = cfg.get("database", "path", fallback="oil_sentinel.db")
    tickers = [
        cfg.get("market", "wti_ticker", fallback="CL=F"),
        cfg.get("market", "brent_ticker", fallback="BZ=F"),
    ]
    zscore_window = cfg.getint("market", "zscore_window", fallback=288)
    zscore_threshold = cfg.getfloat("market", "zscore_threshold", fallback=2.0)
    bot_token = cfg.get("telegram", "bot_token", fallback="")
    chat_id = cfg.get("telegram", "chat_id", fallback="")
    cooldown = cfg.getint("telegram", "cooldown_minutes", fallback=60)
    logger = logging.getLogger("market_loop")

    logger.info("Market loop started (interval=%ds)", interval)
    loop = asyncio.get_running_loop()
    while True:
        try:
            results = await loop.run_in_executor(
                None,
                lambda: market_mod.poll_and_store(
                    db_path,
                    tickers=tickers,
                    zscore_window=zscore_window,
                    zscore_threshold=zscore_threshold,
                ),
            )
            state.market_anomaly = market_mod.any_anomaly(results)

            if state.market_anomaly:
                logger.warning("Market anomaly detected: %s", results)
                now = datetime.now(timezone.utc)
                if (
                    state.last_market_alert is None
                    or now - state.last_market_alert > timedelta(minutes=cooldown)
                ):
                    if bot_token and bot_token != "YOUR_TELEGRAM_BOT_TOKEN_HERE":
                        await send_market_alert(session, bot_token, chat_id, results)
                        state.last_market_alert = now
                        logger.info("Market anomaly alert sent")
        except Exception as exc:
            logger.exception("Market loop error: %s", exc)
        await asyncio.sleep(interval)


async def scoring_loop(
    cfg: configparser.ConfigParser,
    session: aiohttp.ClientSession,
    state: State,
) -> None:
    """Score pending articles and dispatch alerts every 2 minutes."""
    interval = 120
    db_path = cfg.get("database", "path", fallback="oil_sentinel.db")
    api_key = cfg.get("gemini", "api_key", fallback="")
    model_name = cfg.get("gemini", "model", fallback="gemini-2.0-flash")
    batch_size = cfg.getint("gemini", "batch_size", fallback=5)
    bot_token = cfg.get("telegram", "bot_token", fallback="")
    chat_id = cfg.get("telegram", "chat_id", fallback="")
    alert_threshold = cfg.getfloat("telegram", "alert_threshold", fallback=3.0)
    cooldown = cfg.getint("telegram", "cooldown_minutes", fallback=60)
    logger = logging.getLogger("scoring_loop")

    if not api_key or api_key == "YOUR_GEMINI_API_KEY_HERE":
        logger.error("Gemini API key not configured -- scoring loop disabled")
        return
    if not bot_token or bot_token == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
        logger.error("Telegram bot token not configured -- alerts disabled")
        return

    client = make_gemini_client(api_key)
    logger.info("Scoring loop started (interval=%ds)", interval)

    while True:
        try:
            n = await score_pending_articles(
                db_path,
                client,
                model=model_name,
                batch_size=batch_size,
                market_anomaly=state.market_anomaly,
            )
            if n:
                logger.info("Scored %d articles -> dispatching alerts", n)

            sent = await dispatch_alerts(
                db_path,
                session,
                bot_token=bot_token,
                chat_id=chat_id,
                alert_threshold=alert_threshold,
                cooldown_minutes=cooldown,
            )
            if sent:
                logger.info("Sent %d Telegram alerts", sent)
        except Exception as exc:
            logger.exception("Scoring loop error: %s", exc)
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    cfg = load_config()

    setup_logging(
        cfg.get("logging", "level", fallback="INFO"),
        cfg.get("logging", "file", fallback="oil_sentinel.log"),
    )
    logger = logging.getLogger("main")

    db_path = cfg.get("database", "path", fallback="oil_sentinel.db")
    init_db(db_path)
    logger.info("Database initialised at %s", db_path)

    state = State()

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info("oil_sentinel starting -- three loops launching")
        await asyncio.gather(
            news_loop(cfg, session),
            market_loop(cfg, session, state),
            scoring_loop(cfg, session, state),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested -- goodbye.")
