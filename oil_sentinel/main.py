"""
oil_sentinel — entry point.

Three independent async polling loops:
  news_loop    : GDELT poll every 15 min
  market_loop  : yfinance poll every 5 min (sync via thread executor)
  scoring_loop : Gemini scoring + Telegram dispatch every 2 min

Usage:
    cp config.ini.example config.ini   # fill in your keys
    python main.py
"""

import asyncio
import logging
import logging.handlers
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp

from oil_sentinel.config import Config, load as load_config
from oil_sentinel.db import get_connection, init_db, latest_market_sample
from oil_sentinel.ingestion import poll_and_store as gdelt_poll
from oil_sentinel.market import poll_and_store as market_poll, any_anomaly
from oil_sentinel.narrative import evaluate_narrative
from oil_sentinel.notifications import (
    dispatch_alerts,
    dispatch_digest,
    dispatch_morning_summary,
    send_market_alert,
    send_narrative_transition_alert,
)
from oil_sentinel.scoring import make_gemini_client, score_pending_articles

CONFIG_PATH = Path(__file__).parent / "config.ini"


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
# Idle mode helpers
# ---------------------------------------------------------------------------

def _is_overnight(cfg: Config, now: datetime) -> bool:
    """Return True if the current UTC hour falls in the configured overnight window."""
    if not cfg.idle.enabled:
        return False
    h = now.hour
    s, e = cfg.idle.overnight_start, cfg.idle.overnight_end
    if s > e:  # window crosses midnight, e.g. 22 → 09
        return h >= s or h < e
    return s <= h < e


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

class State:
    def __init__(self) -> None:
        self.market_anomaly: bool = False
        self.last_market_alert: Optional[datetime] = None
        self.sent_digests: set = set()  # (date, hour) tuples already dispatched
        self.narrative: dict = {}       # latest narrative evaluation result


# ---------------------------------------------------------------------------
# Polling loops
# ---------------------------------------------------------------------------

async def news_loop(cfg: Config, session: aiohttp.ClientSession) -> None:
    """Poll GDELT every N minutes (slower during overnight idle window)."""
    logger = logging.getLogger("news_loop")
    logger.info(
        "News loop started (interval=%dm, idle=%s, overnight=%s)",
        cfg.gdelt.poll_interval_minutes,
        cfg.idle.enabled,
        f"{cfg.idle.overnight_start:02d}:00-{cfg.idle.overnight_end:02d}:00 UTC" if cfg.idle.enabled else "n/a",
    )
    while True:
        now = datetime.now(timezone.utc)
        overnight = _is_overnight(cfg, now)
        interval_min = cfg.idle.poll_interval_minutes if overnight else cfg.gdelt.poll_interval_minutes
        interval = interval_min * 60

        try:
            n = await gdelt_poll(
                cfg.db_path,
                session,
                tone_threshold=cfg.gdelt.tone_threshold,
                unknown_tone_threshold=cfg.gdelt.unknown_source_tone_threshold,
                min_relevance=cfg.gdelt.min_relevance,
                timespan=f"{interval_min}m",
                max_age_hours=cfg.gdelt.max_article_age_hours,
                tier1_domains=cfg.gdelt.tier1_domains,
            )
            logger.info("GDELT poll done: %d new articles%s", n, " [idle]" if overnight else "")
        except Exception as exc:
            logger.exception("News loop error: %s", exc)
        await asyncio.sleep(interval)


async def market_loop(cfg: Config, session: aiohttp.ClientSession, state: State) -> None:
    """Poll yfinance every N minutes (blocking call -> thread pool)."""
    interval = cfg.market.poll_interval_minutes * 60
    logger = logging.getLogger("market_loop")
    logger.info("Market loop started (interval=%ds)", interval)
    loop = asyncio.get_running_loop()
    while True:
        if _is_overnight(cfg, datetime.now(timezone.utc)):
            state.market_anomaly = False
            await asyncio.sleep(60)  # wake up every minute to re-check if overnight ended
            continue

        try:
            results = await loop.run_in_executor(
                None,
                lambda: market_poll(
                    cfg.db_path,
                    tickers=cfg.market.tickers,
                    zscore_window=cfg.market.zscore_window,
                    zscore_threshold=cfg.market.zscore_threshold,
                ),
            )
            state.market_anomaly = any_anomaly(results)

            if state.market_anomaly:
                logger.warning("Market anomaly detected: %s", results)
                now = datetime.now(timezone.utc)
                if (
                    state.last_market_alert is None
                    or now - state.last_market_alert > timedelta(minutes=cfg.telegram.cooldown_minutes)
                ):
                    if cfg.telegram.bot_token and cfg.telegram.bot_token != "YOUR_TELEGRAM_BOT_TOKEN_HERE":
                        await send_market_alert(session, cfg.telegram.bot_token, cfg.telegram.chat_id, results)
                        state.last_market_alert = now
                        logger.info("Market anomaly alert sent")
        except Exception as exc:
            logger.exception("Market loop error: %s", exc)
        await asyncio.sleep(interval)


def _latest_wti_price(db_path: str) -> Optional[float]:
    conn = get_connection(db_path)
    try:
        row = latest_market_sample(conn, "CL=F")
        return float(row["price"]) if row else None
    finally:
        conn.close()


async def scoring_loop(cfg: Config, session: aiohttp.ClientSession, state: State) -> None:
    """Score pending articles, evaluate narrative trend, and dispatch alerts every 2 minutes."""
    interval = 120
    logger = logging.getLogger("scoring_loop")

    if not cfg.gemini.api_key or cfg.gemini.api_key == "YOUR_GEMINI_API_KEY_HERE":
        logger.error("Gemini API key not configured -- scoring loop disabled")
        return
    if not cfg.telegram.bot_token or cfg.telegram.bot_token == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
        logger.error("Telegram bot token not configured -- alerts disabled")
        return

    client = make_gemini_client(cfg.gemini.api_key)
    loop = asyncio.get_running_loop()
    logger.info("Scoring loop started (interval=%ds)", interval)

    while True:
        try:
            n = await score_pending_articles(
                cfg.db_path,
                client,
                model=cfg.gemini.model,
                batch_size=cfg.gemini.batch_size,
                market_anomaly=state.market_anomaly,
            )
            if n:
                logger.info("Scored %d articles", n)

            # Evaluate narrative every cycle — state can change as the 48h window rolls forward
            narrative = await loop.run_in_executor(
                None,
                lambda: evaluate_narrative(cfg.db_path, cfg.gdelt.tier1_domains),
            )
            state.narrative = narrative

            overnight = _is_overnight(cfg, datetime.now(timezone.utc))

            # Narrative transition: highest-priority signal, bypasses all cooldowns
            # Only send during active hours (not overnight) to avoid noise
            if (
                narrative.get("is_transition")
                and narrative.get("state_id")
                and not overnight
            ):
                wti_price = await loop.run_in_executor(None, lambda: _latest_wti_price(cfg.db_path))
                await send_narrative_transition_alert(
                    cfg.db_path,
                    session,
                    bot_token=cfg.telegram.bot_token,
                    chat_id=cfg.telegram.chat_id,
                    narrative=narrative,
                    wti_price=wti_price,
                )

            if overnight:
                logger.debug("Idle overnight — skipping regular alert dispatch")
            else:
                sent = await dispatch_alerts(
                    cfg.db_path,
                    session,
                    bot_token=cfg.telegram.bot_token,
                    chat_id=cfg.telegram.chat_id,
                    alert_threshold=cfg.telegram.alert_threshold,
                    cooldown_minutes=cfg.telegram.cooldown_minutes,
                    narrative_state=narrative if narrative.get("state") else None,
                )
                if sent:
                    logger.info("Sent %d Telegram alert batch(es)", sent)
        except Exception as exc:
            logger.exception("Scoring loop error: %s", exc)
        await asyncio.sleep(interval)


async def digest_loop(cfg: Config, session: aiohttp.ClientSession, state: State) -> None:
    """
    Send sub-threshold digests at configured UTC hours.
    When idle mode is enabled, also sends a morning summary at overnight_end
    covering all signals accumulated during the night.
    """
    logger = logging.getLogger("digest_loop")
    digest_hours = cfg.telegram.digest_hours
    slot_labels = {12: "Noon", 20: "Evening"}
    logger.info(
        "Digest loop started (slots=%s UTC, idle=%s)",
        digest_hours, cfg.idle.enabled,
    )

    while True:
        now = datetime.now(timezone.utc)
        current_date = now.date()
        current_hour = now.hour

        # Morning summary (idle mode only) — fires at overnight_end hour (e.g. 09:00)
        if cfg.idle.enabled:
            morning_hour = cfg.idle.morning_summary_hour
            morning_key = ("morning", current_date, morning_hour)
            if current_hour >= morning_hour and morning_key not in state.sent_digests:
                logger.info("Sending overnight summary (idle mode, %02d:00 UTC)", morning_hour)
                try:
                    sent = await dispatch_morning_summary(
                        cfg.db_path,
                        session,
                        bot_token=cfg.telegram.bot_token,
                        chat_id=cfg.telegram.chat_id,
                    )
                    if sent:
                        logger.info("Morning summary sent")
                    state.sent_digests.add(morning_key)
                except Exception as exc:
                    logger.exception("Morning summary error: %s", exc)

        # Regular daytime digests — skip during overnight window in idle mode
        if not _is_overnight(cfg, now):
            for hour in digest_hours:
                key = ("digest", current_date, hour)
                if current_hour >= hour and key not in state.sent_digests:
                    label = slot_labels.get(hour, f"{hour:02d}:00")
                    logger.info("Sending %s digest (slot %02d:00 UTC)", label, hour)
                    try:
                        sent = await dispatch_digest(
                            cfg.db_path,
                            session,
                            bot_token=cfg.telegram.bot_token,
                            chat_id=cfg.telegram.chat_id,
                            alert_threshold=cfg.telegram.alert_threshold,
                            slot_label=label,
                        )
                        if sent:
                            logger.info("%s digest sent", label)
                        state.sent_digests.add(key)
                    except Exception as exc:
                        logger.exception("Digest loop error: %s", exc)

        await asyncio.sleep(60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    cfg = load_config(CONFIG_PATH)

    setup_logging(cfg.logging.level, cfg.logging.file)
    logger = logging.getLogger("main")

    init_db(cfg.db_path)
    logger.info("Database initialised at %s", cfg.db_path)

    state = State()

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        logger.info("oil_sentinel starting -- three loops launching")
        await asyncio.gather(
            news_loop(cfg, session),
            market_loop(cfg, session, state),
            scoring_loop(cfg, session, state),
            digest_loop(cfg, session, state),
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested -- goodbye.")
