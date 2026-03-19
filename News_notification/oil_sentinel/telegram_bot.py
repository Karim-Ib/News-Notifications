"""
Format and send oil-market alerts via Telegram Bot API.
Features:
  - Severity levels mapped from composite score
  - Per-narrative cooldown/dedup to avoid alert fatigue
  - Market anomaly badge when news + price spike coincide
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp

from db import (
    get_connection,
    get_unsent_alerts,
    last_sent_for_narrative,
    mark_alert_sent,
    transaction,
)

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

# Severity thresholds (composite score)
SEVERITY = [
    (4.5, "🔴 CRITICAL"),
    (3.0, "🟠 HIGH"),
    (1.5, "🟡 MEDIUM"),
    (0.0, "⚪ LOW"),
]

DIRECTION_EMOJI = {
    "bullish": "📈",
    "bearish": "📉",
    "neutral": "➡️",
}

EVENT_LABELS = {
    "sanctions":          "Sanctions",
    "tanker_seizure":     "Tanker Seizure",
    "supply_disruption":  "Supply Disruption",
    "diplomatic":         "Diplomatic",
    "military_action":    "Military Action",
    "rhetoric":           "Rhetoric",
}


def _severity_label(composite: float) -> str:
    for threshold, label in SEVERITY:
        if composite >= threshold:
            return label
    return "⚪ LOW"


def _h(text: str) -> str:
    """Escape text for Telegram HTML mode: only <, >, & need escaping."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


SEP = "\u2500" * 22  # ──────────────────────


def _format_alert_entry(alert: dict) -> str:
    """Format a single alert as one section inside a batch message."""
    composite = alert.get("composite_score") or 0.0
    severity = _severity_label(composite)
    direction = alert.get("direction") or "neutral"
    d_emoji = DIRECTION_EMOJI.get(direction, "->")
    event_raw = alert.get("event_type") or "unknown"
    event_label = EVENT_LABELS.get(event_raw, event_raw.replace("_", " ").title())
    magnitude = alert.get("magnitude") or 0
    confidence = alert.get("confidence") or 0.0
    summary_raw = alert.get("summary") or "(no summary)"
    narrative = alert.get("narrative_key") or ""
    market_flag = " \U0001f6a8" if alert.get("market_anomaly") else ""

    mag_bar = "\u2588" * magnitude + "\u2591" * (5 - magnitude)

    # summary field stores "headline\n\ndetail" since scorer concatenates them
    parts = summary_raw.split("\n\n", 1)
    headline = parts[0].strip()
    detail = parts[1].strip() if len(parts) > 1 else ""

    lines = [
        f"{severity}{market_flag}  {d_emoji} <b>{_h(headline)}</b>",
        f"<code>{mag_bar}</code> {magnitude}/5  \u00b7  {_h(direction.upper())}  \u00b7  {_h(event_label)}  \u00b7  {confidence:.0%} conf  \u00b7  score {composite:.1f}",
    ]
    if detail:
        lines.append(f"\n{_h(detail)}")
    lines.append(f"<i>thread: {_h(narrative)}</i>")
    return "\n".join(lines)


def _format_batch(alerts: list[dict]) -> str:
    """Build one combined HTML message for all qualifying alerts in a sweep."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    any_mkt = any(a.get("market_anomaly") for a in alerts)
    anomaly_tag = "  \U0001f6a8 <b>market spike active</b>" if any_mkt else ""

    header = f"\U0001f6e2 <b>OIL SENTINEL</b>  \u00b7  {len(alerts)} signal{'s' if len(alerts) != 1 else ''}  \u00b7  <i>{now_str}</i>{anomaly_tag}"

    entries = [_format_alert_entry(a) for a in alerts]
    return header + "\n" + SEP + "\n\n" + ("\n\n" + SEP + "\n\n").join(entries)


def _format_market_alert(poll_results: dict) -> str:
    """Format a standalone market-anomaly notification."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"\U0001f6a8 <b>MARKET ANOMALY  \u00b7  OIL SENTINEL</b>  \u00b7  <i>{now_str}</i>",
        SEP,
    ]
    for ticker, data in poll_results.items():
        if not data.get("is_anomaly"):
            continue
        price = data.get("price", 0.0)
        chg = data.get("change_pct") or 0.0
        z = data.get("zscore") or 0.0
        arrow = "\U0001f4c8" if chg >= 0 else "\U0001f4c9"
        label = "WTI" if "CL" in ticker else "Brent" if "BZ" in ticker else ticker
        lines.append(
            f"{arrow} <b>{label}</b>  ${price:.2f}  ({chg:+.2f}%)  z-score: <b>{z:+.2f}</b>"
        )
    lines.append(f"\n<i>Anomaly threshold breached. Watch for follow-on news signals.</i>")
    return "\n".join(lines)


async def send_market_alert(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    poll_results: dict,
) -> Optional[int]:
    """Send a standalone market-anomaly alert. Returns message_id or None."""
    text = _format_market_alert(poll_results)
    msg_id = await send_message(session, bot_token, chat_id, text)
    if msg_id:
        tickers = [t for t, d in poll_results.items() if d.get("is_anomaly")]
        logger.info("Market anomaly alert sent for %s -> msg_id=%d", tickers, msg_id)
    else:
        logger.warning("Market anomaly alert FAILED to send")
    return msg_id


async def send_message(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    text: str,
) -> Optional[int]:
    """
    Post a message to Telegram. Returns the message_id on success, None on failure.
    """
    url = TELEGRAM_API.format(token=bot_token)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("ok"):
                return data["result"]["message_id"]
            logger.error("Telegram error: %s", data.get("description"))
            return None
    except aiohttp.ClientError as exc:
        logger.error("Telegram request failed: %s", exc)
        return None


async def dispatch_alerts(
    db_path: str,
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str,
    alert_threshold: float = 3.0,
    cooldown_minutes: int = 60,
) -> int:
    """
    Collect all unsent qualifying alerts, send them as ONE batched message.
    Returns 1 if a message was sent, 0 otherwise.
    """
    conn = get_connection(db_path)

    try:
        unsent = get_unsent_alerts(conn)
        if not unsent:
            return 0

        now = datetime.now(timezone.utc)
        qualifying = []

        for row in unsent:
            alert = dict(row)
            composite = alert.get("composite_score") or 0.0

            if composite < alert_threshold:
                logger.debug(
                    "Alert %d skipped (composite %.2f < threshold %.2f)",
                    alert["id"], composite, alert_threshold,
                )
                continue

            narrative = alert.get("narrative_key") or "unknown"
            last_sent_str = last_sent_for_narrative(conn, narrative)
            if last_sent_str:
                try:
                    last_sent = datetime.fromisoformat(last_sent_str).replace(tzinfo=timezone.utc)
                    if now - last_sent < timedelta(minutes=cooldown_minutes):
                        logger.debug("Alert %d cooldown active for '%s'", alert["id"], narrative)
                        continue
                except ValueError:
                    pass

            qualifying.append(alert)

        if not qualifying:
            return 0

        # Sort highest composite first
        qualifying.sort(key=lambda a: a.get("composite_score") or 0.0, reverse=True)

        text = _format_batch(qualifying)
        msg_id = await send_message(session, bot_token, chat_id, text)

        with transaction(conn):
            for alert in qualifying:
                mark_alert_sent(conn, alert["id"], telegram_msg_id=msg_id)

        if msg_id:
            logger.info(
                "Sent batch of %d alerts (ids=%s) -> msg_id=%d",
                len(qualifying),
                [a["id"] for a in qualifying],
                msg_id,
            )
            return 1
        else:
            logger.warning("Batch send failed for %d alerts", len(qualifying))
            return 0
    finally:
        conn.close()
