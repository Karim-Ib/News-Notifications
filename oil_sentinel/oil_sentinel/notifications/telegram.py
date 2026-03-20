"""
Format and send oil-market alerts via Telegram Bot API.
Features:
  - Severity levels mapped from magnitude (0-10 scale)
  - Immediate dispatch for magnitude >= alert_threshold (default 7)
  - Twice-daily digest for sub-threshold alerts
  - Per-narrative cooldown/dedup to avoid alert fatigue
  - Market anomaly badge when news + price spike coincide
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp

from oil_sentinel.charts import generate_price_chart
from oil_sentinel.db import (
    get_active_watches_for_ticker,
    get_connection,
    get_price_history,
    get_unsent_alerts,
    last_sent_for_narrative,
    latest_market_sample,
    mark_alert_sent,
    mark_narrative_transition_alerted,
    transaction,
    trigger_watch,
)
from oil_sentinel.narrative import STATE_EMOJI, STATE_LABELS

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_MAX_LEN = 4096

# Severity thresholds based on magnitude (0-10 scale)
SEVERITY = [
    (9, "🔴 CRITICAL"),
    (7, "🟠 HIGH"),
    (4, "🟡 MEDIUM"),
    (1, "🟢 LOW"),
    (0, "⚪ NOISE"),
]

DIRECTION_EMOJI = {
    "bullish": "📈",
    "bearish": "📉",
    "neutral": "➡️",
}

EVENT_LABELS = {
    "sanctions":            "Sanctions",
    "tanker_seizure":       "Tanker Seizure",
    "supply_disruption":    "Supply Disruption",
    "diplomatic":           "Diplomatic",
    "military_action":      "Military Action",
    "infrastructure_attack": "Infrastructure Attack",
    "blockade_threat":      "Blockade Threat",
    "opec_cut":             "OPEC Cut",
    "sanctions_relief":     "Sanctions Relief",
    "nuclear_deal":         "Nuclear Deal",
    "ceasefire":            "Ceasefire",
    "diplomatic_progress":  "Diplomatic Progress",
    "supply_increase":      "Supply Increase",
    "export_waiver":        "Export Waiver",
    "demand_destruction":   "Demand Destruction",
    "spr_release":          "SPR Release",
    "rhetoric":             "Rhetoric",
    "opec_meeting":         "OPEC Meeting",
    "analysis":             "Analysis",
}


def _severity_label(magnitude: int) -> str:
    for threshold, label in SEVERITY:
        if magnitude >= threshold:
            return label
    return "⚪ NOISE"


def _h(text: str) -> str:
    """Escape text for Telegram HTML mode."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


SEP = "\u2500" * 24


def _parse_published(raw: str) -> str:
    if not raw:
        return "unknown date"
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw.strip(), fmt).replace(tzinfo=timezone.utc)
            return f"{dt.day} {dt.strftime('%b')} {dt.year} {dt.strftime('%H:%M')} UTC"
        except ValueError:
            continue
    return raw[:16]


def _mag_bar(magnitude: int) -> str:
    """10-char block bar for 0-10 scale."""
    filled = max(0, min(10, magnitude))
    return "\u2588" * filled + "\u2591" * (10 - filled)


def _format_alert_entry(alert: dict) -> str:
    """Format a single alert as one section inside a batch message."""
    magnitude = alert.get("magnitude") or 0
    severity = _severity_label(magnitude)
    direction = alert.get("direction") or "neutral"
    d_emoji = DIRECTION_EMOJI.get(direction, "->")
    event_raw = alert.get("event_type") or "unknown"
    event_label = EVENT_LABELS.get(event_raw, event_raw.replace("_", " ").title())
    confidence = alert.get("confidence") or 0.0
    composite = alert.get("composite_score") or 0.0
    summary_raw = alert.get("summary") or "(no summary)"
    narrative = alert.get("narrative_key") or ""
    market_flag = " \U0001f6a8" if alert.get("market_anomaly") else ""
    pub_date = _parse_published(alert.get("article_published_at") or "")
    source = alert.get("article_source") or ""

    bar = _mag_bar(magnitude)

    parts = summary_raw.split("\n\n", 1)
    headline = parts[0].strip()
    detail = parts[1].strip() if len(parts) > 1 else ""

    meta_parts = []
    if source:
        meta_parts.append(_h(source))
    meta_parts.append(_h(pub_date))
    meta_line = "  \u00b7  ".join(meta_parts)

    lines = [
        f"{severity}{market_flag}  {d_emoji} <b>{_h(headline)}</b>",
        f"<code>{bar}</code> {magnitude}/10  \u00b7  {_h(direction.upper())}  \u00b7  {_h(event_label)}  \u00b7  {confidence:.0%} conf  \u00b7  score {composite:.1f}",
        f"<i>{meta_line}</i>",
    ]
    if detail:
        lines.append(f"\n{_h(detail)}")
    lines.append(f"<i>thread: {_h(narrative)}</i>")
    return "\n".join(lines)


def _format_digest_entry(alert: dict) -> str:
    """Compact one-liner format for digest messages."""
    magnitude = alert.get("magnitude") or 0
    direction = alert.get("direction") or "neutral"
    d_emoji = DIRECTION_EMOJI.get(direction, "->")
    event_raw = alert.get("event_type") or "unknown"
    event_label = EVENT_LABELS.get(event_raw, event_raw.replace("_", " ").title())
    confidence = alert.get("confidence") or 0.0
    summary_raw = alert.get("summary") or "(no summary)"
    source = alert.get("article_source") or ""

    headline = summary_raw.split("\n\n", 1)[0].strip()
    source_str = f" <i>{_h(source)}</i>" if source else ""

    return (
        f"{d_emoji} <b>[{magnitude}/10]</b> {_h(headline)}{source_str}\n"
        f"   <i>{_h(event_label)}  \u00b7  {confidence:.0%} conf</i>"
    )


def _pack_messages(header: str, entries: list[str], continuation_header: str) -> list[str]:
    """
    Greedily pack formatted entries into <=TELEGRAM_MAX_LEN messages.
    Splits at entry boundaries so no entry is ever truncated.
    Returns a list of ready-to-send message strings.
    """
    messages = []
    current_header = header
    current_entries: list[str] = []

    def _build(h: str, es: list[str]) -> str:
        return h + "\n" + SEP + "\n\n" + ("\n\n" + SEP + "\n\n").join(es)

    for entry in entries:
        candidate = _build(current_header, current_entries + [entry])
        if len(candidate) <= TELEGRAM_MAX_LEN:
            current_entries.append(entry)
        else:
            if current_entries:
                messages.append(_build(current_header, current_entries))
            current_header = continuation_header
            current_entries = [entry]

    if current_entries:
        messages.append(_build(current_header, current_entries))

    return messages


def _batch_messages(alerts: list[dict], narrative_state: Optional[dict] = None) -> list[str]:
    """Build one or more HTML messages for immediate alerts, split to fit Telegram."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    any_mkt = any(a.get("market_anomaly") for a in alerts)
    anomaly_tag = "  \U0001f6a8 <b>market spike active</b>" if any_mkt else ""

    narrative_tag = ""
    if narrative_state and narrative_state.get("state"):
        state = narrative_state["state"]
        emoji = STATE_EMOJI.get(state, "")
        label = STATE_LABELS.get(state, state.upper())
        momentum = narrative_state.get("momentum", "")
        momentum_icon = {"strengthening": "↑", "weakening": "↓", "stable": "→"}.get(momentum, "")
        narrative_tag = f"\n\U0001f4ca Narrative: {emoji} <b>{label}</b>  {momentum_icon} {momentum}"

    header = (
        f"\U0001f6e2 <b>OIL SENTINEL</b>  \u00b7  "
        f"{len(alerts)} signal{'s' if len(alerts) != 1 else ''}  \u00b7  "
        f"<i>{now_str}</i>{anomaly_tag}{narrative_tag}"
    )
    continuation = f"\U0001f6e2 <b>OIL SENTINEL</b>  \u00b7  <i>{now_str}</i>  \u00b7  (continued)"
    entries = [_format_alert_entry(a) for a in alerts]
    return _pack_messages(header, entries, continuation)


def _format_narrative_transition(narrative: dict, wti_price: Optional[float]) -> str:
    """Format a narrative state transition alert."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    prev  = narrative.get("previous_state") or "unknown"
    curr  = narrative["state"]
    prev_label = f"{STATE_EMOJI.get(prev, '')} {STATE_LABELS.get(prev, prev.upper())}"
    curr_label = f"{STATE_EMOJI.get(curr, '')} {STATE_LABELS.get(curr, curr.upper())}"

    momentum = narrative.get("momentum", "stable")
    momentum_icon = {"strengthening": "↑ strengthening", "weakening": "↓ weakening", "stable": "→ stable"}.get(momentum, momentum)

    bull_count = narrative.get("bull_count", 0)
    bear_count = narrative.get("bear_count", 0)
    neutral_count = narrative.get("neutral_count", 0)
    avg_bull = narrative.get("avg_bull_mag")
    avg_bear = narrative.get("avg_bear_mag")
    score = narrative.get("weighted_score", 0.0)

    bull_line  = f"{bull_count} bullish" + (f"  avg mag {avg_bull:.1f}" if avg_bull else "")
    bear_line  = f"{bear_count} bearish" + (f"  avg mag {avg_bear:.1f}" if avg_bear else "")

    price_line = f"\U0001f4b0 WTI: <b>${wti_price:.2f}</b>" if wti_price else ""

    drivers = narrative.get("key_drivers") or []
    driver_lines = []
    for i, d in enumerate(drivers[:3], 1):
        mag = d.get("magnitude") or 0
        direction = d.get("direction") or "neutral"
        d_emoji = DIRECTION_EMOJI.get(direction, "")
        summary = (d.get("summary") or "").split("\n\n")[0][:80]
        source = d.get("source_name") or ""
        source_str = f"  <i>{_h(source)}</i>" if source else ""
        driver_lines.append(
            f"  {i}. {d_emoji} <b>[{mag}/10]</b> {_h(summary)}{source_str}"
        )

    parts = [
        f"\U0001f504 <b>NARRATIVE SHIFT  \u00b7  OIL SENTINEL</b>  \u00b7  <i>{now_str}</i>",
        SEP,
        f"{prev_label}  \u2192  {curr_label}",
        "",
        f"\U0001f4ca 48h window:  {bull_line}  \u00b7  {bear_line}  \u00b7  {neutral_count} neutral",
        f"   Weighted sentiment score: <b>{score:+.2f}</b>",
        f"\U0001f4c8 Momentum: <b>{momentum_icon}</b>",
    ]
    if price_line:
        parts.append(price_line)
    if driver_lines:
        parts.append("")
        parts.append("\U0001f511 <b>Key Drivers</b>")
        parts.extend(driver_lines)

    return "\n".join(parts)


def _digest_messages(alerts: list[dict], slot_label: str) -> list[str]:
    """Build one or more HTML digest messages, split to fit Telegram."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Group by direction, sorted highest magnitude first
    groups: dict[str, list[dict]] = {"bullish": [], "bearish": [], "neutral": []}
    for a in alerts:
        groups.setdefault(a.get("direction") or "neutral", []).append(a)
    for g in groups.values():
        g.sort(key=lambda a: a.get("magnitude") or 0, reverse=True)

    # Build flat entry list with section headers interleaved
    entries: list[str] = []
    for direction, emoji, label in [
        ("bullish", "📈", "BULLISH SIGNALS"),
        ("bearish", "📉", "BEARISH SIGNALS"),
        ("neutral", "➡️", "NEUTRAL / CONTEXT"),
    ]:
        items = groups.get(direction, [])
        if not items:
            continue
        section_lines = [f"{emoji} <b>{label}</b>"]
        section_lines += [_format_digest_entry(a) for a in items]
        entries.append("\n".join(section_lines))

    header = (
        f"\U0001f4f0 <b>OIL SENTINEL — {slot_label} Digest</b>  \u00b7  "
        f"<i>{now_str}</i>\n"
        f"{len(alerts)} background signal{'s' if len(alerts) != 1 else ''} since last digest"
    )
    continuation = (
        f"\U0001f4f0 <b>OIL SENTINEL — {slot_label} Digest</b>  \u00b7  "
        f"<i>{now_str}</i>  \u00b7  (continued)"
    )
    return _pack_messages(header, entries, continuation)


def _format_morning_summary(alerts: list[dict], top_n: int = 7) -> str:
    """Concise overnight summary — top N signals by magnitude."""
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = len(alerts)
    top = alerts[:top_n]

    header = (
        f"\u2600\ufe0f <b>OIL SENTINEL \u2014 Overnight Summary</b>  \u00b7  <i>{now_str}</i>\n"
        f"{total} signal{'s' if total != 1 else ''} overnight"
        + (f"  \u00b7  showing top {top_n}" if total > top_n else "")
    )

    entries = []
    for a in top:
        magnitude = a.get("magnitude") or 0
        direction = a.get("direction") or "neutral"
        d_emoji = DIRECTION_EMOJI.get(direction, "->")
        event_raw = a.get("event_type") or ""
        event_label = EVENT_LABELS.get(event_raw, event_raw.replace("_", " ").title())
        summary_raw = a.get("summary") or "(no summary)"
        headline = summary_raw.split("\n\n", 1)[0].strip()
        source = a.get("article_source") or ""
        source_str = f"  <i>{_h(source)}</i>" if source else ""

        entries.append(
            f"{d_emoji} <b>[{magnitude}/10]</b> {_h(headline)}{source_str}\n"
            f"   <i>{_h(event_label)}</i>"
        )

    return header + "\n" + SEP + "\n\n" + "\n\n".join(entries)


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


async def send_message(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    text: str,
) -> Optional[int]:
    """Post a message to Telegram. Returns the message_id on success, None on failure."""
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


async def send_photo(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    photo_bytes: bytes,
    caption: str = "",
) -> Optional[int]:
    """Upload a PNG to Telegram. Returns message_id or None on failure."""
    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    form = aiohttp.FormData()
    form.add_field("chat_id", str(chat_id))
    form.add_field(
        "photo", photo_bytes,
        filename="wti_chart.png",
        content_type="image/png",
    )
    if caption:
        form.add_field("caption", caption[:1024])
    try:
        async with session.post(url, data=form, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            data = await resp.json()
            if data.get("ok"):
                return data["result"]["message_id"]
            logger.error("Telegram sendPhoto error: %s", data.get("description"))
            return None
    except aiohttp.ClientError as exc:
        logger.error("Telegram sendPhoto failed: %s", exc)
        return None


async def _send_price_chart(
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    alert_markers: Optional[list[tuple[datetime, str]]] = None,
    caption: str = "",
) -> None:
    """
    Fetch 24h WTI price history, generate chart, send to Telegram.
    Silently skips if there is insufficient data or if chart generation fails.
    """
    conn = get_connection(db_path)
    try:
        prices = get_price_history(conn, "CL=F", hours=24)
    finally:
        conn.close()

    if len(prices) < 3:
        logger.debug("Price chart skipped: only %d samples available", len(prices))
        return

    chart_bytes = generate_price_chart(prices, alert_markers=alert_markers)
    if not chart_bytes:
        logger.debug("Price chart generation returned None")
        return

    await send_photo(session, bot_token, chat_id, chart_bytes, caption=caption)


async def send_narrative_transition_alert(
    db_path: str,
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str,
    narrative: dict,
    wti_price: Optional[float] = None,
) -> bool:
    """
    Send a narrative state-transition alert. Bypasses all cooldowns.
    Marks the state record as alerted in DB on success.
    Returns True on success.
    """
    # Determine dominant direction of the new narrative state for the marker color
    new_state = narrative.get("state", "")
    if "escalation" in new_state and "de_" not in new_state:
        chart_direction = "bullish"
    elif "de_escalation" in new_state:
        chart_direction = "bearish"
    else:
        chart_direction = "neutral"

    now_utc = datetime.now(timezone.utc)
    caption = f"📊 WTI 24h  •  Narrative shift: {STATE_LABELS.get(narrative.get('previous_state',''), '?')} → {STATE_LABELS.get(new_state, new_state)}"
    await _send_price_chart(
        db_path, session, bot_token, chat_id,
        alert_markers=[(now_utc, chart_direction)],
        caption=caption,
    )

    text = _format_narrative_transition(narrative, wti_price)
    msg_id = await send_message(session, bot_token, chat_id, text)
    if msg_id:
        conn = get_connection(db_path)
        try:
            with transaction(conn):
                mark_narrative_transition_alerted(conn, narrative["state_id"])
        finally:
            conn.close()
        logger.info(
            "Narrative transition alert sent: %s → %s  msg_id=%d",
            narrative.get("previous_state"), narrative["state"], msg_id,
        )
        return True
    logger.warning(
        "Narrative transition alert FAILED: %s → %s",
        narrative.get("previous_state"), narrative["state"],
    )
    return False


async def send_market_alert(
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    poll_results: dict,
) -> Optional[int]:
    """Send a standalone market-anomaly alert with price chart. Returns message_id or None."""
    now_utc = datetime.now(timezone.utc)

    # Build a caption showing which tickers spiked
    anomaly_tickers = []
    for ticker, data in poll_results.items():
        if data.get("is_anomaly"):
            label = "WTI" if "CL" in ticker else "Brent" if "BZ" in ticker else ticker
            chg = data.get("change_pct") or 0.0
            anomaly_tickers.append(f"{label} {chg:+.2f}%")
    caption = "🚨 Market anomaly: " + "  •  ".join(anomaly_tickers) if anomaly_tickers else "🚨 Market anomaly detected"

    await _send_price_chart(
        db_path, session, bot_token, chat_id,
        alert_markers=[(now_utc, "neutral")],
        caption=caption,
    )

    text = _format_market_alert(poll_results)
    msg_id = await send_message(session, bot_token, chat_id, text)
    if msg_id:
        tickers = [t for t, d in poll_results.items() if d.get("is_anomaly")]
        logger.info("Market anomaly alert sent for %s -> msg_id=%d", tickers, msg_id)
    else:
        logger.warning("Market anomaly alert FAILED to send")
    return msg_id


async def dispatch_alerts(
    db_path: str,
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str,
    alert_threshold: int = 7,
    cooldown_minutes: int = 60,
    narrative_state: Optional[dict] = None,
) -> int:
    """
    Send immediate alerts for signals with magnitude >= alert_threshold.
    Sub-threshold signals are left unsent for the digest.
    Returns 1 if a batch was sent, 0 otherwise.
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
            magnitude = alert.get("magnitude") or 0

            # Sub-threshold → leave for digest
            if magnitude < alert_threshold:
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

        qualifying.sort(key=lambda a: a.get("magnitude") or 0, reverse=True)

        # Build chart markers from alert creation timestamps
        chart_markers = []
        for alert in qualifying:
            direction = alert.get("direction") or "neutral"
            raw_ts = alert.get("created_at") or ""
            try:
                ts = datetime.fromisoformat(raw_ts).replace(tzinfo=timezone.utc)
                chart_markers.append((ts, direction))
            except (ValueError, TypeError):
                chart_markers.append((now, direction))

        wti_price_str = ""
        conn2 = get_connection(db_path)
        try:
            row = latest_market_sample(conn2, "CL=F")
            if row:
                wti_price_str = f"  •  WTI ${float(row['price']):.2f}"
        finally:
            conn2.close()

        caption = f"📊 WTI 24h{wti_price_str}  •  {len(qualifying)} alert{'s' if len(qualifying) != 1 else ''} firing"
        await _send_price_chart(
            db_path, session, bot_token, chat_id,
            alert_markers=chart_markers,
            caption=caption,
        )

        messages = _batch_messages(qualifying, narrative_state=narrative_state)
        last_msg_id = None
        failed = False
        for text in messages:
            msg_id = await send_message(session, bot_token, chat_id, text)
            if msg_id:
                last_msg_id = msg_id
            else:
                failed = True

        if not failed:
            with transaction(conn):
                for alert in qualifying:
                    mark_alert_sent(conn, alert["id"], telegram_msg_id=last_msg_id)
            logger.info(
                "Sent immediate batch: %d alerts in %d message(s) (ids=%s) -> last msg_id=%s",
                len(qualifying), len(messages), [a["id"] for a in qualifying], last_msg_id,
            )
            return 1
        else:
            logger.warning("Immediate batch send failed for %d alerts", len(qualifying))
            return 0
    finally:
        conn.close()


async def dispatch_morning_summary(
    db_path: str,
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str,
    top_n: int = 7,
) -> int:
    """
    Send a concise summary of all alerts accumulated overnight.
    Marks every unsent alert as sent regardless of magnitude threshold —
    the overnight window replaces normal dispatch entirely.
    Returns 1 if sent, 0 if nothing to send.
    """
    conn = get_connection(db_path)
    try:
        unsent = get_unsent_alerts(conn)
        if not unsent:
            logger.info("Morning summary: no overnight alerts to summarise")
            return 0

        alerts = sorted(
            [dict(r) for r in unsent],
            key=lambda a: a.get("magnitude") or 0,
            reverse=True,
        )

        text = _format_morning_summary(alerts, top_n=top_n)
        msg_id = await send_message(session, bot_token, chat_id, text)

        if msg_id:
            with transaction(conn):
                for alert in alerts:
                    mark_alert_sent(conn, alert["id"], telegram_msg_id=msg_id)
            logger.info(
                "Morning summary: sent %d overnight alerts -> msg_id=%d",
                len(alerts), msg_id,
            )
            return 1
        else:
            logger.warning("Morning summary send failed")
            return 0
    finally:
        conn.close()


async def dispatch_digest(
    db_path: str,
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str,
    alert_threshold: int = 7,
    slot_label: str = "Noon",
) -> int:
    """
    Send a digest of all unsent sub-threshold signals.
    Returns 1 if sent, 0 if nothing to send.
    """
    conn = get_connection(db_path)
    try:
        unsent = get_unsent_alerts(conn)
        sub_threshold = [
            dict(row) for row in unsent
            if (row["magnitude"] or 0) < alert_threshold
        ]

        if not sub_threshold:
            logger.info("Digest (%s): no sub-threshold alerts to summarise", slot_label)
            return 0

        sub_threshold.sort(key=lambda a: a.get("magnitude") or 0, reverse=True)
        messages = _digest_messages(sub_threshold, slot_label)
        last_msg_id = None
        failed = False
        for text in messages:
            msg_id = await send_message(session, bot_token, chat_id, text)
            if msg_id:
                last_msg_id = msg_id
            else:
                failed = True

        if not failed:
            with transaction(conn):
                for alert in sub_threshold:
                    mark_alert_sent(conn, alert["id"], telegram_msg_id=last_msg_id)
            logger.info(
                "Digest (%s): sent %d alerts in %d message(s) -> last msg_id=%s",
                slot_label, len(sub_threshold), len(messages), last_msg_id,
            )
            return 1
        else:
            logger.warning("Digest (%s) send failed", slot_label)
            return 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Price watch alert
# ---------------------------------------------------------------------------

# Shared ticker → human label mapping (also used by commands.py)
TICKER_LABELS: dict[str, str] = {
    "CL=F": "WTI",
    "BZ=F": "Brent",
}


def _format_watch_alert(watch: dict, current_price: float) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    ticker_label = TICKER_LABELS.get(watch["ticker"], watch["ticker"])
    direction = watch["direction"]
    target = watch["target_price"]
    label = watch.get("label") or ""
    label_str = f"  —  {_h(label)}" if label else ""

    return (
        f"\U0001f3af <b>PRICE WATCH TRIGGERED</b>  \u00b7  <i>{now_str}</i>\n"
        f"{SEP}\n"
        f"<b>{_h(ticker_label)}</b> {_h(direction)} <b>${target:.2f}</b>{label_str}\n"
        f"Current: <b>${current_price:.2f}</b>"
    )


async def check_price_watches(
    db_path: str,
    poll_results: dict,
    session: aiohttp.ClientSession,
    *,
    bot_token: str,
    chat_id: str,
) -> int:
    """
    Check active price watches against fresh poll results.
    Sends an alert and deactivates each watch that has been triggered.
    Returns the number of watches fired.
    """
    if not poll_results:
        return 0

    conn = get_connection(db_path)
    fired = 0
    try:
        for ticker, data in poll_results.items():
            current_price = data.get("price")
            if current_price is None:
                continue

            watches = get_active_watches_for_ticker(conn, ticker)
            for watch in watches:
                watch = dict(watch)
                target = watch["target_price"]
                direction = watch["direction"]

                triggered = (
                    (direction == "above" and current_price >= target) or
                    (direction == "below" and current_price <= target)
                )
                if not triggered:
                    continue

                text = _format_watch_alert(watch, current_price)
                msg_id = await send_message(session, bot_token, chat_id, text)
                if msg_id:
                    with transaction(conn):
                        trigger_watch(conn, watch["id"])
                    ticker_label = TICKER_LABELS.get(ticker, ticker)
                    logger.info(
                        "Price watch #%d triggered: %s %s $%.2f (current $%.2f)",
                        watch["id"], ticker_label, direction, target, current_price,
                    )
                    fired += 1
                else:
                    logger.warning(
                        "Price watch #%d alert failed to send — will retry next poll",
                        watch["id"],
                    )
    finally:
        conn.close()

    return fired
