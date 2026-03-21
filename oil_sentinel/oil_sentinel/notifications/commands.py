"""
Telegram bot command handler.

Polls getUpdates (long-polling) for incoming messages and dispatches
recognised slash commands. Only responds to the configured chat_id.

Supported commands
------------------
/chart   — Send the WTI 24h price chart with recent alert markers
/status  — Narrative state, WTI price, anomaly flag, and active mode
/idle    — Show / change idle mode (manual on/off, auto, timezone)
/help    — List available commands
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    _ZONEINFO_AVAILABLE = True
except ImportError:  # pragma: no cover — only on very old Python builds
    _ZONEINFO_AVAILABLE = False
    ZoneInfo = None  # type: ignore

import aiohttp

from oil_sentinel.charts import generate_price_chart, generate_price_narrative_chart
from oil_sentinel.db import (
    deactivate_all_watches,
    deactivate_watch,
    deactivate_portfolio,
    get_active_portfolios,
    get_active_watches,
    get_connection,
    get_narrative_history,
    get_portfolio_by_name,
    get_portfolio_snapshots,
    get_price_history,
    get_recently_sent_alerts,
    get_transactions,
    get_watch_by_id,
    insert_portfolio,
    insert_transaction,
    latest_market_sample,
    transaction,
    update_watch_price,
)
from oil_sentinel.narrative import STATE_EMOJI, STATE_LABELS
from oil_sentinel.notifications.telegram import TICKER_LABELS, send_message, send_photo
from oil_sentinel.portfolio import (
    PRODUCT_NAMES,
    PRODUCT_TICKERS,
    fetch_etp_price,
    get_portfolio_position,
    get_portfolio_stats,
)
from oil_sentinel.portfolio.chart import generate_portfolio_chart

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Command registry — single source of truth for /help and setMyCommands
# ---------------------------------------------------------------------------

# Ticker alias resolution — also used by command handlers
TICKER_ALIASES: dict[str, str] = {
    "wti":  "CL=F",
    "cl":   "CL=F",
    "cl=f": "CL=F",
    "brent": "BZ=F",
    "bz":   "BZ=F",
    "bz=f": "BZ=F",
}

# Add new public commands here. /shutdown_bot and /confirm are intentionally absent.
BOT_COMMANDS: list[tuple[str, str]] = [
    ("chart",      "WTI price+narrative chart. /chart [days 1–30, default 7]"),
    ("status",     "Narrative state, live WTI price & anomaly flag"),
    ("watch",      "Set a price alert  e.g. /watch wti below 85 Entry"),
    ("watches",    "List all active price watches"),
    ("unwatch",    "Remove a watch by ID  e.g. /unwatch 1  or  /unwatch all"),
    ("editwatch",  "Change a watch target  e.g. /editwatch 1 88.50"),
    ("portfolio",  "View/manage portfolios  e.g. /portfolio hormuz-short"),
    ("portfolios", "List all active portfolios"),
    ("buy",        "Record a buy  e.g. /buy hormuz-short 100"),
    ("sell",       "Record a sell  e.g. /sell hormuz-short 100  or  all"),
    ("idle",       "Show or change idle mode and timezone"),
    ("help",       "List all available commands"),
]

_HELP_TEXT = (
    "🛢 <b>Oil Sentinel — Commands</b>\n\n"
    + "\n".join(f"/{cmd}  — {desc}" for cmd, desc in BOT_COMMANDS)
    + "\n\n<b>/portfolio subcommands:</b>\n"
    "  /portfolio create &lt;name&gt; &lt;long|short&gt;\n"
    "  /portfolio &lt;name&gt;            — view state\n"
    "  /portfolio history &lt;name&gt;\n"
    "  /portfolio chart &lt;name&gt; [7d|30d|90d|all]\n"
    "  /portfolio stats &lt;name&gt;\n"
    "  /portfolio delete &lt;name&gt;\n"
    "\n<b>/idle subcommands:</b>\n"
    "  /idle on          — force idle mode\n"
    "  /idle off         — force normal mode\n"
    "  /idle auto        — return to automatic (time-based)\n"
    "  /idle tz &lt;name&gt;   — set timezone  e.g. <code>Europe/Berlin</code>\n"
    "  /idle tz local    — revert to server local time"
)

# All commands that the dispatcher will route (public + hidden)
_ROUTABLE: set[str] = {f"/{cmd}" for cmd, _ in BOT_COMMANDS} | {"/shutdown_bot", "/confirm"}

# ---------------------------------------------------------------------------
# Rate limit for /chart
# ---------------------------------------------------------------------------

CHART_COOLDOWN_SECONDS = 60

# ---------------------------------------------------------------------------
# Telegram update polling
# ---------------------------------------------------------------------------

async def get_updates(
    session: aiohttp.ClientSession,
    bot_token: str,
    offset: int = 0,
    timeout: int = 30,
) -> list[dict]:
    """Long-poll Telegram getUpdates. Blocks up to `timeout` seconds."""
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    params = {
        "offset": offset,
        "timeout": timeout,
        "allowed_updates": ["message"],
    }
    try:
        async with session.get(
            url, params=params,
            timeout=aiohttp.ClientTimeout(total=timeout + 10),
        ) as resp:
            data = await resp.json()
            if data.get("ok"):
                return data.get("result", [])
            logger.warning("getUpdates error: %s", data.get("description"))
            return []
    except aiohttp.ClientError as exc:
        logger.debug("getUpdates connection error: %s", exc)
        return []


async def register_commands(
    session: aiohttp.ClientSession,
    bot_token: str,
) -> None:
    """Register BOT_COMMANDS with Telegram for autocomplete. Safe to call on startup."""
    url = f"https://api.telegram.org/bot{bot_token}/setMyCommands"
    payload = {
        "commands": [{"command": cmd, "description": desc} for cmd, desc in BOT_COMMANDS]
    }
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            if data.get("ok"):
                logger.info("Bot commands registered with Telegram (%d commands)", len(BOT_COMMANDS))
            else:
                logger.warning("setMyCommands failed: %s", data.get("description"))
    except aiohttp.ClientError as exc:
        logger.warning("setMyCommands request failed: %s", exc)


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

async def _cmd_chart(
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    state,
    args: Optional[list[str]] = None,
) -> None:
    """
    Send WTI price/narrative chart. Enforces a per-request cooldown.

    /chart          — 7-day price + narrative sentiment chart (default)
    /chart 1        — 24h intraday price chart
    /chart <days>   — multi-day narrative chart, 2–30 days
    """
    now = datetime.now(timezone.utc)

    # Rate limit
    if state.last_chart_request is not None:
        elapsed = (now - state.last_chart_request).total_seconds()
        if elapsed < CHART_COOLDOWN_SECONDS:
            remaining = int(CHART_COOLDOWN_SECONDS - elapsed)
            await send_message(
                session, bot_token, chat_id,
                f"⏳ Chart cooldown — please wait <b>{remaining}s</b> before requesting another.",
            )
            return

    # Parse optional days argument
    days = 7
    if args:
        try:
            days = max(1, min(30, int(args[0])))
        except (ValueError, TypeError):
            await send_message(
                session, bot_token, chat_id,
                "⚠️ Usage: <code>/chart [days]</code>  where days is 1–30\n"
                "Examples: <code>/chart</code>  (7d default)  ·  <code>/chart 1</code>  (24h intraday)",
            )
            return

    state.last_chart_request = now
    hours = days * 24

    conn = get_connection(db_path)
    try:
        prices     = get_price_history(conn, "CL=F", hours=hours)
        sent       = get_recently_sent_alerts(conn, hours=hours)
        latest     = latest_market_sample(conn, "CL=F")
        narratives = get_narrative_history(conn, hours=hours) if days > 1 else []
    finally:
        conn.close()

    if len(prices) < 3:
        await send_message(
            session, bot_token, chat_id,
            "⚠️ Not enough price history yet — market loop needs a few samples first.",
        )
        return

    markers = []
    for row in sent:
        direction = row["direction"] or "neutral"
        try:
            ts = datetime.fromisoformat(row["sent_at"]).replace(tzinfo=timezone.utc)
            markers.append((ts, direction))
        except (ValueError, TypeError):
            pass

    price_str = f"WTI ${float(latest['price']):.2f}" if latest else "WTI n/a"
    n_markers = len(markers)
    days_str  = f"{days}d" if days > 1 else "24h"

    price_caption = (
        f"📊 {price_str}  •  {days_str}  •  "
        f"{n_markers} alert{'s' if n_markers != 1 else ''}"
    )
    price_bytes = generate_price_chart(
        prices,
        alert_markers=markers or None,
        title=f"WTI Crude  ·  {days_str}",
    )
    if price_bytes:
        await send_photo(session, bot_token, chat_id, price_bytes, caption=price_caption)
    else:
        await send_message(session, bot_token, chat_id, "⚠️ Price chart generation failed.")

    if days > 1:
        narrative_bytes = generate_price_narrative_chart(
            prices, narratives,
            alert_markers=markers or None,
            title=f"Price vs Narrative  ·  {days_str}",
        )
        if narrative_bytes:
            await send_photo(
                session, bot_token, chat_id, narrative_bytes,
                caption=f"📊 Price vs narrative  •  {days_str}",
            )
        else:
            await send_message(session, bot_token, chat_id,
                               "⚠️ Not enough narrative history yet.")


async def _cmd_status(
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    state,
) -> None:
    """Send a concise system status summary."""
    conn = get_connection(db_path)
    try:
        latest = latest_market_sample(conn, "CL=F")
    finally:
        conn.close()

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # WTI price
    if latest:
        price = float(latest["price"])
        chg   = float(latest["change_pct"] or 0)
        z     = float(latest["zscore"] or 0)
        arrow = "📈" if chg >= 0 else "📉"
        price_line = f"{arrow} WTI  <b>${price:.2f}</b>  ({chg:+.2f}%)  z-score: {z:+.2f}"
    else:
        price_line = "WTI  <i>no data yet</i>"

    anomaly_line = "🚨 <b>Market anomaly active</b>" if state.market_anomaly else "✅ No market anomaly"

    if state.idle_manual is True:
        mode_line = "🌙 Mode: <b>idle</b>  (manual override)"
    elif state.idle_manual is False:
        mode_line = "☀️ Mode: <b>normal</b>  (manual override)"
    else:
        mode_line = ("🌙 Mode: <b>overnight/idle</b>  (auto)"
                     if state.overnight else "☀️ Mode: <b>normal</b>  (auto)")

    tz_line = (f"🕐 Timezone: <code>{state.idle_tz}</code>"
               if state.idle_tz else "🕐 Timezone: server local")

    # Narrative
    narrative = state.narrative or {}
    if narrative.get("state"):
        ns    = narrative["state"]
        emoji = STATE_EMOJI.get(ns, "")
        label = STATE_LABELS.get(ns, ns.upper())
        score = narrative.get("weighted_score", 0.0)
        mom   = narrative.get("momentum", "stable")
        mom_icon = {"strengthening": "↑", "weakening": "↓", "stable": "→"}.get(mom, "")
        narrative_line = (
            f"📊 Narrative: {emoji} <b>{label}</b>  {mom_icon} {mom}\n"
            f"   Score: <b>{score:+.2f}</b>  •  "
            f"Bull {narrative.get('bull_count', 0)} / "
            f"Bear {narrative.get('bear_count', 0)} / "
            f"Neutral {narrative.get('neutral_count', 0)} (48h)"
        )
    else:
        narrative_line = "📊 Narrative: <i>not yet computed</i>"

    sep = "─" * 24
    text = (
        f"🛢 <b>OIL SENTINEL — Status</b>  •  <i>{now_str}</i>\n"
        f"{sep}\n"
        f"{price_line}\n"
        f"{anomaly_line}\n"
        f"{mode_line}\n"
        f"{tz_line}\n"
        f"{narrative_line}"
    )
    await send_message(session, bot_token, chat_id, text)


async def _cmd_watch(
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    args: list[str],
) -> None:
    """
    /watch <ticker> <direction> <price> [label...]
    Example: /watch wti below 85 Entry target
    """
    if len(args) < 3:
        await send_message(
            session, bot_token, chat_id,
            "⚠️ Usage: <code>/watch &lt;ticker&gt; &lt;above|below&gt; &lt;price&gt; [label]</code>\n"
            "Example: <code>/watch wti below 85 Entry target</code>\n"
            "Tickers: <code>wti</code>, <code>brent</code>",
        )
        return

    ticker_raw = args[0].lower()
    ticker = TICKER_ALIASES.get(ticker_raw)
    if not ticker:
        await send_message(
            session, bot_token, chat_id,
            f"❌ Unknown ticker <code>{ticker_raw}</code>. Use <code>wti</code> or <code>brent</code>.",
        )
        return

    direction = args[1].lower()
    if direction not in ("above", "below"):
        await send_message(
            session, bot_token, chat_id,
            "❌ Direction must be <code>above</code> or <code>below</code>.",
        )
        return

    try:
        target = float(args[2])
        if target <= 0:
            raise ValueError
    except ValueError:
        await send_message(
            session, bot_token, chat_id,
            f"❌ Invalid price <code>{args[2]}</code> — must be a positive number.",
        )
        return

    label = " ".join(args[3:]) if args[3:] else None
    label_str = f"  —  {label}" if label else ""

    conn = get_connection(db_path)
    try:
        with transaction(conn):
            watch_id = insert_watch(conn, ticker=ticker, direction=direction,
                                    target_price=target, label=label)
    finally:
        conn.close()

    ticker_label = TICKER_LABELS.get(ticker, ticker)
    await send_message(
        session, bot_token, chat_id,
        f"✅ Watching <b>{ticker_label} {direction} ${target:.2f}</b>{label_str}\n"
        f"<i>ID #{watch_id}  ·  /unwatch {watch_id} to remove</i>",
    )
    logger.info("Price watch #%d set: %s %s $%.2f label=%r", watch_id, ticker_label, direction, target, label)


async def _cmd_watches(
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
) -> None:
    """/watches — list all active price watches."""
    conn = get_connection(db_path)
    try:
        watches = get_active_watches(conn)
    finally:
        conn.close()

    if not watches:
        await send_message(session, bot_token, chat_id,
                           "📋 No active price watches.\nSet one with <code>/watch wti below 85</code>")
        return

    sep = "─" * 24
    lines = [f"📋 <b>Active price watches</b>  ({len(watches)})\n{sep}"]
    for w in watches:
        ticker_label = TICKER_LABELS.get(w["ticker"], w["ticker"])
        label_str = f"  —  {w['label']}" if w["label"] else ""
        lines.append(f"<b>#{w['id']}</b>  {ticker_label}  {w['direction']}  <b>${w['target_price']:.2f}</b>{label_str}")

    lines.append(f"\n<i>/unwatch &lt;id&gt;  ·  /unwatch all  ·  /editwatch &lt;id&gt; &lt;price&gt;</i>")
    await send_message(session, bot_token, chat_id, "\n".join(lines))


async def _cmd_unwatch(
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    args: list[str],
) -> None:
    """/unwatch <id>  or  /unwatch all"""
    if not args:
        await send_message(session, bot_token, chat_id,
                           "⚠️ Usage: <code>/unwatch &lt;id&gt;</code>  or  <code>/unwatch all</code>")
        return

    conn = get_connection(db_path)
    try:
        if args[0].lower() == "all":
            with transaction(conn):
                count = deactivate_all_watches(conn)
            await send_message(session, bot_token, chat_id,
                               f"❌ Removed all {count} active watch{'es' if count != 1 else ''}.")
            logger.info("All price watches cleared via Telegram (%d removed)", count)
            return

        try:
            watch_id = int(args[0])
        except ValueError:
            await send_message(session, bot_token, chat_id,
                               f"❌ <code>{args[0]}</code> is not a valid watch ID.")
            return

        watch = get_watch_by_id(conn, watch_id)
        if not watch or not watch["active"]:
            await send_message(session, bot_token, chat_id,
                               f"⚠️ No active watch with ID #{watch_id}.")
            return

        with transaction(conn):
            deactivate_watch(conn, watch_id)
    finally:
        conn.close()

    ticker_label = TICKER_LABELS.get(watch["ticker"], watch["ticker"])
    label_str = f"  —  {watch['label']}" if watch["label"] else ""
    await send_message(
        session, bot_token, chat_id,
        f"❌ Removed: <b>{ticker_label} {watch['direction']} ${watch['target_price']:.2f}</b>{label_str}",
    )
    logger.info("Price watch #%d removed via Telegram", watch_id)


async def _cmd_editwatch(
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    args: list[str],
) -> None:
    """/editwatch <id> <new_price>"""
    if len(args) < 2:
        await send_message(session, bot_token, chat_id,
                           "⚠️ Usage: <code>/editwatch &lt;id&gt; &lt;new_price&gt;</code>\n"
                           "Example: <code>/editwatch 1 88.50</code>")
        return

    try:
        watch_id = int(args[0])
    except ValueError:
        await send_message(session, bot_token, chat_id,
                           f"❌ <code>{args[0]}</code> is not a valid watch ID.")
        return

    try:
        new_price = float(args[1])
        if new_price <= 0:
            raise ValueError
    except ValueError:
        await send_message(session, bot_token, chat_id,
                           f"❌ Invalid price <code>{args[1]}</code>.")
        return

    conn = get_connection(db_path)
    try:
        watch = get_watch_by_id(conn, watch_id)
        if not watch or not watch["active"]:
            await send_message(session, bot_token, chat_id,
                               f"⚠️ No active watch with ID #{watch_id}.")
            return

        old_price = watch["target_price"]
        with transaction(conn):
            update_watch_price(conn, watch_id, new_price)
    finally:
        conn.close()

    ticker_label = TICKER_LABELS.get(watch["ticker"], watch["ticker"])
    label_str = f"  —  {watch['label']}" if watch["label"] else ""
    await send_message(
        session, bot_token, chat_id,
        f"✏️ Updated <b>#{watch_id}</b>: "
        f"{ticker_label} {watch['direction']} <b>${new_price:.2f}</b>{label_str}\n"
        f"<i>(was ${old_price:.2f})</i>",
    )
    logger.info("Price watch #%d updated: $%.2f → $%.2f via Telegram", watch_id, old_price, new_price)


async def _cmd_idle(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    args: list[str],
    state,
    cfg,
) -> None:
    """
    Handle /idle subcommands.

    /idle            — show current idle configuration
    /idle on         — force idle mode (disables automatic schedule)
    /idle off        — force normal mode (disables automatic schedule)
    /idle auto       — return to automatic time-based switching
    /idle tz <name>  — set timezone for overnight window (returns to auto)
    /idle tz local   — revert to server local time (returns to auto)
    """
    sub = args[0].lower() if args else ""

    if sub == "on":
        state.idle_manual = True
        await send_message(
            session, bot_token, chat_id,
            "🌙 <b>Idle mode ON</b>  (manual)\n"
            "Alerts suppressed, market polling paused.\n"
            "Use /idle off to resume or /idle auto to restore schedule.",
        )
        logger.info("Idle mode manually forced ON via Telegram")
        return

    if sub == "off":
        state.idle_manual = False
        await send_message(
            session, bot_token, chat_id,
            "☀️ <b>Normal mode ON</b>  (manual)\n"
            "All loops running at full frequency.\n"
            "Use /idle auto to restore automatic schedule.",
        )
        logger.info("Idle mode manually forced OFF via Telegram")
        return

    if sub == "auto":
        state.idle_manual = None
        tz_note = (f"  •  timezone: <code>{state.idle_tz}</code>"
                   if state.idle_tz else "  •  timezone: server local")
        s = cfg.idle.overnight_start
        e = cfg.idle.overnight_end
        await send_message(
            session, bot_token, chat_id,
            f"🔄 <b>Automatic idle schedule restored</b>{tz_note}\n"
            f"Overnight window: <b>{s:02d}:00 – {e:02d}:00</b>",
        )
        logger.info("Idle mode returned to automatic schedule via Telegram")
        return

    if sub == "tz":
        if not args[1:]:
            await send_message(
                session, bot_token, chat_id,
                "⚠️ Usage: <code>/idle tz Europe/Berlin</code>  or  <code>/idle tz local</code>",
            )
            return

        tz_arg = args[1]

        if tz_arg.lower() == "local":
            state.idle_tz = None
            state.idle_manual = None
            await send_message(
                session, bot_token, chat_id,
                "🕐 Timezone reset to <b>server local time</b>. Schedule is now automatic.",
            )
            logger.info("Idle timezone reset to server local via Telegram")
            return

        # Validate timezone
        if not _ZONEINFO_AVAILABLE:
            await send_message(session, bot_token, chat_id,
                               "⚠️ Timezone support unavailable on this Python build.")
            return
        try:
            ZoneInfo(tz_arg)   # raises ZoneInfoNotFoundError if unknown
        except Exception:
            await send_message(
                session, bot_token, chat_id,
                f"❌ Unknown timezone: <code>{tz_arg}</code>\n"
                "Use IANA names like <code>Europe/Berlin</code>, <code>America/New_York</code>, "
                "<code>Asia/Dubai</code>.",
            )
            return

        state.idle_tz = tz_arg
        state.idle_manual = None   # return to auto with the new tz
        s = cfg.idle.overnight_start
        e = cfg.idle.overnight_end
        try:
            local_now = datetime.now(ZoneInfo(tz_arg)).strftime("%H:%M")
        except Exception:
            local_now = "?"
        await send_message(
            session, bot_token, chat_id,
            f"🕐 Timezone set to <code>{tz_arg}</code>  (currently {local_now})\n"
            f"Overnight window: <b>{s:02d}:00 – {e:02d}:00 {tz_arg}</b>\n"
            "Schedule is now automatic in this timezone.",
        )
        logger.info("Idle timezone set to %s via Telegram", tz_arg)
        return

    # No subcommand — show current status
    if state.idle_manual is True:
        mode_str = "🌙 <b>idle</b>  (manual override — schedule suspended)"
    elif state.idle_manual is False:
        mode_str = "☀️ <b>normal</b>  (manual override — schedule suspended)"
    else:
        mode_str = ("🌙 <b>overnight/idle</b>  (automatic)"
                    if state.overnight else "☀️ <b>normal</b>  (automatic)")

    s = cfg.idle.overnight_start
    e = cfg.idle.overnight_end
    tz_str = f"<code>{state.idle_tz}</code>" if state.idle_tz else "server local"

    try:
        tz_obj = ZoneInfo(state.idle_tz) if (state.idle_tz and _ZONEINFO_AVAILABLE) else None
        now_local = datetime.now(tz_obj).strftime("%H:%M") if tz_obj else datetime.now().strftime("%H:%M")
    except Exception:
        now_local = datetime.now().strftime("%H:%M")

    sep = "─" * 24
    text = (
        f"🌙 <b>Idle Mode Status</b>\n{sep}\n"
        f"Current mode:  {mode_str}\n"
        f"Schedule:      <b>{s:02d}:00 – {e:02d}:00</b>  ({tz_str})\n"
        f"Local time:    <b>{now_local}</b>\n"
        f"{sep}\n"
        "<i>Subcommands:</i>\n"
        "/idle on  •  /idle off  •  /idle auto\n"
        "/idle tz &lt;name&gt;  •  /idle tz local"
    )
    await send_message(session, bot_token, chat_id, text)


# ---------------------------------------------------------------------------
# Portfolio helpers
# ---------------------------------------------------------------------------

def _h(text: str) -> str:
    """Escape text for Telegram HTML."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


TIMEFRAME_HOURS: dict[str, Optional[int]] = {
    "7d":  7 * 24,
    "30d": 30 * 24,
    "90d": 90 * 24,
    "all": None,
}


async def _portfolio_create(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, args: list[str],
) -> None:
    """/portfolio create <name> <long|short>"""
    if len(args) < 2:
        await send_message(
            session, bot_token, chat_id,
            "⚠️ Usage: <code>/portfolio create &lt;name&gt; &lt;long|short&gt;</code>\n"
            "Example: <code>/portfolio create hormuz-short short</code>",
        )
        return

    name = args[0].lower()
    product = args[1].lower()

    if product not in ("long", "short"):
        await send_message(
            session, bot_token, chat_id,
            "❌ Product must be <code>long</code> or <code>short</code>.",
        )
        return

    ticker_primary, _ = PRODUCT_TICKERS[product]
    product_name = PRODUCT_NAMES[product]

    conn = get_connection(db_path)
    try:
        # Check if name already exists
        existing = get_portfolio_by_name(conn, name)
        if existing:
            status = "active" if existing["active"] else "deleted"
            await send_message(
                session, bot_token, chat_id,
                f"❌ Portfolio <code>{_h(name)}</code> already exists ({status}).",
            )
            return
        with transaction(conn):
            portfolio_id = insert_portfolio(
                conn, name=name, ticker=ticker_primary,
                product=product, currency="EUR",
            )
    finally:
        conn.close()

    await send_message(
        session, bot_token, chat_id,
        f"📁 Portfolio <b>{_h(name)}</b> created — tracking {_h(product_name)} "
        f"(<code>{_h(ticker_primary)}</code>)\n"
        f"<i>Use /buy {_h(name)} &lt;amount&gt; to record your first purchase.</i>",
    )
    logger.info("Portfolio '%s' created: %s (%s)", name, product_name, ticker_primary)


async def _portfolio_show(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, name: str,
) -> None:
    """Show current state of a portfolio."""
    loop = asyncio.get_running_loop()

    conn = get_connection(db_path)
    try:
        p = get_portfolio_by_name(conn, name)
        if not p or not p["active"]:
            await send_message(
                session, bot_token, chat_id,
                f"❌ Portfolio <code>{_h(name)}</code> not found.\n"
                f"<i>/portfolio create {_h(name)} long|short</i> to create it.",
            )
            return
        p = dict(p)
        pos = get_portfolio_position(conn, p["id"])
    finally:
        conn.close()

    ticker_primary = p["ticker"]
    ticker_fallback = PRODUCT_TICKERS.get(p["product"], (ticker_primary, ticker_primary))[1]
    product_name = PRODUCT_NAMES.get(p["product"], p["product"])

    price, used_ticker, is_stale, fetched_at = await loop.run_in_executor(
        None, lambda: fetch_etp_price(ticker_primary, ticker_fallback)
    )

    if price is None:
        await send_message(
            session, bot_token, chat_id,
            f"⚠️ Could not fetch price for <code>{_h(ticker_primary)}</code>. "
            "Try again shortly.",
        )
        return

    current_value = pos["total_units"] * price
    pnl_eur = current_value - pos["net_invested"]
    pnl_pct = (pnl_eur / pos["net_invested"] * 100) if pos["net_invested"] > 0 else 0.0
    pnl_sign = "+" if pnl_eur >= 0 else ""

    stale_note = ""
    if is_stale and fetched_at:
        age_min = int((datetime.now(timezone.utc) - fetched_at).total_seconds() / 60)
        stale_note = f"\n⚠️ <i>Stale price — last fetched {age_min}min ago</i>"
    elif used_ticker != ticker_primary:
        stale_note = f"\n⚠️ <i>Using London fallback ticker {_h(used_ticker)} (GBP)</i>"

    buys_label = f"{pos['buy_count']} buy{'s' if pos['buy_count'] != 1 else ''}"
    sep = "─" * 24
    text = (
        f"📊 <b>Portfolio: {_h(name)}</b>\n{sep}\n"
        f"Product:        {_h(product_name)} (<code>{_h(used_ticker)}</code>)\n"
        f"Total invested: €{pos['total_invested']:.2f} ({buys_label})\n"
        f"Units held:     {pos['total_units']:.4f}\n"
        f"Avg cost basis: €{pos['avg_cost']:.4f}\n"
        f"Current price:  €{price:.4f}\n"
        f"Current value:  €{current_value:.2f}\n"
        f"P/L:            {pnl_sign}€{pnl_eur:.2f} ({pnl_sign}{pnl_pct:.1f}%)"
        f"{stale_note}"
    )
    await send_message(session, bot_token, chat_id, text)


async def _portfolio_history(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, name: str,
) -> None:
    """Show transaction history for a portfolio."""
    conn = get_connection(db_path)
    try:
        p = get_portfolio_by_name(conn, name)
        if not p:
            await send_message(
                session, bot_token, chat_id,
                f"❌ Portfolio <code>{_h(name)}</code> not found.",
            )
            return
        p = dict(p)
        txs = get_transactions(conn, p["id"])
    finally:
        conn.close()

    if not txs:
        await send_message(
            session, bot_token, chat_id,
            f"📜 Portfolio <b>{_h(name)}</b> has no transactions yet.",
        )
        return

    sep = "─" * 24
    lines = [f"📜 <b>History: {_h(name)}</b>\n{sep}"]
    for i, tx in enumerate(txs, 1):
        date_str = tx["timestamp"][:10]
        action = tx["action"].upper()
        lines.append(
            f"#{i} {date_str}  {action}  "
            f"€{tx['amount_eur']:.2f} @ €{tx['price_per_unit']:.4f} "
            f"→ {tx['units']:.4f} units"
        )

    await send_message(session, bot_token, chat_id, "\n".join(lines))


async def _portfolio_delete(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, name: str, state,
) -> None:
    """Ask for confirmation before deactivating a portfolio."""
    conn = get_connection(db_path)
    try:
        p = get_portfolio_by_name(conn, name)
        if not p or not p["active"]:
            await send_message(
                session, bot_token, chat_id,
                f"❌ No active portfolio named <code>{_h(name)}</code>.",
            )
            return
    finally:
        conn.close()

    state.pending_confirm = {"type": "delete_portfolio", "name": name}
    await send_message(
        session, bot_token, chat_id,
        f"⚠️ Delete portfolio <b>{_h(name)}</b>? This cannot be undone.\n"
        f"Transactions and history are preserved.\n"
        f"Send <code>/confirm</code> to proceed.",
    )


async def _portfolio_chart(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, name: str, timeframe: str = "30d",
) -> None:
    """/portfolio chart <name> [7d|30d|90d|all]"""
    loop = asyncio.get_running_loop()

    hours = TIMEFRAME_HOURS.get(timeframe, 30 * 24)

    conn = get_connection(db_path)
    try:
        p = get_portfolio_by_name(conn, name)
        if not p or not p["active"]:
            await send_message(
                session, bot_token, chat_id,
                f"❌ Portfolio <code>{_h(name)}</code> not found.",
            )
            return
        p = dict(p)
        snapshots = get_portfolio_snapshots(conn, p["id"], hours=hours)
        txs = get_transactions(conn, p["id"])
    finally:
        conn.close()

    if len(snapshots) < 2:
        await send_message(
            session, bot_token, chat_id,
            f"⚠️ Not enough snapshot history yet for <b>{_h(name)}</b> ({timeframe}).\n"
            "<i>Snapshots are taken hourly — check back later.</i>",
        )
        return

    product_name = PRODUCT_NAMES.get(p["product"], p["product"])
    chart_bytes = await loop.run_in_executor(
        None,
        lambda: generate_portfolio_chart(
            snapshots, txs,
            title=f"{_h(name)}  ·  {_h(product_name)}  ·  {timeframe}",
        ),
    )

    if chart_bytes:
        await send_photo(
            session, bot_token, chat_id, chart_bytes,
            caption=f"📈 Portfolio: {name} — last {timeframe}",
        )
    else:
        await send_message(
            session, bot_token, chat_id,
            "⚠️ Chart generation failed.",
        )


async def _portfolio_stats(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, name: str,
) -> None:
    """/portfolio stats <name>"""
    loop = asyncio.get_running_loop()

    conn = get_connection(db_path)
    try:
        p = get_portfolio_by_name(conn, name)
        if not p or not p["active"]:
            await send_message(
                session, bot_token, chat_id,
                f"❌ Portfolio <code>{_h(name)}</code> not found.",
            )
            return
        p = dict(p)
    finally:
        conn.close()

    ticker_primary = p["ticker"]
    ticker_fallback = PRODUCT_TICKERS.get(p["product"], (ticker_primary, ticker_primary))[1]

    price, used_ticker, is_stale, _ = await loop.run_in_executor(
        None, lambda: fetch_etp_price(ticker_primary, ticker_fallback)
    )

    if price is None:
        await send_message(
            session, bot_token, chat_id,
            f"⚠️ Could not fetch price for <code>{_h(ticker_primary)}</code>.",
        )
        return

    conn = get_connection(db_path)
    try:
        stats = get_portfolio_stats(conn, p["id"], price)
    finally:
        conn.close()

    pos = stats["position"]
    product_name = PRODUCT_NAMES.get(p["product"], p["product"])
    sep = "─" * 25
    pnl_sign = "+" if stats["pnl_eur"] >= 0 else ""
    price_vs_cost = ((price - pos["avg_cost"]) / pos["avg_cost"] * 100) if pos["avg_cost"] else 0.0
    price_vs_sign = "+" if price_vs_cost >= 0 else ""

    lines = [
        f"📊 <b>Statistics: {_h(name)}</b>",
        sep,
        f"Product:   {_h(product_name)} (<code>{_h(used_ticker)}</code>)",
        f"Active since: {stats['active_since']} ({stats['days_active']} days)",
        "",
        "💰 <b>Position</b>",
        f"Total invested:  €{pos['total_invested']:.2f}",
        f"Total withdrawn: €{pos['total_withdrawn']:.2f}",
        f"Net invested:    €{pos['net_invested']:.2f}",
        f"Current value:   €{stats['current_value']:.2f}",
        "",
        "📈 <b>Performance</b>",
        f"P/L (EUR): {pnl_sign}€{stats['pnl_eur']:.2f}",
        f"P/L (%):   {pnl_sign}{stats['pnl_pct']:.1f}%",
    ]

    if stats["best_day"]:
        bd = stats["best_day"]
        bs = "+" if bd["pnl_eur"] >= 0 else ""
        lines.append(f"Best day:  {bs}€{bd['pnl_eur']:.2f} ({bs}{bd['pnl_pct']:.1f}%) on {bd['date']}")
    if stats["worst_day"]:
        wd = stats["worst_day"]
        ws = "+" if wd["pnl_eur"] >= 0 else ""
        lines.append(f"Worst day: {ws}€{wd['pnl_eur']:.2f} ({ws}{wd['pnl_pct']:.1f}%) on {wd['date']}")
    if stats["max_drawdown"]:
        dd = stats["max_drawdown"]
        lines.append(f"Max drawdown: €{dd['pnl_eur']:.2f} ({dd['pnl_pct']:.1f}%) on {dd['date']}")

    lines += [
        "",
        "🛒 <b>DCA Stats</b>",
        f"Total buys:    {pos['buy_count']}",
        f"Avg buy price: €{pos['avg_cost']:.4f}",
    ]

    if stats["lowest_buy_tx"]:
        lx = stats["lowest_buy_tx"]
        lines.append(f"Lowest buy:   €{lx['price_per_unit']:.4f} on {lx['timestamp'][:10]}")
    if stats["highest_buy_tx"]:
        hx = stats["highest_buy_tx"]
        lines.append(f"Highest buy:  €{hx['price_per_unit']:.4f} on {hx['timestamp'][:10]}")

    lines += [
        "",
        "📦 <b>Holdings</b>",
        f"Units held:       {pos['total_units']:.4f}",
        f"Current price:    €{price:.4f}",
        f"Price vs avg cost: {price_vs_sign}{price_vs_cost:.1f}%",
    ]

    if is_stale:
        lines.append("\n⚠️ <i>Price may be stale</i>")

    await send_message(session, bot_token, chat_id, "\n".join(lines))


# ---------------------------------------------------------------------------
# /portfolios — list all active portfolios
# ---------------------------------------------------------------------------

async def _cmd_portfolios(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str,
) -> None:
    loop = asyncio.get_running_loop()

    conn = get_connection(db_path)
    try:
        portfolios = get_active_portfolios(conn)
        if not portfolios:
            await send_message(
                session, bot_token, chat_id,
                "📋 No active portfolios.\n"
                "<i>/portfolio create &lt;name&gt; &lt;long|short&gt; to create one.</i>",
            )
            return
        portfolio_data = []
        for p in portfolios:
            p = dict(p)
            pos = get_portfolio_position(conn, p["id"])
            p["_pos"] = pos
            portfolio_data.append(p)
    finally:
        conn.close()

    sep = "─" * 24
    lines = [f"📋 <b>Portfolios</b>  ({len(portfolio_data)})\n{sep}"]

    for i, p in enumerate(portfolio_data, 1):
        pos = p["_pos"]
        product_name = PRODUCT_NAMES.get(p["product"], p["product"])

        # Fetch price for P/L (use cache — fast if recently fetched)
        ticker_primary = p["ticker"]
        ticker_fallback = PRODUCT_TICKERS.get(p["product"], (ticker_primary, ticker_primary))[1]
        price, _, is_stale, _ = await loop.run_in_executor(
            None, lambda tp=ticker_primary, tf=ticker_fallback: fetch_etp_price(tp, tf)
        )

        if price is not None and pos["net_invested"] > 0:
            current_value = pos["total_units"] * price
            pnl_pct = (current_value - pos["net_invested"]) / pos["net_invested"] * 100
            pnl_sign = "+" if pnl_pct >= 0 else ""
            value_str = f"€{current_value:.2f} ({pnl_sign}{pnl_pct:.1f}%)"
        elif pos["total_units"] == 0:
            value_str = "0 units"
        else:
            value_str = "€? (price unavailable)"

        lines.append(f"{i}. <b>{_h(p['name'])}</b>  —  {_h(product_name)}  —  {value_str}")

    await send_message(session, bot_token, chat_id, "\n".join(lines))


# ---------------------------------------------------------------------------
# /portfolio dispatcher
# ---------------------------------------------------------------------------

async def _cmd_portfolio(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, args: list[str], state,
) -> None:
    """Route /portfolio subcommands."""
    if not args:
        await send_message(
            session, bot_token, chat_id,
            "📁 <b>/portfolio</b> — subcommands:\n"
            "  <code>/portfolio create &lt;name&gt; &lt;long|short&gt;</code>\n"
            "  <code>/portfolio &lt;name&gt;</code>          — view state\n"
            "  <code>/portfolio history &lt;name&gt;</code>\n"
            "  <code>/portfolio chart &lt;name&gt; [7d|30d|90d|all]</code>\n"
            "  <code>/portfolio stats &lt;name&gt;</code>\n"
            "  <code>/portfolio delete &lt;name&gt;</code>",
        )
        return

    sub = args[0].lower()

    if sub == "create":
        await _portfolio_create(db_path, session, bot_token, chat_id, args[1:])
    elif sub == "history":
        if len(args) < 2:
            await send_message(session, bot_token, chat_id,
                               "⚠️ Usage: <code>/portfolio history &lt;name&gt;</code>")
            return
        await _portfolio_history(db_path, session, bot_token, chat_id, args[1].lower())
    elif sub == "delete":
        if len(args) < 2:
            await send_message(session, bot_token, chat_id,
                               "⚠️ Usage: <code>/portfolio delete &lt;name&gt;</code>")
            return
        await _portfolio_delete(db_path, session, bot_token, chat_id, args[1].lower(), state)
    elif sub == "chart":
        if len(args) < 2:
            await send_message(session, bot_token, chat_id,
                               "⚠️ Usage: <code>/portfolio chart &lt;name&gt; [7d|30d|90d|all]</code>")
            return
        timeframe = args[2].lower() if len(args) >= 3 else "30d"
        if timeframe not in TIMEFRAME_HOURS:
            timeframe = "30d"
        await _portfolio_chart(db_path, session, bot_token, chat_id, args[1].lower(), timeframe)
    elif sub == "stats":
        if len(args) < 2:
            await send_message(session, bot_token, chat_id,
                               "⚠️ Usage: <code>/portfolio stats &lt;name&gt;</code>")
            return
        await _portfolio_stats(db_path, session, bot_token, chat_id, args[1].lower())
    else:
        # Treat as portfolio name
        await _portfolio_show(db_path, session, bot_token, chat_id, sub)


# ---------------------------------------------------------------------------
# /buy and /sell
# ---------------------------------------------------------------------------

async def _cmd_buy(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, args: list[str],
) -> None:
    """/buy <portfolio-name> <amount>"""
    if len(args) < 2:
        await send_message(
            session, bot_token, chat_id,
            "⚠️ Usage: <code>/buy &lt;portfolio-name&gt; &lt;amount&gt;</code>\n"
            "Example: <code>/buy hormuz-short 100</code>",
        )
        return

    loop = asyncio.get_running_loop()
    name = args[0].lower()

    try:
        amount = float(args[1])
        if amount <= 0:
            raise ValueError
    except ValueError:
        await send_message(session, bot_token, chat_id,
                           f"❌ Invalid amount <code>{_h(args[1])}</code>.")
        return

    conn = get_connection(db_path)
    try:
        p = get_portfolio_by_name(conn, name)
        if not p or not p["active"]:
            await send_message(
                session, bot_token, chat_id,
                f"❌ Portfolio <code>{_h(name)}</code> not found.\n"
                f"<i>Create it first: /portfolio create {_h(name)} long|short</i>",
            )
            return
        p = dict(p)
        pos_before = get_portfolio_position(conn, p["id"])
    finally:
        conn.close()

    ticker_primary = p["ticker"]
    ticker_fallback = PRODUCT_TICKERS.get(p["product"], (ticker_primary, ticker_primary))[1]
    price, used_ticker, is_stale, fetched_at = await loop.run_in_executor(
        None, lambda: fetch_etp_price(ticker_primary, ticker_fallback)
    )

    if price is None:
        await send_message(
            session, bot_token, chat_id,
            f"⚠️ Could not fetch price for <code>{_h(ticker_primary)}</code>. "
            "Cannot record buy without a valid price.",
        )
        return

    if is_stale:
        age_min = 0
        if fetched_at:
            age_min = int((datetime.now(timezone.utc) - fetched_at).total_seconds() / 60)
        await send_message(
            session, bot_token, chat_id,
            f"⚠️ Price is stale ({age_min}min old). Buy aborted — try again when price is fresh.",
        )
        return

    units = amount / price

    conn = get_connection(db_path)
    try:
        with transaction(conn):
            insert_transaction(
                conn,
                portfolio_id=p["id"],
                action="buy",
                amount_eur=amount,
                price_per_unit=price,
                units=units,
            )
        pos_after = get_portfolio_position(conn, p["id"])
    finally:
        conn.close()

    product_name = PRODUCT_NAMES.get(p["product"], p["product"])
    total_value = pos_after["total_units"] * price
    pnl_eur = total_value - pos_after["net_invested"]
    pnl_pct = (pnl_eur / pos_after["net_invested"] * 100) if pos_after["net_invested"] > 0 else 0.0
    pnl_sign = "+" if pnl_eur >= 0 else ""

    await send_message(
        session, bot_token, chat_id,
        f"✅ Bought €{amount:.2f} of {_h(product_name)} @ €{price:.4f} "
        f"→ {units:.4f} units\n"
        f"Portfolio <b>{_h(name)}</b>: {pos_after['total_units']:.4f} units | "
        f"Avg cost: €{pos_after['avg_cost']:.4f} | "
        f"Value: €{total_value:.2f} ({pnl_sign}€{pnl_eur:.2f}, {pnl_sign}{pnl_pct:.1f}%)",
    )
    logger.info("Buy recorded: portfolio=%s €%.2f @ €%.4f = %.4f units", name, amount, price, units)


async def _cmd_sell(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, args: list[str],
) -> None:
    """/sell <portfolio-name> <amount|all>"""
    if len(args) < 2:
        await send_message(
            session, bot_token, chat_id,
            "⚠️ Usage: <code>/sell &lt;portfolio-name&gt; &lt;amount|all&gt;</code>\n"
            "Example: <code>/sell hormuz-short 50</code>  or  <code>/sell hormuz-short all</code>",
        )
        return

    loop = asyncio.get_running_loop()
    name = args[0].lower()
    amount_raw = args[1].lower()
    sell_all = (amount_raw == "all")

    if not sell_all:
        try:
            amount = float(amount_raw)
            if amount <= 0:
                raise ValueError
        except ValueError:
            await send_message(session, bot_token, chat_id,
                               f"❌ Invalid amount <code>{_h(amount_raw)}</code>.")
            return

    conn = get_connection(db_path)
    try:
        p = get_portfolio_by_name(conn, name)
        if not p or not p["active"]:
            await send_message(
                session, bot_token, chat_id,
                f"❌ Portfolio <code>{_h(name)}</code> not found.",
            )
            return
        p = dict(p)
        pos = get_portfolio_position(conn, p["id"])
    finally:
        conn.close()

    if pos["total_units"] <= 0:
        await send_message(
            session, bot_token, chat_id,
            f"❌ Portfolio <b>{_h(name)}</b> has no units to sell.",
        )
        return

    ticker_primary = p["ticker"]
    ticker_fallback = PRODUCT_TICKERS.get(p["product"], (ticker_primary, ticker_primary))[1]
    price, used_ticker, is_stale, fetched_at = await loop.run_in_executor(
        None, lambda: fetch_etp_price(ticker_primary, ticker_fallback)
    )

    if price is None:
        await send_message(
            session, bot_token, chat_id,
            f"⚠️ Could not fetch price for <code>{_h(ticker_primary)}</code>.",
        )
        return

    if is_stale:
        age_min = 0
        if fetched_at:
            age_min = int((datetime.now(timezone.utc) - fetched_at).total_seconds() / 60)
        await send_message(
            session, bot_token, chat_id,
            f"⚠️ Price is stale ({age_min}min old). Sell aborted — try again when price is fresh.",
        )
        return

    if sell_all:
        units_to_sell = pos["total_units"]
        amount = units_to_sell * price
    else:
        units_to_sell = amount / price  # type: ignore[assignment]
        if units_to_sell > pos["total_units"] + 1e-9:
            current_value = pos["total_units"] * price
            await send_message(
                session, bot_token, chat_id,
                f"❌ Cannot sell €{amount:.2f} — you only hold "
                f"{pos['total_units']:.4f} units worth €{current_value:.2f}.\n"
                f"<i>Use /sell {_h(name)} all to sell everything.</i>",
            )
            return

    product_name = PRODUCT_NAMES.get(p["product"], p["product"])

    conn = get_connection(db_path)
    try:
        with transaction(conn):
            insert_transaction(
                conn,
                portfolio_id=p["id"],
                action="sell",
                amount_eur=amount,
                price_per_unit=price,
                units=units_to_sell,
            )
        pos_after = get_portfolio_position(conn, p["id"])
    finally:
        conn.close()

    # P/L vs net cost basis
    cost_of_sold = units_to_sell * pos["avg_cost"]
    pl_eur = amount - cost_of_sold
    pl_pct = (pl_eur / cost_of_sold * 100) if cost_of_sold > 0 else 0.0
    pl_sign = "+" if pl_eur >= 0 else ""

    remaining_str = (
        f"Portfolio <b>{_h(name)}</b>: 0 units remaining"
        if pos_after["total_units"] <= 1e-9
        else f"Portfolio <b>{_h(name)}</b>: {pos_after['total_units']:.4f} units remaining"
    )

    await send_message(
        session, bot_token, chat_id,
        f"💰 Sold {units_to_sell:.4f} units of {_h(product_name)} @ €{price:.4f} "
        f"→ €{amount:.2f}\n"
        f"P/L: {pl_sign}€{pl_eur:.2f} ({pl_sign}{pl_pct:.1f}%)\n"
        f"{remaining_str}",
    )
    logger.info("Sell recorded: portfolio=%s %.4f units @ €%.4f = €%.2f", name, units_to_sell, price, amount)


# ---------------------------------------------------------------------------
# /confirm — execute pending confirmation
# ---------------------------------------------------------------------------

async def _cmd_confirm(
    db_path: str, session: aiohttp.ClientSession,
    bot_token: str, chat_id: str, state,
) -> None:
    """Execute a pending confirmed action."""
    if not getattr(state, "pending_confirm", None):
        await send_message(session, bot_token, chat_id,
                           "ℹ️ Nothing pending confirmation.")
        return

    action = state.pending_confirm
    state.pending_confirm = None

    if action["type"] == "delete_portfolio":
        name = action["name"]
        conn = get_connection(db_path)
        try:
            p = get_portfolio_by_name(conn, name)
            if not p or not p["active"]:
                await send_message(session, bot_token, chat_id,
                                   f"⚠️ Portfolio <code>{_h(name)}</code> no longer exists.")
                return
            with transaction(conn):
                deactivate_portfolio(conn, p["id"])
        finally:
            conn.close()

        await send_message(
            session, bot_token, chat_id,
            f"🗑️ Portfolio <b>{_h(name)}</b> deleted. "
            f"Transaction history is preserved.",
        )
        logger.info("Portfolio '%s' deactivated via /confirm", name)
    else:
        await send_message(session, bot_token, chat_id,
                           "⚠️ Unknown pending action — cleared.")


async def _cmd_shutdown(
    session: aiohttp.ClientSession,
    bot_token: str,
    chat_id: str,
    update_id: int,
) -> None:
    """
    Send confirmation, acknowledge the update with Telegram, then exit.

    The acknowledgement step (getUpdates with offset=update_id+1) is critical:
    without it Telegram keeps the /shutdown_bot update queued and replays it
    on every restart, causing an immediate re-shutdown loop.
    """
    logger.warning("Shutdown requested via Telegram by chat %s", chat_id)
    await send_message(
        session, bot_token, chat_id,
        "🔴 <b>Shutting down Oil Sentinel.</b>",
    )
    # Acknowledge this update so Telegram won't re-deliver it on next startup
    await get_updates(session, bot_token, offset=update_id + 1, timeout=0)
    logger.info("Shutdown update acknowledged (offset=%d)", update_id + 1)
    sys.exit(0)


# ---------------------------------------------------------------------------
# Update dispatcher
# ---------------------------------------------------------------------------

async def handle_update(
    update: dict,
    db_path: str,
    session: aiohttp.ClientSession,
    bot_token: str,
    allowed_chat_id: str,
    state,
    cfg,
) -> None:
    """Route one Telegram update to the appropriate command handler."""
    msg  = update.get("message") or {}
    text = (msg.get("text") or "").strip()
    chat_id_incoming = str(msg.get("chat", {}).get("id", ""))

    if not text.startswith("/"):
        return

    # Security: only respond to the configured chat
    if chat_id_incoming != str(allowed_chat_id):
        logger.debug("Ignoring command from unrecognised chat %s", chat_id_incoming)
        return

    parts   = text.split()
    command = parts[0].split("@")[0].lower()   # strip /cmd@BotName suffix
    args    = parts[1:]

    if command not in _ROUTABLE:
        return

    update_id: int = update.get("update_id", 0)
    logger.info("Command received: %s %s from chat %s", command, args, chat_id_incoming)

    if command == "/chart":
        await _cmd_chart(db_path, session, bot_token, allowed_chat_id, state, args)
    elif command == "/status":
        await _cmd_status(db_path, session, bot_token, allowed_chat_id, state)
    elif command == "/watch":
        await _cmd_watch(db_path, session, bot_token, allowed_chat_id, args)
    elif command == "/watches":
        await _cmd_watches(db_path, session, bot_token, allowed_chat_id)
    elif command == "/unwatch":
        await _cmd_unwatch(db_path, session, bot_token, allowed_chat_id, args)
    elif command == "/editwatch":
        await _cmd_editwatch(db_path, session, bot_token, allowed_chat_id, args)
    elif command == "/portfolio":
        await _cmd_portfolio(db_path, session, bot_token, allowed_chat_id, args, state)
    elif command == "/portfolios":
        await _cmd_portfolios(db_path, session, bot_token, allowed_chat_id)
    elif command == "/buy":
        await _cmd_buy(db_path, session, bot_token, allowed_chat_id, args)
    elif command == "/sell":
        await _cmd_sell(db_path, session, bot_token, allowed_chat_id, args)
    elif command == "/confirm":
        await _cmd_confirm(db_path, session, bot_token, allowed_chat_id, state)
    elif command == "/idle":
        if cfg is not None:
            await _cmd_idle(session, bot_token, allowed_chat_id, args, state, cfg)
        else:
            await send_message(session, bot_token, allowed_chat_id,
                               "⚠️ /idle is unavailable — config not loaded.")
    elif command == "/help":
        await send_message(session, bot_token, allowed_chat_id, _HELP_TEXT)
    elif command == "/shutdown_bot":
        await _cmd_shutdown(session, bot_token, allowed_chat_id, update_id)
