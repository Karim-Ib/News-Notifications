"""Notifications subpackage (Telegram alerts + bot commands)."""

from oil_sentinel.notifications.commands import get_updates, handle_update, register_commands
from oil_sentinel.notifications.telegram import (
    check_price_watches,
    dispatch_alerts,
    dispatch_digest,
    dispatch_morning_summary,
    send_market_alert,
    send_narrative_transition_alert,
)

__all__ = [
    "get_updates",
    "handle_update",
    "register_commands",
    "check_price_watches",
    "dispatch_alerts",
    "dispatch_digest",
    "dispatch_morning_summary",
    "send_market_alert",
    "send_narrative_transition_alert",
]
