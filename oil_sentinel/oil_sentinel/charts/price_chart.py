"""
WTI price and narrative sentiment chart generators.

Two separate charts, each returned as PNG bytes:

  generate_price_chart     — price line + alert markers + legend
  generate_sentiment_chart — weighted score + state shading + legend

Color conventions:
  Bullish marker  : red  (#ef5350) ▲ — supply risk, price up
  Bearish marker  : green (#66bb6a) ▼ — supply relief, price down
  Anomaly marker  : amber (#ffa726) ● — market spike without directional label
"""

import io
import logging
from datetime import datetime
from typing import Optional

import matplotlib
matplotlib.use("Agg")   # non-interactive backend — must be set before pyplot import
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D

logger = logging.getLogger(__name__)

_WIDTH  = 10    # figure width  (inches)
_HEIGHT = 4     # figure height (inches)
_DPI    = 100   # 1000 × 400 px output

_BG_OUTER = "#0f0f1a"
_BG_INNER = "#131320"
_GRID     = "#1e1e3a"
_LINE     = "#4fc3f7"
_TICK     = "#7777aa"
_SPINE    = "#2a2a4a"
_TITLE    = "#aaaacc"

_MARKER = {
    "bullish": ("#ef5350", "▲"),
    "bearish": ("#66bb6a", "▼"),
    "neutral": ("#ffa726", "●"),
}

# Narrative state colours (background shading + legend)
_STATE_COLORS: dict[str, str] = {
    "strong_escalation":    "#c62828",
    "escalation":           "#ef5350",
    "stable":               "#f9a825",
    "de_escalation":        "#43a047",
    "strong_de_escalation": "#1b5e20",
}
_STATE_DISPLAY_LABELS: dict[str, str] = {
    "strong_escalation":    "Strong Escalation",
    "escalation":           "Escalation",
    "stable":               "Stable",
    "de_escalation":        "De-escalation",
    "strong_de_escalation": "Strong De-escalation",
}
_STATE_TRANSITION_ARROW: dict[str, str] = {
    "strong_escalation":    "▲▲",
    "escalation":           "▲",
    "stable":               "→",
    "de_escalation":        "▼",
    "strong_de_escalation": "▼▼",
}
_STATE_BG_ALPHA = 0.18


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _style_axes(ax) -> None:
    """Apply dark-theme spine and tick styling."""
    for spine in ax.spines.values():
        spine.set_color(_SPINE)
    ax.tick_params(colors=_TICK, labelsize=8)
    ax.grid(color=_GRID, linestyle="-", linewidth=0.7, alpha=0.9)


def _apply_date_fmt(ax, times: list) -> None:
    """Choose an appropriate date/time formatter based on the time span."""
    if len(times) < 2:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        return
    span_hours = (times[-1] - times[0]).total_seconds() / 3600
    if span_hours <= 26:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    elif span_hours <= 72:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=12))
    elif span_hours <= 240:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))


def _apply_legend(ax, handles: list) -> None:
    """Attach a styled legend to an axes object."""
    if not handles:
        return
    leg = ax.legend(
        handles=handles,
        loc="upper left",
        fontsize=7,
        framealpha=0.9,
        borderpad=0.5,
        ncol=min(len(handles), 3),
    )
    leg.get_frame().set_facecolor(_BG_OUTER)
    leg.get_frame().set_edgecolor(_SPINE)
    for text in leg.get_texts():
        text.set_color(_TICK)


def _fig_to_bytes(fig) -> bytes:
    """Render figure to PNG bytes and close it."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# Chart generators
# ---------------------------------------------------------------------------

def generate_price_chart(
    prices: list[tuple[datetime, float]],
    alert_markers: Optional[list[tuple[datetime, str]]] = None,
    title: str = "WTI Crude",
) -> Optional[bytes]:
    """
    Price line with optional alert markers and a marker legend.

    Parameters
    ----------
    prices        : (utc_datetime, price) pairs ordered oldest → newest.
    alert_markers : (utc_datetime, direction) pairs where direction is
                    'bullish', 'bearish', or 'neutral'.
    title         : chart title shown at top-left.

    Returns None when fewer than 3 data points are available.
    """
    if len(prices) < 3:
        logger.debug("generate_price_chart: only %d samples — skipping", len(prices))
        return None

    times  = [p[0] for p in prices]
    values = [p[1] for p in prices]

    y_min = min(values)
    y_max = max(values)
    y_range = y_max - y_min or 1.0

    fig, ax = plt.subplots(figsize=(_WIDTH, _HEIGHT), dpi=_DPI)
    fig.patch.set_facecolor(_BG_OUTER)
    ax.set_facecolor(_BG_INNER)

    # ── Price line + gradient fill ──────────────────────────────────────────
    ax.plot(times, values, color=_LINE, linewidth=1.8, zorder=3,
            solid_capstyle="round")
    ax.fill_between(times, values, y_min - y_range * 0.02,
                    alpha=0.12, color=_LINE, zorder=2)

    # ── Alert markers — vertical dashed lines + symbol at top ───────────────
    if alert_markers:
        for ts, direction in alert_markers:
            color, sym = _MARKER.get(direction, _MARKER["neutral"])
            ax.axvline(ts, color=color, linewidth=1.4, linestyle="--",
                       alpha=0.85, zorder=4)
            ax.text(
                ts, y_max + y_range * 0.06, sym,
                ha="center", va="bottom", fontsize=11, color=color, zorder=5,
            )

    # ── Current price label ─────────────────────────────────────────────────
    current = values[-1]
    first   = values[0]
    pct     = (current - first) / first * 100
    price_color = "#66bb6a" if pct >= 0 else "#ef5350"
    ax.annotate(
        f"  ${current:.2f}  {pct:+.2f}%",
        xy=(times[-1], current),
        xytext=(0, 0),
        textcoords="offset points",
        ha="left", va="center",
        fontsize=9, color=price_color, fontweight="bold", zorder=6,
    )

    # ── Axes formatting ─────────────────────────────────────────────────────
    _apply_date_fmt(ax, times)
    fig.autofmt_xdate(rotation=0, ha="center")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.2f"))
    _style_axes(ax)
    ax.set_title(title, color=_TITLE, fontsize=10, pad=6, loc="left")
    # Extend right margin so the price annotation (ha="left" at rightmost point) isn't clipped
    x_pad = (times[-1] - times[0]) * 0.12
    ax.set_xlim(times[0], times[-1] + x_pad)
    ax.set_ylim(y_min - y_range * 0.05, y_max + y_range * 0.15)

    # ── Marker legend ────────────────────────────────────────────────────────
    if alert_markers:
        seen: set[str] = set()
        handles = []
        for _, direction in alert_markers:
            if direction not in seen:
                color, sym = _MARKER.get(direction, _MARKER["neutral"])
                handles.append(Line2D(
                    [0], [0], color=color, linewidth=1.4, linestyle="--",
                    label=f"{sym} {direction.title()}",
                ))
                seen.add(direction)
        _apply_legend(ax, handles)

    plt.tight_layout(pad=0.6)
    return _fig_to_bytes(fig)


def generate_price_narrative_chart(
    prices: list[tuple[datetime, float]],
    narratives: list[tuple[datetime, float, str]],
    alert_markers: Optional[list[tuple[datetime, str]]] = None,
    title: str = "Price vs Narrative",
) -> Optional[bytes]:
    """
    Price chart with narrative state shown as background colour regions.

    Narrative state transitions are marked with directional arrows:
      ▲▲ strong escalation   ▲ escalation   → stable
      ▼ de-escalation   ▼▼ strong de-escalation

    This lets you see directly whether shifts in the narrative correlate
    with price movements.

    Returns None if fewer than 3 price data points are available.
    """
    if len(prices) < 3:
        logger.debug("generate_price_narrative_chart: only %d price samples — skipping",
                     len(prices))
        return None

    times  = [p[0] for p in prices]
    values = [p[1] for p in prices]
    t_start = times[0]
    t_end   = times[-1]

    y_min   = min(values)
    y_max   = max(values)
    y_range = y_max - y_min or 1.0

    fig, ax = plt.subplots(figsize=(_WIDTH, _HEIGHT), dpi=_DPI)
    fig.patch.set_facecolor(_BG_OUTER)
    ax.set_facecolor(_BG_INNER)

    # ── Background state shading ─────────────────────────────────────────────
    seen_states: set[str] = set()
    legend_patches: list = []

    for i, (ts, _score, state) in enumerate(narratives):
        seg_end   = narratives[i + 1][0] if i + 1 < len(narratives) else t_end
        seg_start = max(ts, t_start)
        seg_end   = min(seg_end, t_end)
        if seg_start >= t_end:
            break
        color = _STATE_COLORS.get(state, "#888888")
        ax.axvspan(seg_start, seg_end, alpha=_STATE_BG_ALPHA, color=color, zorder=1)
        if state not in seen_states:
            label = _STATE_DISPLAY_LABELS.get(state, state.replace("_", " ").title())
            legend_patches.append(mpatches.Patch(facecolor=color, alpha=0.7, label=label))
            seen_states.add(state)

    # ── Narrative transition markers ─────────────────────────────────────────
    # Dotted vertical line + directional arrow at each state change
    for i in range(1, len(narratives)):
        prev_state = narratives[i - 1][2]
        curr_ts, _score, curr_state = narratives[i]
        if curr_state == prev_state:
            continue
        if curr_ts < t_start or curr_ts > t_end:
            continue
        color = _STATE_COLORS.get(curr_state, "#888888")
        arrow = _STATE_TRANSITION_ARROW.get(curr_state, "→")
        ax.axvline(curr_ts, color=color, linewidth=0.9, linestyle=":",
                   alpha=0.75, zorder=3)
        # Arrow sits just above the price area (lower row, below alert markers)
        ax.text(curr_ts, y_max + y_range * 0.03, arrow,
                ha="center", va="bottom", fontsize=9, color=color, zorder=5)

    # ── Price line + fill ────────────────────────────────────────────────────
    ax.plot(times, values, color=_LINE, linewidth=1.8, zorder=4,
            solid_capstyle="round")
    ax.fill_between(times, values, y_min - y_range * 0.02,
                    alpha=0.12, color=_LINE, zorder=3)

    # ── Alert markers ─────────────────────────────────────────────────────────
    if alert_markers:
        for ts, direction in alert_markers:
            if ts < t_start or ts > t_end:
                continue
            color, sym = _MARKER.get(direction, _MARKER["neutral"])
            ax.axvline(ts, color=color, linewidth=1.4, linestyle="--",
                       alpha=0.85, zorder=5)
            # Alert symbols sit above transition arrows (upper row)
            ax.text(ts, y_max + y_range * 0.10, sym,
                    ha="center", va="bottom", fontsize=11, color=color, zorder=6)

    # ── Current price label ───────────────────────────────────────────────────
    current = values[-1]
    first   = values[0]
    pct     = (current - first) / first * 100
    price_color = "#66bb6a" if pct >= 0 else "#ef5350"
    ax.annotate(
        f"  ${current:.2f}  {pct:+.2f}%",
        xy=(times[-1], current),
        xytext=(0, 0), textcoords="offset points",
        ha="left", va="center",
        fontsize=9, color=price_color, fontweight="bold", zorder=7,
    )

    # ── Axes formatting ───────────────────────────────────────────────────────
    _apply_date_fmt(ax, times)
    fig.autofmt_xdate(rotation=0, ha="center")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.2f"))
    _style_axes(ax)
    ax.set_title(title, color=_TITLE, fontsize=10, pad=6, loc="left")
    x_pad = (t_end - t_start) * 0.12
    ax.set_xlim(t_start, t_end + x_pad)
    # Extra headroom: two rows of annotations (transition arrows + alert markers)
    ax.set_ylim(y_min - y_range * 0.05, y_max + y_range * 0.22)

    # ── Legend (narrative states present in window) ───────────────────────────
    _apply_legend(ax, legend_patches)

    plt.tight_layout(pad=0.6)
    return _fig_to_bytes(fig)
