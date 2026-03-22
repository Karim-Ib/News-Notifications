"""
WTI price and narrative sentiment chart generators.

Two separate charts, each returned as PNG bytes:

  generate_price_chart          — price line + alert markers + legend
  generate_price_narrative_chart — price + narrative state shading + transitions

Color conventions:
  Bullish marker  : red  (#ef5350) ▲ — supply risk, price up
  Bearish marker  : green (#66bb6a) ▼ — supply relief, price down
  Anomaly marker  : amber (#ffa726) ● — market spike without directional label

Layout notes:
  - All overlay text (markers, price label) uses axes-fraction or blended
    transforms so positioning is independent of the data y-scale.
  - constrained_layout=True handles margins automatically; tight_layout()
    is not called so it cannot fight with bbox_inches="tight" at save time.
  - Minimum y-range of $0.50 prevents a flat price period from producing an
    unreadable chart where the line fills the entire plot area.
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
_DPI    = 150   # 1500 × 600 px — sharper on mobile screens

_BG_OUTER = "#0f0f1a"
_BG_INNER = "#131320"
_GRID     = "#1e1e3a"
_LINE     = "#4fc3f7"
_TICK     = "#7777aa"
_SPINE    = "#2a2a4a"
_TITLE    = "#aaaacc"

_MARKER: dict[str, tuple[str, str]] = {
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


def _apply_legend(ax, handles: list, loc: str = "upper left") -> None:
    """Attach a styled legend to an axes object."""
    if not handles:
        return
    leg = ax.legend(
        handles=handles,
        loc=loc,
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


def _y_limits(values: list[float], pad_frac: float = 0.07) -> tuple[float, float]:
    """
    Compute y-axis limits with symmetric padding.

    Enforces a minimum visible range of $0.50 so flat-price charts
    (e.g. outside trading hours) don't collapse to a single horizontal line.
    Padding is the larger of pad_frac of the range or $0.10 absolute.
    """
    y_min = min(values)
    y_max = max(values)
    y_range = max(y_max - y_min, 0.50)
    pad = max(y_range * pad_frac, 0.10)
    return y_min - pad, y_max + pad


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
    y_lo, y_hi = _y_limits(values)
    span = times[-1] - times[0]

    # constrained_layout handles all margin/padding automatically and is
    # compatible with bbox_inches="tight" at save time — no tight_layout() needed.
    fig, ax = plt.subplots(
        figsize=(_WIDTH, _HEIGHT), dpi=_DPI, constrained_layout=True
    )
    fig.patch.set_facecolor(_BG_OUTER)
    ax.set_facecolor(_BG_INNER)

    # ── Price line + gradient fill ──────────────────────────────────────────
    ax.plot(times, values, color=_LINE, linewidth=1.8, zorder=3,
            solid_capstyle="round")
    ax.fill_between(times, values, y_lo, alpha=0.12, color=_LINE, zorder=2)

    # ── Alert markers ───────────────────────────────────────────────────────
    # Blended transform: x = data coordinates, y = axes fraction (0=bottom, 1=top).
    # Symbols are pinned to 96% of the axes height regardless of y-scale,
    # so they are always inside the plot area and never clipped.
    if alert_markers:
        xaxis_trans = ax.get_xaxis_transform()
        for ts, direction in alert_markers:
            color, sym = _MARKER.get(direction, _MARKER["neutral"])
            ax.axvline(ts, color=color, linewidth=1.4, linestyle="--",
                       alpha=0.85, zorder=4)
            ax.text(
                ts, 0.96, sym,
                transform=xaxis_trans,
                ha="center", va="top",
                fontsize=11, color=color, zorder=5,
                clip_on=False,
            )

    # ── Current price label ─────────────────────────────────────────────────
    # Fixed axes-fraction position (upper-right) — avoids right-margin
    # clipping entirely and stays readable regardless of x-scale.
    current = values[-1]
    pct     = (current - values[0]) / values[0] * 100
    price_color = "#66bb6a" if pct >= 0 else "#ef5350"
    ax.text(
        0.99, 0.97,
        f"${current:.2f}  {pct:+.2f}%",
        transform=ax.transAxes,
        ha="right", va="top",
        fontsize=9, color=price_color, fontweight="bold", zorder=6,
    )

    # ── Axes limits ─────────────────────────────────────────────────────────
    # 1 % right margin so the last data point isn't flush with the spine.
    ax.set_xlim(times[0], times[-1] + span * 0.01)
    ax.set_ylim(y_lo, y_hi)

    # ── Axes formatting ─────────────────────────────────────────────────────
    _apply_date_fmt(ax, times)
    fig.autofmt_xdate(rotation=30, ha="right")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.2f"))
    _style_axes(ax)
    ax.set_title(title, color=_TITLE, fontsize=10, pad=6, loc="left")

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

    return _fig_to_bytes(fig)


def generate_price_narrative_chart(
    prices: list[tuple[datetime, float]],
    narratives: list[tuple[datetime, float, str]],
    alert_markers: Optional[list[tuple[datetime, str]]] = None,  # kept for API compat, unused
    title: str = "Price vs Narrative",
) -> Optional[bytes]:
    """
    Price chart with narrative state shown as background colour bands.

    Each band covers the period a state was active; colour changes at the
    exact transition point, so shifts in narrative are visible as band edges.
    No vertical lines or symbol overlays — the bands do the work.

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
    span    = t_end - t_start
    y_lo, y_hi = _y_limits(values)

    fig, ax = plt.subplots(
        figsize=(_WIDTH, _HEIGHT), dpi=_DPI, constrained_layout=True
    )
    fig.patch.set_facecolor(_BG_OUTER)
    ax.set_facecolor(_BG_INNER)

    # ── Background state bands ───────────────────────────────────────────────
    # Each band spans the period the state was active. Transition is implicit
    # at the colour boundary — no additional lines or markers needed.
    seen_states: set[str] = set()
    legend_patches: list = []

    for i, (ts, _score, state) in enumerate(narratives):
        seg_start = max(ts, t_start)
        seg_end   = min(narratives[i + 1][0] if i + 1 < len(narratives) else t_end, t_end)
        if seg_start >= t_end:
            break
        color = _STATE_COLORS.get(state, "#888888")
        ax.axvspan(seg_start, seg_end, alpha=_STATE_BG_ALPHA, color=color, zorder=1)
        if state not in seen_states:
            label = _STATE_DISPLAY_LABELS.get(state, state.replace("_", " ").title())
            legend_patches.append(mpatches.Patch(facecolor=color, alpha=0.7, label=label))
            seen_states.add(state)

    # ── Price line + fill ────────────────────────────────────────────────────
    ax.fill_between(times, values, y_lo, alpha=0.12, color=_LINE, zorder=2)
    ax.plot(times, values, color=_LINE, linewidth=1.8, zorder=3,
            solid_capstyle="round")

    # ── Current price label (upper-right, axes fraction) ─────────────────────
    current = values[-1]
    pct     = (current - values[0]) / values[0] * 100
    price_color = "#66bb6a" if pct >= 0 else "#ef5350"
    ax.text(
        0.99, 0.97,
        f"${current:.2f}  {pct:+.2f}%",
        transform=ax.transAxes,
        ha="right", va="top",
        fontsize=9, color=price_color, fontweight="bold", zorder=4,
    )

    # ── Axes limits ───────────────────────────────────────────────────────────
    ax.set_xlim(t_start, t_end + span * 0.01)
    ax.set_ylim(y_lo, y_hi)

    # ── Axes formatting ───────────────────────────────────────────────────────
    _apply_date_fmt(ax, times)
    fig.autofmt_xdate(rotation=30, ha="right")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.2f"))
    _style_axes(ax)
    ax.set_title(title, color=_TITLE, fontsize=10, pad=6, loc="left")

    # ── Legend (narrative states present in window) ───────────────────────────
    # lower-left keeps it away from the price label at upper-right.
    _apply_legend(ax, legend_patches, loc="lower left")

    return _fig_to_bytes(fig)
