"""
Portfolio value chart generator.

Shows two lines:
  - Portfolio value (blue)     — what it's worth now
  - Total invested (gray dash) — what was put in

Buy/sell transactions are marked with triangles:
  ▲ green — buy
  ▼ red   — sell
"""

import io
import logging
from datetime import datetime, timezone
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D

logger = logging.getLogger(__name__)

_WIDTH  = 10
_HEIGHT = 4
_DPI    = 100

_BG_OUTER = "#0f0f1a"
_BG_INNER = "#131320"
_GRID     = "#1e1e3a"
_LINE_VAL = "#4fc3f7"   # blue  — portfolio value
_LINE_INV = "#888899"   # gray  — invested baseline
_TICK     = "#7777aa"
_SPINE    = "#2a2a4a"
_TITLE    = "#aaaacc"

_BUY_COLOR  = "#66bb6a"   # green
_SELL_COLOR = "#ef5350"   # red


def _style_axes(ax) -> None:
    for spine in ax.spines.values():
        spine.set_color(_SPINE)
    ax.tick_params(colors=_TICK, labelsize=8)
    ax.grid(color=_GRID, linestyle="-", linewidth=0.7, alpha=0.9)


def _apply_date_fmt(ax, times: list) -> None:
    if len(times) < 2:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        return
    span_hours = (times[-1] - times[0]).total_seconds() / 3600
    if span_hours <= 48:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    elif span_hours <= 240:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    elif span_hours <= 720:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))


def _fig_to_bytes(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _parse_ts(ts_str: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def generate_portfolio_chart(
    snapshots: list,
    transactions: list,
    title: str = "Portfolio Value",
) -> Optional[bytes]:
    """
    Generate portfolio value chart from snapshot and transaction rows.

    Parameters
    ----------
    snapshots    : list of portfolio_snapshots rows (sqlite3.Row or dict)
    transactions : list of transactions rows (sqlite3.Row or dict)
    title        : chart title

    Returns None if fewer than 2 snapshots.
    """
    if len(snapshots) < 2:
        logger.debug("generate_portfolio_chart: only %d snapshots — skipping", len(snapshots))
        return None

    times:    list[datetime] = []
    values:   list[float]    = []
    invested: list[float]    = []

    for s in snapshots:
        ts = _parse_ts(s["timestamp"])
        if ts is None:
            continue
        times.append(ts)
        values.append(float(s["total_value"]))
        invested.append(float(s["total_invested"]))

    if len(times) < 2:
        return None

    y_all  = values + invested
    y_min  = min(y_all)
    y_max  = max(y_all)
    y_range = y_max - y_min or 1.0

    fig, ax = plt.subplots(figsize=(_WIDTH, _HEIGHT), dpi=_DPI)
    fig.patch.set_facecolor(_BG_OUTER)
    ax.set_facecolor(_BG_INNER)

    # Portfolio value line + fill
    ax.plot(times, values, color=_LINE_VAL, linewidth=1.8, zorder=3,
            solid_capstyle="round", label="Portfolio value")
    ax.fill_between(times, values, y_min - y_range * 0.02,
                    alpha=0.10, color=_LINE_VAL, zorder=2)

    # Total invested dashed line
    ax.plot(times, invested, color=_LINE_INV, linewidth=1.4,
            linestyle="--", zorder=3, label="Total invested")

    # Transaction markers
    t_start = times[0]
    t_end   = times[-1]

    for tx in transactions:
        ts = _parse_ts(tx["timestamp"])
        if ts is None or ts < t_start or ts > t_end:
            continue
        action = tx["action"]
        color  = _BUY_COLOR if action == "buy" else _SELL_COLOR
        sym    = "▲" if action == "buy" else "▼"
        ax.axvline(ts, color=color, linewidth=1.2, linestyle=":", alpha=0.7, zorder=4)
        ax.text(
            ts, y_max + y_range * 0.06, sym,
            ha="center", va="bottom", fontsize=10, color=color, zorder=5,
        )

    # Current value annotation
    current = values[-1]
    first   = invested[0] if invested else values[0]
    pnl     = current - first
    pnl_pct = (pnl / first * 100) if first else 0.0
    val_color = _BUY_COLOR if pnl >= 0 else _SELL_COLOR
    ax.annotate(
        f"  €{current:.2f}  {'+' if pnl >= 0 else ''}{pnl_pct:.1f}%",
        xy=(times[-1], current),
        xytext=(0, 0), textcoords="offset points",
        ha="left", va="center",
        fontsize=9, color=val_color, fontweight="bold", zorder=6,
    )

    # Axes
    _apply_date_fmt(ax, times)
    fig.autofmt_xdate(rotation=0, ha="center")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("€%.2f"))
    _style_axes(ax)
    ax.set_title(title, color=_TITLE, fontsize=10, pad=6, loc="left")
    x_pad = (times[-1] - times[0]) * 0.12
    ax.set_xlim(times[0], times[-1] + x_pad)
    ax.set_ylim(y_min - y_range * 0.05, y_max + y_range * 0.18)

    # Legend
    legend_handles = [
        Line2D([0], [0], color=_LINE_VAL, linewidth=1.8, label="Portfolio value"),
        Line2D([0], [0], color=_LINE_INV, linewidth=1.4, linestyle="--", label="Total invested"),
    ]
    if any(tx["action"] == "buy" for tx in transactions):
        legend_handles.append(
            Line2D([0], [0], color=_BUY_COLOR, linewidth=0, marker="^",
                   markersize=8, label="▲ Buy")
        )
    if any(tx["action"] == "sell" for tx in transactions):
        legend_handles.append(
            Line2D([0], [0], color=_SELL_COLOR, linewidth=0, marker="v",
                   markersize=8, label="▼ Sell")
        )

    leg = ax.legend(
        handles=legend_handles,
        loc="upper left", fontsize=7, framealpha=0.9,
        borderpad=0.5, ncol=min(len(legend_handles), 4),
    )
    leg.get_frame().set_facecolor(_BG_OUTER)
    leg.get_frame().set_edgecolor(_SPINE)
    for text in leg.get_texts():
        text.set_color(_TICK)

    plt.tight_layout(pad=0.6)
    return _fig_to_bytes(fig)
