"""
Portfolio tracker: ETP price fetching with 5-minute cache, position calculation,
hourly snapshot taking, and statistics.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import yfinance as yf

from oil_sentinel.db import (
    get_active_portfolios,
    get_connection,
    get_last_portfolio_snapshot,
    get_portfolio_snapshots,
    get_transactions,
    insert_portfolio_snapshot,
    transaction,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Product → ETP ticker mapping
# ---------------------------------------------------------------------------

# (primary Milan EUR, fallback London GBP)
PRODUCT_TICKERS: dict[str, tuple[str, str]] = {
    "long":  ("3OIL.MI", "3OIL.L"),
    "short": ("3OIS.MI", "3OIS.L"),
}

PRODUCT_NAMES: dict[str, str] = {
    "long":  "WTI 3x Daily Long",
    "short": "WTI 3x Daily Short",
}

# ---------------------------------------------------------------------------
# Price cache: ticker_primary → (price, fetched_at, used_ticker, is_stale)
# ---------------------------------------------------------------------------

_CACHE_TTL = 300  # 5 minutes

_price_cache: dict[str, dict] = {}
# Format: { "3OIS.MI": {"price": 3.12, "fetched_at": datetime, "used_ticker": "3OIS.MI"} }


def _yf_fetch(ticker: str) -> Optional[float]:
    """Fetch latest price via yfinance. Returns None on any error."""
    try:
        tk = yf.Ticker(ticker)
        price = tk.fast_info.get("last_price") or tk.fast_info.get("previous_close")
        if price is None:
            hist = tk.history(period="1d", interval="1m")
            if hist.empty:
                return None
            price = float(hist["Close"].iloc[-1])
        return float(price)
    except Exception as exc:
        logger.debug("yfinance error for %s: %s", ticker, exc)
        return None


def fetch_etp_price(
    ticker_primary: str, ticker_fallback: str
) -> tuple[Optional[float], str, bool, Optional[datetime]]:
    """
    Fetch ETP price with 5-minute cache.

    Returns:
        (price, used_ticker, is_stale, fetched_at)
        is_stale = True if serving an expired cache entry (all fetches failed)
    """
    now = datetime.now(timezone.utc)

    # Check live cache
    cached = _price_cache.get(ticker_primary)
    if cached:
        age = (now - cached["fetched_at"]).total_seconds()
        if age < _CACHE_TTL:
            return cached["price"], cached["used_ticker"], False, cached["fetched_at"]

    # Try primary (Milan, EUR)
    price = _yf_fetch(ticker_primary)
    if price is not None:
        _price_cache[ticker_primary] = {
            "price": price, "fetched_at": now, "used_ticker": ticker_primary
        }
        return price, ticker_primary, False, now

    # Try fallback (London, GBP)
    logger.warning("ETP primary %s unavailable, trying fallback %s", ticker_primary, ticker_fallback)
    price = _yf_fetch(ticker_fallback)
    if price is not None:
        _price_cache[ticker_primary] = {
            "price": price, "fetched_at": now, "used_ticker": ticker_fallback
        }
        return price, ticker_fallback, False, now

    # Return stale cache if available
    if cached:
        logger.warning("All ETP fetches failed for %s — serving stale cache", ticker_primary)
        return cached["price"], cached["used_ticker"], True, cached["fetched_at"]

    return None, ticker_primary, False, None


# ---------------------------------------------------------------------------
# Portfolio position calculation
# ---------------------------------------------------------------------------

def get_portfolio_position(conn, portfolio_id: int) -> dict:
    """
    Calculate current portfolio position from all transactions.

    Returns dict with:
        total_units, total_invested, total_withdrawn, net_invested,
        avg_cost, total_buy_units, buy_count, sell_count,
        buy_prices, lowest_buy, highest_buy
    """
    rows = get_transactions(conn, portfolio_id)

    total_units = 0.0
    total_invested = 0.0     # sum of all buy amounts
    total_withdrawn = 0.0    # sum of all sell amounts
    total_buy_units = 0.0
    buy_prices: list[float] = []
    buy_count = 0
    sell_count = 0

    for row in rows:
        if row["action"] == "buy":
            total_units += row["units"]
            total_buy_units += row["units"]
            total_invested += row["amount_eur"]
            buy_prices.append(row["price_per_unit"])
            buy_count += 1
        elif row["action"] == "sell":
            total_units -= row["units"]
            total_withdrawn += row["amount_eur"]
            sell_count += 1

    # Clamp to zero (shouldn't go negative, but guard against float drift)
    total_units = max(0.0, total_units)
    net_invested = total_invested - total_withdrawn
    avg_cost = total_invested / total_buy_units if total_buy_units > 0 else 0.0

    return {
        "total_units":    total_units,
        "total_invested": total_invested,
        "total_withdrawn": total_withdrawn,
        "net_invested":   net_invested,
        "avg_cost":       avg_cost,
        "total_buy_units": total_buy_units,
        "buy_count":      buy_count,
        "sell_count":     sell_count,
        "buy_prices":     buy_prices,
        "lowest_buy":     min(buy_prices) if buy_prices else None,
        "highest_buy":    max(buy_prices) if buy_prices else None,
    }


# ---------------------------------------------------------------------------
# Portfolio statistics (from snapshots)
# ---------------------------------------------------------------------------

def get_portfolio_stats(conn, portfolio_id: int, current_price: float) -> dict:
    """
    Compute detailed statistics for a portfolio.

    Best/worst day and max drawdown are derived from hourly snapshots.
    Returns None for those fields if insufficient snapshot history.
    """
    pos = get_portfolio_position(conn, portfolio_id)

    # Current value & P/L
    current_value = pos["total_units"] * current_price
    net_inv = pos["net_invested"]
    pnl_eur = current_value - net_inv
    pnl_pct = (pnl_eur / net_inv * 100) if net_inv > 0 else 0.0

    # Snapshot-based metrics
    snapshots = get_portfolio_snapshots(conn, portfolio_id)
    best_day: Optional[dict] = None
    worst_day: Optional[dict] = None
    max_drawdown: Optional[dict] = None

    if len(snapshots) >= 2:
        # Group snapshots by date; pick last snapshot per day
        by_date: dict[str, dict] = {}
        for s in snapshots:
            date_key = s["timestamp"][:10]
            by_date[date_key] = dict(s)

        daily = sorted(by_date.values(), key=lambda x: x["timestamp"])

        if len(daily) >= 2:
            # Best/worst day: max/min single-day change
            best_pnl_eur: Optional[float] = None
            worst_pnl_eur: Optional[float] = None

            for i in range(1, len(daily)):
                day_val_prev = daily[i - 1]["total_value"]
                day_val_curr = daily[i]["total_value"]
                day_delta = day_val_curr - day_val_prev
                day_pct = (day_delta / day_val_prev * 100) if day_val_prev else 0.0

                if best_pnl_eur is None or day_delta > best_pnl_eur:
                    best_pnl_eur = day_delta
                    best_day = {
                        "date": daily[i]["timestamp"][:10],
                        "pnl_eur": day_delta,
                        "pnl_pct": day_pct,
                    }
                if worst_pnl_eur is None or day_delta < worst_pnl_eur:
                    worst_pnl_eur = day_delta
                    worst_day = {
                        "date": daily[i]["timestamp"][:10],
                        "pnl_eur": day_delta,
                        "pnl_pct": day_pct,
                    }

        # Max drawdown (from all hourly snapshots)
        peak = -float("inf")
        max_dd_eur: Optional[float] = None
        max_dd_date = None
        for s in snapshots:
            v = s["total_value"]
            if v > peak:
                peak = v
            if peak > 0:
                drawdown = v - peak
                if max_dd_eur is None or drawdown < max_dd_eur:
                    max_dd_eur = drawdown
                    max_dd_date = s["timestamp"][:10]

        if max_dd_eur is not None and peak > 0:
            max_drawdown = {
                "date": max_dd_date,
                "pnl_eur": max_dd_eur,
                "pnl_pct": (max_dd_eur / peak * 100),
            }

    # Portfolio creation date
    first_tx = get_transactions(conn, portfolio_id)
    active_since = first_tx[0]["timestamp"][:10] if first_tx else "unknown"
    days_active = 0
    if first_tx:
        try:
            first_date = datetime.fromisoformat(first_tx[0]["timestamp"]).replace(tzinfo=timezone.utc)
            days_active = max(0, (datetime.now(timezone.utc) - first_date).days)
        except Exception:
            pass

    # Lowest/highest buy with timestamps
    buy_txs = [dict(r) for r in first_tx if r["action"] == "buy"]
    lowest_buy_tx = min(buy_txs, key=lambda x: x["price_per_unit"]) if buy_txs else None
    highest_buy_tx = max(buy_txs, key=lambda x: x["price_per_unit"]) if buy_txs else None

    return {
        "position":      pos,
        "current_price": current_price,
        "current_value": current_value,
        "pnl_eur":       pnl_eur,
        "pnl_pct":       pnl_pct,
        "active_since":  active_since,
        "days_active":   days_active,
        "best_day":      best_day,
        "worst_day":     worst_day,
        "max_drawdown":  max_drawdown,
        "lowest_buy_tx": lowest_buy_tx,
        "highest_buy_tx": highest_buy_tx,
        "snapshot_count": len(snapshots),
    }


# ---------------------------------------------------------------------------
# Hourly snapshot-taking
# ---------------------------------------------------------------------------

def take_all_portfolio_snapshots(db_path: str) -> int:
    """
    Take hourly snapshots for all active portfolios that hold units.
    Returns count of snapshots taken.
    """
    conn = get_connection(db_path)
    taken = 0
    try:
        portfolios = get_active_portfolios(conn)
        for p in portfolios:
            p = dict(p)
            pos = get_portfolio_position(conn, p["id"])
            if pos["total_units"] <= 0:
                continue

            # Determine ticker pair from product
            ticker_primary, ticker_fallback = PRODUCT_TICKERS.get(
                p["product"], ("3OIL.MI", "3OIL.L")
            )
            # Use stored ticker if different from product default
            ticker_primary = p["ticker"]
            ticker_fallback = PRODUCT_TICKERS.get(p["product"], (p["ticker"], p["ticker"]))[1]

            price, _used, is_stale, _ts = fetch_etp_price(ticker_primary, ticker_fallback)
            if price is None:
                logger.warning("Snapshot skipped for portfolio %s — no price available", p["name"])
                continue

            total_value = pos["total_units"] * price
            net_inv = pos["net_invested"]
            pnl_eur = total_value - net_inv
            pnl_pct = (pnl_eur / net_inv * 100) if net_inv > 0 else 0.0

            with transaction(conn):
                insert_portfolio_snapshot(
                    conn,
                    portfolio_id=p["id"],
                    unit_price=price,
                    total_units=pos["total_units"],
                    total_value=total_value,
                    total_invested=net_inv,
                    pnl_eur=pnl_eur,
                    pnl_pct=pnl_pct,
                )
            taken += 1
            logger.info(
                "Snapshot: portfolio %s — %.4f units @ €%.4f = €%.2f (P/L €%+.2f)",
                p["name"], pos["total_units"], price, total_value, pnl_eur,
            )
    finally:
        conn.close()
    return taken


# ---------------------------------------------------------------------------
# Morning briefing helper
# ---------------------------------------------------------------------------

def format_portfolio_morning_lines(db_path: str) -> str:
    """
    Build portfolio summary lines for the morning briefing.
    Uses last snapshot price (no live yfinance call needed).
    Returns empty string if no portfolios with holdings exist.
    """
    conn = get_connection(db_path)
    try:
        portfolios = get_active_portfolios(conn)
        lines: list[str] = []
        for p in portfolios:
            p = dict(p)
            pos = get_portfolio_position(conn, p["id"])
            if pos["total_units"] <= 0:
                continue

            # Use last snapshot for price
            snap = get_last_portfolio_snapshot(conn, p["id"])
            if snap is None:
                continue

            pnl_sign = "+" if snap["pnl_pct"] >= 0 else ""
            lines.append(
                f"💼 <b>{p['name']}</b>: €{snap['total_value']:.2f} "
                f"({pnl_sign}{snap['pnl_pct']:.1f}%) | "
                f"{pos['total_units']:.4f} units @ avg €{pos['avg_cost']:.4f}"
            )
        return "\n".join(lines)
    finally:
        conn.close()
