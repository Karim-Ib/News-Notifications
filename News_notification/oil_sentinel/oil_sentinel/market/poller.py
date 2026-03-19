"""
Poll yfinance for WTI (CL=F) and Brent (BZ=F) prices every 5 minutes.
Computes rolling z-score and flags anomalies.
"""

import logging
import math
from typing import Optional

import yfinance as yf

from oil_sentinel.db import (
    get_connection,
    get_recent_prices,
    insert_market_sample,
    latest_market_sample,
    transaction,
)

logger = logging.getLogger(__name__)

DEFAULT_TICKERS = ["CL=F", "BZ=F"]


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------

def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _stdev(values: list[float], mean: Optional[float] = None) -> float:
    if len(values) < 2:
        return 0.0
    m = mean if mean is not None else _mean(values)
    variance = sum((v - m) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def compute_zscore(series: list[float], current: float) -> Optional[float]:
    """
    Compute z-score of `current` against the rolling `series`.
    Returns None if series is too short to be meaningful (< 5 samples).
    """
    if len(series) < 5:
        return None
    m = _mean(series)
    s = _stdev(series, m)
    if s == 0.0:
        return 0.0
    return (current - m) / s


# ---------------------------------------------------------------------------
# Fetch current price
# ---------------------------------------------------------------------------

def fetch_price(ticker: str) -> Optional[float]:
    """
    Fetch the latest close/current price for a ticker via yfinance.
    Returns None on failure.
    """
    try:
        tk = yf.Ticker(ticker)
        price = tk.fast_info.get("last_price") or tk.fast_info.get("previous_close")
        if price is None:
            hist = tk.history(period="1d", interval="1m")
            if hist.empty:
                logger.warning("No price data for %s", ticker)
                return None
            price = float(hist["Close"].iloc[-1])
        return float(price)
    except Exception as exc:
        logger.error("yfinance error for %s: %s", ticker, exc)
        return None


# ---------------------------------------------------------------------------
# Main polling function
# ---------------------------------------------------------------------------

def poll_and_store(
    db_path: str,
    *,
    tickers: list[str] = DEFAULT_TICKERS,
    zscore_window: int = 288,
    zscore_threshold: float = 2.0,
) -> dict[str, dict]:
    """
    Fetch current prices for each ticker, compute z-scores, persist to DB.
    Returns a dict of {ticker: {price, change_pct, zscore, is_anomaly}}.
    """
    conn = get_connection(db_path)
    results = {}

    try:
        for ticker in tickers:
            price = fetch_price(ticker)
            if price is None:
                continue

            prev_row = latest_market_sample(conn, ticker)
            prev_price = prev_row["price"] if prev_row else None
            change_pct = ((price - prev_price) / prev_price * 100) if prev_price else None

            history = get_recent_prices(conn, ticker, limit=zscore_window)
            zscore = compute_zscore(history, price)
            is_anomaly = (zscore is not None and abs(zscore) >= zscore_threshold)

            with transaction(conn):
                insert_market_sample(
                    conn,
                    ticker=ticker,
                    price=price,
                    change_pct=change_pct,
                    zscore=zscore,
                    is_anomaly=is_anomaly,
                )

            results[ticker] = {
                "price": price,
                "change_pct": change_pct,
                "zscore": zscore,
                "is_anomaly": is_anomaly,
            }

            flag = " [ANOMALY]" if is_anomaly else ""
            logger.info(
                "%s: $%.2f  chg=%.2f%%  z=%.2f%s",
                ticker,
                price,
                change_pct or 0.0,
                zscore or 0.0,
                flag,
            )
    finally:
        conn.close()

    return results


def any_anomaly(poll_results: dict[str, dict]) -> bool:
    """Return True if any ticker in the latest poll result is anomalous."""
    return any(v.get("is_anomaly") for v in poll_results.values())
