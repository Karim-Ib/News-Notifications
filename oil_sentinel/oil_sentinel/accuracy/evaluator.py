"""
Daily prediction accuracy evaluator.

Reads existing narrative_states, alerts, and market_data tables to grade
how well the system's sentiment predicted next-day WTI direction.
Pure read + insert — never modifies any existing data.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from oil_sentinel.db import (
    get_connection,
    transaction,
    insert_daily_score,
    daily_score_exists,
    get_daily_scores,
)

logger = logging.getLogger(__name__)

# Minimum price move to count as a meaningful directional day
_SKIP_THRESHOLD   = 0.3   # % — skip grading if |change| < this
_STABLE_THRESHOLD = 1.0   # % — stable is correct if |change| < this

_STATE_DISPLAY: dict[str, str] = {
    "strong_escalation":    "Strong Escalation",
    "escalation":           "Escalation",
    "stable":               "Stable",
    "de_escalation":        "De-escalation",
    "strong_de_escalation": "Strong De-escalation",
}

_DIR_SYMBOL: dict[str, str] = {
    "bullish": "\u25b2",   # ▲
    "bearish": "\u25bc",   # ▼
    "stable":  "\u25cf",   # ●
}


def _net_direction(weighted_score: float) -> str:
    if weighted_score > 0.5:
        return "bullish"
    if weighted_score < -0.5:
        return "bearish"
    return "stable"


def _grade(net_direction: str, wti_change_pct: float) -> tuple[Optional[int], Optional[str]]:
    """Return (prediction_correct, skip_reason)."""
    if abs(wti_change_pct) < _SKIP_THRESHOLD:
        return None, "insufficient_move"
    if net_direction == "bullish" and wti_change_pct > _SKIP_THRESHOLD:
        return 1, None
    if net_direction == "bearish" and wti_change_pct < -_SKIP_THRESHOLD:
        return 1, None
    if net_direction == "stable" and abs(wti_change_pct) < _STABLE_THRESHOLD:
        return 1, None
    return 0, None


def backfill_accuracy(db_path: str) -> int:
    """
    Evaluate all past dates that have both narrative state and WTI market data
    but no daily_scores entry yet.  Safe to call on every startup — already-graded
    dates are skipped via the UNIQUE constraint in insert_daily_score().

    Returns the number of new rows inserted.
    """
    conn = get_connection(db_path)
    try:
        # Dates that have a narrative state entry
        ns_dates = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT date(computed_at) FROM narrative_states"
            ).fetchall()
        }
        # Dates that have at least one WTI sample
        md_dates = {
            r[0] for r in conn.execute(
                "SELECT DISTINCT date(sampled_at) FROM market_data WHERE ticker = 'CL=F'"
            ).fetchall()
        }
        # Dates already evaluated
        done_dates = {
            r[0] for r in conn.execute(
                "SELECT date FROM daily_scores"
            ).fetchall()
        }
    finally:
        conn.close()

    candidates = sorted(ns_dates & md_dates - done_dates)
    if not candidates:
        logger.debug("backfill_accuracy: nothing to backfill")
        return 0

    logger.info("backfill_accuracy: %d date(s) to evaluate", len(candidates))
    inserted = 0
    for date_str in candidates:
        try:
            if evaluate_day(db_path, date_str):
                inserted += 1
        except Exception as exc:
            logger.warning("backfill_accuracy: error on %s — %s", date_str, exc)

    logger.info("backfill_accuracy: inserted %d / %d rows", inserted, len(candidates))
    return inserted


def evaluate_day(db_path: str, target_date: str) -> bool:
    """
    Evaluate prediction accuracy for target_date (YYYY-MM-DD UTC).
    Reads narrative_states, alerts, and market_data; inserts one daily_scores row.
    Returns True if a new row was inserted, False if already evaluated or skipped.
    Does NOT modify any existing data.
    """
    conn = get_connection(db_path)
    try:
        # Idempotent: skip if already evaluated
        if daily_score_exists(conn, target_date):
            logger.debug("evaluate_day %s: already evaluated", target_date)
            return False

        # 1. End-of-day narrative state (latest entry for that UTC date)
        ns_row = conn.execute(
            """
            SELECT state, weighted_score
            FROM narrative_states
            WHERE date(computed_at) = ?
            ORDER BY computed_at DESC LIMIT 1
            """,
            (target_date,),
        ).fetchone()

        if ns_row is None:
            logger.debug("evaluate_day %s: no narrative state — skipping", target_date)
            return False

        # 2. WTI open (first sample) and close (last sample) for that UTC date
        open_row = conn.execute(
            """
            SELECT price FROM market_data
            WHERE ticker = 'CL=F' AND date(sampled_at) = ?
            ORDER BY sampled_at ASC LIMIT 1
            """,
            (target_date,),
        ).fetchone()

        close_row = conn.execute(
            """
            SELECT price FROM market_data
            WHERE ticker = 'CL=F' AND date(sampled_at) = ?
            ORDER BY sampled_at DESC LIMIT 1
            """,
            (target_date,),
        ).fetchone()

        if open_row is None or close_row is None:
            logger.debug(
                "evaluate_day %s: no WTI market data — skipping (weekend/holiday/down)",
                target_date,
            )
            return False

        wti_open  = float(open_row["price"])
        wti_close = float(close_row["price"])
        wti_change_pct = (wti_close - wti_open) / wti_open * 100

        # 3. Alert counts by direction for that UTC date
        count_rows = conn.execute(
            """
            SELECT direction, COUNT(*) AS cnt
            FROM alerts
            WHERE date(created_at) = ?
            GROUP BY direction
            """,
            (target_date,),
        ).fetchall()
        counts = {r["direction"]: r["cnt"] for r in count_rows}

        # 4. Average magnitude across all alerts that day
        mag_row = conn.execute(
            """
            SELECT AVG(magnitude) AS avg_mag
            FROM alerts
            WHERE date(created_at) = ? AND magnitude IS NOT NULL
            """,
            (target_date,),
        ).fetchone()

        # 5. Compute grading
        weighted_score = float(ns_row["weighted_score"])
        net_dir = _net_direction(weighted_score)
        prediction_correct, skip_reason = _grade(net_dir, wti_change_pct)

        # 6. Insert
        inserted = False
        with transaction(conn):
            inserted = insert_daily_score(
                conn,
                date=target_date,
                narrative_state=str(ns_row["state"]),
                weighted_score=weighted_score,
                net_direction=net_dir,
                bull_count=counts.get("bullish", 0),
                bear_count=counts.get("bearish", 0),
                neutral_count=counts.get("neutral", 0),
                avg_magnitude=float(mag_row["avg_mag"]) if mag_row and mag_row["avg_mag"] is not None else None,
                wti_open=wti_open,
                wti_close=wti_close,
                wti_change_pct=wti_change_pct,
                prediction_correct=prediction_correct,
                skip_reason=skip_reason,
            )

        if inserted:
            result_str = skip_reason if skip_reason else ("correct" if prediction_correct else "wrong")
            logger.info(
                "Daily accuracy eval %s: %s -> WTI %+.1f%% -> %s",
                target_date, net_dir, wti_change_pct, result_str,
            )
        return inserted

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Formatting helpers for /accuracy command
# ---------------------------------------------------------------------------

def _compute_streak(graded: list) -> int:
    """
    graded is sorted newest-first (only rows where prediction_correct is not None).
    Returns +N for a correct streak of N, -N for a wrong streak of N, 0 if empty.
    """
    if not graded:
        return 0
    latest = graded[0]["prediction_correct"]
    count = 0
    for r in graded:
        if r["prediction_correct"] == latest:
            count += 1
        else:
            break
    return count if latest == 1 else -count


def _state_label(state: str) -> str:
    return _STATE_DISPLAY.get(state, state.replace("_", " ").title())


def format_accuracy_report(db_path: str, window_days: Optional[int] = 30) -> str:
    """
    Format a full accuracy report for the /accuracy Telegram command.
    window_days=None means all time.
    """
    conn = get_connection(db_path)
    try:
        rows = get_daily_scores(conn, days=window_days)
    finally:
        conn.close()

    if not rows:
        label = f"last {window_days}d" if window_days else "all time"
        return f"\U0001f4ca No accuracy data available ({label})."

    graded  = [r for r in rows if r["prediction_correct"] is not None]
    correct = sum(1 for r in graded if r["prediction_correct"] == 1)
    total   = len(graded)
    skipped = len(rows) - total

    label = f"last {window_days}d" if window_days else "all time"
    lines: list[str] = [f"\U0001f4ca <b>Prediction accuracy \u2014 {label}</b>\n"]

    # Overall accuracy
    if total >= 5:
        pct = correct / total * 100
        lines.append(f"Direction: {correct}/{total} correct ({pct:.1f}%)")
    elif total > 0:
        lines.append(f"Direction: {correct}/{total} correct (need 5+ graded days for %)")
    else:
        lines.append("Direction: No graded days yet")

    # Streak
    streak = _compute_streak(graded)
    if streak > 0:
        sym = "\u2705" * min(streak, 7)
        lines.append(f"Current streak: {sym} ({streak})")
    elif streak < 0:
        sym = "\u274c" * min(-streak, 7)
        lines.append(f"Current streak: {sym} ({-streak} wrong)")

    # By narrative state
    state_stats: dict[str, list[int]] = {}
    for r in graded:
        s = str(r["narrative_state"] or "unknown")
        if s not in state_stats:
            state_stats[s] = [0, 0]
        state_stats[s][1] += 1
        if r["prediction_correct"] == 1:
            state_stats[s][0] += 1

    if state_stats:
        lines.append("\n<b>By narrative state:</b>")
        for s, (sc, st) in sorted(state_stats.items()):
            pct_str = f" ({sc / st * 100:.1f}%)" if st > 0 else ""
            lines.append(f"  {_state_label(s)}: {sc}/{st}{pct_str}")

    # Last 7 days detail
    last7 = list(rows[:7])
    lines.append("\n<b>Last 7 days:</b>")
    for r in last7:
        try:
            d = datetime.strptime(str(r["date"]), "%Y-%m-%d")
            date_str = d.strftime("%b %d")
        except ValueError:
            date_str = str(r["date"])

        direction = str(r["net_direction"] or "stable")
        sym = _DIR_SYMBOL.get(direction, "\u25cf")
        dir_label = direction.ljust(7)

        chg = r["wti_change_pct"]
        chg_str = f"WTI {chg:+.1f}%" if chg is not None else "WTI n/a"

        if r["skip_reason"]:
            result = "\u26aa"   # ⚪
        elif r["prediction_correct"] == 1:
            result = "\u2705"   # ✅
        else:
            result = "\u274c"   # ❌

        lines.append(f"  {date_str}: {sym} {dir_label} \u2192 {chg_str} {result}")

    if skipped:
        lines.append(f"\nSkipped: {skipped} days (insufficient market movement)")

    return "\n".join(lines)


def format_accuracy_oneliner(db_path: str) -> Optional[str]:
    """
    Returns a one-liner for the morning summary, e.g.:
      '📊 Accuracy: 69.2% (18/26) last 30d | streak: ✅✅✅'
    Returns None if fewer than 5 graded days exist.
    """
    conn = get_connection(db_path)
    try:
        rows = get_daily_scores(conn, days=30)
    finally:
        conn.close()

    graded  = [r for r in rows if r["prediction_correct"] is not None]
    if len(graded) < 5:
        return None

    correct = sum(1 for r in graded if r["prediction_correct"] == 1)
    total   = len(graded)
    pct     = correct / total * 100

    streak = _compute_streak(graded)
    if streak > 0:
        streak_str = "\u2705" * min(streak, 5)
    elif streak < 0:
        streak_str = "\u274c" * min(-streak, 5)
    else:
        streak_str = "\u26aa"

    return f"\U0001f4ca Accuracy: {pct:.1f}% ({correct}/{total}) last 30d | streak: {streak_str}"
