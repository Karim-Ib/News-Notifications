"""
Situation-report–based deduplication filter.

Maintains a living document (situation report) describing what the system
already knows.  Before each article reaches the Gemini scoring pipeline,
it is compared against the current situation report.  Only genuinely new
information passes through; duplicate coverage is logged and skipped.

Flow
----
  GDELT poll → store article → sitrep dedup check
    → if NEW  → append new_information to report section → score with Gemini → alert
    → if DUP  → mark article skipped (scored=2), no Gemini scoring call

Report structure
----------------
  SITUATION REPORT — Last updated: <timestamp> UTC

  MILITARY: ...
  HORMUZ STATUS: ...
  DIPLOMATIC: ...
  SUPPLY & ROUTING: ...
  MARKET CONTEXT: ...
  REGIONAL IMPACT: ...

Compaction
----------
  When len(content) > COMPACTION_THRESHOLD (12 000 chars), a Gemini call
  compacts the report to ~COMPACTION_TARGET (6 000 chars), preserving
  structure, timestamps, and key facts.
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from google import genai
from google.genai import types

from oil_sentinel.db import get_connection, get_current_sitrep, insert_sitrep_row, transaction

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COMPACTION_THRESHOLD = 12_000   # characters — trigger compaction above this
COMPACTION_TARGET    =  6_000   # characters — aim for this after compaction

SECTION_HEADERS: dict[str, str] = {
    "military":   "MILITARY:",
    "hormuz":     "HORMUZ STATUS:",
    "diplomatic": "DIPLOMATIC:",
    "supply":     "SUPPLY & ROUTING:",
    "market":     "MARKET CONTEXT:",
    "regional":   "REGIONAL IMPACT:",
}

SEED_TEMPLATE = (
    "SITUATION REPORT — Last updated: {timestamp} UTC\n\n"
    "MILITARY: No data yet.\n\n"
    "HORMUZ STATUS: No data yet.\n\n"
    "DIPLOMATIC: No data yet.\n\n"
    "SUPPLY & ROUTING: No data yet.\n\n"
    "MARKET CONTEXT: {market_line}\n\n"
    "REGIONAL IMPACT: No data yet."
)

# ---------------------------------------------------------------------------
# Module-level running stats (reset hourly by the caller)
# ---------------------------------------------------------------------------

_stats: dict = {"new": 0, "dup": 0, "window_start": datetime.now(timezone.utc)}


def get_stats_snapshot() -> dict:
    """Return current stats dict (new, dup, window_start). Does not reset."""
    return dict(_stats)


def reset_stats() -> dict:
    """Return current stats and reset counters. Used for hourly logging."""
    global _stats
    snap = dict(_stats)
    _stats = {"new": 0, "dup": 0, "window_start": datetime.now(timezone.utc)}
    return snap


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def initialize_sitrep(db_path: str, wti_price: Optional[float] = None,
                      brent_price: Optional[float] = None) -> None:
    """
    Create a seed situation report if none exists.
    Safe to call on every startup — no-op when a report already exists.
    """
    conn = get_connection(db_path)
    try:
        if get_current_sitrep(conn) is not None:
            return  # already initialized

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        if wti_price and brent_price:
            market_line = f"WTI ${wti_price:.2f}, Brent ${brent_price:.2f}."
        elif wti_price:
            market_line = f"WTI ${wti_price:.2f}."
        else:
            market_line = "No price data yet."

        content = SEED_TEMPLATE.format(timestamp=now, market_line=market_line)
        with transaction(conn):
            insert_sitrep_row(conn, content=content)
        logger.info("SitRep: initialized seed report (v1)")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Internal helpers: text manipulation
# ---------------------------------------------------------------------------

def _update_timestamp(content: str, new_ts: str) -> str:
    """Replace the Last updated line in the report header."""
    return re.sub(
        r"(SITUATION REPORT — Last updated: )[^\n]+",
        rf"\g<1>{new_ts} UTC",
        content,
    )


def _append_to_section(content: str, section: str, entry: str) -> str:
    """
    Append `entry` to the end of the named section, before the next section
    header (or end of string).  Returns the updated content.
    """
    header = SECTION_HEADERS.get(section)
    if not header:
        return content.rstrip() + f"\n\n{entry}"

    idx = content.find(header)
    if idx == -1:
        return content.rstrip() + f"\n\n{entry}"

    # Find where the next section starts (so we insert inside this section)
    next_idx = len(content)
    for h in SECTION_HEADERS.values():
        if h == header:
            continue
        pos = content.find(h, idx + len(header))
        if pos != -1 and pos < next_idx:
            next_idx = pos

    section_block = content[idx:next_idx].rstrip()
    return content[:idx] + section_block + f"\n{entry}\n\n" + content[next_idx:]


# ---------------------------------------------------------------------------
# DB helpers (operate on an open connection)
# ---------------------------------------------------------------------------

def _do_append_to_sitrep(conn, current_row, section: str, new_info: str) -> str:
    """
    Append new_info to the current report and persist as a new version.
    Must be called inside a transaction.  Returns new content string.
    """
    ts = datetime.now(timezone.utc).strftime("%b %d %H:%M")
    entry = f"[{ts}] {new_info}"
    new_content = _append_to_section(current_row["content"], section, entry)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    new_content = _update_timestamp(new_content, now_str)
    insert_sitrep_row(conn, content=new_content, previous_id=current_row["id"])
    return new_content


# ---------------------------------------------------------------------------
# Gemini: dedup check
# ---------------------------------------------------------------------------

_DEDUP_SYSTEM = (
    "You are a news deduplication filter for an oil market intelligence system. "
    "Respond ONLY with a valid JSON object — no markdown, no explanation."
)

_DEDUP_TEMPLATE = """\
You are a news deduplication filter. Compare this article against \
the current situation report below.

SITUATION REPORT:
{report}

NEW ARTICLE:
Title: {title}
Source: {source}
Body: {body}

Respond with ONLY a JSON object:
{{
  "is_new": true/false,
  "new_information": "specific new fact not in the report" or null,
  "section": "military|hormuz|diplomatic|supply|market|regional" or null,
  "reasoning": "one sentence why this is/isn't new"
}}

Rules:
- An article is NEW only if it contains specific facts, events, or \
developments NOT already covered in the situation report
- Evolution of a known story IS new: threat→action, proposal→agreement, \
unconfirmed→confirmed
- Same event from a different source is NOT new
- New details/numbers about a known event ARE new (e.g., casualty count \
updated, specific dollar amount added)
- Opinion/analysis about known events is NOT new\
"""


async def _call_gemini_dedup(
    client: genai.Client,
    article: dict,
    sitrep_content: str,
    model: str,
) -> Optional[dict]:
    """Send one dedup-check request to Gemini. Returns parsed dict or None."""
    title = article.get("title") or "(no title)"
    source = article.get("source_name") or "unknown"
    body = article.get("body_text") or ""
    body_truncated = body[:2000] if body else title

    prompt = _DEDUP_TEMPLATE.format(
        report=sitrep_content,
        title=title,
        source=source,
        body=body_truncated,
    )
    config = types.GenerateContentConfig(
        system_instruction=_DEDUP_SYSTEM,
        temperature=0.1,
        max_output_tokens=512,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )
    for attempt in range(3):
        try:
            response = await client.aio.models.generate_content(
                model=model, contents=prompt, config=config,
            )
            text = response.text.strip()
            if text.startswith("```"):
                lines = text.splitlines()
                text = "\n".join(ln for ln in lines if not ln.startswith("```")).strip()
            result = json.loads(text)
            if "is_new" not in result or "reasoning" not in result:
                logger.warning("SitRep dedup: missing keys in response: %s", text[:200])
                return None
            return result
        except json.JSONDecodeError as exc:
            logger.warning("SitRep dedup: JSON parse error: %s", exc)
            return None
        except Exception as exc:
            msg = str(exc)
            if ("503" in msg or "unavailable" in msg.lower()) and attempt < 2:
                logger.warning("SitRep dedup: Gemini 503 (attempt %d/3), retrying in 20s", attempt + 1)
                await asyncio.sleep(20)
                continue
            logger.warning("SitRep dedup: Gemini error: %s", exc)
            return None
    return None


# ---------------------------------------------------------------------------
# Gemini: compaction
# ---------------------------------------------------------------------------

_COMPACT_PROMPT = """\
Compact this situation report to roughly half its length.

Rules:
- Keep the same section structure (MILITARY, HORMUZ STATUS, DIPLOMATIC, \
SUPPLY & ROUTING, MARKET CONTEXT, REGIONAL IMPACT)
- Merge entries about the same topic into single updated statements
- Drop information that has been superseded by later entries \
(e.g., 'Iran threatens closure' superseded by 'Iran confirms closure')
- Keep all timestamps on remaining entries
- Preserve specific numbers, names, and facts — don't vague them up
- When in doubt, keep it rather than drop it

Current report:
{content}\
"""


async def _call_gemini_compact(
    client: genai.Client,
    content: str,
    model: str,
) -> Optional[str]:
    """Ask Gemini to compact the report. Returns compacted text or None."""
    config = types.GenerateContentConfig(
        temperature=0.2,
        max_output_tokens=4096,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )
    for attempt in range(3):
        try:
            response = await client.aio.models.generate_content(
                model=model,
                contents=_COMPACT_PROMPT.format(content=content),
                config=config,
            )
            return response.text.strip() or None
        except Exception as exc:
            msg = str(exc)
            if ("503" in msg or "unavailable" in msg.lower()) and attempt < 2:
                logger.warning("SitRep compaction: Gemini 503 (attempt %d/3), retrying in 20s", attempt + 1)
                await asyncio.sleep(20)
                continue
            logger.warning("SitRep: compaction Gemini error: %s", exc)
            return None
    return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def _model_label(model: str) -> str:
    """'gemini-2.5-flash-lite' → 'flash-lite', 'gemini-2.5-flash' → 'flash'."""
    import re as _re
    m = _re.match(r'^gemini-[\d.]+-(.+)$', model)
    return m.group(1) if m else model


async def run_sitrep_dedup(
    db_path: str,
    client: genai.Client,
    article: dict,
    dedup_model: str = "gemini-2.5-flash",
    compact_model: str = "gemini-2.5-flash",
) -> bool:
    """
    Run the situation-report dedup check for a single article.

    Returns True  → article contains new information; proceed to scoring.
    Returns False → duplicate; mark article as skipped (no scoring call).

    On any Gemini error the function returns True (fail-open) to avoid
    silently suppressing articles.
    """
    global _stats

    conn = get_connection(db_path)
    try:
        current = get_current_sitrep(conn)
        if current is None:
            logger.warning("SitRep: no situation report found — skipping dedup for this article")
            return True

        result = await _call_gemini_dedup(client, article, current["content"], dedup_model)
        if result is None:
            return True  # fail-open

        is_new: bool = bool(result.get("is_new"))
        new_info: str = result.get("new_information") or ""
        section: str = result.get("section") or "military"
        reasoning: str = result.get("reasoning") or ""
        dedup_label = _model_label(dedup_model)

        if is_new:
            _stats["new"] += 1
            logger.info("SitRep dedup [%s]: NEW — %s — %s", dedup_label, section, new_info[:80])

            if new_info:
                # Step 1: append to the report
                with transaction(conn):
                    fresh = get_current_sitrep(conn)
                    if fresh is not None:
                        _do_append_to_sitrep(conn, fresh, section, new_info)

                # Step 2: check if compaction is needed (async Gemini call is
                # outside the transaction)
                fresh2 = get_current_sitrep(conn)
                if fresh2 and len(fresh2["content"]) > COMPACTION_THRESHOLD:
                    old_version = fresh2["version"]
                    old_chars = len(fresh2["content"])
                    compact_content = await _call_gemini_compact(
                        client, fresh2["content"], compact_model
                    )
                    if compact_content:
                        with transaction(conn):
                            # Re-read inside transaction in case another append raced
                            latest = get_current_sitrep(conn)
                            if latest:
                                insert_sitrep_row(
                                    conn,
                                    content=compact_content,
                                    previous_id=latest["id"],
                                    compacted_from=old_version,
                                )
                        new_version_row = get_current_sitrep(conn)
                        new_version = new_version_row["version"] if new_version_row else "?"
                        logger.info(
                            "SitRep compaction [%s]: v%s→v%s (%d→%d chars)",
                            _model_label(compact_model),
                            old_version, new_version, old_chars, len(compact_content),
                        )
            return True

        else:
            _stats["dup"] += 1
            logger.info("SitRep dedup [%s]: DUPLICATE — %s", dedup_label, reasoning[:80])
            return False

    finally:
        conn.close()
