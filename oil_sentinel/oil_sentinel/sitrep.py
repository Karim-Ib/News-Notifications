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
  When len(content) > COMPACTION_THRESHOLD (8 000 chars), a Gemini call
  compacts the report to ~COMPACTION_TARGET (3 000 chars), preserving
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

COMPACTION_THRESHOLD = 8_000   # characters — trigger compaction above this
COMPACTION_TARGET    = 3_000   # characters — aim for this after compaction

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

_last_compaction_attempt: Optional[datetime] = None
_COMPACTION_COOLDOWN = 1800  # seconds — 30 min between automatic compaction attempts


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

_DEDUP_SYSTEM = """\
You are a news deduplication filter for an oil market intelligence system. \
Respond ONLY with a valid JSON object — no markdown, no explanation.

Output schema:
{
  "is_new": true/false,
  "new_information": "one factual sentence, present tense, max 150 characters" or null,
  "section": "military|hormuz|diplomatic|supply|market|regional" or null,
  "reasoning": "one sentence why this is/isn't new"
}

Section values:
  military    — military operations, strikes, armed conflict
  hormuz      — Strait of Hormuz navigation, blockade threats, transit disruptions
  diplomatic  — negotiations, agreements, sanctions, official statements
  supply      — production levels, pipelines, exports, cargo routing
  market      — price movements, trader reactions, demand forecasts
  regional    — country-level or regional political/economic impacts\
"""

_DEDUP_TEMPLATE = """\
Compare this article against the current situation report below.

SITUATION REPORT:
{report}

NEW ARTICLE:
Title: {title}
Source: {source}
Body (first 600 chars): {body}

TREAT AS NEW if any of the following apply:
- A new named-source quote, even if the general topic is already covered
- A new specific number or figure (updated barrel count, price, casualty count, timeline)
- A new named actor taking action, even on a known story thread
- Evolution of a known story: threat→action, proposal→agreement, \
unconfirmed→confirmed, warning→enforcement

TREAT AS DUPLICATE if any of the following apply:
- The situation report already contains the SAME specific claim, figure, or event \
(not just the same general topic)
- The same event re-reported verbatim by a different source
- Pure opinion or analysis with no new facts

When uncertain, default to NEW — missing a genuine signal is worse \
than scoring a near-duplicate.

EXAMPLES:
NEW — Sitrep records "Iran warned of Hormuz closure." Article reports Iran has begun \
deploying naval vessels to the strait. This is threat→action evolution. \
→ new_information: "Iran deploying naval vessels to Hormuz, escalating from verbal threat to action."

DUPLICATE — Sitrep already records "Saudi Arabia warned oil could hit $180." \
Article is a second outlet repeating the same Saudi minister quote with no new \
figures or statements. → is_new: false.\
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
    body_truncated = body[:600] if body else title

    # Cap sitrep at 5000 chars — matches the compaction target so dedup sees
    # the full compacted report rather than a truncated half of it.
    report_truncated = sitrep_content[:5000]

    prompt = _DEDUP_TEMPLATE.format(
        report=report_truncated,
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
Compact this situation report to strictly under {target} characters.

Rules:
- Keep the same section structure using these exact headers verbatim: \
MILITARY:, HORMUZ STATUS:, DIPLOMATIC:, SUPPLY & ROUTING:, MARKET CONTEXT:, REGIONAL IMPACT:
- For each section, keep ONLY the current state of affairs — drop the \
timeline of how we got here
- Merge all entries about the same topic into ONE updated statement with \
the latest timestamp
- Aggressively drop information that has been superseded by later entries
- Drop any entry older than 24 hours unless it describes an ongoing \
condition that hasn't changed
- Preserve specific numbers, names, and facts in remaining entries
- The result MUST be under {target} characters — if you're over, cut the \
least impactful entries

Current report:
{content}\
"""


def _section_aware_truncate(content: str, max_chars: int) -> str:
    """
    Truncate content to max_chars while preserving all 6 section headers
    and the most recent entries in each (entries are appended at the bottom).
    Returns content unchanged if already within max_chars.
    """
    if len(content) <= max_chars:
        return content

    # Separate the report header from the sectioned body
    header_end = content.find('\n\n')
    if header_end < 0:
        return content[:max_chars]
    header = content[:header_end + 2]
    body = content[header_end + 2:]

    # Locate each known section in the body, in document order
    section_positions: list[tuple[int, str]] = []
    for h in SECTION_HEADERS.values():
        pos = body.find(h)
        if pos >= 0:
            section_positions.append((pos, h))
    section_positions.sort(key=lambda x: x[0])

    if not section_positions:
        return content[:max_chars]

    # Extract per-section content slices
    parsed: list[tuple[str, str]] = []
    for i, (pos, h) in enumerate(section_positions):
        start = pos + len(h)
        end = section_positions[i + 1][0] if i + 1 < len(section_positions) else len(body)
        parsed.append((h, body[start:end]))

    # Distribute available budget evenly; most-recent (bottom) entries kept per section
    per_budget = max(0, (max_chars - len(header)) // len(parsed))

    parts = [header]
    for h, sec_content in parsed:
        content_budget = per_budget - len(h)
        if content_budget <= 0:
            parts.append(h + "\n")
        elif len(sec_content) <= content_budget:
            parts.append(h + sec_content)
        else:
            tail = sec_content[-content_budget:]
            nl = tail.find('\n')
            if 0 < nl < len(tail) - 1:
                tail = tail[nl:]
            parts.append(h + tail)

    result = "".join(parts)
    logger.warning(
        "SitRep section-aware truncation: %d → %d chars",
        len(content), len(result),
    )
    return result


async def _call_gemini_compact(
    client: genai.Client,
    content: str,
    model: str,
    target_chars: int = COMPACTION_TARGET,
) -> Optional[str]:
    """Ask Gemini to compact the report. Returns compacted text or None."""
    # Section-aware truncation caps the model input to a manageable size.
    # A runaway 90k-char sitrep becomes ~16k chars here, keeping the newest
    # entries from every section instead of a blind tail slice.
    content = _section_aware_truncate(content, max_chars=16_000)

    config = types.GenerateContentConfig(
        temperature=0.2,
        max_output_tokens=2048,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )
    for attempt in range(3):
        try:
            response = await client.aio.models.generate_content(
                model=model,
                contents=_COMPACT_PROMPT.format(
                    target=target_chars, content=content
                ),
                config=config,
            )
            result = response.text.strip() or None
            if result and len(result) > COMPACTION_THRESHOLD:
                logger.warning(
                    "SitRep compaction insufficient: %d chars (threshold %d, target %d) — discarding",
                    len(result), COMPACTION_THRESHOLD, target_chars,
                )
                return None
            return result
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
    compact_model: str = "gemini-2.5-flash-lite",
) -> bool:
    """
    Run the situation-report dedup check for a single article.

    Returns True  → article contains new information; proceed to scoring.
    Returns False → duplicate; mark article as skipped (no scoring call).

    On any Gemini error the function returns True (fail-open) to avoid
    silently suppressing articles.
    """
    global _stats, _last_compaction_attempt

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
                    now = datetime.now(timezone.utc)
                    if (
                        _last_compaction_attempt is not None
                        and (now - _last_compaction_attempt).total_seconds() < _COMPACTION_COOLDOWN
                    ):
                        pass  # cooldown active — skip until next window
                    else:
                        _last_compaction_attempt = now
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
                        else:
                            # Gemini compaction failed — fall back to deterministic
                            # section-aware truncation so the sitrep never grows unboundedly.
                            fallback = _section_aware_truncate(fresh2["content"], COMPACTION_TARGET)
                            with transaction(conn):
                                latest = get_current_sitrep(conn)
                                if latest:
                                    insert_sitrep_row(
                                        conn,
                                        content=fallback,
                                        previous_id=latest["id"],
                                        compacted_from=old_version,
                                    )
                            logger.warning(
                                "SitRep hard-truncated (compaction failed): %d → %d chars",
                                old_chars, len(fallback),
                            )
            return True

        else:
            _stats["dup"] += 1
            logger.info("SitRep dedup [%s]: DUPLICATE — %s", dedup_label, reasoning[:80])
            return False

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public: manual compaction (Telegram /compact command)
# ---------------------------------------------------------------------------

async def compact_sitrep_to_target(
    db_path: str,
    client: genai.Client,
    model: str,
    target_chars: int,
) -> Optional[tuple[int, int, int, int]]:
    """
    Compact the current situation report to approximately target_chars.

    Returns (old_len, new_len, old_version, new_version) on success,
    or None if compaction failed or the result was not shorter.
    """
    conn = get_connection(db_path)
    try:
        current = get_current_sitrep(conn)
        if current is None:
            return None

        old_len = len(current["content"])
        old_version = current["version"]

        compact_content = await _call_gemini_compact(
            client, current["content"], model, target_chars=target_chars
        )
        if not compact_content or len(compact_content) >= old_len:
            return None

        with transaction(conn):
            fresh = get_current_sitrep(conn)
            if fresh is None:
                return None
            insert_sitrep_row(
                conn,
                content=compact_content,
                previous_id=fresh["id"],
                compacted_from=old_version,
            )

        new_row = get_current_sitrep(conn)
        new_version = new_row["version"] if new_row else old_version + 1
        new_len = len(compact_content)

        logger.info(
            "SitRep manual compaction [%s]: v%s→v%s (%d→%d chars)",
            _model_label(model), old_version, new_version, old_len, new_len,
        )
        return (old_len, new_len, old_version, new_version)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public: deterministic reset (/reset_sitrep command)
# ---------------------------------------------------------------------------

def reset_sitrep(db_path: str) -> Optional[tuple[int, int, int, int]]:
    """
    Deterministically truncate the live sitrep to COMPACTION_TARGET chars
    using section-aware truncation.  No Gemini call — safe to run at any time.

    Returns (old_len, new_len, old_version, new_version) on success,
    or None if the sitrep is already within COMPACTION_TARGET or not found.
    """
    conn = get_connection(db_path)
    try:
        current = get_current_sitrep(conn)
        if current is None:
            return None

        old_len = len(current["content"])
        old_version = current["version"]

        new_content = _section_aware_truncate(current["content"], COMPACTION_TARGET)
        if new_content == current["content"]:
            return None  # already small enough

        with transaction(conn):
            insert_sitrep_row(
                conn,
                content=new_content,
                previous_id=current["id"],
                compacted_from=old_version,
            )

        new_row = get_current_sitrep(conn)
        new_version = new_row["version"] if new_row else old_version + 1
        new_len = len(new_content)
        logger.info(
            "SitRep deterministic reset: v%s→v%s (%d→%d chars)",
            old_version, new_version, old_len, new_len,
        )
        return (old_len, new_len, old_version, new_version)
    finally:
        conn.close()
