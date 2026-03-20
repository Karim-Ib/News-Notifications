"""
Send qualifying articles to Gemini for structured impact scoring.

Output schema per article:
{
  "direction":    "bullish" | "bearish" | "neutral",
  "magnitude":    1-5,
  "confidence":   0.0-1.0,
  "event_type":   str,
  "narrative_key": str,
  "summary":      str,
  "detail":       str,
}
"""

import asyncio
import json
import logging
import re
from typing import Optional

from google import genai
from google.genai import types

from oil_sentinel.db import (
    get_connection,
    get_recent_narrative_keys,
    get_unscored_articles,
    insert_alert,
    mark_article_scored,
    narrative_exists_recent,
    transaction,
    update_article_body,
)
from oil_sentinel.ingestion.extractor import fetch_article_text

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\>
You are a neutral crude-oil market analyst. Your job is to score news articles \
for their directional impact on oil prices. You must be equally rigorous for \
bullish AND bearish signals — do not default to bullish.

OUTPUT a JSON object with EXACTLY these fields:

  direction     : "bullish", "bearish", or "neutral"
                  Use the definitions below — do not guess.
  magnitude     : integer 0-10
                    0  = complete noise, zero market relevance
                    1-2 = trivial signal, already priced in or immaterial
                    3-4 = low impact, <1% price move expected
                    5-6 = moderate, 1-3% move expected
                    7-8 = significant, 3-6% move expected — warrants immediate alert
                    9   = major event, >6% move expected
                    10  = historic / structural market shift
                  Be granular — use the full range. Reserve 9-10 for genuine crises.
  confidence    : float 0.0-1.0 — two-step calculation, output the final value.

                  STEP 1 — source credibility baseline:
                    0.85-1.00 = tier-1 wires: Reuters, Bloomberg, AP, FT, WSJ,
                                Al Jazeera, BBC
                    0.65-0.80 = established press: CNBC, Guardian, NYT, major nationals
                    0.45-0.60 = trade press, regional outlets, aggregators
                                (oilprice.com, rigzone.com, hellenicshippingnews.com, etc.)
                    0.20-0.40 = blogs, unknown sources, unverified, no byline

                  STEP 2 — multiply by certainty factor:
                    × 1.0 = confirmed event, named official sources, on-record quotes
                    × 0.7 = hedged language ("may", "could", "reportedly",
                            "sources say", "is expected to", "according to unnamed")
                    × 0.5 = speculation, opinion piece, analyst forecast,
                            unverified claim, single unnamed source

                  EXAMPLES (source baseline × certainty = final):
                    Reuters confirmed strike, named officials  → 0.90 × 1.0 = 0.90
                    Bloomberg "sources say" ceasefire talks    → 0.90 × 0.7 = 0.63
                    oilprice.com confirmed OPEC cut            → 0.50 × 1.0 = 0.50
                    regional site, "may close" Hormuz          → 0.50 × 0.7 = 0.35
                    unknown blog speculating about supply       → 0.30 × 0.5 = 0.15

                  NEVER output 0.70 as a default. Every article needs a reasoned value.
  event_type    : one label from the taxonomy below
  narrative_key : lowercase snake_case slug <=30 chars.
                  Use SPECIFIC event details — never generic conflict labels.
                    GOOD: "saudi_180_oil_price_warning"
                          "houthi_bab_el_mandeb_threat"
                          "iran_hormuz_closure_drill"
                          "opec_march_output_cut"
                    BAD:  "iran_conflict_escalation"
                          "middle_east_oil_tension"
                          "iran_israel_conflict"
                          "oil_market_disruption"
                  For follow-up articles on the SAME specific event reuse the
                  EXACT key. Different events with the same actors get different keys.
  summary       : <=120 chars, present-tense factual headline
  detail        : exactly 2 sentences:
                    [1] which supply/demand/flow mechanism is affected and how
                    [2] near-term price trajectory + the single biggest risk to that view

DIRECTION DEFINITIONS — apply strictly:
  bullish  : net reduction in supply, increase in risk premium, or demand surge
             e.g. military strike, tanker seizure, sanctions tightening,
                  Hormuz closure threat, OPEC surprise cut, pipeline sabotage
  bearish  : net increase in supply, reduction in risk premium, or demand drop
             e.g. sanctions relief/nuclear deal progress, ceasefire, Iran export
                  waiver granted, OPEC+ quota increase, demand destruction news,
                  diplomatic breakthrough, US SPR release
  neutral  : headline noise with no clear supply/flow impact, or offsetting factors

EVENT TYPE TAXONOMY:
  Bullish types  : military_action, tanker_seizure, supply_disruption,
                   sanctions_tightening, infrastructure_attack, blockade_threat,
                   opec_cut
  Bearish types  : sanctions_relief, nuclear_deal, ceasefire, diplomatic_progress,
                   supply_increase, export_waiver, demand_destruction, spr_release
  Neutral types  : rhetoric, diplomatic_meeting, opec_meeting, analysis,
                   election, economic_data

BIAS CHECK — before writing direction, ask yourself:
  - Does this article describe an event that adds oil to the market or removes it?
  - Is the risk premium going up or down?
  - If a deal/ceasefire/diplomatic progress article, it is BEARISH, not neutral.

MAGNITUDE CALIBRATION — before writing magnitude, ask yourself:
  - Would a professional oil trader react immediately to this? If yes, score >= 7.
  - Is this a follow-up to an already-known story? Downgrade 1-2 points.
  - Is the source speculative or unconfirmed? Downgrade confidence, not magnitude.

Return ONLY the JSON object. No markdown fences, no extra keys.\
"""

USER_TEMPLATE = (
    "Title: {title}\n"
    "Source: {source}\n"
    "Published: {published_at}\n"
    "GDELT tone: {tone} (negative = negative sentiment; positive = positive sentiment)\n"
    "Actors: {actors}\n"
    "{body_section}"
    "Active narrative threads (reuse the EXACT key if this article is a follow-up):\n"
    "{narrative_context}\n\n"
    "Score this article. Remember: diplomatic/deal/ceasefire articles are bearish. "
    "Military/seizure/sanction articles are bullish. Apply the bias check. "
    "Use a specific narrative_key (actor + action + context, not generic labels). "
    "Calculate confidence as source_baseline × certainty_factor — do not default to 0.70."
)

_MAX_RETRIES = 3
_BASE_RETRY_DELAY = 40  # seconds -- matches free-tier RPM window


def _build_prompt(
    article: dict,
    recent_narratives: list[str],
    body_text: Optional[str] = None,
) -> str:
    actors = []
    try:
        actors = json.loads(article.get("actors") or "[]")
    except json.JSONDecodeError:
        pass
    raw_tone = article.get("gdelt_tone")
    try:
        tone_str = f"{float(raw_tone):.2f}" if raw_tone is not None else "n/a"
    except (TypeError, ValueError):
        tone_str = "n/a"
    if recent_narratives:
        narrative_context = "  " + "\n  ".join(recent_narratives)
    else:
        narrative_context = "  (none yet)"

    if body_text and body_text.strip():
        body_section = f"\nArticle text (first 2000 chars):\n{body_text}\n\n"
    else:
        body_section = "\n"

    return USER_TEMPLATE.format(
        title=article.get("title") or "(no title)",
        source=article.get("source_name") or "unknown",
        published_at=article.get("published_at") or "unknown",
        tone=tone_str,
        actors=", ".join(actors) or "unknown",
        body_section=body_section,
        narrative_context=narrative_context,
    )


def _extract_retry_delay(exc: Exception) -> int:
    """Pull suggested retry_delay seconds from a Gemini 429 exception if present."""
    msg = str(exc)
    m = re.search(r'retry_delay\s*\{\s*seconds:\s*(\d+)', msg)
    if m:
        return int(m.group(1)) + 5
    return _BASE_RETRY_DELAY


def _parse_response(text: str) -> Optional[dict]:
    """Extract and validate JSON from Gemini response."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(line for line in lines if not line.startswith("```")).strip()
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning("JSON parse error from Gemini: %s | raw: %s", exc, text[:200])
        return None

    required = {"direction", "magnitude", "confidence", "event_type", "narrative_key", "summary", "detail"}
    if not required.issubset(obj.keys()):
        logger.warning("Gemini response missing keys: %s", required - obj.keys())
        return None

    try:
        obj["magnitude"] = max(0, min(10, int(obj["magnitude"])))
        obj["confidence"] = max(0.0, min(1.0, float(obj["confidence"])))
        if obj["direction"] not in ("bullish", "bearish", "neutral"):
            obj["direction"] = "neutral"
    except (ValueError, TypeError):
        return None

    return obj


def make_gemini_client(api_key: str) -> genai.Client:
    return genai.Client(api_key=api_key)


async def score_article(
    client: genai.Client,
    article: dict,
    model: str = "gemini-2.5-flash",
    recent_narratives: Optional[list[str]] = None,
    body_text: Optional[str] = None,
) -> Optional[dict]:
    """Call Gemini for one article with retry on 429. Returns parsed dict or None."""
    prompt = _build_prompt(article, recent_narratives or [], body_text=body_text)
    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        temperature=0.2,
        max_output_tokens=8192,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )

    for attempt in range(_MAX_RETRIES):
        try:
            response = await client.aio.models.generate_content(
                model=model,
                contents=prompt,
                config=config,
            )
            return _parse_response(response.text)
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "quota" in msg.lower() or "rate" in msg.lower():
                delay = _extract_retry_delay(exc)
                if attempt < _MAX_RETRIES - 1:
                    logger.warning(
                        "Gemini 429 (attempt %d/%d), retrying in %ds",
                        attempt + 1, _MAX_RETRIES, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error("Gemini 429 persists after %d retries -- will retry next cycle", _MAX_RETRIES)
                return None
            logger.error("Gemini API error: %s", exc)
            return None

    return None


async def score_pending_articles(
    db_path: str,
    client: genai.Client,
    *,
    model: str = "gemini-2.5-flash",
    batch_size: int = 5,
    market_anomaly: bool = False,
) -> int:
    """
    Pull unscored articles from DB, extract body text, score with Gemini, create alerts.
    Returns number of alerts created.
    """
    conn = get_connection(db_path)
    alerts_created = 0
    extract_attempted = 0
    extract_succeeded = 0

    try:
        rows = get_unscored_articles(conn, limit=batch_size)
        if not rows:
            logger.debug("No unscored articles")
            return 0

        logger.info("Scoring %d articles with Gemini", len(rows))

        for row in rows:
            article = dict(row)

            # ── Body text extraction ─────────────────────────────────────────
            # Use cached body_text if already in DB; otherwise fetch now.
            body_text: Optional[str] = article.get("body_text")
            if not body_text:
                extract_attempted += 1
                body_text = await asyncio.to_thread(fetch_article_text, article["url"])
                if body_text:
                    extract_succeeded += 1
                    with transaction(conn):
                        update_article_body(conn, article["id"], body_text)
                    logger.debug(
                        "Extracted %d chars from %s", len(body_text), article.get("source_name", "")
                    )
                else:
                    logger.debug("No body text for %s — title-only scoring", article.get("url", "")[:80])
                # Brief pause between HTTP fetches to be polite to news sites
                await asyncio.sleep(1.5)

            # ── Gemini scoring ───────────────────────────────────────────────
            # Refresh narrative context before each article so Gemini sees
            # keys generated earlier in this same batch
            recent_narratives = get_recent_narrative_keys(conn, within_hours=12)

            result = await score_article(
                client, article, model=model,
                recent_narratives=recent_narratives,
                body_text=body_text,
            )

            with transaction(conn):
                if result is None:
                    mark_article_scored(conn, article["id"], skipped=True)
                    continue

                narrative_key = result["narrative_key"]

                # Block duplicate: if a matching narrative already exists in the last
                # 12h (exact or >=75% word-overlap), skip alert creation.
                # Exception: direction flip on the same narrative thread passes through.
                existing = narrative_exists_recent(
                    conn, narrative_key,
                    within_hours=12,
                    incoming_direction=result["direction"],
                )
                if existing:
                    logger.info(
                        "Dedup: skipping [%s] — matches existing narrative [%s] (dir=%s)",
                        narrative_key, existing, result["direction"],
                    )
                    mark_article_scored(conn, article["id"], skipped=True)
                    continue

                composite = result["magnitude"] * result["confidence"]
                if market_anomaly:
                    composite += 1.0

                insert_alert(
                    conn,
                    article_id=article["id"],
                    narrative_key=narrative_key,
                    event_type=result["event_type"],
                    direction=result["direction"],
                    magnitude=result["magnitude"],
                    confidence=result["confidence"],
                    market_anomaly=market_anomaly,
                    composite_score=composite,
                    summary=result["summary"] + "\n\n" + result.get("detail", ""),
                )
                mark_article_scored(conn, article["id"])
                alerts_created += 1

            has_body = "full-text" if body_text else "title-only"
            logger.info(
                "Scored [%s] mag=%d conf=%.2f dir=%s comp=%.2f [%s] | %s",
                result["narrative_key"],
                result["magnitude"],
                result["confidence"],
                result["direction"],
                composite,
                has_body,
                article.get("title", "")[:60],
            )

            await asyncio.sleep(4)

        if extract_attempted:
            logger.info(
                "Body extraction: %d/%d succeeded (%.0f%%) this batch",
                extract_succeeded, extract_attempted,
                100 * extract_succeeded / extract_attempted,
            )
    finally:
        conn.close()

    return alerts_created
