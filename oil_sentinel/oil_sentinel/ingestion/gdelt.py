"""
Poll GDELT 2.0 DOC API for Iran/Hormuz/oil-related articles.
- Deduplicates by URL SHA-256 hash and first-8-word title hash
- Pre-filters by actor mentions, tone deviation, age, and relevance
- Stores qualifying articles in the DB
"""

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp

from oil_sentinel.db import (
    article_exists,
    get_connection,
    insert_article,
    title_hash_exists,
    transaction,
)

logger = logging.getLogger(__name__)

# GDELT DOC API v2 endpoint
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Split into two queries to stay under GDELT's ~250-char query limit.
GDELT_QUERIES = [
    '(Hormuz OR "Persian Gulf" OR "Iranian oil" OR "Iran sanctions" OR "Iran nuclear" OR "oil embargo")',
    '(Houthi OR "Red Sea" OR "Saudi Aramco" OR "OPEC cut" OR "Bab el-Mandeb" OR "Fifth Fleet" OR "Yemen oil")',
]

# Actor / keyword sets for secondary pre-filter (lowercased)
ACTOR_PATTERNS = [
    r'\biran\b', r'\birgc\b', r'\bhormuz\b', r'\bkhamenei\b', r'\braisi\b',
    r'\bopec\b', r'\bsaudi\b', r'\baramco\b',
    r'\bhouthi\b', r'\byemen\b',
    r'\bchina\b', r'\brussia\b', r'\bpentagon\b', r'\bcentcom\b',
    r'\buae\b', r'\bisrael\b', r'\biraq\b',
]
KEYWORD_PATTERNS = [
    r'\boil\b', r'\bcrude\b', r'\btanker\b', r'\bsanction', r'\bstrait\b',
    r'\bblockade\b', r'\battack\b', r'\bmissile\b', r'\bdrone\b',
    r'\bseizure\b', r'\bembargo\b', r'\bceasefire\b', r'\bnuclear\b',
    r'\bsupply\b', r'\bexport\b', r'\brefinery\b', r'\bpipeline\b',
]

_ACTOR_RE = [re.compile(p, re.I) for p in ACTOR_PATTERNS]
_KEYWORD_RE = [re.compile(p, re.I) for p in KEYWORD_PATTERNS]


def _passes_content_filter(title: str, actors_raw: str) -> bool:
    """Return True if title/actors contain at least one actor AND one keyword."""
    combined = f"{title} {actors_raw}".lower()
    has_actor = any(r.search(combined) for r in _ACTOR_RE)
    has_keyword = any(r.search(combined) for r in _KEYWORD_RE)
    return has_actor and has_keyword


def _extract_tone(article: dict) -> Optional[float]:
    """Pull GDELT tone from the 'tone' field (comma-separated, first value)."""
    tone_str = article.get("tone", "")
    if not tone_str:
        return None
    try:
        return float(tone_str.split(",")[0])
    except (ValueError, IndexError):
        return None


def _extract_actors(article: dict) -> list[str]:
    """Collect actor1name / actor2name / actor3name fields."""
    actors = []
    for key in ("actor1name", "actor2name", "actor3name"):
        v = article.get(key, "").strip()
        if v:
            actors.append(v)
    return actors


def _title_hash(title: str) -> str:
    """Hash the first 8 normalised words of a title for near-duplicate detection."""
    words = re.sub(r"[^\w\s]", "", title.lower()).split()
    key = " ".join(words[:8])
    return hashlib.sha256(key.encode()).hexdigest()


def _parse_gdelt_date(raw: str) -> Optional[datetime]:
    """Parse GDELT seendate (20260319T143022Z) to UTC datetime."""
    try:
        return datetime.strptime(raw.strip(), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return None


def _source_tier(domain: str, tier1_domains: set[str]) -> int:
    """Return 1 for known quality sources, 2 for everything else."""
    domain = domain.lower().strip()
    return 1 if any(domain == d or domain.endswith("." + d) for d in tier1_domains) else 2


async def fetch_gdelt_articles(
    session: aiohttp.ClientSession,
    *,
    query: str,
    max_records: int = 250,
    tone_threshold: float = 2.5,
    unknown_tone_threshold: float = 5.0,
    min_relevance: int = 60,
    timespan: str = "15m",
    max_age_hours: int = 12,
    tier1_domains: Optional[set[str]] = None,
) -> list[dict]:
    """
    Query GDELT DOC API for a single query string, return pre-filtered article dicts.
    Applies: content filter, tone threshold (per source tier), age cap.
    Title dedup is handled in poll_and_store against the DB.
    """
    if tier1_domains is None:
        tier1_domains = set()
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": max_records,
        "timespan": timespan,
        "format": "json",
        "sort": "DateDesc",
    }

    data = None
    for attempt in range(4):
        try:
            async with session.get(
                GDELT_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 429:
                    wait = 15 * (2 ** attempt)
                    logger.warning("GDELT 429 rate-limited, retrying in %ds (attempt %d)", wait, attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                raw_text = await resp.text()
                if not raw_text or not raw_text.strip():
                    logger.debug("GDELT returned empty body (no articles in timespan)")
                    data = {}
                    break
                try:
                    data = json.loads(raw_text)
                except Exception as exc:
                    logger.warning("GDELT JSON parse error: %s | body[:200]: %s", exc, raw_text[:200])
                    data = {}
                break
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            wait = 10 * (2 ** attempt)
            logger.error("GDELT fetch error (attempt %d): %s -- retrying in %ds", attempt + 1, exc, wait)
            await asyncio.sleep(wait)

    if data is None:
        logger.error("GDELT fetch failed after all retries")
        return []

    raw_articles = data.get("articles") or []
    logger.debug("GDELT returned %d raw articles", len(raw_articles))

    now = datetime.now(timezone.utc)
    results = []
    drop_counts = {"content": 0, "age": 0, "tone": 0, "relevance": 0}

    for art in raw_articles:
        url = art.get("url", "").strip()
        if not url:
            continue

        title = art.get("title", "") or ""
        domain = art.get("domain", "") or ""
        actors = _extract_actors(art)
        actors_str = " ".join(actors)

        # --- Filter A: content (actor + keyword) ---
        if not _passes_content_filter(title, actors_str):
            drop_counts["content"] += 1
            continue

        # --- Filter B: relevance ---
        relevance = art.get("relevance")
        if relevance is not None:
            try:
                if float(relevance) < min_relevance:
                    drop_counts["relevance"] += 1
                    continue
            except ValueError:
                pass

        # --- Filter C: age cap ---
        seendate = art.get("seendate", "")
        pub_dt = _parse_gdelt_date(seendate)
        if pub_dt and (now - pub_dt) > timedelta(hours=max_age_hours):
            drop_counts["age"] += 1
            continue

        # --- Filter D: tone threshold (varies by source tier) ---
        tone = _extract_tone(art)
        tier = _source_tier(domain, tier1_domains)
        effective_threshold = tone_threshold if tier == 1 else unknown_tone_threshold
        if tone is not None and abs(tone) < effective_threshold:
            drop_counts["tone"] += 1
            continue

        themes = art.get("themes", "")
        theme_list = [t.strip() for t in themes.split(";") if t.strip()] if themes else []

        results.append({
            "url": url,
            "title": title,
            "title_hash": _title_hash(title),
            "source_name": domain,
            "published_at": seendate,
            "gdelt_tone": tone,
            "gdelt_themes": json.dumps(theme_list),
            "actors": json.dumps(actors),
            "raw_json": json.dumps(art),
        })

    logger.info(
        "GDELT: %d passed (from %d raw) | dropped: content=%d age=%d tone=%d relevance=%d",
        len(results), len(raw_articles),
        drop_counts["content"], drop_counts["age"],
        drop_counts["tone"], drop_counts["relevance"],
    )
    return results


async def poll_and_store(
    db_path: str,
    session: aiohttp.ClientSession,
    *,
    tone_threshold: float = 2.5,
    unknown_tone_threshold: float = 5.0,
    min_relevance: int = 60,
    timespan: str = "15m",
    max_age_hours: int = 12,
    tier1_domains: Optional[set[str]] = None,
) -> int:
    """
    Fetch from all GDELT queries, apply all pre-filters, dedup, insert new articles.
    Returns count of newly inserted articles.
    """
    if tier1_domains is None:
        tier1_domains = set()

    seen_urls: set[str] = set()
    articles: list[dict] = []

    for q in GDELT_QUERIES:
        batch = await fetch_gdelt_articles(
            session,
            query=q,
            tone_threshold=tone_threshold,
            unknown_tone_threshold=unknown_tone_threshold,
            min_relevance=min_relevance,
            timespan=timespan,
            max_age_hours=max_age_hours,
            tier1_domains=tier1_domains,
        )
        for art in batch:
            if art["url"] not in seen_urls:
                seen_urls.add(art["url"])
                articles.append(art)
        await asyncio.sleep(8)

    conn = get_connection(db_path)
    new_count = 0
    skipped_title_dedup = 0
    try:
        with transaction(conn):
            for art in articles:
                if article_exists(conn, art["url"]):
                    continue
                if art["title_hash"] and title_hash_exists(conn, art["title_hash"], within_hours=24):
                    skipped_title_dedup += 1
                    continue
                row_id = insert_article(
                    conn,
                    url=art["url"],
                    title=art["title"],
                    title_hash=art["title_hash"],
                    source_name=art["source_name"],
                    published_at=art["published_at"],
                    gdelt_tone=art["gdelt_tone"],
                    gdelt_themes=art["gdelt_themes"],
                    actors=art["actors"],
                    raw_json=art["raw_json"],
                )
                if row_id is not None:
                    new_count += 1
    finally:
        conn.close()

    if skipped_title_dedup:
        logger.info("GDELT poll: %d new articles stored, %d skipped (title dedup)", new_count, skipped_title_dedup)
    else:
        logger.info("GDELT poll: %d new articles stored", new_count)
    return new_count
