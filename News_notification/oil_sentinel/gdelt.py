"""
Poll GDELT 2.0 DOC API for Iran/Hormuz/oil-related articles.
- Deduplicates by URL SHA-256 hash
- Pre-filters by actor mentions and tone deviation
- Stores qualifying articles in the DB
"""

import asyncio
import json
import logging
import re
from typing import Optional

import aiohttp

from db import (
    article_exists,
    get_connection,
    insert_article,
    transaction,
)

logger = logging.getLogger(__name__)

# GDELT DOC API v2 endpoint
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Split into two queries to stay under GDELT's ~250-char query limit.
# Query A: Iran/Hormuz core + Gulf chokepoints
# Query B: broader actors (Houthi/Red Sea, Saudi, great powers, OPEC)
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


async def fetch_gdelt_articles(
    session: aiohttp.ClientSession,
    *,
    query: str,
    max_records: int = 250,
    tone_threshold: float = 2.5,
    min_relevance: int = 60,
    timespan: str = "15m",
) -> list[dict]:
    """
    Query GDELT DOC API for a single query string, return pre-filtered article dicts.
    """
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
        except aiohttp.ClientError as exc:
            wait = 10 * (2 ** attempt)
            logger.error("GDELT fetch error (attempt %d): %s -- retrying in %ds", attempt + 1, exc, wait)
            await asyncio.sleep(wait)

    if data is None:
        logger.error("GDELT fetch failed after all retries")
        return []

    raw_articles = data.get("articles") or []
    logger.debug("GDELT returned %d raw articles", len(raw_articles))

    results = []
    for art in raw_articles:
        url = art.get("url", "").strip()
        if not url:
            continue

        title = art.get("title", "") or ""
        actors = _extract_actors(art)
        actors_str = " ".join(actors)

        # Content pre-filter
        if not _passes_content_filter(title, actors_str):
            continue

        # Relevance filter (GDELT socialimage / relevance field isn't always present;
        # fall back to accepting all that pass content filter)
        relevance = art.get("relevance")
        if relevance is not None:
            try:
                if float(relevance) < min_relevance:
                    continue
            except ValueError:
                pass

        tone = _extract_tone(art)

        # Tone deviation filter -- skip neutral articles
        if tone is not None and abs(tone) < tone_threshold:
            continue

        themes = art.get("themes", "")
        theme_list = [t.strip() for t in themes.split(";") if t.strip()] if themes else []

        results.append({
            "url": url,
            "title": title,
            "source_name": art.get("domain", ""),
            "published_at": art.get("seendate", ""),
            "gdelt_tone": tone,
            "gdelt_themes": json.dumps(theme_list),
            "actors": json.dumps(actors),
            "raw_json": json.dumps(art),
        })

    logger.info("GDELT: %d articles passed pre-filter (from %d raw)", len(results), len(raw_articles))
    return results


async def poll_and_store(
    db_path: str,
    session: aiohttp.ClientSession,
    *,
    tone_threshold: float = 2.5,
    min_relevance: int = 60,
    timespan: str = "15m",
) -> int:
    """
    Fetch from all GDELT queries, merge+dedup, insert new articles.
    Returns count of newly inserted articles.
    """
    seen_urls: set[str] = set()
    articles: list[dict] = []

    for q in GDELT_QUERIES:
        batch = await fetch_gdelt_articles(
            session,
            query=q,
            tone_threshold=tone_threshold,
            min_relevance=min_relevance,
            timespan=timespan,
        )
        for art in batch:
            if art["url"] not in seen_urls:
                seen_urls.add(art["url"])
                articles.append(art)
        # Gap between queries to avoid back-to-back GDELT 429s
        await asyncio.sleep(8)

    conn = get_connection(db_path)
    new_count = 0
    try:
        with transaction(conn):
            for art in articles:
                if article_exists(conn, art["url"]):
                    continue
                row_id = insert_article(
                    conn,
                    url=art["url"],
                    title=art["title"],
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

    logger.info("GDELT poll: %d new articles stored", new_count)
    return new_count
