"""
Poll Google News RSS feeds for Iran/Hormuz/oil articles.

Backup source — fills gaps when GDELT is rate-limited or unavailable.
Articles enter the same pipeline (URL hash dedup → title hash dedup →
sitrep dedup → scoring) with source = "google_news".
"""

import json
import logging
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import quote_plus

import aiohttp
import feedparser

from oil_sentinel.db import (
    article_exists,
    get_connection,
    insert_article,
    title_hash_exists,
    transaction,
)
from oil_sentinel.ingestion.gdelt import _ACTOR_RE, _KEYWORD_RE, _title_hash

logger = logging.getLogger(__name__)

# Search queries — each becomes its own RSS feed URL
GOOGLE_NEWS_QUERIES = [
    '"Strait of Hormuz" OR "Iran oil" OR "Hormuz closure"',
    '"OPEC Iran" OR "Iran sanctions oil" OR "Iran tanker"',
    '"Iran ceasefire" OR "Iran diplomacy oil" OR "Hormuz reopen"',
]

RSS_BASE = "https://news.google.com/rss/search"


def _rss_url(query: str) -> str:
    return f"{RSS_BASE}?q={quote_plus(query)}&hl=en&gl=US&ceid=US:en"


def _passes_content_filter(title: str) -> bool:
    """Filter on title only (RSS provides no actor metadata)."""
    text = title.lower()
    has_actor = any(r.search(text) for r in _ACTOR_RE)
    has_keyword = any(r.search(text) for r in _KEYWORD_RE)
    return has_actor and has_keyword


async def _resolve_url(session: aiohttp.ClientSession, url: str) -> str:
    """Follow Google News redirect to get the actual article URL."""
    try:
        async with session.head(
            url,
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            return str(resp.url)
    except Exception:
        return url


def _parse_pubdate(entry) -> Optional[str]:
    """Parse feedparser entry published date → GDELT-style UTC string."""
    published = getattr(entry, "published", None)
    if published:
        try:
            dt = parsedate_to_datetime(published)
            return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        except Exception:
            pass
    return None


async def fetch_google_news_articles(
    session: aiohttp.ClientSession,
    query: str,
) -> list[dict]:
    """
    Fetch one Google News RSS feed, resolve redirects, apply content filter.
    Returns list of article dicts ready for DB insertion.
    Raises RuntimeError on fetch failure.
    """
    url = _rss_url(query)
    async with session.get(
        url,
        timeout=aiohttp.ClientTimeout(total=15),
    ) as resp:
        resp.raise_for_status()
        xml_text = await resp.text()

    feed = feedparser.parse(xml_text)
    entries = feed.entries
    logger.info("Google News RSS: fetched %d items from %r", len(entries), query)

    results = []
    for entry in entries:
        title = getattr(entry, "title", "") or ""
        if not _passes_content_filter(title):
            continue

        raw_url = getattr(entry, "link", "") or ""
        if not raw_url:
            continue

        # Resolve Google redirect → actual article URL
        actual_url = await _resolve_url(session, raw_url)

        # Source name: prefer the <source> tag, fall back to "google_news"
        source_tag = getattr(entry, "source", None)
        if source_tag and getattr(source_tag, "title", None):
            source_name = f"google_news:{source_tag.title}"
        else:
            source_name = "google_news"

        pub_str = _parse_pubdate(entry)

        results.append({
            "url": actual_url,
            "title": title,
            "title_hash": _title_hash(title),
            "source_name": source_name,
            "published_at": pub_str,
            "gdelt_tone": None,
            "gdelt_themes": json.dumps([]),
            "actors": json.dumps([]),
            "raw_json": json.dumps({
                "title": title,
                "link": actual_url,
                "source": source_name,
            }),
        })

    return results


async def poll_and_store(
    db_path: str,
    session: aiohttp.ClientSession,
) -> int:
    """
    Poll all Google News RSS feeds, dedup, store new articles.
    Returns count of newly inserted articles.
    Per-feed errors are logged as warnings but do not propagate —
    remaining feeds are still attempted.
    """
    seen_urls: set[str] = set()
    articles: list[dict] = []

    for query in GOOGLE_NEWS_QUERIES:
        try:
            batch = await fetch_google_news_articles(session, query)
            for art in batch:
                if art["url"] not in seen_urls:
                    seen_urls.add(art["url"])
                    articles.append(art)
        except Exception as exc:
            logger.warning("Google News RSS: feed error — %s.", exc)

    conn = get_connection(db_path)
    new_count = 0
    already_in_db = 0
    try:
        with transaction(conn):
            for art in articles:
                if article_exists(conn, art["url"]):
                    already_in_db += 1
                    continue
                if art["title_hash"] and title_hash_exists(conn, art["title_hash"], within_hours=24):
                    already_in_db += 1
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

    logger.info(
        "Google News RSS: %d passed content filter, %d already in DB",
        len(articles),
        already_in_db,
    )
    logger.info("Google News RSS: %d new articles stored", new_count)
    return new_count
