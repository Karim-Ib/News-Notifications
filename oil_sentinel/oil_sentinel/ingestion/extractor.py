"""
Article body text extraction using trafilatura.

Fetches the raw HTML via urllib (built-in, custom User-Agent, hard timeout)
then hands it to trafilatura for boilerplate-free text extraction.

Design decisions:
- sync function — callers run it in a thread executor so the event loop is never blocked
- returns None on any failure; callers fall back to title-only scoring
- truncates to MAX_BODY_CHARS before returning to keep Gemini prompts bounded
"""

import logging
import socket
import urllib.error
import urllib.request
from typing import Optional

import trafilatura

logger = logging.getLogger(__name__)

FETCH_TIMEOUT  = 10          # seconds per request
MAX_BODY_CHARS = 2000        # characters passed to Gemini

USER_AGENT = (
    "Mozilla/5.0 (compatible; OilSentinelBot/1.0; "
    "https://github.com/Karim-Ib/News-Notifications; news-scoring-bot)"
)

_HEADERS = {
    "User-Agent":      USER_AGENT,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "close",
}


def fetch_article_text(url: str) -> Optional[str]:
    """
    Fetch and extract the main body text of an article.

    Returns up to MAX_BODY_CHARS of clean text, or None if:
    - the HTTP request fails or times out
    - the page returns a non-200 status
    - trafilatura cannot identify a main content block
    - any other exception occurs

    This function is synchronous and intended for use inside
    asyncio.to_thread() / loop.run_in_executor().
    """
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            if resp.status != 200:
                logger.debug("Extractor: HTTP %d for %s", resp.status, url)
                return None
            # Read up to 2 MB — enough for any article, avoids huge pages
            html = resp.read(2_097_152).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        logger.debug("Extractor: HTTP error %d for %s", exc.code, url)
        return None
    except (urllib.error.URLError, socket.timeout, OSError) as exc:
        logger.debug("Extractor: fetch failed for %s: %s", url, exc)
        return None
    except Exception as exc:
        logger.debug("Extractor: unexpected error for %s: %s", url, exc)
        return None

    try:
        text = trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            favor_precision=True,
        )
    except Exception as exc:
        logger.debug("Extractor: trafilatura error for %s: %s", url, exc)
        return None

    if not text or not text.strip():
        return None

    return text[:MAX_BODY_CHARS]
