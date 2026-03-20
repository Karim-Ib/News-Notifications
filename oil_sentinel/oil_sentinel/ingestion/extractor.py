"""
Article body text extraction using trafilatura.

Fetches the raw HTML via urllib (built-in, custom User-Agent, hard timeout)
then hands it to trafilatura for boilerplate-free text extraction.

Design decisions:
- sync function — callers run it in a thread executor so the event loop is never blocked
- returns None on any failure; callers fall back to title-only scoring
- truncates to MAX_BODY_CHARS before returning to keep Gemini prompts bounded
"""

import gzip
import logging
import socket
import urllib.error
import urllib.request
import zlib
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
    "Accept-Encoding": "identity",   # ask for uncompressed — urllib doesn't auto-decompress
    "Connection":      "close",
}


def _decompress(raw: bytes, content_encoding: str) -> bytes:
    """Decompress response body if the server sent it compressed anyway."""
    enc = content_encoding.lower()
    try:
        if "gzip" in enc:
            return gzip.decompress(raw)
        if "deflate" in enc:
            return zlib.decompress(raw)
    except Exception:
        pass
    return raw


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
    logger.info("Extractor: fetching %s", url)

    html: Optional[str] = None
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            status = resp.status
            content_encoding = resp.headers.get("Content-Encoding", "")
            raw = resp.read(2_097_152)
            raw = _decompress(raw, content_encoding)
            content_len = len(raw)
            html = raw.decode("utf-8", errors="replace")
            logger.info(
                "Extractor: HTTP %d  content_length=%d bytes  encoding=%r  url=%s",
                status, content_len, content_encoding or "none", url,
            )
            if status != 200:
                logger.warning("Extractor: non-200 status %d — skipping  url=%s", status, url)
                return None
    except urllib.error.HTTPError as exc:
        logger.warning("Extractor: HTTP error %d  url=%s", exc.code, url)
        return None
    except urllib.error.URLError as exc:
        logger.warning("Extractor: URL error (%s)  url=%s", exc.reason, url)
        return None
    except socket.timeout:
        logger.warning("Extractor: timed out after %ds  url=%s", FETCH_TIMEOUT, url)
        return None
    except OSError as exc:
        logger.warning("Extractor: OS error (%s)  url=%s", exc, url)
        return None
    except Exception as exc:
        logger.warning("Extractor: unexpected fetch error (%s: %s)  url=%s", type(exc).__name__, exc, url)
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
        logger.warning("Extractor: trafilatura raised %s: %s  url=%s", type(exc).__name__, exc, url)
        return None

    if not text or not text.strip():
        preview = html[:200].encode("ascii", errors="replace").decode("ascii")
        logger.warning(
            "Extractor: trafilatura returned None  html_preview=%r  url=%s",
            preview, url,
        )
        return None

    logger.info("Extractor: extracted %d chars  url=%s", len(text), url)
    return text[:MAX_BODY_CHARS]
