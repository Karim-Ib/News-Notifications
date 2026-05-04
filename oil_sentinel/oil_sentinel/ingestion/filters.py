"""
Shared content-filter patterns and title-hashing used by all ingestion sources.

Extracted here so google_news.py and gdelt.py share one definition rather
than gdelt.py exporting private symbols (_ACTOR_RE, _KEYWORD_RE, _title_hash)
that google_news.py imports across module boundaries.
"""

import hashlib
import re

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


def _title_hash(title: str) -> str:
    """Hash the first 8 normalised words of a title for near-duplicate detection."""
    words = re.sub(r"[^\w\s]", "", title.lower()).split()
    key = " ".join(words[:8])
    return hashlib.sha256(key.encode()).hexdigest()
