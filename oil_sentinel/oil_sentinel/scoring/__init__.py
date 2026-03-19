"""Scoring subpackage (Gemini AI article scoring)."""

from oil_sentinel.scoring.gemini import make_gemini_client, score_pending_articles

__all__ = ["make_gemini_client", "score_pending_articles"]
