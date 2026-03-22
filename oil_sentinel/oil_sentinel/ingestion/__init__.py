"""News ingestion subpackage (GDELT + Google News RSS backup)."""

from oil_sentinel.ingestion.gdelt import poll_and_store, fetch_gdelt_articles
from oil_sentinel.ingestion import google_news

__all__ = ["poll_and_store", "fetch_gdelt_articles", "google_news"]
