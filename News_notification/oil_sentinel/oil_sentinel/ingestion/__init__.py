"""News ingestion subpackage (GDELT)."""

from oil_sentinel.ingestion.gdelt import poll_and_store, fetch_gdelt_articles

__all__ = ["poll_and_store", "fetch_gdelt_articles"]
