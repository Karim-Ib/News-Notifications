"""
Typed configuration loader for oil_sentinel.

Reads config.ini and exposes a single Config dataclass instance.
All callers import from here instead of touching configparser directly.
"""

import configparser
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class GdeltConfig:
    poll_interval_minutes: int
    base_url: str
    max_records: int
    tone_threshold: float
    unknown_source_tone_threshold: float
    max_article_age_hours: int
    min_relevance: int
    tier1_domains: set[str]


@dataclass
class MarketConfig:
    poll_interval_minutes: int
    wti_ticker: str
    brent_ticker: str
    zscore_window: int
    zscore_threshold: float

    @property
    def tickers(self) -> list[str]:
        return [self.wti_ticker, self.brent_ticker]


@dataclass
class GeminiConfig:
    api_key: str
    model: str
    batch_size: int


@dataclass
class TelegramConfig:
    bot_token: str
    chat_id: str
    cooldown_minutes: int
    alert_threshold: int       # minimum magnitude for immediate alert (0-10)
    digest_hours: list[int]    # UTC hours to send daily digest, e.g. [12, 20]


@dataclass
class IdleConfig:
    enabled: bool
    overnight_start: int        # UTC hour when overnight window begins (e.g. 22)
    overnight_end: int          # UTC hour when overnight window ends (e.g. 9)
    poll_interval_minutes: int  # GDELT poll interval during overnight window
    morning_summary_hour: int   # UTC hour to send the overnight summary (e.g. 9)


@dataclass
class LoggingConfig:
    level: str
    file: str


@dataclass
class Config:
    db_path: str
    gdelt: GdeltConfig
    market: MarketConfig
    gemini: GeminiConfig
    telegram: TelegramConfig
    logging: LoggingConfig
    idle: IdleConfig


def load(config_path: Path) -> Config:
    """Parse config.ini and return a fully typed Config. Exits on missing file."""
    if not config_path.exists():
        print(
            f"ERROR: {config_path} not found.\n"
            "Copy config.ini.example -> config.ini and fill in your keys.",
            file=sys.stderr,
        )
        sys.exit(1)

    raw = configparser.ConfigParser()
    raw.read(config_path)

    tier1_raw = raw.get("gdelt", "tier1_sources", fallback="reuters.com,bloomberg.com,apnews.com")
    tier1_domains = {d.strip() for d in tier1_raw.split(",") if d.strip()}

    return Config(
        db_path=raw.get("database", "path", fallback="oil_sentinel.db"),
        gdelt=GdeltConfig(
            poll_interval_minutes=raw.getint("gdelt", "poll_interval_minutes", fallback=15),
            base_url=raw.get("gdelt", "base_url", fallback="https://api.gdeltproject.org/api/v2/doc/doc"),
            max_records=raw.getint("gdelt", "max_records", fallback=250),
            tone_threshold=raw.getfloat("gdelt", "tone_threshold", fallback=2.5),
            unknown_source_tone_threshold=raw.getfloat("gdelt", "unknown_source_tone_threshold", fallback=5.0),
            max_article_age_hours=raw.getint("gdelt", "max_article_age_hours", fallback=12),
            min_relevance=raw.getint("gdelt", "min_relevance", fallback=60),
            tier1_domains=tier1_domains,
        ),
        market=MarketConfig(
            poll_interval_minutes=raw.getint("market", "poll_interval_minutes", fallback=5),
            wti_ticker=raw.get("market", "wti_ticker", fallback="CL=F"),
            brent_ticker=raw.get("market", "brent_ticker", fallback="BZ=F"),
            zscore_window=raw.getint("market", "zscore_window", fallback=288),
            zscore_threshold=raw.getfloat("market", "zscore_threshold", fallback=2.0),
        ),
        gemini=GeminiConfig(
            api_key=raw.get("gemini", "api_key", fallback=""),
            model=raw.get("gemini", "model", fallback="gemini-2.5-flash"),
            batch_size=raw.getint("gemini", "batch_size", fallback=5),
        ),
        telegram=TelegramConfig(
            bot_token=raw.get("telegram", "bot_token", fallback=""),
            chat_id=raw.get("telegram", "chat_id", fallback=""),
            cooldown_minutes=raw.getint("telegram", "cooldown_minutes", fallback=60),
            alert_threshold=raw.getint("telegram", "alert_threshold", fallback=7),
            digest_hours=[
                int(h.strip())
                for h in raw.get("telegram", "digest_hours", fallback="12,20").split(",")
                if h.strip()
            ],
        ),
        logging=LoggingConfig(
            level=raw.get("logging", "level", fallback="INFO"),
            file=raw.get("logging", "file", fallback="oil_sentinel.log"),
        ),
        idle=IdleConfig(
            enabled=raw.getboolean("idle", "enabled", fallback=False),
            overnight_start=raw.getint("idle", "overnight_start", fallback=22),
            overnight_end=raw.getint("idle", "overnight_end", fallback=9),
            poll_interval_minutes=raw.getint("idle", "poll_interval_minutes", fallback=90),
            morning_summary_hour=raw.getint("idle", "morning_summary_hour", fallback=9),
        ),
    )
