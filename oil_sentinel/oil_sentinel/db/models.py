"""
Backward-compatibility shim.

All definitions now live in the db sub-modules (connection, articles, market,
alerts, narrative, watches, portfolio, situation_reports, accuracy).
This file re-exports every public symbol so that any code which imports
directly from oil_sentinel.db.models continues to work unchanged.
"""

from oil_sentinel.db.connection import (       # noqa: F401
    get_connection,
    transaction,
    init_db,
    SCHEMA,
    SITREP_SCHEMA,
    PORTFOLIO_SCHEMA,
    ACCURACY_SCHEMA,
)
from oil_sentinel.db.articles import (         # noqa: F401
    url_hash,
    title_hash_exists,
    article_exists,
    insert_article,
    update_article_body,
    get_unscored_articles,
    mark_article_scored,
)
from oil_sentinel.db.market import (           # noqa: F401
    insert_market_sample,
    get_recent_prices,
    get_price_history,
    latest_market_sample,
)
from oil_sentinel.db.alerts import (           # noqa: F401
    insert_alert,
    mark_alert_sent,
    get_recent_narrative_keys,
    narrative_exists_recent,
    last_sent_for_narrative,
    get_recently_sent_alerts,
    get_unsent_alerts,
)
from oil_sentinel.db.narrative import (        # noqa: F401
    insert_narrative_state,
    get_narrative_history,
    get_latest_narrative_state,
    mark_narrative_transition_alerted,
    prune_old_narrative_states,
)
from oil_sentinel.db.watches import (          # noqa: F401
    insert_watch,
    get_active_watches,
    get_active_watches_for_ticker,
    get_watch_by_id,
    trigger_watch,
    deactivate_watch,
    deactivate_all_watches,
    update_watch_price,
)
from oil_sentinel.db.portfolio import (        # noqa: F401
    insert_portfolio,
    get_portfolio_by_name,
    get_portfolio_by_id,
    get_active_portfolios,
    deactivate_portfolio,
    insert_transaction,
    get_transactions,
    insert_portfolio_snapshot,
    get_portfolio_snapshots,
    get_last_portfolio_snapshot,
)
from oil_sentinel.db.situation_reports import (  # noqa: F401
    get_current_sitrep,
    insert_sitrep_row,
)
from oil_sentinel.db.accuracy import (         # noqa: F401
    insert_daily_score,
    daily_score_exists,
    get_daily_scores,
)
