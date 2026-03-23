# Oil Sentinel

Real-time oil-market intelligence service. Monitors geopolitical news and crude oil price movements, scores articles with AI, and dispatches structured alerts to Telegram.

---

## Disclaimer

**This software is for informational purposes only. It does not constitute financial advice, investment advice, trading advice, or any other type of advice.**

- Nothing produced by this tool should be interpreted as a recommendation to buy, sell, or hold any financial instrument.
- The authors and contributors are not financial advisors and accept no responsibility or liability for any trading or investment decisions made based on information provided by this software.
- Market data, news scores, and alerts may be inaccurate, delayed, or incomplete. Past signals do not predict future price movements.
- You use this software entirely at your own risk. The authors are not liable for any financial loss, direct or indirect, that may result from its use.

See [LICENSE](LICENSE) for full terms.

---

## What it does

Three independent loops run concurrently:

| Loop | Interval | What it does |
|---|---|---|
| **News** | 15 min (90 min overnight) | Polls GDELT for Iran/Hormuz/OPEC articles, pre-filters, deduplicates, stores; falls back to Google News RSS after 2 consecutive GDELT failures |
| **Market** | 5 min (paused overnight) | Fetches WTI & Brent futures prices, computes rolling z-scores, flags anomalies, checks price watches |
| **Scoring** | 2 min | Sends unscored articles to Gemini, creates alerts in DB, recomputes narrative state |
| **Dispatch** | every 2 min (paused overnight) | Sends qualifying alerts to Telegram immediately; attaches 24h price chart |
| **Commands** | continuous (long-poll) | Receives and handles slash commands from the configured Telegram chat |
| **Digest** | 12:00 & 20:00 local | Summarises all sub-threshold signals accumulated since last digest |
| **Morning summary** | 09:00 local (idle mode) | Concise top-7 overnight briefing covering all signals since 22:00 |

### Alert logic

Articles are scored by Gemini on a **0–10 magnitude scale**:

| Magnitude | Meaning | Delivery |
|---|---|---|
| 7–10 | Significant to historic event (3%+ expected price move) | Immediate Telegram alert |
| 4–6 | Moderate signal (1–3% move) | Twice-daily digest |
| 0–3 | Minor / noise | Twice-daily digest |

If a market price anomaly (z-score ≥ 2.0) coincides with scoring, the composite score gets a +1 bonus, making high-magnitude news even more likely to fire immediately.

**Narrative state transitions** are the highest-priority signal. When the 48-hour sentiment window shifts from one state to another (e.g. `stable → escalation`), a dedicated alert fires immediately, bypassing all cooldowns.

**Staleness guard**: When the bot restarts with a backlog of unscored articles, only articles whose GDELT index time is within `max_article_age_hours` (default: same as ingestion limit) are dispatched. Older alerts are silently marked as sent to prevent a burst of stale signals being sent against current prices. Chart markers always use article publication time (not scoring time) to ensure price/news correlation is accurate.

Long batches are automatically split across multiple Telegram messages (4096-char limit per message).

Every immediate alert, market anomaly, and narrative transition also sends a **WTI 24h price chart** as an image, with vertical markers showing exactly when each alert fired. Bullish markers are red (▲ supply risk), bearish markers are green (▼ supply relief), and market anomaly markers are amber (●).

### Deduplication

The system runs a multi-layer dedup pipeline to prevent the same story appearing multiple times a day:

| Layer | Where | Mechanism |
|---|---|---|
| **URL hash** | Ingestion | Exact URL SHA-256 — never stores the same article twice |
| **Title hash** | Ingestion | First 8 normalised words of title — blocks near-identical headlines within 24h |
| **SitRep** | Pre-scoring | Living situation report compared via Gemini — duplicate coverage is discarded without a full scoring call (see [Situation Report](#situation-report)) |
| **Narrative context** | Scoring | Recent narrative keys are injected into the Gemini prompt so it reuses the exact same key for follow-up coverage of the same story |
| **Jaccard similarity** | Scoring | Before creating an alert, word-overlap between the new `narrative_key` and all keys from the last 12h is computed — ≥75% overlap blocks the alert as a duplicate; a direction flip on the same thread bypasses the block |
| **Dispatch cooldown** | Dispatch | Same `narrative_key` cannot be re-sent for 6 hours even if a new alert slips through |

### Situation Report

The situation report is a living document that summarises what the system already knows across six topic sections:

```
SITUATION REPORT — Last updated: 2024-03-21 18:00 UTC

MILITARY:   ongoing strikes, casualties, hardware
HORMUZ STATUS: closure state, ship traffic, toll arrangements
DIPLOMATIC: negotiations, coalitions, ceasefire proposals
SUPPLY & ROUTING: OPEC decisions, alternative routes, force majeure
MARKET CONTEXT: WTI/Brent levels, key price drivers already priced in
REGIONAL IMPACT: country-specific effects (Pakistan, India, China, …)
```

**Pre-scoring dedup flow:**

1. Load current report from DB
2. Send article title + first 2 000 chars of body to Gemini with the report
3. If the article is **new** (contains specific facts not in the report):
   - Append the new fact to the appropriate section with a timestamp
   - Forward article to the regular Gemini scoring pipeline
4. If the article is **duplicate** (same event, different source):
   - Mark article as skipped — no scoring call consumed
   - Log: `SitRep: DUPLICATE — <reason>`

**What counts as new:**
- Evolution of a known story counts (threat → action, proposal → agreement, unconfirmed → confirmed)
- Updated numbers count (casualty count revised, specific dollar amount added)
- Same event reported by a different source does **not** count
- Opinion / analysis about known events does **not** count

**Compaction:** when the report exceeds 12 000 characters (~3 000 tokens), Gemini compacts it to ~6 000 characters. Entries about the same topic are merged, superseded facts are dropped, and specific numbers/names are preserved. Previous versions are kept in the database for rollback.

**Hourly stats** are logged: `SitRep stats: N new / M filtered in last hour`.

**Telegram:** `/sitrep` sends the current report. `/status` appends it after the standard status block (or sends it as a follow-up if the combined message would exceed 4 096 chars).

---

## Project structure

```
oil_sentinel/
├── oil_sentinel/              # Python package
│   ├── config.py              # Typed config loader (dataclasses)
│   ├── db/
│   │   ├── __init__.py
│   │   └── models.py          # SQLite schema + CRUD (articles, alerts, market_data, narrative_states)
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── gdelt.py           # GDELT DOC API polling & pre-filtering
│   │   └── google_news.py     # Google News RSS backup source (feedparser)
│   ├── market/
│   │   ├── __init__.py
│   │   └── poller.py          # yfinance price fetching + z-score anomaly detection
│   ├── narrative/
│   │   ├── __init__.py
│   │   └── engine.py          # 48h rolling sentiment state, momentum, key drivers, transition detection
│   ├── charts/
│   │   ├── __init__.py
│   │   └── price_chart.py     # Matplotlib WTI intraday chart generator
│   ├── scoring/
│   │   ├── __init__.py
│   │   └── gemini.py          # Gemini AI article scoring
│   ├── portfolio/
│   │   ├── __init__.py
│   │   ├── tracker.py         # ETP price cache, position calculation, hourly snapshots
│   │   └── chart.py           # Portfolio value vs invested chart (matplotlib)
│   ├── accuracy/
│   │   ├── __init__.py
│   │   └── evaluator.py       # Daily prediction accuracy grading + /accuracy report formatter
│   ├── sitrep.py              # Situation-report dedup (living doc, Gemini compaction)
│   └── notifications/
│       ├── __init__.py
│       ├── telegram.py        # Alert formatting + Telegram dispatch (including narrative transitions)
│       └── commands.py        # Interactive bot commands (long-polling getUpdates)
├── main.py                    # Async orchestrator — run this
├── diagnostics.py             # CLI for inspecting DB stats and Gemini prompt previews
├── config.ini                 # Your local config (not committed)
├── config.ini.example         # Template
├── pyproject.toml
└── requirements.txt
```

---

## Setup

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd oil_sentinel
pip install -r requirements.txt
```

Dependencies: `aiohttp`, `yfinance`, `google-genai`, `trafilatura`, `matplotlib`, `tzdata` (for timezone support on Windows), `feedparser` (for Google News RSS).

### 2. Configure

```bash
cp config.ini.example config.ini
```

Edit `config.ini` and fill in the three required secrets:

| Key | Where to get it |
|---|---|
| `[gemini] api_key` | [Google AI Studio](https://aistudio.google.com) → API keys |
| `[telegram] bot_token` | Message `@BotFather` on Telegram → `/newbot` |
| `[telegram] chat_id` | Add your bot to a group/channel, then check the Telegram API or use `@userinfobot` |

### 3. Run

```bash
python main.py
```

Logs go to stdout and `oil_sentinel.log` (rotating, 10 MB max, 3 backups).

---

## Configuration reference

```ini
[database]
path = oil_sentinel.db          # SQLite file path

[gdelt]
poll_interval_minutes = 15      # How often to query GDELT
max_records = 250               # Articles fetched per query
tone_threshold = 2.5            # Min |tone| for tier-1 sources to pass
unknown_source_tone_threshold = 5.0   # Stricter threshold for unknown sources
max_article_age_hours = 12      # Drop articles older than this
min_relevance = 60              # GDELT relevance score floor (0-100)
tier1_sources = reuters.com,bloomberg.com,...

[market]
poll_interval_minutes = 5       # Price fetch interval
wti_ticker = CL=F               # WTI front-month futures (NYMEX)
brent_ticker = BZ=F             # Brent front-month futures (ICE)
zscore_window = 288             # Rolling window in samples (288 × 5min = 24h)
zscore_threshold = 2.0          # |z-score| to flag as anomaly

[gemini]
api_key = ...                        # Required
scoring_model = gemini-2.5-flash     # Full scoring and sitrep compaction
dedup_model = gemini-2.5-flash-lite  # Sitrep dedup check (omit to use scoring_model)
batch_size = 5                       # Articles scored per 2-min cycle

[telegram]
bot_token = ...                 # Required
chat_id = ...                   # Required (group/channel ID, e.g. -100xxxxxxxxx)
cooldown_minutes = 360          # Min gap before re-alerting on same story thread (6h)
alert_threshold = 7             # Min magnitude (0-10) for immediate alert
digest_hours = 12,20            # Local server time hours to send the background signal digest

[idle]
enabled = true                  # Overnight idle mode — switches automatically, no manual toggle
overnight_start = 22            # Local server time hour: alerts suppressed, market polling paused
overnight_end = 9               # Local server time hour: window ends, morning summary fires
poll_interval_minutes = 90      # GDELT poll interval during overnight window
morning_summary_hour = 9        # Local server time hour for the overnight briefing (match overnight_end)

[google_news]
enabled = true                  # Enable Google News RSS fallback (triggered after 2 consecutive GDELT failures)
```

---

## How anomaly detection works

The market loop computes a **rolling z-score** over the last 24 hours of 5-minute price samples:

```
z = (current_price − 24h_mean) / 24h_std_dev
```

A z-score of ±2 means the move is more than 2 standard deviations from the day's baseline — statistically unusual. Unlike a fixed percentage threshold, the z-score self-adjusts: during already-volatile periods the standard deviation widens, so the bar for flagging rises automatically, suppressing noise.

On anomaly detection:
- A standalone Telegram alert fires immediately (once per cooldown window)
- The scoring loop boosts article composite scores by +1 for that cycle

Known limitations: futures roll-date jumps and the first print after a market-closed gap may generate false positives.

---

## Scoring model

Gemini scores each article on six dimensions:

| Field | Values |
|---|---|
| `direction` | `bullish` / `bearish` / `neutral` |
| `magnitude` | 0–10 integer |
| `confidence` | 0.0–1.0 float (source credibility × certainty) |
| `event_type` | Taxonomy label (e.g. `military_action`, `ceasefire`, `opec_cut`) |
| `narrative_key` | Snake-case slug grouping follow-up articles into threads |
| `summary` + `detail` | Human-readable headline + 2-sentence mechanism/outlook |

`composite_score = magnitude × confidence` (+ 1 if concurrent market anomaly).

Duplicate narratives are suppressed by a three-layer dedup pipeline (see [Deduplication](#deduplication) above) and a 6-hour per-`narrative_key` dispatch cooldown.

---

## Narrative trend tracking

Every scoring cycle, the system recomputes a **rolling 48-hour sentiment state** from all scored alerts in the window.

### Weighted sentiment score

Each alert contributes:
```
direction_sign × magnitude × confidence × tier_weight
```
- `direction_sign`: `+1` bullish, `−1` bearish, `0` neutral
- `tier_weight`: `1.2` for tier-1 sources (Reuters, Bloomberg, etc.), `1.0` for others

The contributions are averaged across all alerts in the window.

### State classification

| Score | State |
|---|---|
| ≥ 3.0 | 🔴 `strong_escalation` |
| ≥ 1.0 | 🟠 `escalation` |
| −1.0 to 1.0 | 🟡 `stable` |
| ≤ −1.0 | 🟢 `de_escalation` |
| ≤ −3.0 | 💚 `strong_de_escalation` |

### Momentum

Compares the previous 12h window score to the current 12h window score:
- Δ > +0.5 → **strengthening**
- Δ < −0.5 → **weakening**
- else → **stable**

### State transitions

When the state changes, a dedicated alert fires immediately — **no cooldown, no threshold gate**. It includes:
- Previous state → new state
- 48h article counts (bullish / bearish / neutral) and average magnitudes per direction
- Weighted sentiment score
- Momentum indicator
- Top-3 key driver articles (highest magnitude in the direction matching the new state)
- Live WTI price

Narrative state is also shown as context in the header of every regular immediate alert.

---

## Database

SQLite with WAL mode. Eight tables:

- **`articles`** — raw GDELT records, deduped by URL hash and 8-word title hash
- **`market_data`** — 5-min price samples with z-scores
- **`alerts`** — scored signals with dispatch state (`sent_at = NULL` until sent)
- **`narrative_states`** — full history of computed narrative states, including weighted score, momentum, counts, and `transition_alerted` flag
- **`price_watches`** — user-defined price triggers (`active=0` once fired or manually removed)
- **`portfolios`** — named ETP tracking entries (`long` → 3OIL.MI, `short` → 3OIS.MI)
- **`transactions`** — buy/sell records with EUR amount, price per unit, and units
- **`portfolio_snapshots`** — hourly snapshots of portfolio value and P/L for charting and statistics
- **`situation_reports`** — versioned situation report rows; `previous_id` links to prior version for rollback; `compacted_from` is set when a row was created by compaction
- **`daily_scores`** — one row per UTC date; stores the end-of-day narrative state, net direction, WTI open/close prices, and whether the prediction was correct

Schema migrations run automatically on startup.

---

## Prediction accuracy scoring

Every day at 22:00 UTC the `digest_loop` runs a lightweight accuracy evaluation that grades whether the end-of-day narrative sentiment predicted WTI price direction.

### How grading works

1. The latest narrative state for that UTC date is read from `narrative_states`.
2. WTI open (first `market_data` sample of the day) and close (last sample) are read.
3. The `weighted_score` is mapped to a net direction: `bullish` (> +0.5), `bearish` (< −0.5), or `stable`.
4. The day is graded:

| Condition | Result |
|---|---|
| `\|change\|` < 0.3% | Skipped — insufficient market movement |
| bullish + price up > 0.3% | Correct |
| bearish + price down > 0.3% | Correct |
| stable + `\|change\|` < 1.0% | Correct |
| Any other combination | Wrong |

Results are stored in `daily_scores` (one row per date, idempotent).

### Telegram commands

- `/accuracy` — full report for the last 30 days: overall accuracy %, current win/loss streak, breakdown by narrative state, and a per-day summary of the last 7 days
- `/accuracy 7d` — last 7 days only
- `/accuracy all` — all-time history

The morning summary also includes a one-liner (e.g. `📊 Accuracy: 69.2% (18/26) last 30d | streak: ✅✅✅`) once at least 5 graded days exist.

---

## Bot commands

The bot responds to slash commands sent directly in the configured Telegram chat. No webhook or extra infrastructure is needed — it uses Telegram's long-polling `getUpdates` API, running as a fifth async loop alongside the monitoring loops.

| Command | Response |
|---|---|
| `/chart` | WTI 7-day price+narrative chart (default). `/chart 1` for 24h intraday. `/chart <days>` for 1–30 days. 60s cooldown. |
| `/status` | Narrative state, live WTI price + z-score, anomaly flag, active mode, timezone |
| `/watch wti below 85 Entry` | Set a price alert — fires once when WTI crosses $85 |
| `/watch brent above 95` | Set a Brent alert with no label |
| `/watches` | List all active price watches |
| `/unwatch 1` | Remove watch #1 |
| `/unwatch all` | Remove all active watches |
| `/editwatch 1 88.50` | Change watch #1's target price (was $85 → now $88.50) |
| `/idle` | Show idle mode status and subcommands |
| `/idle on` | Force idle mode immediately (suspends automatic schedule) |
| `/idle off` | Force normal mode immediately (suspends automatic schedule) |
| `/idle auto` | Return to automatic time-based switching |
| `/idle tz Europe/Berlin` | Set the timezone used for the overnight window (returns to auto) |
| `/idle tz local` | Revert to server local time |
| `/sitrep` | Show the current situation report (all six sections) |
| `/accuracy` | Prediction accuracy stats for the last 30 days (default). `/accuracy 7d` or `/accuracy all` for other windows. |
| `/help` | Command list |

### Portfolio commands

| Command | Response |
|---|---|
| `/portfolio create hormuz-short short` | Create a portfolio tracking WTI 3x Daily Short (3OIS.MI) |
| `/portfolio create oil-bull long` | Create a portfolio tracking WTI 3x Daily Long (3OIL.MI) |
| `/portfolio hormuz-short` | Show current state: units, avg cost, live value, P/L |
| `/portfolios` | List all active portfolios with one-line P/L summary |
| `/buy hormuz-short 100` | Record a €100 purchase at current live price |
| `/sell hormuz-short 50` | Record a €50 sale at current live price |
| `/sell hormuz-short all` | Sell all held units |
| `/portfolio history hormuz-short` | Full transaction log |
| `/portfolio chart hormuz-short 30d` | Portfolio value vs invested chart (7d / 30d / 90d / all) |
| `/portfolio stats hormuz-short` | Detailed statistics: best/worst day, max drawdown, DCA averages |
| `/portfolio delete hormuz-short` | Deactivate portfolio (prompts `/confirm`; history preserved) |
| `/confirm` | Confirm a pending destructive action |

**Products supported:**
- `long` → WisdomTree WTI 3x Daily Leveraged (3OIL.MI, Milan). Fallback: 3OIL.L (London)
- `short` → WisdomTree WTI 3x Daily Short (3OIS.MI, Milan). Fallback: 3OIS.L (London)

ETP prices are fetched via yfinance with a 5-minute cache. Hourly snapshots are stored in `portfolio_snapshots` for charting and statistics. A one-line summary per portfolio is included in the morning briefing.

Commands are registered with Telegram on startup via `setMyCommands`, so they appear as autocomplete suggestions when you type `/` in the chat. They are silently ignored if sent from any chat other than the configured `chat_id`.

To add a new command, edit the `BOT_COMMANDS` list in `notifications/commands.py` — the `/help` text, the autocomplete list, and the allowed-command guard all derive from that single list automatically.

---

## Diagnostics CLI

`diagnostics.py` lets you inspect the database and preview exactly what Gemini receives without running a full scoring cycle.

```bash
# Extraction success rates by source, body text length distribution, recent failures
python diagnostics.py stats

# Preview the Gemini prompt for the most recent unscored article
python diagnostics.py prompt

# Preview for a specific article ID
python diagnostics.py prompt --id 42

# List recent articles with their IDs, scoring status, and body text availability
python diagnostics.py prompt --list
```

The `prompt` subcommand renders the exact system + user prompt that would be sent to Gemini, including the current narrative thread context from the database.

---

## Idle / overnight mode

When `[idle] enabled = true`, the system automatically switches modes based on local server time:

| Mode | Hours | Behaviour |
|---|---|---|
| **Normal** | `overnight_end` → `overnight_start` | All loops run at full frequency |
| **Overnight / idle** | `overnight_start` → `overnight_end` | GDELT polls every 90 min; market polling paused; no immediate alerts |

Mode switches are detected automatically every polling cycle — no restart or manual toggle needed. Transitions are logged:
```
Switching to overnight/idle mode (poll interval 90m)
Returning to normal mode (poll interval 15m)
```

At `morning_summary_hour` (local time), a concise briefing covering all signals accumulated during the night is sent to Telegram.

---

## Telegram message format

### Charts (/chart command)

`/chart` sends **two separate images** in sequence:

**Image 1 — Clean price chart**
WTI price line with alert dispatch times as coloured dashed verticals (▲ red = bullish, ▼ green = bearish, ● amber = anomaly). Legend shows directions present in the window.

**Image 2 — Price vs narrative chart**
Same price line, but with coloured background shading showing the active narrative state. State transitions are marked with a dotted vertical line and a directional arrow: ▲▲ strong escalation · ▲ escalation · → stable · ▼ de-escalation · ▼▼ strong de-escalation. This lets you directly see whether narrative shifts correlate with price movements. Legend lists all states present in the window.

`/chart 1` sends only the clean price chart (24h intraday). Narrative transition alerts send both charts automatically (clean 24h + 7d price-vs-narrative).

---

### Immediate alert
```
🛢 OIL SENTINEL · 2 signals · 2026-03-19 18:34 UTC
────────────────────────
🟠 HIGH  📈 Oil Prices Spike After Iran Strikes Gulf Refinery
██████████ 8/10 · BULLISH · Infrastructure Attack · 80% conf · score 6.4
rigzone.com  ·  19 Mar 2026 18:20 UTC

Supply disruption affects ~400kb/d of Gulf Coast processing capacity.
Prices likely to test $98 in 24h; key risk is US diplomatic intervention.
thread: iran_gulf_refinery_strikes
```

### Immediate alert (with narrative context)
```
🛢 OIL SENTINEL · 2 signals · 2026-03-19 18:34 UTC
📊 Narrative: 🟠 ESCALATION  ↑ strengthening
────────────────────────
🟠 HIGH  📈 Oil Prices Spike After Iran Strikes Gulf Refinery
██████████ 8/10 · BULLISH · Infrastructure Attack · 80% conf · score 6.4
rigzone.com  ·  19 Mar 2026 18:20 UTC

Supply disruption affects ~400kb/d of Gulf Coast processing capacity.
Prices likely to test $98 in 24h; key risk is US diplomatic intervention.
thread: iran_gulf_refinery_strikes
```

### Narrative state transition alert
```
🔄 NARRATIVE SHIFT · OIL SENTINEL · 2026-03-19 16:02 UTC
────────────────────────
🟡 STABLE → 🟠 ESCALATION

📊 48h window: 8 bullish avg mag 6.3 · 3 bearish avg mag 3.8 · 2 neutral
   Weighted sentiment score: +1.83
📈 Momentum: ↑ strengthening
💰 WTI: $84.37

🔑 Key Drivers
  1. 📈 [8/10] Iran closes Hormuz for naval exercises  reuters.com
  2. 📈 [7/10] Saudi Aramco halts Red Sea exports  bloomberg.com
  3. 📈 [6/10] OPEC emergency session called  oilprice.com
```

### Digest
```
📰 OIL SENTINEL — Noon Digest · 2026-03-19 12:00 UTC
14 background signals since last digest
────────────────────────
📈 BULLISH SIGNALS
📈 [6/10] Saudi Aramco suspends Red Sea shipments  oilprice.com
   Supply Disruption · 75% conf
...
```
