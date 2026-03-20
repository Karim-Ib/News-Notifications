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
| **News** | 15 min (90 min overnight) | Polls GDELT for Iran/Hormuz/OPEC articles, pre-filters, deduplicates, stores |
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

Long batches are automatically split across multiple Telegram messages (4096-char limit per message).

Every immediate alert, market anomaly, and narrative transition also sends a **WTI 24h price chart** as an image, with vertical markers showing exactly when each alert fired. Bullish markers are red (▲ supply risk), bearish markers are green (▼ supply relief), and market anomaly markers are amber (●).

### Deduplication

The system runs a three-layer dedup pipeline to prevent the same story appearing multiple times a day:

| Layer | Where | Mechanism |
|---|---|---|
| **URL hash** | Ingestion | Exact URL SHA-256 — never stores the same article twice |
| **Title hash** | Ingestion | First 8 normalised words of title — blocks near-identical headlines within 24h |
| **Narrative context** | Scoring | Recent narrative keys are injected into the Gemini prompt so it reuses the exact same key for follow-up coverage of the same story |
| **Jaccard similarity** | Scoring | Before creating an alert, word-overlap between the new `narrative_key` and all keys from the last 12h is computed — ≥75% overlap blocks the alert as a duplicate; a direction flip on the same thread bypasses the block |
| **Dispatch cooldown** | Dispatch | Same `narrative_key` cannot be re-sent for 6 hours even if a new alert slips through |

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
│   │   └── gdelt.py           # GDELT DOC API polling & pre-filtering
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

Dependencies: `aiohttp`, `yfinance`, `google-genai`, `trafilatura`, `matplotlib`, `tzdata` (for timezone support on Windows).

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
api_key = ...                   # Required
model = gemini-2.5-flash        # Model to use for scoring
batch_size = 5                  # Articles scored per 2-min cycle

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

SQLite with WAL mode. Four tables:

- **`articles`** — raw GDELT records, deduped by URL hash and 8-word title hash
- **`market_data`** — 5-min price samples with z-scores
- **`alerts`** — scored signals with dispatch state (`sent_at = NULL` until sent)
- **`narrative_states`** — full history of computed narrative states, including weighted score, momentum, counts, and `transition_alerted` flag
- **`price_watches`** — user-defined price triggers (`active=0` once fired or manually removed)

Schema migrations run automatically on startup.

---

## Bot commands

The bot responds to slash commands sent directly in the configured Telegram chat. No webhook or extra infrastructure is needed — it uses Telegram's long-polling `getUpdates` API, running as a fifth async loop alongside the monitoring loops.

| Command | Response |
|---|---|
| `/chart` | WTI 24h price chart with markers for every alert sent in the last 24h (60s cooldown) |
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
| `/help` | Command list |

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
