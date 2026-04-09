# Paisa Paisa — Personal Finance Agent
## Project Memory

---

## Overview
Personal financial intelligence platform for an Indian retail investor.
Built with FastAPI + MongoDB Atlas + Celery + Telegram Bot.
LLM: OpenAI (GPT-4o-mini for classification, GPT-4o for weekly digest).
Embeddings: OpenAI text-embedding-3-small (1536 dimensions).
**Note:** Will switch back to Anthropic Claude when credits are available.

---

## Tech Stack
- **Backend:** FastAPI + Beanie ODM (MongoDB async)
- **Database:** MongoDB Atlas (single cluster — ledger + vector search)
- **Background jobs:** Celery + Redis
- **PDF parsing:** casparser (MF CAS), pdfminer (research PDFs)
- **Market data:** NSE direct API (via httpx), AMFI daily NAV file
- **LLM:** OpenAI GPT-4o-mini (research), GPT-4o (weekly digest)
- **Embeddings:** OpenAI text-embedding-3-small
- **Notifications:** Telegram Bot (webhook via ngrok locally)
- **Python:** 3.13

---

## Environment Variables (.env)
```
MONGO_URI=mongodb+srv://...
DB_NAME=finance_agent
ZERODHA_API_KEY=
ZERODHA_API_SECRET=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
OPENAI_API_KEY=
REDIS_URL=redis://localhost:6379/0
# Future: ANTHROPIC_API_KEY, VOYAGE_API_KEY
```

---

## Folder Structure
```
app/
├── main.py                          # FastAPI entry point
├── db.py                            # MongoDB Atlas + Beanie init
├── models/
│   ├── instruments.py               # Master: stocks, MFs, ETFs
│   ├── accounts.py                  # Bank, demat, MF folio accounts
│   ├── holdings.py                  # Current positions (+ folio_number field)
│   ├── transactions.py              # Immutable ledger
│   ├── prices.py                    # Daily price snapshots
│   ├── goals.py                     # Goals + GoalAllocation (plain ObjectIds, no Link)
│   ├── cashflows.py                 # Salary, expenses, dividends
│   ├── documents.py                 # Uploaded PDFs + parse state
│   ├── signals.py                   # Rules engine output
│   ├── recommendations.py           # Weekly digest (RecommendedAction is BaseModel not Document)
│   ├── research.py                  # News/podcast content + embeddings
│   ├── policies.py                  # PolicyRule documents
│   └── zerodha_token.py             # Daily Kite token (TTL) + field_validator for UTC
├── integrations/
│   ├── zerodha_auth.py              # Kite OAuth — use get_zerodha_auth() lazy singleton
│   ├── zerodha_sync.py              # Holdings + transactions sync (Personal plan)
│   ├── amfi_fetcher.py              # AMFI NAV file (semicolon-delimited, portal.amfiindia.com)
│   ├── market_fetcher.py            # NSE direct API (manual gzip decompression)
│   ├── market_data_sync.py          # Orchestrates AMFI + NSE price sync
│   ├── cas_parser.py                # CAS PDF ingestion (CAMS + NSDL/CDSL)
│   ├── research_fetcher.py          # RSS (Mint only) + URL scraper + PDF text extractor
│   ├── research_processor.py        # GPT-4o-mini classify + OpenAI embed
│   └── research_ingester.py         # Pipeline orchestrator with keyword pre-filter
├── services/
│   ├── telegram_service.py          # Low-level send_telegram_message()
│   ├── telegram_bot.py              # Full bot: commands + URL/PDF drops + _format_markdown()
│   ├── analytics_service.py         # Net worth, XIRR, allocation, concentration, drift, goals
│   ├── rules_engine.py              # Policy checks → Signal documents
│   └── recommendation_engine.py    # Signals + research → GPT-4o weekly digest
├── routes/
│   ├── zerodha_router.py            # /zerodha/login, /callback, /sync, /status
│   ├── market_data_router.py        # /market-data/sync, /prices, /debug/amfi
│   ├── documents_router.py          # /documents/upload/cas (auto-detects CAMS vs NSDL)
│   ├── analytics_router.py          # /analytics/snapshot, /xirr, /concentration, /drift, /goals, /report
│   ├── rules_router.py              # /rules/run, /rules/signals, /rules/signals/{id}/resolve
│   ├── research_router.py           # /research/ingest/url|rss|pdf, /items, /themes, /relevant
│   ├── recommendations_router.py    # /recommendations/generate, /latest, /history
│   └── telegram_router.py          # /telegram/webhook, /set-webhook, /status
├── tasks/
│   ├── zerodha_tasks.py             # Daily Zerodha sync + login reminder (Celery)
│   └── market_data_tasks.py         # Daily market sync + research ingestion (Celery)
└── scripts/
    ├── create_indexes.py            # Run once — MongoDB indexes + TTL
    ├── seed_instruments.py          # Seeds GOLDBEES, SILVERBEES, index instruments
    ├── seed_policies.py             # Seeds default PolicyRule documents
    ├── fix_instrument_names.py      # Backfills CNC → trading symbol for equity names
    ├── fix_ncd_holding.py           # One-time NCD quantity fix (run already)
    └── merge_zerodha_accounts.py    # One-time dedup fix (run already)
```

---

## Key Design Decisions

### MongoDB Query Syntax
**Always use dict syntax for Beanie queries, not expression syntax.**
```python
# CORRECT
Signal.find({"is_resolved": False})
Signal.find_one({"dedup_key": dedup, "is_resolved": False})

# WRONG — hangs or fails silently
Signal.find(Signal.is_resolved == False)
```

### Zerodha Auth
- Use `get_zerodha_auth()` lazy singleton everywhere (not `zerodha_auth` directly)
- Token expires at 6am IST daily — Celery sends Telegram login reminder at 7am
- Personal plan: no `kite.quote()`, no WebSockets, no historical data
- Prices come from NSE API + AMFI, not Zerodha

### CAS PDF Parser
- Auto-detects file_type from casparser output
- CAMS/KARVY/KFINTECH → `ingest_cams_kfintech_from_parsed()` (full transaction history)
- NSDL/CDSL → `ingest_nsdl_cdsl_from_parsed()` (holdings snapshot only)
- Parse once, pass dict to ingestion function (avoids double-parsing)
- NCD quantity fix: use `value / price` not `num_shares` for debt instruments
- Zerodha account dedup: matched by `account_number_masked = "dp_id-client_id"`

### AMFI NAV File
- URL: `https://portal.amfiindia.com/spages/NAVAll.txt`
- Delimiter: **semicolon** (not pipe)
- Date formats vary — try multiple formats with `_parse_nav_date()`

### NSE Market Data
- NSE returns gzip-compressed responses without Content-Encoding header
- Manual decompression via `_decode_response()` — checks magic bytes `\x1f\x8b`
- Session requires homepage cookie prefetch before API calls
- Index map: `^NSEI → "NIFTY 50"`, `^NSEBANK → "NIFTY BANK"` etc.
- `^BSESN` (Sensex) not available on NSE API — skip silently

### XIRR Calculation
- Scope: MF instruments with CAMS transaction history only
- Equity excluded: only 60 days of Zerodha order history, insufficient
- BUY/SIP = outflow (negative), REDEMPTION/SELL = inflow (positive)
- SWITCH_IN/SWITCH_OUT = excluded from overall XIRR, per-instrument only
- Skip per-instrument XIRR for switch-only instruments (no real purchase history)
- Overall XIRR of ~70% is correct for portfolio started Feb 2025 (young SIPs)

### Telegram Bot
- Security: only responds to `TELEGRAM_CHAT_ID` — rejects all other chats
- Markdown formatting: use `_format_markdown()` before sending — escapes `_` to avoid broken italics
- Webhook must return within 5s — slow commands (`/digest`) show progress message first
- Commands: `/ping`, `/start`, `/snapshot`, `/signals`, `/digest`, `/sync`, `/research`, `/help`
- URL drop → research ingestion via `ingest_url()`
- PDF drop → auto-detect CAS vs research PDF
- CAS via Telegram: currently redirects to web portal (needs password, no session state)

### Research Pipeline
- RSS: Mint Markets only (`https://www.livemint.com/rss/markets`)
- Keyword pre-filter runs BEFORE any LLM call (free)
- Only articles passing filter → GPT-4o-mini classify + OpenAI embed
- Manual drops (Telegram URL/PDF) bypass keyword filter
- `relevant_sectors` is free-form (IT, Banking, Auto, etc.) — not enum-constrained
- Cost: ~$0.00043 per article, ~$0.03 per weekly digest

### Recommendation Engine
- One GPT-4o call per week
- Input: portfolio snapshot + active signals + relevant research (last 7 days)
- Output: 1-5 prioritised actions + market narrative + policy violations
- Stored as `Recommendation` document, upserted per week_start date
- "No action" is an explicit valid output

---

## Data Quality Issues (Already Fixed)
1. **Zerodha account duplicate** — NSDL CAS created second account; merged via `merge_zerodha_accounts.py`; Zerodha account now has `account_number_masked = "12081601-24471648"`
2. **NCD quantity** — Edelweiss NCD stored qty=1000 (face value), fixed to qty=50 via `fix_ncd_holding.py`
3. **Instrument names** — Zerodha equities stored as "CNC"; fixed via `fix_instrument_names.py`
4. **XIRR sign bug** — Switch-in amounts needed to be excluded from overall XIRR
5. **Beanie query hanging** — Use dict syntax `{"field": value}` not expression syntax

---

## What's Built (Phase 1 ✓ + Phase 2 partial ✓)

### Phase 1 ✓
- MongoDB schema (13 collections)
- Zerodha API sync (holdings + transactions, Personal plan)
- CAMS detailed CAS parser (MF holdings + full transaction history)
- NSDL/CDSL CAS parser (demat holdings snapshot)
- Market data sync (NSE prices, AMFI NAVs, indices, commodities)
- Analytics engine (net worth, XIRR, allocation, concentration, drift, goal progress)
- Rules engine (6 checks → Signal documents)
- Instrument seeding (GOLDBEES, SILVERBEES, Nifty indices)
- Policy Store with default rules

### Phase 2 (partial ✓)
- Research pipeline (Mint RSS + URL/PDF drops + GPT-4o-mini + OpenAI embed)
- Recommendation engine (signals + research → GPT-4o weekly digest)
- Telegram bot (commands + drops + markdown formatting fix)

---

## What's NOT Built Yet
- `/research` Telegram command to trigger RSS ingestion manually
- Bank statement parser (cashflows, salary detection)
- Portfolio API endpoints (for frontend)
- Next.js dashboard
- Goal planning UI + GoalAllocation CRUD
- KFintech PDF (skipped — Mirae/Axis/Motilal/Invesco folios missing)
- Celery beat schedule (tasks exist but scheduler not configured)
- Atlas Vector Search index (needs manual setup via Atlas UI, 1536 dimensions for OpenAI)
- Weekly digest Celery task (manual trigger only via API/Telegram)
- WhatsApp integration (deferred)

---

## User's Portfolio Context
- **Investor:** Utkarsh Sahu, Gurgaon
- **Email:** utkarshsahu08@gmail.com
- **Zerodha:** KTA885 (CDSL DP: 12081601-24471648) — 25 equity holdings
- **CAMS MF:** HDFC, ICICI Prudential, SBI, Tata (8 schemes, ~₹12L)
- **NSDL demat:** ICICI Bank demat (IN303028-70558484) — 3 equities + 1 NCD + 2 demat MFs
- **KFintech MFs:** Mirae, Axis ELSS, Motilal, Invesco, Edelweiss Gold — NOT ingested yet
- **Total portfolio:** ~₹20-21L (post dedup fix)
- **SIPs started:** Feb 2025 — portfolio is young (~13 months)
- **XIRR:** ~70% (mathematically correct for young portfolio in bull market)

---

## Pending Decisions
- Target asset allocation not formally set (default 50/30/10/10 used for drift signals)
- KFintech PDF: skipped for now, can upload later via `/documents/upload/cas`
- Mint RSS auto-trigger: not wired to Telegram yet (manual via API only)
- Claude API: switch back when payment issue resolved

---

## How to Run
```bash
# Install
pip install -r requirements.txt

# One-time setup
python -m app.scripts.create_indexes
python -m app.scripts.seed_instruments
python -m app.scripts.seed_policies

# Start API
uvicorn app.main:app --reload --port 8000

# Celery (separate terminal)
celery -A app.tasks.zerodha_tasks.celery_app worker --loglevel=info

# ngrok (for Telegram webhook locally)
ngrok http 8000
curl -X POST "http://localhost:8000/telegram/set-webhook?url=https://xxx.ngrok.io"

# Daily flow
# 1. Morning: /zerodha/login → tap link → token stored
# 2. POST /zerodha/sync
# 3. POST /market-data/sync
# 4. POST /rules/run
# 5. (Weekly) POST /recommendations/generate