# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Paisa Paisa** is a personal finance agent for Indian retail investor Utkarsh Sahu. It ingests portfolio data from Zerodha (equities), mutual fund CAS PDFs, and live market feeds, then applies a rules engine and GPT-4o to generate actionable weekly investment digests delivered via Telegram.

## Commands

### Run the application locally

```bash
# Terminal 1: FastAPI dev server
uvicorn main:app --reload --port 8000

# Terminal 2: Celery worker
celery -A tasks.zerodha_tasks.celery_app worker --loglevel=info

# Terminal 3: ngrok for Telegram webhook
ngrok http 8000
curl -X POST "http://localhost:8000/telegram/set-webhook?url=https://xxx.ngrok.io"
```

### Lint and format

```bash
ruff check .
ruff format .
```

### One-time database setup

```bash
python -m scripts.create_indexes
python -m scripts.seed_instruments
python -m scripts.seed_policies
```

## Architecture

Single FastAPI monolith deployed as two Railway services (API + Celery worker) from the same codebase. Entry point is `main.py` at the repo root (not nested under `app/`).

### Layer overview

| Layer | Directory | Purpose |
|-------|-----------|---------|
| Data models | `models/` | 14 Beanie ODM documents (MongoDB) |
| API | `routes/` | 8 FastAPI routers |
| Business logic | `services/` | Analytics, rules engine, recommendations, Telegram |
| Data connectors | `integrations/` | Zerodha, NSE/AMFI, CAS PDFs, research pipeline |
| Background jobs | `tasks/` | Celery beat scheduled tasks |
| Admin scripts | `scripts/` | One-off DB setup and seeding |

### Daily data flow

1. **7:00 AM** — Celery sends Telegram login reminder with Zerodha OAuth URL
2. **User taps link** → `/zerodha/callback` exchanges `request_token` → stores `access_token` in MongoDB
3. **9:05 AM** — Celery runs full sync: Zerodha holdings/transactions → NSE/AMFI prices → MongoDB upsert
4. **Post-sync** — Rules engine checks policies, generates deduplicated `Signal` documents, notifies Telegram
5. **3:45 PM** — Lightweight price refresh only
6. **Weekly (Monday)** — `POST /recommendations/generate` pulls signals + research + portfolio snapshot → GPT-4o digest → Telegram

## Critical Design Rules

### Beanie query syntax — always use dict syntax
Expression syntax hangs or fails silently. This is a known issue.
```python
# CORRECT
Signal.find({"is_resolved": False})
Signal.find_one({"dedup_key": dedup, "is_resolved": False})

# WRONG — hangs silently
Signal.find(Signal.is_resolved == False)
```

### Zerodha auth
- Use `get_zerodha_auth()` lazy singleton (not the module-level `zerodha_auth` directly)
- Personal plan limitations: no `kite.quote()`, no WebSockets, no historical data
- Prices come from NSE API + AMFI, **not** Zerodha

### Holdings vs transactions
- Holdings: UPSERT (fresh snapshot each sync)
- Transactions: INSERT-only with `dedup_hash` — never overwrite

### Signal deduplication
Each `Signal` has a `dedup_key` hash — re-running the rules engine won't create duplicate alerts.

### NSE market data quirks
- Returns gzip-compressed responses **without** `Content-Encoding` header — manual decompression via `_decode_response()` checks magic bytes `\x1f\x8b`
- Session requires homepage cookie prefetch before API calls
- `^BSESN` (Sensex) not available on NSE API — skip silently

### AMFI NAV file
- URL: `https://portal.amfiindia.com/spages/NAVAll.txt`
- Delimiter: **semicolon** (not pipe)
- Date formats vary — handled by `_parse_nav_date()`

### CAS PDF parser (`integrations/cas_parser.py`)
- Auto-detects type from casparser output
- CAMS/KARVY/KFINTECH → `ingest_cams_kfintech_from_parsed()` (full transaction history)
- NSDL/CDSL → `ingest_nsdl_cdsl_from_parsed()` (holdings snapshot only)
- NCD quantity: use `value / price`, not `num_shares` (face value mismatch)

### XIRR calculation
- MF instruments with CAMS transaction history only — equities excluded (only 60 days of Zerodha order history)
- BUY/SIP = outflow (negative), REDEMPTION/SELL = inflow (positive)
- SWITCH_IN/SWITCH_OUT excluded from overall XIRR
- ~70% XIRR is mathematically correct for this portfolio (started Feb 2025, young SIPs in bull market)

### Telegram bot
- Security: rejects all chats except `TELEGRAM_CHAT_ID`
- Always run `_format_markdown()` before sending — escapes `_` to avoid broken italics
- Webhook must return within 5s — slow commands (e.g. `/digest`) send a progress message first
- URL drop → research ingestion; PDF drop → auto-detect CAS vs research

### Research pipeline
- RSS source: Mint Markets only (`https://www.livemint.com/rss/markets`)
- Keyword pre-filter runs **before** any LLM call (free); only passing articles → GPT-4o-mini
- Manual URL/PDF drops via Telegram bypass the keyword filter
- Cost: ~$0.00043/article, ~$0.03/week for digest

## LLM / Embedding Stack

- **Classification:** OpenAI GPT-4o-mini (`integrations/research_processor.py`)
- **Weekly digest:** OpenAI GPT-4o (`services/recommendation_engine.py`)
- **Embeddings:** OpenAI `text-embedding-3-small` (1536 dimensions) — Voyage AI is in `requirements.txt` but OpenAI embeddings are active
- **Planned:** Switch back to Anthropic Claude when API credits are available (`ANTHROPIC_API_KEY` in `.env`)

## Environment Variables

```
MONGO_URI=
DB_NAME=finance_agent
ZERODHA_API_KEY=
ZERODHA_API_SECRET=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
OPENAI_API_KEY=
REDIS_URL=redis://localhost:6379/0
# Future: ANTHROPIC_API_KEY
```

## What's Not Built Yet

- Celery beat schedule not configured (tasks exist but scheduler needs wiring)
- Atlas Vector Search index needs manual setup in Atlas UI (1536 dimensions)
- KFintech PDF ingestion (Mirae, Axis ELSS, Motilal, Invesco, Edelweiss folios)
- Bank statement parser (cashflows, salary detection)
- Weekly digest Celery task (manual trigger only via `POST /recommendations/generate`)
- Next.js dashboard + Goal planning UI
- WhatsApp integration (deferred)

## Known Data Fixes (Already Applied)

These scripts in `scripts/` were one-time fixes and should not be re-run:
- `merge_zerodha_accounts.py` — merged duplicate Zerodha account created by NSDL CAS
- `fix_ncd_holding.py` — fixed Edelweiss NCD quantity (was 1000 face value, corrected to 50)
- `fix_instrument_names.py` — backfilled Zerodha equity names from "CNC" to trading symbols
