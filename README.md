# Paisa Paisa — Personal Finance Agent

**Paisa Paisa** is a personal financial intelligence platform designed for Indian retail investors. It orchestrates data from Zerodha (Kite), Mutual Fund CAS (CAMS/NSDL), and live market feeds to provide automated portfolio analytics, rule-based signals, and AI-driven weekly investment digests.

---

## 🚀 Features

* **Multi-Source Ingestion:** Seamlessly syncs equity holdings from **Zerodha (Kite)** and Mutual Fund data via **CAMS/NSDL/CDSL CAS** (PDF).
* **Live Market Integration:** Real-time price updates using direct **NSE API** and **AMFI** NAV feeds.
* **Automated Analytics:** Calculates Net Worth, XIRR (for MF portfolios), Asset Allocation, and Concentration risk.
* **Intelligent Rules Engine:** Monitors portfolio drift, goal progress, and diversification, generating actionable **Signal** documents.
* **Technicals & Corporate Actions:** Per-holding technical signals (52W drawdown, near-low, high volatility) sourced from NSE; fundamental concerns (ROE, D/E) from Screener.in; upcoming corporate actions (rights, buybacks, bonuses, splits, dividends) from NSE corporate actions API — surfaced via `/technicals` Telegram command.
* **Research Pipeline:** Scrapes market news (Mint RSS), processes PDFs, and uses **GPT-4o-mini + OpenAI Embeddings** to categorize relevant financial themes.
* **AI Weekly Digest:** A **GPT-4o** powered recommendation engine that synthesizes portfolio signals and market research into a prioritized weekly action plan.
* **Telegram Interface:** A full-featured bot for real-time notifications, portfolio snapshots, and document uploads.

---

## 🛠 Tech Stack

| Component | Technology |
| :--- | :--- |
| **Backend** | FastAPI (Python 3.13) |
| **Database** | MongoDB Atlas (Beanie ODM) + Redis |
| **Async Tasks** | Celery |
| **LLMs** | OpenAI (GPT-4o, GPT-4o-mini) |
| **Parsing** | casparser (MF CAS), pdfminer (Research) |
| **Market Data** | NSE Direct API, AMFI Daily NAV |
| **Interface** | Telegram Bot API |

---

## 📂 Project Structure

```bash
./
├── models/         # MongoDB/Beanie schemas (Ledger, Holdings, Signals, etc.)
├── integrations/   # Zerodha OAuth, AMFI/NSE fetchers, CAS parsers
├── services/       # Analytics, Rules Engine, Telegram Bot, Recommendations
├── routes/         # FastAPI endpoints (Market data, Analytics, Research)
├── tasks/          # Celery background jobs (Daily syncs, Login reminders)
└── scripts/        # Database seeding and index management
```

---

## ⚙️ Setup & Installation

### 1. Environment Configuration
Create a `.env` file with the following keys:
```env
MONGO_URI=mongodb+srv://...
DB_NAME=finance_agent
ZERODHA_API_KEY=
ZERODHA_API_SECRET=
# Leave this blank — it gets written automatically after /zerodha/callback
ZERODHA_ACCESS_TOKEN=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
OPENAI_API_KEY=
REDIS_URL=redis://localhost:6379/0
```

### 2. Installation
```bash
pip install -r requirements.txt
```

### 3. Initialize Database
```bash
python -m scripts.create_indexes
python -m scripts.seed_instruments
python -m scripts.seed_policies
```

---

## 🛠 Running the Application

1.  **Start the API:**
    ```bash
    uvicorn main:app --reload
    ```
2.  **Start Celery Worker:**
    ```bash
    celery -A tasks.zerodha_tasks.celery_app worker --loglevel=info
    ```
3.  **Telegram Webhook (Development):**
    Use `ngrok` to expose port 8000 and set the webhook via the `/telegram/set-webhook` endpoint.

---

## 📈 Operational Workflow

1.  **Morning Sync:** Complete Zerodha OAuth via Telegram login reminder (7 AM IST).
2.  **Data Ingestion:** Trigger `/zerodha/sync` and `/market-data/sync` to refresh holdings and prices.
3.  **Signal Generation:** Run the Rules Engine to identify portfolio drift or policy violations.
4.  **Weekly Strategy:** Generate the AI Digest to receive a structured summary of research and recommended portfolio actions.

---

## Improvements added:

1. **Strategic Discovery Logic**: We updated compute_portfolio_relevance to give a "Discovery Bonus" to bullish research in sectors where your AnalyticsService shows 0% exposure, effectively flagging gaps in your portfolio.
2. **Analytics-Driven Rebalancing**: The pipeline now fuses research with your real-time Portfolio Drift and Asset Allocation data, prioritizing news that helps you move toward your target goals (e.g., boosting gold research if you are underweight in gold).
3. **LLM "Scout" Persona**: We refined the GPT-4o classification prompt and the ActionType enum to explicitly identify NEW_SECTOR_ENTRY and STRATEGIC_DIVERSIFICATION as high-priority actionable signals.