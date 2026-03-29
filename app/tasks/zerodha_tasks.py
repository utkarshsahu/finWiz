"""
tasks/zerodha_tasks.py — Celery tasks for scheduled Zerodha sync.

Schedule (configured in celery_config.py):
  - 7:00am IST: Send Telegram login reminder with auth URL
  - 9:05am IST: Run full sync (assumes user logged in by 9am)
  - 3:45pm IST: Refresh prices mid-day (market closes at 3:30pm)

Why asyncio in Celery tasks?
  Celery workers are synchronous by default. We use asyncio.run()
  to call our async MongoDB/Kite code from within Celery tasks.
  This is the standard pattern for Beanie + Celery.
"""

import asyncio
import logging
import os
from celery import Celery
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Celery app setup
# ---------------------------------------------------------------------------
celery_app = Celery(
    "finance_agent",
    broker=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    backend=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Kolkata",
    enable_utc=True,
    beat_schedule={
        # Daily login reminder at 7am IST
        "zerodha-login-reminder": {
            "task": "app.tasks.zerodha_tasks.send_login_reminder",
            "schedule": 7 * 3600,  # Use crontab for production
            # In production: crontab(hour=7, minute=0)
        },
        # Daily full sync at 9:05am IST
        "zerodha-daily-sync": {
            "task": "app.tasks.zerodha_tasks.run_daily_sync",
            "schedule": 9 * 3600 + 5 * 60,
        },
        # Afternoon price refresh after market close (3:45pm IST)
        "zerodha-price-refresh": {
            "task": "app.tasks.zerodha_tasks.refresh_prices_only",
            "schedule": 15 * 3600 + 45 * 60,
        },
    },
)


# ---------------------------------------------------------------------------
# Async helper — sets up Beanie for tasks that need DB access
# ---------------------------------------------------------------------------
async def _get_db():
    """Initialize Beanie for use inside a Celery task."""
    from app.models.instruments import Instrument
    from app.models.accounts import Account
    from app.models.holdings import Holding
    from app.models.transactions import Transaction
    from app.models.zerodha_token import ZerodhaToken

    client = AsyncIOMotorClient(os.getenv("MONGO_URI"))
    await init_beanie(
        database=client[os.getenv("DB_NAME", "finance_agent")],
        document_models=[Instrument, Account, Holding, Transaction, ZerodhaToken],
    )
    return client


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@celery_app.task(name="app.tasks.zerodha_tasks.send_login_reminder")
def send_login_reminder():
    """
    Sends a Telegram message each morning with the Kite login URL.
    User taps the link → logs in → token stored automatically.
    Takes ~5 seconds on the phone.
    """
    async def _run():
        client = await _get_db()
        try:
            from app.integrations.zerodha_auth import get_zerodha_auth
            from app.services.telegram_service import send_telegram_message

            # Check if already authenticated (e.g. user logged in manually)
            if await get_zerodha_auth().is_authenticated():
                logger.info("Already authenticated — skipping login reminder")
                return

            login_url = get_zerodha_auth().get_login_url()
            message = (
                "🔐 *Zerodha Daily Login*\n\n"
                "Tap the link below to refresh your Kite session.\n"
                "Takes 5 seconds — required for today's portfolio sync.\n\n"
                f"[Login to Zerodha]({login_url})"
            )
            await send_telegram_message(message)
            logger.info("Login reminder sent via Telegram")
        finally:
            client.close()

    asyncio.run(_run())


@celery_app.task(name="app.tasks.zerodha_tasks.run_daily_sync")
def run_daily_sync():
    """
    Full sync: holdings + transactions + prices.
    Runs at 9:05am IST assuming login happened between 7am-9am.
    """
    async def _run():
        client = await _get_db()
        try:
            from app.integrations.zerodha_sync import ZerodhaSync
            from app.services.telegram_service import send_telegram_message

            sync = ZerodhaSync()
            results = await sync.run_full_sync()

            if results.get("requires_auth"):
                # Token not available — notify user
                await send_telegram_message(
                    "⚠️ *Sync Failed*\n\n"
                    "Zerodha token not available. "
                    "Please login via the morning reminder link."
                )
                return

            # Send a brief sync confirmation (not the full digest — that's weekly)
            h = results.get("holdings", {})
            t = results.get("transactions", {})
            p = results.get("prices", {})

            await send_telegram_message(
                f"✅ *Daily Sync Complete*\n"
                f"Holdings: {h.get('holdings_upserted', 0)} updated\n"
                f"Transactions: {t.get('transactions_inserted', 0)} new\n"
                f"Prices: {p.get('prices_updated', 0)} refreshed"
            )

        except Exception as e:
            logger.error(f"Daily sync failed: {e}")
            from app.services.telegram_service import send_telegram_message
            await send_telegram_message(f"❌ *Sync Error*\n{str(e)}")
        finally:
            client.close()

    asyncio.run(_run())


@celery_app.task(name="app.tasks.zerodha_tasks.refresh_prices_only")
def refresh_prices_only():
    """
    Lightweight afternoon task — just refresh current_price on holdings.
    No transaction sync needed (market just closed).
    """
    async def _run():
        client = await _get_db()
        try:
            from app.integrations.zerodha_sync import ZerodhaSync
            sync = ZerodhaSync()
            result = await sync.refresh_prices()
            logger.info(f"Afternoon price refresh: {result}")
        except Exception as e:
            logger.error(f"Price refresh failed: {e}")
        finally:
            client.close()

    asyncio.run(_run())