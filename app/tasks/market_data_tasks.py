"""
market_data_tasks.py — Celery tasks for scheduled market data sync.

Schedule:
  - 6:00pm IST daily: full sync (after NSE close at 3:30pm + AMFI publish by ~6pm)
  - 8:30pm IST daily: retry if AMFI NAV not yet available at 6pm (rare)

AMFI NAV publication timing:
  AMFI typically publishes by 6pm IST on trading days.
  On holidays it publishes the previous trading day's NAV.
  The 8:30pm retry handles the rare case where publication is delayed.
"""

import asyncio
import logging
import os
from app.tasks.zerodha_tasks import celery_app  # Reuse the same Celery instance

logger = logging.getLogger(__name__)


async def _run_sync():
    """Initialize DB and run market data sync."""
    from motor.motor_asyncio import AsyncIOMotorClient
    from beanie import init_beanie
    from app.models.instruments import Instrument
    from app.models.holdings import Holding
    from app.models.prices import PriceSnapshot
    from app.models.accounts import Account

    client = AsyncIOMotorClient(os.getenv("MONGO_URI"))
    await init_beanie(
        database=client[os.getenv("DB_NAME", "finance_agent")],
        document_models=[Instrument, Holding, PriceSnapshot, Account],
    )

    try:
        from app.integrations.market_data_sync import MarketDataSync
        sync = MarketDataSync()
        return await sync.run_full_sync()
    finally:
        client.close()


@celery_app.task(name="app.tasks.market_data_tasks.run_daily_market_sync")
def run_daily_market_sync():
    """
    Full market data sync — equities, MF NAVs, commodities, indices.
    Runs at 6pm IST after market close.
    """
    try:
        results = asyncio.run(_run_sync())

        # Send brief Telegram summary
        async def _notify():
            from motor.motor_asyncio import AsyncIOMotorClient
            from beanie import init_beanie
            client = AsyncIOMotorClient(os.getenv("MONGO_URI"))
            await init_beanie(database=client[os.getenv("DB_NAME", "finance_agent")], document_models=[])
            try:
                from app.services.telegram_service import send_telegram_message
                mf = results.get("mutual_funds", {})
                eq = results.get("equities", {})
                await send_telegram_message(
                    f"📊 *Market Data Synced*\n"
                    f"MF NAVs: {mf.get('mf_navs_updated', 0)} updated "
                    f"(date: {mf.get('amfi_nav_date', 'unknown')})\n"
                    f"Equities: {eq.get('equity_prices_updated', 0)} updated"
                )
            finally:
                client.close()

        asyncio.run(_notify())
        logger.info(f"Market data sync complete: {results}")

    except Exception as e:
        logger.error(f"Market data sync failed: {e}")


# Register in beat schedule — add to celery_app.conf.beat_schedule in zerodha_tasks.py:
#
# "market-data-daily-sync": {
#     "task": "app.tasks.market_data_tasks.run_daily_market_sync",
#     "schedule": crontab(hour=18, minute=0),  # 6pm IST
# },
# "market-data-retry": {
#     "task": "app.tasks.market_data_tasks.run_daily_market_sync",
#     "schedule": crontab(hour=20, minute=30),  # 8:30pm retry
# },