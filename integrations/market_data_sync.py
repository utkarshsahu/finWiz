"""
market_data_sync.py — fetches and stores daily prices for all assets.

This replaces the kite.quote() call we removed from zerodha_sync.py.

What it syncs:
  1. Equity/ETF prices  → yfinance (.NS symbols)
  2. Mutual fund NAVs   → AMFI daily file
  3. Gold/Silver prices → yfinance (GOLDBEES.NS, SILVERBEES.NS)
  4. Index prices       → yfinance (^NSEI, ^BSESN, etc.)

For each instrument:
  - Writes a PriceSnapshot document (historical record)
  - Updates current_price + current_price_date on the active Holding
  - Calls holding.recompute() to refresh P&L fields

Run order matters:
  Fetch AMFI first (one HTTP call, covers all MFs) → then yfinance batches.

Called by:
  - Celery task: daily at 6pm IST (after market close + AMFI publish)
  - Manual: POST /market-data/sync
"""

import logging
from datetime import datetime, timezone, date
from typing import Optional

from integrations.amfi_fetcher import fetch_amfi_navs, AmfiNavData
from integrations.market_fetcher import (
    fetch_prices, fetch_commodity_prices, fetch_index_prices,
    to_nse_symbol, COMMODITY_TICKERS, INDEX_TICKERS, PriceFetchResult
)
from models.holdings import Holding
from models.instruments import Instrument, AssetClass, Exchange
from models.prices import PriceSnapshot

logger = logging.getLogger(__name__)


class MarketDataSync:
    """
    Orchestrates a full market data sync.

    Typical daily call sequence:
        sync = MarketDataSync()
        result = await sync.run_full_sync()
    """

    # ---------------------------------------------------------------------------
    # PriceSnapshot upsert helper
    # ---------------------------------------------------------------------------

    async def _upsert_price_snapshot(
        self,
        instrument: Instrument,
        price: float,
        price_date: date,
        source: str,
        nav: Optional[float] = None,
    ) -> PriceSnapshot:
        """
        Insert a PriceSnapshot for today, or update if one already exists.
        One snapshot per (instrument, date) — idempotent.
        """
        existing = await PriceSnapshot.find_one(
            PriceSnapshot.instrument.id == instrument.id,  # type: ignore
            PriceSnapshot.price_date == price_date,
        )

        if existing:
            existing.close = price
            existing.nav = nav
            existing.source = source
            await existing.save()
            return existing

        snapshot = PriceSnapshot(
            instrument=instrument,
            price_date=price_date,
            close=price,
            nav=nav,
            source=source,
        )
        await snapshot.insert()
        return snapshot

    async def _update_holding_price(
        self, holding: Holding, price: float, price_date: date
    ):
        """Update current_price on a holding and recompute P&L."""
        holding.current_price = price
        holding.current_price_date = datetime.combine(price_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        holding.recompute()
        holding.updated_at = datetime.now(timezone.utc)
        await holding.save()

    # ---------------------------------------------------------------------------
    # 1. Mutual fund NAV sync (AMFI)
    # ---------------------------------------------------------------------------

    async def sync_mf_navs(self) -> dict:
        """
        Fetch AMFI NAV file and update all MF holdings.

        Matching strategy (in order):
          1. instrument.isin → AmfiNavData._by_isin (most reliable)
          2. instrument.scheme_code → AmfiNavData._by_scheme_code
          3. No match → log warning, skip

        This covers all mutual fund folios whether they came from CAS or manual entry.
        """
        nav_data: AmfiNavData = await fetch_amfi_navs()

        # Get all active MF holdings
        mf_holdings = await Holding.find(Holding.is_active == True).to_list()

        updated = 0
        not_found = 0
        errors = 0

        for holding in mf_holdings:
            try:
                await holding.fetch_link(Holding.instrument)
                instrument = holding.instrument

                # Only process mutual funds
                if instrument.asset_class != AssetClass.MUTUAL_FUND:
                    continue

                # Try to find NAV
                nav_record = None
                if instrument.isin:
                    nav_record = nav_data.get_by_isin(instrument.isin)
                if not nav_record and instrument.scheme_code:
                    nav_record = nav_data.get_by_scheme_code(instrument.scheme_code)

                if not nav_record:
                    logger.warning(
                        f"NAV not found for {instrument.name} "
                        f"(ISIN: {instrument.isin}, scheme: {instrument.scheme_code})"
                    )
                    not_found += 1
                    continue

                nav_value = nav_record["nav"]
                nav_date = nav_record["nav_date"]

                # Write PriceSnapshot
                await self._upsert_price_snapshot(
                    instrument=instrument,
                    price=nav_value,
                    price_date=nav_date,
                    source="amfi",
                    nav=nav_value,
                )

                # Update holding
                await self._update_holding_price(holding, nav_value, nav_date)
                updated += 1

            except Exception as e:
                logger.error(f"Error updating MF NAV for holding {holding.id}: {e}")
                errors += 1

        result = {
            "mf_navs_updated": updated,
            "not_found": not_found,
            "errors": errors,
            "amfi_nav_date": str(nav_data.nav_date),
            "total_amfi_schemes": nav_data.total_schemes,
        }
        logger.info(f"MF NAV sync: {result}")
        return result

    # ---------------------------------------------------------------------------
    # 2. Equity + ETF price sync (yfinance)
    # ---------------------------------------------------------------------------

    async def sync_equity_prices(self) -> dict:
        """
        Fetch end-of-day prices for all active equity/ETF holdings via yfinance.

        Symbol mapping:
          - Exchange = NSE → symbol + ".NS"
          - Exchange = BSE → symbol + ".BO"
          - Default → try NSE first
        """
        equity_holdings = await Holding.find(Holding.is_active == True).to_list()

        # Build ticker → holding map
        ticker_to_holding: dict[str, Holding] = {}
        tickers_to_fetch: list[str] = []

        for holding in equity_holdings:
            await holding.fetch_link(Holding.instrument)
            instrument = holding.instrument

            if instrument.asset_class not in (AssetClass.EQUITY, AssetClass.ETF):
                continue

            exchange = instrument.exchange
            if exchange == Exchange.BSE:
                ticker = f"{instrument.symbol}.BO"
            else:
                ticker = f"{instrument.symbol}.NS"  # Default to NSE

            ticker_to_holding[ticker] = holding
            tickers_to_fetch.append(ticker)

        if not tickers_to_fetch:
            return {"equity_prices_updated": 0}

        price_results = await fetch_prices(tickers_to_fetch)

        updated = 0
        failed = 0

        for ticker, result in price_results.items():
            holding = ticker_to_holding.get(ticker)
            if not holding:
                continue

            if not result.success:
                logger.warning(f"Price fetch failed for {ticker}: {result.error}")
                failed += 1
                continue

            await holding.fetch_link(Holding.instrument)
            instrument = holding.instrument

            await self._upsert_price_snapshot(
                instrument=instrument,
                price=result.price,
                price_date=result.price_date,
                source="yfinance",
            )
            await self._update_holding_price(holding, result.price, result.price_date)
            updated += 1

        result_summary = {"equity_prices_updated": updated, "failed": failed}
        logger.info(f"Equity price sync: {result_summary}")
        return result_summary

    # ---------------------------------------------------------------------------
    # 3. Commodity prices (gold, silver — yfinance)
    # ---------------------------------------------------------------------------

    async def sync_commodity_prices(self) -> dict:
        """
        Fetch gold and silver prices for commodity holdings.

        Matching: look for holdings whose instrument has asset_class GOLD or SILVER.
        We try to match the Indian ETF price first (GOLDBEES.NS) — already in INR.
        For custom/physical gold holdings without a ticker, we store the
        GOLDBEES price as a reference price on a synthetic "GOLD_INR" instrument.
        """
        results = await fetch_commodity_prices()

        updated_tickers = []
        failed_tickers = []

        for name, ticker in COMMODITY_TICKERS.items():
            result = results.get(ticker)
            if not result or not result.success:
                logger.warning(f"Commodity price failed for {ticker}: {result.error if result else 'no result'}")
                failed_tickers.append(ticker)
                continue

            # Find instrument by symbol
            # Try with and without exchange suffix
            bare_symbol = ticker.replace(".NS", "").replace(".BO", "").replace("=F", "")
            instrument = await Instrument.find_one(Instrument.symbol == bare_symbol)
            if not instrument:
                instrument = await Instrument.find_one(Instrument.symbol == ticker)

            if not instrument:
                # Log but don't fail — commodity instruments may not have holdings yet
                logger.info(f"No instrument found for {ticker} — skipping snapshot")
                updated_tickers.append(f"{ticker}(no instrument)")
                continue

            await self._upsert_price_snapshot(
                instrument=instrument,
                price=result.price,
                price_date=result.price_date,
                source="yfinance",
            )

            # Update any active holdings for this instrument
            holdings = await Holding.find(
                Holding.instrument.id == instrument.id,  # type: ignore
                Holding.is_active == True,
            ).to_list()

            for holding in holdings:
                await self._update_holding_price(holding, result.price, result.price_date)

            updated_tickers.append(ticker)

        return {
            "commodity_prices_updated": len(updated_tickers),
            "tickers": updated_tickers,
            "failed": failed_tickers,
        }

    # ---------------------------------------------------------------------------
    # 4. Index prices (for benchmark tracking)
    # ---------------------------------------------------------------------------

    async def sync_index_prices(self) -> dict:
        """
        Fetch benchmark index prices.

        Index prices are stored as PriceSnapshots for instruments with
        asset_class = EQUITY and symbol matching the index ticker.
        These are used by the analytics engine for benchmark comparison.

        We auto-create index instruments if they don't exist — you don't
        hold them directly, but you need the prices for XIRR comparison.
        """
        results = await fetch_index_prices()

        synced = 0
        failed = 0

        for index_name, ticker in INDEX_TICKERS.items():
            result = results.get(ticker)
            if not result or not result.success:
                logger.warning(f"Index price failed for {ticker}")
                failed += 1
                continue

            # Find or create a synthetic index instrument
            instrument = await Instrument.find_one(Instrument.symbol == ticker)
            if not instrument:
                instrument = Instrument(
                    symbol=ticker,
                    name=index_name.replace("_", " ").title(),
                    asset_class=AssetClass.EQUITY,
                    sub_class=__import__("models.instruments", fromlist=["SubClass"]).SubClass.INDEX,
                    exchange=Exchange.NSE,
                    is_active=True,
                )
                await instrument.insert()
                logger.info(f"Created index instrument: {ticker}")

            await self._upsert_price_snapshot(
                instrument=instrument,
                price=result.price,
                price_date=result.price_date,
                source="yfinance",
            )
            synced += 1

        result_summary = {"indices_synced": synced, "failed": failed}
        logger.info(f"Index sync: {result_summary}")
        return result_summary

    # ---------------------------------------------------------------------------
    # Full sync — called by Celery daily job
    # ---------------------------------------------------------------------------

    async def run_full_sync(self) -> dict:
        """
        Run all market data sync steps.
        Called by Celery task daily at 6pm IST.

        Order matters:
          MF NAVs first (one big HTTP call, fast)
          → Equities (batched yfinance calls)
          → Commodities
          → Indices (small batch, fast)
        """
        logger.info("Starting full market data sync...")
        results = {}

        results["mutual_funds"] = await self.sync_mf_navs()
        results["equities"] = await self.sync_equity_prices()
        results["commodities"] = await self.sync_commodity_prices()
        results["indices"] = await self.sync_index_prices()

        logger.info("Market data sync complete.")
        return results