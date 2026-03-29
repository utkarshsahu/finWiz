"""
zerodha_sync.py — fetches holdings + positions from Kite and writes to MongoDB.

What this syncs:
  - Holdings: long-term equity/ETF positions (your actual portfolio)
  - Positions: intraday/short-term (usually empty for buy-and-hold investors)
  - Orders (last 60 days): for transaction backfill
  - OHLC quotes for all held instruments: updates current_price on holdings

Sync strategy:
  - Holdings are UPSERTED (not appended) — each sync gives a fresh snapshot
  - Transactions are INSERT-ONLY with dedup_hash to prevent double ingestion
  - Instrument master is upserted on first encounter

Run this daily via Celery beat task at ~9am IST (after token refresh).

Important: Kite's rate limit is 3 requests/second. The sync adds a small
delay between batch calls to stay well under this.
"""

import asyncio
import hashlib
import json
import logging
from datetime import date, datetime, timezone
from typing import Optional

from beanie.operators import Set
from kiteconnect import KiteConnect
from tenacity import retry, stop_after_attempt, wait_exponential

from app.models.accounts import Account, AccountType, DataSource
from app.models.holdings import Holding
from app.models.instruments import (
    AssetClass, Exchange, Instrument, SubClass
)
from app.models.transactions import Transaction, TransactionSource, TransactionType
from app.integrations.zerodha_auth import get_zerodha_auth

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Instrument type mapping — Kite instrument_type → our AssetClass/SubClass
# ---------------------------------------------------------------------------
INSTRUMENT_TYPE_MAP: dict[str, tuple[AssetClass, SubClass]] = {
    "EQ":   (AssetClass.EQUITY,      SubClass.NONE),
    "BE":   (AssetClass.EQUITY,      SubClass.NONE),   # Bond + equity (rare)
    "BL":   (AssetClass.DEBT,        SubClass.NONE),   # Bond
    "GS":   (AssetClass.DEBT,        SubClass.GILT),   # Govt securities
    "ETF":  (AssetClass.ETF,         SubClass.INDEX),
    "MF":   (AssetClass.MUTUAL_FUND, SubClass.NONE),
    "SGB":  (AssetClass.GOLD,        SubClass.NONE),   # Sovereign Gold Bond
}


class ZerodhaSync:
    """
    Orchestrates a full Zerodha portfolio sync into MongoDB.

    Typical call:
        sync = ZerodhaSync()
        result = await sync.run_full_sync()
        print(result)  # {"holdings_upserted": 12, "transactions_added": 3, ...}
    """

    def __init__(self):
        self._kite: Optional[KiteConnect] = None
        self._account: Optional[Account] = None

    async def _get_kite(self) -> KiteConnect:
        if not self._kite:
            self._kite = await get_zerodha_auth().get_kite_client()
        return self._kite

    async def _get_or_create_account(self) -> Account:
        """
        Finds or creates the Zerodha demat account in MongoDB.
        This account document is linked to every holding and transaction.
        """
        if self._account:
            return self._account

        account = await Account.find_one(Account.data_source == DataSource.ZERODHA)
        if not account:
            kite = await self._get_kite()
            profile = kite.profile()  # {user_id, user_name, ...}
            account = Account(
                name=f"Zerodha Demat — {profile.get('user_id', '')}",
                account_type=AccountType.DEMAT,
                institution="Zerodha",
                data_source=DataSource.ZERODHA,
                sync_frequency_days=1,
            )
            await account.insert()
            logger.info(f"Created Zerodha account: {account.id}")

        self._account = account
        return account

    # ---------------------------------------------------------------------------
    # Instrument upsert
    # ---------------------------------------------------------------------------

    async def _upsert_instrument(self, holding_data: dict) -> Instrument:
        """
        Find or create an Instrument from Kite holding data.

        Kite holdings contain enough info to classify the instrument —
        we don't need to call the instruments API separately.
        """
        symbol = holding_data["tradingsymbol"]
        exchange_str = holding_data.get("exchange", "NSE")
        isin = holding_data.get("isin")
        instrument_type = holding_data.get("instrument_type", "EQ")

        # Try to find by ISIN first (most reliable), fall back to symbol
        existing = None
        if isin:
            existing = await Instrument.find_one(Instrument.isin == isin)
        if not existing:
            existing = await Instrument.find_one(Instrument.symbol == symbol)

        asset_class, sub_class = INSTRUMENT_TYPE_MAP.get(
            instrument_type, (AssetClass.EQUITY, SubClass.NONE)
        )

        # Classify gold instruments by name
        name_lower = (holding_data.get("product", "") + symbol).lower()
        if "gold" in name_lower or "sgb" in instrument_type.lower():
            asset_class = AssetClass.GOLD
        elif "silver" in name_lower:
            asset_class = AssetClass.SILVER

        try:
            exchange = Exchange(exchange_str)
        except ValueError:
            exchange = Exchange.NSE

        if existing:
            # Update fields that may have changed (name, ISIN if missing)
            if isin and not existing.isin:
                existing.isin = isin
                existing.updated_at = datetime.now(timezone.utc)
                await existing.save()
            return existing

        # Kite holdings don't include the full company name.
        # `product` is the order type (CNC/MIS) — not the name.
        # Use tradingsymbol as name for now; backfilled via instruments API
        # or NSE data in a future enhancement.
        instrument = Instrument(
            isin=isin,
            symbol=symbol,
            name=holding_data.get("tradingsymbol", symbol),
            asset_class=asset_class,
            sub_class=sub_class,
            exchange=exchange,
        )
        await instrument.insert()
        logger.info(f"Created instrument: {symbol} ({asset_class})")
        return instrument

    # ---------------------------------------------------------------------------
    # Holdings sync
    # ---------------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def _fetch_holdings(self) -> list[dict]:
        """Fetch all long-term holdings from Kite (with retry)."""
        kite = await self._get_kite()
        # KiteConnect SDK is synchronous — run in executor to avoid blocking
        loop = asyncio.get_event_loop()
        holdings = await loop.run_in_executor(None, kite.holdings)
        return holdings

    async def sync_holdings(self) -> dict:
        """
        Pull all holdings from Zerodha and upsert into MongoDB.

        Holdings are idempotent — running this twice gives the same result.
        old holdings not returned by Kite (fully sold) are marked inactive.
        """
        kite_holdings = await self._fetch_holdings()
        account = await self._get_or_create_account()

        upserted = 0
        errors = 0
        instruments_created = 0
        returned_instrument_ids = set()

        for kh in kite_holdings:
            try:
                instrument = await self._upsert_instrument(kh)
                returned_instrument_ids.add(str(instrument.id))

                # Check if holding already exists
                existing = await Holding.find_one(
                    Holding.account.id == account.id,  # type: ignore
                    Holding.instrument.id == instrument.id,  # type: ignore
                )

                quantity = float(kh.get("quantity", 0))
                avg_cost = float(kh.get("average_price", 0))
                current_price = float(kh.get("last_price", 0)) or None

                if existing:
                    # Update existing holding
                    existing.quantity = quantity
                    existing.avg_cost = avg_cost
                    existing.current_price = current_price
                    existing.current_price_date = datetime.now(timezone.utc)
                    existing.source_raw = kh
                    existing.last_synced_at = datetime.now(timezone.utc)
                    existing.updated_at = datetime.now(timezone.utc)
                    existing.is_active = quantity > 0
                    existing.recompute()
                    await existing.save()
                else:
                    holding = Holding(
                        account=account,
                        instrument=instrument,
                        quantity=quantity,
                        avg_cost=avg_cost,
                        current_price=current_price,
                        current_price_date=datetime.now(timezone.utc),
                        source_raw=kh,
                        last_synced_at=datetime.now(timezone.utc),
                        is_active=quantity > 0,
                    )
                    holding.recompute()
                    await holding.insert()
                    instruments_created += 1

                upserted += 1
                await asyncio.sleep(0.05)  # Gentle rate limiting

            except Exception as e:
                logger.error(f"Error syncing holding {kh.get('tradingsymbol')}: {e}")
                errors += 1

        # Mark holdings no longer in Zerodha as inactive (position closed)
        all_holdings = await Holding.find(
            Holding.account.id == account.id  # type: ignore
        ).to_list()

        deactivated = 0
        for h in all_holdings:
            if str(h.instrument.ref.id) not in returned_instrument_ids and h.is_active:
                h.is_active = False
                h.updated_at = datetime.now(timezone.utc)
                await h.save()
                deactivated += 1

        # Update account sync timestamp
        account.last_synced_at = datetime.now(timezone.utc)
        account.updated_at = datetime.now(timezone.utc)
        await account.save()

        result = {
            "holdings_upserted": upserted,
            "new_instruments_created": instruments_created,
            "holdings_deactivated": deactivated,
            "errors": errors,
        }
        logger.info(f"Holdings sync complete: {result}")
        return result

    # ---------------------------------------------------------------------------
    # Transaction backfill (last 60 days of orders)
    # ---------------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def _fetch_orders(self) -> list[dict]:
        """Fetch all orders from Kite. Returns up to 60 days of history."""
        kite = await self._get_kite()
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, kite.orders)

    def _make_dedup_hash(self, account_id: str, order: dict) -> str:
        """
        Generate a stable dedup hash for an order.
        Same order imported twice → same hash → second import skipped.
        """
        key = f"{account_id}:{order.get('order_id', '')}:{order.get('tradingsymbol', '')}:{order.get('transaction_type', '')}:{order.get('quantity', '')}:{order.get('average_price', '')}"
        return hashlib.sha256(key.encode()).hexdigest()

    async def sync_transactions(self) -> dict:
        """
        Pull recent orders from Zerodha and insert as Transactions.
        Only inserts orders not already in the ledger (dedup_hash check).
        Only syncs COMPLETED orders (status = "COMPLETE").
        """
        orders = await self._fetch_orders()
        account = await self._get_or_create_account()

        inserted = 0
        skipped_dedup = 0
        skipped_incomplete = 0
        errors = 0

        for order in orders:
            # Only process completed orders
            if order.get("status") != "COMPLETE":
                skipped_incomplete += 1
                continue

            dedup_hash = self._make_dedup_hash(str(account.id), order)

            # Check if already ingested
            existing = await Transaction.find_one(
                Transaction.dedup_hash == dedup_hash
            )
            if existing:
                skipped_dedup += 1
                continue

            try:
                # Find the instrument
                symbol = order.get("tradingsymbol")
                instrument = await Instrument.find_one(Instrument.symbol == symbol)

                # Map Kite transaction type to our enum
                kite_type = order.get("transaction_type", "BUY").upper()
                txn_type = TransactionType.BUY if kite_type == "BUY" else TransactionType.SELL

                quantity = float(order.get("quantity", 0))
                price = float(order.get("average_price", 0))
                amount = quantity * price
                if txn_type == TransactionType.SELL:
                    amount = amount  # Inflow (positive)
                else:
                    amount = -amount  # Outflow (negative)

                # Parse order date
                order_timestamp = order.get("order_timestamp") or order.get("exchange_timestamp")
                if isinstance(order_timestamp, str):
                    from dateutil.parser import parse as parse_dt
                    txn_date = parse_dt(order_timestamp).date()
                elif isinstance(order_timestamp, datetime):
                    txn_date = order_timestamp.date()
                else:
                    txn_date = date.today()

                txn = Transaction(
                    account=account,
                    instrument=instrument,
                    transaction_type=txn_type,
                    transaction_date=txn_date,
                    quantity=quantity,
                    price=price,
                    amount=amount,
                    source=TransactionSource.ZERODHA,
                    source_reference_id=order.get("order_id"),
                    dedup_hash=dedup_hash,
                    is_verified=True,  # Zerodha data is authoritative
                )
                await txn.insert()
                inserted += 1

            except Exception as e:
                logger.error(f"Error inserting transaction for order {order.get('order_id')}: {e}")
                errors += 1

        result = {
            "transactions_inserted": inserted,
            "skipped_dedup": skipped_dedup,
            "skipped_incomplete": skipped_incomplete,
            "errors": errors,
        }
        logger.info(f"Transaction sync complete: {result}")
        return result

    # ---------------------------------------------------------------------------
    # Quote refresh — update current_price on all active holdings
    # ---------------------------------------------------------------------------

    async def refresh_prices(self) -> dict:
        """
        Fetch live quotes for all active holdings and update current_price.

        Kite's quote API accepts up to 500 instruments per call.
        We batch in groups of 200 to stay comfortable under the limit.
        """
        account = await self._get_or_create_account()
        active_holdings = await Holding.find(
            Holding.account.id == account.id,  # type: ignore
            Holding.is_active == True,
        ).to_list()

        if not active_holdings:
            return {"prices_updated": 0}

        kite = await self._get_kite()

        # Build exchange:symbol strings that Kite quote API expects
        # We need to fetch instrument details to get exchange
        instrument_map: dict[str, Holding] = {}
        quote_keys = []

        for holding in active_holdings:
            await holding.fetch_link(Holding.instrument)
            instrument = holding.instrument
            exchange = instrument.exchange.value if instrument.exchange else "NSE"
            key = f"{exchange}:{instrument.symbol}"
            instrument_map[key] = holding
            quote_keys.append(key)

        # Batch into groups of 200
        BATCH_SIZE = 200
        updated = 0
        errors = 0

        for i in range(0, len(quote_keys), BATCH_SIZE):
            batch = quote_keys[i : i + BATCH_SIZE]
            try:
                loop = asyncio.get_event_loop()
                quotes = await loop.run_in_executor(None, lambda b=batch: kite.quote(b))

                for key, quote_data in quotes.items():
                    holding = instrument_map.get(key)
                    if not holding:
                        continue
                    last_price = quote_data.get("last_price")
                    if last_price:
                        holding.current_price = float(last_price)
                        holding.current_price_date = datetime.now(timezone.utc)
                        holding.recompute()
                        holding.updated_at = datetime.now(timezone.utc)
                        await holding.save()
                        updated += 1

                await asyncio.sleep(0.35)  # ~3 requests/second limit

            except Exception as e:
                logger.error(f"Quote fetch error for batch {i}: {e}")
                errors += 1

        result = {"prices_updated": updated, "errors": errors}
        logger.info(f"Price refresh complete: {result}")
        return result

    # ---------------------------------------------------------------------------
    # Full sync — called by Celery daily job
    # ---------------------------------------------------------------------------

    async def run_full_sync(self) -> dict:
        """
        Run all sync steps in sequence.
        Called by the daily Celery task at 9am IST.
        """
        if not await get_zerodha_auth().is_authenticated():
            return {
                "error": "Not authenticated. Visit /zerodha/login to get today's token.",
                "requires_auth": True,
            }

        logger.info("Starting full Zerodha sync...")
        results = {}

        results["holdings"] = await self.sync_holdings()
        await asyncio.sleep(1)

        results["transactions"] = await self.sync_transactions()
        await asyncio.sleep(1)

        results["prices"] = await self.refresh_prices()

        logger.info(f"Full sync complete: {json.dumps(results, indent=2)}")
        return results