"""
scripts/restore_instrument_links.py

Repairs broken holding→instrument DBRefs after an instruments collection wipe.

The instruments collection was re-created with new ObjectIds, leaving all
holdings pointing to dead DBRefs. This script:
  1. Builds a symbol→instrument lookup from the current instruments collection
  2. For Zerodha holdings (have source_raw.tradingsymbol): matches by symbol
  3. Updates each holding's instrument DBRef to the correct current ObjectId

CAS holdings (no source_raw) cannot be repaired here — re-upload CAS PDFs
via POST /documents/upload/cas to fix those.

Run:
    python -m scripts.restore_instrument_links
"""

import asyncio
import os
import certifi
from dotenv import load_dotenv

load_dotenv()


async def restore():
    from motor.motor_asyncio import AsyncIOMotorClient
    from beanie import init_beanie
    from bson import ObjectId, DBRef
    from models.instruments import Instrument, AssetClass
    from models.accounts import Account
    from models.holdings import Holding
    from models.transactions import Transaction
    from models.prices import PriceSnapshot
    from models.goals import Goal, GoalAllocation
    from models.cashflows import Cashflow
    from models.documents import FinancialDocument
    from models.signals import Signal
    from models.recommendations import Recommendation
    from models.research import ResearchItem
    from models.policies import PolicyRule
    from models.zerodha_token import ZerodhaToken

    client = AsyncIOMotorClient(os.getenv("MONGO_URI"), tls=True, tlsCAFile=certifi.where())
    db = client[os.getenv("DB_NAME", "finance_agent")]
    await init_beanie(
        database=db,
        document_models=[
            PolicyRule, Account, Holding, Instrument, Transaction,
            PriceSnapshot, Goal, GoalAllocation, Cashflow, FinancialDocument,
            Signal, Recommendation, ResearchItem, ZerodhaToken,
        ],
    )

    # Build lookup maps from current instruments collection
    by_symbol = {}   # symbol → ObjectId
    by_isin = {}     # isin → ObjectId

    async for doc in db.instruments.find({}):
        oid = doc["_id"]
        if doc.get("symbol"):
            by_symbol[doc["symbol"]] = oid
        if doc.get("isin"):
            by_isin[doc["isin"]] = oid

    print(f"Instruments indexed: {len(by_symbol)} by symbol, {len(by_isin)} by ISIN\n")

    fixed = skipped_no_match = skipped_no_source = already_ok = 0
    cas_holdings = []

    async for h in db.holdings.find({"is_active": True}):
        holding_id = h["_id"]
        inst_ref = h.get("instrument")
        src = h.get("source_raw") or {}
        tradingsymbol = src.get("tradingsymbol") if src else None
        isin = src.get("isin") if src else None

        # Check if already pointing to a valid instrument
        if inst_ref:
            ref_id = inst_ref.get("id") if isinstance(inst_ref, dict) else None
            if ref_id and str(ref_id) in {str(v) for v in by_symbol.values()}:
                already_ok += 1
                continue

        if not tradingsymbol and not isin:
            cas_holdings.append(holding_id)
            skipped_no_source += 1
            continue

        # Try to find the matching instrument
        new_instrument_id = None
        if tradingsymbol and tradingsymbol in by_symbol:
            new_instrument_id = by_symbol[tradingsymbol]
        elif isin and isin in by_isin:
            new_instrument_id = by_isin[isin]

        if not new_instrument_id:
            # Instrument not in DB — create a minimal one from source_raw
            from models.instruments import AssetClass, SubClass, Exchange
            from integrations.zerodha_sync import INSTRUMENT_TYPE_MAP
            from datetime import datetime, timezone

            instrument_type = src.get("instrument_type", "EQ")
            exchange_str = src.get("exchange", "NSE")
            asset_class, sub_class = INSTRUMENT_TYPE_MAP.get(
                instrument_type, (AssetClass.EQUITY, SubClass.NONE)
            )

            name_lower = (src.get("product", "") + tradingsymbol).lower()
            if "gold" in name_lower:
                asset_class = AssetClass.GOLD
            elif "silver" in name_lower:
                asset_class = AssetClass.SILVER

            try:
                exchange = Exchange(exchange_str)
            except ValueError:
                exchange = Exchange.NSE

            new_inst = Instrument(
                isin=isin,
                symbol=tradingsymbol,
                name=tradingsymbol,  # will be enriched by analytics/NSE later
                asset_class=asset_class,
                sub_class=sub_class,
                exchange=exchange,
            )
            await new_inst.insert()
            new_instrument_id = new_inst.id
            by_symbol[tradingsymbol] = new_instrument_id
            print(f"  CREATED instrument: {tradingsymbol}")

        # Update holding's instrument reference
        await db.holdings.update_one(
            {"_id": holding_id},
            {"$set": {"instrument": DBRef("instruments", new_instrument_id)}}
        )
        print(f"  FIXED  {tradingsymbol or isin}")
        fixed += 1

    print(f"\n--- Summary ---")
    print(f"Fixed:           {fixed}")
    print(f"Already OK:      {already_ok}")
    print(f"No source data (CAS holdings — need PDF re-upload): {skipped_no_source}")

    if cas_holdings:
        print(f"\nCAS holdings that still need fixing ({len(cas_holdings)}):")
        print("  → Re-upload your CAMS and NSDL CAS PDFs via:")
        print("    POST /documents/upload/cas")

    client.close()


if __name__ == "__main__":
    asyncio.run(restore())
