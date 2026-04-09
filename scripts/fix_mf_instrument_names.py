"""
scripts/fix_mf_instrument_names.py

Fixes mutual fund instruments where `name` was stored as the numeric AMFI
scheme code instead of the actual fund name.

Fetches the live AMFI NAV file, builds a scheme_code → scheme_name map,
then updates every MF instrument whose `name` field is all-numeric.

Run:
    python -m scripts.fix_mf_instrument_names
"""

import asyncio
import os
import certifi
from dotenv import load_dotenv

load_dotenv()


async def fix():
    from motor.motor_asyncio import AsyncIOMotorClient
    from beanie import init_beanie
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
    from integrations.amfi_fetcher import fetch_amfi_navs

    client = AsyncIOMotorClient(os.getenv("MONGO_URI"), tls=True, tlsCAFile=certifi.where())
    await init_beanie(
        database=client[os.getenv("DB_NAME", "finance_agent")],
        document_models=[
            PolicyRule, Account, Holding, Instrument, Transaction,
            PriceSnapshot, Goal, GoalAllocation, Cashflow, FinancialDocument,
            Signal, Recommendation, ResearchItem, ZerodhaToken,
        ],
    )

    print("Fetching AMFI NAV file...")
    nav_data = await fetch_amfi_navs()
    print(f"Loaded {nav_data.total_schemes} schemes from AMFI.")

    # Find all MF instruments where name looks like a numeric scheme code
    all_mf = await Instrument.find({"asset_class": AssetClass.MUTUAL_FUND}).to_list()
    to_fix = [i for i in all_mf if i.name and i.name.strip().isdigit()]

    print(f"\nFound {len(all_mf)} MF instruments, {len(to_fix)} with numeric names.\n")

    fixed = skipped = 0
    for inst in to_fix:
        # Try lookup by symbol first (scheme code), then by scheme_code field, then by ISIN
        record = (
            nav_data.get_by_scheme_code(inst.symbol)
            or (nav_data.get_by_scheme_code(inst.scheme_code) if inst.scheme_code else None)
            or (nav_data.get_by_isin(inst.isin) if inst.isin else None)
        )

        if not record:
            print(f"  SKIP  symbol={inst.symbol!r}  isin={inst.isin!r} — not found in AMFI file")
            skipped += 1
            continue

        old_name = inst.name
        inst.name = record["scheme_name"]

        # Set scheme_code from symbol if not already set
        if not inst.scheme_code:
            inst.scheme_code = inst.symbol

        await inst.save()
        print(f"  FIXED  {old_name!r}  →  {inst.name!r}")
        fixed += 1

    print(f"\nDone: {fixed} fixed, {skipped} skipped (not in AMFI file).")
    client.close()


if __name__ == "__main__":
    asyncio.run(fix())
