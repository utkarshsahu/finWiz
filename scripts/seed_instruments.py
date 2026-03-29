"""
app/scripts/seed_instruments.py

Seeds master instrument documents for commodities and indices
that don't come through Zerodha or CAS — gold/silver ETFs,
index trackers, etc.

Run once:
    python -m app.scripts.seed_instruments
"""

import asyncio
import os
from dotenv import load_dotenv
load_dotenv()

import certifi
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie

from app.models.instruments import Instrument, AssetClass, SubClass, Exchange


INSTRUMENTS_TO_SEED = [
    # Gold / Silver ETFs
    {
        "isin": "INF204KB14I2",
        "symbol": "GOLDBEES",
        "name": "Nippon India Gold BeES ETF",
        "short_name": "Gold BeES",
        "asset_class": AssetClass.GOLD,
        "sub_class": SubClass.INDEX,
        "exchange": Exchange.NSE,
        "sector": None,
        "fund_house": "Nippon India AMC",
    },
    {
        "isin": "INF769K01EW1",
        "symbol": "SILVERBEES",
        "name": "Mirae Asset Silver ETF",
        "short_name": "Silver BeES",
        "asset_class": AssetClass.SILVER,
        "sub_class": SubClass.INDEX,
        "exchange": Exchange.NSE,
        "fund_house": "Mirae Asset",
    },
    # Nifty index trackers (synthetic — for benchmark comparison)
    {
        "symbol": "^NSEI",
        "name": "Nifty 50",
        "short_name": "Nifty 50",
        "asset_class": AssetClass.EQUITY,
        "sub_class": SubClass.INDEX,
        "exchange": Exchange.NSE,
    },
    {
        "symbol": "^NSEMDCP50",
        "name": "Nifty Midcap 50",
        "short_name": "Nifty Midcap",
        "asset_class": AssetClass.EQUITY,
        "sub_class": SubClass.INDEX,
        "exchange": Exchange.NSE,
    },
    {
        "symbol": "^CNXIT",
        "name": "Nifty IT",
        "short_name": "Nifty IT",
        "asset_class": AssetClass.EQUITY,
        "sub_class": SubClass.SECTORAL,
        "exchange": Exchange.NSE,
        "sector": "IT",
    },
    {
        "symbol": "^NSEBANK",
        "name": "Nifty Bank",
        "short_name": "Bank Nifty",
        "asset_class": AssetClass.EQUITY,
        "sub_class": SubClass.SECTORAL,
        "exchange": Exchange.NSE,
        "sector": "Banking",
    },
    {
        "symbol": "^CNXPHARMA",
        "name": "Nifty Pharma",
        "short_name": "Nifty Pharma",
        "asset_class": AssetClass.EQUITY,
        "sub_class": SubClass.SECTORAL,
        "exchange": Exchange.NSE,
        "sector": "Pharma",
    },
]


async def seed():
    from app.models.accounts import Account
    from app.models.holdings import Holding
    from app.models.transactions import Transaction
    from app.models.prices import PriceSnapshot
    from app.models.goals import Goal, GoalAllocation
    from app.models.cashflows import Cashflow
    from app.models.documents import FinancialDocument
    from app.models.signals import Signal
    from app.models.recommendations import Recommendation
    from app.models.research import ResearchItem
    from app.models.policies import PolicyRule
    from app.models.zerodha_token import ZerodhaToken

    client = AsyncIOMotorClient(
        os.getenv("MONGO_URI"),
        tls=True,
        tlsCAFile=certifi.where(),
    )
    await init_beanie(
        database=client[os.getenv("DB_NAME", "finance_agent")],
        document_models=[
            Instrument, Account, Holding, Transaction, PriceSnapshot,
            Goal, GoalAllocation, Cashflow, FinancialDocument, Signal,
            Recommendation, ResearchItem, PolicyRule, ZerodhaToken,
        ],
    )

    created = 0
    skipped = 0

    for data in INSTRUMENTS_TO_SEED:
        # Check if already exists by symbol
        existing = await Instrument.find_one(
            Instrument.symbol == data["symbol"]
        )
        if existing:
            print(f"  skip  {data['symbol']} (already exists)")
            skipped += 1
            continue

        instrument = Instrument(**data)
        await instrument.insert()
        print(f"  created  {data['symbol']} — {data['name']}")
        created += 1

    print(f"\nDone: {created} created, {skipped} skipped")
    client.close()


if __name__ == "__main__":
    asyncio.run(seed())