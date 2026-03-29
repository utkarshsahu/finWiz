"""
app/scripts/fix_ncd_holding.py

One-time fix for the Edelweiss NCD holding that was ingested with
wrong quantity (1000 = face value denomination) instead of actual
bond count (50 = value / price).

Run once:
    python -m app.scripts.fix_ncd_holding
"""

import asyncio, os, certifi
from dotenv import load_dotenv
load_dotenv()


async def fix():
    from motor.motor_asyncio import AsyncIOMotorClient
    from beanie import init_beanie
    from models.holdings import Holding
    from models.instruments import Instrument, AssetClass
    from models.accounts import Account
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
    await init_beanie(
        database=client[os.getenv("DB_NAME", "finance_agent")],
        document_models=[
            Holding, Instrument, Account, Transaction, PriceSnapshot,
            Goal, GoalAllocation, Cashflow, FinancialDocument, Signal,
            Recommendation, ResearchItem, PolicyRule, ZerodhaToken,
        ],
    )

    holdings = await Holding.find(Holding.is_active == True).to_list()
    fixed = 0

    for h in holdings:
        await h.fetch_link(Holding.instrument)
        instrument = h.instrument

        # Only fix debt instruments with suspicious quantity
        if instrument.asset_class != AssetClass.DEBT:
            continue

        name_upper = instrument.name.upper()
        is_ncd = any(x in name_upper for x in ["NCD", "BOND", "DEBENTURE"])
        if not is_ncd:
            continue

        price = h.current_price
        value = h.current_value

        if price and value and price > 0:
            correct_quantity = round(value / price, 4)

            if abs(correct_quantity - h.quantity) > 1:  # meaningfully different
                print(f"Fixing: {instrument.name}")
                print(f"  Old quantity: {h.quantity}")
                print(f"  New quantity: {correct_quantity}")
                print(f"  Price: {price}, Value: {value}")

                h.quantity = correct_quantity
                h.avg_cost = value / correct_quantity if correct_quantity else 0
                h.invested_value = value  # use current value as proxy for cost
                h.recompute()
                await h.save()
                fixed += 1

    print(f"\nFixed {fixed} NCD holdings.")
    client.close()


if __name__ == "__main__":
    asyncio.run(fix())