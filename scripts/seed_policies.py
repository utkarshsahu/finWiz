"""
app/scripts/seed_policies.py

Seeds the Policy Store with sensible default rules.
Run once after first deployment.

    python -m app.scripts.seed_policies
"""

import asyncio, os, certifi
from dotenv import load_dotenv
load_dotenv()


async def seed():
    from motor.motor_asyncio import AsyncIOMotorClient
    from beanie import init_beanie
    from app.models.policies import PolicyRule, PolicyRuleType, DEFAULT_POLICIES
    from app.models.accounts import Account
    from app.models.holdings import Holding
    from app.models.instruments import Instrument
    from app.models.transactions import Transaction
    from app.models.prices import PriceSnapshot
    from app.models.goals import Goal, GoalAllocation
    from app.models.cashflows import Cashflow
    from app.models.documents import FinancialDocument
    from app.models.signals import Signal
    from app.models.recommendations import Recommendation
    from app.models.research import ResearchItem
    from app.models.zerodha_token import ZerodhaToken

    client = AsyncIOMotorClient(os.getenv("MONGO_URI"), tls=True, tlsCAFile=certifi.where())
    await init_beanie(
        database=client[os.getenv("DB_NAME", "finance_agent")],
        document_models=[
            PolicyRule, Account, Holding, Instrument, Transaction,
            PriceSnapshot, Goal, GoalAllocation, Cashflow, FinancialDocument,
            Signal, Recommendation, ResearchItem, ZerodhaToken,
        ],
    )

    created = 0
    for policy_data in DEFAULT_POLICIES:
        existing = await PolicyRule.find_one(
            PolicyRule.rule_type == policy_data["rule_type"]
        )
        if existing:
            print(f"  skip  {policy_data['title']}")
            continue
        policy = PolicyRule(**policy_data)
        await policy.insert()
        print(f"  created  {policy_data['title']}")
        created += 1

    print(f"\nDone: {created} policies seeded.")
    client.close()


if __name__ == "__main__":
    asyncio.run(seed())