"""
app/scripts/seed_policies.py

Seeds the Policy Store with sensible default rules.
Run once after first deployment.

    python -m scripts.seed_policies
"""

import asyncio, os, certifi
from dotenv import load_dotenv
load_dotenv()


async def seed():
    from motor.motor_asyncio import AsyncIOMotorClient
    from beanie import init_beanie
    from models.policies import PolicyRule, PolicyRuleType, DEFAULT_POLICIES
    from models.accounts import Account
    from models.holdings import Holding
    from models.instruments import Instrument
    from models.transactions import Transaction
    from models.prices import PriceSnapshot
    from models.goals import Goal, GoalAllocation
    from models.cashflows import Cashflow
    from models.documents import FinancialDocument
    from models.signals import Signal
    from models.recommendations import Recommendation
    from models.research import ResearchItem
    from models.zerodha_token import ZerodhaToken

    client = AsyncIOMotorClient(os.getenv("MONGO_URI"), tls=True, tlsCAFile=certifi.where())
    await init_beanie(
        database=client[os.getenv("DB_NAME", "finance_agent")],
        document_models=[
            PolicyRule, Account, Holding, Instrument, Transaction,
            PriceSnapshot, Goal, GoalAllocation, Cashflow, FinancialDocument,
            Signal, Recommendation, ResearchItem, ZerodhaToken,
        ],
    )

    created = updated = 0
    for policy_data in DEFAULT_POLICIES:
        # For TARGET_ALLOCATION_PCT, match by rule_type + asset_class (one per asset class).
        # For all others, match by rule_type alone.
        from models.policies import PolicyRuleType
        if policy_data["rule_type"] == PolicyRuleType.TARGET_ALLOCATION_PCT:
            query = {
                "rule_type": policy_data["rule_type"],
                "parameters.asset_class": policy_data["parameters"]["asset_class"],
            }
        else:
            query = {"rule_type": policy_data["rule_type"]}

        existing = await PolicyRule.find_one(query)
        if existing:
            for k, v in policy_data.items():
                setattr(existing, k, v)
            await existing.save()
            print(f"  updated  {policy_data['title']}")
            updated += 1
        else:
            policy = PolicyRule(**policy_data)
            await policy.insert()
            print(f"  created  {policy_data['title']}")
            created += 1

    print(f"\nDone: {created} created, {updated} updated.")
    client.close()


if __name__ == "__main__":
    asyncio.run(seed())