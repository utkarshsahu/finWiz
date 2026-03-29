"""
app/db.py

Database connection and Beanie initialization.

Key rule: AsyncIOMotorClient must be created INSIDE an async context
(inside init_db), never at module import time. Creating it at the top
level causes the "MotorDatabase object is not callable" error because
the event loop isn't running yet when the module is imported.
"""

import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# Module-level client reference — set once during init_db(), reused after
_client = None


async def init_db():
    """
    Initialize MongoDB connection and Beanie ODM.
    Must be called inside an async context (FastAPI lifespan).
    All imports are inside this function to ensure models are only
    registered after the event loop is running.
    """
    global _client

    from motor.motor_asyncio import AsyncIOMotorClient
    from beanie import init_beanie

    # Import all document models here — not at module top level
    from models.instruments import Instrument
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

    _client = AsyncIOMotorClient(MONGO_URI)

    # Explicitly get the database object — never pass the client itself
    database = _client[DB_NAME]

    await init_beanie(
        database=database,
        document_models=[
            Instrument,
            Account,
            Holding,
            Transaction,
            PriceSnapshot,
            Goal,
            GoalAllocation,
            Cashflow,
            FinancialDocument,
            Signal,
            Recommendation,
            ResearchItem,
            PolicyRule,
            ZerodhaToken,
        ],
    )
    print(f"✓ Connected to MongoDB Atlas — database: {DB_NAME}")


async def close_db():
    """Call on app shutdown to cleanly close the motor connection."""
    global _client
    if _client:
        _client.close()
        _client = None