"""
create_indexes.py — run once after first deployment to set up indexes.

Also sets up the Atlas Vector Search index for ResearchItem embeddings.
The vector search index is created via the Atlas UI or CLI, not the driver —
see the comment at the bottom of this file.

Run with:
    python -m scripts.create_indexes
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, IndexModel
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "finance_agent")


async def create_indexes():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    print("Creating indexes...")

    # --- instruments ---
    await db.instruments.create_indexes([
        IndexModel([("isin", ASCENDING)], unique=True, sparse=True),
        IndexModel([("symbol", ASCENDING)], unique=True),
        IndexModel([("asset_class", ASCENDING)]),
        IndexModel([("sector", ASCENDING)]),
    ])
    print("✓ instruments")

    # --- accounts ---
    await db.accounts.create_indexes([
        IndexModel([("data_source", ASCENDING)]),
        IndexModel([("last_synced_at", DESCENDING)]),
        IndexModel([("is_active", ASCENDING)]),
    ])
    print("✓ accounts")

    # --- holdings ---
    # Most common query: all active holdings with current values
    # Second most common: all holdings for a specific account
    await db.holdings.create_indexes([
        IndexModel([("account.$id", ASCENDING), ("instrument.$id", ASCENDING)], unique=True),
        IndexModel([("instrument.$id", ASCENDING)]),
        IndexModel([("is_active", ASCENDING), ("current_value", DESCENDING)]),
        IndexModel([("last_synced_at", DESCENDING)]),
    ])
    print("✓ holdings")

    # --- transactions ---
    # Most common queries: by account+date, by instrument, for XIRR calculation
    await db.transactions.create_indexes([
        IndexModel([("account.$id", ASCENDING), ("transaction_date", DESCENDING)]),
        IndexModel([("instrument.$id", ASCENDING), ("transaction_date", DESCENDING)]),
        IndexModel([("transaction_type", ASCENDING), ("transaction_date", DESCENDING)]),
        IndexModel([("dedup_hash", ASCENDING)], unique=True, sparse=True),
        IndexModel([("source", ASCENDING)]),
    ])
    print("✓ transactions")

    # --- price_snapshots ---
    # Most common: latest price for an instrument, price history for a date range
    await db.price_snapshots.create_indexes([
        IndexModel(
            [("instrument.$id", ASCENDING), ("price_date", DESCENDING)],
            unique=True
        ),
        IndexModel([("price_date", DESCENDING)]),
    ])
    print("✓ price_snapshots")

    # --- goals ---
    await db.goals.create_indexes([
        IndexModel([("status", ASCENDING)]),
        IndexModel([("target_date", ASCENDING)]),
    ])
    print("✓ goals")

    # --- goal_allocations ---
    await db.goal_allocations.create_indexes([
        IndexModel([("goal.$id", ASCENDING)]),
        IndexModel([("holding.$id", ASCENDING)]),
        IndexModel([("goal.$id", ASCENDING), ("holding.$id", ASCENDING)], unique=True),
    ])
    print("✓ goal_allocations")

    # --- cashflows ---
    await db.cashflows.create_indexes([
        IndexModel([("account.$id", ASCENDING), ("cashflow_date", DESCENDING)]),
        IndexModel([("cashflow_type", ASCENDING), ("cashflow_date", DESCENDING)]),
        IndexModel([("cashflow_date", DESCENDING)]),
        IndexModel([("dedup_hash", ASCENDING)], unique=True, sparse=True),
    ])
    print("✓ cashflows")

    # --- financial_documents ---
    await db.financial_documents.create_indexes([
        IndexModel([("parse_status", ASCENDING)]),
        IndexModel([("doc_type", ASCENDING), ("date_range_end", DESCENDING)]),
        IndexModel([("uploaded_at", DESCENDING)]),
    ])
    print("✓ financial_documents")

    # --- signals ---
    await db.signals.create_indexes([
        IndexModel([("signal_type", ASCENDING), ("generated_at", DESCENDING)]),
        IndexModel([("severity", ASCENDING), ("is_resolved", ASCENDING)]),
        IndexModel([("generated_at", DESCENDING)]),
        IndexModel([("dedup_key", ASCENDING)], unique=True, sparse=True),
    ])
    print("✓ signals")

    # --- recommendations ---
    await db.recommendations.create_indexes([
        IndexModel([("week_start", DESCENDING)], unique=True),
        IndexModel([("is_delivered", ASCENDING)]),
    ])
    print("✓ recommendations")

    # --- research_items ---
    # Frequent queries: by theme, by date, by relevance score
    await db.research_items.create_indexes([
        IndexModel([("themes", ASCENDING)]),
        IndexModel([("relevant_asset_classes", ASCENDING)]),
        IndexModel([("published_date", DESCENDING)]),
        IndexModel([("portfolio_relevance_score", DESCENDING)]),
        IndexModel([("sentiment", ASCENDING)]),
        IndexModel([("source_url", ASCENDING)], unique=True, sparse=True),  # Dedup
    ])
    print("✓ research_items")

    # --- policies ---
    await db.policies.create_indexes([
        IndexModel([("rule_type", ASCENDING)]),
        IndexModel([("is_active", ASCENDING)]),
    ])
    print("✓ policies")

    print("\nAll indexes created successfully.")
    print("\n" + "="*60)
    print("NEXT STEP: Atlas Vector Search index for research_items")
    print("="*60)
    print("""
Create this via the MongoDB Atlas UI:
  1. Go to your cluster → Search → Create Search Index
  2. Select JSON editor and paste:

{
  "mappings": {
    "dynamic": false,
    "fields": {
      "embedding": {
        "dimensions": 1024,
        "similarity": "cosine",
        "type": "knnVector"
      },
      "themes": { "type": "token" },
      "relevant_asset_classes": { "type": "token" },
      "published_date": { "type": "date" },
      "portfolio_relevance_score": { "type": "number" }
    }
  }
}

  3. Index name: research_vector_index
  4. Collection: research_items
  5. Dimensions: match your embedding model
     - Voyage AI (recommended, cheapest): 1024
     - OpenAI text-embedding-3-small: 1536
     - Claude / Cohere: 1024

Note: Atlas Vector Search is free on M0 (free tier) clusters.
""")

    client.close()


if __name__ == "__main__":
    asyncio.run(create_indexes())