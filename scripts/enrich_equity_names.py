"""
scripts/enrich_equity_names.py

Enriches equity instrument names from NSE's quote API.
Instruments created via Zerodha sync only have tradingsymbol as name.
This fills in: name (company name), industry/sector.

Run:
    python -m scripts.enrich_equity_names
"""

import asyncio
import os
import certifi
import httpx
from dotenv import load_dotenv

load_dotenv()

NSE_BASE = "https://www.nseindia.com"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


async def fetch_company_info(client: httpx.AsyncClient, symbol: str) -> dict:
    try:
        resp = await client.get(
            f"{NSE_BASE}/api/quote-equity",
            params={"symbol": symbol},
            timeout=10,
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        info = data.get("info", {})
        return {
            "company_name": info.get("companyName"),
            "industry": info.get("industry"),
            "isin": info.get("isin"),
        }
    except Exception as e:
        return {}


async def enrich():
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

    client = AsyncIOMotorClient(os.getenv("MONGO_URI"), tls=True, tlsCAFile=certifi.where())
    await init_beanie(
        database=client[os.getenv("DB_NAME", "finance_agent")],
        document_models=[
            PolicyRule, Account, Holding, Instrument, Transaction,
            PriceSnapshot, Goal, GoalAllocation, Cashflow, FinancialDocument,
            Signal, Recommendation, ResearchItem, ZerodhaToken,
        ],
    )

    # Find equity instruments where name == symbol (not enriched yet)
    equity_instruments = await Instrument.find({
        "asset_class": AssetClass.EQUITY,
        "symbol": {"$not": {"$regex": r"^\^"}},   # exclude index instruments (^NSEI etc.)
        "isin": {"$exists": True, "$ne": None},    # only those with ISIN (real equities, not indices)
    }).to_list()

    to_enrich = [i for i in equity_instruments if i.name == i.symbol]
    already_ok = [i for i in equity_instruments if i.name != i.symbol]

    print(f"Equity instruments: {len(equity_instruments)} total")
    print(f"  Already have proper names: {len(already_ok)}")
    print(f"  Need enrichment (name == symbol): {len(to_enrich)}\n")

    if not to_enrich:
        print("Nothing to enrich.")
        client.close()
        return

    # Prime NSE session once
    async with httpx.AsyncClient(headers=NSE_HEADERS, follow_redirects=True, timeout=15) as http:
        await http.get(NSE_BASE)  # prime cookies
        await asyncio.sleep(0.5)

        fixed = skipped = 0
        for inst in to_enrich:
            info = await fetch_company_info(http, inst.symbol)
            await asyncio.sleep(0.3)  # gentle rate limit

            if not info.get("company_name"):
                print(f"  SKIP  {inst.symbol:20} — not found on NSE")
                skipped += 1
                continue

            inst.name = info["company_name"]
            if info.get("industry") and not inst.industry:
                inst.industry = info["industry"]
            if info.get("isin") and not inst.isin:
                inst.isin = info["isin"]
            await inst.save()
            print(f"  OK    {inst.symbol:20} → {inst.name}")
            fixed += 1

    print(f"\nDone: {fixed} enriched, {skipped} skipped.")
    client.close()


if __name__ == "__main__":
    asyncio.run(enrich())
