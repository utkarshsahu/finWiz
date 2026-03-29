"""
market_data_router.py — endpoints for market data sync and price queries.

Endpoints:
  POST /market-data/sync              → trigger full sync manually
  POST /market-data/sync/mf           → MF NAVs only
  POST /market-data/sync/equities     → equities only
  GET  /market-data/prices/{symbol}   → latest price for a symbol
  GET  /market-data/prices/history/{symbol} → price history
"""

from datetime import date
from fastapi import APIRouter, HTTPException, Query
from integrations.market_data_sync import MarketDataSync
from models.instruments import Instrument
from models.prices import PriceSnapshot

router = APIRouter(prefix="/market-data", tags=["market-data"])


@router.post("/sync")
async def trigger_full_sync():
    """Manually trigger a full market data sync."""
    sync = MarketDataSync()
    results = await sync.run_full_sync()
    return {"status": "completed", "results": results}


@router.post("/sync/mf")
async def sync_mf_only():
    """Sync mutual fund NAVs only (useful after a CAS upload)."""
    sync = MarketDataSync()
    result = await sync.sync_mf_navs()
    return {"status": "completed", "result": result}


@router.post("/sync/equities")
async def sync_equities_only():
    """Sync equity prices only."""
    sync = MarketDataSync()
    result = await sync.sync_equity_prices()
    return {"status": "completed", "result": result}


@router.get("/prices/{symbol}")
async def get_latest_price(symbol: str):
    """
    Get the latest stored price for an instrument by symbol.
    Returns the most recent PriceSnapshot.
    """
    instrument = await Instrument.find_one(Instrument.symbol == symbol.upper())
    if not instrument:
        raise HTTPException(status_code=404, detail=f"Instrument '{symbol}' not found")

    snapshot = await PriceSnapshot.find_one(
        PriceSnapshot.instrument.id == instrument.id,  # type: ignore
        sort=[("price_date", -1)],
    )
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"No price data found for '{symbol}'")

    return {
        "symbol": symbol,
        "name": instrument.name,
        "price": snapshot.close,
        "nav": snapshot.nav,
        "date": str(snapshot.price_date),
        "source": snapshot.source,
    }


@router.get("/prices/history/{symbol}")
async def get_price_history(
    symbol: str,
    days: int = Query(default=90, le=365),
):
    """
    Get price history for an instrument.
    Returns up to `days` days of PriceSnapshots, newest first.
    """
    instrument = await Instrument.find_one(Instrument.symbol == symbol.upper())
    if not instrument:
        raise HTTPException(status_code=404, detail=f"Instrument '{symbol}' not found")

    from datetime import datetime, timedelta
    cutoff = date.today() - timedelta(days=days)

    snapshots = await PriceSnapshot.find(
        PriceSnapshot.instrument.id == instrument.id,  # type: ignore
        PriceSnapshot.price_date >= cutoff,
        sort=[("price_date", -1)],
    ).to_list()

    return {
        "symbol": symbol,
        "name": instrument.name,
        "history": [
            {"date": str(s.price_date), "price": s.close, "source": s.source}
            for s in snapshots
        ],
    }


@router.get("/debug/amfi")
async def debug_amfi():
    """
    Debug endpoint — shows first 5 lines of raw AMFI NAV file.
    Use this to verify the file format if total_amfi_schemes returns 0.
    """
    import httpx
    url = "https://portal.amfiindia.com/spages/NAVAll.txt"
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(url)
    lines = resp.text.splitlines()
    return {
        "status_code": resp.status_code,
        "total_lines": len(lines),
        "first_10_lines": lines[:10],
        "encoding": resp.encoding,
        "content_type": resp.headers.get("content-type"),
    }