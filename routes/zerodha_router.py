"""
app/routes/zerodha_router.py

FastAPI endpoints for Zerodha auth and sync.

Endpoints:
  GET  /zerodha/login      → Redirects to Kite login page
  GET  /zerodha/callback   → Handles post-login redirect from Kite
  POST /zerodha/sync       → Manually trigger a full sync
  GET  /zerodha/status     → Current auth + last sync status
"""

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse, JSONResponse

router = APIRouter(prefix="/zerodha", tags=["zerodha"])


@router.get("/login")
async def zerodha_login():
    """Redirects to Kite login page."""
    from integrations.zerodha_auth import get_zerodha_auth
    return RedirectResponse(url=get_zerodha_auth().get_login_url())


@router.get("/callback")
async def zerodha_callback(request_token: str = Query(...)):
    """
    Kite redirects here after login with ?request_token=...
    Set your Kite app redirect URL to:
      http://localhost:8000/zerodha/callback  (local dev)
    """
    from integrations.zerodha_auth import get_zerodha_auth
    try:
        token = await get_zerodha_auth().handle_callback(request_token)
        return JSONResponse({
            "status": "authenticated",
            "user_id": token.user_id,
            "expires_at": token.expires_at.isoformat(),
            "message": "Token stored. You can now close this page.",
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Auth failed: {str(e)}")


@router.post("/sync")
async def trigger_sync(
    holdings: bool = True,
    transactions: bool = True,
):
    """
    Manually trigger a Zerodha sync.
    Prices are now handled by /market-data/sync (yfinance), not Zerodha.
    """
    from integrations.zerodha_auth import get_zerodha_auth
    from integrations.zerodha_sync import ZerodhaSync

    if not await get_zerodha_auth().is_authenticated():
        raise HTTPException(
            status_code=401,
            detail="Zerodha token missing or expired. Visit /zerodha/login first.",
        )

    sync = ZerodhaSync()
    results = {}

    if holdings:
        results["holdings"] = await sync.sync_holdings()
    if transactions:
        results["transactions"] = await sync.sync_transactions()

    return {"status": "completed", "results": results}


@router.get("/status")
async def zerodha_status():
    """Current auth state and last sync timestamp."""
    from integrations.zerodha_auth import get_zerodha_auth
    from models.accounts import Account, DataSource
    from datetime import datetime, timezone

    auth = get_zerodha_auth()
    token = await auth.get_current_token()
    account = await Account.find_one(Account.data_source == DataSource.ZERODHA)

    return {
        "authenticated": token is not None,
        "token_expires_at": token.expires_at.isoformat() if token else None,
        "user_id": token.user_id if token else None,
        "last_sync": account.last_synced_at.isoformat() if account and account.last_synced_at else None,
        "freshness_score": account.freshness_score if account else 0.0,
        "login_url": auth.get_login_url() if not token else None,
    }