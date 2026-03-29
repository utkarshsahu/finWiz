"""
app/routes/analytics_router.py

Endpoints for portfolio analytics.

Endpoints:
  GET /analytics/snapshot      → Net worth + asset allocation
  GET /analytics/xirr          → Portfolio XIRR
  GET /analytics/concentration → Concentration risk
  GET /analytics/drift         → Allocation drift vs target
  GET /analytics/goals         → Goal progress
  GET /analytics/report        → Full combined report
"""

from fastapi import APIRouter
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/snapshot")
async def get_snapshot():
    """Net worth + asset allocation breakdown."""
    svc = AnalyticsService()
    return await svc.get_portfolio_snapshot()


@router.get("/xirr")
async def get_xirr():
    """Portfolio XIRR based on transaction history."""
    svc = AnalyticsService()
    return await svc.get_portfolio_xirr()


@router.get("/concentration")
async def get_concentration():
    """Concentration risk — single holding, sector, AMC."""
    svc = AnalyticsService()
    return await svc.get_concentration_risk()


@router.get("/drift")
async def get_drift(
    equity: float = 50,
    mutual_fund: float = 30,
    debt: float = 10,
    gold: float = 10,
):
    """
    Allocation drift vs target.
    Pass your target allocation as query params (must sum to 100).
    Defaults to 50/30/10/10 equity/MF/debt/gold.
    """
    svc = AnalyticsService()
    target = {
        "equity": equity,
        "mutual_fund": mutual_fund,
        "debt": debt,
        "gold": gold,
    }
    return await svc.get_portfolio_drift(target_allocation=target)


@router.get("/goals")
async def get_goals():
    """Goal progress for all active goals."""
    svc = AnalyticsService()
    return await svc.get_goal_progress()


@router.get("/report")
async def get_full_report():
    """Full combined analytics report — used by the weekly pipeline."""
    svc = AnalyticsService()
    return await svc.get_full_report()