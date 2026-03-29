"""
app/routes/recommendations_router.py

Endpoints for the recommendation engine.

Endpoints:
  POST /recommendations/generate     → Generate this week's digest
  GET  /recommendations/latest       → Get the latest recommendation
  GET  /recommendations/history      → List past recommendations
  POST /recommendations/{id}/actions/{action_id}/respond → Accept/ignore an action
"""

from fastapi import APIRouter, HTTPException
from datetime import datetime, timezone

router = APIRouter(prefix="/recommendations", tags=["recommendations"])


@router.post("/generate")
async def generate_digest():
    """
    Generate the weekly investment digest.
    Calls rules engine + reads research + calls GPT-4o once.
    Cost: ~$0.03 per call.
    """
    from app.services.recommendation_engine import RecommendationEngine
    engine = RecommendationEngine()
    result = await engine.generate_weekly_digest()
    if not result:
        raise HTTPException(status_code=500, detail="Failed to generate digest")
    return result


@router.get("/latest")
async def get_latest():
    """Get the most recent weekly recommendation."""
    from app.models.recommendations import Recommendation
    rec = await Recommendation.find_all().sort([("week_start", -1)]).limit(1).to_list()
    if not rec:
        raise HTTPException(status_code=404, detail="No recommendations yet. POST /recommendations/generate first.")

    r = rec[0]
    return {
        "id": str(r.id),
        "week_start": r.week_start.isoformat(),
        "generated_at": r.generated_at.isoformat(),
        "market_narrative": r.market_narrative,
        "actions": [
            {
                "rank": a.priority_rank,
                "type": a.action_type.value,
                "title": a.title,
                "rationale": a.rationale,
                "suggested_steps": a.suggested_steps,
                "urgency": a.urgency_score,
                "confidence": a.confidence_score,
                "status": a.status.value,
            }
            for a in sorted(r.actions, key=lambda x: x.priority_rank)
        ],
        "policy_violations": r.policy_violations,
        "contradictions": r.belief_portfolio_contradictions,
        "stale_warnings": r.stale_data_warnings,
        "data_freshness": r.overall_data_freshness_score,
        "is_delivered": r.is_delivered,
    }


@router.get("/history")
async def get_history(limit: int = 10):
    """List past weekly recommendations."""
    from app.models.recommendations import Recommendation
    recs = await Recommendation.find_all().sort([("week_start", -1)]).limit(limit).to_list()
    return [
        {
            "id": str(r.id),
            "week_start": r.week_start.isoformat(),
            "actions_count": len(r.actions),
            "generated_at": r.generated_at.isoformat(),
            "is_delivered": r.is_delivered,
        }
        for r in recs
    ]