"""
app/routes/rules_router.py

Endpoints for the rules engine and signals.

Endpoints:
  POST /rules/run          → Run all rules, generate signals
  GET  /rules/signals      → List active signals
  POST /rules/signals/{id}/resolve → Mark a signal resolved
"""

from fastapi import APIRouter, HTTPException
from app.services.rules_engine import RulesEngine

router = APIRouter(prefix="/rules", tags=["rules"])


@router.post("/run")
async def run_rules(
    equity_target: float = 50,
    mutual_fund_target: float = 30,
    debt_target: float = 10,
    gold_target: float = 10,
):
    """
    Run all rules and generate signals.
    Pass your target allocation as query params.
    """
    engine = RulesEngine(target_allocation={
        "equity": equity_target,
        "mutual_fund": mutual_fund_target,
        "debt": debt_target,
        "gold": gold_target,
    })
    return await engine.run_all()


@router.get("/signals")
async def get_signals(
    severity: str = None,
    resolved: bool = False,
):
    """List signals, optionally filtered by severity or resolved status."""
    from app.models.signals import Signal, SignalSeverity

    query = Signal.find(Signal.is_resolved == resolved)

    signals = await query.sort([("generated_at", -1)]).to_list()

    if severity:
        try:
            sev = SignalSeverity(severity)
            signals = [s for s in signals if s.severity == sev]
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid severity: {severity}")

    return [
        {
            "id": str(s.id),
            "type": s.signal_type.value,
            "severity": s.severity.value,
            "title": s.title,
            "description": s.description,
            "data": s.data,
            "data_freshness_score": s.data_freshness_score,
            "generated_at": s.generated_at.isoformat(),
            "is_resolved": s.is_resolved,
        }
        for s in signals
    ]


@router.post("/signals/{signal_id}/resolve")
async def resolve_signal(signal_id: str, note: str = ""):
    """Mark a signal as resolved."""
    from app.models.signals import Signal
    from datetime import datetime, timezone

    signal = await Signal.get(signal_id)
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")

    signal.is_resolved = True
    signal.resolved_at = datetime.now(timezone.utc)
    signal.resolution_note = note
    await signal.save()

    return {"status": "resolved", "signal_id": signal_id}