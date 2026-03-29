"""
app/models/goals.py

Goal + GoalAllocation — goal-based investing engine.

Fix: GoalAllocation previously used Link[Holding] which caused a circular
import chain (goals → holdings → accounts → beanie init order issues).

GoalAllocation now stores holding_id as a plain PydanticObjectId.
Resolve the actual Holding in service layer code:

    from app.models.holdings import Holding
    holding = await Holding.get(allocation.holding_id)
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional
from beanie import Document, PydanticObjectId
from pydantic import Field


class GoalStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    ACHIEVED = "achieved"
    ABANDONED = "abandoned"


class RiskLevel(str, Enum):
    VERY_LOW = "very_low"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    VERY_HIGH = "very_high"


class Goal(Document):
    """
    A financial goal with a target corpus and deadline.
    Reviewed monthly by the Goal Planner agent — not weekly.
    """

    name: str
    description: Optional[str] = None
    status: GoalStatus = GoalStatus.ACTIVE

    target_corpus: float
    target_date: Optional[date] = None
    risk_level: RiskLevel = RiskLevel.MODERATE

    # e.g. {"equity": 60, "debt": 30, "gold": 10} — should sum to 100
    recommended_allocation: dict[str, float] = Field(default_factory=dict)

    current_corpus: float = 0.0
    funding_gap: float = 0.0
    projected_corpus_at_target_date: Optional[float] = None
    is_on_track: Optional[bool] = None

    last_reviewed_at: Optional[datetime] = None
    review_frequency_days: int = 30

    notes: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "goals"


class GoalAllocation(Document):
    """
    Maps a fraction of a holding to a goal.

    Uses plain ObjectIds (not Beanie Links) to avoid circular imports.
    Resolve in service layer when you need the actual documents.

    Example:
        allocation = GoalAllocation(
            goal_id=house_goal.id,
            holding_id=gold_etf_holding.id,
            proportion=0.6,   # 60% of this holding funds the house goal
        )
    """

    goal_id: PydanticObjectId
    holding_id: PydanticObjectId
    proportion: float = 1.0             # 0.0 to 1.0

    allocation_type: str = "proportional"
    fixed_amount: Optional[float] = None

    notes: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "goal_allocations"