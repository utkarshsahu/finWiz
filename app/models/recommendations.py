"""
app/models/recommendations.py

Recommendation — the weekly digest output: scored, prioritised actions.

Fix: RecommendedAction is now an EmbeddedDocument (not a top-level Document)
since it only ever lives inside a Recommendation. This avoids registering
it as a separate collection and fixes the Beanie init error.
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional
from beanie import Document
from beanie.odm.fields import PydanticObjectId
from pydantic import BaseModel, Field


class ActionType(str, Enum):
    REBALANCE_REDUCE = "rebalance_reduce"
    REBALANCE_ADD = "rebalance_add"
    DEPLOY_IDLE_CASH = "deploy_idle_cash"
    STAGGER_BUY = "stagger_buy"
    START_SIP = "start_sip"
    INCREASE_SIP = "increase_sip"
    PAUSE_SIP = "pause_sip"
    REVIEW_CONCENTRATION = "review_concentration"
    STOP_LOSS_REVIEW = "stop_loss_review"
    RESTORE_EMERGENCY_FUND = "restore_emergency_fund"
    REVIEW_GOAL_ALLOCATION = "review_goal_allocation"
    INCREASE_GOAL_CONTRIBUTION = "increase_goal_contribution"
    UPLOAD_DOCUMENT = "upload_document"
    REVIEW_POLICY_VIOLATION = "review_policy_violation"
    NO_ACTION = "no_action"


class ActionStatus(str, Enum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    IGNORED = "ignored"
    POSTPONED = "postponed"
    COMPLETED = "completed"


class RecommendedAction(BaseModel):
    """
    A single actionable recommendation — embedded inside Recommendation.
    Uses BaseModel (not Document) since it has no standalone collection.
    """

    action_type: ActionType
    title: str
    rationale: str
    suggested_steps: list[str] = Field(default_factory=list)

    urgency_score: float = 0.5
    impact_score: float = 0.5
    confidence_score: float = 0.5
    data_freshness_score: float = 1.0
    priority_rank: int = 0

    supporting_signal_ids: list[str] = Field(default_factory=list)
    related_instrument_ids: list[str] = Field(default_factory=list)
    related_goal_ids: list[str] = Field(default_factory=list)

    status: ActionStatus = ActionStatus.PENDING
    user_notes: Optional[str] = None
    responded_at: Optional[datetime] = None


class Recommendation(Document):
    """
    The full weekly digest for one week.
    One document per week — upserted by the weekly pipeline.
    """

    week_start: date
    generated_at: datetime = Field(default_factory=datetime.utcnow)

    actions: list[RecommendedAction] = Field(default_factory=list)
    market_narrative: Optional[str] = None
    no_action_rationale: Optional[str] = None

    # Denormalized portfolio snapshot at generation time
    portfolio_snapshot: dict = Field(default_factory=dict)

    overall_data_freshness_score: float = 1.0
    stale_data_warnings: list[str] = Field(default_factory=list)
    policy_violations: list[str] = Field(default_factory=list)
    belief_portfolio_contradictions: list[str] = Field(default_factory=list)

    is_delivered: bool = False
    delivered_at: Optional[datetime] = None

    class Settings:
        name = "recommendations"