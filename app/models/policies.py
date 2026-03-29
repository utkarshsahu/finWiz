"""
Policy — your personal investment rules, stored as structured documents.

This is the Policy Store from the architecture. Every week, the
Portfolio Analyst agent checks your portfolio against these rules and
flags violations as signals.

The "contradiction detector" also uses this — if you've consumed 5 bearish
articles about small caps but your small-cap allocation is above target,
that's a belief-portfolio contradiction worth surfacing.

Rules are stored as structured documents (not arbitrary code) so the
LLM can reason about them without executing them directly.
The rules_engine.py module translates PolicyRule documents into
actual Python checks.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Any
from beanie import Document
from pydantic import Field


class PolicyRuleType(str, Enum):
    # Concentration limits
    MAX_SINGLE_STOCK_PCT = "max_single_stock_pct"
    MAX_SINGLE_SECTOR_PCT = "max_single_sector_pct"
    MAX_SINGLE_FUND_PCT = "max_single_fund_pct"
    # Asset allocation bounds
    MIN_EQUITY_PCT = "min_equity_pct"
    MAX_EQUITY_PCT = "max_equity_pct"
    MIN_DEBT_PCT = "min_debt_pct"
    MAX_GOLD_PCT = "max_gold_pct"
    # Liquidity rules
    MIN_EMERGENCY_FUND_MONTHS = "min_emergency_fund_months"
    MIN_LIQUID_ASSETS_PCT = "min_liquid_assets_pct"
    # Buying rules
    STAGGER_BUY_ONLY = "stagger_buy_only"           # No lump sum, only tranches
    NO_SMALL_CAP_ABOVE_PE = "no_small_cap_above_pe" # Don't add small-cap above threshold P/E
    GOAL_HORIZON_ASSET_MATCH = "goal_horizon_asset_match"  # Short-term goals → low-risk assets
    # SIP rules
    MAINTAIN_SIP_AMOUNT = "maintain_sip_amount"     # Don't reduce SIPs below threshold
    # Custom
    CUSTOM = "custom"


class PolicyRule(Document):
    """
    One investment policy rule.

    parameters is flexible — its schema depends on rule_type:
      MAX_SINGLE_STOCK_PCT → {"threshold_pct": 10}
      MIN_EMERGENCY_FUND_MONTHS → {"months": 6}
      NO_SMALL_CAP_ABOVE_PE → {"max_pe": 28}
      STAGGER_BUY_ONLY → {"min_tranches": 3, "tranche_interval_days": 7}

    The rules engine reads rule_type + parameters to run the check.
    The LLM reads description to explain violations in plain language.
    """

    rule_type: PolicyRuleType
    title: str                                  # Short display label
    description: str                            # Plain language: "what this rule means"
    rationale: Optional[str] = None            # Why you have this rule

    parameters: dict[str, Any] = Field(default_factory=dict)

    is_active: bool = True
    severity: str = "normal"                   # "urgent" or "normal" — how to surface violations
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "policies"


# ---------------------------------------------------------------------------
# Seed policies — run once to populate your Policy Store with sensible defaults
# ---------------------------------------------------------------------------
DEFAULT_POLICIES = [
    {
        "rule_type": PolicyRuleType.MAX_SINGLE_STOCK_PCT,
        "title": "No single stock > 10% of portfolio",
        "description": "Individual equity positions must not exceed 10% of total portfolio value",
        "rationale": "Concentration in one stock exposes the portfolio to company-specific risk",
        "parameters": {"threshold_pct": 10},
        "severity": "urgent",
    },
    {
        "rule_type": PolicyRuleType.MAX_SINGLE_SECTOR_PCT,
        "title": "No single sector > 25% of equity",
        "description": "Sector concentration in equity holdings must stay below 25%",
        "parameters": {"threshold_pct": 25},
        "severity": "normal",
    },
    {
        "rule_type": PolicyRuleType.MIN_EMERGENCY_FUND_MONTHS,
        "title": "Emergency fund ≥ 6 months expenses",
        "description": "Liquid assets (savings + liquid funds) must cover at least 6 months of average monthly expenses",
        "parameters": {"months": 6},
        "severity": "urgent",
    },
    {
        "rule_type": PolicyRuleType.MAX_GOLD_PCT,
        "title": "Gold ≤ 15% of portfolio",
        "description": "Gold (ETF, SGB, physical) must not exceed 15% of total portfolio",
        "parameters": {"threshold_pct": 15},
        "severity": "normal",
    },
    {
        "rule_type": PolicyRuleType.GOAL_HORIZON_ASSET_MATCH,
        "title": "Short-term goals in low-risk assets only",
        "description": "Goals with target dates under 3 years should be funded only by debt/liquid instruments",
        "parameters": {"short_horizon_years": 3, "allowed_asset_classes": ["debt", "cash", "liquid"]},
        "severity": "normal",
    },
]