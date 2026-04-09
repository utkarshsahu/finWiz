"""
app/services/rules_engine.py

Rules engine — runs deterministic policy checks against the portfolio
and writes Signal documents.

Zero LLM calls. All logic is pure Python.

Signals are deduplicated by dedup_key so running the engine multiple
times in a week doesn't create duplicate signals. A signal is only
re-created if the previous one was resolved.

Called by:
  - Celery daily task (after market data sync)
  - POST /rules/run endpoint (manual trigger)
  - Weekly recommendation pipeline (reads signals generated here)
"""

import hashlib
import logging
from datetime import datetime, date, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


def _dedup_key(signal_type: str, context: str) -> str:
    """Generate a stable dedup key for a signal."""
    raw = f"{signal_type}:{context}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


async def _upsert_signal(
    signal_type,
    severity,
    title: str,
    description: str,
    data: dict,
    data_freshness_score: float = 1.0,
    related_instrument_ids: list[str] = None,
    related_goal_ids: list[str] = None,
    related_account_ids: list[str] = None,
    dedup_context: str = "",
) -> bool:
    """
    Insert a signal if not already active.
    Returns True if a new signal was created, False if already exists.
    """
    from models.signals import Signal, SignalType, SignalSeverity

    dedup = _dedup_key(signal_type.value, dedup_context)

    existing = await Signal.find_one(
        {"dedup_key": dedup, "is_resolved": False}
    )
    if existing:
        return False  # already active, skip

    signal = Signal(
        signal_type=signal_type,
        severity=severity,
        title=title,
        description=description,
        data=data,
        data_freshness_score=data_freshness_score,
        related_instrument_ids=related_instrument_ids or [],
        related_goal_ids=related_goal_ids or [],
        related_account_ids=related_account_ids or [],
        generated_at=datetime.now(timezone.utc),
        dedup_key=dedup,
    )
    await signal.insert()
    return True


class RulesEngine:

    def __init__(self, target_allocation: Optional[dict] = None):
        """
        target_allocation: dict of {asset_class: target_pct}
        If None, reads TARGET_ALLOCATION_PCT PolicyRule documents at runtime,
        falling back to these defaults if none are found in the DB.
        """
        self._override_allocation = target_allocation
        self._fallback_allocation = {
            "equity": 30,
            "mutual_fund": 55,
            "debt": 10,
            "gold": 5,
        }
        self.signals_created = 0
        self.signals_skipped = 0

    async def _load_policies(self) -> list:
        """Load active policy rules from DB."""
        from models.policies import PolicyRule
        return await PolicyRule.find({"is_active": True}).to_list()

    async def _load_target_allocation(self) -> tuple[dict, dict]:
        """
        Returns (target_pct_by_class, thresholds_by_class).
        Reads TARGET_ALLOCATION_PCT rules from DB; falls back to _fallback_allocation.
        thresholds_by_class: {asset_class: {"normal": x, "urgent": y}}
        """
        if self._override_allocation:
            targets = self._override_allocation
            thresholds = {ac: {"normal": 7.0, "urgent": 12.0} for ac in targets}
            return targets, thresholds

        from models.policies import PolicyRule, PolicyRuleType
        rules = await PolicyRule.find(
            {"rule_type": PolicyRuleType.TARGET_ALLOCATION_PCT, "is_active": True}
        ).to_list()

        if not rules:
            targets = self._fallback_allocation
            thresholds = {ac: {"normal": 7.0, "urgent": 12.0} for ac in targets}
            return targets, thresholds

        targets = {}
        thresholds = {}
        for rule in rules:
            ac = rule.parameters.get("asset_class")
            if not ac:
                continue
            targets[ac] = rule.parameters.get("target_pct", 0)
            thresholds[ac] = {
                "normal": rule.parameters.get("drift_normal_pct", 7.0),
                "urgent": rule.parameters.get("drift_urgent_pct", 12.0),
            }
        return targets, thresholds

    # ---------------------------------------------------------------------------
    # Rule 1: Allocation drift
    # ---------------------------------------------------------------------------

    async def check_allocation_drift(self, snapshot: dict) -> int:
        """
        Flag if any asset class has drifted more than threshold from target.
        Thresholds and targets are loaded from TARGET_ALLOCATION_PCT PolicyRule documents.
        """
        from models.signals import SignalType, SignalSeverity

        by_ac = snapshot.get("by_asset_class", {})
        created = 0

        target_allocation, thresholds = await self._load_target_allocation()

        for asset_class, target_pct in target_allocation.items():
            current_pct = by_ac.get(asset_class, {}).get("pct", 0)
            drift = current_pct - target_pct

            drift_normal = thresholds.get(asset_class, {}).get("normal", 7.0)
            drift_urgent = thresholds.get(asset_class, {}).get("urgent", 12.0)

            if abs(drift) < drift_normal:
                continue

            direction = "overweight" if drift > 0 else "underweight"
            severity = (
                SignalSeverity.URGENT if abs(drift) >= drift_urgent
                else SignalSeverity.NORMAL
            )
            display_class = asset_class.replace("_", " ").title()

            created_flag = await _upsert_signal(
                signal_type=SignalType.ALLOCATION_DRIFT,
                severity=severity,
                title=f"{display_class} allocation {direction} by {abs(drift):.1f}%",
                description=(
                    f"Your {display_class} allocation is {current_pct:.1f}% "
                    f"vs target {target_pct:.1f}% — a drift of {drift:+.1f}%. "
                    f"Consider rebalancing."
                ),
                data={
                    "asset_class": asset_class,
                    "current_pct": round(current_pct, 2),
                    "target_pct": target_pct,
                    "drift_pct": round(drift, 2),
                    "direction": direction,
                    "current_value": round(by_ac.get(asset_class, {}).get("value", 0), 2),
                },
                dedup_context=f"{asset_class}:{direction}",
            )
            if created_flag:
                created += 1
                logger.info(f"Signal: allocation drift — {asset_class} {direction} {drift:+.1f}%")

        return created

    # ---------------------------------------------------------------------------
    # Rule 2: Single holding concentration
    # ---------------------------------------------------------------------------

    async def check_concentration(self, snapshot: dict) -> int:
        """
        Flag if a single holding exceeds policy limit (default 10%).
        """
        from models.signals import SignalType, SignalSeverity
        from models.holdings import Holding

        total = snapshot.get("total_value", 0)
        if total == 0:
            return 0

        # Load threshold from policy if available
        policies = await self._load_policies()
        threshold = 10.0
        for p in policies:
            if p.rule_type.value == "max_single_stock_pct":
                threshold = p.parameters.get("threshold_pct", 10.0)

        holdings = await Holding.find(Holding.is_active == True).to_list()
        created = 0

        for h in holdings:
            val = h.current_value or 0
            pct = (val / total) * 100
            if pct <= threshold:
                continue

            await h.fetch_link(Holding.instrument)
            name = h.instrument.name

            severity = (
                SignalSeverity.URGENT if pct > threshold * 1.5
                else SignalSeverity.NORMAL
            )

            # Prefer short_name; fall back to name unless it's a raw numeric code
            # (AMFI scheme codes get stored as name when instrument isn't fully seeded)
            if h.instrument.short_name:
                display_name = h.instrument.short_name
            elif h.instrument.name and not h.instrument.name.strip().isdigit():
                display_name = h.instrument.name[:40]
            elif h.instrument.fund_house:
                display_name = f"{h.instrument.fund_house} Fund"
            else:
                display_name = h.instrument.symbol
            created_flag = await _upsert_signal(
                signal_type=SignalType.CONCENTRATION_BREACH,
                severity=severity,
                title=f"{display_name} is {pct:.1f}% of portfolio",
                description=(
                    f"{name} represents {pct:.1f}% of your total portfolio "
                    f"(₹{val:,.0f}), exceeding the {threshold}% limit."
                ),
                data={
                    "instrument": name,
                    "symbol": h.instrument.symbol,
                    "pct": round(pct, 2),
                    "value": round(val, 2),
                    "threshold_pct": threshold,
                },
                related_instrument_ids=[str(h.instrument.id)],
                dedup_context=f"{h.instrument.symbol}:concentration",
            )
            if created_flag:
                created += 1

        return created

    # ---------------------------------------------------------------------------
    # Rule 3: Document staleness
    # ---------------------------------------------------------------------------

    async def check_document_staleness(self) -> int:
        """
        Flag if key documents haven't been uploaded recently.
        Thresholds: CAS > 35 days, bank statement > 35 days.
        """
        from models.signals import SignalType, SignalSeverity
        from models.documents import FinancialDocument, DocType, ParseStatus

        created = 0
        today = date.today()
        STALE_DAYS = 35

        doc_types = [
            (DocType.CAS, "CAS statement", "Upload a fresh CAS from camsonline.com"),
            (DocType.BANK_STATEMENT, "Bank statement", "Upload your latest bank statement"),
        ]

        for doc_type, label, action in doc_types:
            # Find most recent successfully parsed document of this type
            docs = await FinancialDocument.find(
                FinancialDocument.doc_type == doc_type,
                FinancialDocument.parse_status == ParseStatus.INGESTED,
            ).sort([("uploaded_at", -1)]).limit(1).to_list()

            if not docs:
                age_days = STALE_DAYS + 1  # treat as stale if never uploaded
                last_upload = None
            else:
                last_upload = docs[0].uploaded_at
                if last_upload:
                    if last_upload.tzinfo is None:
                        last_upload = last_upload.replace(tzinfo=timezone.utc)
                    age_days = (datetime.now(timezone.utc) - last_upload).days
                else:
                    age_days = STALE_DAYS + 1

            if age_days <= STALE_DAYS:
                continue

            created_flag = await _upsert_signal(
                signal_type=SignalType.DOCUMENT_STALE,
                severity=SignalSeverity.NORMAL,
                title=f"{label} not updated in {age_days} days",
                description=(
                    f"Your {label} was last uploaded "
                    f"{'never' if not last_upload else f'{age_days} days ago'}. "
                    f"{action} to keep your portfolio data fresh."
                ),
                data={
                    "doc_type": doc_type.value,
                    "age_days": age_days,
                    "last_upload": last_upload.isoformat() if last_upload else None,
                    "threshold_days": STALE_DAYS,
                },
                dedup_context=f"{doc_type.value}:stale",
            )
            if created_flag:
                created += 1
                logger.info(f"Signal: document stale — {label} ({age_days} days)")

        return created

    # ---------------------------------------------------------------------------
    # Rule 4: Policy violations
    # ---------------------------------------------------------------------------

    async def check_policy_violations(self, snapshot: dict) -> int:
        """
        Check portfolio against all active PolicyRule documents.
        """
        from models.signals import SignalType, SignalSeverity
        from models.policies import PolicyRuleType

        policies = await self._load_policies()
        by_ac = snapshot.get("by_asset_class", {})
        total = snapshot.get("total_value", 0)
        created = 0

        for policy in policies:
            params = policy.parameters
            rule_type = policy.rule_type
            violated = False
            violation_data = {}

            if rule_type == PolicyRuleType.MAX_GOLD_PCT:
                threshold = params.get("threshold_pct", 15)
                gold_pct = by_ac.get("gold", {}).get("pct", 0)
                if gold_pct > threshold:
                    violated = True
                    violation_data = {
                        "current_pct": round(gold_pct, 2),
                        "threshold_pct": threshold,
                    }

            elif rule_type == PolicyRuleType.MIN_EQUITY_PCT:
                threshold = params.get("threshold_pct", 40)
                eq_pct = by_ac.get("equity", {}).get("pct", 0)
                mf_pct = by_ac.get("mutual_fund", {}).get("pct", 0)
                total_equity = eq_pct + mf_pct
                if total_equity < threshold:
                    violated = True
                    violation_data = {
                        "current_pct": round(total_equity, 2),
                        "threshold_pct": threshold,
                    }

            elif rule_type == PolicyRuleType.MAX_EQUITY_PCT:
                threshold = params.get("threshold_pct", 80)
                eq_pct = by_ac.get("equity", {}).get("pct", 0)
                mf_pct = by_ac.get("mutual_fund", {}).get("pct", 0)
                total_equity = eq_pct + mf_pct
                if total_equity > threshold:
                    violated = True
                    violation_data = {
                        "current_pct": round(total_equity, 2),
                        "threshold_pct": threshold,
                    }

            if not violated:
                continue

            severity = (
                SignalSeverity.URGENT if policy.severity == "urgent"
                else SignalSeverity.NORMAL
            )

            created_flag = await _upsert_signal(
                signal_type=SignalType.POLICY_VIOLATION,
                severity=severity,
                title=f"Policy violated: {policy.title}",
                description=policy.description,
                data={**violation_data, "policy_id": str(policy.id)},
                dedup_context=f"policy:{str(policy.id)}",
            )
            if created_flag:
                created += 1
                logger.info(f"Signal: policy violation — {policy.title}")

        return created

    # ---------------------------------------------------------------------------
    # Rule 5: Goal underfunding
    # ---------------------------------------------------------------------------

    async def check_goal_progress(self) -> int:
        """
        Flag goals that are significantly underfunded relative to timeline.
        """
        from models.signals import SignalType, SignalSeverity
        from models.goals import Goal, GoalStatus

        goals = await Goal.find(Goal.status == GoalStatus.ACTIVE).to_list()
        created = 0

        for goal in goals:
            if not goal.target_corpus or goal.target_corpus == 0:
                continue

            progress_pct = (goal.current_corpus / goal.target_corpus) * 100

            # Determine expected progress based on time remaining
            if goal.target_date:
                days_total = max((goal.target_date - date(2020, 1, 1)).days, 1)
                days_elapsed = (date.today() - date(2020, 1, 1)).days
                expected_pct = min((days_elapsed / days_total) * 100, 100)
            else:
                expected_pct = 50  # no deadline — expect at least 50% funded

            shortfall = expected_pct - progress_pct
            if shortfall < 20:  # only flag if significantly behind
                continue

            created_flag = await _upsert_signal(
                signal_type=SignalType.GOAL_UNDERFUNDED,
                severity=SignalSeverity.NORMAL,
                title=f"Goal '{goal.name}' is underfunded",
                description=(
                    f"'{goal.name}' is {progress_pct:.0f}% funded "
                    f"(₹{goal.current_corpus:,.0f} of ₹{goal.target_corpus:,.0f}). "
                    f"Expected to be ~{expected_pct:.0f}% funded by now."
                ),
                data={
                    "goal_name": goal.name,
                    "target_corpus": goal.target_corpus,
                    "current_corpus": goal.current_corpus,
                    "progress_pct": round(progress_pct, 1),
                    "expected_pct": round(expected_pct, 1),
                    "funding_gap": goal.funding_gap,
                },
                related_goal_ids=[str(goal.id)],
                dedup_context=f"goal:{str(goal.id)}:underfunded",
            )
            if created_flag:
                created += 1

        return created

    # ---------------------------------------------------------------------------
    # Rule 6: Data freshness warning
    # ---------------------------------------------------------------------------

    async def check_data_freshness(self) -> int:
        """
        Flag accounts with stale data (freshness_score < 0.5).
        """
        from models.signals import SignalType, SignalSeverity
        from models.accounts import Account

        accounts = await Account.find(Account.is_active == True).to_list()
        created = 0

        for account in accounts:
            score = account.freshness_score
            if score >= 0.5:
                continue

            created_flag = await _upsert_signal(
                signal_type=SignalType.DOCUMENT_STALE,
                severity=SignalSeverity.NORMAL,
                title=f"Stale data: {account.name}",
                description=(
                    f"{account.name} has a data freshness score of {score:.0%}. "
                    f"Last synced: {account.last_synced_at.strftime('%d %b %Y') if account.last_synced_at else 'never'}. "
                    f"Recommendations based on this account may be unreliable."
                ),
                data={
                    "account": account.name,
                    "freshness_score": score,
                    "last_synced_at": account.last_synced_at.isoformat() if account.last_synced_at else None,
                },
                related_account_ids=[str(account.id)],
                dedup_context=f"account:{str(account.id)}:stale",
            )
            if created_flag:
                created += 1

        return created

    # ---------------------------------------------------------------------------
    # Run all rules
    # ---------------------------------------------------------------------------

    async def run_all(self) -> dict:
        """
        Run all rules and return a summary of signals created.
        Called by the daily Celery task and the weekly recommendation pipeline.
        """
        from services.analytics_service import AnalyticsService

        logger.info("Running rules engine...")
        analytics = AnalyticsService()
        snapshot = await analytics.get_portfolio_snapshot()

        results = {
            "allocation_drift":   await self.check_allocation_drift(snapshot),
            "concentration":      await self.check_concentration(snapshot),
            "document_staleness": await self.check_document_staleness(),
            "policy_violations":  await self.check_policy_violations(snapshot),
            "goal_progress":      await self.check_goal_progress(),
            "data_freshness":     await self.check_data_freshness(),
        }

        total_created = sum(results.values())
        logger.info(f"Rules engine complete: {total_created} new signals — {results}")
        return {
            "signals_created": total_created,
            "breakdown": results,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }