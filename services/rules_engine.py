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
    # Rule 7: Technical signals (per equity holding)
    # ---------------------------------------------------------------------------

    async def check_technicals(self) -> int:
        """
        Inspect technical data stored on each equity/ETF holding after the
        daily market sync and generate technical signals.

        Thresholds (configurable here):
          DRAWDOWN     : price ≤ 80% of 52W high  (≥ 20% drawdown)
          NEAR_52W_LOW : price ≤ 110% of 52W low  (within 10% of 52W low)
          MOMENTUM     : price ≥ 97% of 52W high  (within 3% of 52W high)
          HIGH_VOL     : annual_volatility ≥ 40%
          FUNDAMENTAL  : ROE < 10%, or D/E > 1.5, or negative P/E (from Screener)

        Only equity and ETF holdings are checked — MFs have no NSE technicals.
        """
        from models.signals import SignalType, SignalSeverity
        from models.holdings import Holding
        from models.instruments import AssetClass
        from integrations.screener_fetcher import fetch_fundamentals
        import asyncio

        holdings = await Holding.find({"is_active": True}).to_list()
        created = 0

        for h in holdings:
            await h.fetch_link(Holding.instrument)
            inst = h.instrument

            if inst.asset_class not in (AssetClass.EQUITY, AssetClass.ETF):
                continue

            price      = h.current_price
            high52     = h.week52_high
            low52      = h.week52_low
            volatility = h.annual_volatility
            pe         = h.pe_ratio
            symbol     = inst.symbol
            name       = inst.short_name or inst.name or symbol
            inst_id    = str(inst.id)

            # --- Drawdown: ≥ 20% below 52W high ---
            if price and high52 and high52 > 0:
                drawdown_pct = ((high52 - price) / high52) * 100
                if drawdown_pct >= 20:
                    severity = (
                        SignalSeverity.URGENT if drawdown_pct >= 35
                        else SignalSeverity.NORMAL
                    )
                    flag = await _upsert_signal(
                        signal_type=SignalType.TECHNICAL_DRAWDOWN,
                        severity=severity,
                        title=f"{name} down {drawdown_pct:.0f}% from 52W high",
                        description=(
                            f"{name} is trading at ₹{price:,.0f}, which is "
                            f"{drawdown_pct:.1f}% below its 52-week high of ₹{high52:,.0f}. "
                            f"Review whether the thesis is intact or if this is a buying opportunity."
                        ),
                        data={
                            "symbol": symbol,
                            "price": price,
                            "week52_high": high52,
                            "drawdown_pct": round(drawdown_pct, 1),
                            "week52_high_date": h.week52_high_date,
                        },
                        related_instrument_ids=[inst_id],
                        dedup_context=f"{symbol}:drawdown:20pct",
                    )
                    created += flag

            # --- Near 52W low: within 10% of low ---
            if price and low52 and low52 > 0:
                from_low_pct = ((price - low52) / low52) * 100
                if from_low_pct <= 10:
                    flag = await _upsert_signal(
                        signal_type=SignalType.TECHNICAL_NEAR_52W_LOW,
                        severity=SignalSeverity.NORMAL,
                        title=f"{name} near 52W low ({from_low_pct:.0f}% above)",
                        description=(
                            f"{name} is trading at ₹{price:,.0f}, only {from_low_pct:.1f}% "
                            f"above its 52-week low of ₹{low52:,.0f} "
                            f"(on {h.week52_low_date or 'unknown date'}). "
                            f"Consider if the fundamental case still holds."
                        ),
                        data={
                            "symbol": symbol,
                            "price": price,
                            "week52_low": low52,
                            "from_low_pct": round(from_low_pct, 1),
                            "week52_low_date": h.week52_low_date,
                        },
                        related_instrument_ids=[inst_id],
                        dedup_context=f"{symbol}:near_52w_low",
                    )
                    created += flag

            # --- Strong momentum: within 3% of 52W high ---
            if price and high52 and high52 > 0:
                from_high_pct = ((high52 - price) / high52) * 100
                if from_high_pct <= 3:
                    flag = await _upsert_signal(
                        signal_type=SignalType.TECHNICAL_MOMENTUM_STRONG,
                        severity=SignalSeverity.INFO,
                        title=f"{name} near 52W high — strong momentum",
                        description=(
                            f"{name} is trading at ₹{price:,.0f}, within {from_high_pct:.1f}% "
                            f"of its 52-week high of ₹{high52:,.0f}. "
                            f"If this is overweight in your portfolio, consider trimming."
                        ),
                        data={
                            "symbol": symbol,
                            "price": price,
                            "week52_high": high52,
                            "from_high_pct": round(from_high_pct, 1),
                        },
                        related_instrument_ids=[inst_id],
                        dedup_context=f"{symbol}:momentum_strong",
                    )
                    created += flag

            # --- High volatility: annual vol ≥ 40% ---
            if volatility and volatility >= 40:
                flag = await _upsert_signal(
                    signal_type=SignalType.TECHNICAL_HIGH_VOLATILITY,
                    severity=SignalSeverity.INFO,
                    title=f"{name} has high annual volatility ({volatility:.0f}%)",
                    description=(
                        f"{name} has an annualised volatility of {volatility:.1f}%, "
                        f"indicating high price swings. Ensure this aligns with your "
                        f"risk tolerance for this holding."
                    ),
                    data={
                        "symbol": symbol,
                        "annual_volatility": volatility,
                    },
                    related_instrument_ids=[inst_id],
                    dedup_context=f"{symbol}:high_vol",
                )
                created += flag

            # --- Fundamental concern (P/E negative = loss-making) ---
            if pe is not None and pe < 0:
                flag = await _upsert_signal(
                    signal_type=SignalType.FUNDAMENTAL_CONCERN,
                    severity=SignalSeverity.NORMAL,
                    title=f"{name} is loss-making (P/E: {pe:.1f})",
                    description=(
                        f"{name} currently has a negative P/E ratio ({pe:.1f}), "
                        f"indicating the company is not profitable. Review whether "
                        f"this is a temporary phase or a structural concern."
                    ),
                    data={
                        "symbol": symbol,
                        "pe_ratio": pe,
                        "concern": "negative_pe",
                    },
                    related_instrument_ids=[inst_id],
                    dedup_context=f"{symbol}:negative_pe",
                )
                created += flag

            # --- Screener fundamentals: ROE, D/E ---
            # Only run if holding has meaningful value (avoid API calls for tiny positions)
            if (h.current_value or 0) > 10000:
                try:
                    fundamentals = await fetch_fundamentals(symbol)
                    await asyncio.sleep(1.0)  # rate-limit Screener
                    if fundamentals.success:
                        concerns = []
                        if fundamentals.roe is not None and fundamentals.roe < 10:
                            concerns.append(f"ROE {fundamentals.roe:.1f}% (below 10%)")
                        if fundamentals.debt_to_equity is not None and fundamentals.debt_to_equity > 1.5:
                            concerns.append(f"D/E {fundamentals.debt_to_equity:.2f} (above 1.5)")
                        if concerns:
                            flag = await _upsert_signal(
                                signal_type=SignalType.FUNDAMENTAL_CONCERN,
                                severity=SignalSeverity.NORMAL,
                                title=f"{name}: fundamental concern",
                                description=(
                                    f"{name} has the following fundamental concerns: "
                                    f"{', '.join(concerns)}. "
                                    f"Source: Screener.in"
                                ),
                                data={
                                    "symbol": symbol,
                                    "roe": fundamentals.roe,
                                    "debt_to_equity": fundamentals.debt_to_equity,
                                    "price_to_book": fundamentals.price_to_book,
                                    "concerns": concerns,
                                },
                                related_instrument_ids=[inst_id],
                                dedup_context=f"{symbol}:fundamental_concern",
                            )
                            created += flag
                except Exception as e:
                    logger.warning(f"Screener fetch failed for {symbol}: {e}")

        logger.info(f"check_technicals: {created} new signals")
        return created

    # ---------------------------------------------------------------------------
    # Rule 8: Corporate actions
    # ---------------------------------------------------------------------------

    async def check_corporate_actions(self) -> int:
        """
        Check NSE for upcoming/recent corporate actions on held equities.

        Signal priority:
          - rights / buyback  → URGENT  (time-sensitive decision required)
          - bonus / split     → NORMAL  (informational, affects cost basis)
          - dividend          → INFO    (credit expected, low urgency)
        """
        from models.signals import SignalType, SignalSeverity
        from models.holdings import Holding
        from models.instruments import AssetClass
        from integrations.nse_corporate_actions import fetch_corporate_actions
        import asyncio

        TYPE_TO_SIGNAL = {
            "rights":   (SignalType.CORPORATE_ACTION_RIGHTS,   SignalSeverity.URGENT),
            "buyback":  (SignalType.CORPORATE_ACTION_BUYBACK,  SignalSeverity.URGENT),
            "bonus":    (SignalType.CORPORATE_ACTION_BONUS,    SignalSeverity.NORMAL),
            "split":    (SignalType.CORPORATE_ACTION_SPLIT,    SignalSeverity.NORMAL),
            "dividend": (SignalType.CORPORATE_ACTION_DIVIDEND, SignalSeverity.INFO),
        }

        holdings = await Holding.find({"is_active": True}).to_list()
        created  = 0

        for h in holdings:
            await h.fetch_link(Holding.instrument)
            inst = h.instrument

            if inst.asset_class not in (AssetClass.EQUITY, AssetClass.ETF):
                continue

            symbol  = inst.symbol
            name    = inst.short_name or inst.name or symbol
            inst_id = str(inst.id)

            try:
                actions = await fetch_corporate_actions(symbol)
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Corp action fetch failed for {symbol}: {e}")
                continue

            for action in actions:
                if not action.is_upcoming():
                    continue

                signal_type, severity = TYPE_TO_SIGNAL.get(
                    action.action_type,
                    (SignalType.CORPORATE_ACTION_DIVIDEND, SignalSeverity.INFO),
                )

                action_label = action.action_type.title()
                ref_date = action.ex_date or action.record_date
                date_str  = str(ref_date) if ref_date else "upcoming"

                # Rights / buyback: deadline-oriented title
                if action.action_type in ("rights", "buyback"):
                    title = f"{name}: {action_label} — action required by {date_str}"
                    description = (
                        f"{name} has an upcoming {action_label} (ex-date: {date_str}). "
                        f"You must decide before the ex-date. Details: {action.subject}"
                    )
                elif action.action_type == "split":
                    title = f"{name}: Stock split on {date_str}"
                    description = (
                        f"{name} will undergo a stock split (ex-date: {date_str}). "
                        f"Your quantity will change — no action required but update your records. "
                        f"Details: {action.subject}"
                    )
                elif action.action_type == "bonus":
                    title = f"{name}: Bonus issue on {date_str}"
                    description = (
                        f"{name} has announced a bonus issue (ex-date: {date_str}). "
                        f"Your holdings will increase proportionally. "
                        f"Details: {action.subject}"
                    )
                else:  # dividend
                    title = f"{name}: Dividend (ex-date {date_str})"
                    description = (
                        f"{name} has an upcoming dividend (ex-date: {date_str}). "
                        f"Hold before ex-date to receive it. Details: {action.subject}"
                    )

                flag = await _upsert_signal(
                    signal_type=signal_type,
                    severity=severity,
                    title=title,
                    description=description,
                    data={
                        "symbol": symbol,
                        "action_type": action.action_type,
                        "subject": action.subject,
                        "ex_date": str(action.ex_date) if action.ex_date else None,
                        "record_date": str(action.record_date) if action.record_date else None,
                        **action.details,
                    },
                    related_instrument_ids=[inst_id],
                    dedup_context=f"{symbol}:{action.action_type}:{date_str}",
                )
                created += flag

        logger.info(f"check_corporate_actions: {created} new signals")
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
            "allocation_drift":    await self.check_allocation_drift(snapshot),
            "concentration":       await self.check_concentration(snapshot),
            "document_staleness":  await self.check_document_staleness(),
            "policy_violations":   await self.check_policy_violations(snapshot),
            "goal_progress":       await self.check_goal_progress(),
            "data_freshness":      await self.check_data_freshness(),
            "technicals":          await self.check_technicals(),
            "corporate_actions":   await self.check_corporate_actions(),
        }

        total_created = sum(results.values())
        logger.info(f"Rules engine complete: {total_created} new signals — {results}")
        return {
            "signals_created": total_created,
            "breakdown": results,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }