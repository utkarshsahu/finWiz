"""
app/services/analytics_service.py

Analytics engine — computes portfolio metrics from holdings and transactions.

All pure Python, zero LLM calls. This is the foundation that the rules
engine, recommendation engine, and frontend all read from.

Key outputs:
  - Net worth snapshot
  - Asset allocation breakdown
  - XIRR (extended internal rate of return) per scheme and overall
  - Concentration risk (single stock, single sector)
  - Portfolio drift vs target allocation
  - Goal progress

XIRR implementation:
  Uses scipy.optimize.brentq to solve for the rate that makes NPV=0
  given a series of dated cashflows. Falls back to simple CAGR if
  scipy is unavailable or if there are insufficient transactions.
"""

import logging
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# XIRR helpers
# ---------------------------------------------------------------------------

def _xirr(cashflows: list[tuple[date, float]]) -> Optional[float]:
    """
    Compute XIRR given a list of (date, amount) tuples.

    Cashflows convention:
      - Negative = money going OUT (purchases, SIP debits)
      - Positive = money coming IN (redemptions, current value as terminal cashflow)

    Returns annualised return as a decimal (e.g. 0.15 = 15%) or None if
    computation fails.
    """
    if len(cashflows) < 2:
        return None

    try:
        from scipy.optimize import brentq
        import numpy as np

        dates = [cf[0] for cf in cashflows]
        amounts = [cf[1] for cf in cashflows]
        t0 = dates[0]

        def npv(rate):
            return sum(
                amt / ((1 + rate) ** ((d - t0).days / 365.0))
                for d, amt in zip(dates, amounts)
            )

        # Search for root in a reasonable range
        try:
            result = brentq(npv, -0.999, 100.0, maxiter=1000)
            return round(result, 4)
        except ValueError:
            return None

    except ImportError:
        # scipy not available — fall back to simple return
        total_invested = sum(abs(amt) for _, amt in cashflows if amt < 0)
        total_returned = sum(amt for _, amt in cashflows if amt > 0)
        if total_invested == 0:
            return None
        simple_return = (total_returned - total_invested) / total_invested
        return round(simple_return, 4)


# ---------------------------------------------------------------------------
# Main analytics service
# ---------------------------------------------------------------------------

class AnalyticsService:

    # ---------------------------------------------------------------------------
    # Net worth + allocation
    # ---------------------------------------------------------------------------

    async def get_portfolio_snapshot(self) -> dict:
        """
        Compute current portfolio snapshot from all active holdings.

        Returns:
          total_value: total current market value in INR
          total_invested: total cost basis across all holdings
          unrealized_pnl: total_value - total_invested
          unrealized_pnl_pct: as percentage
          by_asset_class: breakdown by AssetClass enum value
          by_account: breakdown by account name
          holdings_count: number of active positions
          data_freshness: min freshness score across accounts
        """
        from app.models.holdings import Holding
        from app.models.accounts import Account

        holdings = await Holding.find(Holding.is_active == True).to_list()

        if not holdings:
            return {
                "total_value": 0,
                "total_invested": 0,
                "unrealized_pnl": 0,
                "unrealized_pnl_pct": 0,
                "by_asset_class": {},
                "by_account": {},
                "holdings_count": 0,
                "data_freshness": 0,
                "computed_at": datetime.now(timezone.utc).isoformat(),
            }

        total_value = 0.0
        total_invested = 0.0
        by_asset_class: dict[str, dict] = defaultdict(
            lambda: {"value": 0.0, "invested": 0.0, "count": 0}
        )
        by_account: dict[str, dict] = defaultdict(
            lambda: {"value": 0.0, "invested": 0.0, "count": 0}
        )
        freshness_scores = []

        for holding in holdings:
            await holding.fetch_link(Holding.instrument)
            await holding.fetch_link(Holding.account)

            instrument = holding.instrument
            account = holding.account

            current_val = holding.current_value or 0.0
            invested_val = holding.invested_value or 0.0

            total_value += current_val
            total_invested += invested_val

            asset_class = instrument.asset_class.value
            by_asset_class[asset_class]["value"] += current_val
            by_asset_class[asset_class]["invested"] += invested_val
            by_asset_class[asset_class]["count"] += 1

            acc_name = account.name
            by_account[acc_name]["value"] += current_val
            by_account[acc_name]["invested"] += invested_val
            by_account[acc_name]["count"] += 1

            freshness_scores.append(account.freshness_score)

        unrealized_pnl = total_value - total_invested
        unrealized_pnl_pct = (
            round((unrealized_pnl / total_invested) * 100, 2)
            if total_invested > 0 else 0
        )

        # Add percentage to each asset class
        for ac, data in by_asset_class.items():
            data["pct"] = round(
                (data["value"] / total_value * 100) if total_value > 0 else 0, 2
            )
            data["pnl"] = round(data["value"] - data["invested"], 2)

        return {
            "total_value": round(total_value, 2),
            "total_invested": round(total_invested, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "unrealized_pnl_pct": unrealized_pnl_pct,
            "by_asset_class": dict(by_asset_class),
            "by_account": dict(by_account),
            "holdings_count": len(holdings),
            "data_freshness": round(min(freshness_scores), 2) if freshness_scores else 0,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    # ---------------------------------------------------------------------------
    # XIRR
    # ---------------------------------------------------------------------------

    async def get_portfolio_xirr(self) -> dict:
        """
        Compute XIRR for the MF portfolio using CAMS transaction history.

        Scope: Only instruments that have transaction records in the DB.
        Zerodha equity holdings are excluded since we only have 60 days
        of order history — insufficient for a meaningful XIRR.

        Overall XIRR cashflows:
          - BUY/SIP → negative (real cash out)
          - REDEMPTION/SELL → positive (real cash back)
          - SWITCH_IN/SWITCH_OUT → excluded (internal, no real cash)
          - Terminal inflow today → current value of all instruments
            that have transaction records (MF only)

        Per-instrument XIRR:
          - SWITCH_IN → negative (cost to acquire this fund's units)
          - SWITCH_OUT → positive (proceeds leaving this fund)
          - Terminal inflow → current value of this instrument
        """
        from app.models.transactions import Transaction, TransactionType
        from app.models.holdings import Holding

        OUTFLOW_TYPES   = {TransactionType.BUY, TransactionType.SIP}
        INFLOW_TYPES    = {TransactionType.SELL, TransactionType.REDEMPTION}
        SWITCH_IN_TYPES = {TransactionType.SWITCH_IN}
        SWITCH_OUT_TYPES= {TransactionType.SWITCH_OUT}

        transactions = await Transaction.find_all().to_list()
        today = date.today()

        # Track which instrument IDs have transaction records
        instruments_with_txns: set[str] = set()
        instrument_cashflows: dict[str, list[tuple[date, float]]] = defaultdict(list)
        overall_cashflows: list[tuple[date, float]] = []
        total_real_invested = 0.0

        for txn in transactions:
            if txn.instrument is None:
                continue

            instrument_id = str(txn.instrument.ref.id)
            instruments_with_txns.add(instrument_id)
            amount = abs(float(txn.amount or 0))

            if txn.transaction_type in OUTFLOW_TYPES:
                overall_cashflows.append((txn.transaction_date, -amount))
                instrument_cashflows[instrument_id].append((txn.transaction_date, -amount))
                total_real_invested += amount

            elif txn.transaction_type in INFLOW_TYPES:
                overall_cashflows.append((txn.transaction_date, +amount))
                instrument_cashflows[instrument_id].append((txn.transaction_date, +amount))

            elif txn.transaction_type in SWITCH_IN_TYPES:
                # Per-instrument only — cost to enter this fund
                instrument_cashflows[instrument_id].append((txn.transaction_date, -amount))

            elif txn.transaction_type in SWITCH_OUT_TYPES:
                # Per-instrument only — proceeds from leaving this fund
                instrument_cashflows[instrument_id].append((txn.transaction_date, +amount))

        # Get current values — only for instruments with transaction history
        holdings = await Holding.find(Holding.is_active == True).to_list()
        instrument_current_values: dict[str, float] = {}

        for holding in holdings:
            instrument_id = str(holding.instrument.ref.id)
            if instrument_id not in instruments_with_txns:
                continue  # skip equity holdings with no transaction history
            val = holding.current_value or 0.0
            instrument_current_values[instrument_id] = (
                instrument_current_values.get(instrument_id, 0) + val
            )

        # Terminal inflow for overall XIRR — only MF instruments
        mf_current_value = sum(instrument_current_values.values())
        if mf_current_value > 0:
            overall_cashflows.append((today, mf_current_value))

        overall_cashflows.sort(key=lambda x: x[0])
        overall_xirr = _xirr(overall_cashflows)

        # Instruments with at least one real BUY/SIP (not just switches)
        instruments_with_real_purchases: set[str] = {
            str(t.instrument.ref.id)
            for t in transactions
            if t.transaction_type in OUTFLOW_TYPES
            and t.instrument is not None
        }

        # Per-instrument XIRR — only for instruments with real purchase history.
        # Switch-only instruments (e.g. SBI Infra which was funded via switch
        # from a fully-redeemed fund) lack their original cost basis and will
        # always produce nonsensical negative XIRRs.
        per_instrument = {}
        for instrument_id, cashflows in instrument_cashflows.items():
            if instrument_id not in instruments_with_real_purchases:
                continue  # no BUY/SIP — unreliable XIRR, skip

            current_val = instrument_current_values.get(instrument_id, 0)
            if current_val <= 0:
                continue  # fully redeemed/switched out

            cfs = cashflows + [(today, current_val)]
            cfs.sort(key=lambda x: x[0])
            xirr_val = _xirr(cfs)
            if xirr_val is not None:
                per_instrument[instrument_id] = round(xirr_val * 100, 2)

        return {
            "overall_xirr_pct": round(overall_xirr * 100, 2) if overall_xirr else None,
            "note": "XIRR covers MF portfolio only (instruments with CAMS transaction history). Equity holdings excluded due to incomplete Zerodha transaction history.",
            "total_real_invested": round(total_real_invested, 2),
            "mf_current_value": round(mf_current_value, 2),
            "per_instrument": per_instrument,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    # ---------------------------------------------------------------------------
    # Concentration risk
    # ---------------------------------------------------------------------------

    async def get_concentration_risk(self) -> dict:
        """
        Identify concentration risk in the portfolio.

        Checks:
          - Single holding > 10% of total portfolio
          - Single sector > 25% of equity portion
          - Single AMC > 40% of MF portfolio
        """
        from app.models.holdings import Holding
        from app.models.instruments import AssetClass

        holdings = await Holding.find(Holding.is_active == True).to_list()
        if not holdings:
            return {"risks": [], "total_value": 0}

        total_value = sum(h.current_value or 0 for h in holdings)
        if total_value == 0:
            return {"risks": [], "total_value": 0}

        risks = []
        sector_values: dict[str, float] = defaultdict(float)
        amc_values: dict[str, float] = defaultdict(float)
        equity_total = 0.0

        for holding in holdings:
            await holding.fetch_link(Holding.instrument)
            instrument = holding.instrument
            val = holding.current_value or 0.0
            pct = (val / total_value) * 100

            # Single holding concentration
            if pct > 10:
                risks.append({
                    "type": "single_holding",
                    "severity": "urgent" if pct > 15 else "normal",
                    "instrument": instrument.name,
                    "pct": round(pct, 2),
                    "value": round(val, 2),
                    "message": f"{instrument.short_name or instrument.name} is {pct:.1f}% of portfolio (threshold: 10%)",
                })

            # Sector tracking (equity only)
            if instrument.asset_class == AssetClass.EQUITY:
                equity_total += val
                if instrument.sector:
                    sector_values[instrument.sector] += val

            # AMC tracking (MF only)
            if instrument.asset_class == AssetClass.MUTUAL_FUND:
                if instrument.fund_house:
                    amc_values[instrument.fund_house] += val

        # Sector concentration
        if equity_total > 0:
            for sector, val in sector_values.items():
                sector_pct = (val / equity_total) * 100
                if sector_pct > 25:
                    risks.append({
                        "type": "sector_concentration",
                        "severity": "normal",
                        "sector": sector,
                        "pct": round(sector_pct, 2),
                        "value": round(val, 2),
                        "message": f"{sector} sector is {sector_pct:.1f}% of equity (threshold: 25%)",
                    })

        # AMC concentration
        mf_total = sum(amc_values.values())
        if mf_total > 0:
            for amc, val in amc_values.items():
                amc_pct = (val / mf_total) * 100
                if amc_pct > 40:
                    risks.append({
                        "type": "amc_concentration",
                        "severity": "normal",
                        "amc": amc,
                        "pct": round(amc_pct, 2),
                        "message": f"{amc} is {amc_pct:.1f}% of MF portfolio (threshold: 40%)",
                    })

        return {
            "risks": risks,
            "total_value": round(total_value, 2),
            "risk_count": len(risks),
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    # ---------------------------------------------------------------------------
    # Portfolio drift
    # ---------------------------------------------------------------------------

    async def get_portfolio_drift(
        self,
        target_allocation: Optional[dict[str, float]] = None,
    ) -> dict:
        """
        Compute drift between current and target asset allocation.

        target_allocation: dict of {asset_class: target_pct}
          e.g. {"equity": 60, "mutual_fund": 20, "debt": 10, "gold": 10}

        If not provided, uses a default 60/20/10/10 split as a placeholder.
        In production this will come from the user's Policy Store.
        """
        snapshot = await self.get_portfolio_snapshot()
        by_ac = snapshot.get("by_asset_class", {})
        total = snapshot.get("total_value", 0)

        if not target_allocation:
            target_allocation = {
                "equity": 50,
                "mutual_fund": 30,
                "debt": 10,
                "gold": 10,
            }

        drift = {}
        for asset_class, target_pct in target_allocation.items():
            current_pct = by_ac.get(asset_class, {}).get("pct", 0)
            diff = current_pct - target_pct
            drift[asset_class] = {
                "target_pct": target_pct,
                "current_pct": round(current_pct, 2),
                "drift_pct": round(diff, 2),
                "status": (
                    "overweight" if diff > 5
                    else "underweight" if diff < -5
                    else "on_target"
                ),
            }

        return {
            "drift": drift,
            "target_allocation": target_allocation,
            "total_value": total,
            "rebalance_needed": any(
                abs(d["drift_pct"]) > 5 for d in drift.values()
            ),
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    # ---------------------------------------------------------------------------
    # Goal progress
    # ---------------------------------------------------------------------------

    async def get_goal_progress(self) -> list[dict]:
        """
        Compute current corpus for each active goal from GoalAllocations.

        Each GoalAllocation links a holding to a goal with a proportion.
        Current corpus = sum(holding.current_value * proportion) for all
        allocations linked to this goal.
        """
        from app.models.goals import Goal, GoalAllocation, GoalStatus
        from app.models.holdings import Holding

        goals = await Goal.find(Goal.status == GoalStatus.ACTIVE).to_list()
        results = []

        for goal in goals:
            allocations = await GoalAllocation.find(
                GoalAllocation.goal_id == goal.id
            ).to_list()

            current_corpus = 0.0
            linked_holdings = []

            for alloc in allocations:
                holding = await Holding.get(alloc.holding_id)
                if holding and holding.is_active:
                    contribution = (holding.current_value or 0) * alloc.proportion
                    current_corpus += contribution
                    await holding.fetch_link(Holding.instrument)
                    linked_holdings.append({
                        "instrument": holding.instrument.name,
                        "proportion": alloc.proportion,
                        "contribution": round(contribution, 2),
                    })

            funding_gap = goal.target_corpus - current_corpus
            progress_pct = (
                round((current_corpus / goal.target_corpus) * 100, 1)
                if goal.target_corpus > 0 else 0
            )

            # Days to target
            days_remaining = None
            if goal.target_date:
                days_remaining = (goal.target_date - date.today()).days

            # Update goal document with computed values
            goal.current_corpus = round(current_corpus, 2)
            goal.funding_gap = round(funding_gap, 2)
            goal.is_on_track = (
                progress_pct >= 80 if days_remaining and days_remaining < 365
                else progress_pct >= 50
            )
            goal.last_reviewed_at = datetime.now(timezone.utc)
            await goal.save()

            results.append({
                "goal_id": str(goal.id),
                "name": goal.name,
                "target_corpus": goal.target_corpus,
                "current_corpus": round(current_corpus, 2),
                "funding_gap": round(funding_gap, 2),
                "progress_pct": progress_pct,
                "target_date": goal.target_date.isoformat() if goal.target_date else None,
                "days_remaining": days_remaining,
                "is_on_track": goal.is_on_track,
                "risk_level": goal.risk_level.value,
                "linked_holdings": linked_holdings,
            })

        return results

    # ---------------------------------------------------------------------------
    # Full analytics report — calls all of the above
    # ---------------------------------------------------------------------------

    async def get_full_report(self) -> dict:
        """
        Run all analytics and return a combined report.
        Called by the weekly recommendation pipeline and the dashboard.
        """
        logger.info("Running full analytics report...")

        snapshot = await self.get_portfolio_snapshot()
        xirr = await self.get_portfolio_xirr()
        concentration = await self.get_concentration_risk()
        drift = await self.get_portfolio_drift()
        goals = await self.get_goal_progress()

        return {
            "snapshot": snapshot,
            "xirr": xirr,
            "concentration": concentration,
            "drift": drift,
            "goals": goals,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }