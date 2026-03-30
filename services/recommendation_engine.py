"""
app/services/recommendation_engine.py

Recommendation engine — reads signals + research + portfolio state
and composes the weekly digest using GPT-4o.

Pipeline:
  1. Pull active signals from rules engine
  2. Pull relevant research items from last 7 days
  3. Get portfolio snapshot + XIRR from analytics engine
  4. Build structured context (no LLM yet)
  5. Call GPT-4o once to compose the digest
  6. Store as Recommendation document
  7. Return structured output

Cost: ~$0.03 per weekly run (GPT-4o, ~5000 input + ~1000 output tokens)
"""

import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

WEEKLY_DIGEST_PROMPT = """You are a personal financial advisor for an Indian retail investor.
Based on the portfolio data, signals, and recent market research below, generate a weekly investment digest.

Return a JSON object with exactly this structure:
{{
  "market_narrative": "2-3 sentence summary of what happened in markets this week relevant to this portfolio",
  "actions": [
    {{
      "action_type": "one of: rebalance_reduce, rebalance_add, deploy_idle_cash, stagger_buy, review_concentration, restore_emergency_fund, review_goal_allocation, upload_document, no_action",
      "title": "short action title",
      "rationale": "why this action, referencing specific portfolio data",
      "suggested_steps": ["step 1", "step 2"],
      "urgency_score": 0.0-1.0,
      "impact_score": 0.0-1.0,
      "confidence_score": 0.0-1.0,
      "priority_rank": 1
    }}
  ],
  "opportunities": [
    {{
      "theme": "the sector or theme from research",
      "rationale": "why this is a good addition to the current portfolio based on zero or low exposure",
      "suggested_instruments": ["list 1-2 generic types or indices, e.g., 'Nifty IT Index ETF' or 'Defense focused MF'"]
    }}
  ],
  "no_action_rationale": "if top recommendation is no_action, explain why doing nothing is correct this week",
  "policy_violations": ["list any portfolio rule violations observed"],
  "belief_portfolio_contradictions": ["any contradictions between market research consumed and current portfolio positioning"],
  "stale_data_warnings": ["any data freshness concerns"]
}}

Rules:
- Maximum 5 actions. 
- STRATEGIC DISCOVERY: If a research item has a high relevance score (>0.7) and identifies a bullish sector where the portfolio has 0% exposure, create an action to 'stagger_buy' or 'rebalance_add' to capture this opportunity.
- Reference specific research summaries to justify new 'Opportunity' entries.
- Always include a no_action option if signals are weak
- Be specific — reference actual fund names, percentages, rupee amounts from the data
- If data is stale, lower confidence scores accordingly
- Recommended inaction is a valid and valuable output

PORTFOLIO SNAPSHOT:
{snapshot}

ACTIVE SIGNALS:
{signals}

RECENT MARKET RESEARCH (last 7 days, portfolio-relevant):
{research}

XIRR DATA:
{xirr}

Return only valid JSON."""


class RecommendationEngine:

    async def _get_active_signals(self) -> list[dict]:
        """Fetch unresolved signals, formatted for LLM context."""
        from models.signals import Signal, SignalSeverity

        signals = await Signal.find(
            {"is_resolved": False}
        ).sort([("generated_at", -1)]).limit(20).to_list()

        return [
            {
                "type": s.signal_type.value,
                "severity": s.severity.value,
                "title": s.title,
                "description": s.description,
                "data": s.data,
            }
            for s in signals
        ]

    async def _get_relevant_research(self, days: int = 7) -> list[dict]:
        """Fetch recent portfolio-relevant research items."""
        from models.research import ResearchItem

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        # We prioritize higher relevance scores which now include Discovery/Gaps
        items = await ResearchItem.find(
            ResearchItem.ingested_at >= cutoff,
            ResearchItem.portfolio_relevance_score >= 0.4, # Raised threshold for digest quality
        ).sort([("portfolio_relevance_score", -1)]).limit(10).to_list()

        return [
            {
                "title": i.title,
                "source": i.source_name,
                "summary": i.summary,
                "themes": i.themes,
                "sentiment": i.sentiment.value,
                "relevance_score": i.portfolio_relevance_score,
                "key_claims": i.key_claims[:3],
            }
            for i in items
        ]

    async def _compose_digest(
        self,
        snapshot: dict,
        signals: list[dict],
        research: list[dict],
        xirr: dict,
    ) -> Optional[dict]:
        """Call GPT-4o to compose the weekly digest."""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY not set")
            return None

        from openai import OpenAI

        # Build compact context — avoid token bloat
        snapshot_summary = {
            "total_value": snapshot.get("total_value"),
            "total_invested": snapshot.get("total_invested"),
            "unrealized_pnl_pct": snapshot.get("unrealized_pnl_pct"),
            "by_asset_class": {
                k: {"pct": v.get("pct"), "value": v.get("value"), "pnl": v.get("pnl")}
                for k, v in snapshot.get("by_asset_class", {}).items()
            },
            "holdings_count": snapshot.get("holdings_count"),
            "data_freshness": snapshot.get("data_freshness"),
        }

        xirr_summary = {
            "overall_xirr_pct": xirr.get("overall_xirr_pct"),
            "total_real_invested": xirr.get("total_real_invested"),
            "mf_current_value": xirr.get("mf_current_value"),
            "note": xirr.get("note"),
        }

        prompt = WEEKLY_DIGEST_PROMPT.format(
            snapshot=json.dumps(snapshot_summary, indent=2),
            signals=json.dumps(signals, indent=2) if signals else "No active signals.",
            research=json.dumps(research, indent=2) if research else "No recent research ingested.",
            xirr=json.dumps(xirr_summary, indent=2),
        )

        try:
            client = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model="gpt-4o",
                max_tokens=1500,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a concise, evidence-based financial advisor. "
                            "You cite specific data points. You recommend inaction when signals are weak. "
                            "You never fabricate data. All advice is for informational purposes only."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
            )

            raw = response.choices[0].message.content.strip()
            return json.loads(raw)

        except Exception as e:
            logger.error(f"GPT-4o digest composition failed: {e}")
            return None

    async def generate_weekly_digest(self) -> Optional[dict]:
        """
        Main entry point — generates and stores the weekly recommendation.

        Steps:
          1. Gather portfolio state (no LLM)
          2. Gather signals (no LLM)
          3. Gather research (no LLM)
          4. One GPT-4o call to compose digest
          5. Store as Recommendation document
          6. Return result
        """
        from services.analytics_service import AnalyticsService
        from models.recommendations import (
            Recommendation, RecommendedAction, ActionType, ActionStatus
        )
        from models.signals import SignalSeverity

        logger.info("Generating weekly digest...")

        # Step 1-3: Gather context (all free)
        analytics = AnalyticsService()
        snapshot = await analytics.get_portfolio_snapshot()
        xirr = await analytics.get_portfolio_xirr()
        signals = await self._get_active_signals()
        research = await self._get_relevant_research()

        logger.info(
            f"Context: {len(signals)} signals, "
            f"{len(research)} research items, "
            f"portfolio ₹{snapshot.get('total_value', 0):,.0f}"
        )

        # Step 4: One LLM call
        composed = await self._compose_digest(snapshot, signals, research, xirr)

        if not composed:
            logger.error("Failed to compose digest")
            return None

        # Step 5: Build and store Recommendation document
        week_start = date.today() - timedelta(days=date.today().weekday())

        # Check if recommendation already exists for this week
        existing = await Recommendation.find_one(
            Recommendation.week_start == week_start
        )

        # Parse actions from GPT response
        actions = []
        for i, action_data in enumerate(composed.get("actions", [])[:5]):
            try:
                action_type_str = action_data.get("action_type", "no_action")
                try:
                    action_type = ActionType(action_type_str.replace("-", "_").lower())
                except ValueError:
                    action_type = ActionType.NO_ACTION

                action = RecommendedAction(
                    action_type=action_type,
                    title=action_data.get("title", ""),
                    rationale=action_data.get("rationale", ""),
                    suggested_steps=action_data.get("suggested_steps", []),
                    urgency_score=float(action_data.get("urgency_score", 0.5)),
                    impact_score=float(action_data.get("impact_score", 0.5)),
                    confidence_score=float(action_data.get("confidence_score", 0.5)),
                    priority_rank=int(action_data.get("priority_rank", i + 1)),
                    status=ActionStatus.PENDING,
                )
                actions.append(action)
            except Exception as e:
                logger.error(f"Error parsing action: {e} — {action_data}")

        # Compute overall freshness score
        urgent_signals = [s for s in signals if s["severity"] == "urgent"]
        overall_freshness = snapshot.get("data_freshness", 1.0)

        portfolio_snapshot_data = {
            "total_value": snapshot.get("total_value"),
            "total_invested": snapshot.get("total_invested"),
            "unrealized_pnl_pct": snapshot.get("unrealized_pnl_pct"),
            "by_asset_class": {
                k: v.get("pct") for k, v in
                snapshot.get("by_asset_class", {}).items()
            },
        }

        if existing:
            # Update existing recommendation for this week
            existing.actions = actions
            existing.market_narrative = composed.get("market_narrative", "")
            existing.no_action_rationale = composed.get("no_action_rationale", "")
            existing.portfolio_snapshot = portfolio_snapshot_data
            existing.overall_data_freshness_score = overall_freshness
            existing.stale_data_warnings = composed.get("stale_data_warnings", [])
            existing.policy_violations = composed.get("policy_violations", [])
            existing.belief_portfolio_contradictions = composed.get("belief_portfolio_contradictions", [])
            existing.generated_at = datetime.now(timezone.utc)
            await existing.save()
            rec = existing
        else:
            rec = Recommendation(
                week_start=week_start,
                actions=actions,
                market_narrative=composed.get("market_narrative", ""),
                no_action_rationale=composed.get("no_action_rationale", ""),
                portfolio_snapshot=portfolio_snapshot_data,
                overall_data_freshness_score=overall_freshness,
                stale_data_warnings=composed.get("stale_data_warnings", []),
                policy_violations=composed.get("policy_violations", []),
                belief_portfolio_contradictions=composed.get("belief_portfolio_contradictions", []),
            )
            await rec.insert()

        logger.info(
            f"Weekly digest generated: {len(actions)} actions, "
            f"{len(urgent_signals)} urgent signals"
        )

        return {
            "recommendation_id": str(rec.id),
            "week_start": week_start.isoformat(),
            "actions_count": len(actions),
            "market_narrative": rec.market_narrative,
            "actions": [
                {
                    "rank": a.priority_rank,
                    "type": a.action_type.value,
                    "title": a.title,
                    "rationale": a.rationale,
                    "suggested_steps": a.suggested_steps,
                    "urgency": a.urgency_score,
                    "confidence": a.confidence_score,
                }
                for a in sorted(actions, key=lambda x: x.priority_rank)
            ],
            "policy_violations": rec.policy_violations,
            "contradictions": rec.belief_portfolio_contradictions,
            "stale_warnings": rec.stale_data_warnings,
            "signals_used": len(signals),
            "research_used": len(research),
        }