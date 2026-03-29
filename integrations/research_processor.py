"""
app/integrations/research_processor.py

Processes fetched content through Claude Haiku for classification,
summarization, and theme extraction. Then embeds via Voyage AI.

Cost estimate per article:
  - Claude Haiku: ~$0.001 (input ~800 tokens + output ~300 tokens)
  - Voyage AI embedding: ~$0.00001 (1024 dimensions)
  Total: ~$0.001 per article — very cheap

Theme taxonomy (used for portfolio relevance matching):
  Macro: rate_cut, rate_hike, inflation, recession, usd_inr
  India: sebi_policy, rbi_policy, budget, gst, domestic_flows
  Equity: earnings, nifty_valuation, midcap_rally, smallcap_risk
  Debt: bond_yields, credit_spread, liquid_fund
  Commodities: gold_momentum, silver_momentum, crude_oil, commodity_cycle
  Sectors: it_sector, banking_sector, pharma_sector, fmcg_sector, infra_sector
  Global: us_recession, china_slowdown, fed_policy, global_flows
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from openai import OpenAI

from integrations.research_fetcher import FetchedContent

logger = logging.getLogger(__name__)

OPENAI_MODEL = "gpt-4o-mini"  # cheapest capable model — ~same tier as Claude Haiku
OPENAI_EMBEDDING_MODEL = "text-embedding-3-small"  # 1536 dimensions, $0.02/1M tokens

AVAILABLE_THEMES = [
    # Macro
    "rate_cut", "rate_hike", "inflation", "recession", "usd_inr",
    # India policy
    "rbi_policy", "sebi_policy", "budget", "gst", "domestic_flows",
    # Equity
    "nifty_valuation", "earnings_season", "midcap_momentum",
    "smallcap_risk", "ipo_activity",
    # Debt
    "bond_yields", "credit_risk", "liquid_fund_rates",
    # Commodities
    "gold_momentum", "silver_momentum", "crude_oil", "commodity_cycle",
    # Sectors
    "it_sector", "banking_sector", "pharma_sector",
    "fmcg_sector", "infra_sector", "realty_sector",
    # Global
    "us_recession", "fed_policy", "china_slowdown", "global_flows",
    # Portfolio actions
    "rebalancing_signal", "sip_strategy", "tax_planning",
]

CLASSIFICATION_PROMPT = """You are a financial research classifier for an Indian retail investor.

Analyze this article and return a JSON object with exactly these fields:

{{
  "summary": "2-3 sentence summary of the key insight for an Indian retail investor",
  "key_claims": ["claim 1", "claim 2", "claim 3"],
  "important_numbers": ["any specific numbers, percentages, or figures mentioned"],
  "themes": ["pick 1-5 from the available themes list that best match"],
  "relevant_asset_classes": ["pick from: equity, mutual_fund, debt, gold, silver, real_estate, crypto — only include what the article actually discusses"],
  "relevant_sectors": ["list any Indian market sectors mentioned, e.g. IT, Banking, Pharma, FMCG, Auto, Energy, Metals, Realty, Telecom, Infrastructure, Consumer, Healthcare — use empty list if none"],
  "sentiment": "bullish" or "bearish" or "neutral" or "mixed",
  "time_horizon": "short_term" or "medium_term" or "long_term",
  "action_relevance": "high" or "medium" or "low"
}}

Available themes: {themes}

Article title: {title}
Source: {source}

Article content:
{content}

Return only valid JSON, no markdown, no explanation."""


class ProcessedResearch:
    """Result of LLM processing — ready to store as ResearchItem."""

    def __init__(
        self,
        summary: str,
        key_claims: list[str],
        important_numbers: list[str],
        themes: list[str],
        relevant_asset_classes: list[str],
        relevant_sectors: list[str],
        sentiment: str,
        time_horizon: str,
        action_relevance: str,
        embedding: Optional[list[float]] = None,
    ):
        self.summary = summary
        self.key_claims = key_claims
        self.important_numbers = important_numbers
        self.themes = themes
        self.relevant_asset_classes = relevant_asset_classes
        self.relevant_sectors = relevant_sectors
        self.sentiment = sentiment
        self.time_horizon = time_horizon
        self.action_relevance = action_relevance
        self.embedding = embedding


async def classify_and_summarize(
    content: FetchedContent,
) -> Optional[ProcessedResearch]:
    """
    Send content to GPT-4o-mini for classification and summarization.
    Returns ProcessedResearch or None if processing fails.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not set")
        return None

    prompt = CLASSIFICATION_PROMPT.format(
        themes=", ".join(AVAILABLE_THEMES),
        title=content.title,
        source=content.source_name,
        content=content.content[:8000],
    )

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            max_tokens=600,
            response_format={"type": "json_object"},  # forces valid JSON output
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.choices[0].message.content.strip()
        data = json.loads(raw)

        return ProcessedResearch(
            summary=data.get("summary", ""),
            key_claims=data.get("key_claims", []),
            important_numbers=data.get("important_numbers", []),
            themes=[t for t in data.get("themes", []) if t in AVAILABLE_THEMES],
            relevant_asset_classes=data.get("relevant_asset_classes", []),
            relevant_sectors=data.get("relevant_sectors", []),
            sentiment=data.get("sentiment", "neutral"),
            time_horizon=data.get("time_horizon", "medium_term"),
            action_relevance=data.get("action_relevance", "low"),
        )

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error from OpenAI for '{content.title}': {e}")
        return None
    except Exception as e:
        logger.error(f"OpenAI processing error for '{content.title}': {e}")
        return None


async def embed_content(text: str) -> Optional[list[float]]:
    """
    Generate embedding via OpenAI text-embedding-3-small.
    Returns 1536-dimensional vector or None on failure.
    Cost: $0.02 per million tokens — ~$0.000002 per article.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.warning("OPENAI_API_KEY not set — skipping embedding")
        return None

    try:
        client = OpenAI(api_key=api_key)
        response = client.embeddings.create(
            model=OPENAI_EMBEDDING_MODEL,
            input=text[:8000],
        )
        return response.data[0].embedding

    except Exception as e:
        logger.error(f"OpenAI embedding error: {e}")
        return None


async def compute_portfolio_relevance(
    themes: list[str],
    relevant_asset_classes: list[str],
    relevant_sectors: list[str],
) -> tuple[float, list[str], list[str]]:
    """
    Score how relevant this research item is to the user's actual portfolio.

    Scoring:
      +0.3 if any theme matches a holding's asset class
      +0.2 per relevant sector that matches a held stock's sector
      +0.1 per matched asset class

    Returns (relevance_score, matching_holding_ids, matching_goal_ids)
    """
    from models.holdings import Holding
    from models.instruments import AssetClass

    holdings = await Holding.find(Holding.is_active == True).to_list()

    score = 0.0
    relevant_holding_ids = []

    # Asset class match
    held_asset_classes = set()
    held_sectors = set()

    for h in holdings:
        await h.fetch_link(Holding.instrument)
        instrument = h.instrument
        held_asset_classes.add(instrument.asset_class.value)
        if instrument.sector:
            held_sectors.add(instrument.sector.lower())

    for ac in relevant_asset_classes:
        if ac in held_asset_classes:
            score += 0.1

    # Sector match
    for sector in relevant_sectors:
        if sector.lower() in held_sectors:
            score += 0.2
            # Find specific holdings in this sector
            for h in holdings:
                if h.instrument.sector and h.instrument.sector.lower() == sector.lower():
                    relevant_holding_ids.append(str(h.instrument.id))

    # Theme-based scoring
    HIGH_IMPACT_THEMES = {
        "rate_cut": ["mutual_fund", "debt"],
        "rate_hike": ["debt", "mutual_fund"],
        "gold_momentum": ["gold"],
        "nifty_valuation": ["equity", "mutual_fund"],
        "it_sector": ["equity"],
        "banking_sector": ["equity"],
    }

    for theme in themes:
        if theme in HIGH_IMPACT_THEMES:
            impacted = HIGH_IMPACT_THEMES[theme]
            if any(ac in held_asset_classes for ac in impacted):
                score += 0.3

    return round(min(score, 1.0), 2), list(set(relevant_holding_ids)), []