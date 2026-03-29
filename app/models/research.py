"""
ResearchItem — processed news, podcast, or newsletter content.

Every article or podcast you consume goes through the research pipeline:
  ingest → classify → summarize → embed → store here

The embedding field enables Atlas Vector Search — so the recommendation
engine can retrieve "all content about gold momentum this month" without
scanning the whole collection.

Relevance to YOUR portfolio is computed by comparing themes + asset classes
against your current holdings and goal allocations.
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional
from beanie import Document
from pydantic import Field

from app.models.instruments import AssetClass


class ContentType(str, Enum):
    ARTICLE = "article"
    PODCAST_TRANSCRIPT = "podcast_transcript"
    NEWSLETTER = "newsletter"
    YOUTUBE_TRANSCRIPT = "youtube_transcript"
    REPORT = "report"            # Brokerage/research report
    TWEET_THREAD = "tweet_thread"
    OTHER = "other"


class Sentiment(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    MIXED = "mixed"


class ResearchItem(Document):
    """
    A single piece of market research, processed and ready for retrieval.

    themes are normalized tags like "rate_cut", "gold_momentum", "it_sector_weakness",
    "us_recession_risk", "inr_depreciation" — used for portfolio relevance matching.

    embedding is the vector representation of the content — stored as a list[float]
    for Atlas Vector Search. Dimension depends on your embedding model
    (1536 for OpenAI ada-002, 1024 for Cohere, 768 for Claude/Voyage).
    """

    # --- Source ---
    title: str
    source_name: str                            # "Zerodha Varsity", "Mint", "Capital Mind"
    source_url: Optional[str] = None
    author: Optional[str] = None
    published_date: Optional[date] = None
    content_type: ContentType

    # --- Content ---
    raw_content: Optional[str] = None          # Full text (capped at ~10k chars)
    summary: str                                # LLM-generated summary (2-4 sentences)
    key_claims: list[str] = Field(default_factory=list)  # Bullet points of main claims
    important_numbers: list[str] = Field(default_factory=list)  # e.g. "Nifty PE at 22.4"

    # --- Classification ---
    themes: list[str] = Field(default_factory=list)
    # e.g. ["rate_cut", "banking_stress", "gold_momentum", "domestic_flows"]

    relevant_asset_classes: list[AssetClass] = Field(default_factory=list)
    relevant_sectors: list[str] = Field(default_factory=list)
    sentiment: Sentiment = Sentiment.NEUTRAL
    time_horizon: str = "medium_term"          # "short_term", "medium_term", "long_term"

    # --- Portfolio relevance (computed after ingestion) ---
    portfolio_relevance_score: float = 0.0    # 0-1: how relevant to YOUR portfolio
    relevant_holding_ids: list[str] = Field(default_factory=list)
    relevant_goal_ids: list[str] = Field(default_factory=list)

    # --- Vector embedding (for Atlas Vector Search) ---
    embedding: Optional[list[float]] = None    # Set during pipeline processing

    # --- Pipeline metadata ---
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
    processed_by_model: Optional[str] = None   # e.g. "claude-haiku-3"
    is_duplicate: bool = False
    upload_source: str = "rss"                 # "rss", "telegram_bot", "manual", "google_drive"

    class Settings:
        name = "research_items"