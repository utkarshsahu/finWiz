"""
app/integrations/research_ingester.py

Orchestrates the full research ingestion pipeline:
  fetch → deduplicate → classify → embed → score relevance → store

Entry points:
  ingest_url(url)          — single article from Telegram drop
  ingest_pdf(path, name)   — PDF forwarded via Telegram
  ingest_rss_feeds()       — batch RSS ingestion (daily scheduled)
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from app.integrations.research_fetcher import (
    FetchedContent, fetch_rss_feeds, fetch_url, extract_pdf_text
)
from app.integrations.research_processor import (
    classify_and_summarize, embed_content, compute_portfolio_relevance
)

logger = logging.getLogger(__name__)


async def _is_duplicate(url: str) -> bool:
    """Check if a URL has already been ingested."""
    from app.models.research import ResearchItem
    if not url:
        return False
    existing = await ResearchItem.find_one(ResearchItem.source_url == url)
    return existing is not None


async def _store_research_item(
    content: FetchedContent,
    processed,
    embedding: Optional[list[float]],
    relevance_score: float,
    relevant_holding_ids: list[str],
    relevant_goal_ids: list[str],
    upload_source: str = "rss",
) -> Optional[str]:
    """Store a processed research item in MongoDB."""
    from app.models.research import ResearchItem, ContentType, Sentiment
    from app.models.instruments import AssetClass

    # Map content_type string to enum
    content_type_map = {
        "article": ContentType.ARTICLE,
        "podcast_transcript": ContentType.PODCAST_TRANSCRIPT,
        "report": ContentType.REPORT,
    }
    content_type = content_type_map.get(content.content_type, ContentType.ARTICLE)

    # Map sentiment
    sentiment_map = {
        "bullish": Sentiment.BULLISH,
        "bearish": Sentiment.BEARISH,
        "neutral": Sentiment.NEUTRAL,
        "mixed": Sentiment.MIXED,
    }
    sentiment = sentiment_map.get(processed.sentiment, Sentiment.NEUTRAL)

    # Map asset classes
    ac_map = {
        "equity": AssetClass.EQUITY,
        "mutual_fund": AssetClass.MUTUAL_FUND,
        "debt": AssetClass.DEBT,
        "gold": AssetClass.GOLD,
        "silver": AssetClass.SILVER,
    }
    asset_classes = [
        ac_map[ac] for ac in processed.relevant_asset_classes
        if ac in ac_map
    ]

    item = ResearchItem(
        title=content.title,
        source_name=content.source_name,
        source_url=content.source_url,
        author=content.author,
        published_date=content.published_date,
        content_type=content_type,
        raw_content=content.content[:5000],
        summary=processed.summary,
        key_claims=processed.key_claims,
        important_numbers=processed.important_numbers,
        themes=processed.themes,
        relevant_asset_classes=asset_classes,
        relevant_sectors=processed.relevant_sectors,
        sentiment=sentiment,
        time_horizon=processed.time_horizon,
        portfolio_relevance_score=relevance_score,
        relevant_holding_ids=relevant_holding_ids,
        relevant_goal_ids=relevant_goal_ids,
        embedding=embedding,
        ingested_at=datetime.now(timezone.utc),
        processed_by_model="gpt-4o-mini",
        upload_source=upload_source,
    )

    await item.insert()
    logger.info(
        f"Stored: '{content.title[:50]}' "
        f"themes={processed.themes} "
        f"relevance={relevance_score}"
    )
    return str(item.id)


# ---------------------------------------------------------------------------
# Relevance pre-filter — runs BEFORE any LLM/embedding call
# Only articles mentioning these keywords pass through to Claude + Voyage
# Add your own holdings/sectors here over time
# ---------------------------------------------------------------------------

RELEVANCE_KEYWORDS = [
    # Asset classes
    "mutual fund", "sip", "nifty", "sensex", "equity", "debt fund",
    "index fund", "etf", "smallcap", "midcap", "largecap",
    # Macro India
    "rbi", "repo rate", "inflation", "gdp", "fiscal", "sebi",
    "budget", "tax", "gst", "rupee", "inr",
    # Sectors relevant to your holdings
    "icici", "hdfc", "sbi", "tata", "bharti", "airtel",
    "banking", "pharma", "it sector", "infrastructure",
    # Commodities
    "gold", "silver", "crude", "commodity",
    # Global macro
    "fed", "federal reserve", "us recession", "rate cut", "rate hike",
    "china", "global market", "dollar",
    # Investment concepts
    "portfolio", "rebalance", "asset allocation", "xirr", "returns",
    "valuation", "pe ratio", "earnings",
]

def _passes_relevance_filter(content: "FetchedContent") -> bool:
    """
    Fast keyword pre-filter — no API calls.
    Returns True if the article is likely relevant to an Indian investor.
    Runs on title + first 500 chars of content only for speed.
    """
    text = (content.title + " " + content.content[:500]).lower()
    return any(kw in text for kw in RELEVANCE_KEYWORDS)


async def _process_and_store(
    content: FetchedContent,
    upload_source: str = "rss",
) -> Optional[str]:
    """
    Full pipeline for one piece of content:
    filter → deduplicate → classify → embed → score → store.
    Returns the new ResearchItem ID or None if failed/duplicate/filtered.
    """
    # Step 0: Keyword pre-filter (free — no API calls)
    # Skip for manual drops (telegram_bot) — user explicitly chose this content
    if upload_source == "rss" and not _passes_relevance_filter(content):
        logger.debug(f"Filtered out (no relevant keywords): {content.title[:60]}")
        return None

    # Deduplicate by URL
    if content.source_url and await _is_duplicate(content.source_url):
        logger.info(f"Duplicate skipped: {content.source_url}")
        return None

    # Step 1: Classify and summarize via Claude Haiku
    processed = await classify_and_summarize(content)
    if not processed:
        logger.warning(f"Classification failed for: {content.title}")
        return None

    # Step 2: Embed via Voyage AI
    embed_text = f"{content.title}\n\n{processed.summary}\n\n{' '.join(processed.key_claims)}"
    embedding = await embed_content(embed_text)

    # Step 3: Score portfolio relevance
    relevance_score, relevant_holding_ids, relevant_goal_ids = (
        await compute_portfolio_relevance(
            processed.themes,
            processed.relevant_asset_classes,
            processed.relevant_sectors,
        )
    )

    # Step 4: Store
    return await _store_research_item(
        content=content,
        processed=processed,
        embedding=embedding,
        relevance_score=relevance_score,
        relevant_holding_ids=relevant_holding_ids,
        relevant_goal_ids=relevant_goal_ids,
        upload_source=upload_source,
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

async def ingest_url(url: str) -> Optional[str]:
    """
    Ingest a single article URL — called when user drops a link via Telegram.
    Returns ResearchItem ID or None.
    """
    logger.info(f"Ingesting URL: {url}")
    content = await fetch_url(url)
    if not content:
        logger.error(f"Could not fetch content from: {url}")
        return None
    return await _process_and_store(content, upload_source="telegram_bot")


async def ingest_pdf(pdf_path: str, filename: str = "") -> Optional[str]:
    """
    Ingest a PDF file — called when user forwards a PDF via Telegram.
    Returns ResearchItem ID or None.
    """
    logger.info(f"Ingesting PDF: {filename or pdf_path}")
    text = extract_pdf_text(pdf_path)
    if not text:
        logger.error(f"Could not extract text from PDF: {pdf_path}")
        return None

    content = FetchedContent(
        title=filename.replace(".pdf", "").replace("_", " ").title() or "Research Report",
        source_name="Manual Upload",
        source_url="",  # no URL for PDFs
        content=text,
        content_type="report",
    )
    return await _process_and_store(content, upload_source="telegram_bot")


async def ingest_rss_feeds(
    max_items_per_feed: int = 10,
) -> dict:
    """
    Batch ingest from all configured RSS feeds.
    Called by daily Celery task.

    Cost control:
      - Fetch up to max_items_per_feed per feed (RSS only, free)
      - Pre-filter by keywords (free)
      - Only call Claude + Voyage on articles passing the filter
      - Typical filter rate: 10-20% pass through on broad feeds
    """
    logger.info("Starting RSS feed ingestion...")
    items = await fetch_rss_feeds(max_items_per_feed=max_items_per_feed)

    total_fetched = len(items)
    filtered_out = 0
    ingested = 0
    skipped_duplicate = 0
    failed = 0

    for item in items:
        # Pre-filter check (free)
        if not _passes_relevance_filter(item):
            filtered_out += 1
            continue

        # Dedup check (free)
        if item.source_url and await _is_duplicate(item.source_url):
            skipped_duplicate += 1
            continue

        # Only now call Claude + Voyage (costs money)
        result = await _process_and_store(item, upload_source="rss")
        if result:
            ingested += 1
        else:
            failed += 1

    result = {
        "total_fetched": total_fetched,
        "filtered_out": filtered_out,
        "skipped_duplicate": skipped_duplicate,
        "ingested": ingested,
        "failed": failed,
        "llm_calls_made": ingested + failed,
    }
    logger.info(f"RSS ingestion complete: {result}")
    return result