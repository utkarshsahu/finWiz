"""
app/routes/research_router.py

Research pipeline endpoints.

Endpoints:
  POST /research/ingest/url        → Ingest a single article URL
  POST /research/ingest/rss        → Trigger RSS batch ingestion
  POST /research/ingest/pdf        → Ingest an uploaded PDF
  GET  /research/items             → List recent research items
  GET  /research/themes            → Weekly theme summary
  GET  /research/relevant          → Items relevant to current portfolio
"""

import os
import tempfile
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

router = APIRouter(prefix="/research", tags=["research"])


@router.post("/ingest/url")
async def ingest_url(url: str = Form(...)):
    """Ingest a single article URL."""
    from app.integrations.research_ingester import ingest_url as _ingest
    item_id = await _ingest(url)
    if not item_id:
        raise HTTPException(status_code=422, detail="Could not fetch or process URL")
    return {"status": "ingested", "item_id": item_id}


@router.post("/ingest/rss")
async def ingest_rss(max_items: int = Query(default=5, le=50)):
    """Trigger RSS batch ingestion from all configured feeds."""
    from app.integrations.research_ingester import ingest_rss_feeds
    result = await ingest_rss_feeds(max_items_per_feed=max_items)
    return {"status": "completed", "result": result}


@router.post("/ingest/pdf")
async def ingest_pdf(file: UploadFile = File(...)):
    """Ingest a research PDF — earnings report, RBI circular, etc."""
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files accepted")

    contents = await file.read()
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    try:
        from app.integrations.research_ingester import ingest_pdf as _ingest
        item_id = await _ingest(tmp_path, filename=file.filename)
        if not item_id:
            raise HTTPException(status_code=422, detail="Could not process PDF")
        return {"status": "ingested", "item_id": item_id}
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


@router.get("/items")
async def list_research_items(
    days: int = Query(default=7, le=90),
    limit: int = Query(default=20, le=100),
    theme: str = None,
    min_relevance: float = Query(default=0.0, ge=0.0, le=1.0),
):
    """List recent research items, optionally filtered by theme or relevance."""
    from app.models.research import ResearchItem
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    items = await ResearchItem.find(
        ResearchItem.ingested_at >= cutoff,
        ResearchItem.portfolio_relevance_score >= min_relevance,
    ).sort([("ingested_at", -1)]).limit(limit).to_list()

    if theme:
        items = [i for i in items if theme in i.themes]

    return [
        {
            "id": str(i.id),
            "title": i.title,
            "source": i.source_name,
            "url": i.source_url,
            "summary": i.summary,
            "themes": i.themes,
            "sentiment": i.sentiment.value,
            "relevance_score": i.portfolio_relevance_score,
            "key_claims": i.key_claims,
            "published_date": i.published_date.isoformat() if i.published_date else None,
            "ingested_at": i.ingested_at.isoformat(),
        }
        for i in items
    ]


@router.get("/themes")
async def get_weekly_themes(days: int = Query(default=7, le=30)):
    """
    Summarise the dominant themes from research ingested in the last N days.
    Returns themes ranked by frequency + average relevance.
    """
    from app.models.research import ResearchItem
    from datetime import datetime, timedelta, timezone
    from collections import Counter

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    items = await ResearchItem.find(
        ResearchItem.ingested_at >= cutoff
    ).to_list()

    if not items:
        return {"themes": [], "total_items": 0, "period_days": days}

    theme_counts = Counter()
    theme_relevance: dict[str, list[float]] = {}

    for item in items:
        for theme in item.themes:
            theme_counts[theme] += 1
            if theme not in theme_relevance:
                theme_relevance[theme] = []
            theme_relevance[theme].append(item.portfolio_relevance_score)

    themes_ranked = []
    for theme, count in theme_counts.most_common(15):
        avg_relevance = sum(theme_relevance[theme]) / len(theme_relevance[theme])
        themes_ranked.append({
            "theme": theme,
            "count": count,
            "avg_portfolio_relevance": round(avg_relevance, 2),
        })

    return {
        "themes": themes_ranked,
        "total_items": len(items),
        "period_days": days,
    }


@router.get("/relevant")
async def get_relevant_items(
    days: int = Query(default=7, le=30),
    min_relevance: float = Query(default=0.3, ge=0.0, le=1.0),
):
    """Items most relevant to the current portfolio, sorted by relevance score."""
    from app.models.research import ResearchItem
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    items = await ResearchItem.find(
        ResearchItem.ingested_at >= cutoff,
        ResearchItem.portfolio_relevance_score >= min_relevance,
    ).sort([("portfolio_relevance_score", -1)]).limit(10).to_list()

    return [
        {
            "id": str(i.id),
            "title": i.title,
            "source": i.source_name,
            "summary": i.summary,
            "themes": i.themes,
            "sentiment": i.sentiment.value,
            "relevance_score": i.portfolio_relevance_score,
            "relevant_sectors": i.relevant_sectors,
        }
        for i in items
    ]