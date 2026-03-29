"""
app/integrations/research_fetcher.py

Fetches content from RSS feeds and URLs for the research pipeline.

Sources:
  - RSS feeds: parsed via feedparser
  - URLs: scraped via httpx + basic text extraction
  - PDFs: text extracted via pdfminer (already installed)
"""

import logging
import re
from datetime import date, datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx
import feedparser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Curated RSS feeds — finance focused, India + global macro
# ---------------------------------------------------------------------------

RSS_FEEDS = [
    # Single curated feed — Mint Markets
    # Additional content comes via Telegram bot (PDF/URL drops)
    {
        "name": "Mint Markets",
        "url": "https://www.livemint.com/rss/markets",
        "source": "Mint",
    },
]


class FetchedContent:
    """Raw content fetched from a source, before LLM processing."""

    def __init__(
        self,
        title: str,
        source_name: str,
        source_url: str,
        content: str,
        published_date: Optional[date] = None,
        author: Optional[str] = None,
        content_type: str = "article",
    ):
        self.title = title
        self.source_name = source_name
        self.source_url = source_url
        self.content = content[:12000]  # cap at 12k chars for LLM processing
        self.published_date = published_date or date.today()
        self.author = author
        self.content_type = content_type


# ---------------------------------------------------------------------------
# RSS fetcher
# ---------------------------------------------------------------------------

async def fetch_rss_feeds(
    feeds: list[dict] = None,
    max_items_per_feed: int = 5,
) -> list[FetchedContent]:
    """
    Fetch latest items from all configured RSS feeds.
    Returns list of FetchedContent ready for LLM processing.
    """
    feeds = feeds or RSS_FEEDS
    results = []

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for feed_config in feeds:
            try:
                resp = await client.get(feed_config["url"])
                if resp.status_code != 200:
                    logger.warning(f"RSS fetch failed for {feed_config['name']}: {resp.status_code}")
                    continue

                parsed = feedparser.parse(resp.text)
                entries = parsed.entries[:max_items_per_feed]

                for entry in entries:
                    title = entry.get("title", "").strip()
                    url = entry.get("link", "")
                    summary = entry.get("summary", "") or entry.get("description", "")

                    # Clean HTML tags from summary
                    clean_summary = re.sub(r"<[^>]+>", " ", summary).strip()
                    clean_summary = re.sub(r"\s+", " ", clean_summary)

                    if not title or not url:
                        continue

                    # Parse published date
                    pub_date = None
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        import time
                        pub_date = date.fromtimestamp(time.mktime(entry.published_parsed))

                    results.append(FetchedContent(
                        title=title,
                        source_name=feed_config["source"],
                        source_url=url,
                        content=clean_summary or title,
                        published_date=pub_date,
                        author=entry.get("author"),
                        content_type="article",
                    ))

                logger.info(f"RSS: {feed_config['name']} → {len(entries)} items")

            except Exception as e:
                logger.error(f"RSS fetch error for {feed_config['name']}: {e}")

    return results


# ---------------------------------------------------------------------------
# URL scraper
# ---------------------------------------------------------------------------

async def fetch_url(url: str) -> Optional[FetchedContent]:
    """
    Fetch and extract text from a URL.
    Used when user forwards an article link via Telegram.
    Basic extraction — strips HTML, gets meaningful text.
    """
    try:
        async with httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; FinanceBot/1.0)"},
        ) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return None

            html = resp.text

            # Extract title
            title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
            title = title_match.group(1).strip() if title_match else urlparse(url).netloc

            # Extract meta description
            desc_match = re.search(
                r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)',
                html, re.IGNORECASE
            )
            description = desc_match.group(1).strip() if desc_match else ""

            # Extract body text — strip all HTML tags
            # Remove script, style, nav, header, footer blocks first
            for tag in ["script", "style", "nav", "header", "footer", "aside"]:
                html = re.sub(
                    rf"<{tag}[^>]*>.*?</{tag}>", "", html,
                    flags=re.IGNORECASE | re.DOTALL
                )

            text = re.sub(r"<[^>]+>", " ", html)
            text = re.sub(r"\s+", " ", text).strip()

            # Combine description + body, truncate
            content = f"{description}\n\n{text}"[:12000]

            # Try to detect source name from domain
            domain = urlparse(url).netloc.replace("www.", "")
            source_map = {
                "livemint.com": "Mint",
                "economictimes.indiatimes.com": "Economic Times",
                "moneycontrol.com": "Moneycontrol",
                "zerodha.com": "Zerodha",
                "capitalmind.in": "Capital Mind",
                "freefincal.com": "Freefincal",
                "bloomberg.com": "Bloomberg",
                "reuters.com": "Reuters",
            }
            source_name = next(
                (v for k, v in source_map.items() if k in domain),
                domain,
            )

            return FetchedContent(
                title=title,
                source_name=source_name,
                source_url=url,
                content=content,
                content_type="article",
            )

    except Exception as e:
        logger.error(f"URL fetch error for {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# PDF text extractor
# ---------------------------------------------------------------------------

def extract_pdf_text(pdf_path: str) -> Optional[str]:
    """
    Extract text from a PDF file using pdfminer.
    Used for research reports, earnings releases, RBI circulars forwarded via Telegram.
    """
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(pdf_path)
        if text:
            # Clean whitespace
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r"[ \t]+", " ", text)
            return text[:12000]
        return None
    except Exception as e:
        logger.error(f"PDF text extraction failed for {pdf_path}: {e}")
        return None