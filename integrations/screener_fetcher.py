"""
integrations/screener_fetcher.py

Fetches fundamental data from Screener.in (no API key required).
Uses the public company page, scrapes key ratios from the structured HTML.

Data extracted:
  - ROE (Return on Equity, %)
  - ROCE (Return on Capital Employed, %)
  - Debt to Equity ratio
  - P/B ratio (Price to Book)
  - Market cap category (Large/Mid/Small cap) if present

Rate-limit: gentle 1-second sleep between calls — Screener is a free service.
"""

import asyncio
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

SCREENER_BASE = "https://www.screener.in"

SCREENER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.screener.in/",
}


class FundamentalData:
    def __init__(
        self,
        symbol: str,
        roe: Optional[float] = None,          # % e.g. 18.5
        roce: Optional[float] = None,         # % e.g. 22.1
        debt_to_equity: Optional[float] = None,  # ratio e.g. 0.4
        price_to_book: Optional[float] = None,   # ratio e.g. 3.2
        error: Optional[str] = None,
    ):
        self.symbol         = symbol
        self.roe            = roe
        self.roce           = roce
        self.debt_to_equity = debt_to_equity
        self.price_to_book  = price_to_book
        self.error          = error
        self.success        = error is None and any(
            v is not None for v in [roe, roce, debt_to_equity, price_to_book]
        )


def _parse_num(text: str) -> Optional[float]:
    """Extract first float-like value from a string, ignoring % and commas."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", text.strip().replace(",", ""))
    try:
        val = float(cleaned)
        return val if val != 0.0 else None
    except ValueError:
        return None


def _extract_ratio(html: str, label: str) -> Optional[float]:
    """
    Screener renders key ratios as:
      <li class="..."><span class="name">Label</span><span class="number">Value</span></li>
    or in a two-column table. We do a simple regex sweep.
    """
    # Try ratio list pattern
    pattern = rf'{re.escape(label)}[^<]*</span>\s*<span[^>]*>\s*([0-9,.\-]+)'
    m = re.search(pattern, html, re.IGNORECASE)
    if m:
        return _parse_num(m.group(1))
    return None


async def fetch_fundamentals(symbol: str) -> FundamentalData:
    """
    Fetch fundamental ratios for a single NSE symbol from Screener.in.

    Screener URL: https://www.screener.in/company/<SYMBOL>/consolidated/
    Falls back to /standalone/ if consolidated returns no data.
    """
    bare = symbol.replace(".NS", "").replace(".BO", "").upper()

    async with httpx.AsyncClient(
        headers=SCREENER_HEADERS,
        follow_redirects=True,
        timeout=httpx.Timeout(20.0),
    ) as client:
        for suffix in ("consolidated", "standalone"):
            url = f"{SCREENER_BASE}/company/{bare}/{suffix}/"
            try:
                resp = await client.get(url)
                if resp.status_code == 404:
                    continue
                if resp.status_code != 200:
                    logger.warning(f"Screener {url} → HTTP {resp.status_code}")
                    continue

                html = resp.text

                roe  = _extract_ratio(html, "Return on equity")
                roce = _extract_ratio(html, "ROCE")
                d2e  = _extract_ratio(html, "Debt to equity")
                pb   = _extract_ratio(html, "Price to book value")

                if any(v is not None for v in [roe, roce, d2e, pb]):
                    logger.info(
                        f"Screener {bare} ({suffix}): ROE={roe} ROCE={roce} "
                        f"D/E={d2e} P/B={pb}"
                    )
                    return FundamentalData(
                        symbol=bare,
                        roe=roe,
                        roce=roce,
                        debt_to_equity=d2e,
                        price_to_book=pb,
                    )

                await asyncio.sleep(1.0)

            except Exception as e:
                logger.warning(f"Screener fetch error for {bare}: {e}")
                return FundamentalData(symbol=bare, error=str(e))

    return FundamentalData(symbol=bare, error="No data found on Screener")
