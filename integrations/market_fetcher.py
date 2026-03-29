"""
app/integrations/market_fetcher.py

Price fetcher for Indian market instruments using NSE India's direct API.

Key fix: NSE returns gzip-compressed responses even when the response
Content-Encoding header is missing/wrong, causing httpx to not decompress.
We manually decompress using the zlib module as a fallback.
"""

import asyncio
import gzip
import logging
import zlib
from datetime import date, datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

NSE_BASE = "https://www.nseindia.com"

# Minimal headers — simpler is more reliable with NSE
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

COMMODITY_TICKERS = {
    "GOLD_ETF":   "GOLDBEES.NS",
    "SILVER_ETF": "SILVERBEES.NS",
}

INDEX_TICKERS = {
    "NIFTY_50":     "^NSEI",
    "SENSEX":       "^BSESN",
    "NIFTY_MIDCAP": "^NSEMDCP50",
    "NIFTY_IT":     "^CNXIT",
    "NIFTY_BANK":   "^NSEBANK",
    "NIFTY_PHARMA": "^CNXPHARMA",
}

NSE_INDEX_MAP = {
    "^NSEI":       "NIFTY 50",
    "^BSESN":      None,
    "^NSEMDCP50":  "NIFTY MIDCAP 50",
    "^CNXIT":      "NIFTY IT",
    "^NSEBANK":    "NIFTY BANK",
    "^CNXPHARMA":  "NIFTY PHARMA",
}


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

class PriceFetchResult:
    def __init__(
        self,
        ticker: str,
        price: Optional[float],
        price_date: Optional[date],
        error: Optional[str] = None,
    ):
        self.ticker = ticker
        self.price = price
        self.price_date = price_date
        self.error = error
        self.success = price is not None and price > 0


# ---------------------------------------------------------------------------
# Decompression helper
# ---------------------------------------------------------------------------

def _decode_response(content: bytes) -> str:
    """
    Decode response bytes to string, handling gzip/deflate compression
    even when Content-Encoding header is absent or wrong.

    NSE sometimes sends gzip without declaring it in headers.
    We detect by magic bytes and decompress manually.
    """
    # gzip magic bytes: 1f 8b
    if content[:2] == b'\x1f\x8b':
        try:
            return gzip.decompress(content).decode("utf-8")
        except Exception:
            pass

    # zlib deflate
    if content[:1] == b'\x78':
        try:
            return zlib.decompress(content).decode("utf-8")
        except Exception:
            pass

    # Plain UTF-8
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        # Last resort — ignore bad bytes
        return content.decode("utf-8", errors="ignore")


# ---------------------------------------------------------------------------
# NSE session
# ---------------------------------------------------------------------------

class NseSession:
    """
    Persistent httpx session with NSE cookies.
    Uses raw content + manual decompression to handle NSE's inconsistent
    Content-Encoding headers.
    """

    def __init__(self):
        self._client: Optional[httpx.AsyncClient] = None
        self._cookies_loaded = False

    async def _ensure_session(self):
        if self._client is None:
            # Do NOT set Accept-Encoding — let httpx skip auto-decompression
            # so we can handle it manually via _decode_response()
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                follow_redirects=True,
            )

        if not self._cookies_loaded:
            try:
                logger.info("Loading NSE session cookies...")
                await self._client.get(
                    NSE_BASE,
                    headers={"User-Agent": NSE_HEADERS["User-Agent"]}
                )
                await asyncio.sleep(1.0)
                self._cookies_loaded = True
                logger.info("NSE session ready")
            except Exception as e:
                logger.warning(f"NSE session init failed: {e}")

    async def get_json(self, url: str) -> Optional[dict]:
        """
        Fetch JSON from NSE endpoint.
        Manually decompresses response bytes before JSON parsing.
        """
        await self._ensure_session()
        import json

        try:
            resp = await self._client.get(url, headers=NSE_HEADERS)

            if resp.status_code != 200:
                logger.warning(f"NSE {url} → HTTP {resp.status_code}")
                return None

            # Use raw bytes + manual decode instead of resp.text or resp.json()
            # This handles missing/wrong Content-Encoding from NSE
            text = _decode_response(resp.content)
            return json.loads(text)

        except Exception as e:
            logger.warning(f"NSE request failed for {url}: {e}")
            return None

    async def reset(self):
        if self._client:
            await self._client.aclose()
        self._client = None
        self._cookies_loaded = False


_nse_session: Optional[NseSession] = None


def _get_nse_session() -> NseSession:
    global _nse_session
    if _nse_session is None:
        _nse_session = NseSession()
    return _nse_session


# ---------------------------------------------------------------------------
# NSE equity / ETF quote
# ---------------------------------------------------------------------------

async def _fetch_nse_equity(symbol: str) -> PriceFetchResult:
    bare = symbol.replace(".NS", "").replace(".BO", "").upper()
    url = f"{NSE_BASE}/api/quote-equity?symbol={bare}"

    session = _get_nse_session()
    data = await session.get_json(url)

    if not data:
        await session.reset()
        data = await session.get_json(url)

    if not data:
        return PriceFetchResult(symbol, None, None, "No data from NSE")

    price_info = data.get("priceInfo", {})
    last_price = price_info.get("lastPrice") or price_info.get("close")

    if not last_price:
        return PriceFetchResult(
            symbol, None, None,
            f"No price in response. Keys: {list(price_info.keys())}"
        )

    meta = data.get("metadata", {})
    try:
        trade_date = datetime.strptime(
            meta.get("lastUpdateTime", "").split(" ")[0], "%d-%b-%Y"
        ).date()
    except Exception:
        trade_date = date.today()

    logger.info(f"  NSE {bare}: ₹{last_price} ({trade_date})")
    return PriceFetchResult(symbol, float(last_price), trade_date)


# ---------------------------------------------------------------------------
# NSE indices
# ---------------------------------------------------------------------------

async def _fetch_all_nse_indices() -> dict[str, float]:
    url = f"{NSE_BASE}/api/allIndices"
    session = _get_nse_session()
    data = await session.get_json(url)

    if not data:
        await session.reset()
        data = await session.get_json(url)

    if not data:
        return {}

    result = {}
    for item in data.get("data", []):
        name = item.get("index", "").upper()
        last = item.get("last") or item.get("lastPrice")
        if name and last:
            result[name] = float(last)

    logger.info(f"NSE indices: {len(result)} fetched")
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_single_price(ticker: str) -> PriceFetchResult:
    if ticker.endswith(".NS") or ticker.endswith(".BO"):
        return await _fetch_nse_equity(ticker)
    elif ticker.startswith("^"):
        return await fetch_index_price(ticker)
    else:
        return PriceFetchResult(ticker, None, None, "Futures not supported yet")


async def fetch_index_price(ticker: str) -> PriceFetchResult:
    nse_name = NSE_INDEX_MAP.get(ticker)
    if nse_name is None:
        return PriceFetchResult(ticker, None, None, "Not available on NSE")

    index_data = await _fetch_all_nse_indices()
    price = index_data.get(nse_name.upper())

    if price:
        logger.info(f"  {ticker} ({nse_name}): {price}")
        return PriceFetchResult(ticker, price, date.today())

    return PriceFetchResult(
        ticker, None, None,
        f"'{nse_name}' not found. Sample keys: {list(index_data.keys())[:5]}"
    )


async def fetch_prices(tickers: list[str]) -> dict[str, PriceFetchResult]:
    results: dict[str, PriceFetchResult] = {}
    for ticker in tickers:
        results[ticker] = await fetch_single_price(ticker)
        await asyncio.sleep(0.3)
    success = sum(1 for r in results.values() if r.success)
    logger.info(f"fetch_prices: {success}/{len(tickers)} succeeded")
    return results


async def fetch_commodity_prices() -> dict[str, PriceFetchResult]:
    logger.info("Fetching commodity prices via NSE...")
    results = {}
    for name, ticker in COMMODITY_TICKERS.items():
        results[ticker] = await fetch_single_price(ticker)
        await asyncio.sleep(0.3)
    return results


async def fetch_index_prices() -> dict[str, PriceFetchResult]:
    logger.info("Fetching index prices via NSE allIndices...")
    index_data = await _fetch_all_nse_indices()
    today = date.today()
    results = {}

    for ticker, nse_name in NSE_INDEX_MAP.items():
        if nse_name is None:
            results[ticker] = PriceFetchResult(ticker, None, None, "Not on NSE")
            continue
        price = index_data.get(nse_name.upper())
        if price:
            logger.info(f"  {ticker} ({nse_name}): {price}")
            results[ticker] = PriceFetchResult(ticker, price, today)
        else:
            results[ticker] = PriceFetchResult(
                ticker, None, None,
                f"'{nse_name}' not found"
            )

    return results


def to_nse_symbol(symbol: str) -> str:
    symbol = symbol.upper().strip()
    if symbol.endswith(".NS") or symbol.endswith(".BO"):
        return symbol
    return f"{symbol}.NS"


def to_bse_symbol(symbol: str) -> str:
    symbol = symbol.upper().strip()
    if symbol.endswith(".BO"):
        return symbol
    return f"{symbol}.BO"