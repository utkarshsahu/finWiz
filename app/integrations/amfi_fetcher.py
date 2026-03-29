"""
amfi_fetcher.py — fetches and parses the AMFI daily NAV file.

AMFI (Association of Mutual Funds in India) publishes NAVs for all
registered MF schemes daily at ~8pm IST at a fixed public URL.

The file is pipe-delimited, plain text, no auth required. Format:
  Scheme Code|ISIN Div Payout/IDCW|ISIN Div Reinvestment|Scheme Name|Net Asset Value|Date
  120503|INF179K01VK5|INF179K01VL3|HDFC Mid-Cap Opp Fund - Direct Growth|112.345|28-Mar-2025

We fetch this once daily, parse into a dict keyed by scheme_code AND isin,
then use it to update all MF holdings in one pass — no per-scheme API calls.
"""

import httpx
import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

AMFI_NAV_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"


class AmfiNavData:
    """
    Parsed NAV data from the AMFI daily file.

    Provides lookup by scheme_code (str) or ISIN (str).
    """

    def __init__(self):
        self._by_scheme_code: dict[str, dict] = {}
        self._by_isin: dict[str, dict] = {}
        self.nav_date: Optional[date] = None
        self.total_schemes: int = 0

    def get_by_scheme_code(self, scheme_code: str) -> Optional[dict]:
        return self._by_scheme_code.get(scheme_code)

    def get_by_isin(self, isin: str) -> Optional[dict]:
        return self._by_isin.get(isin)

    def get_nav(self, scheme_code: Optional[str] = None, isin: Optional[str] = None) -> Optional[float]:
        """Convenience: get NAV by scheme_code or ISIN, whichever is available."""
        record = None
        if scheme_code:
            record = self.get_by_scheme_code(scheme_code)
        if not record and isin:
            record = self.get_by_isin(isin)
        return record["nav"] if record else None


async def fetch_amfi_navs() -> AmfiNavData:
    """
    Downloads and parses the AMFI NAV file.

    Returns an AmfiNavData object ready for lookups.
    Raises httpx.HTTPError on download failure.

    Typical file size: ~3MB, ~20,000 lines. Parse time: <1 second.
    """
    logger.info(f"Fetching AMFI NAV data from {AMFI_NAV_URL}")

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.get(AMFI_NAV_URL)
        response.raise_for_status()

    nav_data = AmfiNavData()
    lines = response.text.splitlines()
    parsed = 0
    skipped = 0

    for line in lines:
        line = line.strip()

        # Skip headers, blank lines, and section separators
        if not line or line.startswith("Scheme Code") or "Open Ended" in line or "Close Ended" in line or "Interval Fund" in line:
            continue

        parts = line.split(";")
        if len(parts) < 6:
            skipped += 1
            continue

        scheme_code = parts[0].strip()
        isin_payout = parts[1].strip()       # ISIN for dividend payout option
        isin_reinvest = parts[2].strip()     # ISIN for dividend reinvestment option
        scheme_name = parts[3].strip()
        nav_str = parts[4].strip()
        date_str = parts[5].strip()

        # Skip non-numeric NAVs (e.g. "N.A." for schemes not yet launched)
        try:
            nav_value = float(nav_str)
        except ValueError:
            skipped += 1
            continue

        # Parse NAV date — AMFI format is DD-Mon-YYYY e.g. "28-Mar-2025"
        try:
            from datetime import datetime
            nav_date = datetime.strptime(date_str, "%d-%b-%Y").date()
            if not nav_data.nav_date:
                nav_data.nav_date = nav_date
        except ValueError:
            nav_date = date.today()

        record = {
            "scheme_code": scheme_code,
            "scheme_name": scheme_name,
            "nav": nav_value,
            "nav_date": nav_date,
        }

        nav_data._by_scheme_code[scheme_code] = record

        # Index by both ISINs (same NAV, different options)
        if isin_payout and isin_payout != "-":
            nav_data._by_isin[isin_payout] = record
        if isin_reinvest and isin_reinvest != "-":
            nav_data._by_isin[isin_reinvest] = record

        parsed += 1

    nav_data.total_schemes = parsed
    logger.info(
        f"AMFI NAV parse complete: {parsed} schemes, "
        f"NAV date: {nav_data.nav_date}, skipped: {skipped}"
    )
    return nav_data