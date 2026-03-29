"""
PriceSnapshot — daily price record per instrument.

Used by the analytics engine to compute current portfolio value,
drawdowns, XIRR, and benchmark comparisons.

Kept as a separate time-series collection (not embedded in holdings)
so you can compute historical performance without relying on
the broker's API being available.

One document per (instrument, date) pair.
"""

from datetime import date, datetime
from typing import Optional
from beanie import Document, Link
from pydantic import Field

from app.models.instruments import Instrument


class PriceSnapshot(Document):
    """
    End-of-day price for one instrument on one date.

    For mutual funds: NAV (published daily by AMFI).
    For equities/ETFs: closing price from NSE/BSE.
    For gold: MCX or LBMA spot price.
    """

    instrument: Link[Instrument]
    price_date: date

    # OHLCV — close is the canonical price for portfolio valuation
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: float                     # The price used for portfolio valuation
    volume: Optional[float] = None   # Not available for MF NAV

    # For mutual funds
    nav: Optional[float] = None      # Same as close for MFs, kept explicit for clarity

    source: str = "yahoo_finance"    # "yahoo_finance", "amfi", "mcx", "manual"
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "price_snapshots"