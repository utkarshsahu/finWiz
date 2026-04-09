"""
Holding — current position in a single instrument within an account.

Holdings are a SNAPSHOT, refreshed on each sync from Zerodha or CAS.
They are NOT the source of truth for history — transactions are.

Think of holdings as a materialized view: always derivable from the
full transaction history, but stored here for fast portfolio queries.

One holding document = one instrument in one account.
"""

from datetime import datetime
from typing import Optional
from beanie import Document, Link
from pydantic import Field, computed_field

from models.instruments import Instrument, AssetClass
from models.accounts import Account


class Holding(Document):
    """
    Current position for one instrument in one account.

    quantity and avg_cost come from the broker/CAS directly.
    current_price is updated daily from the market data sync.
    All P&L fields are derived from these three values.
    """

    # --- References ---
    account: Link[Account]
    instrument: Link[Instrument]

    # --- Position ---
    quantity: float                          # Units held (can be fractional for MFs)
    avg_cost: float                          # Average acquisition price per unit (in INR)
    current_price: Optional[float] = None   # Last known market price per unit
    current_price_date: Optional[datetime] = None

    # --- Computed (stored for fast aggregation, recomputed on sync) ---
    invested_value: float = 0.0             # quantity × avg_cost
    current_value: Optional[float] = None  # quantity × current_price
    unrealized_pnl: Optional[float] = None # current_value - invested_value
    unrealized_pnl_pct: Optional[float] = None

    # --- Technical data (updated on equity sync) ---
    week52_high: Optional[float] = None
    week52_low: Optional[float] = None
    week52_high_date: Optional[str] = None
    week52_low_date: Optional[str] = None
    pe_ratio: Optional[float] = None
    annual_volatility: Optional[float] = None  # % annualised, from NSE
    vwap: Optional[float] = None
    day_change_pct: Optional[float] = None

    # --- Sync metadata ---
    last_synced_at: datetime = Field(default_factory=datetime.utcnow)
    source_raw: Optional[dict] = None       # Raw payload from Zerodha/CAS for debugging

    # --- Soft state ---
    is_active: bool = True                   # False if position fully closed
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def recompute(self):
        """Call after updating quantity, avg_cost, or current_price."""
        self.invested_value = self.quantity * self.avg_cost
        if self.current_price:
            self.current_value = self.quantity * self.current_price
            self.unrealized_pnl = self.current_value - self.invested_value
            if self.invested_value > 0:
                self.unrealized_pnl_pct = round(
                    (self.unrealized_pnl / self.invested_value) * 100, 2
                )

    class Settings:
        name = "holdings"