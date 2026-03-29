"""
Instrument — master reference data for every tradeable asset.

One document per unique financial instrument. Holdings and transactions
reference instruments by their ObjectId (or ISIN) — never embed the
full instrument info inside a transaction.

Examples:
  - Reliance Industries (NSE equity)
  - HDFC Mid-Cap Opportunities Fund — Direct Growth (mutual fund folio)
  - Nippon India Gold ETF (ETF)
  - Sovereign Gold Bond 2028 (SGB)
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from beanie import Document
from pydantic import Field


class AssetClass(str, Enum):
    EQUITY = "equity"
    MUTUAL_FUND = "mutual_fund"
    ETF = "etf"
    DEBT = "debt"           # bonds, NCDs
    GOLD = "gold"           # physical, ETF, SGB
    SILVER = "silver"
    REAL_ESTATE = "real_estate"
    CASH = "cash"
    FIXED_DEPOSIT = "fixed_deposit"
    PPF = "ppf"
    EPF = "epf"
    NPS = "nps"
    CRYPTO = "crypto"
    OTHER = "other"


class SubClass(str, Enum):
    # Equity sub-classes
    LARGE_CAP = "large_cap"
    MID_CAP = "mid_cap"
    SMALL_CAP = "small_cap"
    FLEXI_CAP = "flexi_cap"
    SECTORAL = "sectoral"
    INTERNATIONAL = "international"
    # Debt sub-classes
    LIQUID = "liquid"
    SHORT_DURATION = "short_duration"
    LONG_DURATION = "long_duration"
    GILT = "gilt"
    CORPORATE_BOND = "corporate_bond"
    # Others
    INDEX = "index"
    HYBRID = "hybrid"
    COMMODITY = "commodity"
    NONE = "none"


class Exchange(str, Enum):
    NSE = "NSE"
    BSE = "BSE"
    AMFI = "AMFI"   # Mutual funds registered under AMFI
    MCX = "MCX"     # Commodity exchange
    OTHER = "OTHER"


class Instrument(Document):
    """
    Master reference for all tradeable assets.

    ISIN is the canonical identifier across CAS, Zerodha, and market data.
    For instruments without an ISIN (e.g. some SGBs or real estate placeholders),
    use a generated slug as the symbol.
    """

    # --- Identifiers ---
    isin: Optional[str] = None          # International Securities Identification Number
    symbol: str                          # NSE/BSE ticker or AMFI scheme code
    name: str                            # Full name e.g. "HDFC Mid-Cap Opportunities Fund"
    short_name: Optional[str] = None     # Display name e.g. "HDFC MidCap"

    # --- Classification ---
    asset_class: AssetClass
    sub_class: SubClass = SubClass.NONE
    exchange: Optional[Exchange] = None

    # --- Sector / Geography (for equity analysis) ---
    sector: Optional[str] = None         # e.g. "Banking", "IT", "FMCG"
    industry: Optional[str] = None       # e.g. "Private Banks", "IT Services"
    geography: str = "India"             # "India", "US", "Global"

    # --- Mutual fund specific ---
    fund_house: Optional[str] = None     # e.g. "HDFC AMC", "Nippon India"
    scheme_code: Optional[str] = None    # AMFI scheme code
    is_direct_plan: Optional[bool] = None
    is_growth_option: Optional[bool] = None

    # --- Metadata ---
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "instruments"             # MongoDB collection name
        # Indexes defined in create_indexes.py

    class Config:
        json_schema_extra = {
            "example": {
                "isin": "INF179K01VK5",
                "symbol": "HDFCMIDCAP",
                "name": "HDFC Mid-Cap Opportunities Fund - Direct Plan Growth",
                "short_name": "HDFC MidCap Direct",
                "asset_class": "mutual_fund",
                "sub_class": "mid_cap",
                "exchange": "AMFI",
                "sector": None,
                "fund_house": "HDFC AMC",
                "is_direct_plan": True,
                "is_growth_option": True,
            }
        }