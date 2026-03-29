"""
Transaction — the immutable financial ledger.

Every buy, sell, SIP instalment, dividend credit, redemption, or
bonus issue is a transaction. These are NEVER deleted or edited.
If a correction is needed, add a reversal transaction.

The analytics engine derives XIRR, cost basis, and realized P&L
entirely from this collection + price snapshots.

Sources:
  - Zerodha contract notes / order history → equity/ETF transactions
  - CAS PDF → mutual fund transactions (SIP, redemption, switch)
  - Bank statement → salary, dividend credit, SIP debit detection
  - Manual entry → FD maturity, PPF contribution, gold purchase
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional
from beanie import Document, Link
from pydantic import Field

from models.instruments import Instrument
from models.accounts import Account


class TransactionType(str, Enum):
    # Investment transactions
    BUY = "buy"
    SELL = "sell"
    SIP = "sip"                     # Systematic Investment Plan instalment
    REDEMPTION = "redemption"       # MF redemption
    SWITCH_IN = "switch_in"         # MF switch into this fund
    SWITCH_OUT = "switch_out"       # MF switch out of this fund
    # Corporate actions
    DIVIDEND = "dividend"           # Dividend credit (for dividend-option MFs / stocks)
    BONUS = "bonus"                 # Bonus shares
    RIGHTS = "rights"               # Rights issue
    SPLIT = "split"                 # Stock split
    MERGER = "merger"
    # Cash transactions (non-investment)
    SALARY_CREDIT = "salary_credit"
    FD_MATURITY = "fd_maturity"
    INTEREST_CREDIT = "interest_credit"
    EXPENSE = "expense"
    # Corrections
    REVERSAL = "reversal"           # To correct a wrong entry


class TransactionSource(str, Enum):
    ZERODHA = "zerodha"
    CAS_PDF = "cas_pdf"
    BANK_STATEMENT = "bank_statement"
    MANUAL = "manual"


class Transaction(Document):
    """
    One financial event in the ledger.

    instrument is nullable because some transactions (salary credit,
    expense) are pure cashflow events not tied to a specific instrument.

    amount = total transaction value in INR (quantity × price + charges)
    This is what actually left or entered your bank account.
    """

    # --- References ---
    account: Link[Account]
    instrument: Optional[Link[Instrument]] = None   # Null for pure cashflows

    # --- Core fields ---
    transaction_type: TransactionType
    transaction_date: date                           # Date of the trade/event (not booking date)
    quantity: Optional[float] = None                 # Units (null for pure cashflows)
    price: Optional[float] = None                    # Price per unit in INR at time of trade
    amount: float                                    # Total INR value (signed: +inflow, -outflow)

    # --- Charges (for accurate cost basis) ---
    brokerage: float = 0.0
    stt: float = 0.0             # Securities Transaction Tax
    stamp_duty: float = 0.0
    other_charges: float = 0.0

    # --- MF specific ---
    nav: Optional[float] = None  # NAV at time of transaction (for MF units)
    folio_number: Optional[str] = None

    # --- Categorization ---
    category: Optional[str] = None          # For cashflows: "rent", "groceries", "emi", etc.
    tags: list[str] = Field(default_factory=list)

    # --- Source tracking ---
    source: TransactionSource
    source_reference_id: Optional[str] = None  # Zerodha order ID, CAS txn ref, etc.
    is_verified: bool = False                    # Manually verified by user

    # --- Deduplication ---
    # Hash of (account_id, date, type, quantity, price) to prevent double-import
    dedup_hash: Optional[str] = None

    notes: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "transactions"