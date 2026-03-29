"""
Account — represents any financial account the user holds.

A demat account holds equity/ETF holdings.
A bank account holds cash and is the source of salary, SIPs, and expenses.
A mutual fund folio is also modelled as an account (linked to AMFI/CAS).

Having a clean account model lets you trace every transaction to its source
and compute account-level cashflows (e.g. which bank account SIPs are debited from).
"""

from datetime import datetime
from enum import Enum
from typing import Optional
from beanie import Document
from pydantic import Field


class AccountType(str, Enum):
    DEMAT = "demat"             # Zerodha, Groww, etc.
    BANK_SAVINGS = "bank_savings"
    BANK_CURRENT = "bank_current"
    MUTUAL_FUND_FOLIO = "mutual_fund_folio"  # CAS folio
    CREDIT_CARD = "credit_card"
    WALLET = "wallet"           # Paytm, GPay balance
    PPF = "ppf"
    EPF = "epf"
    NPS = "nps"
    FIXED_DEPOSIT = "fixed_deposit"
    REAL_ESTATE = "real_estate"  # Placeholder for property assets
    OTHER = "other"


class DataSource(str, Enum):
    ZERODHA = "zerodha"
    CAS_PDF = "cas_pdf"
    BANK_STATEMENT = "bank_statement"
    CARD_STATEMENT = "card_statement"
    MANUAL = "manual"


class Account(Document):
    """
    A single financial account or folio.

    Every holding and transaction is linked to an account.
    last_synced_at tracks data freshness — used to compute the
    time-decay weight applied to signals derived from this account's data.
    """

    # --- Identity ---
    name: str                             # e.g. "Zerodha Demat", "SBI Savings ****1234"
    account_type: AccountType
    institution: str                      # e.g. "Zerodha", "SBI", "HDFC AMC"
    account_number_masked: Optional[str] = None  # Last 4 digits only, never full number

    # --- Data sync ---
    data_source: DataSource
    last_synced_at: Optional[datetime] = None
    sync_frequency_days: int = 1         # Expected refresh cadence (1 = daily, 30 = monthly)

    # --- State ---
    is_active: bool = True
    notes: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def freshness_score(self) -> float:
        """
        Time-decay freshness score between 0.0 and 1.0.

        Score = 1.0 if synced within expected cadence.
        Decays linearly to 0.2 at 3x the expected cadence.
        Never drops to 0 — even stale data carries some weight.

        Used by the recommendation engine to discount signals
        derived from outdated account data.
        """
        if not self.last_synced_at:
            return 0.2
        age_days = (datetime.utcnow() - self.last_synced_at).days
        expected = self.sync_frequency_days
        if age_days <= expected:
            return 1.0
        decay = 1.0 - (0.8 * min(age_days - expected, expected * 2) / (expected * 2))
        return round(max(decay, 0.2), 2)

    class Settings:
        name = "accounts"