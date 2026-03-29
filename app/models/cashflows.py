"""
Cashflow — salary credits, dividend inflows, SIP debits, expenses.

While Transaction covers investment events, Cashflow covers the
money moving in and out of your bank accounts — the "personal finance"
side of the platform.

These are extracted from bank and credit card statement PDFs.
Key use cases:
  - Monthly surplus calculation (salary - expenses - SIPs = deployable cash)
  - Emergency fund runway (liquid assets / avg monthly expense)
  - Salary credit detection → trigger "idle cash" signal
  - Dividend tracking across all holdings
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional
from beanie import Document, Link
from pydantic import Field

from app.models.accounts import Account


class CashflowType(str, Enum):
    # Inflows
    SALARY = "salary"
    DIVIDEND = "dividend"
    INTEREST = "interest"
    RENTAL_INCOME = "rental_income"
    FREELANCE = "freelance"
    FD_MATURITY = "fd_maturity"
    REIMBURSEMENT = "reimbursement"
    # Outflows
    SIP_DEBIT = "sip_debit"
    EMI = "emi"
    RENT = "rent"
    INSURANCE_PREMIUM = "insurance_premium"
    CREDIT_CARD_PAYMENT = "credit_card_payment"
    UTILITY = "utility"
    GROCERIES = "groceries"
    DINING = "dining"
    TRANSPORT = "transport"
    ENTERTAINMENT = "entertainment"
    MEDICAL = "medical"
    EDUCATION = "education"
    TRAVEL = "travel"
    SHOPPING = "shopping"
    OTHER_EXPENSE = "other_expense"
    # Transfers (net zero for net worth)
    INTERNAL_TRANSFER = "internal_transfer"


class Cashflow(Document):
    """
    A money movement event from bank or credit card statements.

    amount is always positive; direction is determined by cashflow_type.
    Use is_inflow to determine sign for net calculations.
    """

    account: Link[Account]
    cashflow_type: CashflowType
    cashflow_date: date
    amount: float                               # Always positive INR value
    is_inflow: bool                             # True = money coming in

    description: str                            # Raw text from bank statement
    category: Optional[str] = None             # Normalized category for aggregation
    tags: list[str] = Field(default_factory=list)

    # For SIP debits — link to the investment it funded
    linked_instrument_name: Optional[str] = None

    # Parsing metadata
    source_document_id: Optional[str] = None   # ID of the FinancialDocument it came from
    is_auto_categorized: bool = False
    confidence: float = 1.0                    # 0-1: how confident the categorization is

    dedup_hash: Optional[str] = None           # Prevent double import
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "cashflows"