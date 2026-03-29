"""
FinancialDocument — tracks uploaded statement PDFs and their processing state.

The document pipeline is:
  upload → queued → parsing → parsed (or failed) → ingested

parse_status drives the UI — the dashboard shows a document portal
with each document's freshness and any gaps in coverage.

The staleness alerting system uses date_range_end to decide when to
prompt for a new upload (e.g. CAS not refreshed in >35 days).
"""

from datetime import date, datetime
from enum import Enum
from typing import Optional
from beanie import Document, Link
from pydantic import Field

from models.accounts import Account, AccountType


class DocType(str, Enum):
    CAS = "cas"                   # Consolidated Account Statement (CDSL/NSDL)
    BANK_STATEMENT = "bank_statement"
    CARD_STATEMENT = "card_statement"
    CONTRACT_NOTE = "contract_note"    # Zerodha contract note
    FD_RECEIPT = "fd_receipt"
    DIVIDEND_STATEMENT = "dividend_statement"
    OTHER = "other"


class ParseStatus(str, Enum):
    QUEUED = "queued"             # Uploaded, waiting for parser worker
    PARSING = "parsing"           # Currently being processed
    PARSED = "parsed"             # Successfully extracted
    FAILED = "failed"             # Parse failed, needs manual review
    INGESTED = "ingested"         # Parsed + written to ledger collections
    DUPLICATE = "duplicate"       # Same date range already ingested


class ParseMethod(str, Enum):
    PDFPLUMBER = "pdfplumber"     # Primary: fast, deterministic, free
    TESSERACT = "tesseract"       # Fallback for scanned PDFs
    CLAUDE_API = "claude_api"     # Last resort for complex tables (costs money)


class FinancialDocument(Document):
    """
    Metadata and parse state for an uploaded financial document.

    The actual extracted data is written to Transaction and Cashflow
    collections — this document is the job record for the parse pipeline.
    """

    # --- File ---
    original_filename: str
    storage_path: str                          # S3/GCS object key or local path
    file_size_bytes: Optional[int] = None

    # --- Classification ---
    doc_type: DocType
    linked_account: Optional[Link[Account]] = None   # Which account this belongs to

    # --- Date coverage ---
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    statement_period_description: Optional[str] = None  # e.g. "April 2024 - March 2025"

    # --- Parse pipeline state ---
    parse_status: ParseStatus = ParseStatus.QUEUED
    parse_method_used: Optional[ParseMethod] = None
    parse_confidence: Optional[float] = None   # 0-1: overall confidence of extraction
    parse_error: Optional[str] = None          # Error message if failed
    low_confidence_fields: list[str] = Field(default_factory=list)  # Fields flagged for review

    # --- Ingestion results ---
    transactions_created: int = 0
    cashflows_created: int = 0
    holdings_updated: int = 0

    # --- Timing ---
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    parsed_at: Optional[datetime] = None
    ingested_at: Optional[datetime] = None

    # --- Upload source ---
    upload_source: str = "web"                 # "web", "telegram_bot", "google_drive"

    class Settings:
        name = "financial_documents"