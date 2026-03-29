"""
app/integrations/cas_parser.py

CAS PDF ingestion for NSDL/CDSL demat statements.

This handles the demat CAS format (NSDL/CDSL) which gives a holdings
snapshot across all your demat accounts — Zerodha, Groww, ICICI, SBICAP etc.

Structure from casparser:
  parsed['accounts'] → list of demat accounts
    account['equities'] → equity/NCD holdings
    account['mutual_funds'] → MF units held in demat form

What we do:
  1. For each account → find/create Account document
  2. For each equity/MF → find/create Instrument document
  3. For each holding → upsert Holding document
  4. Update FinancialDocument parse status

Note: This statement gives SNAPSHOTS not transaction history.
For full MF transaction history, request a CAMS/KFintech detailed
statement from camsonline.com separately.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import casparser

from app.models.accounts import Account, AccountType, DataSource
from app.models.documents import FinancialDocument, ParseStatus, ParseMethod
from app.models.holdings import Holding
from app.models.instruments import (
    AssetClass, Exchange, Instrument, SubClass
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Account type mapping
# ---------------------------------------------------------------------------

def _map_account_type(acc_type: str) -> AccountType:
    t = acc_type.lower()
    if "demat" in t:
        return AccountType.DEMAT
    if "mutual fund" in t or "folio" in t:
        return AccountType.MUTUAL_FUND_FOLIO
    return AccountType.DEMAT


def _map_data_source(acc_type: str) -> DataSource:
    return DataSource.CAS_PDF


def _map_institution(acc_name: str) -> str:
    """Normalize institution name to a short form."""
    name = acc_name.upper()
    if "ZERODHA" in name:
        return "Zerodha"
    if "GROWW" in name:
        return "Groww"
    if "ICICI" in name:
        return "ICICI"
    if "SBICAP" in name or "SBI" in name:
        return "SBI"
    if "HDFC" in name:
        return "HDFC"
    if "KOTAK" in name:
        return "Kotak"
    return acc_name.title()


# ---------------------------------------------------------------------------
# Instrument classification helpers
# ---------------------------------------------------------------------------

def _classify_equity(name: str, isin: str) -> tuple[AssetClass, SubClass]:
    """
    Classify an equity holding by name/ISIN.
    NCDs and bonds have 'NCD', 'BOND', or start with debt-like names.
    """
    name_upper = name.upper()
    if any(x in name_upper for x in ["NCD", "BOND", "DEBENTURE", "TBILL", "GSEC"]):
        return AssetClass.DEBT, SubClass.CORPORATE_BOND
    if "ETF" in name_upper or "BEES" in name_upper:
        if "GOLD" in name_upper:
            return AssetClass.GOLD, SubClass.INDEX
        if "SILVER" in name_upper:
            return AssetClass.SILVER, SubClass.INDEX
        return AssetClass.ETF, SubClass.INDEX
    return AssetClass.EQUITY, SubClass.NONE


def _classify_mf(name: str, isin: str) -> tuple[AssetClass, SubClass]:
    name_upper = name.upper()
    if "LIQUID" in name_upper or "OVERNIGHT" in name_upper or "MONEY MARKET" in name_upper:
        return AssetClass.MUTUAL_FUND, SubClass.LIQUID
    if "GILT" in name_upper:
        return AssetClass.MUTUAL_FUND, SubClass.GILT
    if "DEBT" in name_upper or "BOND" in name_upper or "INCOME" in name_upper:
        return AssetClass.MUTUAL_FUND, SubClass.SHORT_DURATION
    if "INDEX" in name_upper or "NIFTY" in name_upper or "SENSEX" in name_upper:
        return AssetClass.MUTUAL_FUND, SubClass.INDEX
    if "MIDCAP" in name_upper or "MID CAP" in name_upper:
        return AssetClass.MUTUAL_FUND, SubClass.MID_CAP
    if "SMALLCAP" in name_upper or "SMALL CAP" in name_upper:
        return AssetClass.MUTUAL_FUND, SubClass.MID_CAP
    if "LARGE" in name_upper:
        return AssetClass.MUTUAL_FUND, SubClass.LARGE_CAP
    if "FLEXI" in name_upper or "MULTI" in name_upper or "DIVERSIFIED" in name_upper:
        return AssetClass.MUTUAL_FUND, SubClass.FLEXI_CAP
    return AssetClass.MUTUAL_FUND, SubClass.NONE


# ---------------------------------------------------------------------------
# Core upsert helpers
# ---------------------------------------------------------------------------

async def _upsert_account(
    cas_account: dict,
    dp_id: str,
    client_id: str,
) -> Account:
    """
    Find or create an Account for a demat account from CAS.

    Dedup strategy:
      1. Match by account_number_masked (dp_id-client_id) — catches re-uploads
      2. If not found, check if any existing account has the same masked number
         set from a prior Zerodha API sync (which now stores the DP ID)
      This prevents creating a duplicate when the same demat account exists
      both from Zerodha API sync and NSDL CAS upload.
    """
    institution = _map_institution(cas_account["name"])
    acc_type = _map_account_type(cas_account.get("type", "demat"))
    account_number = f"{dp_id}-{client_id}"

    # Match by DP ID + client ID
    existing = await Account.find_one(
        Account.account_number_masked == account_number
    )
    if existing:
        existing.last_synced_at = datetime.now(timezone.utc)
        existing.updated_at = datetime.now(timezone.utc)
        await existing.save()
        return existing

    account = Account(
        name=f"{institution} Demat ({account_number})",
        account_type=acc_type,
        institution=institution,
        account_number_masked=account_number,
        data_source=DataSource.CAS_PDF,
        last_synced_at=datetime.now(timezone.utc),
        sync_frequency_days=30,
    )
    await account.insert()
    logger.info(f"Created account: {account.name}")
    return account


async def _upsert_instrument_equity(holding: dict) -> Instrument:
    """Find or create an Instrument for an equity/NCD holding."""
    isin = holding.get("isin", "").strip()
    name = holding.get("name", "").strip()

    existing = None
    if isin:
        existing = await Instrument.find_one(Instrument.isin == isin)
    if not existing:
        existing = await Instrument.find_one(Instrument.name == name)

    asset_class, sub_class = _classify_equity(name, isin)

    if existing:
        # Update ISIN if it was missing
        if isin and not existing.isin:
            existing.isin = isin
            existing.updated_at = datetime.now(timezone.utc)
            await existing.save()
        return existing

    # Derive a clean symbol from name (fallback — ISIN is the real identifier)
    symbol = isin if isin else name[:20].replace(" ", "_").upper()

    instrument = Instrument(
        isin=isin or None,
        symbol=symbol,
        name=name,
        asset_class=asset_class,
        sub_class=sub_class,
        exchange=Exchange.NSE,  # default — equity in demat can be NSE or BSE
    )
    await instrument.insert()
    logger.info(f"Created instrument: {name} ({asset_class})")
    return instrument


async def _upsert_instrument_mf(holding: dict) -> Instrument:
    """Find or create an Instrument for a demat MF holding."""
    isin = holding.get("isin", "").strip()
    name = holding.get("name", "").strip().replace("\n", " ")

    existing = None
    if isin:
        existing = await Instrument.find_one(Instrument.isin == isin)
    if not existing:
        existing = await Instrument.find_one(Instrument.name == name)

    asset_class, sub_class = _classify_mf(name, isin)

    if existing:
        return existing

    instrument = Instrument(
        isin=isin or None,
        symbol=isin or name[:20].replace(" ", "_").upper(),
        name=name,
        asset_class=asset_class,
        sub_class=sub_class,
        exchange=Exchange.AMFI,
    )
    await instrument.insert()
    logger.info(f"Created MF instrument: {name}")
    return instrument


async def _upsert_holding(
    account: Account,
    instrument: Instrument,
    quantity: float,
    price: Optional[float],
    value: Optional[float],
) -> Holding:
    """Upsert a Holding — one per (account, instrument)."""
    existing = await Holding.find_one(
        Holding.account.id == account.id,       # type: ignore
        Holding.instrument.id == instrument.id, # type: ignore
    )

    avg_cost = (value / quantity) if quantity and value else (price or 0)

    if existing:
        existing.quantity = quantity
        existing.current_price = price
        existing.invested_value = quantity * (existing.avg_cost or avg_cost)
        existing.current_value = value
        if value and existing.invested_value:
            existing.unrealized_pnl = value - existing.invested_value
            existing.unrealized_pnl_pct = round(
                (existing.unrealized_pnl / existing.invested_value) * 100, 2
            )
        existing.last_synced_at = datetime.now(timezone.utc)
        existing.updated_at = datetime.now(timezone.utc)
        existing.is_active = quantity > 0
        await existing.save()
        return existing

    holding = Holding(
        account=account,
        instrument=instrument,
        quantity=quantity,
        avg_cost=avg_cost,
        current_price=price,
        current_price_date=datetime.now(timezone.utc),
        invested_value=quantity * avg_cost,
        current_value=value,
        last_synced_at=datetime.now(timezone.utc),
        is_active=quantity > 0,
    )
    holding.recompute()
    await holding.insert()
    return holding


# ---------------------------------------------------------------------------
# Main ingestion entry point
# ---------------------------------------------------------------------------

async def ingest_cas_pdf(
    pdf_path: str,
    password: str,
    document_id: Optional[str] = None,
) -> dict:
    """
    Parse a CAS PDF and ingest all holdings into MongoDB.

    Args:
        pdf_path:    Path to the CAS PDF file
        password:    PDF password (usually PAN in uppercase)
        document_id: Optional FinancialDocument._id to update parse status

    Returns:
        Summary dict with counts of what was created/updated
    """
    doc: Optional[FinancialDocument] = None

    # Update document status to parsing
    if document_id:
        doc = await FinancialDocument.get(document_id)
        if doc:
            doc.parse_status = ParseStatus.PARSING
            doc.parse_method_used = ParseMethod.CLAUDE_API  # closest enum
            await doc.save()

    try:
        # ---------------------------------------------------------------------------
        # Step 1: Parse PDF via casparser
        # ---------------------------------------------------------------------------
        logger.info(f"Parsing CAS PDF: {pdf_path}")
        import json as _json
        raw = casparser.read_cas_pdf(pdf_path, password, output="json")
        parsed = _json.loads(raw)

        file_type = parsed.get("file_type", "UNKNOWN")
        statement_period = parsed.get("statement_period", {})
        accounts_data = parsed.get("accounts", [])

        logger.info(
            f"CAS type: {file_type}, "
            f"period: {statement_period}, "
            f"accounts: {len(accounts_data)}"
        )

        # ---------------------------------------------------------------------------
        # Step 2: Ingest each account's holdings
        # ---------------------------------------------------------------------------
        total_holdings_upserted = 0
        total_instruments_created = 0
        total_accounts_processed = 0
        errors = []

        for cas_account in accounts_data:
            dp_id = cas_account.get("dp_id", "")
            client_id = cas_account.get("client_id", "")
            acc_name = cas_account.get("name", "Unknown")

            equities = cas_account.get("equities", [])
            mutual_funds = cas_account.get("mutual_funds", [])

            if not equities and not mutual_funds:
                logger.info(f"Skipping empty account: {acc_name}")
                continue

            try:
                account = await _upsert_account(cas_account, dp_id, client_id)
                total_accounts_processed += 1
            except Exception as e:
                logger.error(f"Failed to upsert account {acc_name}: {e}")
                errors.append(f"Account {acc_name}: {e}")
                continue

            # Process equities
            for eq in equities:
                try:
                    price = float(eq.get("price", 0) or 0) or None
                    value = float(eq.get("value", 0) or 0) or None
                    raw_shares = float(eq.get("num_shares", 0) or 0)

                    # For NCDs/bonds, NSDL stores face value denomination
                    # in num_shares (e.g. 1000 for a ₹1000 face value bond),
                    # not the actual unit count.
                    # Derive actual quantity from value / price when possible.
                    name_upper = (eq.get("name") or "").upper()
                    is_debt = any(x in name_upper for x in ["NCD", "BOND", "DEBENTURE", "TBILL", "GSEC"])

                    if is_debt and price and value and price > 0:
                        # Actual bond count = total market value / price per bond
                        quantity = round(value / price, 4)
                    else:
                        quantity = raw_shares

                    if quantity <= 0:
                        continue

                    instrument = await _upsert_instrument_equity(eq)
                    await _upsert_holding(account, instrument, quantity, price, value)
                    total_holdings_upserted += 1

                except Exception as e:
                    name = eq.get("name", "unknown")
                    logger.error(f"Error processing equity {name}: {e}")
                    errors.append(f"Equity {name}: {e}")

            # Process demat mutual funds
            for mf in mutual_funds:
                try:
                    quantity = float(mf.get("balance", 0) or 0)
                    nav = float(mf.get("nav", 0) or 0) or None
                    value = float(mf.get("value", 0) or 0) or None

                    if quantity <= 0:
                        continue

                    instrument = await _upsert_instrument_mf(mf)
                    await _upsert_holding(account, instrument, quantity, nav, value)
                    total_holdings_upserted += 1

                except Exception as e:
                    name = mf.get("name", "unknown")
                    logger.error(f"Error processing MF {name}: {e}")
                    errors.append(f"MF {name}: {e}")

        # ---------------------------------------------------------------------------
        # Step 3: Update document status
        # ---------------------------------------------------------------------------
        result = {
            "file_type": file_type,
            "statement_period": statement_period,
            "accounts_processed": total_accounts_processed,
            "holdings_upserted": total_holdings_upserted,
            "errors": errors,
        }

        if doc:
            doc.parse_status = ParseStatus.INGESTED
            doc.holdings_updated = total_holdings_upserted
            doc.parsed_at = datetime.now(timezone.utc)
            doc.ingested_at = datetime.now(timezone.utc)
            await doc.save()

        logger.info(f"CAS ingestion complete: {result}")
        return result

    except Exception as e:
        logger.error(f"CAS parse failed: {e}")
        if doc:
            doc.parse_status = ParseStatus.FAILED
            doc.parse_error = str(e)
            await doc.save()
        raise


# ---------------------------------------------------------------------------
# CAMS / KFintech folio-based ingestion
# Handles Detailed statements with full transaction history
# ---------------------------------------------------------------------------

async def _upsert_mf_account(amc: str, folio: str) -> Account:
    """Find or create a MF folio Account."""
    account_number = f"{amc}-{folio}"
    existing = await Account.find_one(
        Account.account_number_masked == account_number
    )
    if existing:
        existing.last_synced_at = datetime.now(timezone.utc)
        await existing.save()
        return existing

    account = Account(
        name=f"{amc} — Folio {folio}",
        account_type=AccountType.MUTUAL_FUND_FOLIO,
        institution=amc,
        account_number_masked=account_number,
        data_source=DataSource.CAS_PDF,
        last_synced_at=datetime.now(timezone.utc),
        sync_frequency_days=30,
    )
    await account.insert()
    logger.info(f"Created MF folio account: {account.name}")
    return account


async def _upsert_mf_instrument(scheme: dict) -> Instrument:
    """Find or create an Instrument for a CAMS/KFintech MF scheme."""
    isin = (scheme.get("isin") or "").strip()
    amfi_code = str(scheme.get("amfi") or "").strip()  # casparser returns int, cast to str
    name = (scheme.get("scheme") or "").strip()

    # Try ISIN first, then AMFI code, then name
    existing = None
    if isin:
        existing = await Instrument.find_one(Instrument.isin == isin)
    if not existing and amfi_code:
        existing = await Instrument.find_one(Instrument.scheme_code == amfi_code)
    if not existing and name:
        existing = await Instrument.find_one(Instrument.name == name)

    asset_class, sub_class = _classify_mf(name, isin)

    if existing:
        # Backfill missing fields
        changed = False
        if isin and not existing.isin:
            existing.isin = isin
            changed = True
        if amfi_code and not existing.scheme_code:
            existing.scheme_code = amfi_code
            changed = True
        if changed:
            existing.updated_at = datetime.now(timezone.utc)
            await existing.save()
        return existing

    # Determine if direct plan
    name_upper = name.upper()
    is_direct = "DIRECT" in name_upper
    is_growth = "GROWTH" in name_upper and "DIVIDEND" not in name_upper

    instrument = Instrument(
        isin=isin or None,
        symbol=amfi_code or isin or name[:20].replace(" ", "_").upper(),
        name=name,
        asset_class=asset_class,
        sub_class=sub_class,
        exchange=Exchange.AMFI,
        scheme_code=amfi_code or None,
        is_direct_plan=is_direct,
        is_growth_option=is_growth,
        fund_house=scheme.get("rta", ""),
    )
    await instrument.insert()
    logger.info(f"Created MF instrument: {name}")
    return instrument


async def _ingest_mf_transactions(
    account: Account,
    instrument: Instrument,
    transactions: list[dict],
) -> int:
    """
    Ingest all transactions for one MF scheme.
    Returns count of new transactions inserted.

    casparser provides a `type` field on each transaction:
      PURCHASE, PURCHASE_SIP, SWITCH_IN, SWITCH_OUT, REDEMPTION,
      STAMP_DUTY_TAX, STT_TAX, REVERSAL, DIVIDEND_PAYOUT,
      DIVIDEND_REINVESTMENT, etc.
    We use this directly instead of parsing the description string.
    """
    from app.models.transactions import Transaction, TransactionSource, TransactionType
    import hashlib
    from dateutil.parser import parse as parse_date

    # casparser type → our TransactionType
    TYPE_MAP = {
        "PURCHASE":               TransactionType.BUY,
        "PURCHASE_SIP":           TransactionType.SIP,
        "PURCHASE_STP":           TransactionType.SIP,
        "SWITCH_IN":              TransactionType.SWITCH_IN,
        "SWITCH_IN_MERGER":       TransactionType.SWITCH_IN,
        "SWITCH_OUT":             TransactionType.SWITCH_OUT,
        "SWITCH_OUT_MERGER":      TransactionType.SWITCH_OUT,
        "REDEMPTION":             TransactionType.REDEMPTION,
        "REDEMPTION_SWP":         TransactionType.REDEMPTION,
        "DIVIDEND_PAYOUT":        TransactionType.DIVIDEND,
        "DIVIDEND_REINVESTMENT":  TransactionType.DIVIDEND,
        "BONUS":                  TransactionType.BONUS,
        "REVERSAL":               TransactionType.REVERSAL,
    }

    # Skip these — not real investment transactions
    SKIP_TYPES = {
        "STAMP_DUTY_TAX", "STT_TAX", "TDS_TAX",
        "MISC", "UNKNOWN",
    }

    inserted = 0

    for txn in transactions:
        try:
            cas_type = (txn.get("type") or "").upper()

            # Skip tax/duty rows and annotation rows (no units)
            if cas_type in SKIP_TYPES:
                continue

            # Skip annotation rows casparser sometimes includes
            # (e.g. ***Address Updated***, ***Cancelled***)
            units = txn.get("units")
            amount = txn.get("amount")
            if units is None and amount is None:
                continue

            date_str = txn.get("date", "")
            if not date_str:
                continue

            try:
                txn_date = parse_date(date_str).date()
            except Exception:
                continue

            txn_type = TYPE_MAP.get(cas_type, TransactionType.BUY)

            # casparser already signs amounts correctly:
            # purchases = positive, redemptions/switch-outs = negative
            amount_float = float(amount or 0)
            units_float = float(units) if units is not None else None
            nav_float = float(txn.get("nav") or 0) or None

            # Dedup hash — use casparser's balance field too for uniqueness
            # (same date+amount can occur for STP instalments)
            balance = txn.get("balance") or ""
            hash_key = f"{account.id}:{instrument.id}:{date_str}:{cas_type}:{amount_float}:{units_float}:{balance}"
            dedup_hash = hashlib.sha256(hash_key.encode()).hexdigest()

            existing = await Transaction.find_one(
                Transaction.dedup_hash == dedup_hash
            )
            if existing:
                continue

            transaction = Transaction(
                account=account,
                instrument=instrument,
                transaction_type=txn_type,
                transaction_date=txn_date,
                quantity=units_float,
                price=nav_float,
                amount=amount_float,
                nav=nav_float,
                source=TransactionSource.CAS_PDF,
                dedup_hash=dedup_hash,
                notes=(txn.get("description") or "")[:200],  # cap length
            )
            await transaction.insert()
            inserted += 1

        except Exception as e:
            logger.error(f"Error inserting MF transaction: {e} — {txn}")

    return inserted


async def ingest_cams_kfintech_pdf(
    pdf_path: str,
    password: str,
    document_id: Optional[str] = None,
) -> dict:
    """
    Parse a CAMS or KFintech Detailed CAS PDF and ingest into MongoDB.

    Creates:
      - Account documents (one per folio)
      - Instrument documents (one per MF scheme)
      - Holding documents (current units + valuation)
      - Transaction documents (full SIP/purchase/redemption history)
    """
    doc: Optional[FinancialDocument] = None

    if document_id:
        doc = await FinancialDocument.get(document_id)
        if doc:
            doc.parse_status = ParseStatus.PARSING
            await doc.save()

    try:
        import json as _json
        logger.info(f"Parsing CAMS/KFintech PDF: {pdf_path}")
        raw = casparser.read_cas_pdf(pdf_path, password, output="json")
        parsed = _json.loads(raw)

        file_type = parsed.get("file_type", "UNKNOWN")
        cas_type = parsed.get("cas_type", "UNKNOWN")
        folios = parsed.get("folios", [])

        logger.info(f"file_type={file_type}, cas_type={cas_type}, folios={len(folios)}")

        total_holdings = 0
        total_transactions = 0
        total_accounts = 0
        errors = []

        for folio_data in folios:
            amc = folio_data.get("amc", "Unknown AMC")
            folio_number = folio_data.get("folio", "")
            schemes = folio_data.get("schemes", [])

            for scheme in schemes:
                try:
                    account = await _upsert_mf_account(amc, folio_number)
                    total_accounts += 1

                    instrument = await _upsert_mf_instrument(scheme)

                    # Current holding from valuation + close units
                    # Note: casparser returns valuation fields as strings — cast explicitly
                    close_units = float(scheme.get("close") or scheme.get("close_calculated") or 0)
                    valuation = scheme.get("valuation") or {}
                    nav   = float(valuation.get("nav")   or 0) or None
                    value = float(valuation.get("value") or 0) or None
                    cost  = float(valuation.get("cost")  or 0) or None

                    if close_units > 0:
                        avg_cost = (cost / close_units) if cost and close_units else (nav or 0)
                        existing_holding = await Holding.find_one(
                            Holding.account.id == account.id,       # type: ignore
                            Holding.instrument.id == instrument.id, # type: ignore
                        )
                        if existing_holding:
                            existing_holding.quantity = close_units
                            existing_holding.current_price = nav
                            existing_holding.current_value = value
                            existing_holding.last_synced_at = datetime.now(timezone.utc)
                            existing_holding.recompute()
                            await existing_holding.save()
                        else:
                            holding = Holding(
                                account=account,
                                instrument=instrument,
                                quantity=close_units,
                                avg_cost=avg_cost,
                                current_price=nav,
                                current_value=value,
                                invested_value=cost or (close_units * avg_cost),
                                last_synced_at=datetime.now(timezone.utc),
                                folio_number=folio_number,
                                is_active=close_units > 0,
                            )
                            holding.recompute()
                            await holding.insert()
                        total_holdings += 1

                    # Ingest full transaction history
                    txns = scheme.get("transactions", [])
                    if txns:
                        n = await _ingest_mf_transactions(account, instrument, txns)
                        total_transactions += n

                except Exception as e:
                    scheme_name = scheme.get("scheme", "unknown")
                    logger.error(f"Error processing scheme {scheme_name}: {e}")
                    errors.append(f"{scheme_name}: {e}")

        result = {
            "file_type": file_type,
            "cas_type": cas_type,
            "folios_processed": len(folios),
            "accounts_created": total_accounts,
            "holdings_upserted": total_holdings,
            "transactions_inserted": total_transactions,
            "errors": errors,
        }

        if doc:
            doc.parse_status = ParseStatus.INGESTED
            doc.holdings_updated = total_holdings
            doc.transactions_created = total_transactions
            doc.parsed_at = datetime.now(timezone.utc)
            doc.ingested_at = datetime.now(timezone.utc)
            await doc.save()

        logger.info(f"CAMS/KFintech ingestion complete: {result}")
        return result

    except Exception as e:
        logger.error(f"CAMS/KFintech parse failed: {e}")
        if doc:
            doc.parse_status = ParseStatus.FAILED
            doc.parse_error = str(e)
            await doc.save()
        raise


# ---------------------------------------------------------------------------
# Public entry points — accept already-parsed dict to avoid double-parsing
# ---------------------------------------------------------------------------

async def ingest_cams_kfintech_from_parsed(
    parsed: dict,
    document_id: Optional[str] = None,
) -> dict:
    """
    Ingest a CAMS/KFintech CAS from an already-parsed dict.
    Called by the upload endpoint after it detects the CAS type.
    """
    from app.models.documents import FinancialDocument, ParseStatus

    doc: Optional[FinancialDocument] = None
    if document_id:
        doc = await FinancialDocument.get(document_id)
        if doc:
            doc.parse_status = ParseStatus.PARSING
            await doc.save()

    try:
        file_type = parsed.get("file_type", "UNKNOWN")
        cas_type = parsed.get("cas_type", "UNKNOWN")
        folios = parsed.get("folios", [])

        logger.info(f"CAMS ingestion: file_type={file_type}, cas_type={cas_type}, folios={len(folios)}")

        total_holdings = 0
        total_transactions = 0
        total_accounts = 0
        errors = []

        for folio_data in folios:
            amc = folio_data.get("amc", "Unknown AMC")
            folio_number = folio_data.get("folio", "")
            schemes = folio_data.get("schemes", [])

            for scheme in schemes:
                try:
                    account = await _upsert_mf_account(amc, folio_number)
                    total_accounts += 1

                    instrument = await _upsert_mf_instrument(scheme)

                    close_units = float(scheme.get("close") or scheme.get("close_calculated") or 0)
                    valuation = scheme.get("valuation") or {}
                    nav   = float(valuation.get("nav")   or 0) or None
                    value = float(valuation.get("value") or 0) or None
                    cost  = float(valuation.get("cost")  or 0) or None

                    if close_units > 0:
                        avg_cost = (cost / close_units) if cost and close_units else (nav or 0)
                        existing_holding = await Holding.find_one(
                            Holding.account.id == account.id,       # type: ignore
                            Holding.instrument.id == instrument.id, # type: ignore
                        )
                        if existing_holding:
                            existing_holding.quantity = close_units
                            existing_holding.current_price = nav
                            existing_holding.current_value = value
                            existing_holding.last_synced_at = datetime.now(timezone.utc)
                            existing_holding.recompute()
                            await existing_holding.save()
                        else:
                            holding = Holding(
                                account=account,
                                instrument=instrument,
                                quantity=close_units,
                                avg_cost=avg_cost,
                                current_price=nav,
                                current_value=value,
                                invested_value=cost or (close_units * avg_cost),
                                last_synced_at=datetime.now(timezone.utc),
                                folio_number=folio_number,
                                is_active=close_units > 0,
                            )
                            holding.recompute()
                            await holding.insert()
                        total_holdings += 1

                    txns = scheme.get("transactions", [])
                    if txns:
                        n = await _ingest_mf_transactions(account, instrument, txns)
                        total_transactions += n

                except Exception as e:
                    scheme_name = scheme.get("scheme", "unknown")
                    logger.error(f"Error processing scheme {scheme_name}: {e}")
                    errors.append(f"{scheme_name}: {e}")

        result = {
            "file_type": file_type,
            "cas_type": cas_type,
            "folios_processed": len(folios),
            "accounts_created": total_accounts,
            "holdings_upserted": total_holdings,
            "transactions_inserted": total_transactions,
            "errors": errors,
        }

        if doc:
            doc.parse_status = ParseStatus.INGESTED
            doc.holdings_updated = total_holdings
            doc.transactions_created = total_transactions
            doc.parsed_at = datetime.now(timezone.utc)
            doc.ingested_at = datetime.now(timezone.utc)
            await doc.save()

        logger.info(f"CAMS ingestion complete: {result}")
        return result

    except Exception as e:
        logger.error(f"CAMS ingestion failed: {e}")
        if doc:
            doc.parse_status = ParseStatus.FAILED
            doc.parse_error = str(e)
            await doc.save()
        raise


async def ingest_nsdl_cdsl_from_parsed(
    parsed: dict,
    document_id: Optional[str] = None,
) -> dict:
    """
    Ingest an NSDL/CDSL demat CAS from an already-parsed dict.
    """
    from app.models.documents import FinancialDocument, ParseStatus

    doc: Optional[FinancialDocument] = None
    if document_id:
        doc = await FinancialDocument.get(document_id)
        if doc:
            doc.parse_status = ParseStatus.PARSING
            await doc.save()

    try:
        file_type = parsed.get("file_type", "UNKNOWN")
        statement_period = parsed.get("statement_period", {})
        accounts_data = parsed.get("accounts", [])

        total_holdings = 0
        total_accounts = 0
        errors = []

        for cas_account in accounts_data:
            dp_id = cas_account.get("dp_id", "")
            client_id = cas_account.get("client_id", "")
            equities = cas_account.get("equities", [])
            mutual_funds = cas_account.get("mutual_funds", [])

            if not equities and not mutual_funds:
                continue

            try:
                account = await _upsert_account(cas_account, dp_id, client_id)
                total_accounts += 1
            except Exception as e:
                errors.append(f"Account {cas_account.get('name')}: {e}")
                continue

            for eq in equities:
                try:
                    price = float(eq.get("price", 0) or 0) or None
                    value = float(eq.get("value", 0) or 0) or None
                    raw_shares = float(eq.get("num_shares", 0) or 0)

                    name_upper = (eq.get("name") or "").upper()
                    is_debt = any(x in name_upper for x in ["NCD", "BOND", "DEBENTURE", "TBILL", "GSEC"])

                    if is_debt and price and value and price > 0:
                        quantity = round(value / price, 4)
                    else:
                        quantity = raw_shares

                    if quantity <= 0:
                        continue

                    instrument = await _upsert_instrument_equity(eq)
                    await _upsert_holding(account, instrument, quantity, price, value)
                    total_holdings += 1
                except Exception as e:
                    errors.append(f"Equity {eq.get('name')}: {e}")

            for mf in mutual_funds:
                try:
                    quantity = float(mf.get("balance", 0) or 0)
                    if quantity <= 0:
                        continue
                    nav = float(mf.get("nav", 0) or 0) or None
                    value = float(mf.get("value", 0) or 0) or None
                    instrument = await _upsert_instrument_mf(mf)
                    await _upsert_holding(account, instrument, quantity, nav, value)
                    total_holdings += 1
                except Exception as e:
                    errors.append(f"MF {mf.get('name')}: {e}")

        result = {
            "file_type": file_type,
            "statement_period": statement_period,
            "accounts_processed": total_accounts,
            "holdings_upserted": total_holdings,
            "errors": errors,
        }

        if doc:
            doc.parse_status = ParseStatus.INGESTED
            doc.holdings_updated = total_holdings
            doc.parsed_at = datetime.now(timezone.utc)
            doc.ingested_at = datetime.now(timezone.utc)
            await doc.save()

        logger.info(f"NSDL/CDSL ingestion complete: {result}")
        return result

    except Exception as e:
        logger.error(f"NSDL/CDSL ingestion failed: {e}")
        if doc:
            doc.parse_status = ParseStatus.FAILED
            doc.parse_error = str(e)
            await doc.save()
        raise