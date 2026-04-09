"""
integrations/nse_corporate_actions.py

Fetches upcoming/recent corporate actions from NSE for a given equity symbol.

NSE endpoint: /api/quote-equity?symbol=RELIANCE&section=corp_info
Returns dividends, bonuses, splits, rights, buybacks.

Called by the rules engine's check_corporate_actions() before generating signals.
Only surfaces actions within a configurable forward-looking window (default: 30 days).
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from integrations.market_fetcher import _get_nse_session, NSE_BASE

logger = logging.getLogger(__name__)

LOOKAHEAD_DAYS = 30   # flag actions due within this many days
LOOKBACK_DAYS  = 7    # also flag very recent actions (e.g. just-announced dividends)


class CorporateAction:
    def __init__(
        self,
        action_type: str,           # "dividend" | "bonus" | "split" | "rights" | "buyback"
        symbol: str,
        subject: str,               # raw description from NSE
        ex_date: Optional[date],
        record_date: Optional[date],
        details: dict,
    ):
        self.action_type  = action_type
        self.symbol       = symbol
        self.subject      = subject
        self.ex_date      = ex_date
        self.record_date  = record_date
        self.details      = details   # raw fields for the signal data dict

    def is_upcoming(self, lookahead: int = LOOKAHEAD_DAYS, lookback: int = LOOKBACK_DAYS) -> bool:
        ref = self.ex_date or self.record_date
        if not ref:
            return False
        today = date.today()
        return (today - timedelta(days=lookback)) <= ref <= (today + timedelta(days=lookahead))


def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


def _classify(subject: str) -> str:
    s = subject.lower()
    if "buyback" in s or "buy back" in s:
        return "buyback"
    if "rights" in s:
        return "rights"
    if "split" in s or "sub-division" in s:
        return "split"
    if "bonus" in s:
        return "bonus"
    if "dividend" in s or "div" in s:
        return "dividend"
    return "other"


async def fetch_corporate_actions(symbol: str) -> list[CorporateAction]:
    """
    Fetch corporate actions for a single NSE equity symbol.

    Returns a list of CorporateAction objects (may be empty on error).
    The caller filters for upcoming actions with .is_upcoming().
    """
    bare = symbol.replace(".NS", "").replace(".BO", "").upper()
    url  = f"{NSE_BASE}/api/quote-equity?symbol={bare}&section=corp_info"

    session = _get_nse_session()
    data = await session.get_json(url)

    if not data:
        logger.warning(f"No corp_info data for {bare}")
        return []

    actions: list[CorporateAction] = []

    # NSE returns corp actions under different keys depending on endpoint version
    corp_info = data.get("corporate", data.get("corpInfo", {}))
    items = (
        corp_info.get("corpAction", [])
        or corp_info.get("corporateAction", [])
        or data.get("corpInfo", {}).get("corpAction", [])
        or []
    )

    for item in items:
        subject    = item.get("subject") or item.get("purpose") or ""
        ex_date    = _parse_date(item.get("exDate") or item.get("ex_date"))
        record_date= _parse_date(item.get("recordDate") or item.get("record_date"))
        action_type= _classify(subject)

        if action_type == "other":
            continue   # skip noise (AGM notices etc.)

        actions.append(CorporateAction(
            action_type=action_type,
            symbol=bare,
            subject=subject,
            ex_date=ex_date,
            record_date=record_date,
            details={
                "subject":      subject,
                "ex_date":      str(ex_date)     if ex_date     else None,
                "record_date":  str(record_date) if record_date else None,
                **{k: v for k, v in item.items()
                   if k not in ("subject", "purpose", "exDate", "ex_date",
                                "recordDate", "record_date")},
            },
        ))

    logger.info(f"Corporate actions for {bare}: {len(actions)} found")
    return actions
