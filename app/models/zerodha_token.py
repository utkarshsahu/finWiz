"""
app/models/zerodha_token.py

ZerodhaToken — stores the daily Kite Connect access token in MongoDB.

Kite access tokens expire at 6am IST every day. This collection holds
the current valid token. A TTL index auto-deletes expired documents
so you always know: if a document exists here, it's valid.

One document at most — we upsert, never insert a second one.

Fix: Beanie/MongoDB strips timezone info from datetime fields on read.
The field_validator ensures expires_at and login_time always have
UTC tzinfo attached, preventing naive vs aware comparison errors.
"""

from datetime import datetime, timezone
from typing import Optional
from beanie import Document
from pydantic import Field, field_validator


class ZerodhaToken(Document):
    """
    Current Kite Connect session token.

    expires_at is set to 6am IST of the next day at creation time.
    The TTL index on expires_at auto-deletes this document when it expires,
    so checking `await ZerodhaToken.find_one()` returning None means
    re-authentication is required.
    """

    access_token: str
    public_token: Optional[str] = None
    user_id: str
    login_time: datetime
    expires_at: datetime                        # 6am IST next day

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @field_validator("expires_at", "login_time", "created_at", mode="before")
    @classmethod
    def ensure_utc(cls, v):
        """
        Attach UTC tzinfo to any naive datetime read back from MongoDB.
        MongoDB stores datetimes as UTC but Beanie may return them without
        tzinfo, causing TypeError on comparison with timezone-aware datetimes.
        """
        if isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    class Settings:
        name = "zerodha_tokens"
        # TTL index set in create_indexes.py:
        # IndexModel([("expires_at", ASCENDING)], expireAfterSeconds=0)