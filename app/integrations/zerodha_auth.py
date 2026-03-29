"""
app/integrations/zerodha_auth.py

Kite Connect OAuth flow and token management.

The daily auth flow:
  1. User visits /zerodha/login → redirected to Kite login page
  2. Kite redirects back to /zerodha/callback?request_token=...
  3. We exchange request_token → access_token and store it
  4. All subsequent API calls use the stored access_token

Token lifespan: 6am IST today → 6am IST tomorrow (~24h).

Fix: load_dotenv() is called at the top of this module so that
os.getenv() works when ZerodhaAuthService is instantiated at
import time. The singleton is replaced with a lazy getter function
so instantiation only happens when first used, after the event loop
and .env loading are both complete.
"""

import os
from datetime import datetime, timezone, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Load .env before any os.getenv() calls
load_dotenv()

from kiteconnect import KiteConnect
from app.models.zerodha_token import ZerodhaToken


IST = ZoneInfo("Asia/Kolkata")


def _next_6am_ist() -> datetime:
    """
    Returns the next 6am IST as a UTC-aware datetime.
    This is when Kite access tokens expire.
    """
    now_ist = datetime.now(IST)
    expiry_ist = now_ist.replace(hour=6, minute=0, second=0, microsecond=0)
    if now_ist >= expiry_ist:
        expiry_ist += timedelta(days=1)
    return expiry_ist.astimezone(timezone.utc)


class ZerodhaAuthService:
    """
    Manages the Kite Connect OAuth lifecycle.

    Usage:
        from app.integrations.zerodha_auth import get_zerodha_auth
        auth = get_zerodha_auth()
        login_url = auth.get_login_url()
        await auth.handle_callback(request_token)
        kite = await auth.get_kite_client()
    """

    def __init__(self):
        self.api_key = os.getenv("ZERODHA_API_KEY")
        self.api_secret = os.getenv("ZERODHA_API_SECRET")
        if not self.api_key or not self.api_secret:
            raise ValueError(
                "ZERODHA_API_KEY and ZERODHA_API_SECRET must be set in .env\n"
                f"  Looked in: {os.path.abspath('.env')}\n"
                f"  ZERODHA_API_KEY={'set' if self.api_key else 'MISSING'}\n"
                f"  ZERODHA_API_SECRET={'set' if self.api_secret else 'MISSING'}"
            )

    def get_login_url(self) -> str:
        """Generate the Kite login URL."""
        kite = KiteConnect(api_key=self.api_key)
        print(self.api_key)
        return kite.login_url()

    async def handle_callback(self, request_token: str) -> ZerodhaToken:
        """
        Exchange the request_token for an access_token and store it.
        Called by the /zerodha/callback FastAPI endpoint.
        """
        kite = KiteConnect(api_key=self.api_key)
        data = kite.generate_session(request_token, api_secret=self.api_secret)

        token_doc = ZerodhaToken(
            access_token=data["access_token"],
            public_token=data.get("public_token"),
            user_id=data["user_id"],
            login_time=datetime.now(timezone.utc),
            expires_at=_next_6am_ist(),
        )

        await ZerodhaToken.find_all().delete()
        await token_doc.insert()

        print(f"Zerodha token stored for {token_doc.user_id}, expires {token_doc.expires_at}")
        return token_doc

    async def get_kite_client(self) -> KiteConnect:
        """
        Returns a ready-to-use KiteConnect client with the current access token.
        Raises ValueError if no valid token exists.
        """
        token_doc = await ZerodhaToken.find_one()
        if not token_doc:
            raise ValueError(
                "No valid Zerodha token. Visit /zerodha/login to authenticate."
            )
        kite = KiteConnect(api_key=self.api_key)
        kite.set_access_token(token_doc.access_token)
        return kite

    async def get_current_token(self) -> Optional[ZerodhaToken]:
        return await ZerodhaToken.find_one()

    async def is_authenticated(self) -> bool:
        token = await self.get_current_token()
        if not token:
            return False
        expires = token.expires_at
        # MongoDB may strip timezone info on retrieval — normalize to UTC
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) < expires


# ---------------------------------------------------------------------------
# Lazy singleton — instantiated once on first call, not at import time.
# This avoids the ValueError if .env isn't loaded yet when the module
# is first imported during FastAPI startup.
# ---------------------------------------------------------------------------
_zerodha_auth_instance: Optional[ZerodhaAuthService] = None


def get_zerodha_auth() -> ZerodhaAuthService:
    """
    Returns the ZerodhaAuthService singleton.
    Call this instead of importing zerodha_auth directly.

    Example:
        from app.integrations.zerodha_auth import get_zerodha_auth
        auth = get_zerodha_auth()
    """
    global _zerodha_auth_instance
    if _zerodha_auth_instance is None:
        _zerodha_auth_instance = ZerodhaAuthService()
    return _zerodha_auth_instance