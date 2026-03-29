"""
telegram_service.py — send messages to your personal Telegram chat.

This is the minimal version needed to support sync notifications.
The full Telegram bot (document drops, weekly digest, commands) 
will be built as a separate module in Phase 3.

Setup:
  1. Message @BotFather on Telegram → /newbot → get your token
  2. Message your bot once (any text)
  3. Visit https://api.telegram.org/bot<TOKEN>/getUpdates
  4. Copy your chat_id from the response
  5. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env
"""

import os
import httpx
import logging

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"


async def send_telegram_message(
    text: str,
    parse_mode: str = "Markdown",
    chat_id: str | None = None,
) -> bool:
    """
    Send a message to your personal Telegram chat.

    Returns True on success, False on failure (never raises — 
    notification failure should never crash a sync job).

    parse_mode = "Markdown" supports *bold*, _italic_, and [links](url).
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    target_chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not target_chat_id:
        logger.warning("Telegram not configured — TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")
        return False

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": target_chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                logger.error(f"Telegram API error {resp.status_code}: {resp.text}")
                return False
        return True
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")
        return False


async def send_telegram_document(
    file_bytes: bytes,
    filename: str,
    caption: str = "",
    chat_id: str | None = None,
) -> bool:
    """
    Send a file (PDF, CSV) to your Telegram chat.
    Used later for sending weekly digest as a formatted document.
    """
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    target_chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token or not target_chat_id:
        return False

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendDocument"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                url,
                data={"chat_id": target_chat_id, "caption": caption},
                files={"document": (filename, file_bytes, "application/octet-stream")},
            )
            return resp.status_code == 200
    except Exception as e:
        logger.error(f"Failed to send Telegram document: {e}")
        return False