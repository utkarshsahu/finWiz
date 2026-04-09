"""
app/routes/telegram_router.py

Telegram webhook endpoint.

Endpoints:
  POST /telegram/webhook    → Receives updates from Telegram
  POST /telegram/set-webhook → Register webhook URL with Telegram
  GET  /telegram/status     → Check bot status
"""

import asyncio
import logging
import os
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

router = APIRouter(prefix="/telegram", tags=["telegram"])
logger = logging.getLogger(__name__)


@router.post("/webhook")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Telegram sends all updates here via webhook.
    Returns 200 immediately — Telegram requires a response within 5 seconds.
    The update is processed in a FastAPI background task so slow commands
    (/sync, /digest) never cause Telegram to retry and double-execute.
    """
    try:
        update = await request.json()
    except Exception as e:
        logger.error(f"Webhook parse error: {e}")
        return {"ok": True}

    async def _run():
        try:
            from services.telegram_bot import handle_update
            await handle_update(update)
        except Exception as e:
            logger.error(f"Webhook handler error: {e}")

    background_tasks.add_task(_run)
    return {"ok": True}


@router.post("/set-webhook")
async def set_webhook(url: str):
    """
    Register your webhook URL with Telegram.
    Call once after deployment:
      POST /telegram/set-webhook?url=https://yourdomain.com/telegram/webhook

    For local dev with ngrok:
      1. Run: ngrok http 8000
      2. POST /telegram/set-webhook?url=https://abc123.ngrok.io/telegram/webhook
    """
    import httpx
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN not set")

    webhook_url = f"{url}/telegram/webhook" if not url.endswith("/webhook") else url

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://api.telegram.org/bot{bot_token}/setWebhook",
            json={"url": webhook_url},
        )
        result = resp.json()

    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=f"Telegram error: {result}")

    return {
        "status": "webhook registered",
        "url": webhook_url,
        "telegram_response": result,
    }


@router.get("/status")
async def telegram_status():
    """Check bot info and webhook status."""
    import httpx
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        return {"configured": False}

    async with httpx.AsyncClient() as client:
        me = await client.get(f"https://api.telegram.org/bot{bot_token}/getMe")
        webhook = await client.get(f"https://api.telegram.org/bot{bot_token}/getWebhookInfo")

    return {
        "configured": True,
        "bot": me.json().get("result", {}),
        "webhook": webhook.json().get("result", {}),
    }