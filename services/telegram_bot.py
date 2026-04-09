"""
app/services/telegram_bot.py

Full Telegram bot with webhook handling.

Commands:
  /start        → Welcome + help
  /digest       → Generate + send this week's recommendation digest
  /snapshot     → Send current portfolio snapshot
  /signals      → Send active signals
  /sync         → Trigger Zerodha + market data sync
  /research     → Show recent research items
  /help         → Command list

Message handling:
  URL in text   → Ingest as research article
  PDF file      → Auto-detect: CAS PDF → ingest holdings
                               Research PDF → ingest as research item
  Any other     → Help message

Webhook setup (run once after deployment):
  curl -X POST https://api.telegram.org/bot<TOKEN>/setWebhook \
    -H "Content-Type: application/json" \
    -d '{"url": "https://yourdomain.com/telegram/webhook"}'

For local dev use ngrok:
  ngrok http 8000
  # then set webhook to https://<ngrok-url>/telegram/webhook
"""

import logging
import os
import re
import tempfile
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"

# --- Markdown (legacy) formatter ---
def _format_markdown(text: str) -> str:
    """
    Minimal escaping for Telegram Markdown (not V2).
    - Escapes underscores to avoid unintended italics
    - Preserves *bold* formatting
    """

    # Step 1: Temporarily protect bold segments (*text*)
    bold_tokens = []

    def _protect(match):
        bold_tokens.append(match.group(0))
        return f"@@BOLD{len(bold_tokens)-1}@@"

    text = re.sub(r'\*[^*]+\*', _protect, text)

    # Step 2: Escape underscores everywhere else
    text = text.replace("_", r"\_")

    # Step 3: Restore bold
    for i, token in enumerate(bold_tokens):
        text = text.replace(f"@@BOLD{i}@@", token)

    return text

# ---------------------------------------------------------------------------
# Low-level send helpers
# ---------------------------------------------------------------------------

async def _send(chat_id: str, text: str, parse_mode: str = "Markdown") -> bool:
    """Send a text message. Truncates at 4096 chars (Telegram limit)."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        return False

    # Telegram message limit
    text = text[:4096]

    if parse_mode == "Markdown":
        text = _format_markdown(text)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            #print('chat_id: ', chat_id, 'message', text, parse_mode)
            resp = await client.post(
                f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True,
                },
            )
            return resp.status_code == 200
    except Exception as e:
        logger.error(f"Telegram send error: {e}")
        return False


async def _send_long(chat_id: str, text: str, parse_mode: str = "Markdown"):
    """Split and send long messages in chunks."""
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for chunk in chunks:
        await _send(chat_id, chunk, parse_mode)


async def _download_file(file_id: str) -> Optional[bytes]:
    """Download a file from Telegram by file_id."""
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Get file path
            resp = await client.get(
                f"{TELEGRAM_API_BASE}/bot{bot_token}/getFile",
                params={"file_id": file_id},
            )
            file_path = resp.json()["result"]["file_path"]

            # Download file
            file_resp = await client.get(
                f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
            )
            return file_resp.content
    except Exception as e:
        logger.error(f"File download error: {e}")
        return None


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

async def handle_start(chat_id: str):
    await _send(chat_id, (
        "👋 *Paisa Paisa — Your Personal Finance Agent*\n\n"
        "Commands:\n"
        "/digest — This week's investment recommendations\n"
        "/snapshot — Current portfolio summary\n"
        "/signals — Active alerts from rules engine\n"
        "/technicals — Technical + corporate action checks on held equities\n"
        "/sync — Refresh Zerodha + market prices\n"
        "/research — Recent market research\n"
        "/help — Show this message\n\n"
        "📎 *Drop anything:*\n"
        "• Paste a URL → ingested as research\n"
        "• Send a PDF → CAS statement or research report\n"
    ))


async def handle_snapshot(chat_id: str):
    await _send(chat_id, "⏳ Fetching portfolio snapshot...")
    try:
        from services.analytics_service import AnalyticsService
        svc = AnalyticsService()
        snap = await svc.get_portfolio_snapshot()

        by_ac = snap.get("by_asset_class", {})
        total = snap.get("total_value", 0)
        invested = snap.get("total_invested", 0)
        pnl = snap.get("unrealized_pnl", 0)
        pnl_pct = snap.get("unrealized_pnl_pct", 0)
        pnl_emoji = "📈" if pnl >= 0 else "📉"

        lines = [
            f"*Portfolio Snapshot*",
            f"",
            f"💰 Total Value: ₹{total:,.0f}",
            f"📥 Invested: ₹{invested:,.0f}",
            f"{pnl_emoji} P&L: ₹{pnl:,.0f} ({pnl_pct:+.1f}%)",
            f"",
            f"*Allocation:*",
        ]

        for ac, data in sorted(by_ac.items(), key=lambda x: -x[1].get("pct", 0)):
            pct = data.get("pct", 0)
            val = data.get("value", 0)
            lines.append(f"  {ac.replace('_', ' ').title()}: {pct:.1f}% (₹{val:,.0f})")

        freshness = snap.get("data_freshness", 1.0)
        if freshness < 0.8:
            lines.append(f"\n⚠️ Data freshness: {freshness:.0%} — some data may be stale")

        await _send(chat_id, "\n".join(lines))

    except Exception as e:
        await _send(chat_id, f"❌ Error fetching snapshot: {e}")


async def handle_signals(chat_id: str):
    logger.info(f"handle_signals called for {chat_id}")
    await _send(chat_id, "⏳ Checking signals...")
    logger.info("sent checking signals message")
    try:
        import asyncio
        from models.signals import Signal, SignalSeverity

        # Fetch all then filter in Python to avoid Beanie query hanging
        all_signals = await asyncio.wait_for(
            Signal.find_all().to_list(),
            timeout=10.0
        )
        active = [s for s in all_signals if not s.is_resolved]

        if not active:
            await _send(chat_id, "✅ No active signals — portfolio looks clean.")
            return

        active.sort(key=lambda s: s.generated_at, reverse=True)
        active = active[:10]

        urgent = [s for s in active if s.severity == SignalSeverity.URGENT]
        normal = [s for s in active if s.severity == SignalSeverity.NORMAL]

        lines = [f"*Active Signals ({len(active)})*\n"]

        if urgent:
            lines.append("🔴 *Urgent:*")
            for s in urgent:
                lines.append(f"  • {s.title}")

        if normal:
            lines.append("\n🟡 *Normal:*")
            for s in normal:
                lines.append(f"  • {s.title}")

        await _send(chat_id, "\n".join(lines))

    except asyncio.TimeoutError:
        await _send(chat_id, "⏱️ Timed out. Try again.")
    except Exception as e:
        await _send(chat_id, f"❌ Error: {str(e)[:200]}")


async def handle_digest(chat_id: str):
    await _send(chat_id, "⏳ Scouting for opportunities and analyzing portfolio... (~10 seconds)")
    try:
        from services.recommendation_engine import RecommendationEngine
        from models.recommendations import ActionType
        
        engine = RecommendationEngine()
        result = await engine.generate_weekly_digest()

        if not result:
            await _send(chat_id, "❌ Failed to generate digest. Check API keys.")
            return

        # Use our markdown formatter on narrative and dates
        lines = [
            f"📊 *Weekly Digest — {_format_markdown(result['week_start'])}*\n",
            f"_{_format_markdown(result.get('market_narrative', ''))}_\n",
        ]

        actions = result.get("actions", [])
        if actions:
            lines.append("🚀 *Top Recommended Actions:*")
            for action in actions:
                # 1. Determine Icon based on Type + Urgency
                a_type = action.get("type")
                urgency = action.get("urgency", 0)
                
                # Default urgency color
                icon = "🔴" if urgency >= 0.7 else "🟡" if urgency >= 0.4 else "🟢"
                
                # Override icon for "Discovery" or specific types
                if a_type == ActionType.NEW_SECTOR_ENTRY:
                    icon = "✨"
                elif "rebalance" in a_type:
                    icon = "⚖️"
                elif "buy" in a_type:
                    icon = "💰"

                # 2. Format Title (Highlighting New Opportunities)
                title = action['title']
                if a_type == ActionType.NEW_SECTOR_ENTRY:
                    title = f"NEW OPPORTUNITY: {title}"

                # 3. Build Action Block with escaped markdown
                lines.append(
                    f"{icon} *{action['rank']}. {_format_markdown(title)}*\n"
                    f"   └ {_format_markdown(action['rationale'])}\n"
                )

        violations = result.get("policy_violations", [])
        if violations:
            lines.append("\n🚨 *Policy Violations:*")
            for v in violations:
                lines.append(f"  • {_format_markdown(v)}")

        contradictions = result.get("contradictions", [])
        if contradictions:
            lines.append("\n🔄 *Market Contradictions:*")
            for c in contradictions:
                lines.append(f"  • {_format_markdown(c)}")

        # Stale data warnings
        stale = result.get("stale_warnings", [])
        if stale:
            lines.append("\n⚠️ *Data Freshness:*")
            for s in stale:
                lines.append(f"  • {_format_markdown(s)}")

        footer = (
            f"\n_Based on {result.get('signals_used', 0)} signals "
            f"and {result.get('research_used', 0)} research items_"
        )
        lines.append(_format_markdown(footer))

        # Ensure we use the long sender for large digests
        await _send_long(chat_id, "\n".join(lines))

    except Exception as e:
        logger.error(f"Digest error: {e}")
        # Escape the error message itself just in case
        await _send(chat_id, f"❌ Error generating digest: {_format_markdown(str(e))}")


async def handle_sync(chat_id: str):
    await _send(chat_id, "⏳ Syncing Zerodha + market data...")
    try:
        from integrations.zerodha_auth import get_zerodha_auth
        from integrations.zerodha_sync import ZerodhaSync
        from integrations.market_data_sync import MarketDataSync

        auth = get_zerodha_auth()
        if not await auth.is_authenticated():
            login_url = auth.get_login_url()
            await _send(
                chat_id,
                f"🔐 Zerodha token expired.\nTap to login: <a href=\"{login_url}\">Link</a>",
                parse_mode="HTML"
            )
            return

        z_sync = ZerodhaSync()
        z_result = await z_sync.sync_holdings()

        m_sync = MarketDataSync()
        m_result = await m_sync.run_full_sync()

        await _send(chat_id, (
            f"✅ *Sync Complete*\n\n"
            f"Zerodha: {z_result.get('holdings_upserted', 0)} holdings updated\n"
            f"MF NAVs: {m_result.get('mutual_funds', {}).get('mf_navs_updated', 0)} updated\n"
            f"Equities: {m_result.get('equities', {}).get('equity_prices_updated', 0)} updated"
        ))

    except Exception as e:
        await _send(chat_id, f"❌ Sync error: {e}")


async def handle_technicals(chat_id: str):
    """
    /technicals — run technical + corporate action checks and surface signals
    that need the user's attention.

    Only shows signals that warrant a market decision:
      - TECHNICAL_DRAWDOWN (significant price drop)
      - TECHNICAL_NEAR_52W_LOW (near bottom)
      - FUNDAMENTAL_CONCERN (loss-making, weak ROE/D/E)
      - CORPORATE_ACTION_RIGHTS / BUYBACK (time-sensitive)
      - CORPORATE_ACTION_BONUS / SPLIT / DIVIDEND (informational)

    TECHNICAL_MOMENTUM_STRONG and TECHNICAL_HIGH_VOLATILITY are INFO-level and
    are not shown here (they appear in /signals if the user wants them).
    """
    await _send(chat_id, "⏳ Running technicals analysis... (may take ~30 seconds)")

    try:
        from services.rules_engine import RulesEngine
        from models.signals import Signal, SignalType, SignalSeverity

        engine = RulesEngine()
        tech_count     = await engine.check_technicals()
        corp_count     = await engine.check_corporate_actions()
        total_new      = tech_count + corp_count

        # Pull all active technical + corporate action signals
        TECHNICAL_TYPES = {
            SignalType.TECHNICAL_DRAWDOWN,
            SignalType.TECHNICAL_NEAR_52W_LOW,
            SignalType.FUNDAMENTAL_CONCERN,
            SignalType.CORPORATE_ACTION_RIGHTS,
            SignalType.CORPORATE_ACTION_BUYBACK,
            SignalType.CORPORATE_ACTION_BONUS,
            SignalType.CORPORATE_ACTION_SPLIT,
            SignalType.CORPORATE_ACTION_DIVIDEND,
        }

        import asyncio
        all_signals = await asyncio.wait_for(
            Signal.find_all().to_list(),
            timeout=15.0
        )

        relevant = [
            s for s in all_signals
            if not s.is_resolved and s.signal_type in TECHNICAL_TYPES
        ]

        if not relevant:
            await _send(
                chat_id,
                f"✅ *Technicals clean* ({total_new} new signals generated)\n\n"
                f"No equity holdings need attention right now."
            )
            return

        # Sort: urgent first, then by generated_at desc
        relevant.sort(key=lambda s: (
            0 if s.severity == SignalSeverity.URGENT else
            1 if s.severity == SignalSeverity.NORMAL else 2,
            -s.generated_at.timestamp()
        ))

        lines = [f"*Technicals — {len(relevant)} signal(s) need attention*\n"]

        urgent  = [s for s in relevant if s.severity == SignalSeverity.URGENT]
        normal  = [s for s in relevant if s.severity == SignalSeverity.NORMAL]

        if urgent:
            lines.append("🔴 *Urgent — action required:*")
            for s in urgent:
                lines.append(f"  • *{s.title}*")
                lines.append(f"    {s.description[:140]}")
                lines.append("")

        if normal:
            lines.append("🟡 *Review:*")
            for s in normal:
                lines.append(f"  • *{s.title}*")
                lines.append(f"    {s.description[:140]}")
                lines.append("")

        if total_new:
            lines.append(f"\n_{total_new} new signal(s) added to /signals_")

        await _send_long(chat_id, "\n".join(lines))

    except Exception as e:
        logger.error(f"handle_technicals error: {e}")
        await _send(chat_id, f"❌ Error running technicals: {str(e)[:200]}")


async def handle_research(chat_id: str):
    try:
        from models.research import ResearchItem
        from datetime import datetime, timedelta, timezone

        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        items = await ResearchItem.find(
            ResearchItem.ingested_at >= cutoff
        ).sort([("portfolio_relevance_score", -1)]).limit(5).to_list()

        if not items:
            await _send(chat_id, "No research items in the last 7 days.\nDrop a URL or PDF to add some.")
            return

        lines = ["*📰 Recent Research (last 7 days)*\n"]
        for item in items:
            sentiment_emoji = {"bullish": "📈", "bearish": "📉", "neutral": "➡️", "mixed": "↔️"}.get(
                item.sentiment.value, "➡️"
            )
            lines.append(
                f"{sentiment_emoji} *{item.title[:60]}*\n"
                f"   {item.summary[:120]}...\n"
                f"   Themes: {', '.join(item.themes[:3])}\n"
            )

        await _send_long(chat_id, "\n".join(lines))

    except Exception as e:
        await _send(chat_id, f"❌ Error: {e}")


# ---------------------------------------------------------------------------
# URL and file handlers
# ---------------------------------------------------------------------------

async def handle_url_drop(chat_id: str, url: str):
    """User dropped a URL — ingest as research."""
    await _send(chat_id, f"🔍 Processing article...")
    try:
        from integrations.research_ingester import ingest_url
        item_id = await ingest_url(url)
        if item_id:
            from models.research import ResearchItem
            item = await ResearchItem.get(item_id)
            await _send(chat_id, (
                f"✅ *Research ingested*\n\n"
                f"*{item.title}*\n\n"
                f"{item.summary}\n\n"
                f"Themes: {', '.join(item.themes)}\n"
                f"Relevance: {item.portfolio_relevance_score:.0%}"
            ))
        else:
            await _send(chat_id, "⚠️ Could not process this URL. Try another.")
    except Exception as e:
        await _send(chat_id, f"❌ Error: {e}")


async def handle_pdf_drop(chat_id: str, file_id: str, filename: str):
    """
    User sent a PDF.
    Auto-detect: CAS statement → ingest holdings
                 Other PDF → ingest as research
    """
    await _send(chat_id, f"📄 Processing PDF: {filename}...")

    file_bytes = await _download_file(file_id)
    if not file_bytes:
        await _send(chat_id, "❌ Could not download the file.")
        return

    # Save to temp file
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    import os as _os
    try:
        # Try casparser to detect if it's a CAS PDF
        import casparser, json as _json
        try:
            raw = casparser.read_cas_pdf(tmp_path, "", output="json")  # empty password first
            parsed = _json.loads(raw)
            is_cas = parsed.get("file_type") in ("CAMS", "KARVY", "KFINTECH", "NSDL", "CDSL")
        except Exception:
            is_cas = False

        if is_cas:
            # It's a CAS — ask for password
            await _send(
                chat_id,
                "🔐 This looks like a CAS statement.\n\n"
                "Please reply with your PAN (password) to process it:\n"
                "e.g. `ABCDE1234F`\n\n"
                "_Upload via web portal at /docs for better control._"
            )
            # Store pending file path in a simple way
            # For now direct to web upload — full interactive flow needs session state
            await _send(
                chat_id,
                f"For now, please upload your CAS at:\n"
                f"`POST /documents/upload/cas`\n"
                f"via the API docs at your server URL."
            )
        else:
            # Research PDF
            from integrations.research_ingester import ingest_pdf
            item_id = await ingest_pdf(tmp_path, filename=filename)
            if item_id:
                from models.research import ResearchItem
                item = await ResearchItem.get(item_id)
                await _send(chat_id, (
                    f"✅ *Research PDF ingested*\n\n"
                    f"*{item.title}*\n\n"
                    f"{item.summary}\n\n"
                    f"Themes: {', '.join(item.themes)}\n"
                    f"Relevance: {item.portfolio_relevance_score:.0%}"
                ))
            else:
                await _send(chat_id, "⚠️ Could not extract useful content from this PDF.")

    finally:
        if _os.path.exists(tmp_path):
            _os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Main webhook handler
# ---------------------------------------------------------------------------

async def handle_update(update: dict):
    """
    Process an incoming Telegram update.
    Called by the webhook endpoint in telegram_router.py.
    """
    message = update.get("message") or update.get("edited_message")
    if not message:
        return

    chat_id = str(message["chat"]["id"])
    allowed_chat_id = os.getenv("TELEGRAM_CHAT_ID")

    # Security — only respond to your own chat
    if allowed_chat_id and chat_id != allowed_chat_id:
        logger.warning(f"Rejected message from unknown chat_id: {chat_id}")
        return

    text = message.get("text", "")
    document = message.get("document")

    logger.info(f"Bot received: chat_id={chat_id} text={text!r} has_doc={bool(document)}")

    # Handle commands
    if text.startswith("/ping"):
        await _send(chat_id, "pong ✅")
    elif text.startswith("/start"):
        await handle_start(chat_id)
    elif text.startswith("/digest"):
        await handle_digest(chat_id)
    elif text.startswith("/snapshot"):
        await handle_snapshot(chat_id)
    elif text.startswith("/signals"):
        await handle_signals(chat_id)
    elif text.startswith("/sync"):
        await handle_sync(chat_id)
    elif text.startswith("/technicals"):
        await handle_technicals(chat_id)
    elif text.startswith("/research"):
        await handle_research(chat_id)
    elif text.startswith("/help"):
        await handle_start(chat_id)

    # Handle URL drops
    elif text:
        urls = re.findall(r'https?://[^\s]+', text)
        if urls:
            await handle_url_drop(chat_id, urls[0])
        else:
            await handle_start(chat_id)

    # Handle PDF drops
    elif document:
        filename = document.get("file_name", "document.pdf")
        file_id = document.get("file_id")
        if filename.lower().endswith(".pdf"):
            await handle_pdf_drop(chat_id, file_id, filename)
        else:
            await _send(chat_id, "Only PDF files are supported for now.")