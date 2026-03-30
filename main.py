"""
app/main.py

FastAPI application entry point.

Run with:
    uvicorn main:app --reload --port 8000
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup: init DB connection inside the running event loop.
    Shutdown: close the motor client cleanly.

    The import of init_db is inside lifespan (not at module top level)
    so the AsyncIOMotorClient is only created after the event loop starts.
    """
    from db import init_db, close_db
    await init_db()
    yield
    await close_db()


app = FastAPI(
    title="Finance Agent API",
    description="Personal financial intelligence platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows any domain to hit your API
    allow_credentials=False, # Must be False if using "*"
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

from routes.zerodha_router import router as zerodha_router
from routes.market_data_router import router as market_data_router
from routes.documents_router import router as documents_router
from routes.analytics_router import router as analytics_router
from routes.rules_router import router as rules_router
from routes.research_router import router as research_router
from routes.recommendations_router import router as recommendations_router
from routes.telegram_router import router as telegram_router

"""
app.include_router(zerodha_router)
app.include_router(market_data_router)
app.include_router(documents_router)
app.include_router(analytics_router)
app.include_router(rules_router)
app.include_router(research_router)
app.include_router(recommendations_router)
app.include_router(telegram_router)
"""

@app.get("/health", tags=["system"])
async def health():
    return {"status": "ok"}


@app.get("/")
async def root():
    return {
        "message": "Finance Agent API",
        "docs": "/docs",
        "zerodha_status": "/zerodha/status",
    }