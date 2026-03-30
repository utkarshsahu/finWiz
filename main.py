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

# 1. Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

# 2. Define Lifespan (Heavy lifting)
import asyncio
import logging

@asynccontextmanager
async def lifespan(app: FastAPI):
    from db import init_db, close_db
    
    # We create a task so FastAPI can finish starting up 
    # and stay 'Ready' while the DB finishes its handshake.
    db_init_task = asyncio.create_task(init_db())
    
    def check_db_status(task):
        try:
            task.result()
            logging.info("!!! DB Initialization Task Completed Successfully !!!")
        except Exception as e:
            logging.error(f"!!! DB Initialization Task Failed: {e} !!!")

    db_init_task.add_done_callback(check_db_status)

    yield
    
    # Clean shutdown
    db_init_task.cancel()
    await close_db()

# 3. Initialize FastAPI
app = FastAPI(
    title="Finance Agent API",
    description="Personal financial intelligence platform",
    version="0.1.0",
    lifespan=lifespan,
)

# 4. Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=False, 
    allow_methods=["*"],
    allow_headers=["*"],
)

# 5. Fast-Responding Routes (Crucial for Health Checks)
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

# 6. Routers (Uncommented and imported)
from routes.zerodha_router import router as zerodha_router
from routes.market_data_router import router as market_data_router
from routes.documents_router import router as documents_router
from routes.analytics_router import router as analytics_router
from routes.rules_router import router as rules_router
from routes.research_router import router as research_router
from routes.recommendations_router import router as recommendations_router
from routes.telegram_router import router as telegram_router

app.include_router(zerodha_router)
app.include_router(market_data_router)
app.include_router(documents_router)
app.include_router(analytics_router)
app.include_router(rules_router)
app.include_router(research_router)
app.include_router(recommendations_router)
app.include_router(telegram_router)