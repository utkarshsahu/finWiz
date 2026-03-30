import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    from db import init_db, close_db
    # Run DB init in the background so the web server stays 'Ready'
    db_task = asyncio.create_task(init_db())
    
    def on_db_complete(task):
        try:
            task.result()
            logging.info("✓ Database and Beanie fully initialized")
        except Exception as e:
            logging.error(f"✗ Database initialization failed: {e}")

    db_task.add_done_callback(on_db_complete)
    yield
    db_task.cancel()
    await close_db()

app = FastAPI(
    title="Finance Agent API",
    description="Personal financial intelligence platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Standard Health Checks ---
@app.get("/health", tags=["system"])
async def health():
    return {"status": "ok"}

@app.get("/")
async def root():
    return {"message": "Finance Agent API is Live", "docs": "/docs"}

# --- Include All Routers ---
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