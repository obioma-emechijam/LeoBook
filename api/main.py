# main.py: FastAPI application factory.
# Part of LeoBook API
#
# Run: uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

"""
LeoBook FastAPI Backend — Option C Hybrid MVP

Serves Flutter-facing endpoints while Leo.py handles orchestration.
Zero changes to existing Leo.py / sync_manager / pipeline.

Endpoints:
  GET  /health              — Server status
  GET  /predictions          — Paginated predictions
  GET  /recommendations      — Safety-gated recommendations
  GET  /recommendations/stairway — Stairway betting state
  GET  /standings/{league}   — League standings with crests
  GET  /leagues              — All leagues
  WS   /ws/live              — Live score streaming
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import get_settings
from api.routes import predictions, recommendations, standings, leagues, websocket
from api.services.live_service import live_manager

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("leobook.api")


# ── Lifespan (startup / shutdown) ────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("LeoBook API starting up...")
    settings = get_settings()
    if not settings.SUPABASE_URL:
        logger.warning("SUPABASE_URL not set — endpoints will fail")
    if not settings.SUPABASE_JWT_SECRET:
        logger.warning("SUPABASE_JWT_SECRET not set — auth will fail")

    # Start live score polling
    await live_manager.start_polling(interval=30)
    logger.info("Live score polling started (30s interval)")

    yield

    # Shutdown
    await live_manager.stop_polling()
    logger.info("LeoBook API shut down.")


# ── App factory ──────────────────────────────────────────────────

app = FastAPI(
    title="LeoBook API",
    description="Hybrid backend for LeoBook Flutter app. "
                "Serves predictions, recommendations, standings, and live scores. "
                "Project Stairway safety rules enforced server-side.",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ─────────────────────────────────────────────────────────
settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Register routes ──────────────────────────────────────────────
app.include_router(predictions.router)
app.include_router(recommendations.router)
app.include_router(standings.router)
app.include_router(leagues.router)
app.include_router(websocket.router)


# ── Health check ─────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    """Server health check."""
    return {
        "status": "ok",
        "service": "LeoBook API",
        "version": "1.0.0",
        "safety_gate": "active",
        "leo_integration": "hybrid (Option C)",
    }
