# config.py: FastAPI settings — reads from .env (same as Leo.py)
# Part of LeoBook API

import os
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Central configuration for FastAPI backend."""

    # ── Supabase ─────────────────────────────────────────────────
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_SERVICE_KEY: str = os.getenv("SUPABASE_SERVICE_KEY", "")
    SUPABASE_JWT_SECRET: str = os.getenv("SUPABASE_JWT_SECRET", "")

    # ── Server ───────────────────────────────────────────────────
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    DEBUG: bool = os.getenv("API_DEBUG", "false").lower() == "true"

    # ── CORS (Flutter origins) ───────────────────────────────────
    CORS_ORIGINS: list = [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost:5000",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8080",
        "*",  # Tighten in production
    ]

    # ── Cache TTL (seconds) ──────────────────────────────────────
    PREDICTIONS_CACHE_TTL: int = 120   # 2 minutes
    STANDINGS_CACHE_TTL: int = 300     # 5 minutes
    LEAGUES_CACHE_TTL: int = 600       # 10 minutes

    # ── Rate limiting ────────────────────────────────────────────
    RATE_LIMIT_PER_MINUTE: int = 60


@lru_cache()
def get_settings() -> Settings:
    return Settings()
