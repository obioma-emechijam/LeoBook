# prediction_service.py: Supabase query layer for predictions + standings + leagues.
# Part of LeoBook API — Services

"""
Read-only Supabase query service.
All writes happen through Leo.py / sync_manager — this is read-only.
In-memory cache with TTL for performance.
"""

import time
import logging
from typing import List, Dict, Optional, Any
from supabase import create_client, Client

from api.config import get_settings

logger = logging.getLogger(__name__)

# ── In-memory cache ──────────────────────────────────────────────

_cache: Dict[str, Dict] = {}  # key -> {"data": ..., "expires": float}


def _cache_get(key: str) -> Optional[Any]:
    entry = _cache.get(key)
    if entry and time.time() < entry["expires"]:
        return entry["data"]
    if entry:
        del _cache[key]
    return None


def _cache_set(key: str, data: Any, ttl: int):
    _cache[key] = {"data": data, "expires": time.time() + ttl}


def invalidate_cache(prefix: str = ""):
    """Clear cache entries matching prefix (or all if empty)."""
    if not prefix:
        _cache.clear()
        return
    keys_to_del = [k for k in _cache if k.startswith(prefix)]
    for k in keys_to_del:
        del _cache[k]


# ── Supabase client (read-only, service key) ─────────────────────

_client: Optional[Client] = None


def _get_client() -> Client:
    global _client
    if _client is None:
        settings = get_settings()
        if not settings.SUPABASE_URL or not settings.SUPABASE_SERVICE_KEY:
            raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_KEY not configured")
        _client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_KEY)
    return _client


# ── Predictions ──────────────────────────────────────────────────

def fetch_predictions(
    date: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> Dict[str, Any]:
    """Fetch predictions with pagination and optional date filter."""
    settings = get_settings()
    cache_key = f"predictions:{date}:{page}:{page_size}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    client = _get_client()
    query = client.table("predictions").select("*", count="exact")

    if date:
        query = query.eq("date", date)

    query = query.order("date", desc=True).order(
        "recommendation_score", desc=True
    )

    # Pagination
    offset = (page - 1) * page_size
    query = query.range(offset, offset + page_size - 1)

    response = query.execute()
    result = {
        "data": response.data or [],
        "count": response.count or 0,
        "page": page,
        "page_size": page_size,
    }
    _cache_set(cache_key, result, settings.PREDICTIONS_CACHE_TTL)
    return result


# ── Standings ────────────────────────────────────────────────────

def fetch_standings(league: str) -> List[Dict]:
    """Fetch standings for a league, enriched with team crests."""
    settings = get_settings()
    cache_key = f"standings:{league}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    client = _get_client()

    # Try exact match
    response = (
        client.table("standings")
        .select("*")
        .eq("region_league", league)
        .order("position", desc=False)
        .execute()
    )
    rows = response.data or []

    # Fallback: ILIKE
    if not rows and league:
        response = (
            client.table("standings")
            .select("*")
            .ilike("region_league", f"%{league}%")
            .order("position", desc=False)
            .execute()
        )
        rows = response.data or []

    # Enrich with crests
    if rows:
        team_names = list({r.get("team_name", "") for r in rows if r.get("team_name")})
        if team_names:
            try:
                teams_resp = (
                    client.table("teams")
                    .select("name, crest")
                    .in_("name", team_names)
                    .execute()
                )
                crest_map = {
                    t["name"]: t["crest"]
                    for t in (teams_resp.data or [])
                    if t.get("name") and t.get("crest")
                }
                for row in rows:
                    name = row.get("team_name", "")
                    if name in crest_map and not row.get("team_crest_url"):
                        row["team_crest_url"] = crest_map[name]
            except Exception as e:
                logger.warning(f"Could not enrich standings crests: {e}")

    _cache_set(cache_key, rows, settings.STANDINGS_CACHE_TTL)
    return rows


# ── Leagues ──────────────────────────────────────────────────────

def fetch_leagues() -> List[Dict]:
    """Fetch all leagues."""
    settings = get_settings()
    cache_key = "leagues:all"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    client = _get_client()
    response = (
        client.table("leagues")
        .select(
            "league_id, fs_league_id, name, crest, continent, region, "
            "region_flag, current_season, country_code, url"
        )
        .order("name", desc=False)
        .execute()
    )
    rows = response.data or []
    _cache_set(cache_key, rows, settings.LEAGUES_CACHE_TTL)
    return rows
