# leagues.py: GET /leagues endpoint.
# Part of LeoBook API — Routes

from fastapi import APIRouter, Depends, Request

from api.auth.jwt_handler import get_current_user
from api.middleware.rate_limiter import rate_limiter
from api.services.prediction_service import fetch_leagues

router = APIRouter(prefix="/leagues", tags=["Leagues"])


@router.get("")
async def get_leagues(
    request: Request,
    user=Depends(get_current_user),
):
    """
    Get all available leagues.

    Returns cached data (10-min TTL). Auth optional.
    """
    rate_limiter.check(request)
    data = fetch_leagues()
    return {"data": data, "count": len(data)}
