# standings.py: GET /standings/{league} endpoint.
# Part of LeoBook API — Routes

from fastapi import APIRouter, Depends, Request

from api.auth.jwt_handler import get_current_user
from api.middleware.rate_limiter import rate_limiter
from api.services.prediction_service import fetch_standings

router = APIRouter(prefix="/standings", tags=["Standings"])


@router.get("/{league}")
async def get_standings(
    league: str,
    request: Request,
    user=Depends(get_current_user),
):
    """
    Get league standings with team crests.

    - **league**: League name or partial match (e.g. "ENGLAND: Premier League")

    Returns cached data (5-min TTL). Auth optional.
    """
    rate_limiter.check(request)
    data = fetch_standings(league)
    return {"league": league, "data": data, "count": len(data)}
