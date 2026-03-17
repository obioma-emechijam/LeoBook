# recommendations.py: GET /recommendations endpoint (safety-gated).
# Part of LeoBook API — Routes

from fastapi import APIRouter, Query, Depends, Request
from typing import Optional

from api.auth.jwt_handler import get_current_user
from api.middleware.rate_limiter import rate_limiter
from api.services.recommendation_service import fetch_recommendations, build_stairway_status

router = APIRouter(prefix="/recommendations", tags=["Recommendations"])


@router.get("")
async def get_recommendations(
    request: Request,
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    limit: int = Query(20, ge=1, le=50, description="Max results"),
    user=Depends(get_current_user),
):
    """
    Get safety-gated recommendations.

    All results have passed the Project Stairway safety gate:
    - Odds: 1.20 ≤ per leg < 4.00
    - Confidence: ≥ 70%
    - Priority: highest confidence first (NOT highest EV)

    Response includes safety rule metadata.
    """
    rate_limiter.check(request)
    return fetch_recommendations(date=date, limit=limit)


@router.get("/stairway")
async def get_stairway_status(
    request: Request,
    balance: float = Query(50000, description="Current account balance"),
    user=Depends(get_current_user),
):
    """
    Get current Stairway betting state.

    Returns stake amount, safety rules, and gate status.
    """
    rate_limiter.check(request)
    return build_stairway_status(balance=balance)
