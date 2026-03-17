# predictions.py: GET /predictions endpoint.
# Part of LeoBook API — Routes

from fastapi import APIRouter, Query, Depends, Request
from typing import Optional

from api.auth.jwt_handler import get_current_user
from api.middleware.rate_limiter import rate_limiter
from api.services.prediction_service import fetch_predictions

router = APIRouter(prefix="/predictions", tags=["Predictions"])


@router.get("")
async def get_predictions(
    request: Request,
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Results per page"),
    user=Depends(get_current_user),
):
    """
    Get predictions with pagination.

    - **date**: Optional date filter (YYYY-MM-DD format)
    - **page**: Page number (default: 1)
    - **page_size**: Results per page (default: 50, max: 200)

    Returns cached data (2-min TTL). Auth optional.
    """
    rate_limiter.check(request)
    return fetch_predictions(date=date, page=page, page_size=page_size)
