# recommendation_service.py: Safety-gated recommendations.
# Part of LeoBook API — Services

"""
Recommendations service — applies Project Stairway safety gate
server-side before returning results to Flutter.

Rules enforced:
  - Per-leg: odds 1.20–3.99, confidence ≥ 70%
  - Accumulator: total odds 3.50–5.00, max 4 legs
  - Stake: ₦1,000 fixed
  - Priority: confidence DESC (probability-first)
"""

import logging
from typing import List, Dict, Any, Optional

from api.services.prediction_service import _get_client, _cache_get, _cache_set
from api.config import get_settings

# Import safety gate from Core (reuse existing logic)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from Core.Safety.safety_gate import (
    is_stairway_safe,
    validate_accumulator,
    get_stairway_stake,
    filter_and_rank_candidates,
    SINGLE_ODDS_MIN,
    SINGLE_ODDS_MAX,
    MIN_CONFIDENCE_PCT,
    ACCA_MAX_LEGS,
    FIXED_STAKE,
)

logger = logging.getLogger(__name__)


def fetch_recommendations(
    date: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, Any]:
    """
    Fetch predictions with recommendation_score > 0,
    then apply safety gate server-side.
    """
    settings = get_settings()
    cache_key = f"recommendations:{date}:{limit}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    client = _get_client()
    query = (
        client.table("predictions")
        .select("*")
        .gt("recommendation_score", 0)
        .order("recommendation_score", desc=True)
    )
    if date:
        query = query.eq("date", date)

    query = query.limit(limit * 3)  # Fetch extra since safety gate will filter
    response = query.execute()
    raw = response.data or []

    # ── Apply safety gate ────────────────────────────────────────
    safe = []
    rejected = []
    for rec in raw:
        # Build bet dict for safety gate
        bet = {
            "odds": rec.get("booking_odds") or rec.get("odds") or 0,
            "confidence": rec.get("market_reliability") or rec.get("confidence") or 0,
            "fixture_id": rec.get("fixture_id", ""),
            "home_team": rec.get("home_team", ""),
            "away_team": rec.get("away_team", ""),
        }
        ok, reason = is_stairway_safe(bet)
        if ok:
            rec["safety_status"] = "PASS"
            safe.append(rec)
        else:
            rec["safety_status"] = "REJECTED"
            rec["safety_reason"] = reason
            rejected.append(rec)

    # Sort safe by confidence DESC (probability-first)
    safe.sort(
        key=lambda x: float(x.get("market_reliability") or x.get("recommendation_score") or 0),
        reverse=True,
    )

    # Cap at limit
    safe = safe[:limit]

    result = {
        "data": safe,
        "total_fetched": len(raw),
        "passed_safety": len(safe),
        "rejected_safety": len(rejected),
        "safety_rules": {
            "odds_range": f"{SINGLE_ODDS_MIN}–{SINGLE_ODDS_MAX}",
            "min_confidence_pct": MIN_CONFIDENCE_PCT,
            "max_acca_legs": ACCA_MAX_LEGS,
            "fixed_stake": FIXED_STAKE,
            "priority": "confidence_desc",
        },
    }
    _cache_set(cache_key, result, settings.PREDICTIONS_CACHE_TTL)
    return result


def build_stairway_status(balance: float = 50000) -> Dict[str, Any]:
    """Return current Stairway state for the Flutter UI."""
    stake = get_stairway_stake(balance)
    return {
        "current_stake": stake,
        "balance": balance,
        "odds_range": f"{SINGLE_ODDS_MIN}–{SINGLE_ODDS_MAX}",
        "min_confidence_pct": MIN_CONFIDENCE_PCT,
        "max_legs": ACCA_MAX_LEGS,
        "priority": "probability_first",
        "safety_gate_active": True,
    }
