# ensemble.py: Neuro-Symbolic Ensemble Engine for LeoBook.
# Part of LeoBook Core — Intelligence
#
# Classes: EnsembleEngine

"""
Neuro-Symbolic Ensemble Engine
Merges Rule Engine (Symbolic) and RL (Neural) predictions using weighted averaging.
Supports per-league weighting and fallback logic for low-confidence neural outputs.
"""

import json
import os
import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class EnsembleEngine:
    """
    Neuro-Symbolic Ensemble Engine
    Merges Rule Engine (Symbolic) and RL (Neural) predictions with weighted averaging.
    """

    _weights_path = os.path.join(os.path.dirname(__file__), '..', '..', 'Config', 'ensemble_weights.json')
    _weights = None

    # Module-level richness cache: league_id -> data_richness_score
    # Loaded once at first call, refreshed every 6 hours.
    _richness_cache: Dict[str, float] = {}
    _richness_loaded_at: float = 0.0
    _RICHNESS_TTL: float = 6 * 3600  # 6 hours

    @classmethod
    def _load_weights(cls):
        """Lazy loader for ensemble weights."""
        if cls._weights is not None:
            return cls._weights
        try:
            if os.path.exists(cls._weights_path):
                with open(cls._weights_path, 'r', encoding='utf-8') as f:
                    cls._weights = json.load(f)
            else:
                cls._weights = {"default": {"W_symbolic": 0.7, "W_neural": 0.3}, "leagues": {}}
        except Exception as e:
            logger.error(f"[Ensemble] Failed to load weights: {e}")
            cls._weights = {"default": {"W_symbolic": 0.7, "W_neural": 0.3}, "leagues": {}}
        return cls._weights

    @classmethod
    def merge(cls, rule_logits: Dict[str, float], rule_conf: float,
              rl_logits: Optional[Dict[str, float]], rl_conf: Optional[float],
              league_id: str,
              data_richness_score: float = 1.0) -> Dict[str, Any]:
        """
        Merge symbolic and neural outputs.

        Args:
            rule_logits:         Dict with {'home': score, 'draw': score, 'away': score}
            rule_conf:           Confidence (0.0 - 1.0) from Rule Engine
            rl_logits:           Dict with {'home_win': prob, 'draw': prob, 'away_win': prob} or None
            rl_conf:             Confidence (0.0 - 1.0) from RL Engine or None
            league_id:           League ID for per-league weighting
            data_richness_score: [0.0, 1.0] — scales W_neural.
                                 0.0 = no prior season data (pure Rule Engine).
                                 1.0 = 3+ prior seasons (full RL weight).
        """
        weights_data = cls._load_weights()
        league_weights = weights_data.get("leagues", {}).get(league_id, weights_data["default"])

        w_s_base = league_weights.get("W_symbolic", 0.7)
        w_n_base = league_weights.get("W_neural", 0.3)

        # Scale W_neural by data_richness_score [0.0, 1.0].
        # With 0 prior seasons: W_neural = 0.0 (pure Rule Engine, no RL history).
        # With 3+ prior seasons: W_neural = full configured weight.
        # Remaining weight redistributed to W_symbolic to always sum to 1.0.
        w_n = round(w_n_base * max(0.0, min(1.0, data_richness_score)), 4)
        w_s = round(1.0 - w_n, 4)

        # Fallback to symbolic if RL confidence is too low or RL failed
        if rl_logits is None or rl_conf is None or rl_conf < 0.3:
            path = "symbolic_fallback"
            reason = "RL failed" if rl_logits is None else f"low confidence ({rl_conf:.2f} < 0.3)"

            logger.debug(
                "[Ensemble] %s | Path: %s | Reason: %s | richness: %.2f",
                league_id, path, reason, data_richness_score
            )
            
            # Normalize rule_logits for consistency
            total = sum(rule_logits.values()) or 1.0
            norm_logits = {k: v / total for k, v in rule_logits.items()}
            
            return {
                "logits": norm_logits,
                "confidence": rule_conf,
                "path": path,
                "weights": {"W_symbolic": 1.0, "W_neural": 0.0}
            }

        # Ensemble path
        path = "ensemble"
        
        # Mapping RL action probs to consistent keys
        rl_1x2 = {
            "home": rl_logits.get("home_win", 0.33),
            "draw": rl_logits.get("draw", 0.34),
            "away": rl_logits.get("away_win", 0.33)
        }
        
        # Normalize Rule Logits
        s_total = sum(rule_logits.values()) or 1.0
        s_1x2 = {k: v / s_total for k, v in rule_logits.items()}

        # Weighted Merge
        final_1x2 = {
            "home": (s_1x2["home"] * w_s) + (rl_1x2["home"] * w_n),
            "draw": (s_1x2["draw"] * w_s) + (rl_1x2["draw"] * w_n),
            "away": (s_1x2["away"] * w_s) + (rl_1x2["away"] * w_n)
        }
        
        # Normalize final logits to ensure they sum to 1.0
        f_total = sum(final_1x2.values()) or 1.0
        final_1x2 = {k: v / f_total for k, v in final_1x2.items()}

        final_conf = (rule_conf * w_s) + (rl_conf * w_n)
        
        logger.info(
            "[Ensemble] %s | Path: %s | RL Conf: %.2f | richness: %.2f | s:%.2f n:%.2f",
            league_id, path, rl_conf, data_richness_score, w_s, w_n
        )
        
        return {
            "logits": final_1x2,
            "confidence": final_conf,
            "path": path,
            "weights": {"W_symbolic": w_s, "W_neural": w_n}
        }

    @classmethod
    def get_richness_score(cls, league_id: str, current_season: str = "") -> float:
        """Return data_richness_score for a league from the cached table.

        Loads all scores on first call (one DB query for all leagues),
        then serves from memory. Refreshes every 6 hours.

        Returns 1.0 as default if the league is not found — this means
        leagues with no completeness data are treated as data-rich (safe
        assumption for established leagues whose data pre-dates the tracker).
        Returns 0.0 explicitly only when the league IS tracked and has no
        prior seasons.
        """
        now = time.monotonic()

        if not cls._richness_cache or (now - cls._richness_loaded_at) > cls._RICHNESS_TTL:
            cls._load_richness_cache()
            cls._richness_loaded_at = now

        return cls._richness_cache.get(league_id, 1.0)

    @classmethod
    def _load_richness_cache(cls) -> None:
        """Bulk-load data_richness_scores for all tracked leagues in one query."""
        try:
            from Data.Access.league_db import get_connection
            from Data.Access.season_completeness import SeasonCompletenessTracker
            conn = get_connection()
            SeasonCompletenessTracker._ensure_table()

            # Get all leagues tracked in season_completeness
            rows = conn.execute("""
                SELECT sc.league_id, l.current_season
                FROM (SELECT DISTINCT league_id FROM season_completeness) sc
                LEFT JOIN leagues l ON l.league_id = sc.league_id
            """).fetchall()

            new_cache: Dict[str, float] = {}
            for row in rows:
                lid = row[0] if not hasattr(row, "keys") else row["league_id"]
                cur_season = (
                    (row[1] if not hasattr(row, "keys") else row["current_season"]) or ""
                )
                new_cache[lid] = SeasonCompletenessTracker.get_data_richness_score(
                    lid, cur_season, conn=conn
                )

            cls._richness_cache = new_cache
            logger.debug("[Ensemble] Richness cache loaded: %d leagues", len(new_cache))

        except Exception as e:
            logger.warning("[Ensemble] Failed to load richness cache: %s", e)
            # Keep existing cache rather than clearing it


# ── 30-dim RL output → structured recommendation ─────────────

def rl_action_to_recommendation(
    action_idx: int,
    model_probs: list,
    live_odds: Optional[Dict[str, float]] = None,
    rl_ev: Optional[float] = None,
) -> Optional[Dict]:
    """
    Convert 30-dim RL output to a structured recommendation.
    Applies stairway gate with live odds if available.
    Returns None if action is no_bet or fails gate.

    Args:
        action_idx:   Index of the selected action in ACTIONS (0–29).
        model_probs:  Raw softmax probabilities over all 30 actions.
                      These represent action *preference*, not outcome win probability.
        live_odds:    Dict of {market_key: decimal_odds} from the live book (optional).
        rl_ev:        Expected value from the model's value head (optional).
                      When provided, the calibrated true win probability is derived as:
                          true_prob = (rl_ev + 1.0) / odds
                      and used for gate evaluation and EV computation instead of the
                      raw softmax action probability (~1/30 ≈ 3.3%), which is too low
                      to ever pass an EV > 0 gate regardless of model quality.
                      Falls back to model_probs[action_idx] if rl_ev is None or odds
                      are unavailable (backward-compatible).
    """
    from Core.Intelligence.rl.market_space import (
        ACTIONS, N_ACTIONS, stairway_gate, SYNTHETIC_ODDS
    )

    if action_idx >= N_ACTIONS:
        return None

    action = ACTIONS[action_idx]
    key    = action["key"]

    if key == "no_bet":
        return None

    # Raw softmax action preference — used as fallback only.
    # This is NOT the win probability for the outcome; it is the model's
    # relative preference for this market across the 30-dim action space.
    model_prob = model_probs[action_idx] if action_idx < len(model_probs) else 0.0

    live_odds_val = (live_odds or {}).get(key)
    fair_odds_val = SYNTHETIC_ODDS.get(key)
    odds_to_use   = live_odds_val or fair_odds_val or 0.0

    # ── Calibrated probability derivation ────────────────────────────────
    # When the value head EV is available, back-calculate the true win
    # probability the model has estimated for this outcome:
    #   EV = true_prob * odds - 1  →  true_prob = (EV + 1) / odds
    #
    # This corrects the gate logic, which previously used the ~3.3% softmax
    # action score and always produced EV ≈ -0.90, causing every selection
    # to fail the EV > 0 threshold regardless of actual model confidence.
    if rl_ev is not None and odds_to_use > 0.0:
        true_prob = (rl_ev + 1.0) / odds_to_use
        true_prob = max(0.0, min(1.0, true_prob))  # clamp to [0, 1]
    else:
        # Fallback: use softmax action probability (backward-compatible).
        true_prob = model_prob

    bettable, reason = stairway_gate(key, live_odds_val, true_prob)
    if not bettable:
        return None

    ev = (true_prob * odds_to_use) - 1.0 if odds_to_use > 0 else None

    return {
        "market_key":     key,
        "market_name":    action["market"],
        "outcome":        action["outcome"],
        "line":           action["line"],
        "market_id":      action["market_id"],
        "model_prob":     round(true_prob, 4),
        "raw_action_prob": round(model_prob, 4),  # preserved for diagnostics
        "live_odds":      live_odds_val,
        "fair_odds":      fair_odds_val,
        "is_value_bet":   (ev is not None and ev > 0),
        "ev":             round(ev, 4) if ev is not None else None,
        "likelihood_pct": action["likelihood"],
    }




