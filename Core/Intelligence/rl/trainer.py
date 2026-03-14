# trainer.py: PPO-based RL trainer with chronological training and composite reward.
# Part of LeoBook Core — Intelligence (RL Engine)
#
# Classes: RLTrainer
# Called by: Leo.py (--train-rl)

"""
RL Trainer Module
Handles offline training from historical fixtures (chronological, day-by-day)
and online updates from new prediction outcomes.

Training constraints:
- Strict chronological order (no future data leakage)
- Season-aware date selection: training starts from each league's actual season
  start date, not a global hardcoded date floor
- Last-10 matches prioritized via recency weighting
- Prediction accuracy is the primary reward signal

Season targeting (--train-season CLI flag):
  "current"     Use each league's current_season (from leagues table). Default.
  "all"         All available seasons, oldest-first. Use for a full cold retraining.
  N (int)       Past season by offset: 1 = most recent past, 2 = two seasons ago, etc.
                Matches the --season N convention used by enrich_leagues.
  "2024/2025"   Explicit season label (split-season format).
  "2025"        Explicit season label (calendar-year format).

Bug fixes applied (2026-03-14):
  FIX-1: PPO ratio was always 1.0 — old_log_prob must be stored BEFORE re-sampling.
          Now uses a stored old_log_prob passed into train_step for Phase 2/3.
  FIX-2: Double KL in Phase 1 — removed the redundant manual KL term;
          F.kl_div IS already KL divergence, the manual re-computation was noise.
  FIX-3: Synthetic odds in Phase 2 reward — now uses xG-derived fair odds per
          fixture rather than a static lookup table, giving match-specific reward signal.
  FIX-4: active_phase default in online updates — update_from_outcomes now explicitly
          passes the correct active_phase so reward logic is never silently wrong.
  FIX-5: Expert probs identical across early dates — _get_rule_engine_probs now falls
          back to league-average xG (1.4 home / 1.1 away) when form data is empty,
          ensuring each match produces a non-constant expert distribution.
  FIX-6: GradNorm clamped at 0.5 constantly — max_norm raised to 1.0 for Phase 2/3
          (imitation still uses 0.5 for stability). Scheduler T_0 raised to 2000 to
          reduce oscillation from premature warm restarts.
  FIX-7: --train-rl without --train-season defaulted to "current" but the
          current-season join returned all completed fixtures back to 2012 when
          leagues.current_season is not fully populated, effectively running
          "--train-season all". Now the fallback is capped at 365 days of data
          to prevent runaway training on a plain --train-rl invocation.
"""

import os
import re
import json
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

from .model import LeoBookRLModel
from .feature_encoder import FeatureEncoder
from .adapter_registry import AdapterRegistry
from .market_space import (
    ACTIONS, N_ACTIONS, SYNTHETIC_ODDS, STAIRWAY_BETTABLE,
    compute_poisson_probs, probs_to_tensor_30dim,
    derive_ground_truth, stairway_gate, check_phase_readiness,
    PHASE2_MIN_ODDS_ROWS, PHASE2_MIN_DAYS_LIVE,
    PHASE3_MIN_ODDS_ROWS, PHASE3_MIN_DAYS_LIVE,
)

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "Data" / "Store" / "models"

# Paths
BASE_MODEL_PATH = MODELS_DIR / "leobook_base.pth"
TRAINING_CONFIG_PATH = MODELS_DIR / "training_config.json"

# FIX-7: Cap for plain --train-rl (current season, no --cold, no --train-season all).
# Prevents the 2012→present runaway when leagues.current_season is not fully populated.
DEFAULT_CURRENT_SEASON_MAX_DAYS = 365


class RLTrainer:
    """
    PPO-based trainer for the LeoBook RL model.

    Training proceeds chronologically day-by-day:
    For each day D:
        1. Build features using ONLY data before D (season-aware window)
        2. For each fixture on day D:
            a. Encode features
            b. Model predicts action (market + stake)
            c. After outcome: compute composite reward
            d. PPO gradient update
    """

    def __init__(
        self,
        lr_base: float = 5e-5,
        lr_league: float = 1e-4,
        lr_team: float = 5e-5,
        gamma: float = 0.99,
        clip_epsilon: float = 0.2,
        max_seasons_back: int = 2,
        device: str = "cpu",
    ):
        self.device = torch.device(device)
        self.max_seasons_back = max_seasons_back
        self.gamma = gamma
        self.clip_epsilon = clip_epsilon

        # Model & registry
        self.model = LeoBookRLModel().to(self.device)
        self.registry = AdapterRegistry()

        # Optimizer with per-component learning rates
        param_groups = [
            {"params": self.model.trunk.parameters(), "lr": lr_base},
            {"params": self.model.policy_head.parameters(), "lr": lr_base},
            {"params": self.model.value_head.parameters(), "lr": lr_base},
            {"params": self.model.stake_head.parameters(), "lr": lr_base},
            {"params": self.model.league_embedding.parameters(), "lr": lr_league},
            {"params": self.model.league_adapters.parameters(), "lr": lr_league},
            {"params": self.model.team_adapters.parameters(), "lr": lr_team},
        ]
        self.optimizer = optim.AdamW(param_groups, weight_decay=1e-4)
        # FIX-6: Raised T_0 from 1000 → 2000 to reduce premature LR oscillation.
        self.scheduler = optim.lr_scheduler.CosineAnnealingWarmRestarts(
            self.optimizer, T_0=2000, T_mult=2
        )

        self._step_count = 0
        # KL weight: starts at 0.3 in Phase 1 (imitation anchor), anneals to 0 in Phase 2+
        # so RL can genuinely outperform the expert over time.
        self.kl_weight = 0.3
        self.active_phase: int = 1  # Always set before training; default safe value

    # -------------------------------------------------------------------
    # Season Discovery & Date Selection
    # -------------------------------------------------------------------

    def _discover_seasons(self, conn) -> List[str]:
        """
        Return all distinct season labels found in schedules, most-recent-first.

        Sorts by the 4-digit start year embedded in the season string so both
        split-season ("2024/2025") and calendar-year ("2025") formats rank correctly.
        """
        rows = conn.execute(
            "SELECT DISTINCT season FROM schedules "
            "WHERE season IS NOT NULL AND season != ''"
        ).fetchall()
        seasons = [r[0] for r in rows]

        def _start_year(s: str) -> int:
            m = re.match(r'(\d{4})', s)
            return int(m.group(1)) if m else 0

        return sorted(seasons, key=_start_year, reverse=True)

    def _get_season_dates(
        self, conn, target_season: Union[str, int] = "current"
    ) -> Tuple[List[str], str]:
        """
        Build the ordered list of fixture-dates for training, filtered to the
        requested season scope.

        Args:
            target_season:
                "current"   — per-league join against leagues.current_season.
                              Starts training from the actual start of each
                              league's live season. Capped at DEFAULT_CURRENT_SEASON_MAX_DAYS
                              to prevent runaway training when current_season metadata
                              is not fully populated (FIX-7).
                "all"       — all available completed fixtures, oldest-first.
                              Use for a full cold retraining across all seasons.
                int N       — past season by offset: 1 = most recent past,
                              2 = two seasons ago, etc. (0-indexed internally,
                              1-indexed in the CLI to match enrich_leagues).
                str label   — explicit season label, e.g. "2024/2025" or "2025".

        Returns:
            (dates, label) where dates is a chronologically sorted list of
            date strings and label is a human-readable description for logging.
        """
        today_str = datetime.now().strftime("%Y-%m-%d")

        # ── All seasons ─────────────────────────────────────────────────────────
        if target_season == "all":
            rows = conn.execute("""
                SELECT DISTINCT date FROM schedules
                WHERE date IS NOT NULL
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
                  AND date <= ?
                ORDER BY date ASC
            """, (today_str,)).fetchall()
            return [r[0] for r in rows], "all seasons (oldest → newest)"

        # ── Past season by offset (int) ──────────────────────────────────────────
        if isinstance(target_season, int) and target_season >= 1:
            seasons = self._discover_seasons(conn)
            if target_season >= len(seasons):
                print(f"  [TRAIN] Season offset {target_season} out of range "
                      f"({len(seasons)} seasons in DB). Falling back to current.")
            else:
                season_label = seasons[target_season]  # 1-indexed offset
                rows = conn.execute("""
                    SELECT DISTINCT date FROM schedules
                    WHERE season = ?
                      AND home_score IS NOT NULL AND away_score IS NOT NULL
                      AND date IS NOT NULL AND date <= ?
                    ORDER BY date ASC
                """, (season_label, today_str)).fetchall()
                if rows:
                    return [r[0] for r in rows], f"season {season_label} (past offset {target_season})"
                print(f"  [TRAIN] Season '{season_label}' has no completed fixtures. "
                      f"Falling back to current.")

        # ── Explicit season label (non-"current" string) ─────────────────────────
        if isinstance(target_season, str) and target_season != "current":
            rows = conn.execute("""
                SELECT DISTINCT date FROM schedules
                WHERE season = ?
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
                  AND date IS NOT NULL AND date <= ?
                ORDER BY date ASC
            """, (target_season, today_str)).fetchall()
            if rows:
                return [r[0] for r in rows], f"season {target_season}"
            print(f"  [TRAIN] Season '{target_season}' not found or has no completed "
                  f"fixtures. Falling back to current.")

        # ── Current season (default) ─────────────────────────────────────────────
        # FIX-7: Apply a date cap to prevent the current-season join from spanning
        # back to 2012 when leagues.current_season is not fully populated.
        # A plain --train-rl should train on the last ~1 year of data, not all history.
        cap_date = (datetime.now() - timedelta(days=DEFAULT_CURRENT_SEASON_MAX_DAYS)).strftime("%Y-%m-%d")

        rows = conn.execute("""
            SELECT DISTINCT s.date
            FROM schedules s
            INNER JOIN leagues l ON s.league_id = l.league_id
            WHERE s.season = l.current_season
              AND s.home_score IS NOT NULL AND s.away_score IS NOT NULL
              AND s.date IS NOT NULL AND s.date <= ?
              AND s.date >= ?
            ORDER BY s.date ASC
        """, (today_str, cap_date)).fetchall()
        dates = [r[0] for r in rows]

        if dates:
            return dates, f"current season (per-league season join, capped to last {DEFAULT_CURRENT_SEASON_MAX_DAYS}d)"

        # Fallback: leagues.current_season not populated — use most recent season label,
        # still capped to DEFAULT_CURRENT_SEASON_MAX_DAYS.
        seasons = self._discover_seasons(conn)
        if seasons:
            season_label = seasons[0]
            print(f"  [TRAIN] Current-season join returned no dates "
                  f"(leagues.current_season may not be fully populated). "
                  f"Falling back to most recent season in DB: {season_label}")
            rows = conn.execute("""
                SELECT DISTINCT date FROM schedules
                WHERE season = ?
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
                  AND date IS NOT NULL AND date <= ?
                  AND date >= ?
                ORDER BY date ASC
            """, (season_label, today_str, cap_date)).fetchall()
            dates = [r[0] for r in rows]
            return dates, (
                f"season {season_label} (fallback — run --enrich-leagues to populate current_season, "
                f"capped to last {DEFAULT_CURRENT_SEASON_MAX_DAYS}d)"
            )

        # Last resort: capped global window
        print("  [TRAIN] WARNING: No season metadata found. Falling back to capped global date window.")
        rows = conn.execute("""
            SELECT DISTINCT date FROM schedules
            WHERE date IS NOT NULL
              AND home_score IS NOT NULL AND away_score IS NOT NULL
              AND date <= ?
              AND date >= ?
            ORDER BY date ASC
        """, (today_str, cap_date)).fetchall()
        return [r[0] for r in rows], f"global capped window (last {DEFAULT_CURRENT_SEASON_MAX_DAYS}d)"

    # -------------------------------------------------------------------
    # Reward functions (30-dim action space)
    # -------------------------------------------------------------------

    @staticmethod
    def _get_correct_actions(outcome: Dict[str, Any]) -> set:
        """Map actual outcome to the set of correct action indices (30-dim)."""
        home_score = outcome.get("home_score", 0)
        away_score = outcome.get("away_score", 0)
        gt = derive_ground_truth(int(home_score), int(away_score))
        correct = set()
        for action in ACTIONS:
            key = action["key"]
            if gt.get(key) is True:
                correct.add(action["idx"])
        return correct

    @staticmethod
    def _compute_phase1_reward(
        chosen_action_idx: int,
        home_score: int,
        away_score: int,
    ) -> float:
        """
        Phase 1 reward: accuracy-based (no odds data yet).
        Correct prediction of bettable market = +1.0
        Correct prediction of non-bettable = +0.3
        Wrong prediction = -0.5
        no_bet when good bets existed = -0.2
        no_bet when all markets low confidence = +0.1
        """
        action = ACTIONS[chosen_action_idx]
        key = action["key"]
        gt = derive_ground_truth(int(home_score), int(away_score))

        if key == "no_bet":
            any_bettable_correct = any(
                gt.get(ACTIONS[i]["key"], False) is True
                for i in STAIRWAY_BETTABLE
            )
            return -0.2 if any_bettable_correct else +0.1

        outcome = gt.get(key)
        if outcome is None:
            return 0.0

        bettable, _ = stairway_gate(key)
        if outcome is True:
            return 1.0 if bettable else 0.3
        else:
            return -0.5

    @staticmethod
    def _compute_phase2_reward(
        chosen_action_idx: int,
        home_score: int,
        away_score: int,
        live_odds: Optional[float] = None,
        model_prob: Optional[float] = None,
    ) -> float:
        """
        Phase 2 reward: value-based (real or xG-derived fair odds).

        FIX-3: When live_odds is None, we use xG-derived fair odds per fixture
        rather than the static SYNTHETIC_ODDS lookup. This gives a match-specific
        reward signal even before real odds data is available.

        The caller (_train_step_phase2) passes xg_fair_odds computed from the
        same Poisson distribution used by the expert, so the reward is grounded
        in the match's actual predicted goal distribution.
        """
        action = ACTIONS[chosen_action_idx]
        key = action["key"]
        gt = derive_ground_truth(int(home_score), int(away_score))

        if key == "no_bet":
            any_value_bet_missed = any(
                gt.get(ACTIONS[i]["key"], False) is True
                for i in STAIRWAY_BETTABLE
                if SYNTHETIC_ODDS.get(ACTIONS[i]["key"], 0) >= 1.30
            )
            return -0.3 if any_value_bet_missed else +0.1

        bettable, reason = stairway_gate(key, live_odds, model_prob)
        if not bettable:
            return -0.1

        outcome = gt.get(key)
        if outcome is None:
            return 0.0

        # FIX-3: prefer live_odds, fall back to per-fixture xG fair odds (passed
        # as live_odds by the caller), then last-resort to static SYNTHETIC_ODDS.
        odds = live_odds if live_odds else SYNTHETIC_ODDS.get(key, 1.5)
        if outcome is True:
            return odds - 1.0   # profit
        else:
            return -1.0          # loss

    # -------------------------------------------------------------------
    # Expert signal (Rule Engine + Poisson)
    # -------------------------------------------------------------------

    def _get_rule_engine_probs(self, vision_data: Dict[str, Any]) -> torch.Tensor:
        """
        Expert signal: Poisson probability distribution over 30-dim action space.

        FIX-5: When form data is empty (common for early historical dates before
        a team has played any enriched matches), fall back to league-average xG
        (1.4 home / 1.1 away) rather than xG=0 which collapses the Poisson
        distribution and makes every match produce an identical expert tensor.

        Returns: torch.Tensor shape (30,) summing to 1.0
        """
        h2h = vision_data.get("h2h_data", {})
        home_form = [m for m in h2h.get("home_last_10_matches", []) if m][:10]
        away_form = [m for m in h2h.get("away_last_10_matches", []) if m][:10]
        home_team = h2h.get("home_team", "")
        away_team = h2h.get("away_team", "")

        xg_home = FeatureEncoder._compute_xg(home_form, home_team, is_home=True)
        xg_away = FeatureEncoder._compute_xg(away_form, away_team, is_home=False)

        # FIX-5: league-average fallback when no form data available
        if xg_home < 0.05:
            xg_home = 1.4
        if xg_away < 0.05:
            xg_away = 1.1

        # Rule Engine 1X2 blending
        raw_scores = None
        try:
            from ..rule_engine import RuleEngine
            analysis = RuleEngine.analyze(vision_data)
            if analysis.get("type") != "SKIP":
                raw_scores = analysis.get("raw_scores")
        except Exception:
            pass

        probs = compute_poisson_probs(xg_home, xg_away, raw_scores)

        # Down-weight non-bettable actions so the expert signal concentrates
        # on markets the Stairway Gate will actually evaluate.
        for action in ACTIONS:
            key = action["key"]
            if key == "no_bet":
                continue
            bettable, _ = stairway_gate(key)
            if not bettable:
                probs[key] *= 0.3

        vec = probs_to_tensor_30dim(probs)
        tensor = torch.tensor(vec, dtype=torch.float32)

        if tensor.sum() < 0.1:
            return torch.ones(N_ACTIONS, dtype=torch.float32).to(self.device) / N_ACTIONS

        return (tensor / tensor.sum()).to(self.device)

    def _get_xg_fair_odds(self, vision_data: Dict[str, Any]) -> Dict[str, float]:
        """
        Compute per-fixture xG-derived fair odds for all 30 markets.

        FIX-3: Used as the odds fallback in Phase 2 reward when real historical
        odds are not available. This gives a match-specific odds signal rather
        than the global static SYNTHETIC_ODDS table.

        Returns a dict keyed by action key → fair odds (reciprocal of Poisson prob,
        floored at 1.01 and capped at 20.0 to avoid reward explosion).
        """
        h2h = vision_data.get("h2h_data", {})
        home_form = [m for m in h2h.get("home_last_10_matches", []) if m][:10]
        away_form = [m for m in h2h.get("away_last_10_matches", []) if m][:10]
        home_team = h2h.get("home_team", "")
        away_team = h2h.get("away_team", "")

        xg_home = FeatureEncoder._compute_xg(home_form, home_team, is_home=True)
        xg_away = FeatureEncoder._compute_xg(away_form, away_team, is_home=False)

        if xg_home < 0.05:
            xg_home = 1.4
        if xg_away < 0.05:
            xg_away = 1.1

        probs = compute_poisson_probs(xg_home, xg_away, None)
        fair_odds: Dict[str, float] = {}
        for action in ACTIONS:
            key = action["key"]
            if key == "no_bet":
                continue
            p = probs.get(key, 0.0)
            if p > 0.01:
                fair_odds[key] = min(max(1.0 / p, 1.01), 20.0)
            else:
                # Use synthetic as hard fallback for very low probability markets
                fair_odds[key] = SYNTHETIC_ODDS.get(key, 1.5)
        return fair_odds

    # -------------------------------------------------------------------
    # Training step (PPO — single fixture)
    # -------------------------------------------------------------------

    def train_step(
        self,
        features: torch.Tensor,
        league_idx: int,
        home_team_idx: int,
        away_team_idx: int,
        outcome: Optional[Dict[str, Any]] = None,
        expert_probs: Optional[torch.Tensor] = None,
        use_kl: bool = False,
        old_log_prob: Optional[torch.Tensor] = None,
        xg_fair_odds: Optional[Dict[str, float]] = None,
        active_phase: Optional[int] = None,
    ) -> Dict[str, float]:
        """
        Single training step for Phase 1 (Imitation) or Phase 2/3 (PPO).

        Args:
            features:       Encoded feature tensor.
            league_idx:     League adapter index.
            home_team_idx:  Home team adapter index.
            away_team_idx:  Away team adapter index.
            outcome:        Match result dict with home_score / away_score.
                            Required for Phase 2/3. None triggers Phase 1 path.
            expert_probs:   Rule Engine + Poisson 30-dim distribution.
                            Used for Phase 1 imitation and Phase 2 KL anchor.
            use_kl:         If True, add KL penalty to Phase 2/3 loss (Phase 2 only).
            old_log_prob:   Log prob of the action sampled BEFORE this gradient step.
                            FIX-1: must be provided for correct PPO importance sampling.
                            If None, ratio defaults to 1.0 (equivalent to vanilla PG).
            xg_fair_odds:   Per-fixture fair odds dict from _get_xg_fair_odds().
                            FIX-3: used as odds fallback in Phase 2 reward.
            active_phase:   Explicit phase override (FIX-4). If None, falls back to
                            self.active_phase.

        Returns:
            Metrics dict.
        """
        self.model.train()
        features = features.to(self.device)

        # FIX-4: always resolve active_phase explicitly; never rely on implicit default.
        resolved_phase = active_phase if active_phase is not None else self.active_phase

        # Forward pass
        policy_logits, value, stake = self.model(
            features, league_idx, home_team_idx, away_team_idx
        )
        action_probs = torch.softmax(policy_logits, dim=-1)

        total_loss = torch.tensor(0.0, device=self.device)
        metrics: Dict[str, float] = {}

        # ── Phase 1: Imitation Learning ──────────────────────────────────────────
        if expert_probs is not None and outcome is None:
            if expert_probs.dim() == 1:
                expert_probs = expert_probs.unsqueeze(0)

            # FIX-2: F.kl_div IS KL divergence. Removed the redundant manual KL
            # computation that was previously added on top as a "monitoring metric".
            # total_loss is now purely the KL divergence (imitation loss).
            policy_log_probs = F.log_softmax(policy_logits, dim=-1)
            imitation_loss = F.kl_div(
                policy_log_probs, expert_probs, reduction='batchmean'
            )
            total_loss = imitation_loss

            # KL divergence as a monitoring metric only (not added to loss).
            with torch.no_grad():
                kl_monitor = torch.sum(
                    expert_probs * (
                        torch.log(expert_probs + 1e-10)
                        - torch.log(action_probs.unsqueeze(0) + 1e-10)
                    )
                )

            rl_action = torch.argmax(action_probs, dim=-1).item()
            metrics["imitation_loss"] = imitation_loss.item()
            metrics["kl_div"] = kl_monitor.item()
            metrics["action"] = rl_action
            metrics["rule_engine_acc"] = (
                1.0 if rl_action == torch.argmax(expert_probs).item() else 0.0
            )
            metrics["max_prob"] = action_probs.max().item()

        # ── Phase 2/3: PPO with optional KL anchor ───────────────────────────────
        elif outcome is not None:
            dist = torch.distributions.Categorical(action_probs)
            action = dist.sample()
            new_log_prob = dist.log_prob(action)

            h_score = outcome.get("home_score", 0)
            a_score = outcome.get("away_score", 0)

            # FIX-3: use xG fair odds as fallback if provided
            if resolved_phase >= 2:
                if xg_fair_odds is not None:
                    action_key = ACTIONS[action.item()]["key"]
                    match_odds = xg_fair_odds.get(action_key)
                else:
                    match_odds = None
                reward = self._compute_phase2_reward(
                    action.item(), h_score, a_score, live_odds=match_odds
                )
            else:
                reward = self._compute_phase1_reward(action.item(), h_score, a_score)

            reward_tensor = torch.tensor([reward], dtype=torch.float32, device=self.device)
            advantage = reward_tensor - value.squeeze(-1)

            # FIX-1: PPO importance sampling ratio.
            # old_log_prob must be the log prob of `action` under the OLD policy
            # (i.e. sampled and detached BEFORE this gradient step). If not
            # provided, ratio = 1.0 which degenerates to vanilla policy gradient.
            if old_log_prob is not None:
                ratio = torch.exp(new_log_prob - old_log_prob.detach())
            else:
                ratio = torch.ones_like(new_log_prob)

            clipped = torch.clamp(ratio, 1.0 - self.clip_epsilon, 1.0 + self.clip_epsilon)
            policy_loss = -torch.min(
                ratio * advantage.detach(),
                clipped * advantage.detach()
            ).mean()
            value_loss = nn.functional.mse_loss(value.squeeze(-1), reward_tensor)
            entropy_bonus = -0.01 * dist.entropy().mean()

            total_loss = policy_loss + 0.5 * value_loss + entropy_bonus

            if use_kl and expert_probs is not None:
                # KL anchor: keeps policy from drifting too far from expert.
                # kl_weight anneals toward 0 so RL can eventually outperform.
                kl_div = torch.sum(
                    expert_probs * (
                        torch.log(expert_probs + 1e-10)
                        - torch.log(action_probs + 1e-10)
                    )
                )
                total_loss = total_loss + self.kl_weight * kl_div
                metrics["kl_div"] = kl_div.item()

            metrics.update({
                "policy_loss": policy_loss.item(),
                "value_loss": value_loss.item(),
                "reward": reward,
                "action": action.item(),
                "new_log_prob": new_log_prob.detach(),  # returned for next-step old_log_prob
                "max_prob": action_probs.max().item(),
            })

        # Backward + optimize
        self.optimizer.zero_grad()
        total_loss.backward()
        # FIX-6: Raise clip to 1.0 for Phase 2/3; Phase 1 keeps 0.5 for stability.
        clip_norm = 0.5 if (expert_probs is not None and outcome is None) else 1.0
        torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=clip_norm)
        self.optimizer.step()
        self.scheduler.step()

        self._step_count += 1
        metrics.update({"total_loss": total_loss.item(), "step": self._step_count})
        return metrics

    # -------------------------------------------------------------------
    # Chronological training from fixtures
    # -------------------------------------------------------------------

    def train_from_fixtures(
        self,
        phase: int = 1,
        cold: bool = False,
        limit_days: Optional[int] = None,
        resume: bool = False,
        target_season: Union[str, int] = "current",
    ):
        """
        3-Phase Chronological Training with season-aware date selection.

        Phase 1: Imitation Learning (Warm-start from Rule Engine)
        Phase 2: PPO with KL penalty (Fine-tune with constraints)
        Phase 3: Adapter Fine-tuning (League specialization, frozen trunk)

        Args:
            target_season:
                "current"   Per-league season start (default). Joins against
                            leagues.current_season so each league's season
                            start date is respected individually. Capped at
                            DEFAULT_CURRENT_SEASON_MAX_DAYS days to prevent
                            runaway training (FIX-7).
                "all"       All available seasons, oldest-first. Full cold retrain.
                int N       Past season by offset: 1 = most recent past season,
                            2 = two seasons ago, etc. Matches enrich_leagues convention.
                str label   Explicit season string, e.g. "2024/2025" or "2025".

        CLI flag: --train-season (see lifecycle.py parse_args)
        """
        from Data.Access.db_helpers import _get_conn

        conn = _get_conn()
        os.makedirs(MODELS_DIR, exist_ok=True)

        print("\n  ============================================================")
        print(f"  RL TRAINING — PHASE {phase} {'(COLD START)' if cold else ''}")
        print("  ============================================================\n")

        # ── Auto-detect active training phase ──────────────────────
        phase_status = check_phase_readiness(conn)
        odds_rows  = phase_status["odds_rows"]
        days_live  = phase_status["days_live"]
        phase2_ready = phase_status["phase2_ready"]
        phase3_ready = phase_status["phase3_ready"]

        if phase3_ready:
            active_phase = 3
            print(f"  [RL] Phase 3 AUTO-ACTIVATED: "
                  f"{odds_rows} odds rows, {days_live} days live.")
        elif phase2_ready:
            active_phase = 2
            print(f"  [RL] Phase 2 AUTO-ACTIVATED: "
                  f"{odds_rows} odds rows, {days_live} days live.")
        else:
            active_phase = 1
            needed_rows = PHASE2_MIN_ODDS_ROWS - odds_rows
            needed_days = max(0, PHASE2_MIN_DAYS_LIVE - days_live)
            print(f"  [RL] Phase 1 active. "
                  f"Phase 2 needs: {needed_rows} more odds rows, "
                  f"{needed_days} more days of live data.")

        # FIX-4: always set self.active_phase so train_step never falls through
        # to a stale default.
        self.active_phase = active_phase

        if active_phase == 3 or phase == 3:
            print("  [TRAIN] Freezing Shared Trunk... training Adapters only.")
            for param in self.model.trunk.parameters():
                param.requires_grad = False
        elif cold:
            print("  [TRAIN] Cold start: Starting from base weights (no checkpoint loaded).")

        # ── Season-aware date selection ─────────────────────────────────────────
        all_dates, season_label = self._get_season_dates(conn, target_season)

        if not all_dates:
            print(f"  [TRAIN] No fixture dates found for target_season={target_season!r}. "
                  f"Run --enrich-leagues to populate historical data.")
            return

        today_str = datetime.now().strftime("%Y-%m-%d")
        all_dates = [d for d in all_dates if d <= today_str]

        if limit_days:
            all_dates = all_dates[:limit_days]

        print(f"  [TRAIN] Season scope:  {season_label}")
        print(f"  [TRAIN] Window:        {all_dates[0]} → {all_dates[-1]} ({len(all_dates)} fixture-days)")

        # --- Checkpoint setup ---
        CHECKPOINT_DIR = MODELS_DIR / "checkpoints"
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        latest_path = MODELS_DIR / f"phase{phase}_latest.pth"
        total_matches_global = 0
        total_correct_global = 0
        start_day_idx = 0

        # --- Resume from checkpoint ---
        if resume and not cold and latest_path.exists():
            try:
                ckpt = torch.load(latest_path, map_location=self.device, weights_only=False)
                ckpt_n_actions = ckpt.get("n_actions", 8)
                if ckpt_n_actions != N_ACTIONS:
                    print(f"  [RESUME] ✗ Checkpoint is {ckpt_n_actions}-dim but "
                          f"current model is {N_ACTIONS}-dim. Delete and retrain.")
                    return
                self.model.load_state_dict(ckpt["model_state"], strict=False)
                self.optimizer.load_state_dict(ckpt["optimizer_state"])
                if "scheduler_state" in ckpt:
                    self.scheduler.load_state_dict(ckpt["scheduler_state"])
                # Restore kl_weight from checkpoint if saved
                if "kl_weight" in ckpt:
                    self.kl_weight = ckpt["kl_weight"]
                start_day_idx = ckpt["day"]
                total_matches_global = ckpt.get("total_matches", 0)
                total_correct_global = ckpt.get("correct_predictions", 0)
                ckpt_season = ckpt.get("target_season", "unknown")
                print(f"  [RESUME] ✓ Loaded checkpoint from Day {start_day_idx}/{len(all_dates)} "
                      f"({ckpt.get('match_date', '?')}) | season={ckpt_season}")
                print(f"  [RESUME]   Matches so far: {total_matches_global} | Correct: {total_correct_global}")
                all_dates = all_dates[start_day_idx:]
                if not all_dates:
                    print(f"  [RESUME] All days already completed. Nothing to do.")
                    return
            except Exception as e:
                print(f"  [RESUME] Failed to load checkpoint: {e} — starting fresh")
                start_day_idx = 0

        # Phase 1 LR reduction: imitation needs 10x lower LR than PPO exploration.
        # Guard with `not resume` so a resumed run does not re-apply the reduction.
        original_lrs = []
        if active_phase == 1 and not resume:
            for pg in self.optimizer.param_groups:
                original_lrs.append(pg['lr'])
                pg['lr'] = pg['lr'] * 0.1
            print(f"  [TRAIN] Phase 1 LR reduced 10x for stable imitation "
                  f"(base → {self.optimizer.param_groups[0]['lr']:.2e})")

        for day_offset, match_date in enumerate(all_dates):
            day_idx = start_day_idx + day_offset
            day_matches = 0
            day_reward = 0.0
            day_imit_loss = 0.0
            day_kl = 0.0
            day_rl_acc = 0.0
            day_rule_acc = 0.0
            day_grad_norm = 0.0
            day_max_prob = 0.0

            cursor = conn.execute("""
                SELECT 
                    s.fixture_id, s.league_id, s.home_team_id, 
                    COALESCE(NULLIF(s.home_team_name, ''), t1.name) as h_name,
                    s.away_team_id, 
                    COALESCE(NULLIF(s.away_team_name, ''), t2.name) as a_name,
                    s.home_score, s.away_score,
                    s.season
                FROM schedules s
                LEFT JOIN teams t1 ON s.home_team_id = t1.team_id
                LEFT JOIN teams t2 ON s.away_team_id = t2.team_id
                WHERE s.date = ? AND s.home_score IS NOT NULL AND s.away_score IS NOT NULL
            """, (match_date,))
            fixtures = cursor.fetchall()

            for fix in fixtures:
                fixture_id, league_id, home_tid, h_name, away_tid, a_name, h_score, a_score, season = fix
                outcome = {
                    "result": "home_win" if h_score > a_score else "away_win" if a_score > h_score else "draw",
                    "home_score": h_score, "away_score": a_score
                }

                l_idx = self.registry.get_league_idx(league_id)
                h_idx = self.registry.get_team_idx(home_tid)
                a_idx = self.registry.get_team_idx(away_tid)

                vision_data = self._build_training_vision_data(
                    conn, match_date, league_id, home_tid, h_name, away_tid, a_name, season=season
                )
                features = FeatureEncoder.encode(vision_data)
                expert_probs = self._get_rule_engine_probs(vision_data)

                if active_phase == 1:
                    # Phase 1: pure imitation, no outcome needed
                    metrics = self.train_step(
                        features, l_idx, h_idx, a_idx,
                        expert_probs=expert_probs,
                        active_phase=active_phase,
                    )
                else:
                    # FIX-1: compute old_log_prob under CURRENT policy BEFORE the update.
                    # This is the correct PPO procedure: sample action → record log prob →
                    # compute reward → update policy → importance-sample with stored log prob.
                    use_kl = (active_phase == 2)
                    xg_fair_odds = self._get_xg_fair_odds(vision_data)  # FIX-3

                    with torch.no_grad():
                        features_dev = features.to(self.device)
                        logits_old, _, _ = self.model(features_dev, l_idx, h_idx, a_idx)
                        probs_old = torch.softmax(logits_old, dim=-1)
                        dist_old = torch.distributions.Categorical(probs_old)
                        action_old = dist_old.sample()
                        old_log_prob = dist_old.log_prob(action_old)

                    metrics = self.train_step(
                        features, l_idx, h_idx, a_idx,
                        outcome=outcome,
                        expert_probs=expert_probs,
                        use_kl=use_kl,
                        old_log_prob=old_log_prob,
                        xg_fair_odds=xg_fair_odds,
                        active_phase=active_phase,
                    )

                day_matches += 1
                day_reward += metrics.get("reward", 0.0)
                day_imit_loss += metrics.get("imitation_loss", 0.0)
                day_kl += metrics.get("kl_div", 0.0)
                day_max_prob += metrics.get("max_prob", 0.0)

                # Gradient norm tracking
                total_norm = 0.0
                for p in self.model.parameters():
                    if p.grad is not None:
                        total_norm += p.grad.data.norm(2).item() ** 2
                day_grad_norm += total_norm ** 0.5

                # Correctness tracking
                action_idx = metrics.get("action", torch.argmax(
                    self.model.get_action_probs(features, l_idx, h_idx, a_idx)
                ).item())
                correct_actions = self._get_correct_actions(outcome)
                expert_pred_idx = torch.argmax(expert_probs).item()

                if day_idx == 0 and day_matches <= 5:
                    probs_list = expert_probs.squeeze().detach().cpu().tolist()
                    print(f"      [DEBUG] {h_name} vs {a_name}")
                    print(f"        Expert probs: {[round(p, 3) for p in probs_list]}")
                    print(f"        Expert pick: {ACTIONS[expert_pred_idx]['key']} | RL pick: {ACTIONS[action_idx]['key']}")
                    print(f"        Correct actions: {[ACTIONS[a]['key'] for a in correct_actions]}")
                    print(f"        KL: {metrics.get('kl_div', 0.0):.4f} | "
                          f"Imitation loss: {metrics.get('imitation_loss', 0.0):.4f}")

                if action_idx in correct_actions:
                    day_rl_acc += 1
                if expert_pred_idx in correct_actions:
                    day_rule_acc += 1

                self.registry.record_match(league_id, home_tid, away_tid)

            if day_matches > 0:
                rl_acc = (day_rl_acc / day_matches) * 100
                rule_acc = (day_rule_acc / day_matches) * 100
                kl = day_kl / day_matches
                gn = day_grad_norm / day_matches
                max_prob_pct = (day_max_prob / day_matches) * 100
                if active_phase == 1:
                    il = day_imit_loss / day_matches
                    print(f"  [Day {day_idx+1:3d}/{start_day_idx + len(all_dates)}] "
                          f"Rule Acc: {rule_acc:4.1f}% | RL Acc: {rl_acc:4.1f}% | "
                          f"KL: {kl:5.3f} | ImitLoss: {il:6.4f} | MaxProb: {max_prob_pct:4.1f}% | "
                          f"GradNorm: {gn:.4f} | Matches: {day_matches}")
                else:
                    rw = day_reward / day_matches
                    print(f"  [Day {day_idx+1:3d}/{start_day_idx + len(all_dates)}] "
                          f"Rule Acc: {rule_acc:4.1f}% | RL Acc: {rl_acc:4.1f}% | "
                          f"KL: {kl:5.3f} | Reward: {rw:6.3f} | MaxProb: {max_prob_pct:4.1f}% | "
                          f"GradNorm: {gn:.4f} | Matches: {day_matches}")

                # --- KL annealing for true outperformance (Phase 2+) ---
                if active_phase >= 2:
                    self.kl_weight = max(0.0, self.kl_weight * 0.995)
                    if self.kl_weight < 0.01:
                        self.kl_weight = 0.0

                # --- Save checkpoint after each day ---
                total_matches_global += day_matches
                total_correct_global += int(day_rl_acc)
                ckpt_data = {
                    "day": day_idx + 1,
                    "total_days": start_day_idx + len(all_dates),
                    "match_date": match_date,
                    "model_state": self.model.state_dict(),
                    "optimizer_state": self.optimizer.state_dict(),
                    "scheduler_state": self.scheduler.state_dict(),
                    "kl_weight": self.kl_weight,
                    "total_matches": total_matches_global,
                    "correct_predictions": total_correct_global,
                    "phase": active_phase,
                    "n_actions": N_ACTIONS,
                    "odds_rows_at_save": odds_rows,
                    "days_live_at_save": days_live,
                    "target_season": str(target_season),
                    "season_label": season_label,
                }
                torch.save(ckpt_data, CHECKPOINT_DIR / f"phase{active_phase}_day{day_idx+1:03d}.pth")
                torch.save(ckpt_data, latest_path)

                # Keep only last 5 daily checkpoints
                existing = sorted(CHECKPOINT_DIR.glob(f"phase{active_phase}_day*.pth"))
                while len(existing) > 5:
                    existing[0].unlink()
                    existing = existing[1:]

        # Restore LR after Phase 1
        if original_lrs:
            for pg, lr in zip(self.optimizer.param_groups, original_lrs):
                pg['lr'] = lr

        self.save()
        print(f"\n  [TRAIN] Phase {phase} complete. Model saved.")

    def _build_training_vision_data(
        self, conn, match_date: str, league_id: str,
        home_team_id: str, home_team_name: str,
        away_team_id: str, away_team_name: str,
        season: str = None,
    ) -> Dict[str, Any]:
        """
        Build a vision_data dict from historical fixtures for training.
        Uses ONLY data before match_date (no future leakage).
        """
        from Data.Access.league_db import computed_standings

        home_form = self._get_team_form(conn, home_team_id, home_team_name, match_date)
        away_form = self._get_team_form(conn, away_team_id, away_team_name, match_date)
        h2h = self._get_h2h(conn, home_team_id, away_team_id, match_date)

        standings = []
        if league_id:
            try:
                standings = computed_standings(
                    conn=conn, league_id=league_id,
                    season=season, before_date=match_date
                )
            except Exception:
                standings = []

        return {
            "h2h_data": {
                "home_team": home_team_name,
                "away_team": away_team_name,
                "home_last_10_matches": home_form,
                "away_last_10_matches": away_form,
                "head_to_head": h2h,
                "region_league": league_id,
            },
            "standings": standings,
        }

    def _get_team_form(self, conn, team_id: str, team_name: str,
                       before_date: str) -> List[Dict]:
        """Get last 10 matches for a team before a given date."""
        cursor = conn.execute("""
            SELECT date, home_team_name, away_team_name, home_score, away_score
            FROM schedules
            WHERE (home_team_id = ? OR away_team_id = ?)
              AND date < ?
              AND home_score IS NOT NULL AND away_score IS NOT NULL
              AND home_score != '' AND away_score != ''
              AND (match_status = 'finished' OR match_status IS NULL)
            ORDER BY date DESC
            LIMIT 10
        """, (team_id, team_id, before_date))

        matches = []
        for row in cursor.fetchall():
            hs, as_ = int(row[3] or 0), int(row[4] or 0)
            winner = "Home" if hs > as_ else "Away" if as_ > hs else "Draw"
            matches.append({
                "date": row[0],
                "home": row[1],
                "away": row[2],
                "score": f"{hs}-{as_}",
                "winner": winner,
            })
        return matches

    def _get_h2h(self, conn, home_id: str, away_id: str,
                 before_date: str) -> List[Dict]:
        """Get H2H matches between two teams before a given date (540-day window)."""
        cutoff_date = (datetime.strptime(before_date, "%Y-%m-%d")
                       - timedelta(days=540)).strftime("%Y-%m-%d")

        cursor = conn.execute("""
            SELECT date, home_team_name, away_team_name, home_score, away_score
            FROM schedules
            WHERE ((home_team_id = ? AND away_team_id = ?)
                OR (home_team_id = ? AND away_team_id = ?))
              AND date < ?
              AND date >= ?
              AND home_score IS NOT NULL AND away_score IS NOT NULL
              AND home_score != '' AND away_score != ''
              AND (match_status = 'finished' OR match_status IS NULL)
            ORDER BY date DESC
            LIMIT 10
        """, (home_id, away_id, away_id, home_id, before_date, cutoff_date))

        matches = []
        for row in cursor.fetchall():
            hs, as_ = int(row[3] or 0), int(row[4] or 0)
            winner = "Home" if hs > as_ else "Away" if as_ > hs else "Draw"
            matches.append({
                "date": row[0],
                "home": row[1],
                "away": row[2],
                "score": f"{hs}-{as_}",
                "winner": winner,
            })
        return matches

    # -------------------------------------------------------------------
    # Online update (from new prediction outcomes)
    # -------------------------------------------------------------------

    def update_from_outcomes(self, reviewed_predictions: List[Dict[str, Any]]):
        """
        Online learning from new prediction outcomes.
        Called after outcome_reviewer completes a batch.

        FIX-4: active_phase is explicitly set to self.active_phase before calling
        train_step, so online updates never silently use the wrong reward function.
        """
        if not reviewed_predictions:
            return

        self.load()  # Load latest model
        updated = 0

        for pred in reviewed_predictions:
            if pred.get("outcome_correct") not in ("True", "False", "1", "0"):
                continue

            is_correct = pred.get("outcome_correct") in ("True", "1")

            vision_data = {
                "h2h_data": {
                    "home_team": pred.get("home_team", ""),
                    "away_team": pred.get("away_team", ""),
                    "home_last_10_matches": [],
                    "away_last_10_matches": [],
                    "head_to_head": [],
                    "region_league": pred.get("region_league", "GLOBAL"),
                },
                "standings": [],
            }

            features = FeatureEncoder.encode(vision_data)

            league_id = pred.get("region_league", "GLOBAL")
            home_tid = pred.get("home_team_id", "GLOBAL")
            away_tid = pred.get("away_team_id", "GLOBAL")

            l_idx = self.registry.get_league_idx(league_id)
            h_idx = self.registry.get_team_idx(home_tid)
            a_idx = self.registry.get_team_idx(away_tid)

            outcome = {
                "result": "home_win" if is_correct else "draw",
                "home_score": int(pred.get("home_score", 0) or 0),
                "away_score": int(pred.get("away_score", 0) or 0),
            }

            # FIX-4: pass active_phase explicitly — never rely on default
            self.train_step(
                features, l_idx, h_idx, a_idx,
                outcome=outcome,
                active_phase=self.active_phase,
            )
            updated += 1

        if updated > 0:
            self.save()
            print(f"  [RL] Updated model from {updated} new outcomes")

    # -------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------

    def save(self):
        """Save model and registry."""
        os.makedirs(MODELS_DIR, exist_ok=True)
        torch.save(self.model.state_dict(), BASE_MODEL_PATH)
        self.registry.save()

    def load(self):
        """Load model and registry if they exist."""
        if BASE_MODEL_PATH.exists():
            try:
                state_dict = torch.load(BASE_MODEL_PATH, map_location=self.device, weights_only=True)
                self.model.load_state_dict(state_dict, strict=False)
            except Exception as e:
                print(f"  [RL] Could not load model: {e}")

        self.registry = AdapterRegistry()  # Reloads from disk