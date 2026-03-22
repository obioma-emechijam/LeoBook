# LeoBook Systems Architecture — Deep Audit Report
### Evidence-Based · Line-Number Traced · 2026-03-22

---

## 1. PREDICTION SYSTEM (Chapter 1 Page 2)

### 1.1 Entry Point
[prediction_pipeline.py:242](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L242) [run_predictions()](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#242-451) is the main loop.

**Data Source**: `schedules` table via [prediction_pipeline.py:83-99](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L83-L99) [get_weekly_fixtures()](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#67-102).
- Joins `teams` table for names (preferring fixture-specific `home_team_name` over `teams.name` to avoid alias pollution — see comment at line 86-88).
- Returns list of `Dict` rows for the next 7 days of unplayed matches.

### 1.2 Feature Construction
For each fixture, [prediction_pipeline.py:304](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L304) [build_rule_engine_input()](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#143-231) assembles:

| Feature | Source | Lines |
|:---|:---|:---|
| Home form (last 10) | `schedules` WHERE `home_team_id = ?` | [L110-118](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L110-L118) |
| Away form (last 10) | `schedules` WHERE `away_team_id = ?` | [L110-118](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L110-L118) |
| H2H (last 10) | `schedules` cross-join both team IDs | [L129-138](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L129-L138) |
| Standings | `computed_standings()` on-the-fly from `schedules` | [L170-172](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L170-L172) |
| Real odds | `match_odds` table (from Football.com harvester) | [L179-214](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L179-L214) |

### 1.3 Symbolic Prediction (Rule Engine)
[rule_engine.py:24](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L24) `RuleEngine.analyze()`:

**Sub-components ACTUALLY USED (not dead)**:
1. **LearningEngine** ([rule_engine.py:16](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L16)) → Loads per-league voting weights from `learning_weights.json`. **NOT dead code** — called at [L88](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L88).
2. **TagGenerator** ([rule_engine.py:17](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L17)) → Generates `FORM_S2+`, `H2H_HOME_WIN`, `TABLE_ADV8+` tags. Called at [L72-75](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L72-L75).
3. **GoalPredictor** ([rule_engine.py:18](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L18)) → Computes xG from goal distributions. Called at [L78-82](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L78-L82).
4. **BettingMarkets** ([rule_engine.py:19](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L19)) → Generates 30-dim Poisson predictions. Called at [L169-179](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L169-L179).
5. **RuleConfig** ([rule_engine.py:20](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L20)) → Custom voting weights + scope filtering.

**Guardrails in Rule Engine**:
- **Scope Filter** [L42-43](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L42-L43): `config.matches_scope()` → SKIP if outside engine scope.
- **xG Contradiction Check** [L226-248](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L226-L248): If prediction says "Away Win" but `home_xg > away_xg + 1.25`, returns SKIP.
- **Score Sanity** [L245-248](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine.py#L245-L248): If most probable score is "0-0" but prediction is "Over 2.5", downgrades confidence to "Low".

### 1.4 Neural Prediction (RL Engine)
[inference.py:75](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/inference.py#L75) `RLPredictor.predict()`:

**Architecture** ([model.py:69-84](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/model.py#L69-L84)):
```
FeatureEncoder (222-dim) → SharedTrunk (222→256→256→128)
  → LeagueAdapter (LoRA rank=16, ~8K params each)
  → ConditionedTeamAdapter (league-conditioned, LoRA rank=8)
  → PolicyHead → 30-dim action distribution
  → ValueHead  → scalar EV
  → StakeHead  → Kelly fraction [0, 5%]
```

**Feature Vector** ([feature_encoder.py:19](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/feature_encoder.py#L19)): `FEATURE_DIM = 222`

| Block | Floats | Content |
|:---|:---|:---|
| xG | 4 | home_xg, away_xg, diff, total |
| Home Form | 30 | Recency-weighted W/D/L one-hot |
| Away Form | 30 | Same structure |
| Home Goals | 20 | Avg/std/max/min scored/conceded, BTTS%, O2.5% |
| Away Goals | 20 | Same |
| H2H | 8 | Win rates, avg goals, dominance flags |
| Standings | 10 | Normalized positions, points, GD |
| Schedule | 6 | Rest days, fatigue/rested flags |
| League Meta | 4 | Level, avg goals, home advantage |
| Market Priors | 30 | Base likelihood for each of 30 actions |
| Padding | 60 | Zeros to reach 222 |

**Guardrails in RL**:
- **Model Availability** [inference.py:94-99](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/inference.py#L94-L99): Returns SKIP if `leobook_base.pth` doesn't exist.
- **Abstention** [inference.py:142-147](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/inference.py#L142-L147): If `no_bet` action wins, returns SKIP.

### 1.5 Ensemble Merge
[ensemble.py:52](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/ensemble.py#L52) `EnsembleEngine.merge()`:

**Weight Scaling**:
```python
# Default: W_symbolic=0.7, W_neural=0.3
# W_neural is scaled by data_richness_score [0.0, 1.0]
# 0 prior seasons → W_neural = 0.0 (pure Rule Engine)
# 3+ prior seasons → W_neural = full 0.3
```

**Guardrails in Ensemble**:
- **RL Fallback** [ensemble.py:83-101](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/ensemble.py#L83-L101): If `rl_conf < 0.3` OR RL failed → pure symbolic path.
- **Richness Scaling** [ensemble.py:79-80](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/ensemble.py#L79-L80): New leagues with no historical seasons get `W_neural = 0.0`.

### 1.6 Semantic Market Selection
[rule_engine_manager.py:3](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine_manager.py#L3) `SemanticRuleEngine.choose_market()`:

**Guardrails**:
- [rule_engine_manager.py:31-33](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_engine_manager.py#L31-L33): If `risk_profile == "safe"` but `total_xg > 3.8`, overrides to "Over 2.5".

### 1.7 Final Output
[prediction_pipeline.py:434](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L434) `save_prediction()` writes to:
- **SQLite** [predictions](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#242-451) table (local)
- **Supabase** [predictions](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#242-451) table (cloud, via SyncManager)

**Data Quality Gate** [prediction_pipeline.py:311-313](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#L311-L313):
```python
if home_form_n < 3 or away_form_n < 3:
    skipped += 1; continue  # Not enough data to predict
```

---

## 2. RECOMMENDATION SYSTEM (Chapter 1 Page 3)

[recommend_bets.py:124](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L124) [get_recommendations()](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#123-341):

### 2.1 Data Flow
1. Loads ALL predictions from [predictions](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#242-451) table [L73-75](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L73-L75).
2. Builds market reliability index from historical outcomes [L77-121](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L77-L121).
3. Loads `fb_matches` for bookie availability [L137-139](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L137-L139).

### 2.2 Scoring Formula
[L187](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L187):
```python
total_score = (overall_acc * 0.3) + (recent_acc * 0.5) + (conf_score * 0.2)
```
- 30% weight: all-time market accuracy
- 50% weight: last 7 days accuracy (momentum)
- 20% weight: model confidence label

### 2.3 Guardrails (Filtering)
| Gate | Lines | Rule |
|:---|:---|:---|
| **Tier 1 Anchor** (>70% likelihood) | [L196-199](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L196-L199) | Always include |
| **Tier 2 Value** (40-70%) | [L196-197](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L196-L197) | Must have `score > 0.6` |
| **Tier 3 Specialist** (<40%) | [L198-199](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L198-L199) | Must have `score > 0.8` AND `recent_acc > 60%` |
| **Stairway Odds Gate** | [L203-217](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L203-L217) | Odds must be within [1.20, 4.00] |

### 2.4 Output
- `recommended.json` → Flutter app [L320-326](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L320-L326)
- `recommendations_DATE.txt` → Human audit [L309-316](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L309-L316)
- Updates [predictions](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/prediction_pipeline.py#242-451) table with `recommendation_score` and [is_available](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/inference.py#236-240) [L342-364](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#L342-L364)

---

## 3. RL TRAINING SYSTEM

### 3.1 Three-Phase Training
[trainer_phases.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/trainer_phases.py):

| Phase | Reward | Threshold |
|:---|:---|:---|
| **Phase 1** ([L41-73](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/trainer_phases.py#L41-L73)) | Accuracy-only: +1.0 correct/bettable, -0.5 wrong | Immediate |
| **Phase 2** ([L76-114](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/trainer_phases.py#L76-L114)) | Value-based: `odds - 1.0` if correct, `-1.0` if wrong | 5K odds rows + 30 days |
| **Phase 3** | Expert KL + Phase 2 reward | 15K odds rows + 60 days |

### 3.2 Expert Signal (Imitation Learning)
[trainer_phases.py:120-168](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/trainer_phases.py#L120-L168):
- Computes Poisson probabilities as the "teacher" distribution
- Blends with Rule Engine raw_scores (40% Rule Engine, 60% Poisson)
- Penalizes non-bettable markets (×0.3)
- Used as KL-divergence target during training

### 3.3 Stairway Gate (30-dim Action Filter)
[market_space.py:255-283](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/market_space.py#L255-L283):
```python
STAIRWAY_ODDS_MIN = 1.20
STAIRWAY_ODDS_MAX = 4.00
STAIRWAY_MIN_EV = -0.10  # Slightly relaxed for high-prob events
```

---

## 4. DEAD CODE vs. ALIVE CODE

### ✅ ALIVE (Actively Imported)
| File | Imported By | Evidence |
|:---|:---|:---|
| [learning_engine.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/learning_engine.py) | `rule_engine.py:16` | Loads per-league voting weights |
| [tag_generator.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/tag_generator.py) | `rule_engine.py:17` | Generates form/H2H/standings tags |
| [goal_predictor.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/goal_predictor.py) | `rule_engine.py:18` | Computes goal distributions |
| [betting_markets.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/betting_markets.py) | `rule_engine.py:19` | Generates 30-dim predictions |
| [rule_config.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rule_config.py) | `rule_engine.py:20` | Custom engine configuration |
| [ensemble.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/ensemble.py) | `prediction_pipeline.py:22` | Merges symbolic + neural |
| [market_ontology.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/market_ontology.py) | `rule_engine_manager.py:1` | Semantic market metadata |

### ❌ DEAD (Zero Imports Found)
| File | Size | Reason |
|:---|:---|:---|
| [intelligence.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/intelligence.py) | 7KB | Legacy wrapper, no imports anywhere |
| [visual_analyzer.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/visual_analyzer.py) | 12.5KB | Old "Project Vision" OCR era |
| [page_analyzer.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/page_analyzer.py) | 789B | Old browser-scraping pipeline |

---

## 5. BUGS FOUND DURING AUDIT

> [!CAUTION]
> ### BUG: `recommend_bets.py:137` — Undefined `conn` Variable
> ```python
> fb_matches = query_all(conn, 'fb_matches')  # L137
> ```
> The variable `conn` is **never defined** in the [get_recommendations()](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#123-341) function scope. The function calls [load_data()](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py#73-76) at L125 which internally uses `_get_conn()`, but that connection is not returned. This will crash at runtime when the recommendation system tries to load Football.com bookie availability data.
>
> **Fix**: Add `conn = _get_conn()` before line 137.

---

## 6. RL FRAMEWORK RECOMMENDATION

### The Question
> "What are the free, light and accurate RL systems I can use for LeoBook?"

### The Evidence-Based Answer

Your current system is a **custom PPO** with:
- 222-dim features → 30-dim actions
- LoRA league adapters (rank 16) + conditioned team adapters (rank 8)
- 3-phase training (accuracy → value → expert KL)
- Stairway gate (odds 1.20–4.00, EV ≥ -0.10)

**The core RL logic is sound.** The architecture (LoRA adapters, expert imitation, phased rewards) is sophisticated. The question is: should you keep the custom trainer or adopt a framework?

### Verdict: **Keep Custom, Upgrade Selectively**

| Option | Fit | Reasoning |
|:---|:---|:---|
| **Keep Custom** (current [trainer.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/trainer.py)) | ⭐⭐⭐⭐ | Your LoRA adapter registry, 3-phase rewards, and season-aware `data_richness_score` are deeply custom. SB3 would require significant wrapping to replicate this. |
| **Stable-Baselines3** | ⭐⭐⭐ | Best if you want battle-tested PPO internals (gradient clipping, GAE, entropy bonus). But you'd need a custom `gymnasium.Env` that wraps your entire feature encoder + reward logic. Migration cost: ~2 days. |
| **CleanRL** | ⭐⭐⭐⭐ | Single-file PPO you can fork. Closest to your current approach. You could literally copy their `ppo.py` and swap in your reward functions. Migration cost: ~1 day. |
| **Tianshou** | ⭐⭐ | Overkill for your use case. Better for multi-agent or massive-scale environments. |

### Actual Recommendation for Stairway Success

The accuracy problem isn't the RL framework — it's the **data pipeline**:
1. **Phase 2 isn't active** — you need 5,000 odds rows + 30 days of Football.com data. Check: `python Leo.py --train-rl` and look for "Phase readiness" output.
2. **The [recommend_bets.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py) bug** means [is_available](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Core/Intelligence/rl/inference.py#236-240) is never set from Football.com data, so the Flutter app can't show which matches are bookable.
3. **The ensemble weight** (`W_neural = 0.3 × data_richness`) means RL has very low influence for new leagues.

**Priority order**:
1. Fix the `conn` bug in [recommend_bets.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Scripts/recommend_bets.py) (5 minutes)
2. Run the Football.com odds harvester to populate `match_odds` (enables Phase 2)
3. Train RL with `--train-rl --phase 2` once odds threshold is met
4. Only then consider framework migration (CleanRL if you want it light)

---

## 7. DOCUMENTATION STATUS

| Document | Status | Issue |
|:---|:---|:---|
| [leobook_algorithm.md](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/leobook_algorithm.md) | ⚠️ Outdated | Doesn't mention 30-dim action space or ensemble |
| [LeoBook_Technical_Master_Report.md](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/LeoBook_Technical_Master_Report.md) | ⚠️ Partially current | Missing LoRA adapter details |
| [PROJECT_STAIRWAY.md](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/PROJECT_STAIRWAY.md) | ✅ Current | Accurately describes the 7-step compounding |
| [RULEBOOK.md](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/RULEBOOK.md) | ⚠️ Outdated | Doesn't reflect SemanticRuleEngine changes |
| [ROADMAP.md](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/ROADMAP.md) | ⚠️ Outdated | Contains completed items not marked done |
