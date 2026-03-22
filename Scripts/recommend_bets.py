# recommend_bets.py: Adaptive Learning Recommendation System (Chapter 1 Page 3).
# Part of LeoBook Scripts — Pipeline
#
# Functions: load_data(), calculate_market_reliability(), get_recommendations(),
#            save_recommendations_to_predictions_csv(), AdaptiveRecommender
#
# PURPOSE: Select the TOP 20% of predictions from Ch1 P2 for Project Stairway.
#          Uses EMA-smoothed per-market accuracy to learn which markets are
#          reliable over time and continuously improve selection quality.

import os
import sys
import argparse
import json
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from Core.Intelligence.aigo_suite import AIGOSuite

# Handle Windows terminal encoding for emojis
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.append(project_root)

from Data.Access.db_helpers import _get_conn
from Data.Access.league_db import query_all
from Data.Access.prediction_accuracy import get_market_option

# ═══════════════════════════════════════════════════════════════
# ADAPTIVE RECOMMENDER — Learns from historical accuracy
# ═══════════════════════════════════════════════════════════════

RECOMMENDER_DB = Path(project_root) / "Data" / "Store" / "recommender_weights.json"
# Stairway constants
STAIRWAY_ODDS_MIN = 1.20
STAIRWAY_ODDS_MAX = 4.00
STAIRWAY_DAILY_MIN = 2
STAIRWAY_DAILY_MAX = 8
TARGET_ACCURACY = 0.70  # 70% accuracy floor for Stairway survival


class AdaptiveRecommender:
    """
    Learning-based recommendation selector for Project Stairway.

    Tracks per-market and per-league accuracy using Exponential Moving Average (EMA).
    Each run of `learn()` updates the weights so that future `score()` calls
    naturally prefer markets/leagues that have historically been accurate.

    Weight structure:
        {
            "market_weights": {"1X2 - Home": {"ema_acc": 0.65, "n": 42}, ...},
            "league_weights": {"England - Premier League": {"ema_acc": 0.72, "n": 31}, ...},
            "confidence_weights": {"Very High": {"ema_acc": 0.78, "n": 50}, ...},
            "meta": {"last_learn": "2026-03-22", "total_learned": 1234}
        }
    """

    EMA_ALPHA = 0.15  # Smoothing factor — higher = more recent-weighting

    def __init__(self, weights_path: str = None):
        self._weights_path = Path(weights_path) if weights_path else RECOMMENDER_DB
        self.weights = self._load()

    def _load(self) -> dict:
        """Load weights from disk, or create defaults."""
        if self._weights_path.exists():
            try:
                with open(self._weights_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "market_weights": {},
            "league_weights": {},
            "confidence_weights": {},
            "meta": {"last_learn": "", "total_learned": 0}
        }

    def _save(self):
        """Persist weights to disk."""
        os.makedirs(self._weights_path.parent, exist_ok=True)
        with open(self._weights_path, 'w', encoding='utf-8') as f:
            json.dump(self.weights, f, indent=2, ensure_ascii=False)

    def learn(self, all_predictions: list):
        """
        Update EMA weights from all resolved predictions (outcome_correct known).
        Called once per recommendation cycle to keep weights fresh.
        """
        mw = self.weights["market_weights"]
        lw = self.weights["league_weights"]
        cw = self.weights["confidence_weights"]
        learned = 0

        for p in all_predictions:
            outcome = str(p.get('outcome_correct', ''))
            if outcome not in ('True', 'False', '1', '0'):
                continue

            is_correct = 1.0 if outcome in ('True', '1') else 0.0
            market = get_market_option(
                p.get('prediction', ''), p.get('home_team', ''), p.get('away_team', '')
            )
            league = p.get('region_league', 'Unknown')
            conf = p.get('confidence', 'Medium')

            # EMA update: new_ema = alpha * observation + (1 - alpha) * old_ema
            for bucket, key in [(mw, market), (lw, league), (cw, conf)]:
                if key not in bucket:
                    bucket[key] = {"ema_acc": 0.50, "n": 0}
                entry = bucket[key]
                entry["ema_acc"] = (
                    self.EMA_ALPHA * is_correct + (1 - self.EMA_ALPHA) * entry["ema_acc"]
                )
                entry["n"] += 1

            learned += 1

        self.weights["meta"]["last_learn"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        self.weights["meta"]["total_learned"] = learned
        self._save()
        return learned

    def learn_from_day(self, day_predictions: list):
        """
        Walk-forward single-day EMA update.
        Called by trainer.py for each historical date during --train-rl.

        Each item in day_predictions must have:
            - 'market': str (e.g. 'Over/Under - Over 2.5')
            - 'region_league': str
            - 'confidence': str
            - 'is_correct': bool
        """
        mw = self.weights["market_weights"]
        lw = self.weights["league_weights"]
        cw = self.weights["confidence_weights"]

        for p in day_predictions:
            is_correct = 1.0 if p.get('is_correct') else 0.0
            market = p.get('market', 'Unknown')
            league = p.get('region_league', 'Unknown')
            conf = p.get('confidence', 'Medium')

            for bucket, key in [(mw, market), (lw, league), (cw, conf)]:
                if key not in bucket:
                    bucket[key] = {"ema_acc": 0.50, "n": 0}
                entry = bucket[key]
                entry["ema_acc"] = (
                    self.EMA_ALPHA * is_correct + (1 - self.EMA_ALPHA) * entry["ema_acc"]
                )
                entry["n"] += 1

        self.weights["meta"]["total_learned"] = (
            self.weights["meta"].get("total_learned", 0) + len(day_predictions)
        )
        self._save()

    def select_top_picks(self, candidates: list, min_picks: int = 2, max_picks: int = 8) -> list:
        """
        Score and select top 20% of candidates, bounded by [min_picks, max_picks].
        Returns the selected candidates sorted by score descending.
        """
        for c in candidates:
            c['rec_score'] = self.score(c, c.get('market', 'Unknown'))
        candidates.sort(key=lambda x: x['rec_score'], reverse=True)
        n = max(min_picks, int(len(candidates) * 0.20))
        n = min(n, max_picks, len(candidates))
        return candidates[:n]

    def copy_to_production(self):
        """Copy training weights to production path."""
        import shutil
        if self._weights_path != RECOMMENDER_DB and self._weights_path.exists():
            shutil.copy2(self._weights_path, RECOMMENDER_DB)
            print(f"  [Recommender] Training weights copied to production: {RECOMMENDER_DB}")

    def score(self, prediction: dict, market: str) -> float:
        """
        Compute adaptive recommendation score for a single prediction.

        Score components:
            40%  Market EMA accuracy (learned)
            25%  League EMA accuracy (learned)
            20%  Confidence label EMA accuracy (learned)
            15%  Recency bonus (predictions with recent positive momentum)

        Returns float in [0, 1].
        """
        mw = self.weights.get("market_weights", {})
        lw = self.weights.get("league_weights", {})
        cw = self.weights.get("confidence_weights", {})

        league = prediction.get('region_league', 'Unknown')
        conf = prediction.get('confidence', 'Medium')

        # Get learned accuracies (default 0.50 for unknown entities)
        market_acc = mw.get(market, {}).get("ema_acc", 0.50)
        league_acc = lw.get(league, {}).get("ema_acc", 0.50)
        conf_acc = cw.get(conf, {}).get("ema_acc", 0.50)

        # Recency bonus: boost if learned sample size is large (stable signal)
        market_n = mw.get(market, {}).get("n", 0)
        recency_bonus = min(market_n / 100.0, 1.0)  # caps at n=100

        total = (
            market_acc * 0.40 +
            league_acc * 0.25 +
            conf_acc * 0.20 +
            recency_bonus * 0.15
        )
        return round(total, 4)


# ═══════════════════════════════════════════════════════════════
# MARKET LIKELIHOOD PRIORS (unchanged)
# ═══════════════════════════════════════════════════════════════

_LIKELIHOOD_CACHE = None

def _load_likelihood_map():
    """Load market likelihood JSON and build a lookup by market_outcome."""
    global _LIKELIHOOD_CACHE
    if _LIKELIHOOD_CACHE is not None:
        return _LIKELIHOOD_CACHE
    _LIKELIHOOD_CACHE = {}
    json_path = os.path.join(project_root, "Data", "Store", "ranked_markets_likelihood_updated_with_team_ou.json")
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for entry in data.get("ranked_market_outcomes", []):
            key = entry.get("market_outcome", "")
            _LIKELIHOOD_CACHE[key] = entry.get("likelihood_percent", 50)
    except Exception:
        pass
    return _LIKELIHOOD_CACHE

def get_market_likelihood(market_name: str) -> float:
    """Get base likelihood (0-100) for a market outcome string."""
    lmap = _load_likelihood_map()
    if market_name in lmap:
        return lmap[market_name]
    for key, val in lmap.items():
        if market_name.lower() in key.lower() or key.lower() in market_name.lower():
            return val
    return 50.0

def classify_tier(likelihood: float) -> int:
    """Classify market into likelihood tier. 1=anchor, 2=value, 3=specialist."""
    if likelihood > 70:
        return 1
    elif likelihood >= 40:
        return 2
    else:
        return 3


# ═══════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════

def load_data():
    conn = _get_conn()
    return query_all(conn, 'predictions')

def calculate_market_reliability(predictions):
    """Calculates accuracy for each market type based on historical results."""
    market_stats = {}
    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)

    for p in predictions:
        outcome = str(p.get('outcome_correct', ''))
        if outcome not in ['True', 'False', '1', '0']:
            continue

        try:
            p_date_str = p.get('date', '')
            if '-' in p_date_str:
                p_date = datetime.strptime(p_date_str, "%Y-%m-%d")
            else:
                p_date = datetime.strptime(p_date_str, "%d.%m.%Y")
        except:
            continue

        market = get_market_option(p.get('prediction', ''), p.get('home_team', ''), p.get('away_team', ''))
        if market not in market_stats:
            market_stats[market] = {'total': 0, 'correct': 0, 'recent_total': 0, 'recent_correct': 0}

        market_stats[market]['total'] += 1
        if outcome in ('True', '1'):
            market_stats[market]['correct'] += 1

        if p_date >= seven_days_ago:
            market_stats[market]['recent_total'] += 1
            if outcome in ('True', '1'):
                market_stats[market]['recent_correct'] += 1

    reliability = {}
    for m, stats in market_stats.items():
        overall = stats['correct'] / stats['total'] if stats['total'] >= 3 else 0.5
        recent = stats['recent_correct'] / stats['recent_total'] if stats['recent_total'] >= 2 else overall
        reliability[m] = {
            'overall': overall,
            'recent': recent,
            'trend': recent - overall
        }

    return reliability


# ═══════════════════════════════════════════════════════════════
# MAIN RECOMMENDATION ENGINE
# ═══════════════════════════════════════════════════════════════

@AIGOSuite.aigo_retry(max_retries=3, delay=1.0, use_aigo=False)
def get_recommendations(target_date=None, show_all_upcoming=False, **kwargs):
    all_predictions = load_data()
    if not all_predictions:
        print("[ALGO] No predictions found.")
        return {'status': 'empty', 'total': 0, 'scored': 0}

    print(f"[ALGO] Loaded {len(all_predictions)} predictions. Running adaptive learning...")

    # ── Step 0: Train the adaptive recommender from resolved predictions ──
    recommender = AdaptiveRecommender()
    learned = recommender.learn(all_predictions)
    print(f"[ALGO] Adaptive recommender trained on {learned} resolved outcomes.")

    # ── Step 1: Build reliability index (backward-compatible) ──
    reliability = calculate_market_reliability(all_predictions)
    print(f"[ALGO] Built reliability index for {len(reliability)} market types.")

    # ── Step 1.1: Load available bookie matches (Football.com) ──
    # FIX: `conn` was previously undefined — now properly initialized
    conn = _get_conn()
    fb_matches = query_all(conn, 'fb_matches')
    available_fids = {m['fixture_id'] for m in fb_matches if m.get('fixture_id')}
    print(f"[ALGO] Found {len(available_fids)} matches available in bookie (Football.com).")

    # ── Step 2: Filter for future matches ──
    now = datetime.now()
    candidates = []

    for p in all_predictions:
        if p.get('status') in ['reviewed', 'match_canceled']:
            continue

        try:
            p_date_str = p.get('date')
            p_time_str = p.get('match_time')
            if not p_date_str or not p_time_str or p_time_str == 'N/A':
                continue

            fmt = "%Y-%m-%d" if '-' in p_date_str else "%d.%m.%Y"
            p_dt = datetime.strptime(f"{p_date_str} {p_time_str}", f"{fmt} %H:%M")

            # Date Filtering
            if target_date:
                if p_date_str != target_date and p_dt.strftime("%d.%m.%Y") != target_date and p_dt.strftime("%Y-%m-%d") != target_date:
                    continue
            elif not show_all_upcoming:
                today_iso = now.strftime("%Y-%m-%d")
                today_eu = now.strftime("%d.%m.%Y")
                if p_date_str != today_iso and p_date_str != today_eu:
                    continue
                if p_dt <= now:
                    continue
            else:
                if p_dt <= now:
                    continue

            # Classify market
            market = get_market_option(p.get('prediction', ''), p.get('home_team', ''), p.get('away_team', ''))
            rel_info = reliability.get(market, {'overall': 0.5, 'recent': 0.5, 'trend': 0.0})
            likelihood = get_market_likelihood(market)
            tier = classify_tier(likelihood)

            # ── Tier Gate (hard filter) ──
            # Tier 1 (>70%): anchor — always include
            # Tier 2 (40-70%): value — include when adaptive score > 0.55
            # Tier 3 (<40%): specialist — include only when adaptive score > 0.70
            adaptive_score = recommender.score(p, market)

            if tier == 2 and adaptive_score <= 0.55:
                continue
            if tier == 3 and adaptive_score <= 0.70:
                continue

            # ── Stairway Odds Gate ──
            raw_odds = p.get('odds', '')
            odds_value = None
            odds_status = 'missing'
            if raw_odds and str(raw_odds).strip():
                try:
                    odds_value = float(str(raw_odds).strip().replace(',', '.'))
                    if STAIRWAY_ODDS_MIN <= odds_value <= STAIRWAY_ODDS_MAX:
                        odds_status = 'in_range'
                    else:
                        odds_status = 'out_of_range'
                        continue  # Skip — outside Stairway bettable range
                except (ValueError, TypeError):
                    odds_status = 'unparseable'

            tier_labels = {1: "⚓ Anchor", 2: "💎 Value", 3: "🎯 Specialist"}
            trend_icon = "↗️" if rel_info['trend'] > 0.05 else "↘️" if rel_info['trend'] < -0.05 else "➡️" if rel_info['trend'] != 0 else ""

            candidates.append({
                'match': f"{p['home_team']} vs {p['away_team']}",
                'fixture_id': p.get('fixture_id', ''),
                'time': p_time_str,
                'date': p_date_str,
                'prediction': p['prediction'],
                'market': market,
                'confidence': p['confidence'],
                'overall_acc': f"{rel_info['overall']:.1%}",
                'recent_acc': f"{rel_info['recent']:.1%}",
                'trend': trend_icon,
                'score': adaptive_score,
                'league': p.get('region_league', 'Unknown'),
                'tier': tier,
                'tier_label': tier_labels.get(tier, ""),
                'likelihood': likelihood,
                'odds': odds_value,
                'odds_status': odds_status,
                'is_available': p.get('fixture_id') in available_fids,
            })
        except Exception:
            continue

    # ── Step 3: Rank and select top 20% (Stairway constraint: 2-8 per day) ──
    candidates.sort(key=lambda x: x['score'], reverse=True)

    # Top 20% selection with Stairway bounds
    top_20_pct = max(STAIRWAY_DAILY_MIN, int(len(candidates) * 0.20))
    top_20_pct = min(top_20_pct, STAIRWAY_DAILY_MAX)
    recommendations = candidates[:top_20_pct]

    # ── Step 4: Console Output ──
    tier_counts = {1: 0, 2: 0, 3: 0}
    for r in recommendations:
        tier_counts[r['tier']] = tier_counts.get(r['tier'], 0) + 1

    high_conf = [r for r in recommendations if r['score'] >= 0.65]
    print(f"[ALGO] Candidates: {len(candidates)} | Selected top {len(recommendations)} (top 20%)")
    print(f"[ALGO] Tiers: ⚓ Anchor={tier_counts[1]} | 💎 Value={tier_counts[2]} | 🎯 Specialist={tier_counts[3]}")
    print(f"[ALGO] High-adaptive (≥0.65): {len(high_conf)}")
    if recommendations:
        print(f"[ALGO] Top: {recommendations[0]['score']:.3f} — {recommendations[0]['match']}")

    title = "PROJECT STAIRWAY — RECOMMENDATIONS"
    if target_date:
        title += f" FOR {target_date}"
    elif show_all_upcoming:
        title += " (ALL UPCOMING)"
    else:
        title += " (TODAY)"

    output_lines = []
    output_lines.append(f"\n{'='*65}")
    output_lines.append(f"{title:^65}")
    output_lines.append(f"{'='*65}\n")

    if not recommendations:
        output_lines.append("No matches found for the selected criteria.")
    else:
        for i, rec in enumerate(recommendations, 1):
            avail_badge = "🟢" if rec['is_available'] else "⚪"
            output_lines.append(f"{i}. {avail_badge} {rec['match']} [{rec['league']}]")
            output_lines.append(f"   Time: {rec['date']} {rec['time']}")
            output_lines.append(f"   Prediction: {rec['prediction']} ({rec['confidence']})")
            output_lines.append(f"   Adaptive Score: {rec['score']:.3f} | {rec['tier_label']} (Likelihood: {rec['likelihood']:.0f}%)")
            if rec['odds']:
                output_lines.append(f"   Odds: {rec['odds']:.2f} ({rec['odds_status']})")
            output_lines.append(f"   Historical: Recent {rec['recent_acc']} {rec['trend']} | Overall {rec['overall_acc']}")
            output_lines.append(f"{'-'*65}")

    # Print to console
    print(f"\n{'='*65}")
    print(f"{title:^65}")
    print(f"{'='*65}\n")
    if not recommendations:
        print("No matches found for the selected criteria.")
    else:
        for i, rec in enumerate(recommendations, 1):
            avail_badge = "🟢" if rec['is_available'] else "⚪"
            print(f"{i}. {avail_badge} {rec['match']} [{rec['league']}]")
            print(f"   Time: {rec['date']} {rec['time']}")
            print(f"   Prediction: \033[92m{rec['prediction']}\033[0m ({rec['confidence']})")
            print(f"   Adaptive Score: \033[93m{rec['score']:.3f}\033[0m | {rec['tier_label']} (Likelihood: {rec['likelihood']:.0f}%)")
            if rec['odds']:
                print(f"   Odds: {rec['odds']:.2f} ({rec['odds_status']})")
            print(f"   Historical: Recent {rec['recent_acc']} {rec['trend']} | Overall {rec['overall_acc']}")
            print(f"{'-'*65}")

    # ── Step 5: Save ──
    if kwargs.get('save_to_file'):
        p_root = project_root
        recommendations_dir = os.path.join(p_root, "Data", "Store", "RecommendedBets")
        os.makedirs(recommendations_dir, exist_ok=True)

        file_date = target_date if target_date else now.strftime("%d.%m.%Y")

        # Human-readable TXT
        file_path_txt = os.path.join(recommendations_dir, f"recommendations_{file_date}.txt")
        try:
            with open(file_path_txt, 'w', encoding='utf-8') as f:
                f.write("\n".join(output_lines))
            print(f"\n[OK] Recommendations (TXT) saved to: {file_path_txt}")
        except Exception as e:
            print(f"\n[Error] Failed to save TXT recommendations: {e}")

        # Structured JSON for Flutter app
        json_path = os.path.join(p_root, "Data", "Store", "recommended.json")
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(recommendations, f, ensure_ascii=False, indent=2)
            print(f"[OK] Recommendations (JSON) saved to: {json_path}")
        except Exception as e:
            print(f"[Error] Failed to save JSON recommendations: {e}")

        # Update predictions table
        save_recommendations_to_predictions_csv(recommendations)

    return {
        'status': 'ok',
        'total': len(all_predictions),
        'candidates': len(candidates),
        'scored': len(recommendations),
        'high_confidence': len(high_conf),
        'top_score': recommendations[0]['score'] if recommendations else 0,
        'recommendations': recommendations
    }


def save_recommendations_to_predictions_csv(recommendations):
    """Updates predictions with recommendation_score and is_available."""
    from Data.Access.league_db import update_prediction
    conn = _get_conn()

    rec_map = {r['fixture_id']: r for r in recommendations if r.get('fixture_id')}
    rec_map_teams = {f"{r['match']}_{r['date']}": r for r in recommendations}
    updates_count = 0

    all_preds = query_all(conn, 'predictions')
    for row in all_preds:
        fid = row.get('fixture_id')
        match_key = f"{row.get('home_team')} vs {row.get('away_team')}_{row.get('date')}"
        matched_rec = rec_map.get(fid) or rec_map_teams.get(match_key)
        if matched_rec:
            update_data = {
                'recommendation_score': str(round(matched_rec['score'], 4)),
                'is_available': 1 if matched_rec.get('is_available') else 0
            }
            update_prediction(conn, fid, update_data)
            updates_count += 1

    print(f"[ALGO] Updated predictions: {updates_count} scored out of {len(all_preds)} total rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Project Stairway — Adaptive Recommendations.")
    parser.add_argument("--date", help="Target date (DD.MM.YYYY or YYYY-MM-DD)")
    parser.add_argument("--all", action="store_true", help="Show all upcoming matches")
    parser.add_argument("--save", action="store_true", help="Save recommendations to DB and update CSV")
    args = parser.parse_args()

    get_recommendations(target_date=args.date, show_all_upcoming=args.all, save_to_file=args.save)
