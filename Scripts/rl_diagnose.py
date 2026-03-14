# rl_diagnose.py: Per-match RL decision inspector — shows all 30 action probs, EV, Kelly, and Stairway Gate.
# Part of LeoBook Scripts — Intelligence (RL Diagnostics)
#
# Functions: load_model(), run_inference(), display(), main()
# Called by: Leo.py (--diagnose-rl)

"""
Usage:
  python Leo.py --diagnose-rl                                           # Latest 5 upcoming fixtures
  python Leo.py --diagnose-rl --fixture FIXTURE_ID                      # Specific fixture
  python Leo.py --diagnose-rl --top 10                                  # Top 10 upcoming fixtures
  python Leo.py --diagnose-rl --checkpoint path.pth                     # Use a specific checkpoint
  python Leo.py --diagnose-rl --all-played --top 3                      # Last 3 COMPLETED matches
  python Leo.py --diagnose-rl --checkpoint Data/Store/models/checkpoints/phase1_day038.pth

Bug fixes applied (2026-03-14):
  FIX-A: --all-played returned upcoming (unplayed) fixtures because the query
          filtered on home_score IS NOT NULL but did not also require match_status
          to indicate the match was actually finished. Added explicit match_status
          filter: finished OR (score present AND date < today). Also added
          AND date <= today_str to exclude future matches that happen to have
          scores pre-populated in the DB.
  FIX-B: time=None printed literally as "None" in the fixture header. Now
          replaced with an empty string when time is absent.
  FIX-C: --diagnose-rl without --all-played called get_weekly_fixtures() which
          returns upcoming matches; some of those had no time set (pre-season
          enrichment rows). Added a guard to skip fixtures with no date.
"""

import sys
import os
import argparse
import numpy as np
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import torch
from Data.Access.league_db import init_db
from Core.Intelligence.rl.model import LeoBookRLModel
from Core.Intelligence.rl.feature_encoder import FeatureEncoder
from Core.Intelligence.rl.adapter_registry import AdapterRegistry
from Core.Intelligence.rl.market_space import (
    ACTIONS, N_ACTIONS, stairway_gate, SYNTHETIC_ODDS,
)
from Core.Intelligence.prediction_pipeline import (
    build_rule_engine_input, get_weekly_fixtures,
)


def hr(char="─", width=90):
    print(char * width)


def load_model(checkpoint_path=None):
    device = torch.device("cpu")
    registry = AdapterRegistry()
    default_path = os.path.join(PROJECT_ROOT, "Data", "Store", "models", "leobook_base.pth")
    path = checkpoint_path or default_path

    if not os.path.exists(path):
        print(f"  ✗ Checkpoint not found: {path}")
        sys.exit(1)

    model = LeoBookRLModel().to(device)

    # Support both bare state_dict saves and full checkpoint dicts
    raw = torch.load(path, map_location=device, weights_only=False)
    if isinstance(raw, dict) and "model_state" in raw:
        state_dict = raw["model_state"]
        print(f"  ✓ Loaded full checkpoint: {os.path.basename(path)}")
        if "phase" in raw:
            print(f"    Phase: {raw['phase']}  |  Day: {raw.get('day','?')}/{raw.get('total_days','?')}")
        if "total_matches" in raw:
            print(f"    Matches trained: {raw['total_matches']}  |  Correct: {raw.get('correct_predictions','?')}")
    else:
        state_dict = raw
        print(f"  ✓ Loaded: {os.path.basename(path)}")

    model.load_state_dict(state_dict, strict=False)
    model.eval()

    pc = model.count_parameters()
    print(f"    Params: {pc['total']:,} (trunk:{pc['trunk']:,}  heads:{pc['heads']:,}  adapters:{pc['league_adapters']+pc['team_adapters']:,})")
    return model, registry, device


def run_inference(model, registry, device, vision_data, fixture):
    features = FeatureEncoder.encode(vision_data).to(device)

    l_idx = registry.get_league_idx(fixture.get("league_id", "GLOBAL"))
    h_idx = registry.get_team_idx(fixture.get("home_team_id", "GLOBAL"))
    a_idx = registry.get_team_idx(fixture.get("away_team_id", "GLOBAL"))

    with torch.no_grad():
        policy_logits, value, stake = model(features, l_idx, h_idx, a_idx)
        action_probs = torch.softmax(policy_logits, dim=-1).squeeze()

    rl_ev = value.item()

    actions = []
    for i, act in enumerate(ACTIONS):
        key = act["key"]
        prob = action_probs[i].item()
        odds = SYNTHETIC_ODDS.get(key, 0.0)

        # Calibrated true win probability via value head EV back-calculation.
        # EV = p × odds - 1  →  p = (EV + 1) / odds
        if odds > 0.0 and key != "no_bet":
            true_prob = (rl_ev + 1.0) / odds
            true_prob = max(0.0, min(1.0, true_prob))
        else:
            true_prob = prob

        bettable, reason = stairway_gate(key, None, true_prob)
        ev = (true_prob * odds) - 1.0 if odds > 0 else None

        actions.append({
            "idx": i, "key": key, "market": act["market"],
            "outcome": act["outcome"], "line": act["line"],
            "prob": prob,
            "true_prob": true_prob,
            "odds": odds, "ev": ev,
            "bettable": bettable, "reason": reason,
            "base_lk": act["likelihood"],
        })
    actions.sort(key=lambda x: x["prob"], reverse=True)

    return {
        "actions": actions, "top": actions[0],
        "value": rl_ev, "kelly": stake.item() * 5.0,
        "l_idx": l_idx, "h_idx": h_idx, "a_idx": a_idx,
    }


def summarize_form(form, team):
    w = d = l = 0
    scored_list, conceded_list = [], []
    for m in form:
        try:
            gf, ga = map(int, m.get("score", "0-0").replace(" ", "").split("-"))
        except (ValueError, AttributeError):
            continue
        is_home = m.get("home", "") == team
        tg, og = (gf, ga) if is_home else (ga, gf)
        scored_list.append(tg)
        conceded_list.append(og)
        if tg > og: w += 1
        elif tg == og: d += 1
        else: l += 1
    avg_s = np.mean(scored_list) if scored_list else 0
    avg_c = np.mean(conceded_list) if conceded_list else 0
    btts = sum(1 for s, c in zip(scored_list, conceded_list) if s > 0 and c > 0) / max(len(scored_list), 1)
    o25 = sum(1 for s, c in zip(scored_list, conceded_list) if s + c > 2) / max(len(scored_list), 1)
    return w, d, l, avg_s, avg_c, btts, o25


def display(fixture, vision_data, result):
    h2h_data = vision_data.get("h2h_data", {})
    home = h2h_data.get("home_team", "?")
    away = h2h_data.get("away_team", "?")
    league = h2h_data.get("region_league", "?")
    home_form = [m for m in h2h_data.get("home_last_10_matches", []) if m][:10]
    away_form = [m for m in h2h_data.get("away_last_10_matches", []) if m][:10]
    h2h = [m for m in h2h_data.get("head_to_head", []) if m]
    standings = vision_data.get("standings", [])

    # Score (if played)
    hs = fixture.get("home_score")
    as_ = fixture.get("away_score")
    score_str = f"  Final: {hs}-{as_}" if hs is not None and as_ is not None else ""

    # FIX-B: replace None time with empty string
    match_time = fixture.get("time") or ""

    print()
    hr("═")
    print(f"  ⚽  {home}  vs  {away}{score_str}")
    print(f"  📅 {fixture.get('date','?')} {match_time}  |  🏆 {league}")
    print(f"  🔑 fixture: {fixture.get('fixture_id','?')}")
    print(f"  🧠 Adapters: league={result['l_idx']}  home={result['h_idx']}  away={result['a_idx']}")
    hr("═")

    # ── Features ──
    print("\n── FEATURE INPUTS (what the model sees) ──────────────────")
    home_xg = FeatureEncoder._compute_xg(home_form, home, is_home=True)
    away_xg = FeatureEncoder._compute_xg(away_form, away, is_home=False)
    print(f"  xG:    Home={home_xg:.2f}  Away={away_xg:.2f}  Diff={home_xg-away_xg:+.2f}  Total={home_xg+away_xg:.2f}")

    hw, hd, hl, hs_avg, hc_avg, hbtts, ho25 = summarize_form(home_form, home)
    aw, ad, al, as_avg, ac_avg, abtts, ao25 = summarize_form(away_form, away)
    print(f"  {home:20s} Form: W{hw} D{hd} L{hl}  ({len(home_form)} matches)")
    print(f"  {'':20s} Goals: {hs_avg:.1f} scored/g  {hc_avg:.1f} conceded/g  BTTS={hbtts:.0%}  O2.5={ho25:.0%}")
    print(f"  {away:20s} Form: W{aw} D{ad} L{al}  ({len(away_form)} matches)")
    print(f"  {'':20s} Goals: {as_avg:.1f} scored/g  {ac_avg:.1f} conceded/g  BTTS={abtts:.0%}  O2.5={ao25:.0%}")
    print(f"  H2H:   {len(h2h)} matches found")
    print(f"  Table: {len(standings)} teams in standings")
    print(f"  Total: 222-dim feature vector")

    # ── Model Head Outputs ──
    top = result["top"]
    print("\n── MODEL HEAD OUTPUTS ────────────────────────────────────")
    print(f"  🎯 Top Action:  {top['key']}  ({top['market']} → {top['outcome']}{' '+top['line'] if top['line'] else ''})")
    print(f"  📊 Action Prob: {top['prob']:.4f}  ({top['prob']*100:.1f}%)  [model preference across 30 actions]")
    print(f"  🎯 True Win P:  {top['true_prob']:.4f}  ({top['true_prob']*100:.1f}%)  [calibrated from value head]")
    print(f"  💰 Expected EV: {result['value']:+.4f}")
    print(f"  📐 Kelly:       {result['kelly']:.2f}%")
    gate = "✅ PASS" if top["bettable"] else "❌ FAIL"
    print(f"  🚪 Gate:        {gate} — {top['reason']}")

    # ── All 30 Actions ──
    print("\n── ALL 30 ACTIONS (ranked by action preference) ─────────")
    print(f"  {'#':>3s}  {'Action':<22s} {'Market → Outcome':<32s} {'ActProb':>7s} {'TrueP':>7s} {'Odds':>6s} {'EV':>8s} {'Gate':>4s}  Reason")
    hr("─")

    for rank, a in enumerate(result["actions"], 1):
        prob_s = f"{a['prob']*100:.1f}%"
        truep_s = f"{a['true_prob']*100:.1f}%"
        odds_s = f"{a['odds']:.2f}" if a["odds"] > 0 else "  —"
        ev_s = f"{a['ev']:+.3f}" if a["ev"] is not None else "   —"
        gate_s = " ✅" if a["bettable"] else " ❌"
        mo = f"{a['market']} → {a['outcome']}{' '+a['line'] if a['line'] else ''}"
        marker = " ◄" if rank == 1 else ""
        print(f"  {rank:3d}  {a['key']:<22s} {mo:<32s} {prob_s:>7s} {truep_s:>7s} {odds_s:>6s} {ev_s:>8s} {gate_s}  {a['reason']}{marker}")

    # ── Bettable Summary ──
    bettable = [a for a in result["actions"] if a["bettable"]]
    print()
    if bettable:
        print(f"  ✅ {len(bettable)} action(s) pass Stairway Gate (1.20–4.00 odds + EV≥0):")
        for a in bettable:
            print(f"     {a['key']:22s}  true_p={a['true_prob']:.3f}  odds={a['odds']:.2f}  EV={a['ev']:+.3f}")
    else:
        print("  ❌ No actions pass the Stairway Gate for this match.")
    print()


def main(args=None):
    """Entry point — callable from Leo.py or standalone."""
    if args is None:
        parser = argparse.ArgumentParser(description="LeoBook RL Decision Inspector")
        parser.add_argument("--fixture", type=str, help="Specific fixture_id")
        parser.add_argument("--top", type=int, default=5, help="Number of fixtures (default: 5)")
        parser.add_argument("--checkpoint", type=str, help="Path to .pth checkpoint")
        parser.add_argument("--all-played", action="store_true", dest="all_played",
                            help="Inspect recent completed matches")
        args, _ = parser.parse_known_args()

    hr("═")
    print("  🧠 LeoBook RL Decision Inspector")
    print("  See exactly what the model thinks about each match.")
    hr("═")
    print()

    model, registry, device = load_model(getattr(args, "checkpoint", None))
    conn = init_db()

    today_str = datetime.now().strftime("%Y-%m-%d")

    if getattr(args, "fixture", None):
        # ── Single fixture deep-dive ──────────────────────────────────────────
        row = conn.execute(
            """SELECT h.name AS home_team_name, a.name AS away_team_name, s.*
               FROM schedules s
               LEFT JOIN teams h ON s.home_team_id = h.team_id
               LEFT JOIN teams a ON s.away_team_id = a.team_id
               WHERE s.fixture_id = ?""",
            (args.fixture,),
        ).fetchone()
        if not row:
            print(f"  ✗ Fixture not found: {args.fixture}")
            sys.exit(1)
        fixtures = [dict(row)]

    elif getattr(args, "all_played", False):
        # ── FIX-A: Recent COMPLETED matches ─────────────────────────────────
        # Original query: WHERE home_score IS NOT NULL — this also matched
        # upcoming matches that had scores pre-populated (enrichment artefacts)
        # or rows without a match_status.
        #
        # Fixed query adds:
        #   1. AND date <= today_str  — exclude any future-dated rows
        #   2. AND (match_status = 'finished' OR match_status IS NULL)
        #      — prefer finished, fall back to NULL-status rows that have scores
        #      (legacy enriched rows where status was not captured)
        #   3. ORDER BY date DESC, time DESC  — most recent first
        rows = conn.execute(
            """SELECT h.name AS home_team_name, a.name AS away_team_name, s.*
               FROM schedules s
               LEFT JOIN teams h ON s.home_team_id = h.team_id
               LEFT JOIN teams a ON s.away_team_id = a.team_id
               WHERE s.home_score IS NOT NULL AND s.away_score IS NOT NULL
                 AND s.home_score != '' AND s.away_score != ''
                 AND s.date IS NOT NULL AND s.date <= ?
                 AND (s.match_status = 'finished' OR s.match_status IS NULL)
               ORDER BY s.date DESC, s.time DESC
               LIMIT ?""",
            (today_str, getattr(args, "top", 5)),
        ).fetchall()
        fixtures = [dict(r) for r in rows]
        if not fixtures:
            print("  No completed matches found. Check your DB has finished fixtures "
                  "with home_score/away_score populated and date <= today.")
            sys.exit(0)

    else:
        # ── Upcoming fixtures ────────────────────────────────────────────────
        # FIX-C: filter out fixtures with no date (pre-season enrichment rows)
        raw_fixtures = get_weekly_fixtures(conn)
        fixtures = [
            f for f in raw_fixtures
            if f.get("date") and f["date"] >= today_str
        ][:getattr(args, "top", 5)]

    if not fixtures:
        print("  No fixtures found.")
        print("  • For upcoming matches: ensure enrichment has run for this week.")
        print("  • For played matches:   use --all-played flag.")
        sys.exit(0)

    print(f"\n  Diagnosing {len(fixtures)} fixture(s)...\n")

    for fixture in fixtures:
        try:
            vision_data = build_rule_engine_input(conn, fixture)
            h2h_data = vision_data.get("h2h_data", {})
            hf = len([m for m in h2h_data.get("home_last_10_matches", []) if m])
            af = len([m for m in h2h_data.get("away_last_10_matches", []) if m])
            if hf < 1 and af < 1:
                home_name = fixture.get("home_team_name") or fixture.get("home_team", "?")
                away_name = fixture.get("away_team_name") or fixture.get("away_team", "?")
                print(f"  ⚠ Skipping {home_name} vs {away_name} — no form data available")
                continue
            result = run_inference(model, registry, device, vision_data, fixture)
            display(fixture, vision_data, result)
        except Exception as e:
            home_name = fixture.get("home_team_name") or fixture.get("home_team", "?")
            away_name = fixture.get("away_team_name") or fixture.get("away_team", "?")
            print(f"  ✗ Error: {home_name} vs {away_name}: {e}")
            import traceback
            traceback.print_exc()

    hr("═")
    print("  End of diagnosis. Use --fixture ID for deep-dive on a specific match.")
    hr("═")


if __name__ == "__main__":
    main()