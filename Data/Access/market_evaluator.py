# market_evaluator.py: Unified market outcome evaluator for LeoBook.
# Part of LeoBook Data — Access Layer
# Pure function — no I/O, no DB access.
# Imported by: db_helpers.py, outcome_reviewer.py

import re
from typing import Optional


def evaluate_market_outcome(prediction: str, home_score: str, away_score: str,
                            home_team: str = "", away_team: str = "",
                            match_status: str = "") -> Optional[str]:
    """
    Unified First-Principles Outcome Evaluator (v5.0).
    Returns '1' (Correct), '0' (Incorrect), or '' (Unknown/Void).

    Settlement is based on 90min + stoppage time (regulation FT) ONLY.
    If match_status is 'aet'/'pen'/'after pen', the match was a DRAW at FT,
    so any draw-component prediction (1X, X2, draw) wins immediately.

    Handles: 1X2, Double Chance, DNB, Over/Under, BTTS, Team Over/Under,
             Winner & BTTS, Clean Sheet, and team-specific predictions.
    """
    try:
        h = int(home_score)
        a = int(away_score)
        total = h + a
    except (ValueError, TypeError):
        return ''

    p = (prediction or '').strip().lower()
    h_lower = (home_team or '').strip().lower()
    a_lower = (away_team or '').strip().lower()
    status = (match_status or '').strip().lower()

    # AET/Pen detection: match went beyond 90min = it was a DRAW at regulation FT.
    is_regulation_draw = status in ('aet', 'pen', 'after pen', 'after extra time',
                                    'after penalties', 'ap', 'finished aet',
                                    'finished ap', 'finished pen')
    if is_regulation_draw:
        _draw_markets = ('draw', 'x', '1x', 'x2', 'home or draw', 'home_or_draw',
                         'away or draw', 'away_or_draw', 'draw or away',
                         'double chance 1x', 'double chance x2')
        if p in _draw_markets or ' or draw' in p or 'draw or ' in p:
            return '1'
        if p in ('home win', 'home_win', '1', 'away win', 'away_win', '2'):
            return '0'
        if p.endswith(' to win') and 'btts' not in p:
            return '0'
        if '(dnb)' in p:
            return ''

    def _team_matches(candidate: str, reference: str) -> bool:
        if not candidate or not reference:
            return False
        return candidate == reference or reference.startswith(candidate) or candidate.startswith(reference)

    def _is_home(team_str: str) -> bool:
        return _team_matches(team_str, h_lower)

    def _is_away(team_str: str) -> bool:
        return _team_matches(team_str, a_lower)

    # 0. Winner & BTTS (team to win & btts yes)
    btts_win_match = re.match(r'^(.+?)\s+to\s+win\s*&\s*btts\s+yes$', p)
    if btts_win_match:
        team = btts_win_match.group(1).strip()
        btts = h > 0 and a > 0
        if _is_home(team): return '1' if h > a and btts else '0'
        if _is_away(team): return '1' if a > h and btts else '0'

    # 0a. 1X2 & BTTS combos (e.g. "1 & gg", "2 & btts yes", "x & gg")
    ixb = re.match(r'^(1|2|x|home win|away win|draw)\s*&\s*(gg|btts\s*yes|both teams to score)$', p)
    if ixb:
        result_part = ixb.group(1).strip()
        btts = h > 0 and a > 0
        if result_part in ('1', 'home win'): return '1' if h > a and btts else '0'
        if result_part in ('2', 'away win'): return '1' if a > h and btts else '0'
        if result_part in ('x', 'draw'): return '1' if h == a and btts else '0'

    # 0b. 1X2 & Over/Under combos (e.g. "1 & over 2.5", "home & over 2.5")
    ixo = re.match(r'^(1|2|x|home win|away win|draw|home|away)\s*&\s*(over|under)\s+([\d.]+)$', p)
    if ixo:
        result_part = ixo.group(1).strip()
        direction = ixo.group(2)
        threshold = float(ixo.group(3))
        if direction == 'over':
            goals_ok = total > threshold
        else:
            goals_ok = total < threshold
        if result_part in ('1', 'home win', 'home'): return '1' if h > a and goals_ok else '0'
        if result_part in ('2', 'away win', 'away'): return '1' if a > h and goals_ok else '0'
        if result_part in ('x', 'draw'): return '1' if h == a and goals_ok else '0'

    # 0c. Double Chance & BTTS combos (e.g. "1x & gg", "x2 & btts yes", "12 & gg")
    dcb = re.match(r'^(1x|x2|12|home or draw|away or draw|home or away)\s*&\s*(gg|btts\s*yes|both teams to score|ng|btts\s*no)$', p)
    if dcb:
        dc_part = dcb.group(1).strip()
        btts_part = dcb.group(2).strip()
        if btts_part in ('gg', 'btts yes', 'both teams to score'):
            btts = h > 0 and a > 0
        else:
            btts = h == 0 or a == 0
        if dc_part in ('1x', 'home or draw'): return '1' if h >= a and btts else '0'
        if dc_part in ('x2', 'away or draw'): return '1' if a >= h and btts else '0'
        if dc_part in ('12', 'home or away'): return '1' if h != a and btts else '0'

    # 1. Standard Markets
    if p in ("over 2.5", "over 2_5", "over_2.5", "over_2_5"): return '1' if total > 2.5 else '0'
    if p in ("under 2.5", "under 2_5", "under_2.5", "under_2_5"): return '1' if total < 2.5 else '0'
    if p in ("over 1.5", "over 1_5", "over_1.5", "over_1_5"): return '1' if total > 1.5 else '0'
    if p in ("under 1.5", "under 1_5", "under_1.5", "under_1_5"): return '1' if total < 1.5 else '0'
    if p in ("btts yes", "btts_yes", "both teams to score yes", "both teams to score", "gg"): return '1' if h > 0 and a > 0 else '0'
    if p in ("btts no", "btts_no", "both teams to score no", "ng"): return '1' if h == 0 or a == 0 else '0'
    if p in ("home win", "home_win", "1"): return '1' if h > a else '0'
    if p in ("away win", "away_win", "2"): return '1' if a > h else '0'
    if p in ("draw", "x"): return '1' if h == a else '0'

    # 1a. Double Chance
    if p in ("home or away", "12", "1 2", "double chance 12"): return '1' if h != a else '0'
    if p in ("1x", "home or draw", "home_or_draw", "double chance 1x"): return '1' if h >= a else '0'
    if p in ("x2", "away or draw", "away_or_draw", "draw or away", "double chance x2"): return '1' if a >= h else '0'

    # 2. "Team to win"
    if p.endswith(" to win"):
        team = p.replace(" to win", "").strip()
        if _is_home(team): return '1' if h > a else '0'
        if _is_away(team): return '1' if a > h else '0'

    # 3. "Team or Draw" / Double Chance (team-name based)
    if " or draw" in p:
        team = p.replace(" or draw", "").strip()
        if _is_home(team): return '1' if h >= a else '0'
        if _is_away(team): return '1' if a >= h else '0'
    if "draw or " in p:
        team = p.replace("draw or ", "").strip()
        if _is_home(team): return '1' if h >= a else '0'
        if _is_away(team): return '1' if a >= h else '0'

    or_match = re.match(r'^(.+?)\s+or\s+(.+?)$', p)
    if or_match and "draw" not in p:
        t1 = or_match.group(1).strip()
        t2 = or_match.group(2).strip()
        if (_is_home(t1) and _is_away(t2)) or (_is_away(t1) and _is_home(t2)):
            return '1' if h != a else '0'

    # 4. Draw No Bet
    if p.endswith(" (dnb)"):
        team = p.replace(" to win (dnb)", "").replace(" (dnb)", "").strip()
        if h == a: return ''
        if _is_home(team): return '1' if h > a else '0'
        if _is_away(team): return '1' if a > h else '0'

    # 5. Dynamic Over/Under
    over_match = re.search(r'over\s+([\d.]+)', p)
    if over_match:
        threshold = float(over_match.group(1))
        team_part = p[:over_match.start()].strip()
        if team_part:
            if _is_home(team_part): return '1' if h > threshold else '0'
            if _is_away(team_part): return '1' if a > threshold else '0'
        if "away" in p: return '1' if a > threshold else '0'
        if "home" in p: return '1' if h > threshold else '0'
        return '1' if total > threshold else '0'

    under_match = re.search(r'under\s+([\d.]+)', p)
    if under_match:
        threshold = float(under_match.group(1))
        team_part = p[:under_match.start()].strip()
        if team_part:
            if _is_home(team_part): return '1' if h < threshold else '0'
            if _is_away(team_part): return '1' if a < threshold else '0'
        if "away" in p: return '1' if a < threshold else '0'
        if "home" in p: return '1' if h < threshold else '0'
        return '1' if total < threshold else '0'

    # 6. Clean Sheet (team-name based + keyword fallback)
    if "clean sheet" in p:
        team = p.replace(" clean sheet", "").replace("clean sheet ", "").strip()
        if team in ("home", "") or _is_home(team): return '1' if a == 0 else '0'
        if team == "away" or _is_away(team): return '1' if h == 0 else '0'
        # Keyword fallback: "clean sheet - home - yes", "clean sheet - away - yes"
        if "home" in p: return '1' if a == 0 else '0'
        if "away" in p: return '1' if h == 0 else '0'

    # 7. Win to Nil
    if "win to nil" in p:
        if "home" in p or _is_home(p.replace("win to nil", "").replace("to win to nil", "").strip()):
            return '1' if h > 0 and a == 0 else '0'
        if "away" in p or _is_away(p.replace("win to nil", "").replace("to win to nil", "").strip()):
            return '1' if a > 0 and h == 0 else '0'
        # Team-name based: "Arsenal win to nil"
        team = p.replace(" win to nil", "").strip()
        if _is_home(team): return '1' if h > 0 and a == 0 else '0'
        if _is_away(team): return '1' if a > 0 and h == 0 else '0'

    # 8. Correct Score (e.g. "correct score 2-1", "2-1", "correct score - 2:1")
    cs_match = re.search(r'(\d+)\s*[-:]\s*(\d+)', p)
    if cs_match and ('correct score' in p or re.fullmatch(r'\d+\s*-\s*\d+', p)):
        exp_h = int(cs_match.group(1))
        exp_a = int(cs_match.group(2))
        return '1' if h == exp_h and a == exp_a else '0'

    return ''


__all__ = ["evaluate_market_outcome"]

