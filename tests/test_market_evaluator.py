"""Test script for market_evaluator.py — validates all 50 market-outcome rules."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from Data.Access.market_evaluator import evaluate_market_outcome

PASS = 0
FAIL = 0

def test(desc, prediction, h, a, expected, home="HomeTeam", away="AwayTeam", status=""):
    global PASS, FAIL
    result = evaluate_market_outcome(prediction, str(h), str(a), home, away, match_status=status)
    if result == expected:
        PASS += 1
    else:
        FAIL += 1
        print(f"  FAIL: {desc} | pred='{prediction}' score={h}-{a} → got '{result}', expected '{expected}'")

# --- 1X2 ---
test("1X2-1 win",    "1", 2, 1, '1')
test("1X2-1 lose",   "1", 0, 1, '0')
test("1X2-2 win",    "2", 1, 3, '1')
test("1X2-2 lose",   "2", 2, 1, '0')
test("1X2-X win",    "x", 1, 1, '1')
test("1X2-X lose",   "x", 2, 1, '0')

# --- Double Chance ---
test("DC-1X home win",  "1x", 2, 1, '1')
test("DC-1X draw",      "1x", 1, 1, '1')
test("DC-1X away",      "1x", 0, 1, '0')
test("DC-X2 away win",  "x2", 0, 1, '1')
test("DC-X2 draw",      "x2", 1, 1, '1')
test("DC-X2 home",      "x2", 2, 1, '0')
test("DC-12 home win",  "12", 2, 1, '1')
test("DC-12 away win",  "12", 0, 1, '1')
test("DC-12 draw",      "12", 1, 1, '0')

# --- Over/Under ---
test("Over 0.5 win",   "over 0.5", 1, 0, '1')
test("Over 0.5 lose",  "over 0.5", 0, 0, '0')
test("Over 1.5 win",   "over 1.5", 1, 1, '1')
test("Over 1.5 lose",  "over 1.5", 1, 0, '0')
test("Over 2.5 win",   "over 2.5", 2, 1, '1')
test("Over 2.5 lose",  "over 2.5", 1, 1, '0')
test("Under 2.5 win",  "under 2.5", 1, 1, '1')
test("Under 2.5 lose", "under 2.5", 2, 1, '0')
test("Over 3.5 win",   "over 3.5", 2, 2, '1')
test("Over 3.5 lose",  "over 3.5", 2, 1, '0')
test("Under 3.5 win",  "under 3.5", 2, 1, '1')
test("Under 3.5 lose", "under 3.5", 3, 1, '0')
test("Over 4.5 win",   "over 4.5", 3, 2, '1')

# --- Team Over/Under ---
test("Home over 0.5",  "home over 0.5", 1, 0, '1')
test("Home over 0.5 lose", "home over 0.5", 0, 2, '0')
test("Away over 1.5",  "away over 1.5", 0, 2, '1')
test("Away over 1.5 lose", "away over 1.5", 0, 1, '0')

# --- Team name over/under ---
test("Arsenal over 0.5", "arsenal over 0.5", 1, 0, '1', home="Arsenal", away="Chelsea")
test("Chelsea over 1.5", "chelsea over 1.5", 0, 2, '1', home="Arsenal", away="Chelsea")

# --- BTTS ---
test("GG win",   "gg", 1, 1, '1')
test("GG lose",  "gg", 1, 0, '0')
test("NG win",   "ng", 1, 0, '1')
test("NG lose",  "ng", 1, 1, '0')
test("BTTS yes", "btts yes", 2, 1, '1')
test("BTTS no",  "btts no", 2, 0, '1')

# --- Clean Sheet ---
test("CS Home win",   "home clean sheet", 2, 0, '1')
test("CS Home lose",  "home clean sheet", 2, 1, '0')
test("CS Away win",   "away clean sheet", 0, 1, '1')
test("CS Away lose",  "away clean sheet", 1, 1, '0')
test("CS team name",  "arsenal clean sheet", 2, 0, '1', home="Arsenal", away="Chelsea")

# --- Win to Nil ---
test("WTN Home win",   "home win to nil", 2, 0, '1')
test("WTN Home lose (conceded)", "home win to nil", 2, 1, '0')
test("WTN Home lose (lost)",     "home win to nil", 0, 1, '0')
test("WTN Away win",   "away win to nil", 0, 1, '1')
test("WTN Away lose",  "away win to nil", 1, 1, '0')
test("WTN team", "arsenal win to nil", 3, 0, '1', home="Arsenal", away="Chelsea")

# --- Draw No Bet ---
test("DNB home win",  "arsenal (dnb)", 2, 1, '1', home="Arsenal", away="Chelsea")
test("DNB home lose", "arsenal (dnb)", 0, 1, '0', home="Arsenal", away="Chelsea")
test("DNB draw void", "arsenal (dnb)", 1, 1, '', home="Arsenal", away="Chelsea")

# --- Correct Score ---
test("CS 2-1 win",  "correct score 2-1", 2, 1, '1')
test("CS 2-1 lose", "correct score 2-1", 1, 2, '0')
test("CS 0-0 win",  "correct score 0-0", 0, 0, '1')
test("CS bare 2-1", "2-1", 2, 1, '1')
test("CS bare 2-1 wrong", "2-1", 1, 2, '0')

# --- Combo Markets ---
test("1&GG win",       "1 & gg", 2, 1, '1')
test("1&GG no btts",   "1 & gg", 2, 0, '0')
test("2&GG win",       "2 & gg", 1, 2, '1')
test("2&GG no btts",   "2 & gg", 0, 2, '0')
test("1&O2.5 win",     "1 & over 2.5", 2, 1, '1')
test("1&O2.5 under",   "1 & over 2.5", 1, 0, '0')
test("1x&GG win",      "1x & gg", 1, 1, '1')  # draw + btts
test("1x&GG no btts",  "1x & gg", 1, 0, '0')
test("x2&GG win",      "x2 & gg", 1, 2, '1')
test("12&GG win",      "12 & gg", 2, 1, '1')
test("12&GG draw",     "12 & gg", 1, 1, '0')

# --- AET/Penalties ---
test("AET draw → 1X wins", "1x", 2, 3, '1', status="aet")
test("AET draw → X wins",  "x", 2, 3, '1', status="aet")
test("AET draw → 1 loses",  "1", 2, 3, '0', status="aet")

# --- Team to Win ---
test("Team win home", "arsenal to win", 2, 1, '1', home="Arsenal", away="Chelsea")
test("Team win away", "chelsea to win", 0, 1, '1', home="Arsenal", away="Chelsea")

print(f"\n{'='*50}")
print(f"Results: {PASS} PASS, {FAIL} FAIL, {PASS+FAIL} total")
if FAIL == 0:
    print("✅ ALL TESTS PASSED")
else:
    print(f"❌ {FAIL} TESTS FAILED")
    sys.exit(1)
