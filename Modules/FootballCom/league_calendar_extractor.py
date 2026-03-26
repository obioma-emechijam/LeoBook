# league_calendar_extractor.py: Scrape football.com league hub calendar for fixtures.
# Part of LeoBook Modules — FootballCom
#
# Functions: extract_league_calendar()
#
# URL pattern: https://www.football.com/ng/league/football/{country}/{league}/calendar
# DOM source:  fb_league_hub.html (user-provided 2026-03-21)
#
# This is the MOST COMPLETE fixture source on football.com:
#   - All rounds with round numbers
#   - Match IDs (sr:match:XXXXX via data-index)
#   - Home/away team names
#   - Dates + kick-off times
#   - FT scores (for finished matches)
#   - Winner indicators
#
# v1.1 (2026-03-26): _scroll_to_load_all() now delegates to _scroll_to_load()
#   from fs_league_hydration — adaptive micro-poll wait, bottom detection,
#   scroll reset, and consistent logging.

"""
League Calendar Extractor
Scrapes the league hub calendar page for complete fixture data across all rounds.
Returns structured match data with match IDs, scores, and status.
"""

import asyncio
import re
from typing import List, Dict, Optional
from playwright.async_api import Page

from Modules.Flashscore.fs_league_hydration import _scroll_to_load


# ── Selectors (from fb_league_hub.html DOM analysis 2026-03-21) ─────────────

# Round group container
_SEL_ROUND_GROUP = ".mp-league-matches--group"
_SEL_ROUND_NAME = ".group-name"

# Individual match link
_SEL_MATCH_LINK = "a.mp-event-wrap"

# Inside each match link:
_SEL_HOME_TEAM = ".mp-event-teams--home span.tw-truncate"
_SEL_AWAY_TEAM = ".mp-event-teams--away span.tw-truncate"
_SEL_TIME = ".mp-time"
_SEL_STATUS = ".mp-event-status"
_SEL_HOME_SCORE = ".m-score.m-score-home"
_SEL_AWAY_SCORE = ".m-score:not(.m-score-home)"
_SEL_WINNER_HOME = ".mp-event-teams--home span.match-win"
_SEL_WINNER_AWAY = ".mp-event-teams--away span.match-win"


async def extract_league_calendar(
    page: Page,
    league_url: str,
    rounds_filter: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Scrape the league hub calendar page for all fixtures.

    Args:
        page:           Playwright Page (no login required).
        league_url:     Full URL like https://www.football.com/ng/league/football/england/premier_league/calendar
        rounds_filter:  Optional list of round names to include (e.g. ["Round 31", "Round 32"]).
                        If None, all rounds are extracted.

    Returns:
        List of match dicts with keys:
          - match_id:    str  (e.g. "sr:match:61301063")
          - home_team:   str
          - away_team:   str
          - date_time:   str  (e.g. "01/03/26" or "21/03/26 13:30")
          - status:      str  ("FT", "Live", "", etc.)
          - home_score:  str|None
          - away_score:  str|None
          - winner:      str|None  ("home", "away", "draw", None)
          - round:       str  (e.g. "Round 31")
          - match_url:   str  (relative path)
          - league_url:  str  (the source URL)
    """
    print(f"\n  [Calendar] Navigating to: {league_url}")
    await page.goto(league_url, wait_until='domcontentloaded', timeout=30000)

    # Wait for calendar content to load
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    # Ensure Calendar tab is active — click it if needed
    try:
        tabs = page.locator('[data-ref="ui-label"] h4')
        tab_count = await tabs.count()
        for i in range(tab_count):
            tab = tabs.nth(i)
            text = (await tab.inner_text()).strip()
            if text.lower() == "calendar":
                await tab.click()
                await asyncio.sleep(2)
                break
    except Exception as e:
        print(f"  [Calendar] Tab click failed (may already be on calendar): {e}")

    # Wait for match groups to appear
    try:
        await page.wait_for_selector(_SEL_ROUND_GROUP, timeout=10000)
    except Exception:
        print("  [Calendar] No round groups found on page")
        return []

    # Scroll to load all rounds
    await _scroll_to_load_all(page)

    # Extract via JS for speed (avoids N*M locator calls)
    matches = await page.evaluate(r"""() => {
        const results = [];
        const groups = document.querySelectorAll('.mp-league-matches--group');

        groups.forEach(group => {
            const roundEl = group.querySelector('.group-name');
            const roundName = roundEl ? roundEl.innerText.trim() : '';

            const links = group.querySelectorAll('a.mp-event-wrap');
            links.forEach(link => {
                const matchId = link.getAttribute('data-index') || '';
                const href = link.getAttribute('href') || '';

                // Teams
                const homeEl = link.querySelector('.mp-event-teams--home span.tw-truncate');
                const awayEl = link.querySelector('.mp-event-teams--away span.tw-truncate');
                const home = homeEl ? homeEl.innerText.trim() : '';
                const away = awayEl ? awayEl.innerText.trim() : '';

                // Date/Time
                const timeEl = link.querySelector('.mp-time');
                const dateTime = timeEl ? timeEl.innerText.trim() : '';

                // Status (FT, Live, etc.)
                const statusEl = link.querySelector('.mp-event-status');
                let status = '';
                if (statusEl) {
                    // Status is the text NOT inside .mp-time
                    const allText = statusEl.innerText.trim();
                    const timeText = timeEl ? timeEl.innerText.trim() : '';
                    status = allText.replace(timeText, '').trim();
                }

                // Scores
                const homeScoreEl = link.querySelector('.m-score.m-score-home');
                const scoreEls = link.querySelectorAll('.m-score');
                let homeScore = null;
                let awayScore = null;
                if (homeScoreEl) {
                    homeScore = homeScoreEl.innerText.trim() || null;
                }
                if (scoreEls.length >= 2) {
                    // Away score is the second .m-score (not .m-score-home)
                    for (const el of scoreEls) {
                        if (!el.classList.contains('m-score-home')) {
                            awayScore = el.innerText.trim() || null;
                            break;
                        }
                    }
                }

                // Winner
                const homeWin = link.querySelector('.mp-event-teams--home span.match-win');
                const awayWin = link.querySelector('.mp-event-teams--away span.match-win');
                let winner = null;
                if (homeWin) winner = 'home';
                else if (awayWin) winner = 'away';
                else if (homeScore !== null && awayScore !== null &&
                         homeScore === awayScore && status === 'FT') {
                    winner = 'draw';
                }

                if (home && away) {
                    results.push({
                        match_id: matchId,
                        home_team: home,
                        away_team: away,
                        date_time: dateTime,
                        status: status,
                        home_score: homeScore,
                        away_score: awayScore,
                        winner: winner,
                        round: roundName,
                        match_url: href,
                    });
                }
            });
        });

        return results;
    }""")

    # Apply rounds filter
    if rounds_filter:
        rounds_set = {r.lower() for r in rounds_filter}
        matches = [m for m in matches if m.get('round', '').lower() in rounds_set]

    # Add source URL
    for m in matches:
        m['league_url'] = league_url

    # Summary
    rounds_found = set(m.get('round', '') for m in matches)
    ft_count = sum(1 for m in matches if m.get('status') == 'FT')
    upcoming = sum(1 for m in matches if not m.get('status'))

    print(f"  [Calendar] Extracted {len(matches)} matches across {len(rounds_found)} round(s)")
    print(f"  [Calendar] Finished: {ft_count} | Upcoming: {upcoming}")
    for r in sorted(rounds_found):
        r_matches = [m for m in matches if m.get('round') == r]
        print(f"    {r}: {len(r_matches)} matches")

    return matches


async def _scroll_to_load_all(page: Page, max_steps: int = 30) -> None:
    """Scroll down to ensure all lazy-loaded rounds are visible.
    Delegates to _scroll_to_load() from fs_league_hydration for adaptive
    micro-poll wait, bottom detection, scroll reset, and consistent logging.
    """
    await _scroll_to_load(
        page,
        row_selector=_SEL_MATCH_LINK,
        max_steps=max_steps,
        step_wait=0.6,
        no_new_rows_limit=3,
    )


def build_league_calendar_url(country: str, league: str) -> str:
    """
    Build a league hub calendar URL from country and league names.
    Example: build_league_calendar_url("england", "premier_league")
    → "https://www.football.com/ng/league/football/england/premier_league/calendar"
    """
    country_slug = country.lower().replace(' ', '_')
    league_slug = league.lower().replace(' ', '_')
    return f"https://www.football.com/ng/league/football/{country_slug}/{league_slug}/calendar"


def parse_calendar_date(date_time_str: str) -> tuple:
    """
    Parse the date_time string from the calendar.
    Formats: "01/03/26" (date only, FT) or "21/03/26 13:30" (with time, upcoming).

    Returns: (date_str, time_str) where date_str is "DD/MM/YY" and time_str is "HH:MM" or None.
    """
    if not date_time_str:
        return (None, None)
    parts = date_time_str.strip().split(' ', 1)
    date_part = parts[0].strip() if parts else None
    time_part = parts[1].strip() if len(parts) > 1 else None
    return (date_part, time_part)


__all__ = [
    "extract_league_calendar",
    "build_league_calendar_url",
    "parse_calendar_date",
]