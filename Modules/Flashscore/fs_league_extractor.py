# fs_league_extractor.py: JS scripts, season parsing, match extraction, gap verification.
# Part of LeoBook Modules — Flashscore

import re
import json
from datetime import datetime
from typing import Dict, List, Optional

from playwright.async_api import Page

from Core.Utils.constants import now_ng


# ═══════════════════════════════════════════════════════════════════════════════
#  JS Extraction Scripts
# ═══════════════════════════════════════════════════════════════════════════════

EXTRACT_MATCHES_JS = r"""(ctx) => {
    const matches = [];
    const s = ctx.selectors;
    const startYear = ctx.startYear || new Date().getFullYear();
    const endYear = ctx.endYear || startYear;
    const isSplitSeason = ctx.isSplitSeason || false;
    const tab = ctx.tab || 'results';

    function inferYear(day, month) {
        if (!isSplitSeason) return startYear;
        return month >= 7 ? startYear : endYear;
    }

    const container = document.querySelector(s.main_container)?.parentElement || document.body;
    const allEls = container.querySelectorAll(`${s.match_round}, ${s.match_row}`);
    let currentRound = '';

    allEls.forEach(el => {
        if (el.matches(s.match_round)) { currentRound = el.innerText.trim(); return; }
        const rowId = el.getAttribute('id') || '';
        if (!rowId || !rowId.startsWith('g_1_')) return;
        const row = el;
        const fixtureId = rowId.replace('g_1_', '');
        const timeEl = row.querySelector(s.match_time);
        let matchTime = '', matchDate = '', extraTag = '';
        if (timeEl) {
            const stageInTime = timeEl.querySelector(`${s.match_stage_block}, ${s.match_stage_pkv}, ${s.match_stage}`);
            if (stageInTime) extraTag = stageInTime.innerText.trim();
            // Strategy: collect text from (1) text nodes, (2) lineThrough spans, (3) full innerText
            let raw = '';
            for (const node of timeEl.childNodes) {
                if (node.nodeType === 3) raw += node.textContent;
                else if (node.classList && node.classList.contains('lineThrough')) raw += node.textContent;
            }
            raw = raw.trim();
            // Fallback: if text-node scan yielded nothing useful, use full innerText
            if (!raw || !raw.match(/\d/)) {
                raw = timeEl.innerText.trim().replace(/FRO|Postp\.?|Canc\.?|Abn\.?/gi, '').trim();
            }
            // Pattern 1: Full date+time  "19.03.2026 14:00"
            const fullM = raw.match(/(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})/);
            if (fullM) {
                matchDate = `${fullM[3]}-${fullM[2]}-${fullM[1]}`; matchTime = `${fullM[4]}:${fullM[5]}`;
            } else {
                // Pattern 2: Short date+time  "19.03. 14:00"
                const shortM = raw.match(/(\d{2})\.(\d{2})\.\s*(\d{2}):(\d{2})/);
                if (shortM) {
                    const year = inferYear(parseInt(shortM[1]), parseInt(shortM[2]));
                    matchDate = `${year}-${shortM[2]}-${shortM[1]}`; matchTime = `${shortM[3]}:${shortM[4]}`;
                } else {
                    // Pattern 3: Time only  "14:00" (today's matches)
                    const jt = raw.match(/(\d{2}):(\d{2})/);
                    if (jt) matchTime = `${jt[1]}:${jt[2]}`;
                }
            }
            // Ultimate fallback: scan ALL descendant text for a time pattern
            if (!matchTime) {
                const allText = timeEl.innerText || '';
                const tf = allText.match(/(\d{2}):(\d{2})/);
                if (tf) matchTime = `${tf[1]}:${tf[2]}`;
            }
            // Also try to extract date from innerText if we got time but no date
            if (matchTime && !matchDate) {
                const allText = timeEl.innerText || '';
                const df = allText.match(/(\d{2})\.(\d{2})\.(\d{4})/);
                if (df) {
                    matchDate = `${df[3]}-${df[2]}-${df[1]}`;
                } else {
                    const dsf = allText.match(/(\d{2})\.(\d{2})\./);
                    if (dsf) {
                        const year = inferYear(parseInt(dsf[1]), parseInt(dsf[2]));
                        matchDate = `${year}-${dsf[2]}-${dsf[1]}`;
                    }
                }
            }
        }
        const homeEl = row.querySelector(s.home_participant);
        const homeName = homeEl ? (homeEl.querySelector(s.participant_name) || homeEl).innerText.trim().replace(/\s*\(.*?\)\s*$/, '') : '';
        const awayEl = row.querySelector(s.away_participant);
        const awayName = awayEl ? (awayEl.querySelector(s.participant_name) || awayEl).innerText.trim().replace(/\s*\(.*?\)\s*$/, '') : '';
        const homeScoreEl = row.querySelector(s.match_score_home);
        const awayScoreEl = row.querySelector(s.match_score_away);
        const homeScore = homeScoreEl && homeScoreEl.innerText.trim() !== '-' ? parseInt(homeScoreEl.innerText.trim()) : null;
        const awayScore = awayScoreEl && awayScoreEl.innerText.trim() !== '-' ? parseInt(awayScoreEl.innerText.trim()) : null;
        let matchStatus = '';
        const stageEl = row.querySelector(`${s.match_stage_block}, ${s.match_stage}`);
        if (stageEl && !stageEl.closest(s.match_time)) matchStatus = stageEl.innerText.trim();
        else if (homeScoreEl) {
            const state = homeScoreEl.getAttribute('data-state') || '';
            const isFinal = homeScoreEl.className.includes('isFinal') || homeScoreEl.className.includes('Final');
            if (state === 'final' || isFinal || homeScore !== null) matchStatus = 'FT';
        }
        const homeImg = row.querySelector(s.match_logo_home);
        const awayImg = row.querySelector(s.match_logo_away);
        const homeCrest = homeImg ? (homeImg.src || homeImg.getAttribute('data-src') || '') : '';
        const awayCrest = awayImg ? (awayImg.src || awayImg.getAttribute('data-src') || '') : '';
        let homeTeamId = '', awayTeamId = '', homeTeamUrl = '', awayTeamUrl = '';
        let linkEl = row.querySelector(s.match_link);
        if (!linkEl) linkEl = document.querySelector(`a[aria-describedby="${rowId}"]`);
        const mLink = linkEl ? linkEl.getAttribute('href') : '';
        if (mLink && mLink.includes('/match/football/')) {
            const parts = mLink.replace(/^(.*\/match\/football\/)/, '').split('/').filter(p => p && !p.startsWith('?'));
            if (parts.length >= 2) {
                const hSeg = parts[0], aSeg = parts[1];
                homeTeamId = hSeg.substring(hSeg.lastIndexOf('-') + 1);
                awayTeamId = aSeg.substring(aSeg.lastIndexOf('-') + 1);
                const hSlug = hSeg.substring(0, hSeg.lastIndexOf('-'));
                const aSlug = aSeg.substring(0, aSeg.lastIndexOf('-'));
                if (hSlug && homeTeamId) homeTeamUrl = `https://www.flashscore.com/team/${hSlug}/${homeTeamId}/`;
                if (aSlug && awayTeamId) awayTeamUrl = `https://www.flashscore.com/team/${aSlug}/${awayTeamId}/`;
            }
        }
        // ROOT CAUSE 1 FIX: The URL match_link is the canonical ground truth for home/away order.
        // Some Flashscore layouts (Club Friendly, postponed fixtures) render DOM elements in
        // a non-standard order that mismatches the URL path. Detect and correct the swap.
        // URL structure: /match/football/home-slug-HOMEID/away-slug-AWAYID/?mid=FIXID
        if (mLink && homeTeamId && awayTeamId && homeEl && awayEl) {
            const urlParts = mLink.replace(/^.*\/match\/football\//, '').split('/').filter(p => p && !p.startsWith('?'));
            if (urlParts.length >= 2) {
                const urlHomeId = urlParts[0].substring(urlParts[0].lastIndexOf('-') + 1);
                // If the ID we extracted as 'home' doesn't match the URL's first (home) segment,
                // but it DOES match the URL's second (away) segment, the DOM order is swapped.
                if (urlHomeId && urlHomeId !== homeTeamId && urlHomeId === awayTeamId) {
                    // Swap: DOM rendered away-first, URL is canonical home-first
                    [homeName, awayName] = [awayName, homeName];
                    [homeTeamId, awayTeamId] = [awayTeamId, homeTeamId];
                    [homeTeamUrl, awayTeamUrl] = [awayTeamUrl, homeTeamUrl];
                    console.warn(`[Extractor] Swapped team names for fixture ${fixtureId} — DOM order mismatch vs URL canonical`);
                }
            }
        }
        matches.push({ fixture_id: fixtureId, date: matchDate, time: matchTime,
            home_team_name: homeName, away_team_name: awayName,
            home_team_id: homeTeamId, away_team_id: awayTeamId,
            home_team_url: homeTeamUrl, away_team_url: awayTeamUrl,
            home_score: homeScore, away_score: awayScore,
            match_status: matchStatus, home_crest_url: homeCrest, away_crest_url: awayCrest,
            league_stage: currentRound, extra: extraTag || null,
            url: `/match/${fixtureId}/#/match-summary`, match_link: mLink || ''
        });
    });
    return matches;
}"""

EXTRACT_SEASON_JS = r"""(selectors) => {
    const s = selectors;
    for (const sel of s.season_info.split(',').map(x => x.trim())) {
        const el = document.querySelector(sel);
        if (el) { const m = el.innerText.trim().match(/(\d{4}(?:\/\d{4})?)/); if (m) return m[1]; }
    }
    for (const b of document.querySelectorAll(s.breadcrumb_text)) {
        const m = b.innerText.match(/(\d{4}(?:\/\d{4})?)/); if (m) return m[1];
    }
    return '';
}"""

EXTRACT_CREST_JS = r"""(selectors) => {
    const img = document.querySelector(selectors.league_crest);
    return img ? (img.src || img.getAttribute('data-src') || '') : '';
}"""

EXTRACT_FS_LEAGUE_ID_JS = r"""() => {
    if (window.leaguePageHeaderData?.tournamentStageId) return window.leaguePageHeaderData.tournamentStageId;
    if (window.tournament_id) return window.tournament_id;
    if (window.config?.tournamentStage) return window.config.tournamentStage;
    const pathM = (window.location.pathname || '').match(/-([A-Za-z0-9]{6,10})\/?$/);
    if (pathM) return pathM[1];
    for (const link of document.querySelectorAll('a[href*="/standings/"], a[href*="/results/"]')) {
        const m = (link.getAttribute('href') || '').match(/\/([A-Za-z0-9]{6,10})\/standings\//);
        if (m) return m[1];
    }
    const hashM = (window.location.hash || '').match(/#\/([A-Za-z0-9]{6,10})\//);
    if (hashM) return hashM[1];
    return '';
}"""

# v2 (2026-03-26): Row-based iteration via archiveTable__row--entry.
#   Fixes: current-season silently dropped (no year slug in href),
#   winner data ignored, wait_for_selector too early, over-broad selector.
EXTRACT_ARCHIVE_JS = r"""(selectors) => {
    const seasons = [], seen = new Set();

    /* ── Primary: iterate archiveTable rows ─────────────────────── */
    const rows = document.querySelectorAll('div.archiveTable__row--entry');
    for (const row of rows) {
        const cols  = row.querySelectorAll('div.archiveTable__column');
        if (!cols.length) continue;

        /* — Column 1: season link + label ——————————————————————— */
        const link  = cols[0]?.querySelector('a.archiveTable__column--link');
        const span  = link?.querySelector('span');
        const label = span ? span.textContent.trim() : '';
        const href  = link ? (link.getAttribute('href') || '') : '';

        // Extract years from span text:  "Premier League 2025/2026" -> 2025, 2026
        //                                "First Division 1991/1992" -> 1991, 1992
        //                                "Superliga 2024"           -> 2024
        let startYear = 0, endYear = 0, isSplit = false, seasonLabel = '';
        const splitLabelM = label.match(/(\d{4})\/(\d{4})/);
        const singleLabelM = !splitLabelM ? label.match(/(\d{4})/) : null;
        if (splitLabelM) {
            startYear = parseInt(splitLabelM[1]);
            endYear   = parseInt(splitLabelM[2]);
            isSplit   = true;
            seasonLabel = `${startYear}/${endYear}`;
        } else if (singleLabelM) {
            startYear = parseInt(singleLabelM[1]);
            endYear   = startYear;
            seasonLabel = `${startYear}`;
        } else {
            continue;  // No recognisable year in label — skip
        }

        if (seen.has(seasonLabel)) continue;
        seen.add(seasonLabel);

        // Derive slug from href, tolerating current-season (no year in path)
        const hrefM = href.match(/\/football\/([^/]+)\/([^/]+)\/?$/);
        const country = hrefM ? hrefM[1] : '';
        let slug = hrefM ? hrefM[2] : seasonLabel.replace('/', '-');
        const fullUrl = href.startsWith('http') ? href
                      : href.startsWith('/') ? 'https://www.flashscore.com' + href
                      : 'https://www.flashscore.com/' + href;

        /* — Column 2: winner data (optional) ———————————————————— */
        let winnerName = null, winnerTeamId = null, winnerTeamUrl = null, winnerCrestUrl = null;
        if (cols.length >= 2) {
            const wLink = cols[1]?.querySelector('a.archiveTable__winner-content');
            if (wLink) {
                winnerName = wLink.textContent.trim();
                const wHref = wLink.getAttribute('href') || '';
                const teamM = wHref.match(/\/team\/([^/]+)\/([^/]+)\/?/);
                if (teamM) {
                    winnerTeamId  = teamM[2];
                    winnerTeamUrl = wHref.startsWith('http') ? wHref
                                 : 'https://www.flashscore.com' + wHref;
                }
                const logoSpan = wLink.querySelector('span.archiveTable__logo');
                if (logoSpan) {
                    const bg = logoSpan.getAttribute('style') || '';
                    const bgM = bg.match(/url\(["']?([^"')]+)["']?\)/);
                    if (bgM) winnerCrestUrl = bgM[1];
                }
            }
        }

        seasons.push({
            slug, country, start_year: startYear, end_year: endYear,
            is_split: isSplit, label: seasonLabel, url: fullUrl,
            winner_name: winnerName, winner_team_id: winnerTeamId,
            winner_team_url: winnerTeamUrl, winner_crest_url: winnerCrestUrl
        });
    }

    /* ── Fallback: link-based (pre-2024 DOM without archiveTable) ── */
    if (seasons.length === 0) {
        for (const sel of [selectors.archive_links, selectors.archive_table_links,
                           'a.archiveTable__column--link', 'a[href*="/football/"]']) {
            if (!sel) continue;
            for (const a of document.querySelectorAll(sel)) {
                const href = a.getAttribute('href') || '';
                const splitM = href.match(/\/football\/([^/]+)\/([^/]+-(\d{4})-(\d{4}))\/?/i);
                if (splitM && !seen.has(splitM[2])) {
                    seen.add(splitM[2]);
                    seasons.push({ slug: splitM[2], country: splitM[1],
                        start_year: parseInt(splitM[3]), end_year: parseInt(splitM[4]),
                        is_split: true, label: `${splitM[3]}/${splitM[4]}`,
                        url: href.startsWith('http') ? href : (href.startsWith('/') ? 'https://www.flashscore.com' + href : 'https://www.flashscore.com/' + href),
                        winner_name: null, winner_team_id: null, winner_team_url: null, winner_crest_url: null });
                }
                const calM = href.match(/\/football\/([^/]+)\/([^/]+-(\d{4}))\/?$/i);
                if (calM && !seen.has(calM[2])) {
                    if (![...seen].some(s => s.startsWith(calM[2] + '-'))) {
                        seen.add(calM[2]);
                        seasons.push({ slug: calM[2], country: calM[1],
                            start_year: parseInt(calM[3]), end_year: parseInt(calM[3]),
                            is_split: false, label: calM[3],
                            url: href.startsWith('http') ? href : (href.startsWith('/') ? 'https://www.flashscore.com' + href : 'https://www.flashscore.com/' + href),
                            winner_name: null, winner_team_id: null, winner_team_url: null, winner_crest_url: null });
                    }
                }
            }
        }
    }

    seasons.sort((a, b) => b.start_year - a.start_year || b.end_year - a.end_year);
    return seasons;
}"""


# ═══════════════════════════════════════════════════════════════════════════════
#  Season Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def parse_season_string(season_str: str) -> dict:
    if not season_str:
        year = now_ng().year
        return {"startYear": year, "endYear": year, "isSplitSeason": False}
    m = re.match(r"(\d{4})[/\-](\d{4})", season_str)
    if m:
        return {"startYear": int(m.group(1)), "endYear": int(m.group(2)), "isSplitSeason": True}
    m = re.match(r"(\d{4})", season_str)
    if m:
        year = int(m.group(1))
        return {"startYear": year, "endYear": year, "isSplitSeason": False}
    year = datetime.now().year
    return {"startYear": year, "endYear": year, "isSplitSeason": False}


async def get_archive_seasons(page: Page, league_url: str, selector_mgr, context_league: str) -> List[Dict]:
    """Navigate to /archive/ and return all available past seasons, most-recent-first."""
    from Core.Intelligence.aigo_suite import AIGOSuite

    archive_url = league_url.rstrip("/") + "/archive/"
    print(f"    [Archive] {archive_url}")
    try:
        from Core.Browser.site_helpers import fs_universal_popup_dismissal
        await page.goto(archive_url, wait_until="domcontentloaded", timeout=60000)
        await fs_universal_popup_dismissal(page)
        selectors = selector_mgr.get_all_selectors_for_context(context_league)
        link_sel = (
            selectors.get("archive_links")
            or selectors.get("archive_table_links")
            or "a.archiveTable__column--link"
        )
        try:
            await page.wait_for_selector(link_sel, timeout=20000)
        except Exception:
            pass
        seasons = await page.evaluate(EXTRACT_ARCHIVE_JS, selectors)
        print(f"    [Archive] Found {len(seasons)} past seasons")
        return seasons or []
    except Exception as e:
        print(f"    [Archive] Failed: {e}")
        return []


def _select_seasons_from_archive(
    archive_seasons: List[Dict],
    target_season: Optional[int],
    num_seasons: int,
    all_seasons: bool,
    target_season_labels: Optional[List[str]] = None,
) -> List[Dict]:
    """Select seasons from the archive list.
    
    Logic:
    1. If target_season_labels (gaps) exist, include them.
    2. If num_seasons or all_seasons requested, also include those.
    3. Return a deduplicated list of unique seasons.
    """
    if not archive_seasons:
        return []

    selected: List[Dict] = []
    seen_labels = set()

    # 1. Add specific gap seasons
    if target_season_labels:
        label_set = set(target_season_labels)
        for s in archive_seasons:
            if s["label"] in label_set:
                selected.append(s)
                seen_labels.add(s["label"])

    # 2. Add by target relative index (1-indexed for CLI, 0-indexed for internal)
    if target_season is not None and target_season >= 1:
        idx = target_season - 1
        if idx < len(archive_seasons):
            s = archive_seasons[idx]
            if s["label"] not in seen_labels:
                selected.append(s)
                seen_labels.add(s["label"])

    # 3. Add by count or "all"
    if all_seasons:
        for s in archive_seasons:
            if s["label"] not in seen_labels:
                selected.append(s)
                seen_labels.add(s["label"])
    elif num_seasons > 0:
        # Take first N from archive (usually most recent first)
        for s in archive_seasons[:num_seasons]:
            if s["label"] not in seen_labels:
                selected.append(s)
                seen_labels.add(s["label"])

    # Sort final selection by start_year desc (most recent first)
    selected.sort(key=lambda x: (x.get('start_year', 0), x.get('end_year', 0)), reverse=True)
    return selected


def seed_leagues_from_json(conn, leagues_json_path: str) -> None:
    from Data.Access.league_db import upsert_league
    print(f"\n  [Seed] Reading {leagues_json_path}...")
    with open(leagues_json_path, "r", encoding="utf-8") as f:
        leagues = json.load(f)
    count = 0
    for lg in leagues:
        upsert_league(conn, {
            "league_id":    lg["league_id"],
            "country_code": lg.get("country_code"),
            "continent":    lg.get("continent"),
            "name":         lg["name"],
            "url":          lg.get("url"),
        })
        count += 1
    print(f"  [Seed] [OK] Upserted {count} leagues.")


def verify_league_gaps_closed(
    conn, league_id: str, before_gaps: int, idx: int, total: int
) -> tuple:
    """Count remaining gaps for a single league without a full DB rescan.

    Returns:
        (remaining_gaps, closed_gaps)
    """
    import logging
    logger = logging.getLogger(__name__)
    try:
        after_gaps = 0
        for col, cond in [
            ("home_team_name", "home_team_name IS NULL OR home_team_name = ''"),
            ("away_team_name", "away_team_name IS NULL OR away_team_name = ''"),
            ("home_crest",     "home_crest IS NULL OR home_crest = '' OR home_crest NOT LIKE 'http%'"),
            ("away_crest",     "away_crest IS NULL OR away_crest = '' OR away_crest NOT LIKE 'http%'"),
            ("fixture_id",     "fixture_id IS NULL OR fixture_id = ''"),
            ("date",           "date IS NULL OR date = ''"),
            ("season",         "season IS NULL OR season = ''"),
        ]:
            try:
                row = conn.execute(
                    f"SELECT COUNT(*) FROM schedules WHERE league_id = ? AND ({cond})",
                    (league_id,)
                ).fetchone()
                after_gaps += row[0] if row else 0
            except Exception:
                pass

        for col, cond in [
            ("name",         "name IS NULL OR name = ''"),
            ("url",          "url IS NULL OR url = ''"),
            ("country_code", "country_code IS NULL OR country_code = ''"),
            ("crest",        "crest IS NULL OR crest = '' OR crest NOT LIKE 'http%'"),
        ]:
            try:
                row = conn.execute(
                    f"SELECT COUNT(*) FROM leagues WHERE league_id = ? AND ({cond})",
                    (league_id,)
                ).fetchone()
                after_gaps += row[0] if row else 0
            except Exception:
                pass

        try:
            row = conn.execute(
                """SELECT COUNT(*) FROM teams
                   WHERE (crest IS NULL OR crest = '' OR crest NOT LIKE 'http%'
                          OR country_code IS NULL OR country_code = '')
                     AND (league_ids LIKE ? OR league_ids LIKE ? OR league_ids LIKE ?)""",
                (f'["{league_id}"]', f'"{league_id}",%', f'%,"{league_id}"%')
            ).fetchone()
            after_gaps += row[0] if row else 0
        except Exception:
            pass

        closed = max(0, before_gaps - after_gaps)
        if closed > 0 or after_gaps > 0:
            status = "[✓]" if after_gaps == 0 else "[~]"
            print(f"  [{idx}/{total}] {status} Gap delta for {league_id}: "
                  f"{before_gaps} -> {after_gaps} "
                  f"({closed} closed"
                  + (f", {after_gaps} remaining" if after_gaps else "")
                  + ")")

        return after_gaps, closed
    except Exception as e:
        logger.warning("[GapVerify] Failed for %s: %s", league_id, e)
        return 0, 0


def _backfill_schedule_crests(conn, league_id: str, season: str, country_code: str) -> int:
    """Overwrite empty/local-path crests in schedules with the Supabase URL from teams."""
    if country_code:
        cc_filter = "t.country_code = ?"
        params_home = (country_code, league_id, season, country_code)
        params_away = (country_code, league_id, season, country_code)
    else:
        cc_filter = "(t.country_code IS NULL OR t.country_code = '')"
        params_home = (league_id, season)
        params_away = (league_id, season)

    conn.execute(f"""
        UPDATE schedules
        SET home_crest = (
            SELECT t.crest FROM teams t
            WHERE t.name = schedules.home_team_name
              AND {cc_filter}
              AND t.crest LIKE 'http%'
            LIMIT 1
        )
        WHERE league_id = ? AND season = ?
          AND home_team_name IS NOT NULL
          AND (home_crest IS NULL OR home_crest = '' OR home_crest NOT LIKE 'http%')
          AND EXISTS (
              SELECT 1 FROM teams t
              WHERE t.name = schedules.home_team_name
                AND {cc_filter}
                AND t.crest LIKE 'http%'
          )
    """, params_home)
    home_updated = conn.execute("SELECT changes()").fetchone()[0]

    conn.execute(f"""
        UPDATE schedules
        SET away_crest = (
            SELECT t.crest FROM teams t
            WHERE t.name = schedules.away_team_name
              AND {cc_filter}
              AND t.crest LIKE 'http%'
            LIMIT 1
        )
        WHERE league_id = ? AND season = ?
          AND away_team_name IS NOT NULL
          AND (away_crest IS NULL OR away_crest = '' OR away_crest NOT LIKE 'http%')
          AND EXISTS (
              SELECT 1 FROM teams t
              WHERE t.name = schedules.away_team_name
                AND {cc_filter}
                AND t.crest LIKE 'http%'
          )
    """, params_away)
    away_updated = conn.execute("SELECT changes()").fetchone()[0]

    total = home_updated + away_updated
    if total:
        conn.commit()
    return total
