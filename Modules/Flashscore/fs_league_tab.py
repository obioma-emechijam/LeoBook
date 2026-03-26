# fs_league_tab.py: Tab extraction (results/fixtures) and single-league enrichment.
# Part of LeoBook Modules — Flashscore
# Called by: fs_league_enricher.py (main loop)

import asyncio
import logging
import os
import traceback
from concurrent.futures import as_completed as futures_as_completed
from datetime import date, datetime
from typing import Dict, List, Optional, Set

from Core.Intelligence.aigo_suite import AIGOSuite
from Core.Intelligence.selector_manager import SelectorManager
from Core.Browser.site_helpers import fs_universal_popup_dismissal
from Data.Access.league_db import (
    upsert_league, upsert_team, bulk_upsert_fixtures, mark_league_processed,
)
from Modules.Flashscore.data_contract import (
    DataContractViolation, validate_league_metadata,
    validate_tab_extraction,
)

from Modules.Flashscore.fs_league_images import (
    _slugify, schedule_image_download, upload_crest_to_supabase,
)
from Modules.Flashscore.fs_league_hydration import (
    _wait_for_page_hydration, _scroll_to_load, _expand_show_more,
)
from Modules.Flashscore.fs_league_extractor import (
    EXTRACT_MATCHES_JS, EXTRACT_SEASON_JS, EXTRACT_CREST_JS,
    EXTRACT_FS_LEAGUE_ID_JS,
    parse_season_string, get_archive_seasons, _select_seasons_from_archive,
    verify_league_gaps_closed, _backfill_schedule_crests,
)

logger = logging.getLogger(__name__)

selector_mgr = SelectorManager()
CONTEXT_LEAGUE = "fs_league_page"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CRESTS_DIR = os.path.join("Data", "Store", "crests")
LEAGUE_CRESTS_DIR = os.path.join(CRESTS_DIR, "leagues")
TEAM_CRESTS_DIR = os.path.join(CRESTS_DIR, "teams")

MAX_SHOW_MORE = 50


@AIGOSuite.aigo_retry(max_retries=3, delay=3.0)
async def extract_tab(
    page, league_url: str, tab: str, conn,
    league_id: str, season: str, country_code: str,
    country_league: str = "",
    gap_columns: Optional[Set[str]] = None,
    commit: bool = True,
) -> int:
    """Navigate to a league tab, load all rows, extract and persist."""
    url = league_url.rstrip("/") + f"/{tab}/"
    print(f"    [{tab.upper()}] {url}")
    if gap_columns:
        print(f"      [Targeting gaps] {', '.join(sorted(gap_columns))}")

    tab_selectors = selector_mgr.get_all_selectors_for_context(CONTEXT_LEAGUE)
    row_sel: str = tab_selectors.get("match_row", "[id^='g_1_']")

    try:
        resp = await page.goto(url, wait_until="networkidle", timeout=60000)
        await fs_universal_popup_dismissal(page)
        if resp and resp.status >= 400:
            print(f"    [{tab.upper()}] HTTP {resp.status} — not available")
            return 0
        if tab not in page.url.rstrip("/"):
            print(f"    [{tab.upper()}] Redirected — season not available")
            return 0

        initial = await _wait_for_page_hydration(page, tab_selectors)
        if initial:
            print(f"      [Hydrate] {initial} rows initially")
        scrolled = await _scroll_to_load(page, row_sel)
        if scrolled > initial:
            print(f"      [Scroll] +{scrolled - initial} rows revealed")
    except Exception as e:
        print(f"    [{tab.upper()}] Nav failed: {e}")
        return 0

    await _expand_show_more(page, selector_mgr, CONTEXT_LEAGUE, MAX_SHOW_MORE)

    # Content readiness gate: wait for .event__time elements to have rendered text
    time_sel = tab_selectors.get("match_time", ".event__time")
    try:
        await page.wait_for_selector(time_sel, timeout=8000)
        # Poll until at least one time element has actual text content
        has_text = await page.evaluate("""(sel) => {
            const els = document.querySelectorAll(sel);
            for (const el of els) {
                if (el.innerText && el.innerText.trim().length > 0) return true;
            }
            return false;
        }""", time_sel)
        if not has_text:
            await asyncio.sleep(2)  # Extra buffer for text rendering
            print(f"      [Hydrate] Waited extra 2s for time text rendering")
    except Exception:
        print(f"      [Hydrate] [!] No {time_sel} elements found — extraction may fail")

    # Pre-scan: count DOM rows for row-count parity check
    scanned_count = await page.locator(row_sel).count()
    print(f"      [Pre-scan] {scanned_count} DOM rows")

    season_ctx = parse_season_string(season)
    season_ctx["tab"] = tab
    season_ctx["selectors"] = tab_selectors

    try:
        matches_raw = await page.evaluate(EXTRACT_MATCHES_JS, season_ctx)
    except Exception as e:
        print(f"    [{tab.upper()}] JS extraction failed: {e}")
        return 0

    if not matches_raw:
        if scanned_count > 0:
            raise DataContractViolation(
                f"[{tab.upper()}] {scanned_count} rows scanned but 0 extracted"
            )
        print(f"    [{tab.upper()}] No matches found")
        return 0

    fixture_rows: List[Dict] = []
    crest_pending: Dict[str, tuple] = {}
    today = date.today()

    for m in matches_raw:
        home_name = m.get("home_team_name", "")
        away_name = m.get("away_team_name", "")
        if not home_name or not away_name:
            continue

        home_team_id  = m.get("home_team_id",  "")
        away_team_id  = m.get("away_team_id",  "")
        home_team_url = m.get("home_team_url", "")
        away_team_url = m.get("away_team_url", "")

        for tname, tid, turl in (
            (home_name, home_team_id, home_team_url),
            (away_name, away_team_id, away_team_url),
        ):
            td = {"name": tname, "country_code": country_code, "league_ids": [league_id]}
            if tid:  td["team_id"] = tid
            if turl: td["url"]     = turl
            upsert_team(conn, td, commit=commit)

        for tname, ckey in ((home_name, "home_crest_url"), (away_name, "away_crest_url")):
            curl = m.get(ckey, "")
            if curl and not curl.startswith("data:") and tname not in crest_pending:
                dest = os.path.join(TEAM_CRESTS_DIR, f"{_slugify(tname)}.png")
                crest_pending[tname] = (schedule_image_download(curl, dest), dest)

        status = m.get("match_status", "")
        extra  = m.get("extra")
        if status:
            su = status.upper()
            if   "FT" in su or "FINISHED" in su: status = "finished"
            elif "AET" in su:  status = "finished";  extra = extra or "AET"
            elif "PEN" in su:  status = "finished";  extra = extra or "PEN"
            elif "POST" in su: status = "postponed"; extra = extra or "Postp"
            elif "CANC" in su: status = "cancelled"; extra = extra or "Canc"
            elif "ABD" in su or "ABAN" in su: status = "abandoned"; extra = extra or "Abn"
            elif "LIVE" in su or "'" in status: status = "live"
            elif "HT"  in su: status = "halftime"
            elif status == "-": status = "scheduled"

        match_date_str = m.get("date", "")
        if match_date_str and not status:
            try:
                if datetime.strptime(match_date_str, "%Y-%m-%d").date() > today:
                    status = "scheduled"
            except ValueError:
                pass
        if not status:
            status = "scheduled" if tab == "fixtures" else "finished"

        if extra:
            eu = extra.upper().strip()
            if   "FRO"   in eu: extra = "FRO"
            elif "POSTP" in eu: extra = "Postp"
            elif "CANC"  in eu: extra = "Canc"
            elif "ABN" in eu or "ABAN" in eu: extra = "Abn"

        # F3: Winner — prefer JS bold-class detection, fall back to score comparison
        winner = m.get("winner")
        if not winner and status == "finished":
            hs, aws = m.get("home_score"), m.get("away_score")
            if hs is not None and aws is not None:
                if hs > aws:   winner = "home"
                elif aws > hs: winner = "away"
                else:          winner = "draw"

        fixture_rows.append({
            "fixture_id":     m.get("fixture_id", ""),
            "date":           match_date_str,
            "time":           m.get("time", ""),
            "league_id":      league_id,
            "home_team_id":   home_team_id,
            "home_team_name": home_name,
            "away_team_id":   away_team_id,
            "away_team_name": away_name,
            "home_score":     m.get("home_score"),
            "away_score":     m.get("away_score"),
            "home_red_cards": m.get("home_red_cards", 0) or 0,
            "away_red_cards": m.get("away_red_cards", 0) or 0,
            "winner":         winner,
            "extra":          extra,
            "league_stage":   m.get("league_stage", ""),
            "match_status":   status,
            "season":         season,
            "home_crest":     "",
            "away_crest":     "",
            "url":            f"https://www.flashscore.com/match/{m.get('fixture_id', '')}/#/match-summary",
            "country_league":  country_league,
            "match_link":     m.get("match_link", ""),
            # Pass through raw JS fields for contract validation
            "home_team_url":  home_team_url,
            "away_team_url":  away_team_url,
            "home_crest_url": m.get("home_crest_url", ""),
            "away_crest_url": m.get("away_crest_url", ""),
        })

    # ── Data Contract Validation ──────────────────────────────────────────
    passed, summary = validate_tab_extraction(scanned_count, fixture_rows, tab)
    print(f"    {summary}")
    if not passed:
        raise DataContractViolation(summary)

    if fixture_rows:
        bulk_upsert_fixtures(conn, fixture_rows, commit=commit)

    downloaded = 0
    future_to_name = {fut: name for name, (fut, _dest) in crest_pending.items()}
    for fut in futures_as_completed(future_to_name):
        tname = future_to_name[fut]
        try:
            local = fut.result(timeout=0)
            if local:
                sb_url = upload_crest_to_supabase(local, "team-crests", f"{_slugify(tname)}.png")
                cval = sb_url if sb_url else local
                cc = country_code or None
                if cc:
                    conn.execute(
                        "UPDATE teams SET crest = ? WHERE name = ? AND (country_code = ? OR country_code IS NULL)",
                        (cval, tname, cc)
                    )
                else:
                    conn.execute(
                        "UPDATE teams SET crest = ? WHERE name = ? AND (country_code IS NULL OR country_code = '')",
                        (cval, tname)
                    )
                if commit:
                    conn.commit()
                downloaded += 1
        except Exception:
            pass

    if fixture_rows:
        backfilled = _backfill_schedule_crests(conn, league_id, season, country_code)
        if backfilled:
            print(f"      [Crests] Back-filled {backfilled} schedule rows")

    print(f"    [{tab.upper()}] [OK] {len(fixture_rows)} matches, {downloaded} crests")
    return len(fixture_rows)


async def enrich_single_league(
    context,
    league: Dict,
    conn,
    idx: int,
    total: int,
    num_seasons: int = 0,
    all_seasons: bool = False,
    target_season: Optional[int] = None,
    seasons_with_gaps: Optional[List[str]] = None,
    gap_columns: Optional[Set[str]] = None,
    needs_full_re_enrich: bool = False,
) -> None:
    """Process a single league: metadata, current season, and targeted past seasons."""
    league_id    = league["league_id"]
    name         = league["name"]
    url          = league.get("url", "")
    country_code = league.get("country_code", "")
    continent    = league.get("continent", "")

    print(f"\n{'='*60}")
    print(f"  [{idx}/{total}] {name} ({league_id})")
    if seasons_with_gaps:
        print(f"  Gap target seasons: {', '.join(seasons_with_gaps)}")
    if gap_columns:
        print(f"  Gap columns: {', '.join(sorted(gap_columns))}")
    print(f"{'='*60}")

    if not url:
        print(f"  [SKIP] No URL")
        mark_league_processed(conn, league_id)
        return

    page = await context.new_page()
    try:
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await fs_universal_popup_dismissal(page)

        selectors = selector_mgr.get_all_selectors_for_context(CONTEXT_LEAGUE)
        breadcrumb_sel = selectors.get("breadcrumb_links", ".breadcrumb__link")

        # Wait for full DOM hydration: breadcrumb + header (flag/crest live here)
        hydrated = False
        try:
            await page.wait_for_selector(breadcrumb_sel, timeout=10000)
            # Also wait for the league header block where flags/crests render
            try:
                await page.wait_for_selector(
                    ".heading, .tournamentHeader, [class*='heading']",
                    timeout=5000,
                )
            except Exception:
                pass  # Header not always present, breadcrumb is enough
            hydrated = True
        except Exception:
            pass

        if not hydrated:
            await asyncio.sleep(3)

        fs_league_id = await page.evaluate(EXTRACT_FS_LEAGUE_ID_JS)
        if fs_league_id:
            print(f"    [FS ID] {fs_league_id}")

        region_name = ""
        region_url_href = ""
        url_parts = url.rstrip("/").split("/")
        try:
            fb_idx = url_parts.index("football")
            if fb_idx + 1 < len(url_parts):
                slug = url_parts[fb_idx + 1]
                region_name     = slug.replace("-", " ").title()
                region_url_href = f"https://www.flashscore.com/football/{slug}/"
        except ValueError:
            pass

        breadcrumb_region = await page.evaluate("""(s) => {
            const links = document.querySelectorAll(s.breadcrumb_links);
            if (links.length >= 2) return links[1].innerText.trim();
            return '';
        }""", selectors)
        if breadcrumb_region and breadcrumb_region.upper() != "FOOTBALL":
            region_name = breadcrumb_region

        if not region_url_href:
            region_url_href = await page.evaluate("""(s) => {
                const links = document.querySelectorAll(s.breadcrumb_links);
                const el = links.length >= 2 ? links[1] : links[0];
                if (!el) return '';
                const href = el.getAttribute('href') || '';
                return href.startsWith('http') ? href : (href ? 'https://www.flashscore.com' + href : '');
            }""", selectors)

        region_flag_url = await page.evaluate("""(s) => {
            const links = document.querySelectorAll(s.breadcrumb_links);
            const target = links.length >= 2 ? links[1] : links[0];
            if (!target) return '';

            // Strategy 1: direct <img> inside or near the breadcrumb link
            const img = target.querySelector('img')
                     || target.parentElement?.querySelector('img');
            if (img) {
                const src = img.src || img.getAttribute('data-src') || '';
                if (src && !src.startsWith('data:')) return src;
            }

            // Strategy 2: CSS background-image on flag span/div
            const candidates = [
                target.querySelector('.flag'),
                target.querySelector('[class*="flag"]'),
                target.previousElementSibling,
                target.parentElement?.querySelector('.flag'),
                target.parentElement?.querySelector('[class*="flag"]'),
            ].filter(Boolean);
            for (const el of candidates) {
                const bg = getComputedStyle(el).backgroundImage || '';
                const m = bg.match(/url\\(["']?(https?:\\/\\/[^"')]+)["']?\\)/);
                if (m) return m[1];
            }

            // Strategy 3: any element with a flag-like class near breadcrumb
            const header = document.querySelector('.heading') || document.querySelector('.tournamentHeader');
            if (header) {
                const flagEl = header.querySelector('[class*="flag"]') || header.querySelector('img');
                if (flagEl) {
                    if (flagEl.tagName === 'IMG') {
                        const src = flagEl.src || flagEl.getAttribute('data-src') || '';
                        if (src && !src.startsWith('data:')) return src;
                    }
                    const bg = getComputedStyle(flagEl).backgroundImage || '';
                    const m = bg.match(/url\\(["']?(https?:\\/\\/[^"')]+)["']?\\)/);
                    if (m) return m[1];
                }
            }

            return '';
        }""", selectors)

        region_flag_path = ""
        if region_flag_url and not region_flag_url.startswith("data:"):
            print(f"    [Flag] URL: {region_flag_url[:80]}")
            flag_slug = _slugify(region_name or country_code or 'unknown')
            flag_dest = os.path.join(CRESTS_DIR, "flags", f"{flag_slug}.png")
            try:
                os.makedirs(os.path.join(BASE_DIR, os.path.dirname(flag_dest)), exist_ok=True)
                r = schedule_image_download(region_flag_url, flag_dest).result(timeout=10)
                if r:
                    sb_url = upload_crest_to_supabase(r, "flags", f"{flag_slug}.png")
                    region_flag_path = sb_url if sb_url else r
                    print(f"    [Flag] {'Supabase' if sb_url else 'local'}: {flag_slug}.png")
                else:
                    print(f"    [Flag] [!] Download returned empty")
            except Exception as e:
                print(f"    [Flag] [!] Download failed: {e}")
        else:
            print(f"    [Flag] [!] No flag URL found in DOM (url={repr(region_flag_url)[:60]})")

        crest_url  = await page.evaluate(EXTRACT_CREST_JS, selectors)
        crest_path = ""
        if crest_url and not crest_url.startswith("data:"):
            local_dest = os.path.join(LEAGUE_CRESTS_DIR, f"{_slugify(league_id)}.png")
            try:
                r = schedule_image_download(crest_url, local_dest).result(timeout=15)
                if r:
                    sb_url     = upload_crest_to_supabase(r, "league-crests", f"{_slugify(league_id)}.png")
                    crest_path = sb_url if sb_url else r
                    print(f"    [Crest] {'Supabase' if sb_url else 'local'}: {os.path.basename(local_dest)}")
            except Exception:
                print(f"    [Crest] [!] Download failed")

        season = await page.evaluate(EXTRACT_SEASON_JS, selectors)
        print(f"    [Season] {season or '(not found)'}")
        country = region_name or continent
        country_league = f"{country}: {name}" if country else name

        league_data = {
            "league_id":      league_id,
            "fs_league_id":   fs_league_id or None,
            "name":           name,
            "country_code":   country_code,
            "continent":      continent,
            "crest":          crest_path,
            "current_season": season,
            "url":            url,
            "region":         region_name or None,
            "region_flag":    region_flag_path or None,
            "region_url":     region_url_href or None,
        }

        # ── League Metadata Contract ─────────────────────────────────────
        lg_passed, lg_violations = validate_league_metadata(league_data)
        if not lg_passed:
            raise DataContractViolation(
                f"League metadata contract failed for {name} ({league_id}):\n"
                + "\n".join(f"    • {v}" for v in lg_violations)
            )

        upsert_league(conn, league_data, commit=False)

        total_matches = 0

        # Always process the current season if it's a gap or if we are in a normal run
        current_is_gap = season and seasons_with_gaps and (season in seasons_with_gaps)
        if current_is_gap or needs_full_re_enrich or not seasons_with_gaps:
            f_c = await extract_tab(page, url, "fixtures", conn, league_id, season, country_code,
                                    country_league=country_league, gap_columns=gap_columns, commit=False)
            r_c = await extract_tab(page, url, "results", conn, league_id, season, country_code,
                                    country_league=country_league, gap_columns=gap_columns, commit=False)
            total_matches += f_c + r_c

        # Handle past seasons (from gaps or from manual request)
        need_past = (target_season is not None and target_season >= 1) or num_seasons > 0 or all_seasons or (seasons_with_gaps and len(seasons_with_gaps) > (1 if current_is_gap else 0))
        
        if need_past:
            archive = await get_archive_seasons(page, url, selector_mgr, CONTEXT_LEAGUE)
            
            # Extract labels for seasons with gaps (excluding current)
            past_gap_labels = [s for s in (seasons_with_gaps or []) if s != season]
            
            selected_past = _select_seasons_from_archive(
                archive, target_season, num_seasons, all_seasons, 
                target_season_labels=past_gap_labels if past_gap_labels else None
            )

            for s_idx, s_meta in enumerate(selected_past, 1):
                label = s_meta['label']
                print(f"\n    [Season {s_idx}] {label} ({'split' if s_meta.get('is_split') else 'calendar'})")
                
                # If this season was in gaps, pass gap_columns to extract_tab for efficiency
                s_gap_cols = gap_columns if (seasons_with_gaps and label in seasons_with_gaps) else None
                
                r_c = await extract_tab(page, s_meta["url"], "results", conn,
                                        league_id, label, country_code,
                                        country_league=country_league, gap_columns=s_gap_cols, commit=False)
                f_c = await extract_tab(page, s_meta["url"], "fixtures", conn,
                                        league_id, label, country_code,
                                        country_league=country_league, gap_columns=s_gap_cols, commit=False)
                total_matches += r_c + f_c

        mark_league_processed(conn, league_id, commit=False)
        print(f"\n  [{idx}/{total}] [OK] {name} — {total_matches} matches total")

    except Exception as e:
        print(f"\n  [{idx}/{total}] [FAIL] {name}: {e}")
        traceback.print_exc()
    finally:
        await page.close()
