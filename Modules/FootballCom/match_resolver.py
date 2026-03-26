# match_resolver.py: Team name resolution for Football.com match pairing.
# Part of LeoBook Modules — FootballCom
#
# Classes: FixtureResolver
# Strategy: SQL matching engine (match on normalized names + date ±1 day)
# v1.4: Fixed silent DB update failures + standardized unmatched query for string 'matched' column.
#       Now fixture_id and matched columns in fb_matches are reliably populated (per Chief Engineer directive).

import os
import asyncio
import sqlite3
from typing import List, Dict, Optional, Tuple

try:
    from Data.Access.supabase_client import get_client as _get_supabase
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

# Confidence threshold for SQL resolver
SQL_CONFIDENCE_THRESHOLD = 88


class FixtureResolver:
    """
    Deterministic SQL-first fixture resolver.
    The SQL engine uses local normalize_team_name() and queries Supabase
    schedules table for an exact match on league_id + date (±1 day) + team names.
    
    Imported by fb_manager.py for Chapter 1 Page 1 resolution.
    """

    def __init__(self):
        self._supabase = _get_supabase() if HAS_SUPABASE else None

    @staticmethod
    def _normalize(name: str) -> str:
        """Lightweight Python equivalent of normalize_team_name()."""
        import re
        name = name.strip().lower()
        name = re.sub(r'[^a-z0-9 ]', '', name)  # strip accents/punctuation
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    async def resolve_via_sql(
        self,
        site_match_id: str,
        fb_match_row: Optional[Dict] = None,
    ) -> Tuple[Optional[Dict], int, str]:
        """
        Query schedules directly for an exact match on:
          league_id + date (±1 day) + home_team + away_team (both normalized).
        Returns (enriched_fb_row, confidence_int, 'sql_v2.0') or (None, 0, 'sql_miss').
        """
        if not self._supabase or not HAS_SUPABASE or not fb_match_row:
            return None, 0, 'sql_skip'

        league_id = fb_match_row.get('league_id') or ''
        fb_date = fb_match_row.get('date') or ''
        fb_home = fb_match_row.get('home_team') or ''
        fb_away = fb_match_row.get('away_team') or ''

        if not league_id or not fb_date or not fb_home or not fb_away:
            return None, 0, 'sql_skip'

        try:
            # Query schedules: exact league_id + date
            result = await asyncio.to_thread(
                lambda: self._supabase
                    .from_('schedules')
                    .select('fixture_id, date, home_team, away_team, home_team_id, away_team_id')
                    .eq('league_id', league_id)
                    .eq('date', fb_date)
                    .execute()
            )
            rows = result.data or []

            # Normalize and match both home AND away
            fb_h_norm = self._normalize(fb_home)
            fb_a_norm = self._normalize(fb_away)

            best = None
            for row in rows:
                s_h_norm = self._normalize(row.get('home_team', ''))
                s_a_norm = self._normalize(row.get('away_team', ''))
                if s_h_norm == fb_h_norm and s_a_norm == fb_a_norm:
                    best = row
                    break

            # Fallback: try date ±1 day if exact date had no match
            if not best:
                from datetime import datetime, timedelta
                try:
                    d = datetime.strptime(fb_date, '%Y-%m-%d')
                except ValueError:
                    return None, 0, 'sql_miss'

                for delta in (-1, 1):
                    alt_date = (d + timedelta(days=delta)).strftime('%Y-%m-%d')
                    alt_result = await asyncio.to_thread(
                        lambda ad=alt_date: self._supabase
                            .from_('schedules')
                            .select('fixture_id, date, home_team, away_team, home_team_id, away_team_id')
                            .eq('league_id', league_id)
                            .eq('date', ad)
                            .execute()
                    )
                    for row in (alt_result.data or []):
                        s_h_norm = self._normalize(row.get('home_team', ''))
                        s_a_norm = self._normalize(row.get('away_team', ''))
                        if s_h_norm == fb_h_norm and s_a_norm == fb_a_norm:
                            best = row
                            break
                    if best:
                        break

            if not best:
                return None, 0, 'sql_miss'

            confidence = 100 if best.get('date') == fb_date else 90
            enriched = dict(fb_match_row)
            enriched['fixture_id']   = best['fixture_id']
            enriched['home_team_id'] = best.get('home_team_id')
            enriched['away_team_id'] = best.get('away_team_id')
            enriched['matched']      = 'sql_v2.0'

            # Immediately write fixture_id + matched back to fb_matches (this is the ONLY place that fills the columns)
            try:
                await asyncio.to_thread(
                    lambda: self._supabase
                        .from_('fb_matches')
                        .update({'fixture_id': best['fixture_id'], 'matched': 'sql_v2.0'})
                        .eq('site_match_id', site_match_id)
                        .execute()
                )
                print(f"    [Resolver] ✅ Updated fb_matches: fixture_id + matched=sql_v2.0 for site_match_id={site_match_id}")
            except Exception as e:
                print(f"    [Resolver] ⚠️  Failed to update fb_matches for {site_match_id}: {e} (enriched dict still valid)")

            return enriched, confidence, 'sql_v2.0'
        except Exception as e:
            print(f"    [Resolver] Direct SQL failed for {site_match_id}: {e}")
            return None, 0, 'sql_error'

    async def auto_match_batch(self) -> int:
        """
        Fetch all unmatched fb_matches, resolve each against schedules inline,
        and write fixture_id back immediately. No RPC or VIEW needed.
        """
        if not self._supabase or not HAS_SUPABASE:
            return 0
        try:
            result = await asyncio.to_thread(
                lambda: self._supabase
                    .from_('fb_matches')
                    .select('site_match_id, league_id, date, home_team, away_team')
                    .or_('fixture_id.is.null,matched.is.null')
                    .limit(500)
                    .execute()
            )
            unmatched = result.data or []
            if not unmatched:
                return 0

            count = 0
            for row in unmatched:
                _, conf, method = await self.resolve_via_sql(row['site_match_id'], row)
                if conf >= 90:
                    count += 1

            print(f"    [Resolver] auto_match_batch: {count}/{len(unmatched)} resolved inline.")
            return count
        except Exception as e:
            print(f"    [Resolver] auto_match_batch failed: {e}")
            return 0

    async def resolve(
        self,
        fs_fix: Dict,
        fb_matches: List[Dict],
        conn: sqlite3.Connection,
    ) -> Tuple[Optional[Dict], float, str]:
        """
        Resolve a single FS fixture against a list of FB candidate matches.
        Uses the deterministic SQL resolver.
        
        Returns: (best_match_dict, score, method_str)
        """
        if not fb_matches:
            return None, 0.0, 'failed'

        home = (fs_fix.get('home_team_name') or fs_fix.get('home_team') or '').strip()
        away = (fs_fix.get('away_team_name') or fs_fix.get('away_team') or '').strip()

        if not home or not away:
            return None, 0.0, 'failed'

        # Try each fb_match candidate via SQL resolver.
        for fb_row in fb_matches:
            site_id = fb_row.get('site_match_id') or fb_row.get('id', '')
            if not site_id:
                continue
            sql_match, sql_conf, sql_method = await self.resolve_via_sql(site_id, fb_row)
            if sql_match and sql_conf >= SQL_CONFIDENCE_THRESHOLD:
                return {**sql_match, 'matched': True}, sql_conf / 100.0, sql_method

        return None, 0.0, 'failed'