# match_resolver.py: Team name resolution for Football.com match pairing.
# Part of LeoBook Modules — FootballCom
#
# Classes: GrokMatcher
# Cascade: SQL matching engine (match_fb_to_schedule RPC, confidence ≥ 88)
#       → search_dict (exact alias, bidirectional)
#       → Gemini LLM fallback (cold-start / genuinely ambiguous)
# v1.2 (2026-03-17): SQL-first deterministic resolver added.
#   - match_fb_to_schedule() Supabase RPC wired as Stage 0.
#   - auto_match_batch() for batch-resolving all unmatched fb_matches in one call.
#   - LLM fallback preserved for confidence < 88 or cold-start.

import os
import json
import sqlite3
import asyncio
from typing import List, Dict, Optional, Tuple, Set

try:
    from Data.Access.supabase_client import get_client as _get_supabase
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

_session_dead_models: Set[str] = set()

try:
    from google import genai
    from google.genai import types
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

# Confidence threshold for SQL resolver to accept a match without LLM fallback.
SQL_CONFIDENCE_THRESHOLD = 88


class GrokMatcher:
    """
    3-stage cascade resolver:
      Stage 0 — SQL matching engine (match_fb_to_schedule RPC, confidence ≥ 88)
      Stage 1 — search_dict (exact + bidirectional alias lookup, local SQLite)
      Stage 2 — Gemini LLM fallback (cold-start / genuinely ambiguous names)

    v1.2 (2026-03-17): SQL-first deterministic resolver wired as Stage 0.
    The SQL engine uses normalize_team_name() + date-windowed scoring on Supabase.
    Python falls back to search_dict + LLM only when SQL confidence < 88.

    Imported by fb_manager.py for Chapter 1 Page 1 resolution.
    """

    def __init__(self):
        self._cache: Dict[str, Optional[Dict]] = {}
        self._supabase = _get_supabase() if HAS_SUPABASE else None

    @staticmethod
    def _get_name(m: Dict, role: str) -> str:
        """Extract home/away name from a candidate dict."""
        return (m.get(f'{role}_team') or m.get(role) or '').strip()

    def _get_team_id(self, m: Dict, role: str) -> Optional[str]:
        """Extract team_id for auto-learn storage."""
        return m.get(f'{role}_team_id') or m.get(f'{role}_id')

    def _get_search_terms(self, conn: sqlite3.Connection, team_id: Optional[str]) -> List[str]:
        """Load alternative search terms for a team from the search_dict table."""
        if not team_id or not conn:
            return []
        try:
            cur = conn.execute(
                "SELECT search_terms FROM search_dict WHERE team_id = ?", (team_id,)
            )
            row = cur.fetchone()
            if row and row[0]:
                return json.loads(row[0]) if isinstance(row[0], str) else row[0]
        except Exception:
            pass
        return []

    def _auto_learn(self, conn: sqlite3.Connection, team_id: Optional[str], new_alias: str) -> None:
        """Persist a newly discovered alias into search_dict for future sessions."""
        if not team_id or not conn or not new_alias:
            return
        try:
            alias_lower = new_alias.strip().lower()
            terms = self._get_search_terms(conn, team_id)
            if alias_lower not in [t.strip().lower() for t in terms]:
                terms.append(new_alias.strip())
                conn.execute(
                    "INSERT INTO search_dict (team_id, search_terms) VALUES (?, ?) "
                    "ON CONFLICT(team_id) DO UPDATE SET search_terms = excluded.search_terms",
                    (team_id, json.dumps(terms))
                )
                conn.commit()
        except Exception:
            pass  # Non-critical


    # ───────────────────────────────────────────────────────────────────────────
    # Stage 0: Direct schedules lookup (no RPC, no VIEW)
    #   Match on: league_id + date (±1 day) + normalized home + normalized away
    # ───────────────────────────────────────────────────────────────────────────

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

            # Immediately write fixture_id back to fb_matches
            try:
                await asyncio.to_thread(
                    lambda: self._supabase
                        .from_('fb_matches')
                        .update({'fixture_id': best['fixture_id'], 'matched': 'sql_v2.0'})
                        .eq('site_match_id', site_match_id)
                        .execute()
                )
            except Exception:
                pass  # Non-critical — enriched dict still has the fixture_id

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
                    .or_('fixture_id.is.null,matched.is.null,matched.eq.false')
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

    async def resolve_with_cascade(
        self,
        fs_fix: Dict,
        fb_matches: List[Dict],
        conn: sqlite3.Connection,
    ) -> Tuple[Optional[Dict], float, str]:
        """
        3-stage cascade:
          Stage 0 — SQL matching engine via match_fb_to_schedule() RPC.
                     Confidence ≥ 88 → accept immediately (no LLM).
          Stage 1 — search_dict (exact + bidirectional alias lookup).
          Stage 2 — Gemini LLM fallback for genuinely ambiguous names.

        Returns: (best_match_dict, score, method_str)
        """
        if not fb_matches:
            return None, 0.0, 'failed'

        home = (fs_fix.get('home_team_name') or fs_fix.get('home_team') or '').strip()
        away = (fs_fix.get('away_team_name') or fs_fix.get('away_team') or '').strip()
        home_id = fs_fix.get('home_team_id') or fs_fix.get('home_id')
        away_id = fs_fix.get('away_team_id') or fs_fix.get('away_id')

        if not home or not away:
            return None, 0.0, 'failed'

        # ── Stage 0: SQL matching engine ─────────────────────────────────────
        # Try each fb_match candidate via SQL resolver; take the first that hits ≥ 88.
        for fb_row in fb_matches:
            site_id = fb_row.get('site_match_id') or fb_row.get('id', '')
            if not site_id:
                continue
            sql_match, sql_conf, sql_method = await self.resolve_via_sql(site_id, fb_row)
            if sql_match and sql_conf >= SQL_CONFIDENCE_THRESHOLD:
                # Persist to search_dict for offline fallback resilience
                self._auto_learn(conn, home_id, self._get_name(sql_match, 'home'))
                self._auto_learn(conn, away_id, self._get_name(sql_match, 'away'))
                return {**sql_match, 'matched': True}, sql_conf / 100.0, sql_method

        # ── Stage 1: search_dict ─────────────────────────────────────────────
        home_terms = self._get_search_terms(conn, home_id)
        away_terms = self._get_search_terms(conn, away_id)
        h_aliases = [home.lower()] + [t.lower() for t in home_terms]
        a_aliases = [away.lower()] + [t.lower() for t in away_terms]

        for m in fb_matches:
            fb_h = self._get_name(m, 'home').lower()
            fb_a = self._get_name(m, 'away').lower()
            h_match = fb_h in h_aliases or any(fb_h in alias for alias in h_aliases) or any(alias in fb_h for alias in h_aliases if len(alias) >= 4)
            a_match = fb_a in a_aliases or any(fb_a in alias for alias in a_aliases) or any(alias in fb_a for alias in a_aliases if len(alias) >= 4)
            if h_match and a_match:
                self._auto_learn(conn, home_id, self._get_name(m, 'home'))
                self._auto_learn(conn, away_id, self._get_name(m, 'away'))
                return {**m, 'matched': True}, 0.98, 'search_terms'

        return None, 0.0, 'failed'

