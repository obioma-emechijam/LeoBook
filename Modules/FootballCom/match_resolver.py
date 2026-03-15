# match_resolver.py: Team name resolution for Football.com match pairing.
# Part of LeoBook Modules — FootballCom
#
# Classes: GrokMatcher
# Restored from bytecode (CPython 3.13) — original removed during modularisation.

import os
import re
import json
import sqlite3
import asyncio
from typing import List, Dict, Optional, Tuple, Set

_session_dead_models: Set[str] = set()

try:
    from Levenshtein import distance, ratio
except ImportError:
    # Fallback: pure-Python implementation using difflib
    import difflib
    def distance(a: str, b: str) -> int:
        """Levenshtein edit distance fallback via SequenceMatcher."""
        max_len = max(len(a), len(b), 1)
        sim = difflib.SequenceMatcher(None, a, b).ratio()
        return int(round(max_len * (1 - sim)))

    def ratio(a: str, b: str) -> float:
        """Similarity ratio fallback via SequenceMatcher."""
        return difflib.SequenceMatcher(None, a, b).ratio()

try:
    from google import genai
    from google.genai import types
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

# Noise tokens to strip from team names before comparison
_NOISE = re.compile(
    r'\b(?:fc|sc|sk|cf|afc|bsc|fk|nk|cd|ud|rc|rcd|og|de|del|von|und'
    r'|sport(?:ing)?|club|athletic|athletico|association|\d{4})\b',
    re.IGNORECASE
)
_MULTI_SPACE = re.compile(r'\s+')


def _normalize(name: str) -> str:
    """Lowercase, strip noise tokens, collapse whitespace."""
    result = name.lower()
    result = _NOISE.sub('', result)
    result = _MULTI_SPACE.sub(' ', result).strip()
    return result


def _tokenize(name: str) -> List[str]:
    """Split normalized name into tokens."""
    return _normalize(name).split()


def _acronym_match(short: str, long: str) -> bool:
    """Return True if `short` is the acronym of `long`."""
    long_tokens = _tokenize(long)
    initials = ''.join(t[0] for t in long_tokens if t)
    return short.lower() == initials.lower()


def _best_token_lev(token: str, tokens: List[str]) -> float:
    """Return best Levenshtein similarity between `token` and any token in `tokens`."""
    best = 0.0
    for t in tokens:
        shorter = token if len(token) <= len(t) else t
        longer  = t     if len(token) <= len(t) else token
        if len(longer) == 0:
            continue
        sim = 1.0 - distance(shorter, longer) / len(longer)
        if sim > best:
            best = sim
    return best


def _team_score(fs_team: str, fb_team: str) -> float:
    """
    Composite similarity score between two team name strings.
    Returns a float in [0, 1]. Higher = better match.
    """
    fs_n = _normalize(fs_team)
    fb_n = _normalize(fb_team)

    if not fs_n or not fb_n:
        return 0.0

    # Direct substring
    if fs_n in fb_n or fb_n in fs_n:
        return 0.92

    # Acronym check
    if _acronym_match(fs_n, fb_n) or _acronym_match(fb_n, fs_n):
        return 0.88

    # Character-level Levenshtein ratio
    lev = ratio(fs_n, fb_n)

    # Token-level Jaccard + best-token-Levenshtein
    fs_tok = set(_tokenize(fs_team))
    fb_tok = set(_tokenize(fb_team))
    intersection = fs_tok & fb_tok
    union = fs_tok | fb_tok
    jaccard = len(intersection) / max(len(union), 1)

    shorter_set = fs_tok if len(fs_tok) <= len(fb_tok) else fb_tok
    longer_set  = fb_tok if len(fs_tok) <= len(fb_tok) else fs_tok
    token_scores = [_best_token_lev(t, list(longer_set)) for t in shorter_set]
    token_avg = sum(token_scores) / max(len(token_scores), 1)

    # Substring bonus
    shorter = fs_n if len(fs_n) <= len(fb_n) else fb_n
    longer  = fb_n if len(fs_n) <= len(fb_n) else fs_n
    substr  = 0.15 if shorter in longer else 0.0

    return max(lev * 0.4 + jaccard * 0.3 + token_avg * 0.2 + substr, 0.0)


class GrokMatcher:
    """
    Cascade resolver: search_terms → fuzzy → LLM (Gemini).
    Imported by fb_manager.py for Chapter 1 Page 1 resolution.
    """

    def __init__(self):
        self._cache: Dict[str, Optional[Dict]] = {}

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
        if not team_id or not conn:
            return
        try:
            normalized_alias = _normalize(new_alias)
            terms = self._get_search_terms(conn, team_id)
            if normalized_alias not in [_normalize(t) for t in terms]:
                terms.append(new_alias)
                conn.execute(
                    "INSERT INTO search_dict (team_id, search_terms) VALUES (?, ?) "
                    "ON CONFLICT(team_id) DO UPDATE SET search_terms = excluded.search_terms",
                    (team_id, json.dumps(terms))
                )
                conn.commit()
        except Exception as e:
            pass  # Non-critical

    async def resolve_with_cascade(
        self,
        fs_fix: Dict,
        fb_matches: List[Dict],
        conn: sqlite3.Connection,
    ) -> Tuple[Optional[Dict], float, str]:
        """
        3-stage cascade:
          1. search_terms — exact alias lookup from search_dict table
          2. fuzzy — Levenshtein/Jaccard scoring, threshold 0.72
          3. llm — Gemini pro fallback for ambiguous cases

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

        # ── Stage 1: search_terms ────────────────────────────────────────────
        home_terms = self._get_search_terms(conn, home_id)
        away_terms = self._get_search_terms(conn, away_id)
        h_norm = [_normalize(t) for t in [home] + home_terms]
        a_norm = [_normalize(t) for t in [away] + away_terms]

        for m in fb_matches:
            fb_h = _normalize(self._get_name(m, 'home'))
            fb_a = _normalize(self._get_name(m, 'away'))
            h_match = fb_h in h_norm or any(fb_h in t for t in h_norm)
            a_match = fb_a in a_norm or any(fb_a in t for t in a_norm)
            if h_match and a_match:
                return {**m, 'matched': True}, 0.98, 'search_terms'

        # ── Stage 2: fuzzy ───────────────────────────────────────────────────
        best_fuzzy: Optional[Dict] = None
        fuzzy_score = 0.0
        final_home = home
        final_away = away

        for m in fb_matches:
            fb_h = self._get_name(m, 'home')
            fb_a = self._get_name(m, 'away')
            home_s = _team_score(home, fb_h)
            away_s = _team_score(away, fb_a)
            score = (home_s + away_s) / 2.0
            if score > fuzzy_score:
                fuzzy_score = score
                best_fuzzy = m
                final_home = fb_h
                final_away = fb_a

        if best_fuzzy and fuzzy_score >= 0.72:
            self._auto_learn(conn, home_id, final_home)
            self._auto_learn(conn, away_id, final_away)
            return {**best_fuzzy, 'matched': True}, fuzzy_score, 'fuzzy'

        # ── Stage 3: LLM (Gemini) ────────────────────────────────────────────
        llm_match, llm_score = await self._llm_resolve(
            f"{home} vs {away}", fb_matches
        )
        if llm_match:
            return {**llm_match, 'matched': True}, llm_score, 'llm'

        return None, 0.0, 'failed'

    def resolve(
        self,
        fs_name: str,
        fb_matches: List[Dict],
    ) -> Tuple[Optional[Dict], float, str]:
        """Synchronous fuzzy-only resolve (legacy / test use)."""
        res, score, _ = asyncio.get_event_loop().run_until_complete(
            self.resolve_with_cascade(
                {'home_team_name': fs_name.split(' vs ')[0] if ' vs ' in fs_name else fs_name,
                 'away_team_name': fs_name.split(' vs ')[1] if ' vs ' in fs_name else ''},
                fb_matches,
                None,
            )
        )
        return res, score, _

    def _fuzzy_resolve(
        self,
        fs_name: str,
        fb_matches: List[Dict],
    ) -> Tuple[Optional[Dict], float, str]:
        """Direct fuzzy resolve without search_terms or LLM stages."""
        fs_raw = fs_name
        sep = ' vs ' if ' vs ' in fs_raw else ' - '
        parts = fs_raw.split(sep, 1)
        fs_home_raw = parts[0].strip() if len(parts) >= 2 else fs_raw
        fs_away_raw = parts[1].strip() if len(parts) >= 2 else ''

        best_match: Optional[Dict] = None
        best_score = 0.0

        for m in fb_matches:
            fb_home = self._get_name(m, 'home')
            fb_away = self._get_name(m, 'away')
            home_s = _team_score(fs_home_raw, fb_home)
            away_s = _team_score(fs_away_raw, fb_away) if fs_away_raw else home_s
            score = (home_s + away_s) / 2.0
            if score > best_score:
                best_score = score
                best_match = m

        if best_match and best_score >= 0.65:
            return best_match, best_score, 'fuzzy'

        if best_match:
            candidate = best_match
            max_len = max(len(fs_home_raw), 1)
            dist = distance(_normalize(fs_home_raw), _normalize(self._get_name(best_match, 'home')))
            if dist < max_len * 0.4:
                return candidate, best_score, 'fuzzy'

        return None, 0.0, 'failed'

    async def _llm_resolve(
        self,
        fs_name: str,
        fb_matches: List[Dict],
    ) -> Tuple[Optional[Dict], float]:
        """Gemini-based resolver as final fallback for ambiguous matches."""
        if not HAS_GEMINI:
            return None, 0.0

        fallback_match: Optional[Dict] = None
        fallback_score = 0.0

        try:
            from Core.Intelligence.aigo_suite import AIGOSuite
            health_manager = AIGOSuite.get_health_manager()
        except Exception:
            health_manager = None

        candidates = []
        for i, m in enumerate(fb_matches[:8]):
            fb_home = self._get_name(m, 'home')
            fb_away = self._get_name(m, 'away')
            candidates.append(f"{i}: {fb_home} vs {fb_away}")

        if not candidates:
            return None, 0.0

        prompt_text = (
            f"Match to find: '{fs_name}'\n"
            f"Candidates (index: home vs away):\n"
            + '\n'.join(candidates)
            + '\n\nReply with the index number ONLY of the best match, or -1 if none fits.'
        )

        model_chain = ['gemini-2.0-flash', 'gemini-2.0-flash-lite', 'gemini-1.5-flash-8b']

        # Fast-exit: if all models are dead this session, skip entirely
        if all(m in _session_dead_models for m in model_chain):
            return fallback_match, fallback_score

        api_key = os.environ.get('GEMINI_API_KEY', '')
        if not api_key:
            return fallback_match, fallback_score

        for model_name in model_chain:
            if model_name in _session_dead_models:
                continue
            try:
                client = genai.Client(api_key=api_key)
                # Run sync SDK call off the event loop thread
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda mn=model_name: client.models.generate_content(
                        model=mn,
                        contents=prompt_text,
                        config=types.GenerateContentConfig(
                            temperature=0.0,
                            max_output_tokens=10,
                        ),
                    )
                )
                answer = (response.text or '').strip()
                try:
                    i = int(answer)
                    if 0 <= i < len(fb_matches):
                        return fb_matches[i], 0.80
                except ValueError:
                    pass
                break  # valid response, wrong format — stop rotating
            except Exception as e:
                err_str = str(e)
                # Mark dead for ANY non-transient error — stops the 400 flood
                if any(code in err_str for code in ('400', '403', '404', '429', 'quota', 'invalid api key', 'INVALID_ARGUMENT')):
                    _session_dead_models.add(model_name)
                    if 'invalid api key' in err_str.lower() or '403' in err_str:
                        break  # key-level failure — no point trying other models
                else:
                    # Unknown transient — skip this model but keep others alive
                    _session_dead_models.add(model_name)

        return fallback_match, fallback_score

