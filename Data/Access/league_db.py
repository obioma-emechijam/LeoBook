# league_db.py: Unified SQLite database layer for ALL LeoBook data.
# Part of LeoBook Data â€” Access Layer
#
# This is THE SINGLE source of truth for all persistent data.
# CSV files are auto-imported on first init_db() call, then renamed to .csv.bak.

import sqlite3
import json
import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from Core.Utils.constants import now_ng
from Data.Access.league_db_schema import (
    _SCHEMA_SQL, _ALTER_MIGRATIONS, _COMPUTED_STANDINGS_SQL,
)

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Store")
DB_PATH = os.path.join(DB_DIR, "leobook.db")
LEAGUES_JSON_PATH = os.path.join(DB_DIR, "leagues.json")

# Module-level cache for leagues.json
_leagues_json_cache: Optional[Dict[str, Dict[str, Any]]] = None

def get_fb_url_for_league(conn, league_id: str) -> Optional[str]:
    """
    Returns the fb_url for a league from leagues.json if it has been mapped.
    Returns None if the league has no fb_ keys yet or not found.
    Cached at module level to avoid redundant disk I/O.
    """
    global _leagues_json_cache
    
    if _leagues_json_cache is None:
        try:
            if os.path.exists(LEAGUES_JSON_PATH):
                with open(LEAGUES_JSON_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Create a lookup map by league_id
                    _leagues_json_cache = {l['league_id']: l for l in data if 'league_id' in l}
            else:
                _leagues_json_cache = {}
        except Exception as e:
            print(f"  [DB] Error loading leagues.json for cache: {e}")
            _leagues_json_cache = {}

    league_entry = _leagues_json_cache.get(league_id)
    if league_entry:
        return league_entry.get('fb_url')
    return None


def get_connection() -> sqlite3.Connection:
    """Get a thread-safe SQLite connection with WAL mode.
    Auto-recovers from corrupted DB by deleting and recreating."""
    os.makedirs(DB_DIR, exist_ok=True)
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.DatabaseError as e:
        if "malformed" in str(e).lower():
            print(f"  [!] Corrupted DB detected â€” deleting and recreating: {DB_PATH}")
            try:
                conn.close()
            except Exception:
                pass
            # Remove corrupted DB + WAL/SHM files
            for suffix in ('', '-wal', '-shm'):
                path = DB_PATH + suffix
                if os.path.exists(path):
                    os.remove(path)
            # Recreate fresh
            conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")
            conn.row_factory = sqlite3.Row
            return conn
        raise


# ---------------------------------------------------------------------------
# All DDL strings and ALTER migrations are in league_db_schema.py.
# Imported at top of file: _SCHEMA_SQL, _ALTER_MIGRATIONS, _COMPUTED_STANDINGS_SQL




# Columns that need to be added to existing tables that were created
# before the unified schema. ALTER TABLE is idempotent-safe via try/except.

# CSV file â†’ SQLite table mapping for auto-import.
# Key: csv filename, Value: (table_name, primary_key_column, column_rename_map)



def computed_standings(conn=None, league_id=None, season=None, before_date=None):
    """Compute league standings on-the-fly from the schedules table.

    Always up-to-date, even during live matches (if scores are propagated).
    Replaces the old standings table (removed in v7.0).

    Args:
        conn: SQLite connection (optional, uses default)
        league_id: Filter by league_id (optional)
        season: Filter by season (optional)
        before_date: Only include matches before this date (YYYY-MM-DD).
                     Used by RL training to reconstruct historical standings.
                     Default None = no date filter (live behaviour preserved).

    Returns:
        List of dicts with: league_id, team_id, team_name, season,
        played, wins, draws, losses, goals_for, goals_against,
        goal_difference, points
    """
    conn = conn or init_db()
    filters = ""
    params = []
    if league_id:
        filters += " AND league_id = ?"
        params.append(league_id)
    if season:
        filters += " AND season = ?"
        params.append(season)
    if before_date:
        filters += " AND date < ?"
        params.append(before_date)

    sql = _COMPUTED_STANDINGS_SQL.format(filters=filters)
    cursor = conn.execute(sql, params)
    columns = [d[0] for d in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    # Add rank/position since tag_generator expects it
    for i, res in enumerate(results):
        res["position"] = i + 1
    
    return results


def _run_alter_migrations(conn: sqlite3.Connection):
    """Add columns to existing tables. Silently skips if column already exists."""
    for table, column, col_type in _ALTER_MIGRATIONS:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()


def _get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    """Get list of column names for a table."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]




def _create_post_alter_indexes(conn: sqlite3.Connection):
    """Create indexes on columns added by ALTER TABLE."""
    post_alter_indexes = [
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_teams_team_id_unique ON teams(team_id)",
        "CREATE INDEX IF NOT EXISTS idx_teams_team_id ON teams(team_id)",
    ]
    for sql in post_alter_indexes:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass
    conn.commit()


def _reconstruct_teams_table_if_legacy_unique_exists(conn: sqlite3.Connection):
    """
    Remove legacy UNIQUE(name, country_code) constraint from teams table.
    SQLite does not support DROP CONSTRAINT, so we must reconstruct the table.
    """
    try:
        # Check if the constraint exists in the schema
        res = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='teams'").fetchone()
        if not res:
            return
        sql = res[0]

        # Only reconstruct if UNIQUE(name, country_code) is present
        if "UNIQUE(name, country_code)" not in sql and "UNIQUE (name, country_code)" not in sql:
            return

        print("  [Migration] Removing legacy UNIQUE constraint from teams table...")

        # 1. Create temporary table with CORRECT schema (matching _SCHEMA_SQL + migrations)
        temp_table_sql = """
            CREATE TABLE teams_new (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                team_id             TEXT UNIQUE,
                name                TEXT NOT NULL,
                league_ids          JSON,
                crest               TEXT,
                country_code        TEXT,
                url                 TEXT,
                hq_crest            INTEGER DEFAULT 0,
                country             TEXT,
                city                TEXT,
                stadium             TEXT,
                other_names         TEXT,
                abbreviations       TEXT,
                search_terms        TEXT,
                last_updated        TEXT DEFAULT (datetime('now'))
            )
        """
        conn.execute(temp_table_sql)

        # 2. Copy data. Handle columns that might not exist in old table yet
        # (Though by this point init_db has run _run_alter_migrations)
        cursor = conn.execute("PRAGMA table_info(teams)")
        existing_cols = [row[1] for row in cursor.fetchall()]
        
        target_cols = [
            'id', 'team_id', 'name', 'league_ids', 'crest', 'country_code', 'url',
            'hq_crest', 'country', 'city', 'stadium', 'other_names', 'abbreviations',
            'search_terms', 'last_updated'
        ]
        
        # Filter target_cols to only those that exist in the old table
        cols_to_copy = [c for c in target_cols if c in existing_cols]
        cols_str = ", ".join(cols_to_copy)

        conn.execute(f"INSERT INTO teams_new ({cols_str}) SELECT {cols_str} FROM teams")

        # 3. Swap tables
        conn.execute("DROP TABLE teams")
        conn.execute("ALTER TABLE teams_new RENAME TO teams")

        # 4. Re-create indexes
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_teams_team_id_unique ON teams(team_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_teams_team_id ON teams(team_id)")

        conn.commit()
        print("  [Migration] [OK] Teams table reconstructed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"  [Migration] [!] Error reconstructing teams table: {e}")



def _migrate_match_odds_if_needed(conn: sqlite3.Connection):
    """Drop old match_odds table if it has the legacy schema (last_updated column).
    The new schema uses line DEFAULT '' instead of nullable line, and no last_updated.
    Data is re-extractable so dropping is safe."""
    try:
        res = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='match_odds'"
        ).fetchone()
        if not res:
            return  # Table doesn't exist yet â€” will be created by _SCHEMA_SQL
        schema_sql = res[0]
        if 'last_updated' in schema_sql:
            print("  [Migration] Dropping legacy match_odds table (schema v7 -> v8)...")
            conn.execute("DROP TABLE IF EXISTS match_odds")
            conn.commit()
            print("  [Migration] [OK] match_odds will be recreated with v8 schema.")
    except Exception as e:
        print(f"  [Migration] [!] match_odds check failed: {e}")


def init_db(conn: Optional[sqlite3.Connection] = None) -> sqlite3.Connection:
    """Create all tables, run migrations. Returns the connection."""
    if conn is None:
        conn = get_connection()

    _migrate_match_odds_if_needed(conn)

    conn.executescript(_SCHEMA_SQL)
    conn.commit()

    _run_alter_migrations(conn)
    _create_post_alter_indexes(conn)
    _reconstruct_teams_table_if_legacy_unique_exists(conn)
    _initialize_countries(conn)

    return conn


def _initialize_countries(conn: sqlite3.Connection):
    """Populates countries table from Data/Store/country.json if empty."""
    row_count = conn.execute("SELECT COUNT(*) FROM countries").fetchone()[0]
    if row_count > 0:
        return

    json_path = os.path.join(DB_DIR, "country.json")
    if not os.path.exists(json_path):
        print(f"  [DB] Warning: {json_path} not found. Skipping country init.")
        return

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            countries = json.load(f)
        
        now = datetime.utcnow().isoformat()
        countries_data = []
        for c in countries:
            countries_data.append({
                'code': c.get('code'),
                'name': c.get('name'),
                'continent': c.get('continent', ''),
                'capital': c.get('capital', ''),
                'flag_1x1': c.get('flag_1x1', ''),
                'flag_4x3': c.get('flag_4x3', ''),
                'last_updated': now
            })

        conn.executemany("""
            INSERT INTO countries (code, name, continent, capital, flag_1x1, flag_4x3, last_updated)
            VALUES (:code, :name, :continent, :capital, :flag_1x1, :flag_4x3, :last_updated)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name,
                continent=excluded.continent,
                capital=excluded.capital,
                flag_1x1=excluded.flag_1x1,
                flag_4x3=excluded.flag_4x3,
                last_updated=excluded.last_updated
        """, countries_data)
        conn.commit()
        print(f"  [DB] Initialized {len(countries)} countries from country.json.")
    except Exception as e:
        print(f"  [DB] Error initializing countries: {e}")


# ---------------------------------------------------------------------------
# League operations
# ---------------------------------------------------------------------------

def upsert_league(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """Insert or update a league. Returns the row id."""
    now = now_ng().isoformat()
    cur = conn.execute(
        """INSERT INTO leagues (league_id, fs_league_id, country_code, continent, name, crest,
               current_season, url, region, region_flag, region_url,
               other_names, abbreviations, search_terms, date_updated, last_updated)
           VALUES (:league_id, :fs_league_id, :country_code, :continent, :name, :crest,
               :current_season, :url, :region, :region_flag, :region_url,
               :other_names, :abbreviations, :search_terms, :date_updated, :last_updated)
           ON CONFLICT(league_id) DO UPDATE SET
               fs_league_id   = COALESCE(excluded.fs_league_id, leagues.fs_league_id),
               country_code   = COALESCE(excluded.country_code, leagues.country_code),
               continent      = COALESCE(excluded.continent, leagues.continent),
               name           = COALESCE(excluded.name, leagues.name),
               crest          = COALESCE(excluded.crest, leagues.crest),
               current_season = COALESCE(excluded.current_season, leagues.current_season),
               url            = COALESCE(excluded.url, leagues.url),
               region         = COALESCE(excluded.region, leagues.region),
               region_flag    = COALESCE(excluded.region_flag, leagues.region_flag),
               region_url     = COALESCE(excluded.region_url, leagues.region_url),
               other_names    = COALESCE(excluded.other_names, leagues.other_names),
               abbreviations  = COALESCE(excluded.abbreviations, leagues.abbreviations),
               search_terms   = COALESCE(excluded.search_terms, leagues.search_terms),
               date_updated   = COALESCE(excluded.date_updated, leagues.date_updated),
               last_updated   = excluded.last_updated
        """,
        {
            "league_id": data["league_id"],
            "fs_league_id": data.get("fs_league_id"),
            "country_code": data.get("country_code"),
            "continent": data.get("continent"),
            "name": data.get("name", data.get("league", "")),
            "crest": data.get("crest", data.get("league_crest")),
            "current_season": data.get("current_season"),
            "url": data.get("url", data.get("league_url")),
            "region": data.get("region"),
            "region_flag": data.get("region_flag"),
            "region_url": data.get("region_url"),
            "other_names": data.get("other_names"),
            "abbreviations": data.get("abbreviations"),
            "search_terms": data.get("search_terms"),
            "date_updated": data.get("date_updated"),
            "last_updated": now,
        },
    )
    conn.commit()
    return cur.lastrowid


def get_league_db_id(conn: sqlite3.Connection, league_id: str) -> Optional[int]:
    """Get the auto-increment id for a league by its league_id string."""
    row = conn.execute("SELECT id FROM leagues WHERE league_id = ?", (league_id,)).fetchone()
    return row["id"] if row else None


def mark_league_processed(conn: sqlite3.Connection, league_id: str):
    """Flag a league as fully enriched."""
    conn.execute(
        "UPDATE leagues SET processed = 1, last_updated = ? WHERE league_id = ?",
        (now_ng().isoformat(), league_id),
    )
    conn.commit()


def get_unprocessed_leagues(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return all leagues not yet processed."""
    rows = conn.execute(
        "SELECT * FROM leagues WHERE processed = 0 ORDER BY id"
    ).fetchall()
    return [dict(r) for r in rows]


def get_leagues_with_gaps(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Return leagues with missing critical enrichment data.
    
    This is the SMART default: finds leagues that were either never processed
    OR were processed but have empty columns (silent failures).
    Checks: fs_league_id, region, crest, current_season.
    """
    rows = conn.execute(
        """SELECT * FROM leagues
           WHERE url IS NOT NULL AND url != ''
             AND (
               processed = 0
               OR fs_league_id IS NULL OR fs_league_id = ''
               OR region IS NULL OR region = ''
               OR crest IS NULL OR crest = ''
               OR current_season IS NULL OR current_season = ''
             )
           ORDER BY id"""
    ).fetchall()
    return [dict(r) for r in rows]


def get_leagues_missing_seasons(conn: sqlite3.Connection, min_seasons: int = 2) -> List[Dict[str, Any]]:
    """Return processed leagues that have fewer than min_seasons in the schedules table.
    
    Useful for triggering historical enrichment even if metadata is complete.
    """
    # Find league IDs that have at least min_seasons
    rows = conn.execute("""
        SELECT league_id FROM schedules
        WHERE season IS NOT NULL AND season != ''
        GROUP BY league_id
        HAVING COUNT(DISTINCT season) >= ?
    """, (min_seasons,)).fetchall()
    
    ok_ids = {r[0] for r in rows}
    
    # Get all processed leagues
    all_processed = conn.execute("SELECT * FROM leagues WHERE processed = 1 AND url != ''").fetchall()
    
    missing = []
    for row in all_processed:
        if row['league_id'] not in ok_ids:
            missing.append(dict(row))
            
    return missing


def get_stale_leagues(conn: sqlite3.Connection, days: int = 7) -> List[Dict[str, Any]]:
    """Return leagues not updated in the last N days."""
    rows = conn.execute(
        """SELECT * FROM leagues
           WHERE url IS NOT NULL AND url != ''
             AND (
               last_updated IS NULL
               OR last_updated < datetime('now', ? || ' days')
             )
           ORDER BY id""",
        (f"-{days}",)
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Team operations
# ---------------------------------------------------------------------------

def upsert_team(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """Insert or update a team by team_id. Returns the row id."""
    now = now_ng().isoformat()
    new_league_ids = data.get("league_ids", [])
    team_id = data.get("team_id")

    # BUG #6 fix: Merge league_ids with existing instead of replacing
    if team_id:
        existing = conn.execute(
            "SELECT league_ids FROM teams WHERE team_id = ?", (team_id,)
        ).fetchone()
        if existing and existing[0]:
            try:
                old_ids = json.loads(existing[0])
                if isinstance(old_ids, list):
                    merged = list(set(old_ids + new_league_ids))
                    new_league_ids = merged
            except (json.JSONDecodeError, TypeError):
                pass

    league_ids_json = json.dumps(new_league_ids) if new_league_ids else None

    if team_id:
        # Prefer team_id as the unique key
        cur = conn.execute(
            """INSERT INTO teams (team_id, name, league_ids, crest, country_code, url,
                   country, city, stadium, other_names, abbreviations, search_terms, last_updated)
               VALUES (:team_id, :name, :league_ids, :crest, :country_code, :url,
                   :country, :city, :stadium, :other_names, :abbreviations, :search_terms, :last_updated)
               ON CONFLICT(team_id) DO UPDATE SET
                   -- ROOT CAUSE 3 FIX: Canonical name = first ingestion.
                   -- Multi-league teams get scraped many times with different transliterations
                   -- or aliases. We KEEP the first name we stored (NULLIF guard) so that
                   -- predictions always see the canonical home-league name.
                   -- To update a team's name intentionally, do it directly in the DB.
                   name           = COALESCE(NULLIF(teams.name, ''), excluded.name),
                   league_ids     = COALESCE(excluded.league_ids, teams.league_ids),
                   crest          = COALESCE(excluded.crest, teams.crest),
                   -- NULLIF guards against empty string ('') from international leagues
                   -- overwriting a real country_code set by a domestic league worker.
                   country_code   = COALESCE(NULLIF(excluded.country_code, ''), teams.country_code),
                   url            = COALESCE(excluded.url, teams.url),
                   country        = COALESCE(excluded.country, teams.country),
                   city           = COALESCE(excluded.city, teams.city),
                   stadium        = COALESCE(excluded.stadium, teams.stadium),
                   other_names    = COALESCE(excluded.other_names, teams.other_names),
                   abbreviations  = COALESCE(excluded.abbreviations, teams.abbreviations),
                   search_terms   = COALESCE(excluded.search_terms, teams.search_terms),
                   last_updated   = excluded.last_updated
            """,
            {
                "team_id": team_id,
                "name": data.get("name", data.get("team_name", "")),
                "league_ids": league_ids_json,
                "crest": data.get("crest", data.get("team_crest")),
                "country_code": data.get("country_code") or None,  # store NULL not ''
                "url": data.get("url", data.get("team_url")),
                "country": data.get("country"),
                "city": data.get("city"),
                "stadium": data.get("stadium"),
                "other_names": data.get("other_names"),
                "abbreviations": data.get("abbreviations"),
                "search_terms": data.get("search_terms"),
                "last_updated": now,
            },
        )
    else:
        # Fallback: no team_id â€” look up by name+country_code to avoid duplicates
        name = data.get("name", data.get("team_name", ""))
        country_code = data.get("country_code") or None  # store NULL not ''
        existing = None
        if country_code:
            existing = conn.execute(
                "SELECT id FROM teams WHERE name = ? AND country_code = ?",
                (name, country_code),
            ).fetchone()
        if not existing:
            existing = conn.execute(
                "SELECT id FROM teams WHERE name = ?",
                (name,),
            ).fetchone()

        if existing:
            # Update existing row
            cur = conn.execute(
                """UPDATE teams SET
                       league_ids   = :league_ids,
                       crest        = COALESCE(:crest, crest),
                       country_code = COALESCE(NULLIF(:country_code, ''), country_code),
                       url          = COALESCE(:url, url),
                       last_updated = :last_updated
                   WHERE id = :row_id""",
                {
                    "league_ids": league_ids_json,
                    "crest": data.get("crest"),
                    "country_code": country_code,
                    "url": data.get("url"),
                    "last_updated": now,
                    "row_id": existing[0],
                },
            )
        else:
            # Truly new team
            cur = conn.execute(
                """INSERT INTO teams (name, league_ids, crest, country_code, url, last_updated)
                   VALUES (:name, :league_ids, :crest, :country_code, :url, :last_updated)""",
                {
                    "name": name,
                    "league_ids": league_ids_json,
                    "crest": data.get("crest"),
                    "country_code": country_code,
                    "url": data.get("url"),
                    "last_updated": now,
                },
            )
    conn.commit()
    return cur.lastrowid


def get_team_id(conn: sqlite3.Connection, name: str, country_code: str = None) -> Optional[int]:
    """Look up team id by name (and optionally country_code)."""
    if country_code:
        row = conn.execute(
            "SELECT id FROM teams WHERE name = ? AND country_code = ?", (name, country_code)
        ).fetchone()
    else:
        row = conn.execute("SELECT id FROM teams WHERE name = ?", (name,)).fetchone()
    return row["id"] if row else None


# ---------------------------------------------------------------------------
# Fixture operations
# ---------------------------------------------------------------------------

def upsert_fixture(conn: sqlite3.Connection, data: Dict[str, Any]) -> int:
    """Insert or update a fixture. Returns the row id."""
    now = now_ng().isoformat()
    extra_json = json.dumps(data.get("extra")) if data.get("extra") else None
    fixture_id = data.get("fixture_id", "")

    cur = conn.execute(
        """INSERT INTO schedules (
               fixture_id, date, time, league_id,
               home_team_id, home_team_name, away_team_id, away_team_name,
               home_score, away_score, extra, league_stage,
               match_status, season, home_crest, away_crest, url,
               country_league, match_link, last_updated
           ) VALUES (
               :fixture_id, :date, :time, :league_id,
               :home_team_id, :home_team_name, :away_team_id, :away_team_name,
               :home_score, :away_score, :extra, :league_stage,
               :match_status, :season, :home_crest, :away_crest, :url,
               :country_league, :match_link, :last_updated
           )
           ON CONFLICT(fixture_id) DO UPDATE SET
               date           = COALESCE(excluded.date, schedules.date),
               time           = COALESCE(excluded.time, schedules.time),
               home_team_id   = COALESCE(excluded.home_team_id, schedules.home_team_id),
               home_team_name = COALESCE(excluded.home_team_name, schedules.home_team_name),
               away_team_id   = COALESCE(excluded.away_team_id, schedules.away_team_id),
               away_team_name = COALESCE(excluded.away_team_name, schedules.away_team_name),
               home_score     = COALESCE(excluded.home_score, schedules.home_score),
               away_score     = COALESCE(excluded.away_score, schedules.away_score),
               extra          = COALESCE(excluded.extra, schedules.extra),
               match_status   = COALESCE(excluded.match_status, schedules.match_status),
               home_crest     = COALESCE(excluded.home_crest, schedules.home_crest),
               away_crest     = COALESCE(excluded.away_crest, schedules.away_crest),
               country_league  = COALESCE(excluded.country_league, schedules.country_league),
               match_link     = COALESCE(excluded.match_link, schedules.match_link),
               last_updated   = excluded.last_updated
        """,
        {
            "fixture_id": fixture_id,
            "date": data.get("date"),
            "time": data.get("time", data.get("match_time")),
            "league_id": data.get("league_id"),
            "home_team_id": data.get("home_team_id"),
            "home_team_name": data.get("home_team_name", data.get("home_team")),
            "away_team_id": data.get("away_team_id"),
            "away_team_name": data.get("away_team_name", data.get("away_team")),
            "home_score": data.get("home_score"),
            "away_score": data.get("away_score"),
            "extra": extra_json,
            "league_stage": data.get("league_stage"),
            "match_status": data.get("match_status"),
            "season": data.get("season"),
            "home_crest": data.get("home_crest"),
            "away_crest": data.get("away_crest"),
            "url": data.get("url"),
            "country_league": data.get("country_league"),
            "match_link": data.get("match_link"),
            "last_updated": now,
        },
    )
    conn.commit()
    return cur.lastrowid


def bulk_upsert_fixtures(conn: sqlite3.Connection, fixtures: List[Dict[str, Any]]):
    """Batch insert/update fixtures for performance."""
    now = now_ng().isoformat()
    rows = []
    for f in fixtures:
        extra_json = json.dumps(f.get("extra")) if f.get("extra") else None
        rows.append((
            f.get("fixture_id", ""), f.get("date"), f.get("time", f.get("match_time")),
            f.get("league_id"),
            f.get("home_team_id"), f.get("home_team_name", f.get("home_team")),
            f.get("away_team_id"), f.get("away_team_name", f.get("away_team")),
            f.get("home_score"), f.get("away_score"),
            extra_json, f.get("league_stage"),
            f.get("match_status"), f.get("season"),
            f.get("home_crest"), f.get("away_crest"),
            f.get("url"), f.get("country_league"), f.get("match_link"), now,
        ))
    conn.executemany(
        """INSERT INTO schedules (
               fixture_id, date, time, league_id,
               home_team_id, home_team_name, away_team_id, away_team_name,
               home_score, away_score, extra, league_stage,
               match_status, season, home_crest, away_crest, url,
               country_league, match_link, last_updated
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(fixture_id) DO UPDATE SET
               date           = COALESCE(excluded.date, schedules.date),
               time           = COALESCE(excluded.time, schedules.time),
               league_id      = COALESCE(excluded.league_id, schedules.league_id),
               home_team_id   = COALESCE(excluded.home_team_id, schedules.home_team_id),
               home_team_name = COALESCE(excluded.home_team_name, schedules.home_team_name),
               away_team_id   = COALESCE(excluded.away_team_id, schedules.away_team_id),
               away_team_name = COALESCE(excluded.away_team_name, schedules.away_team_name),
               home_score     = COALESCE(excluded.home_score, schedules.home_score),
               away_score     = COALESCE(excluded.away_score, schedules.away_score),
               extra          = COALESCE(excluded.extra, schedules.extra),
               league_stage   = COALESCE(excluded.league_stage, schedules.league_stage),
               match_status   = COALESCE(excluded.match_status, schedules.match_status),
               season         = COALESCE(excluded.season, schedules.season),
               home_crest     = COALESCE(excluded.home_crest, schedules.home_crest),
               away_crest     = COALESCE(excluded.away_crest, schedules.away_crest),
               url            = COALESCE(excluded.url, schedules.url),
               country_league  = COALESCE(excluded.country_league, schedules.country_league),
               match_link     = COALESCE(excluded.match_link, schedules.match_link),
               last_updated   = excluded.last_updated
        """,
        rows,
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Prediction operations
# ---------------------------------------------------------------------------

def upsert_prediction(conn: sqlite3.Connection, data: Dict[str, Any]):
    """Insert or update a prediction row."""
    now = now_ng().isoformat()
    # Normalize over_2.5 â†’ over_2_5
    if "over_2.5" in data:
        data["over_2_5"] = data.pop("over_2.5")

    cols = [
        "fixture_id", "date", "match_time", "country_league",
        "home_team", "away_team", "home_team_id", "away_team_id",
        "prediction", "confidence", "reason",
        "xg_home", "xg_away", "btts", "over_2_5",
        "best_score", "top_scores", "home_form_n", "away_form_n",
        "home_tags", "away_tags", "h2h_tags", "standings_tags",
        "h2h_count", "actual_score", "outcome_correct",
        "status", "match_link", "odds",
        "market_reliability_score", "home_crest_url", "away_crest_url",
        "recommendation_score", "h2h_fixture_ids", "form_fixture_ids",
        "standings_snapshot", "league_stage", "generated_at",
        "home_score", "away_score", "chosen_market", "market_id",
        "rule_explanation", "override_reason", "statistical_edge",
        "pure_model_suggestion", "last_updated",
    ]
    values = {c: data.get(c) for c in cols}
    values["last_updated"] = now

    # JSON-serialize complex fields
    for jf in ("h2h_fixture_ids", "form_fixture_ids", "standings_snapshot"):
        if values[jf] is not None and not isinstance(values[jf], str):
            values[jf] = json.dumps(values[jf])

    present = {k: v for k, v in values.items() if v is not None}
    col_str = ", ".join(present.keys())
    placeholders = ", ".join([f":{k}" for k in present.keys()])
    updates = ", ".join([f"{k} = excluded.{k}" for k in present.keys() if k != "fixture_id"])

    conn.execute(
        f"INSERT INTO predictions ({col_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(fixture_id) DO UPDATE SET {updates}",
        present,
    )
    conn.commit()


def get_predictions(conn: sqlite3.Connection, status: str = None) -> List[Dict[str, Any]]:
    """Get predictions, optionally filtered by status."""
    if status:
        rows = conn.execute("SELECT * FROM predictions WHERE status = ?", (status,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM predictions").fetchall()
    return [dict(r) for r in rows]


def update_prediction(conn: sqlite3.Connection, fixture_id: str, updates: Dict[str, Any]):
    """Update specific fields on a prediction."""
    now = now_ng().isoformat()
    updates["last_updated"] = now
    set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
    updates["fixture_id"] = fixture_id
    conn.execute(f"UPDATE predictions SET {set_clause} WHERE fixture_id = :fixture_id", updates)
    conn.commit()


# ---------------------------------------------------------------------------
# Standings operations
# ---------------------------------------------------------------------------

def upsert_standing(conn: sqlite3.Connection, data: Dict[str, Any]):
    """Insert or update a standings row."""
    now = now_ng().isoformat()
    conn.execute(
        """INSERT INTO standings (standings_key, league_id, team_id, team_name,
               position, played, wins, draws, losses,
               goals_for, goals_against, goal_difference, points,
               country_league, last_updated)
           VALUES (:standings_key, :league_id, :team_id, :team_name,
               :position, :played, :wins, :draws, :losses,
               :goals_for, :goals_against, :goal_difference, :points,
               :country_league, :last_updated)
           ON CONFLICT(standings_key) DO UPDATE SET
               position       = excluded.position,
               played         = excluded.played,
               wins           = excluded.wins,
               draws          = excluded.draws,
               losses         = excluded.losses,
               goals_for      = excluded.goals_for,
               goals_against  = excluded.goals_against,
               goal_difference = excluded.goal_difference,
               points         = excluded.points,
               last_updated   = excluded.last_updated
        """,
        {
            "standings_key": data["standings_key"],
            "league_id": data.get("league_id"),
            "team_id": data.get("team_id"),
            "team_name": data.get("team_name"),
            "position": data.get("position"),
            "played": data.get("played"),
            "wins": data.get("wins"),
            "draws": data.get("draws"),
            "losses": data.get("losses"),
            "goals_for": data.get("goals_for"),
            "goals_against": data.get("goals_against"),
            "goal_difference": data.get("goal_difference"),
            "points": data.get("points"),
            "country_league": data.get("country_league"),
            "last_updated": now,
        },
    )
    conn.commit()


def get_standings(conn: sqlite3.Connection, country_league: str = None) -> List[Dict[str, Any]]:
    """Get standings, optionally filtered by country_league."""
    if country_league:
        rows = conn.execute(
            "SELECT * FROM standings WHERE country_league = ? ORDER BY position",
            (country_league,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM standings ORDER BY country_league, position").fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def log_audit_event(conn: sqlite3.Connection, data: Dict[str, Any]):
    """Insert an audit log entry."""
    now = now_ng().isoformat()
    conn.execute(
        """INSERT INTO audit_log (id, timestamp, event_type, description,
               balance_before, balance_after, stake, status, last_updated)
           VALUES (:id, :timestamp, :event_type, :description,
               :balance_before, :balance_after, :stake, :status, :last_updated)
        """,
        {
            "id": data.get("id", now),
            "timestamp": data.get("timestamp", now),
            "event_type": data.get("event_type"),
            "description": data.get("description"),
            "balance_before": data.get("balance_before"),
            "balance_after": data.get("balance_after"),
            "stake": data.get("stake"),
            "status": data.get("status"),
            "last_updated": now,
        },
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Live scores
# ---------------------------------------------------------------------------

def upsert_live_score(conn: sqlite3.Connection, data: Dict[str, Any]):
    """Insert or update a live score entry."""
    now = now_ng().isoformat()
    conn.execute(
        """INSERT INTO live_scores (fixture_id, home_team, away_team,
               home_score, away_score, minute, status,
               country_league, match_link, timestamp, last_updated)
           VALUES (:fixture_id, :home_team, :away_team,
               :home_score, :away_score, :minute, :status,
               :country_league, :match_link, :timestamp, :last_updated)
           ON CONFLICT(fixture_id) DO UPDATE SET
               home_score     = excluded.home_score,
               away_score     = excluded.away_score,
               minute         = excluded.minute,
               status         = excluded.status,
               timestamp      = excluded.timestamp,
               last_updated   = excluded.last_updated
        """,
        {
            "fixture_id": data["fixture_id"],
            "home_team": data.get("home_team"),
            "away_team": data.get("away_team"),
            "home_score": data.get("home_score"),
            "away_score": data.get("away_score"),
            "minute": data.get("minute"),
            "status": data.get("status"),
            "country_league": data.get("country_league"),
            "match_link": data.get("match_link"),
            "timestamp": data.get("timestamp", now),
            "last_updated": now,
        },
    )
    conn.commit()


# ---------------------------------------------------------------------------
# FB Matches
# ---------------------------------------------------------------------------

def upsert_fb_match(conn: sqlite3.Connection, data: Dict[str, Any]):
    """Insert or update an fb_matches entry.

    Writes only the columns present in both the local SQLite schema AND the
    Supabase v5.0 schema. Columns removed from Supabase (odds, booking_status,
    booking_details, booking_code, booking_url, last_extracted, league, status)
    are intentionally excluded to keep sync_schema.py from pushing dead writes.
    The SQLite table still has those columns for local diagnostics; they are
    simply not populated by this function.
    """
    now = now_ng().isoformat()
    conn.execute(
        """INSERT INTO fb_matches (site_match_id, date, time, home_team, away_team,
               url, last_updated, fixture_id, matched)
           VALUES (:site_match_id, :date, :time, :home_team, :away_team,
               :url, :last_updated, :fixture_id, :matched)
           ON CONFLICT(site_match_id) DO UPDATE SET
               date           = COALESCE(excluded.date, fb_matches.date),
               fixture_id     = COALESCE(excluded.fixture_id, fb_matches.fixture_id),
               matched        = COALESCE(excluded.matched, fb_matches.matched),
               last_updated   = excluded.last_updated
        """,
        {
            "site_match_id": data["site_match_id"],
            "date": data.get("date"),
            "time": data.get("time", data.get("match_time")),
            "home_team": data.get("home_team"),
            "away_team": data.get("away_team"),
            "url": data.get("url"),
            "last_updated": now,
            "fixture_id": data.get("fixture_id"),
            "matched": data.get("matched"),
        },
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Countries
# ---------------------------------------------------------------------------

def upsert_country(conn: sqlite3.Connection, data: Dict[str, Any]):
    """Insert or update a country entry."""
    now = now_ng().isoformat()
    conn.execute(
        """INSERT INTO countries (code, name, continent, capital, flag_1x1, flag_4x3, last_updated)
           VALUES (:code, :name, :continent, :capital, :flag_1x1, :flag_4x3, :last_updated)
           ON CONFLICT(code) DO UPDATE SET
               name      = COALESCE(excluded.name, countries.name),
               continent = COALESCE(excluded.continent, countries.continent),
               capital   = COALESCE(excluded.capital, countries.capital),
               flag_1x1  = COALESCE(excluded.flag_1x1, countries.flag_1x1),
               flag_4x3  = COALESCE(excluded.flag_4x3, countries.flag_4x3),
               last_updated = excluded.last_updated
        """,
        {
            "code": data["code"],
            "name": data.get("name"),
            "continent": data.get("continent"),
            "capital": data.get("capital"),
            "flag_1x1": data.get("flag_1x1"),
            "flag_4x3": data.get("flag_4x3"),
            "last_updated": now,
        },
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Accuracy reports
# ---------------------------------------------------------------------------

def upsert_accuracy_report(conn: sqlite3.Connection, data: Dict[str, Any]):
    """Insert or update an accuracy report."""
    now = now_ng().isoformat()
    conn.execute(
        """INSERT INTO accuracy_reports (report_id, timestamp, volume, win_rate,
               return_pct, period, last_updated)
           VALUES (:report_id, :timestamp, :volume, :win_rate,
               :return_pct, :period, :last_updated)
           ON CONFLICT(report_id) DO UPDATE SET
               volume     = excluded.volume,
               win_rate   = excluded.win_rate,
               return_pct = excluded.return_pct,
               last_updated = excluded.last_updated
        """,
        {
            "report_id": data["report_id"],
            "timestamp": data.get("timestamp"),
            "volume": data.get("volume"),
            "win_rate": data.get("win_rate"),
            "return_pct": data.get("return_pct"),
            "period": data.get("period"),
            "last_updated": now,
        },
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Generic query helpers
# ---------------------------------------------------------------------------

def query_all(conn: sqlite3.Connection, table: str, where: str = None,
              params: tuple = (), order_by: str = None) -> List[Dict[str, Any]]:
    """Generic SELECT * from table with optional WHERE and ORDER BY."""
    sql = f"SELECT * FROM {table}"
    if where:
        sql += f" WHERE {where}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def count_rows(conn: sqlite3.Connection, table: str) -> int:
    """Count rows in a table."""
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
def upsert_match_odds_batch(
    conn: sqlite3.Connection,
    odds_list: List[Dict[str, Any]],
) -> int:
    """Bulk upsert match odds. Returns rows written."""
    if not odds_list:
        return 0
    conn.executemany(
        """
        INSERT INTO match_odds (
            fixture_id, site_match_id, market_id, base_market,
            category, exact_outcome, line, odds_value,
            likelihood_pct, rank_in_list, extracted_at
        ) VALUES (
            :fixture_id, :site_match_id, :market_id, :base_market,
            :category, :exact_outcome, :line, :odds_value,
            :likelihood_pct, :rank_in_list, :extracted_at
        )
        ON CONFLICT(fixture_id, market_id, exact_outcome, line)
        DO UPDATE SET
            odds_value   = excluded.odds_value,
            extracted_at = excluded.extracted_at
        """,
        [
            {
                "fixture_id":     o["fixture_id"],
                "site_match_id":  o["site_match_id"],
                "market_id":      o["market_id"],
                "base_market":    o["base_market"],
                "category":       o.get("category", ""),
                "exact_outcome":  o["exact_outcome"],
                "line":           o.get("line") or "",
                "odds_value":     o["odds_value"],
                "likelihood_pct": o.get("likelihood_pct", 0),
                "rank_in_list":   o.get("rank_in_list", 0),
                "extracted_at":   o["extracted_at"],
            }
            for o in odds_list
        ],
    )
    conn.commit()
    return len(odds_list)
