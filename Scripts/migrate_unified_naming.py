"""
Migration script: Unified Naming + Crest Propagation
1. Renames Supabase tables/columns (idempotent)
2. Renames local fixtures -> schedules
3. Propagates Supabase crest URLs from teams into schedules
4. Auto-populates league_ids (optimized)
5. Drops dead 'country' column

Run once: python Scripts/migrate_unified_naming.py
"""
import sqlite3
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Data.Access.supabase_client import get_supabase_client

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Data', 'Store', 'leobook.db')

SUPABASE_MIGRATIONS = [
    "ALTER TABLE IF EXISTS public.region_league RENAME TO leagues;",
    "ALTER TABLE public.teams RENAME COLUMN team_name TO name;",
    "ALTER TABLE public.teams RENAME COLUMN team_crest TO crest;",
    "ALTER TABLE public.teams RENAME COLUMN team_url TO url;",
    "ALTER TABLE public.leagues RENAME COLUMN league TO name;",
    "ALTER TABLE public.leagues RENAME COLUMN league_crest TO crest;",
    "ALTER TABLE public.leagues RENAME COLUMN league_url TO url;",
    "ALTER TABLE public.teams DROP COLUMN IF EXISTS country;",
]

# Supabase: propagate team crests into schedules
SUPABASE_CREST_FIX = [
    # Update home_crest from teams where team has a Supabase URL
    """UPDATE public.schedules s
       SET home_crest = t.crest
       FROM public.teams t
       WHERE s.home_team_id = t.team_id
         AND t.crest LIKE 'http%'
         AND (s.home_crest IS NULL OR s.home_crest NOT LIKE 'http%supabase%');""",
    # Update away_crest from teams
    """UPDATE public.schedules s
       SET away_crest = t.crest
       FROM public.teams t
       WHERE s.away_team_id = t.team_id
         AND t.crest LIKE 'http%'
         AND (s.away_crest IS NULL OR s.away_crest NOT LIKE 'http%supabase%');""",
]


def migrate_supabase():
    sb = get_supabase_client()
    if not sb:
        print("[!] No Supabase connection. Skipping remote migration.")
        return False

    print("=== SUPABASE: TABLE/COLUMN RENAMES ===")
    for sql in SUPABASE_MIGRATIONS:
        label = sql.strip()[:80]
        try:
            sb.rpc('exec_sql', {'query': sql}).execute()
            print(f"  [OK] {label}")
        except Exception as e:
            err = str(e)
            if any(k in err for k in ['does not exist', 'already exists', '42703', '42P07']):
                print(f"  [SKIP] {label} (already done)")
            else:
                print(f"  [FAIL] {label}: {e}")

    print("\n=== SUPABASE: PROPAGATE CREST URLs ===")
    for sql in SUPABASE_CREST_FIX:
        label = "home_crest" if "home_crest" in sql else "away_crest"
        try:
            sb.rpc('exec_sql', {'query': sql}).execute()
            print(f"  [OK] Propagated {label} from teams")
        except Exception as e:
            print(f"  [FAIL] {label}: {e}")

    return True


def migrate_local_sqlite():
    print("\n=== LOCAL SQLITE ===")
    conn = sqlite3.connect(DB_PATH)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

    # 1. Rename fixtures -> schedules
    if 'schedules' in tables and 'fixtures' not in tables:
        print("  [SKIP] Already renamed (schedules exists).")
    elif 'fixtures' in tables:
        conn.execute("ALTER TABLE fixtures RENAME TO schedules;")
        conn.commit()
        print("  [OK] Renamed fixtures -> schedules")
    
    # Re-check tables after rename
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    sched_table = 'schedules' if 'schedules' in tables else None

    # 2. Propagate Supabase crest URLs locally (teams -> schedules)
    if sched_table:
        print("\n  Propagating crest URLs from teams -> schedules (local)...")
        h = conn.execute(f"""
            UPDATE {sched_table} SET home_crest = (
                SELECT t.crest FROM teams t WHERE t.team_id = {sched_table}.home_team_id AND t.crest LIKE 'http%'
            ) WHERE home_team_id IN (SELECT team_id FROM teams WHERE crest LIKE 'http%')
              AND (home_crest IS NULL OR home_crest NOT LIKE 'http%supabase%')
        """).rowcount
        a = conn.execute(f"""
            UPDATE {sched_table} SET away_crest = (
                SELECT t.crest FROM teams t WHERE t.team_id = {sched_table}.away_team_id AND t.crest LIKE 'http%'
            ) WHERE away_team_id IN (SELECT team_id FROM teams WHERE crest LIKE 'http%')
              AND (away_crest IS NULL OR away_crest NOT LIKE 'http%supabase%')
        """).rowcount
        conn.commit()
        print(f"  [OK] Updated {h} home_crest + {a} away_crest rows")

    # 3. Populate league_ids (optimized: build a temp table first)
    if sched_table:
        print("\n  Populating league_ids from schedules...")
        # Build mapping table for speed
        conn.execute("DROP TABLE IF EXISTS _tmp_team_leagues")
        conn.execute(f"""
            CREATE TEMP TABLE _tmp_team_leagues AS
            SELECT team_id, json_group_array(DISTINCT league_id) AS lids FROM (
                SELECT home_team_id AS team_id, league_id FROM {sched_table}
                WHERE home_team_id IS NOT NULL AND home_team_id != '' AND league_id IS NOT NULL AND league_id != ''
                UNION
                SELECT away_team_id AS team_id, league_id FROM {sched_table}
                WHERE away_team_id IS NOT NULL AND away_team_id != '' AND league_id IS NOT NULL AND league_id != ''
            ) GROUP BY team_id
        """)
        updated = conn.execute("""
            UPDATE teams SET league_ids = (
                SELECT lids FROM _tmp_team_leagues WHERE _tmp_team_leagues.team_id = teams.team_id
            ) WHERE team_id IN (SELECT team_id FROM _tmp_team_leagues)
        """).rowcount
        conn.execute("DROP TABLE IF EXISTS _tmp_team_leagues")
        conn.commit()
        
        total = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
        filled = conn.execute("SELECT COUNT(*) FROM teams WHERE league_ids IS NOT NULL AND league_ids != '' AND league_ids != '[]' AND league_ids != '[null]'").fetchone()[0]
        print(f"  [OK] league_ids: {filled}/{total} teams populated")

    # 4. Drop country column
    try:
        ver = sqlite3.sqlite_version_info
        if ver >= (3, 35, 0):
            conn.execute("ALTER TABLE teams DROP COLUMN country;")
            conn.commit()
            print("  [OK] Dropped country column from teams")
        else:
            print(f"  [SKIP] SQLite {sqlite3.sqlite_version} < 3.35.0, can't DROP COLUMN")
    except Exception as e:
        if 'no such column' in str(e):
            print("  [SKIP] country column already removed")
        else:
            print(f"  [WARN] Drop country: {e}")

    conn.close()
    return True


if __name__ == '__main__':
    print("LeoBook Unified Naming Migration\n")
    migrate_supabase()
    migrate_local_sqlite()
    print("\n=== MIGRATION COMPLETE ===")
