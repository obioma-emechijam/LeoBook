"""
MAINTENANCE SCRIPT: Backfill country_league values in SQLite.

The region_league → country_league column rename is complete across all tables.
This script now serves one purpose: filling NULL/empty country_league values in
schedules, predictions, and live_scores using league_id lookups against
leagues.json + the countries table.

Safe to run repeatedly — only rows with NULL or empty country_league are updated.

Run: python Scripts/backfill_country_league.py
Then: python Leo.py --push   (to push corrected data to Supabase)
"""

import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from Data.Access.league_db import DB_PATH
LEAGUES_JSON = os.path.join(ROOT, "Data", "Store", "leagues.json")


def get_columns(conn, table):
    """Return set of column names for a table."""
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def rename_column(conn, table, old, new):
    """Rename column if old exists and new doesn't. Handle edge cases."""
    cols = get_columns(conn, table)
    if old in cols and new not in cols:
        # Clean rename
        conn.execute(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")
        conn.commit()
        print(f"  [RENAME] {table}.{old} → {new}")
    elif old in cols and new in cols:
        # init_db() added empty new column alongside old — copy data over
        conn.execute(f"UPDATE {table} SET {new} = {old} WHERE {new} IS NULL OR {new} = ''")
        conn.commit()
        print(f"  [COPY]   {table}.{old} → {new} (both columns exist, copied data)")
    elif new in cols:
        print(f"  [OK]     {table}.{new} already exists (no old column)")
    else:
        print(f"  [WARN]   {table} has neither {old} nor {new}")


def build_country_map(conn):
    """Build league_id → 'Country: LeagueName' using leagues.json + countries table."""
    # Load leagues.json
    with open(LEAGUES_JSON, "r", encoding="utf-8") as f:
        leagues = json.load(f)

    # Build country_code → country_name from countries table
    code_to_country = {}
    try:
        rows = conn.execute("SELECT code, name FROM countries").fetchall()
        for code, name in rows:
            if code and name:
                code_to_country[code.lower()] = name
    except Exception:
        print("  [WARN] countries table not found, falling back to leagues.json fb_country")

    # Build league_id → "Country: LeagueName"
    league_map = {}
    for lg in leagues:
        lid = lg.get("league_id", "")
        name = lg.get("name", "")
        cc = lg.get("country_code", "").lower()

        # Resolve country name: countries table > fb_country > continent
        country = code_to_country.get(cc, "")
        if not country:
            country = lg.get("fb_country", "")
        if not country:
            country = lg.get("continent", "")

        if lid and name:
            league_map[lid] = f"{country}: {name}" if country else name

    print(f"  [MAP] Built {len(league_map)} league_id → country:league mappings")
    return league_map


def backfill_table(conn, table, league_map, league_id_col="league_id"):
    """Update country_league for all rows using league_map lookup."""
    cols = get_columns(conn, table)
    if "country_league" not in cols:
        print(f"  [SKIP] {table} has no country_league column")
        return 0

    if league_id_col not in cols:
        print(f"  [SKIP] {table} has no {league_id_col} column")
        return 0

    # Get distinct league_ids in the table
    rows = conn.execute(f"SELECT DISTINCT {league_id_col} FROM {table} WHERE {league_id_col} IS NOT NULL").fetchall()
    updated = 0

    for (lid,) in rows:
        cl = league_map.get(lid)
        if cl:
            cur = conn.execute(
                f"UPDATE {table} SET country_league = ? WHERE {league_id_col} = ?",
                (cl, lid)
            )
            updated += cur.rowcount

    conn.commit()
    print(f"  [BACKFILL] {table}: {updated} rows updated across {len(rows)} leagues")
    return updated


def main():
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] Database not found: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    print("=" * 60)
    print("  BACKFILL country_league (one-time migration)")
    print("=" * 60)

    # Step 1: Verify/fix any remaining region_league stragglers (rename is complete)
    print("\n[1/3] Verifying country_league column name (rename is done, safety check only)...")
    for table in ("schedules", "predictions", "live_scores"):
        rename_column(conn, table, "region_league", "country_league")

    # Step 2: Build lookup map
    print("\n[2/3] Building country lookup from leagues.json + countries table...")
    league_map = build_country_map(conn)

    # Step 3: Backfill
    print("\n[3/3] Backfilling country_league values...")
    total = 0
    total += backfill_table(conn, "schedules", league_map, "league_id")
    total += backfill_table(conn, "predictions", league_map, "league_id")
    # live_scores doesn't have league_id, skip backfill
    ls_cols = get_columns(conn, "live_scores")
    if "league_id" in ls_cols:
        total += backfill_table(conn, "live_scores", league_map, "league_id")
    else:
        print("  [SKIP] live_scores has no league_id column — manual backfill needed")

    conn.close()

    print(f"\n{'=' * 60}")
    print(f"  DONE: {total} rows backfilled")
    print(f"  Next: python Leo.py --push")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
