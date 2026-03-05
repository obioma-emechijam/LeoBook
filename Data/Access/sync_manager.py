# sync_manager.py: Bi-directional sync between local SQLite and Supabase.
# Part of LeoBook Data — Access Layer
#
# Classes: SyncManager
# Functions: run_full_sync()

import logging
import re
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from Data.Access.supabase_client import get_supabase_client
from Data.Access.league_db import get_connection, init_db, query_all
from Core.Intelligence.aigo_suite import AIGOSuite


logger = logging.getLogger(__name__)

# SQLite table -> Supabase table mapping
# local_table: SQLite table name
# remote_table: Supabase table name
# key: primary key / conflict field
TABLE_CONFIG = {
    'predictions':      {'local_table': 'predictions',      'remote_table': 'predictions',      'key': 'fixture_id'},
    'schedules':        {'local_table': 'schedules',        'remote_table': 'schedules',        'key': 'fixture_id'},
    'teams':            {'local_table': 'teams',            'remote_table': 'teams',            'key': 'team_id'},
    'leagues':          {'local_table': 'leagues',          'remote_table': 'leagues',          'key': 'league_id'},
    'fb_matches':       {'local_table': 'fb_matches',       'remote_table': 'fb_matches',       'key': 'site_match_id'},
    'profiles':         {'local_table': 'profiles',         'remote_table': 'profiles',         'key': 'id'},
    'custom_rules':     {'local_table': 'custom_rules',     'remote_table': 'custom_rules',     'key': 'id'},
    'rule_executions':  {'local_table': 'rule_executions',  'remote_table': 'rule_executions',  'key': 'id'},
    'accuracy_reports': {'local_table': 'accuracy_reports', 'remote_table': 'accuracy_reports', 'key': 'report_id'},
    'audit_log':        {'local_table': 'audit_log',        'remote_table': 'audit_log',        'key': 'id'},
    'live_scores':      {'local_table': 'live_scores',      'remote_table': 'live_scores',      'key': 'fixture_id'},
    'countries':        {'local_table': 'countries',        'remote_table': 'countries',        'key': 'code'},
}

# ── Supabase auto-provisioning DDL ─────────────────────────────────────────
# Postgres CREATE TABLE statements for each remote table.
# Used by _ensure_remote_table() when PGRST205 (table not found) is detected.
SUPABASE_SCHEMA = {
    'predictions': """
        CREATE TABLE IF NOT EXISTS public.predictions (
            fixture_id TEXT PRIMARY KEY,
            date TEXT, match_time TEXT, region_league TEXT,
            home_team TEXT, away_team TEXT, home_team_id TEXT, away_team_id TEXT,
            prediction TEXT, confidence TEXT, reason TEXT,
            xg_home REAL, xg_away REAL, btts TEXT, over_2_5 TEXT,
            best_score TEXT, top_scores TEXT,
            home_form_n INTEGER, away_form_n INTEGER,
            home_tags TEXT, away_tags TEXT, h2h_tags TEXT, standings_tags TEXT,
            h2h_count INTEGER, actual_score TEXT, outcome_correct TEXT,
            status TEXT DEFAULT 'pending', match_link TEXT, odds TEXT,
            market_reliability_score REAL, home_crest_url TEXT, away_crest_url TEXT,
            recommendation_score REAL, h2h_fixture_ids JSONB, form_fixture_ids JSONB,
            standings_snapshot JSONB, league_stage TEXT, generated_at TEXT,
            home_score TEXT, away_score TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'schedules': """
        CREATE TABLE IF NOT EXISTS public.schedules (
            fixture_id TEXT PRIMARY KEY,
            date TEXT, match_time TEXT, league_id TEXT,
            home_team_id TEXT, home_team TEXT, away_team_id TEXT, away_team TEXT,
            home_score INTEGER, away_score INTEGER, extra JSONB,
            league_stage TEXT, match_status TEXT, season TEXT,
            home_crest TEXT, away_crest TEXT, match_link TEXT,
            region_league TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'teams': """
        CREATE TABLE IF NOT EXISTS public.teams (
            team_id TEXT PRIMARY KEY,
            name TEXT NOT NULL, league_ids JSONB, crest TEXT,
            country_code TEXT, url TEXT,
            city TEXT, stadium TEXT,
            other_names TEXT, abbreviations TEXT, search_terms TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'leagues': """
        CREATE TABLE IF NOT EXISTS public.leagues (
            league_id TEXT PRIMARY KEY,
            fs_league_id TEXT, country_code TEXT, continent TEXT,
            name TEXT NOT NULL, crest TEXT, current_season TEXT,
            url TEXT, region_flag TEXT,
            other_names TEXT, abbreviations TEXT, search_terms TEXT,
            date_updated TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'audit_log': """
        CREATE TABLE IF NOT EXISTS public.audit_log (
            id TEXT PRIMARY KEY,
            timestamp TEXT, event_type TEXT, description TEXT,
            balance_before REAL, balance_after REAL, stake REAL, status TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'fb_matches': """
        CREATE TABLE IF NOT EXISTS public.fb_matches (
            site_match_id TEXT PRIMARY KEY,
            date TEXT, time TEXT, home_team TEXT, away_team TEXT,
            league TEXT, url TEXT, last_extracted TEXT, fixture_id TEXT,
            matched TEXT, odds TEXT, booking_status TEXT, booking_details TEXT,
            booking_code TEXT, booking_url TEXT, status TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'live_scores': """
        CREATE TABLE IF NOT EXISTS public.live_scores (
            fixture_id TEXT PRIMARY KEY,
            home_team TEXT, away_team TEXT,
            home_score TEXT, away_score TEXT, minute TEXT,
            status TEXT, region_league TEXT, match_link TEXT, timestamp TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'accuracy_reports': """
        CREATE TABLE IF NOT EXISTS public.accuracy_reports (
            report_id TEXT PRIMARY KEY,
            timestamp TEXT, volume INTEGER, win_rate REAL,
            return_pct REAL, period TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'countries': """
        CREATE TABLE IF NOT EXISTS public.countries (
            code TEXT PRIMARY KEY,
            name TEXT, continent TEXT, capital TEXT,
            flag_1x1 TEXT, flag_4x3 TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'profiles': """
        CREATE TABLE IF NOT EXISTS public.profiles (
            id TEXT PRIMARY KEY,
            email TEXT, username TEXT, full_name TEXT,
            avatar_url TEXT, tier TEXT, credits REAL,
            created_at TEXT, updated_at TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'custom_rules': """
        CREATE TABLE IF NOT EXISTS public.custom_rules (
            id TEXT PRIMARY KEY,
            user_id TEXT, name TEXT, description TEXT,
            is_active INTEGER, logic TEXT, priority INTEGER,
            created_at TEXT, updated_at TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
    'rule_executions': """
        CREATE TABLE IF NOT EXISTS public.rule_executions (
            id TEXT PRIMARY KEY,
            rule_id TEXT, fixture_id TEXT, user_id TEXT,
            result TEXT, executed_at TEXT,
            last_updated TIMESTAMPTZ DEFAULT now()
        );""",
}

# No column renames needed — unified naming across local SQLite and Supabase.
# Columns that differ only structurally (e.g. `time` vs `match_time`) are
# handled by _COL_REMAP below.

# ── Derived: allowed columns per remote table (parsed from SUPABASE_SCHEMA DDL) ──
# Only columns in this set will be pushed to Supabase. Everything else is stripped.
_ALLOWED_COLS = {}
for _tbl, _ddl in SUPABASE_SCHEMA.items():
    import re as _re
    # Extract column names from CREATE TABLE DDL
    _cols = set(_re.findall(r'(?:^|\s)(\w+)\s+(?:TEXT|INTEGER|REAL|JSONB|TIMESTAMPTZ|BOOLEAN)', _ddl))
    _cols.discard('TABLE')
    _cols.discard('NOT')
    _cols.discard('IF')
    _cols.discard('EXISTS')
    _cols.discard('DEFAULT')
    _ALLOWED_COLS[_tbl] = _cols

# Column remaps: local name → remote name (applied before schema filtering)
_COL_REMAP = {
    'time': 'match_time',  # schedules local uses 'time', Supabase uses 'match_time'
}

class SyncManager:
    """Manages bi-directional sync between local SQLite and Supabase."""

    def __init__(self):
        self.supabase = get_supabase_client()
        self.conn = init_db()
        self._created_tables = set()  # Track auto-created tables this session
        self._ensure_watermark_table()
        if not self.supabase:
            logger.warning("[!] SyncManager initialized without Supabase connection. Sync disabled.")

    def _ensure_watermark_table(self):
        """Create the watermark table for tracking last sync timestamps."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS _sync_watermarks (
                table_name TEXT PRIMARY KEY,
                last_sync TEXT NOT NULL DEFAULT '1970-01-01T00:00:00'
            )
        """)
        self.conn.commit()

    def _get_watermark(self, table_name: str) -> str:
        """Get the last sync timestamp for a table."""
        row = self.conn.execute(
            "SELECT last_sync FROM _sync_watermarks WHERE table_name = ?", (table_name,)
        ).fetchone()
        return row[0] if row else '1970-01-01T00:00:00'

    def _set_watermark(self, table_name: str, timestamp: str):
        """Update the last sync timestamp for a table."""
        self.conn.execute(
            "INSERT INTO _sync_watermarks (table_name, last_sync) VALUES (?, ?) "
            "ON CONFLICT(table_name) DO UPDATE SET last_sync = excluded.last_sync",
            (table_name, timestamp)
        )
        self.conn.commit()

    def _ensure_remote_table(self, remote_table: str) -> bool:
        """Auto-create a Supabase table if it's missing. Returns True if created.
        Uses the exec_sql() RPC function deployed on Supabase.
        """
        if remote_table in self._created_tables:
            return True

        ddl = SUPABASE_SCHEMA.get(remote_table)
        if not ddl:
            logger.warning(f"    [!] No DDL schema for table '{remote_table}'. Cannot auto-create.")
            return False

        try:
            self.supabase.rpc('exec_sql', {'query': ddl.strip()}).execute()
        except Exception as rpc_err:
            logger.warning(f"    [!] exec_sql RPC failed for '{remote_table}': {rpc_err}")
            return False

        # Verify creation by attempting a simple select
        try:
            self.supabase.table(remote_table).select('*').limit(0).execute()
            self._created_tables.add(remote_table)
            logger.info(f"    [+] Auto-created table '{remote_table}' on Supabase.")
            print(f"    [+] Auto-created table '{remote_table}' on Supabase.")
            return True
        except Exception:
            logger.warning(f"    [!] Table '{remote_table}' still missing after auto-create attempt.")
            return False

    async def sync_on_startup(self):
        """Push local changes to Supabase for all configured tables.
        If local table is empty, bootstrap by pulling from Supabase (one-time)."""
        if not self.supabase:
            return

        logger.info("Starting push-only sync on startup...")
        print("   [SYNC] Push-Only Sync — local SQLite → Supabase...")

        for table_key, config in TABLE_CONFIG.items():
            await self._sync_table(table_key, config)

    async def _sync_table(self, table_key: str, config: Dict):
        """Push-only sync: push local changes to Supabase since last watermark.
        If local table is empty, bootstrap by pulling from Supabase (one-time).
        ZERO reads from Supabase during normal operation."""
        local_table = config['local_table']
        remote_table = config['remote_table']
        key_field = config['key']

        logger.info(f"  Syncing {local_table} → {remote_table}...")

        # 1. Check if local table is empty → bootstrap from Supabase
        try:
            local_count = self.conn.execute(f"SELECT COUNT(*) FROM {local_table}").fetchone()[0]
        except Exception:
            local_count = 0

        if local_count == 0:
            print(f"   [{remote_table}] Empty local — bootstrapping from Supabase...")
            pulled = await self._bootstrap_from_remote(local_table, remote_table, key_field)
            if pulled > 0:
                self._set_watermark(remote_table, datetime.utcnow().isoformat())
                print(f"   [{remote_table}] ✓ Bootstrapped {pulled} rows from Supabase")
            else:
                print(f"   [{remote_table}] ✓ Both local and remote empty")
            return

        # 2. Push-only: query local rows modified since watermark
        watermark = self._get_watermark(remote_table)
        is_first_sync = watermark == '1970-01-01T00:00:00'

        try:
            if is_first_sync:
                # First sync ever: push ALL local rows
                local_rows = query_all(self.conn, local_table)
                if not local_rows:
                    local_rows = []
            else:
                local_rows = self.conn.execute(
                    f"SELECT * FROM {local_table} WHERE last_updated > ? OR last_updated IS NULL",
                    (watermark,)
                ).fetchall()
                local_rows = [dict(r) for r in local_rows]
        except Exception as e:
            logger.error(f"    [x] Failed to query local {local_table}: {e}")
            return

        if not local_rows:
            print(f"   [{remote_table}] ✓ Nothing to push")
            return

        print(f"   [{remote_table}] Pushing {len(local_rows)} rows to Supabase...")

        # 3. Push to Supabase
        await self.batch_upsert(table_key, local_rows)

        # 4. Verify a sample
        push_ids = [str(r.get(key_field, '')) for r in local_rows if r.get(key_field)]
        if push_ids:
            await self._verify_sync_parity(table_key, push_ids)

        # 5. Update watermark to the latest local timestamp
        max_ts = None
        for r in local_rows:
            ts = r.get('last_updated', '')
            if ts and ts not in ('None', 'nan', ''):
                try:
                    norm = pd.to_datetime(ts, utc=True).strftime('%Y-%m-%dT%H:%M:%S')
                    if max_ts is None or norm > max_ts:
                        max_ts = norm
                except Exception:
                    pass
        if max_ts:
            self._set_watermark(remote_table, max_ts)

    async def _bootstrap_from_remote(self, local_table: str, remote_table: str,
                                      key_field: str) -> int:
        """Pull ALL rows from Supabase into empty local SQLite. One-time bootstrap."""
        total_pulled = 0
        batch_size = 1000
        offset = 0

        while True:
            try:
                res = self.supabase.table(remote_table).select("*").order(
                    key_field, desc=False
                ).range(offset, offset + batch_size - 1).execute()
                rows = res.data
                if not rows:
                    break

                # Insert into local SQLite
                table_cols = [c[1] for c in self.conn.execute(
                    f"PRAGMA table_info({local_table})"
                ).fetchall()]

                for row in rows:
                    # Handle over_2.5 → over_2_5
                    if 'over_2.5' in row:
                        row['over_2_5'] = row.pop('over_2.5')

                    filtered = {k: v for k, v in row.items()
                                if k in table_cols and v is not None}
                    if not filtered or key_field not in filtered:
                        continue

                    cols = list(filtered.keys())
                    placeholders = ", ".join([f":{c}" for c in cols])
                    col_str = ", ".join(cols)
                    updates = ", ".join([f"{c} = excluded.{c}" for c in cols if c != key_field])

                    try:
                        self.conn.execute(
                            f"INSERT INTO {local_table} ({col_str}) VALUES ({placeholders}) "
                            f"ON CONFLICT({key_field}) DO UPDATE SET {updates}",
                            filtered,
                        )
                    except Exception as e:
                        logger.warning(f"      [Bootstrap] Row insert failed: {e}")

                self.conn.commit()
                total_pulled += len(rows)

                if len(rows) < batch_size:
                    break
                offset += batch_size

            except Exception as e:
                err_str = str(e)
                if 'PGRST205' in err_str or 'Could not find the table' in err_str:
                    logger.info(f"      [AUTO] Table '{remote_table}' not found — creating...")
                    if self._ensure_remote_table(remote_table):
                        continue
                    else:
                        break
                else:
                    logger.error(f"      [Bootstrap] Pull failed at offset {offset}: {e}")
                    break

        if total_pulled > 0:
            logger.info(f"    [BOOTSTRAP] Pulled {total_pulled} rows into {local_table}.")
        return total_pulled

    # _pull_updates removed — replaced by _bootstrap_from_remote above.
    # Push-only sync: Supabase is a read-only mirror, no per-ID pulls needed.

    async def batch_upsert(self, table_key: str, data: List[Dict[str, Any]]):
        """Upsert a batch of data to Supabase with strict cleaning."""
        if not self.supabase or not data:
            return

        conf = TABLE_CONFIG.get(table_key)
        if not conf:
            return

        local_table = conf['local_table']
        remote_table = conf['remote_table']
        conflict_key = conf['key']
        allowed = _ALLOWED_COLS.get(remote_table, set())

        cleaned_data = []
        for row in data:
            clean = {}
            for k, v in row.items():
                # Apply column remaps first (e.g. time → match_time)
                out_key = _COL_REMAP.get(k, k)

                # Skip columns not in the remote Supabase schema
                if allowed and out_key not in allowed:
                    continue

                if v in ('', 'N/A', None, 'None', 'none', 'nan', 'NaN', 'null', 'NULL'):
                    clean[out_key] = None
                elif isinstance(v, str) and re.match(r"^\[.*\]$", v.strip()):
                    clean[out_key] = None
                else:
                    val = v
                    # CSV date format (DD.MM.YYYY) -> DB (YYYY-MM-DD)
                    if out_key in ['date', 'date_updated', 'last_extracted'] and isinstance(val, str):
                        match_full = re.match(r'^(\d{2})\.(\d{2})\.(\d{4})$', val)
                        if match_full:
                            d, m, y = match_full.groups()
                            val = f"{y}-{m}-{d}"
                        elif not re.match(r'^\d{4}-\d{2}-\d{2}', val):
                            val = None

                    # Sanitize integer score columns: "-" → None
                    if out_key in ('home_score', 'away_score'):
                        if val in ('-', '--', '?'):
                            val = None
                        elif val is not None:
                            try:
                                val = int(val)
                            except (ValueError, TypeError):
                                val = None

                    if out_key == 'over_2.5' or out_key == 'over_2_5':
                        clean['over_2_5'] = val
                    else:
                        clean[out_key] = val

            # Timestamp normalization
            now_iso = datetime.utcnow().isoformat()
            for ts in ['last_updated', 'date_updated', 'last_extracted', 'created_at']:
                if ts in clean:
                    if not clean[ts] or not re.match(r'^\d{4}-\d{2}-\d{2}', str(clean[ts])):
                        clean[ts] = now_iso
            if 'last_updated' not in clean:
                clean['last_updated'] = now_iso

            # Remove auto-increment id if present (Supabase handles this)
            if 'id' in clean and (not clean['id'] or str(clean['id']).isdigit()):
                del clean['id']

            cleaned_data.append(clean)

        # Deduplicate
        keys = [k.strip() for k in conflict_key.split(',')]
        seen = set()
        deduped = []
        for row in cleaned_data:
            if all(row.get(k) not in (None, '') for k in keys):
                kv = tuple(row.get(k) for k in keys)
                if kv not in seen:
                    seen.add(kv)
                    deduped.append(row)

        if not deduped:
            return

        try:
            api_batch_size = 1000
            pbar = tqdm(total=len(deduped), desc=f"    Pushing {remote_table}", unit="row")
            for i in range(0, len(deduped), api_batch_size):
                batch = deduped[i:i + api_batch_size]
                try:
                    self.supabase.table(remote_table).upsert(batch, on_conflict=conflict_key).execute()
                except Exception as batch_err:
                    err_str = str(batch_err)
                    if 'PGRST205' in err_str or 'Could not find the table' in err_str:
                        logger.info(f"    [AUTO] Table '{remote_table}' missing during upsert — auto-creating...")
                        if self._ensure_remote_table(remote_table):
                            # Retry this batch
                            self.supabase.table(remote_table).upsert(batch, on_conflict=conflict_key).execute()
                        else:
                            raise batch_err
                    else:
                        raise batch_err
                pbar.update(len(batch))
            pbar.close()
            logger.info(f"    [SYNC] Upserted {len(deduped)} rows to {remote_table}.")
        except Exception as e:
            pbar.close()
            print(f"    [x] Upsert failed for {remote_table}: {e}")
            logger.error(f"    [x] Upsert failed: {e}")

    async def _verify_sync_parity(self, table_key: str, pushed_ids: List[str], sample_size: int = 10):
        """Pick a sample and verify parity between local SQLite and remote Supabase."""
        if not pushed_ids:
            return

        conf = TABLE_CONFIG[table_key]
        local_table = conf['local_table']
        remote_table = conf['remote_table']
        key_field = conf['key']

        sample_ids = pushed_ids[:sample_size] if len(pushed_ids) <= sample_size else np.random.choice(pushed_ids, sample_size, replace=False).tolist()

        logger.info(f"    Verifying parity for {len(sample_ids)} sample rows...")

        try:
            # Fetch remote sample
            res = self.supabase.table(remote_table).select("*").in_(key_field, sample_ids).execute()
            remote_rows = {str(r[key_field]): r for r in res.data}

            # Fetch local sample from SQLite
            placeholders = ",".join(["?"] * len(sample_ids))
            local_data = self.conn.execute(
                f"SELECT * FROM {local_table} WHERE {key_field} IN ({placeholders})",
                sample_ids,
            ).fetchall()
            local_rows = {str(dict(r)[key_field]): dict(r) for r in local_data}

            mismatches = 0
            for uid in sample_ids:
                l_row = local_rows.get(uid)
                r_row = remote_rows.get(uid)

                if not r_row:
                    logger.warning(f"      [Parity Fail] ID {uid} missing from remote!")
                    mismatches += 1
                    continue

                l_ts = (l_row or {}).get('last_updated', '')
                r_ts = r_row.get('last_updated', '')

                try:
                    dt_l = datetime.fromisoformat(l_ts.replace('Z', '+00:00')) if l_ts else None
                    dt_r = datetime.fromisoformat(r_ts.replace('Z', '+00:00')) if r_ts else None
                    if dt_l and dt_r:
                        if dt_r < dt_l and abs((dt_l - dt_r).total_seconds()) > 1:
                            logger.warning(f"      [Parity Warning] ID {uid} timestamp mismatch!")
                            mismatches += 1
                except (ValueError, TypeError):
                    if r_ts < l_ts and r_ts[:19] != l_ts[:19]:
                        mismatches += 1

            if mismatches > 0:
                logger.error(f"    [PARITY ERROR] {mismatches} mismatches in {remote_table}.")
            else:
                logger.info(f"    [PARITY OK] {remote_table} sample verified.")

        except Exception as e:
            logger.error(f"    [x] Parity verification failed: {e}")


@AIGOSuite.aigo_retry(max_retries=3, delay=2.0, use_aigo=False)
async def run_full_sync(session_name: str = "Periodic"):
    """Wrapper to sync ALL tables with audit logging and AIGO protection."""
    from Data.Access.db_helpers import log_audit_event
    logger.info(f"Starting global full sync [{session_name}]...")

    manager = SyncManager()

    success_count = 0
    fail_count = 0
    errors = []

    for table_key, config in TABLE_CONFIG.items():
        try:
            await manager._sync_table(table_key, config)
            success_count += 1
        except Exception as e:
            logger.error(f"    [Sync Fatal] {table_key}: {e}")
            fail_count += 1
            errors.append(f"{table_key}: {str(e)}")

    status = "success" if fail_count == 0 else "partial_failure" if success_count > 0 else "failed"
    msg = f"Full Chapter Sync ({session_name}): {success_count} passed, {fail_count} failed."
    if errors:
        msg += f" Errors: {'; '.join(errors[:3])}"

    try:
        log_audit_event(event_type="SYSTEM_SYNC", description=msg, status=status)
    except Exception as e:
        logger.error(f"Failed to log audit event for sync: {e}")

    if fail_count > 0:
        print(f"\n[!] Sync Warning: {fail_count} tables failed. AIGO fallback may be required.")
        return False

    return True
