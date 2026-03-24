# SUPABASE_SETUP.md — LeoBook Database Architecture
# Auto-generated 2026-03-24 from live code audit of supabase_schema.sql v6.0

# LeoBook Supabase Setup Guide

**Schema version:** v6.0 (2026-03-24)  
**Source of truth:** [`Data/Supabase/supabase_schema.sql`](file:///C:/Users/Admin/Desktop/ProProjection/LeoBook/Data/Supabase/supabase_schema.sql)  
**SQLite schema:** [`Data/Access/league_db_schema.py`](file:///C:/Users/Admin/Desktop/ProProjection/LeoBook/Data/Access/league_db_schema.py)

---

## Architecture Overview

```
Flashscore / Football.com scrapers
        ↓
   SQLite (leobook.db)          ← primary local store; all writes go here first
   league_db.py / db_helpers.py
        ↓
   sync_schema.py               ← pushes SQLite → Supabase (upsert, conflict-safe)
        ↓
   Supabase (PostgreSQL)        ← read by Flutter app via Supabase client
        ↓
   Flutter leobookapp           ← end user
```

**There are no CSV files in the pipeline.** All data enters through `league_db.py` `upsert_*()` functions. The old CSV ingestion layer was removed in v7.0.

---

## Table Reference

### Tables synced FROM SQLite → Supabase

| Table | Primary Key | SQLite function | Notes |
|-------|------------|-----------------|-------|
| `schedules` | `fixture_id` | `upsert_fixture` | Has `country_league TEXT` column |
| `predictions` | `fixture_id` | `upsert_prediction` | Has `country_league TEXT` column |
| `live_scores` | `fixture_id` | `upsert_live_score` | Has `country_league TEXT` column |
| `teams` | `team_id` | `upsert_team` | |
| `region_league` | `league_id` | `upsert_league` | Supabase table name is `region_league`; SQLite table is `leagues` |
| `fb_matches` | `site_match_id` | `upsert_fb_match` | Only 9 core columns synced (v5.0 — booking/odds columns removed from Supabase) |
| `match_odds` | `(fixture_id, market_id, exact_outcome, line)` | `upsert_match_odds_batch` | |
| `audit_log` | `id` | `log_audit_event` | |
| `accuracy_reports` | `report_id` | `upsert_accuracy_report` | |
| `countries` | `code` | `upsert_country` | |

### Tables that are Supabase-only (NOT synced from SQLite)

| Table | Notes |
|-------|-------|
| `profiles` | Managed by Supabase Auth trigger (`handle_new_user`) |
| `custom_rules` | User-owned rules; written by Flutter app |
| `rule_executions` | Written by Flutter app |
| `learning_weights` | Written by `AdaptiveRecommender`; PK column: `country_league` (renamed from `region_league` in v6.0) |

### Computed Views (no sync, no storage)

| View | Source | Notes |
|------|--------|-------|
| `computed_standings` | `schedules` | Real-time standings computed from finished match scores. Queried by Flutter. |

---

## Column Naming Rules

| Concept | SQLite column | Supabase column | Notes |
|---------|--------------|-----------------|-------|
| League display label | `country_league` | `country_league` | Present in schedules, predictions, live_scores, standings |
| League registry table name | `leagues` (SQLite) | `region_league` (Supabase) | Table names differ; column structure is the same |
| `learning_weights` PK | `country_league` | `country_league` | Renamed from `region_league` in v6.0 with idempotent migration |
| Decimal markets | `over_2_5` | `over_2_5` | Dot illegal in PostgreSQL; stored with underscore |

---

## Key Column Lists (by table)

### `predictions` (37 columns)
```
fixture_id · date · match_time · country_league
home_team · away_team · home_team_id · away_team_id
prediction · confidence · reason
xg_home · xg_away · btts · over_2_5 · best_score · top_scores
home_form_n · away_form_n
home_tags · away_tags · h2h_tags · standings_tags · h2h_count
actual_score · outcome_correct · status
match_link · odds · market_reliability_score
home_crest_url · away_crest_url · recommendation_score
h2h_fixture_ids · form_fixture_ids · standings_snapshot · league_stage
home_score · away_score · last_updated
```
Extra columns added by Rule Engine Manager (on top of base):
`chosen_market · market_id · rule_explanation · override_reason · statistical_edge · pure_model_suggestion`

### `schedules` (15 columns)
```
fixture_id · date · match_time · country_league · league_id
home_team · away_team · home_team_id · away_team_id
home_score · away_score · match_status · match_link · league_stage · last_updated
```

### `fb_matches` (10 columns — v5.0 simplified)
```
site_match_id · league_id · date · match_time
home_team · away_team · url · fixture_id · matched · last_updated
```
> **Note:** `odds`, `booking_status`, `booking_details`, `booking_code`, `booking_url`, `last_extracted`, `league`, `status` were **removed from Supabase in v5.0**. They still exist in the local SQLite `fb_matches` table for diagnostics but are not written by `upsert_fb_match` and are not synced.

### `live_scores` (11 columns)
```
fixture_id · home_team · away_team · home_score · away_score
minute · status · country_league · match_link · timestamp · last_updated
```

### `standings` (15 columns)
```
standings_key · league_id · team_id · team_name
position · played · wins · draws · losses
goals_for · goals_against · goal_difference · points
country_league · last_updated
```

### `learning_weights` (5 columns)
```
country_league (PK) · weights · confidence_calibration
predictions_analyzed · last_updated
```

---

## Row Level Security (RLS)

All tables have RLS enabled. Default policy: **public read, no public write**.

| Table | Read | Write |
|-------|------|-------|
| All sync tables | `anon`, `authenticated` | Service role (sync) only |
| `profiles` | Own profile only (`auth.uid() = id`) | Own profile only |
| `custom_rules` | Own rules only | Own rules only |
| `rule_executions` | Own executions only | Own executions only |

---

## Applying the Schema

Run the full `supabase_schema.sql` in the Supabase SQL editor. It is fully idempotent:
- All `CREATE TABLE` statements use `IF NOT EXISTS`
- All `ALTER TABLE ADD COLUMN` use `IF NOT EXISTS`
- All `ALTER TABLE RENAME COLUMN` are wrapped in `DO $$ BEGIN ... EXCEPTION ... END $$` blocks
- All `DROP POLICY / CREATE POLICY` are safely paired

```sql
-- In Supabase SQL editor or via psql:
\i Data/Supabase/supabase_schema.sql
```

---

## sync_schema.py behaviour

`sync_schema.py` reads rows from SQLite and upserts them to Supabase using the supabase-py client. Key behaviours:

- Uses `upsert()` with `on_conflict` set to the primary key column
- Column remap: `over_2_5` (SQLite) → `over_2_5` (Supabase, unchanged — dot already stripped at write time)
- Skips `None`/`NaN` values to avoid overwriting with nulls
- Pushes in batches of 200 rows
- `fb_matches` sync only pushes the 9 canonical columns (booking columns excluded)

---

## Triggers

All tables have an auto-update trigger on `last_updated`:

```sql
BEFORE UPDATE → SET last_updated = NOW()
```

Tables covered: `profiles`, `custom_rules`, `predictions`, `schedules`, `teams`, `standings`, `fb_matches`, `live_scores`, `accuracy_reports`, `audit_log`, `learning_weights`, `match_odds`

---

## Indexes

```sql
-- Supabase
idx_schedules_league_date  ON schedules(league_id, date)
idx_schedules_date         ON schedules(date)

-- SQLite (league_db_schema.py)
idx_schedules_league       ON schedules(league_id)
idx_schedules_date         ON schedules(date)
idx_schedules_fixture_id   ON schedules(fixture_id)
idx_leagues_league_id      ON leagues(league_id)
idx_predictions_date       ON predictions(date)
idx_predictions_status     ON predictions(status)
idx_match_odds_fixture     ON match_odds(fixture_id)
idx_match_odds_market      ON match_odds(market_id, extracted_at)
idx_match_odds_site        ON match_odds(site_match_id)
```