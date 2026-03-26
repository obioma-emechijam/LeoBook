# Flashscore Extractor Quality Analysis

**Source DOM:** [results_table.html](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/results_table.html), [fixtures_table.html](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/fixtures_table.html)
**Extractors:** [fs_league_extractor.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_extractor.py), [fs_league_tab.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_tab.py), [fs_league_images.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_images.py), [fs_league_enricher.py](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_enricher.py)

---

## DOM Data Model (Ground Truth)

Per-match row (`div.event__match[id^="g_1_"]`) contains:

| Data Point | DOM Element | Selector |
|---|---|---|
| **Match ID** | `div#g_1_{ID}` → ID = `g_1_` prefix stripped | `[id^='g_1_']` |
| **Match Link** | `a.eventRowLink[href=.../match/football/home-slug-HOMEID/away-slug-AWAYID/?mid=FIXID]` | `a.eventRowLink` |
| **Time** | `div.event__time` → `"22.03. 15:15"` | `.event__time` |
| **Home Team** | `div.event__homeParticipant span[data-testid="wcl-scores-simple-text-01"]` → `"Aston Villa"` | `.event__homeParticipant` |
| **Away Team** | `div.event__awayParticipant span[data-testid="wcl-scores-simple-text-01"]` → `"West Ham"` | `.event__awayParticipant` |
| **Home Logo** | `img[data-testid="wcl-participantLogo"]` inside home participant | `img.wcl-logo_UrSpU` |
| **Away Logo** | `img[data-testid="wcl-participantLogo"]` inside away participant | `img.wcl-logo_UrSpU` |
| **Home Score** | `span.event__score--home[data-testid="wcl-tableScore"]` → `"2"` or `"-"` | `.event__score--home` |
| **Away Score** | `span.event__score--away[data-testid="wcl-tableScore"]` → `"0"` or `"-"` | `.event__score--away` |
| **Round** | `div.event__round.event__round--static` → `"Round 31"` (precedes match rows) | `.event__round` |
| **Red Cards** | `svg[data-testid="wcl-icon-incidents-red-card"]` inside participant div | `[data-testid="wcl-icon-incidents-red-card"]` |
| **TV Icon** | `a.event__icon--tv` containing `svg[data-testid="wcl-icon-incidents-tv"]` | `.event__icon--tv` |
| **Bold Winner** | Winning team name span has `wcl-bold_NZXv6` class; losing team's span lacks it | `wcl-bold_NZXv6` on name span |
| **Scheduled** | Fixture rows have extra class `event__match--scheduled` | `.event__match--scheduled` |
| **Season** | `div.heading__info` → `"2025/2026"` | `.heading__info` |
| **League Crest** | `img.heading__logo` → `src` URL | `.heading__logo` |

---

## Findings

### FINDING 1 — OK: Core extraction is solid ✅

`EXTRACT_MATCHES_JS` ([fs_league_extractor.py:L18-160](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_extractor.py#L18-L160)) correctly extracts:

- ✅ Match ID from [id](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/FootballCom/extractor.py#385-411) attribute
- ✅ Home/away names from participant containers
- ✅ Scores from `wcl-tableScore` spans (correctly handles `"-"` → `null`)
- ✅ Round labels from preceding `event__round` divs
- ✅ Match link parsing → `home_team_id`, `away_team_id`, team URLs
- ✅ Team crest URLs from `wcl-participantLogo` images
- ✅ Date/time parsing with 3 format patterns + fallbacks
- ✅ DOM order swap detection via URL canonical (Root Cause 1 Fix)

**No action needed** — the core loop is well-matched to the DOM.

---

### FINDING 2 — LOW: Red card data completely ignored

**DOM Evidence** ([results_table.html:L373-379](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/results_table.html#L373-L379)):
```html
<svg data-testid="wcl-icon-incidents-red-card" class="wcl-icon_WGKvC wcl-card_N34-h" ...>
    <path fill="#dc0000" ...></path>
</svg>
```
This appears inside participant divs for Burnley (L373), Manchester Utd (L455), Leeds (L576).

**Code:** `EXTRACT_MATCHES_JS` never queries `[data-testid="wcl-icon-incidents-red-card"]`. The `fixture_rows` dict in [fs_league_tab.py:L159-179](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_tab.py#L159-L179) has no `red_cards` field.

**Impact:** Red card indicators per team are free data in every result row — useful for match analysis/predictions. Currently lost silently.

**Severity:** LOW — nice-to-have enrichment data, not blocking any flow.

---

### FINDING 3 — LOW: Winner indicator not extracted from DOM class

**DOM Evidence** ([results_table.html:L169](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/results_table.html#L169)):
```html
<!-- Winning team has wcl-bold_NZXv6 on name span: -->
<span class="wcl-bold_NZXv6 wcl-scores-simple-text-01_-OvnR ...">Aston Villa</span>
<!-- Losing team lacks it: -->
<span class="wcl-scores-simple-text-01_-OvnR ...">West Ham</span>
```

**Code:** `EXTRACT_MATCHES_JS` extracts scores and infers FT status, but never checks `wcl-bold_NZXv6` class on name spans to detect the winning side. The enricher relies on score comparison downstream.

**Impact:** Score-based winner inference works, but the bold class is the definitive server-side winner marker — would catch edge cases where scores alone don't indicate winner (walkovers, abandoned matches with awarded results).

**Severity:** LOW — score-based inference is adequate for 99%+ of cases.

---

### FINDING 4 — MEDIUM: `event__match--scheduled` class not captured

**DOM Evidence** ([fixtures_table.html:L155](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/fixtures_table.html#L155)):
```html
<div id="g_1_t4sdnVsR" class="event__match event__match--withRowLink event__match--twoLine event__match--scheduled" ...>
```
Fixture rows have `event__match--scheduled`, result rows don't.

**Code:** `EXTRACT_MATCHES_JS` never reads the `event__match--scheduled` class. Status is inferred via score content (`"-"` → scheduled) and the [tab](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_tab.py#46-216) parameter. In [fs_league_tab.py:L143-150](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_tab.py#L143-L150), status fallback logic uses date comparison + tab name.

**Impact:** The existing fallback works, but reading the `--scheduled` class directly would be simpler and more robust — especially for edge cases where a fixture has no score AND no date (would currently fall through to `"scheduled" if tab == "fixtures" else "finished"`).

**Severity:** MEDIUM — the existing workaround functions, but it's fragile.

---

### FINDING 5 — OK: TV broadcast indicator ignored (non-issue) ✅

**DOM Evidence** ([fixtures_table.html:L191-199](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/docs/fixtures_table.html#L191-L199)):
```html
<a class="event__icon event__icon--tv" href="..."><svg data-testid="wcl-icon-incidents-tv"...></svg></a>
```

Present on fixture rows with TV coverage. Not extracted by any code. This is truly optional metadata with no downstream consumer, so this is **not a failure**.

---

### FINDING 6 — OK: Image pipeline is comprehensive ✅

The flow [fs_league_tab.py:L122-127](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_tab.py#L122-L127) correctly:
- Reads `home_crest_url` / `away_crest_url` from JS output
- Schedules parallel downloads via [schedule_image_download](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_images.py#54-56)
- Uploads to Supabase via [upload_crest_to_supabase](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_images.py#82-103)
- Backfills schedule rows via [_backfill_schedule_crests](file:///c:/Users/Admin/Desktop/ProProjection/LeoBook/Modules/Flashscore/fs_league_extractor.py#500-557)

Crest URLs in the DOM (`img[data-testid="wcl-participantLogo"]`) are correctly captured in JS via `s.match_logo_home` / `s.match_logo_away` selectors.

---

## Verdict

> **The extractors are doing a good job.** The core data pipeline (matches, teams, scores, dates, rounds, crests, team IDs) is thorough and well-matched to the DOM.

Two minor enrichment opportunities exist (red cards, scheduled class) — neither is blocking any critical flow. The biggest past issues (archive extraction, winner data, scroll logic) have already been fixed in this session.

### Priority Matrix

| # | Finding | Severity | Action Needed? |
|---|---------|----------|----------------|
| 1 | Core extraction solid | ✅ OK | No |
| 2 | Red cards ignored | LOW | Optional |
| 3 | Winner bold class not read | LOW | Optional |
| 4 | `--scheduled` class unused | MEDIUM | Recommended |
| 5 | TV indicator ignored | ✅ OK | No |
| 6 | Image pipeline solid | ✅ OK | No |
