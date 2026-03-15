# booking_harvester.py: Ch1 P3 — Booking code extraction for top recommendations.
# Part of LeoBook Modules — FootballCom Booking
#
# Functions: harvest_booking_codes_for_recommendations()
#
# Design: No-login session (same context as Ch1 P1 odds scraping).
# Receives top 20% of recommendations PER DATE from Ch1 P2/recommend_bets.
# For each recommended match-outcome:
#   1. Navigate to match page
#   2. Click the recommended outcome cell (market_outcome_clickable)
#   3. Click "Book Bet" (.m-book-btn)
#   4. Extract booking code from share page
#   5. Save to predictions table (booking_code, booking_odds, booking_url)
#   6. Close modal / clear slip — ready for next
#
# Called by: Core/System/pipeline.py → run_chapter_1_p3()

import asyncio
import math
import sqlite3
from datetime import datetime
from typing import List, Dict

from playwright.async_api import Page

from Core.Utils.constants import now_ng
from Data.Access.db_helpers import _get_conn


# ── Selector constants (from Config/knowledge.json fb_match_page) ──────────

# Outcome row cell — clickable button for each outcome
_SEL_OUTCOME_CELL = ".m-table-row > div.un-rounded-rem-\\[10px\\]"
# Outcome label inside each cell
_SEL_OUTCOME_LABEL = ".m-table-row > div > span.un-text-rem-\\[12px\\]"
# Odds value inside each cell
_SEL_OUTCOME_ODDS = ".m-table-row > div > span.un-text-rem-\\[14px\\].un-font-bold"
# Fallback: .m-outcome-item contains both label and odds
_SEL_OUTCOME_ITEM = ".m-outcome-item"

# "Book Bet" button that appears in betslip bottom bar after adding a selection
_SEL_BOOK_BET = ".m-book-btn"

# Booking code share page selectors
_SEL_BOOKING_CODE = ".booking-code-share-code > span"
_SEL_BOOKING_MODAL = ".drawer.bottom-panel-drawer, #booking-code-detail-panel-container"
_SEL_MODAL_CLOSE = ".middle.close .arrow-icon, .close-icon"

# Stairway odds filter — per PROJECT_STAIRWAY.md
ODDS_MIN = 1.20
ODDS_MAX = 4.00

# Top-N percent per date
TOP_PERCENT = 0.20


# ── DB helpers ──────────────────────────────────────────────────────────────

def _save_booking_code_to_db(
    conn: sqlite3.Connection,
    fixture_id: str,
    date: str,
    booking_code: str,
    booking_odds: float,
    booking_url: str,
) -> None:
    """Save booking code + odds + URL back to the predictions row."""
    try:
        # Add columns if they don't exist yet (idempotent)
        for col_def in [
            "booking_code TEXT",
            "booking_odds REAL",
            "booking_url TEXT",
        ]:
            try:
                conn.execute(f"ALTER TABLE predictions ADD COLUMN {col_def}")
            except Exception:
                pass  # column already exists

        conn.execute(
            """UPDATE predictions
               SET booking_code = ?, booking_odds = ?, booking_url = ?
               WHERE fixture_id = ? AND date = ?""",
            (booking_code, booking_odds, booking_url, fixture_id, date),
        )
        conn.commit()
    except Exception as e:
        print(f"    [Booking] DB save failed for {fixture_id}: {e}")


def _get_match_url_for_fixture(
    conn: sqlite3.Connection, fixture_id: str
) -> str:
    """Look up the football.com match URL from fb_matches table."""
    try:
        # fb_matches stores the resolved match URL after Ch1 P1
        row = conn.execute(
            """SELECT match_url FROM fb_matches
               WHERE fixture_id = ? LIMIT 1""",
            (fixture_id,),
        ).fetchone()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return ""


def _select_top_per_date(recommendations: List[Dict]) -> List[Dict]:
    """
    Return top 20% of recommendations per date.
    recommendations must be sorted by score DESC (recommend_bets already does this).
    """
    from collections import defaultdict
    by_date: Dict[str, List[Dict]] = defaultdict(list)
    for r in recommendations:
        by_date[r.get("date", "")].append(r)

    selected = []
    for date, recs in by_date.items():
        n = max(1, math.ceil(len(recs) * TOP_PERCENT))
        selected.extend(recs[:n])
        print(
            f"  [Booking] Date {date}: {len(recs)} recommendations → "
            f"top {n} ({TOP_PERCENT:.0%}) selected for booking code harvest"
        )
    return selected


# ── Outcome click helpers ──────────────────────────────────────────────────

def _normalise(text: str) -> str:
    """Lowercase + strip for fuzzy label matching."""
    return text.lower().strip()


async def _find_and_click_outcome(
    page: Page,
    target_outcome: str,
    market_name: str,
    fixture_id: str,
) -> float:
    """
    Find the outcome cell on an already-loaded match page and click it.
    Returns the odds value if successful, 0.0 if not found.

    Match page assumption: all markets are already expanded (Ch1 P1 scrolled them).
    We do NOT use search — we scan .m-table-row cells directly.
    """
    try:
        target_norm = _normalise(target_outcome)

        # Strategy 1: .m-table-row cells
        rows = await page.query_selector_all(".m-table-row")
        for row in rows:
            try:
                # Find label inside row
                label_el = await row.query_selector("span.un-text-rem-\\[12px\\]")
                if not label_el:
                    label_el = await row.query_selector("span")
                if not label_el:
                    continue
                label_text = _normalise(await label_el.inner_text())
                if label_text != target_norm:
                    continue

                # Found label — get odds from sibling span
                odds_el = await row.query_selector(
                    "span.un-text-rem-\\[14px\\].un-font-bold"
                )
                if not odds_el:
                    odds_el = await row.query_selector("span.un-font-bold")
                odds_val = 0.0
                if odds_el:
                    try:
                        odds_val = float(
                            (await odds_el.inner_text()).strip().replace(",", ".")
                        )
                    except ValueError:
                        pass

                # Stairway filter
                if not (ODDS_MIN <= odds_val <= ODDS_MAX):
                    print(
                        f"    [Booking] {fixture_id} '{target_outcome}' "
                        f"odds {odds_val:.2f} out of Stairway range "
                        f"[{ODDS_MIN}, {ODDS_MAX}] — skipping"
                    )
                    return 0.0

                # Click the cell div (ancestor of both label + odds)
                cell = await row.query_selector("div.un-rounded-rem-\\[10px\\]")
                if not cell:
                    cell = row  # fall back to row itself
                await cell.scroll_into_view_if_needed()
                await cell.click()
                await asyncio.sleep(0.8)
                return odds_val

            except Exception:
                continue

        # Strategy 2: .m-outcome-item fallback
        items = await page.query_selector_all(".m-outcome-item")
        for item in items:
            try:
                name_el = await item.query_selector(
                    ".m-outcome-name, [class*='outcome-name'], span"
                )
                if not name_el:
                    continue
                if _normalise(await name_el.inner_text()) != target_norm:
                    continue
                odds_el = await item.query_selector(
                    ".m-price, .m-odds-value, [class*='price']"
                )
                odds_val = 0.0
                if odds_el:
                    try:
                        odds_val = float(
                            (await odds_el.inner_text()).strip().replace(",", ".")
                        )
                    except ValueError:
                        pass
                if not (ODDS_MIN <= odds_val <= ODDS_MAX):
                    return 0.0
                await item.scroll_into_view_if_needed()
                await item.click()
                await asyncio.sleep(0.8)
                return odds_val
            except Exception:
                continue

        print(
            f"    [Booking] '{target_outcome}' not found on page "
            f"for {fixture_id} (market: {market_name})"
        )
        return 0.0

    except Exception as e:
        print(f"    [Booking] Outcome click error {fixture_id}: {e}")
        return 0.0


async def _click_book_bet_and_extract_code(page: Page) -> str:
    """
    After an outcome is added to the betslip, click Book Bet and
    extract the booking code from the share page/modal.
    Returns the code string, or '' on failure.
    """
    try:
        # Wait for Book Bet button
        book_btn = page.locator(_SEL_BOOK_BET).first
        await book_btn.wait_for(state="visible", timeout=5000)
        await book_btn.click()
        await asyncio.sleep(2)

        # The page either navigates to a share page or opens a modal
        # Try share page first (.booking-code-share-code > span)
        code_el = page.locator(_SEL_BOOKING_CODE).first
        try:
            await code_el.wait_for(state="visible", timeout=4000)
            code = (await code_el.inner_text()).strip()
            if code:
                return code
        except Exception:
            pass

        # Fallback: modal contains code
        modal = page.locator(_SEL_BOOKING_MODAL).first
        try:
            await modal.wait_for(state="visible", timeout=3000)
            code_in_modal = await modal.locator(_SEL_BOOKING_CODE).first.inner_text()
            if code_in_modal.strip():
                return code_in_modal.strip()
        except Exception:
            pass

        print("    [Booking] Could not find booking code element")
        return ""

    except Exception as e:
        print(f"    [Booking] Book Bet click failed: {e}")
        return ""


async def _dismiss_modal_and_clear(page: Page) -> None:
    """Close the booking code modal and clear the betslip."""
    try:
        # Try modal close button
        close = page.locator(_SEL_MODAL_CLOSE).first
        if await close.count() > 0 and await close.is_visible():
            await close.click()
            await asyncio.sleep(0.5)
    except Exception:
        pass

    try:
        # Use existing force_clear_slip if available
        from Modules.FootballCom.booker.slip import force_clear_slip
        await force_clear_slip(page)
    except Exception:
        # Fallback: press Escape
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.5)


# ── Main entry point ────────────────────────────────────────────────────────

async def harvest_booking_codes_for_recommendations(
    page: Page,
    recommendations: List[Dict],
    conn: sqlite3.Connection = None,
) -> int:
    """
    Ch1 P3 — Booking code harvest for top 20% of recommendations per date.

    Args:
        page:             Playwright Page — no-login session (same as Ch1 P1).
        recommendations:  Sorted list of dicts from get_recommendations().
                          Each must have: fixture_id, date, prediction,
                          market, match (home vs away).
        conn:             SQLite connection. If None, opens one internally.

    Returns:
        Number of booking codes successfully harvested.
    """
    own_conn = conn is None
    if own_conn:
        conn = _get_conn()

    selected = _select_top_per_date(recommendations)
    if not selected:
        print("  [Booking] No recommendations to harvest codes for.")
        return 0

    harvested = 0
    skipped_no_url = 0
    skipped_odds = 0
    skipped_no_code = 0

    print(f"\n  [Ch1 P3] Booking Code Harvest — {len(selected)} matches selected")
    print("  " + "─" * 58)

    for rec in selected:
        fixture_id = str(rec.get("fixture_id", ""))
        date_str = rec.get("date", "")
        match_label = rec.get("match", fixture_id)
        prediction = rec.get("prediction", "")
        market = rec.get("market", "")

        if not fixture_id or not prediction:
            continue

        # 1. Get match URL from fb_matches
        match_url = _get_match_url_for_fixture(conn, fixture_id)
        if not match_url:
            print(f"  [Booking] No match URL for {match_label} ({fixture_id}) — skipping")
            skipped_no_url += 1
            continue

        print(f"\n  [Booking] {match_label}")
        print(f"    Prediction: {prediction} | Market: {market}")

        try:
            # 2. Navigate to match page
            await page.goto(match_url, wait_until="domcontentloaded", timeout=25000)
            await asyncio.sleep(2)

            # 3. Find + click the predicted outcome
            odds_val = await _find_and_click_outcome(
                page, prediction, market, fixture_id
            )
            if odds_val == 0.0:
                skipped_odds += 1
                continue

            print(f"    Clicked: '{prediction}' @ {odds_val:.2f}")

            # 4. Click Book Bet → extract code
            code = await _click_book_bet_and_extract_code(page)
            if not code:
                skipped_no_code += 1
                await _dismiss_modal_and_clear(page)
                continue

            booking_url = f"https://www.football.com/ng/m?shareCode={code}"
            print(f"    ✓ Code: {code}  →  {booking_url}")

            # 5. Persist to DB
            _save_booking_code_to_db(
                conn, fixture_id, date_str, code, odds_val, booking_url
            )

            # 6. Clear slip — ready for next outcome
            await _dismiss_modal_and_clear(page)
            harvested += 1

        except Exception as e:
            print(f"    [Booking] Error for {match_label}: {e}")
            await _dismiss_modal_and_clear(page)
            continue

    print(f"\n  [Ch1 P3] Booking harvest complete:")
    print(f"    ✓ Harvested: {harvested}")
    print(f"    ✗ No URL:    {skipped_no_url}")
    print(f"    ✗ Odds OOR:  {skipped_odds}")
    print(f"    ✗ No code:   {skipped_no_code}")
    print("  " + "─" * 58)

    if own_conn:
        conn.close()

    return harvested
