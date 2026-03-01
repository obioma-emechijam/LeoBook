# football_logos.py: Downloads team & league logo ZIPs from football-logos.cc
# Part of LeoBook Scripts
#
# Functions: download_all_logos(), _download_league_zip()
# Called by: Leo.py (--logos utility)

"""
Downloads curated football logo collections from football-logos.cc CDN.
Each league ZIP is season-tagged (e.g. 2025-2026) and contains individual
team logos in transparent PNG + SVG formats.

Output:  Modules/Assets/logos/<league_slug>/
Usage:   python Leo.py --logos
         python Leo.py --logos --limit 5
"""

import os
import logging
import zipfile
import io
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = PROJECT_ROOT / "Modules" / "Assets" / "logos"

# CDN config
CDN_BASE = "https://assets.football-logos.cc/collections/"
SEASON = "-2025-2026.football-logos.cc.zip"

# Curated league slugs — ordered by priority (top leagues first)
# Format matches CDN path: {CDN_BASE}{slug}{SEASON}
LEAGUE_SLUGS = [
    # ── Major Tournaments ──
    "fifa-world-cup-2026",
    "ucl-champions-league",
    "uefa-europa-league",
    "uefa-conference-league",
    # ── England ──
    "english-premier-league",
    "england-efl-championship",
    "england-efl-league-one",
    "england-efl-league-two",
    # ── Spain ──
    "spain-la-liga",
    "spain-la-liga-2",
    # ── Italy ──
    "italy-serie-a",
    "italy-serie-b",
    # ── Germany ──
    "germany-bundesliga",
    "germany-2-bundesliga",
    # ── France ──
    "france-ligue-1",
    "france-ligue-2",
    # ── Other Top Leagues ──
    "portugal-primeira-liga",
    "netherlands-eredivisie",
    "belgium-pro-league",
    "turkey-super-lig",
    "scotland-premiership",
    # ── Americas ──
    "brazil-serie-a",
    "brazil-serie-b",
    "argentina-primera-division",
    "usa-mls",
    # ── Rest ──
    "saudi-arabia-pro-league",
    "romania-liga-1",
    "czech-republic-first-league",
    "croatia-hnl",
    "denmark-superliga",
    "norway-eliteserien",
    "sweden-allsvenskan",
    "switzerland-super-league",
    "austria-bundesliga",
    "poland-ekstraklasa",
    "ukraine-premier-league",
    "russia-premier-league",
    "greece-super-league",
    "japan-j1-league",
    "south-korea-k-league-1",
    "mexico-liga-mx",
    "colombia-primera-a",
    "chile-primera-division",
]


def _download_league_zip(slug: str) -> dict:
    """Downloads and extracts a single league ZIP. Returns result dict."""
    zip_url = f"{CDN_BASE}{slug}{SEASON}"
    league_dir = OUTPUT_DIR / slug.replace("-", "_")

    # Skip if already downloaded (idempotent)
    if league_dir.exists() and any(league_dir.iterdir()):
        return {"slug": slug, "status": "skipped", "reason": "already exists"}

    try:
        r = requests.get(zip_url, stream=True, timeout=30)
        r.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            league_dir.mkdir(parents=True, exist_ok=True)
            z.extractall(league_dir)

        file_count = sum(1 for _ in league_dir.rglob("*") if _.is_file())
        size_mb = sum(f.stat().st_size for f in league_dir.rglob("*") if f.is_file()) / (1024 * 1024)
        return {"slug": slug, "status": "ok", "files": file_count, "size_mb": round(size_mb, 1)}

    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return {"slug": slug, "status": "not_found", "reason": f"404 — slug or season mismatch"}
        return {"slug": slug, "status": "error", "reason": str(e)}
    except zipfile.BadZipFile:
        return {"slug": slug, "status": "error", "reason": "corrupt ZIP"}
    except Exception as e:
        return {"slug": slug, "status": "error", "reason": str(e)}


def download_all_logos(limit: Optional[int] = None, max_workers: int = 4):
    """Downloads all league logo collections concurrently.

    Args:
        limit: Max number of leagues to download (for testing).
        max_workers: Thread pool concurrency (default 4, CDN-friendly).
    """
    slugs = LEAGUE_SLUGS[:limit] if limit else LEAGUE_SLUGS
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(f"[Logos] Downloading {len(slugs)} league logo packs (workers: {max_workers})")

    results = {"ok": 0, "skipped": 0, "not_found": 0, "error": 0}
    errors = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_download_league_zip, slug): slug for slug in slugs}

        for future in as_completed(futures):
            result = future.result()
            slug = result["slug"]
            status = result["status"]
            results[status] = results.get(status, 0) + 1

            if status == "ok":
                logger.info(f"  [+] {slug}: {result['files']} files ({result['size_mb']}MB)")
            elif status == "skipped":
                logger.info(f"  [=] {slug}: {result['reason']}")
            elif status == "not_found":
                logger.warning(f"  [!] {slug}: {result['reason']}")
            else:
                logger.error(f"  [x] {slug}: {result['reason']}")
                errors.append(slug)

    logger.info(
        f"[Logos] Done: {results['ok']} downloaded, "
        f"{results['skipped']} skipped, "
        f"{results.get('not_found', 0)} not found, "
        f"{results['error']} failed"
    )
    if errors:
        logger.warning(f"[Logos] Failed slugs: {errors}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Download football logo packs from football-logos.cc")
    parser.add_argument("--limit", type=int, help="Limit leagues to download (testing)")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent download threads")
    args = parser.parse_args()
    download_all_logos(limit=args.limit, max_workers=args.workers)