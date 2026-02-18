#!/usr/bin/env python3
"""
crossref.py — Cross-reference NYC Eats venues against Yelp & Google Places.

Maintains a persistent SQLite cache (.cache/crossref.db) so the process
can be resumed across daily runs.  Each run processes up to the daily API
limit, then stops.  The next run picks up where it left off.

Usage (standalone):
    python crossref.py                    # process up to daily limits
    python crossref.py --yelp-limit 100   # override Yelp batch size
    python crossref.py --stats            # just print progress stats

Called from build.py to read cached flags into venue data.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import requests

log = logging.getLogger(__name__)

ROOT = Path(__file__).parent
DB_PATH = ROOT / ".cache" / "crossref.db"

# --- API configuration (set via env or .env) ---
YELP_API_KEY = os.environ.get("YELP_API_KEY", "")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
YELP_DAILY_LIMIT = int(os.environ.get("YELP_DAILY_LIMIT", "4500"))
GOOGLE_DAILY_LIMIT = int(os.environ.get("GOOGLE_DAILY_LIMIT", "1000"))

# Requests per second (conservative)
YELP_QPS = 4
GOOGLE_QPS = 8


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def venue_key(name: str, address: str, borough: str) -> str:
    """Stable key shared by build.py and crossref.py."""
    return f"{name.lower().strip()}|{address.lower().strip()}|{borough.lower().strip()}"


def _name_similarity(a: str, b: str) -> float:
    """Quick fuzzy-match score between two business names."""
    a, b = a.lower().strip(), b.lower().strip()
    if a == b:
        return 1.0
    if a in b or b in a:
        return 0.85
    return SequenceMatcher(None, a, b).ratio()


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crossref (
            venue_key       TEXT PRIMARY KEY,
            venue_name      TEXT,
            venue_address   TEXT,
            venue_borough   TEXT,
            yelp_status     TEXT DEFAULT 'unchecked',
            yelp_checked_at TEXT,
            yelp_business_id TEXT,
            yelp_url        TEXT,
            google_status     TEXT DEFAULT 'unchecked',
            google_checked_at TEXT,
            google_place_id   TEXT
        )
    """)
    conn.commit()
    return conn


def sync_venues(conn: sqlite3.Connection, venues: list[dict]) -> int:
    """Ensure every venue has a row in the crossref table.  Returns new count."""
    existing = {r[0] for r in conn.execute("SELECT venue_key FROM crossref")}
    new = 0
    for v in venues:
        k = venue_key(v["name"], v.get("address", ""), v.get("borough", ""))
        if k not in existing:
            conn.execute(
                "INSERT INTO crossref (venue_key, venue_name, venue_address, venue_borough) "
                "VALUES (?, ?, ?, ?)",
                (k, v["name"], v.get("address", ""), v.get("borough", "")),
            )
            existing.add(k)
            new += 1
    conn.commit()
    log.info("Synced venues: %d new, %d total", new, len(existing))
    return new


# ---------------------------------------------------------------------------
# Yelp Fusion  (all boroughs)
# ---------------------------------------------------------------------------

BORO_MAP = {
    "MANHATTAN": "New York",
    "BROOKLYN": "Brooklyn",
    "QUEENS": "Queens",
    "BRONX": "Bronx",
    "STATEN ISLAND": "Staten Island",
}


def check_yelp(conn: sqlite3.Connection, limit: int | None = None) -> int:
    if not YELP_API_KEY:
        log.warning("YELP_API_KEY not set — skipping")
        return 0

    limit = limit or YELP_DAILY_LIMIT
    rows = conn.execute(
        "SELECT venue_key, venue_name, venue_address, venue_borough "
        "FROM crossref WHERE yelp_status = 'unchecked' LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        log.info("Yelp: nothing to check")
        return 0

    log.info("Yelp: %d venues to check (limit %d)", len(rows), limit)
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    checked = 0

    for key, name, address, borough in rows:
        city = BORO_MAP.get(borough.upper(), "New York")
        try:
            resp = requests.get(
                "https://api.yelp.com/v3/businesses/search",
                headers=headers,
                params={"term": name, "location": f"{address}, {city}, NY", "limit": 3},
                timeout=15,
            )
            if resp.status_code == 429:
                log.warning("Yelp rate-limited after %d requests", checked)
                conn.commit()
                return checked
            resp.raise_for_status()

            found = False
            yelp_id = yelp_url = None
            for biz in resp.json().get("businesses", []):
                if _name_similarity(name, biz.get("name", "")) >= 0.45:
                    found = True
                    yelp_id = biz.get("id")
                    yelp_url = biz.get("url")
                    break

            conn.execute(
                "UPDATE crossref SET yelp_status=?, yelp_checked_at=?, "
                "yelp_business_id=?, yelp_url=? WHERE venue_key=?",
                (
                    "found" if found else "not_found",
                    datetime.now(timezone.utc).isoformat(),
                    yelp_id, yelp_url, key,
                ),
            )
        except requests.RequestException as exc:
            log.error("Yelp error for %s: %s", name, exc)
            conn.execute(
                "UPDATE crossref SET yelp_status='error', yelp_checked_at=? "
                "WHERE venue_key=?",
                (datetime.now(timezone.utc).isoformat(), key),
            )

        checked += 1
        if checked % 200 == 0:
            conn.commit()
            log.info("Yelp: %d / %d checked", checked, len(rows))
        time.sleep(1.0 / YELP_QPS)

    conn.commit()
    log.info("Yelp: finished %d checks", checked)
    return checked


# ---------------------------------------------------------------------------
# Google Places  (Manhattan only by default)
# ---------------------------------------------------------------------------

def check_google(
    conn: sqlite3.Connection,
    limit: int | None = None,
    borough_filter: str = "MANHATTAN",
) -> int:
    if not GOOGLE_API_KEY:
        log.warning("GOOGLE_API_KEY not set — skipping")
        return 0

    limit = limit or GOOGLE_DAILY_LIMIT

    # Mark non-target boroughs as 'skip' so they're never queued
    if borough_filter:
        conn.execute(
            "UPDATE crossref SET google_status = 'skip' "
            "WHERE google_status = 'unchecked' AND UPPER(venue_borough) != ?",
            (borough_filter.upper(),),
        )
        conn.commit()

    where = "google_status = 'unchecked'"
    params: list = []
    if borough_filter:
        where += " AND UPPER(venue_borough) = ?"
        params.append(borough_filter.upper())
    params.append(limit)

    rows = conn.execute(
        f"SELECT venue_key, venue_name, venue_address, venue_borough "
        f"FROM crossref WHERE {where} LIMIT ?",
        params,
    ).fetchall()
    if not rows:
        log.info("Google: nothing to check")
        return 0

    log.info("Google: %d venues to check (limit %d)", len(rows), limit)
    checked = 0

    for key, name, address, borough in rows:
        try:
            resp = requests.get(
                "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
                params={
                    "input": f"{name}, {address}, New York, NY",
                    "inputtype": "textquery",
                    "fields": "place_id,name,formatted_address",
                    "key": GOOGLE_API_KEY,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            candidates = data.get("candidates", [])
            found = len(candidates) > 0
            place_id = candidates[0].get("place_id") if found else None

            conn.execute(
                "UPDATE crossref SET google_status=?, google_checked_at=?, "
                "google_place_id=? WHERE venue_key=?",
                (
                    "found" if found else "not_found",
                    datetime.now(timezone.utc).isoformat(),
                    place_id, key,
                ),
            )
        except requests.RequestException as exc:
            log.error("Google error for %s: %s", name, exc)
            conn.execute(
                "UPDATE crossref SET google_status='error', google_checked_at=? "
                "WHERE venue_key=?",
                (datetime.now(timezone.utc).isoformat(), key),
            )

        checked += 1
        if checked % 200 == 0:
            conn.commit()
            log.info("Google: %d / %d checked", checked, len(rows))
        time.sleep(1.0 / GOOGLE_QPS)

    conn.commit()
    log.info("Google: finished %d checks", checked)
    return checked


# ---------------------------------------------------------------------------
# Read helpers (used by build.py)
# ---------------------------------------------------------------------------

def get_flags(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return {venue_key: {yelp, google}} for all checked venues."""
    flags: dict[str, dict] = {}
    for row in conn.execute(
        "SELECT venue_key, yelp_status, google_status, yelp_url "
        "FROM crossref"
    ):
        key, yelp_st, google_st, yelp_url = row
        flags[key] = {
            "yelp": yelp_st,
            "google": google_st,
            "yelp_url": yelp_url,
        }
    return flags


def get_stats(conn: sqlite3.Connection) -> dict:
    """Summary counts per service."""
    stats: dict[str, dict[str, int]] = {}
    for svc in ("yelp", "google"):
        col = f"{svc}_status"
        counts: dict[str, int] = {}
        for row in conn.execute(
            f"SELECT {col}, COUNT(*) FROM crossref GROUP BY {col}"
        ):
            counts[row[0]] = row[1]
        stats[svc] = counts
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Cross-reference venues vs Yelp & Google")
    parser.add_argument("--yelp-limit", type=int, default=None)
    parser.add_argument("--google-limit", type=int, default=None)
    parser.add_argument("--google-borough", default="MANHATTAN")
    parser.add_argument("--stats", action="store_true", help="Print stats and exit")
    args = parser.parse_args()

    conn = init_db()

    if args.stats:
        stats = get_stats(conn)
        for svc, counts in stats.items():
            total = sum(counts.values())
            print(f"\n{svc.upper()} ({total} total):")
            for status, n in sorted(counts.items()):
                pct = n / total * 100 if total else 0
                print(f"  {status:12s} {n:>7,}  ({pct:.1f}%)")
        conn.close()
        return

    # Load venues from the last build cache
    cache_files = sorted((ROOT / ".cache").glob("*.json"))
    venues: list[dict] = []
    for cf in cache_files:
        if cf.name == "crossref.json":
            continue
        venues.extend(json.loads(cf.read_text()))
    if venues:
        sync_venues(conn, venues)
    else:
        log.warning("No cached venue data found — run build.py first")

    t0 = time.time()
    yelp_n = check_yelp(conn, limit=args.yelp_limit)
    google_n = check_google(conn, limit=args.google_limit, borough_filter=args.google_borough)
    elapsed = time.time() - t0

    stats = get_stats(conn)
    log.info("Yelp:   %d checked this run | %s", yelp_n, stats.get("yelp", {}))
    log.info("Google: %d checked this run | %s", google_n, stats.get("google", {}))
    log.info("Cross-reference completed in %.1fs", elapsed)

    conn.close()


if __name__ == "__main__":
    main()
