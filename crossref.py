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
TRIPADVISOR_API_KEY = os.environ.get("TRIPADVISOR_API_KEY", "")
YELP_DAILY_LIMIT = int(os.environ.get("YELP_DAILY_LIMIT", "4500"))
GOOGLE_DAILY_LIMIT = int(os.environ.get("GOOGLE_DAILY_LIMIT", "1000"))
OPENTABLE_DAILY_LIMIT = int(os.environ.get("OPENTABLE_DAILY_LIMIT", "2000"))
TRIPADVISOR_DAILY_LIMIT = int(os.environ.get("TRIPADVISOR_DAILY_LIMIT", "4500"))

# Requests per second (conservative)
YELP_QPS = 4
GOOGLE_QPS = 8
OPENTABLE_QPS = 3
TRIPADVISOR_QPS = 4

# NYC center for proximity searches
_NYC_LAT, _NYC_LNG = 40.7128, -74.0060


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
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
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
    # Migrate: add review count / rating columns if missing
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(crossref)")}
    migrations = [
        ("yelp_review_count", "INTEGER"),
        ("yelp_rating",       "REAL"),
        ("google_rating_count", "INTEGER"),
        ("google_rating",       "REAL"),
        ("google_lat",          "REAL"),
        ("google_lng",          "REAL"),
        ("yelp_lat",            "REAL"),
        ("yelp_lng",            "REAL"),
        ("yelp_categories",     "TEXT"),
        # OpenTable
        ("opentable_status",     "TEXT DEFAULT 'unchecked'"),
        ("opentable_checked_at", "TEXT"),
        ("opentable_rid",        "TEXT"),
        ("opentable_url",        "TEXT"),
        ("opentable_rating",     "REAL"),
        ("opentable_review_count", "INTEGER"),
        ("opentable_price",      "TEXT"),
        # TripAdvisor
        ("tripadvisor_status",     "TEXT DEFAULT 'unchecked'"),
        ("tripadvisor_checked_at", "TEXT"),
        ("tripadvisor_location_id", "TEXT"),
        ("tripadvisor_url",        "TEXT"),
        ("tripadvisor_rating",     "REAL"),
        ("tripadvisor_review_count", "INTEGER"),
    ]
    for col, typ in migrations:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE crossref ADD COLUMN {col} {typ}")
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
            review_count = None
            yelp_rating = None
            y_lat = y_lng = None
            y_categories = None
            for biz in resp.json().get("businesses", []):
                if _name_similarity(name, biz.get("name", "")) >= 0.45:
                    found = True
                    yelp_id = biz.get("id")
                    yelp_url = biz.get("url")
                    review_count = biz.get("review_count")
                    yelp_rating = biz.get("rating")
                    coords = biz.get("coordinates", {})
                    y_lat = coords.get("latitude")
                    y_lng = coords.get("longitude")
                    cats = biz.get("categories", [])
                    if cats:
                        y_categories = ",".join(c.get("alias", "") for c in cats)
                    break

            conn.execute(
                "UPDATE crossref SET yelp_status=?, yelp_checked_at=?, "
                "yelp_business_id=?, yelp_url=?, yelp_review_count=?, yelp_rating=?, "
                "yelp_lat=?, yelp_lng=?, yelp_categories=? "
                "WHERE venue_key=?",
                (
                    "found" if found else "not_found",
                    datetime.now(timezone.utc).isoformat(),
                    yelp_id, yelp_url, review_count, yelp_rating,
                    y_lat, y_lng, y_categories, key,
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
                    "fields": "place_id,name,formatted_address,rating,user_ratings_total,geometry",
                    "key": GOOGLE_API_KEY,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            candidates = data.get("candidates", [])
            found = len(candidates) > 0
            place_id = candidates[0].get("place_id") if found else None
            g_rating = candidates[0].get("rating") if found else None
            g_rating_count = candidates[0].get("user_ratings_total") if found else None
            g_lat = g_lng = None
            if found:
                geom = candidates[0].get("geometry", {}).get("location", {})
                g_lat = geom.get("lat")
                g_lng = geom.get("lng")

            conn.execute(
                "UPDATE crossref SET google_status=?, google_checked_at=?, "
                "google_place_id=?, google_rating=?, google_rating_count=?, "
                "google_lat=?, google_lng=? "
                "WHERE venue_key=?",
                (
                    "found" if found else "not_found",
                    datetime.now(timezone.utc).isoformat(),
                    place_id, g_rating, g_rating_count,
                    g_lat, g_lng, key,
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
# OpenTable  (autocomplete search)
# ---------------------------------------------------------------------------

_OT_AUTOCOMPLETE = "https://www.opentable.com/dapi/fe/gql"
_OT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.opentable.com",
    "Referer": "https://www.opentable.com/",
}

# GQL autocomplete query (stable, used by the OT search bar)
_OT_GQL_QUERY = """
query Autocomplete($term: String!, $latitude: Float!, $longitude: Float!) {
  autocomplete(term: $term, latitude: $latitude, longitude: $longitude) {
    restaurants {
      rid
      name
      urls { profileLink }
      statistics { reviews { allTimeSummary { overallRating reviewCount } } }
      priceBand
    }
  }
}
"""


def check_opentable(conn: sqlite3.Connection, limit: int | None = None) -> int:
    """Search OpenTable for each unchecked venue via their autocomplete GQL."""
    limit = limit or OPENTABLE_DAILY_LIMIT
    rows = conn.execute(
        "SELECT venue_key, venue_name, venue_address, venue_borough "
        "FROM crossref WHERE (opentable_status IS NULL OR opentable_status = 'unchecked') "
        "LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        log.info("OpenTable: nothing to check")
        return 0

    log.info("OpenTable: %d venues to check (limit %d)", len(rows), limit)
    checked = 0

    for key, name, address, borough in rows:
        try:
            resp = requests.post(
                _OT_AUTOCOMPLETE,
                headers=_OT_HEADERS,
                json={
                    "operationName": "Autocomplete",
                    "variables": {
                        "term": f"{name} {borough}",
                        "latitude": _NYC_LAT,
                        "longitude": _NYC_LNG,
                    },
                    "query": _OT_GQL_QUERY,
                },
                timeout=15,
            )
            if resp.status_code == 429:
                log.warning("OpenTable rate-limited after %d requests", checked)
                conn.commit()
                return checked
            resp.raise_for_status()

            data = resp.json()
            restaurants = (
                data.get("data", {}).get("autocomplete", {}).get("restaurants") or []
            )

            found = False
            ot_rid = ot_url = ot_price = None
            ot_rating = ot_rc = None
            for r in restaurants:
                if _name_similarity(name, r.get("name", "")) >= 0.45:
                    found = True
                    ot_rid = str(r.get("rid", ""))
                    urls = r.get("urls") or {}
                    ot_url = urls.get("profileLink")
                    if ot_url and not ot_url.startswith("http"):
                        ot_url = f"https://www.opentable.com{ot_url}"
                    stats = (r.get("statistics") or {}).get("reviews", {}).get(
                        "allTimeSummary", {}
                    )
                    ot_rating = stats.get("overallRating")
                    ot_rc = stats.get("reviewCount")
                    ot_price = r.get("priceBand")
                    break

            conn.execute(
                "UPDATE crossref SET opentable_status=?, opentable_checked_at=?, "
                "opentable_rid=?, opentable_url=?, opentable_rating=?, "
                "opentable_review_count=?, opentable_price=? "
                "WHERE venue_key=?",
                (
                    "found" if found else "not_found",
                    datetime.now(timezone.utc).isoformat(),
                    ot_rid, ot_url, ot_rating, ot_rc, ot_price, key,
                ),
            )
        except requests.RequestException as exc:
            log.error("OpenTable error for %s: %s", name, exc)
            conn.execute(
                "UPDATE crossref SET opentable_status='error', opentable_checked_at=? "
                "WHERE venue_key=?",
                (datetime.now(timezone.utc).isoformat(), key),
            )

        checked += 1
        if checked % 100 == 0:
            conn.commit()
            log.info("OpenTable: %d / %d checked", checked, len(rows))
        time.sleep(1.0 / OPENTABLE_QPS)

    conn.commit()
    log.info("OpenTable: finished %d checks", checked)
    return checked


# ---------------------------------------------------------------------------
# TripAdvisor Content API  (requires free API key)
# ---------------------------------------------------------------------------

_TA_SEARCH = "https://api.content.tripadvisor.com/api/v1/location/search"
_TA_DETAILS = "https://api.content.tripadvisor.com/api/v1/location/{location_id}/details"


def check_tripadvisor(conn: sqlite3.Connection, limit: int | None = None) -> int:
    """Search TripAdvisor for each unchecked venue via the Content API (free tier).

    Requires TRIPADVISOR_API_KEY set as env var.
    Free tier: 5 000 calls/month.  We use 2 calls per venue (search + details).
    """
    if not TRIPADVISOR_API_KEY:
        log.warning("TRIPADVISOR_API_KEY not set — skipping")
        return 0

    limit = limit or TRIPADVISOR_DAILY_LIMIT
    rows = conn.execute(
        "SELECT venue_key, venue_name, venue_address, venue_borough "
        "FROM crossref WHERE (tripadvisor_status IS NULL OR tripadvisor_status = 'unchecked') "
        "LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        log.info("TripAdvisor: nothing to check")
        return 0

    log.info("TripAdvisor: %d venues to check (limit %d)", len(rows), limit)
    headers = {"Accept": "application/json"}
    checked = 0

    for key, name, address, borough in rows:
        try:
            # Step 1: search
            resp = requests.get(
                _TA_SEARCH,
                headers=headers,
                params={
                    "key": TRIPADVISOR_API_KEY,
                    "searchQuery": f"{name}, {address}, New York",
                    "category": "restaurants",
                    "language": "en",
                    "latLong": f"{_NYC_LAT},{_NYC_LNG}",
                },
                timeout=15,
            )
            if resp.status_code == 429:
                log.warning("TripAdvisor rate-limited after %d requests", checked)
                conn.commit()
                return checked
            resp.raise_for_status()

            results = resp.json().get("data", [])
            found = False
            ta_loc_id = ta_url = None
            ta_rating = ta_rc = None

            for r in results:
                if _name_similarity(name, r.get("name", "")) >= 0.45:
                    ta_loc_id = str(r.get("location_id", ""))
                    found = True
                    break

            # Step 2: get details for rating/reviews
            if found and ta_loc_id:
                time.sleep(1.0 / TRIPADVISOR_QPS)
                d_resp = requests.get(
                    _TA_DETAILS.format(location_id=ta_loc_id),
                    headers=headers,
                    params={
                        "key": TRIPADVISOR_API_KEY,
                        "language": "en",
                        "currency": "USD",
                    },
                    timeout=15,
                )
                if d_resp.status_code == 200:
                    det = d_resp.json()
                    ta_rating = det.get("rating")
                    if ta_rating:
                        ta_rating = float(ta_rating)
                    ta_rc = det.get("num_reviews")
                    if ta_rc:
                        ta_rc = int(ta_rc)
                    ta_url = det.get("web_url")

            conn.execute(
                "UPDATE crossref SET tripadvisor_status=?, tripadvisor_checked_at=?, "
                "tripadvisor_location_id=?, tripadvisor_url=?, tripadvisor_rating=?, "
                "tripadvisor_review_count=? WHERE venue_key=?",
                (
                    "found" if found else "not_found",
                    datetime.now(timezone.utc).isoformat(),
                    ta_loc_id, ta_url, ta_rating, ta_rc, key,
                ),
            )
        except requests.RequestException as exc:
            log.error("TripAdvisor error for %s: %s", name, exc)
            conn.execute(
                "UPDATE crossref SET tripadvisor_status='error', tripadvisor_checked_at=? "
                "WHERE venue_key=?",
                (datetime.now(timezone.utc).isoformat(), key),
            )

        checked += 1
        if checked % 100 == 0:
            conn.commit()
            log.info("TripAdvisor: %d / %d checked", checked, len(rows))
        time.sleep(1.0 / TRIPADVISOR_QPS)

    conn.commit()
    log.info("TripAdvisor: finished %d checks", checked)
    return checked


# ---------------------------------------------------------------------------
# Backfill — re-check 'found' venues missing review count / rating data
# ---------------------------------------------------------------------------

def backfill_yelp(conn: sqlite3.Connection, limit: int = 500) -> int:
    """Backfill review_count and rating for Yelp 'found' venues using Business Details API."""
    if not YELP_API_KEY:
        log.warning("YELP_API_KEY not set — skipping backfill")
        return 0

    rows = conn.execute(
        "SELECT venue_key, yelp_business_id FROM crossref "
        "WHERE yelp_status = 'found' AND yelp_business_id IS NOT NULL "
        "AND yelp_review_count IS NULL LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        log.info("Yelp backfill: nothing to do")
        return 0

    log.info("Yelp backfill: %d venues to update", len(rows))
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    filled = 0

    for key, biz_id in rows:
        try:
            resp = requests.get(
                f"https://api.yelp.com/v3/businesses/{biz_id}",
                headers=headers,
                timeout=15,
            )
            if resp.status_code == 429:
                log.warning("Yelp backfill rate-limited after %d", filled)
                conn.commit()
                return filled
            resp.raise_for_status()
            biz = resp.json()
            coords = biz.get("coordinates", {})
            conn.execute(
                "UPDATE crossref SET yelp_review_count=?, yelp_rating=?, "
                "yelp_lat=?, yelp_lng=? "
                "WHERE venue_key=?",
                (biz.get("review_count"), biz.get("rating"),
                 coords.get("latitude"), coords.get("longitude"), key),
            )
        except requests.RequestException as exc:
            log.error("Yelp backfill error for %s: %s", biz_id, exc)

        filled += 1
        if filled % 100 == 0:
            conn.commit()
            log.info("Yelp backfill: %d / %d", filled, len(rows))
        time.sleep(1.0 / YELP_QPS)

    conn.commit()
    log.info("Yelp backfill: finished %d", filled)
    return filled


def backfill_google(conn: sqlite3.Connection, limit: int = 500) -> int:
    """Backfill rating / rating_count for Google 'found' venues using Place Details."""
    if not GOOGLE_API_KEY:
        log.warning("GOOGLE_API_KEY not set — skipping backfill")
        return 0

    rows = conn.execute(
        "SELECT venue_key, google_place_id FROM crossref "
        "WHERE google_status = 'found' AND google_place_id IS NOT NULL "
        "AND google_rating_count IS NULL LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        log.info("Google backfill: nothing to do")
        return 0

    log.info("Google backfill: %d venues to update", len(rows))
    filled = 0

    for key, place_id in rows:
        try:
            resp = requests.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={
                    "place_id": place_id,
                    "fields": "rating,user_ratings_total,geometry",
                    "key": GOOGLE_API_KEY,
                },
                timeout=15,
            )
            resp.raise_for_status()
            result = resp.json().get("result", {})
            geom = result.get("geometry", {}).get("location", {})
            conn.execute(
                "UPDATE crossref SET google_rating=?, google_rating_count=?, "
                "google_lat=?, google_lng=? "
                "WHERE venue_key=?",
                (result.get("rating"), result.get("user_ratings_total"),
                 geom.get("lat"), geom.get("lng"), key),
            )
        except requests.RequestException as exc:
            log.error("Google backfill error for %s: %s", place_id, exc)

        filled += 1
        if filled % 100 == 0:
            conn.commit()
            log.info("Google backfill: %d / %d", filled, len(rows))
        time.sleep(1.0 / GOOGLE_QPS)

    conn.commit()
    log.info("Google backfill: finished %d", filled)
    return filled


# ---------------------------------------------------------------------------
# Backfill coordinates — re-fetch coords for 'found' venues missing lat/lng
# ---------------------------------------------------------------------------

def backfill_coords_google(conn: sqlite3.Connection, limit: int = 1000) -> int:
    """Re-fetch geometry for Google 'found' venues missing coordinates."""
    if not GOOGLE_API_KEY:
        log.warning("GOOGLE_API_KEY not set — skipping coord backfill")
        return 0

    rows = conn.execute(
        "SELECT venue_key, google_place_id FROM crossref "
        "WHERE google_status = 'found' AND google_place_id IS NOT NULL "
        "AND google_lat IS NULL LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        log.info("Google coord backfill: nothing to do")
        return 0

    log.info("Google coord backfill: %d venues", len(rows))
    filled = 0

    for key, place_id in rows:
        try:
            resp = requests.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={
                    "place_id": place_id,
                    "fields": "geometry",
                    "key": GOOGLE_API_KEY,
                },
                timeout=15,
            )
            resp.raise_for_status()
            result = resp.json().get("result", {})
            geom = result.get("geometry", {}).get("location", {})
            if geom.get("lat") and geom.get("lng"):
                conn.execute(
                    "UPDATE crossref SET google_lat=?, google_lng=? WHERE venue_key=?",
                    (geom["lat"], geom["lng"], key),
                )
                filled += 1
        except requests.RequestException as exc:
            log.error("Google coord backfill error for %s: %s", place_id, exc)

        if filled % 100 == 0 and filled > 0:
            conn.commit()
            log.info("Google coord backfill: %d / %d", filled, len(rows))
        time.sleep(1.0 / GOOGLE_QPS)

    conn.commit()
    log.info("Google coord backfill: finished %d", filled)
    return filled


def backfill_coords_yelp(conn: sqlite3.Connection, limit: int = 1000) -> int:
    """Re-fetch coordinates for Yelp 'found' venues missing lat/lng."""
    if not YELP_API_KEY:
        log.warning("YELP_API_KEY not set — skipping coord backfill")
        return 0

    rows = conn.execute(
        "SELECT venue_key, yelp_business_id FROM crossref "
        "WHERE yelp_status = 'found' AND yelp_business_id IS NOT NULL "
        "AND yelp_lat IS NULL LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        log.info("Yelp coord backfill: nothing to do")
        return 0

    log.info("Yelp coord backfill: %d venues", len(rows))
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    filled = 0

    for key, biz_id in rows:
        try:
            resp = requests.get(
                f"https://api.yelp.com/v3/businesses/{biz_id}",
                headers=headers,
                timeout=15,
            )
            if resp.status_code == 429:
                log.warning("Yelp coord backfill rate-limited after %d", filled)
                conn.commit()
                return filled
            resp.raise_for_status()
            biz = resp.json()
            coords = biz.get("coordinates", {})
            if coords.get("latitude") and coords.get("longitude"):
                conn.execute(
                    "UPDATE crossref SET yelp_lat=?, yelp_lng=? WHERE venue_key=?",
                    (coords["latitude"], coords["longitude"], key),
                )
                filled += 1
        except requests.RequestException as exc:
            log.error("Yelp coord backfill error for %s: %s", biz_id, exc)

        if filled % 100 == 0 and filled > 0:
            conn.commit()
            log.info("Yelp coord backfill: %d / %d", filled, len(rows))
        time.sleep(1.0 / YELP_QPS)

    conn.commit()
    log.info("Yelp coord backfill: finished %d", filled)
    return filled


# ---------------------------------------------------------------------------
# Read helpers (used by build.py)
# ---------------------------------------------------------------------------

def get_flags(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return {venue_key: {yelp, google, opentable, tripadvisor, review counts, ratings, coords}} for all checked venues."""
    flags: dict[str, dict] = {}
    for row in conn.execute(
        "SELECT venue_key, yelp_status, google_status, yelp_url, "
        "yelp_review_count, yelp_rating, google_rating_count, google_rating, "
        "google_lat, google_lng, yelp_lat, yelp_lng, yelp_categories, "
        "opentable_status, opentable_url, opentable_rating, opentable_review_count, "
        "tripadvisor_status, tripadvisor_url, tripadvisor_rating, tripadvisor_review_count "
        "FROM crossref"
    ):
        (key, yelp_st, google_st, yelp_url, y_rc, y_rat, g_rc, g_rat,
         g_lat, g_lng, y_lat, y_lng, y_cats,
         ot_st, ot_url, ot_rat, ot_rc,
         ta_st, ta_url, ta_rat, ta_rc) = row
        flags[key] = {
            "yelp": yelp_st,
            "google": google_st,
            "yelp_url": yelp_url,
            "yelp_reviews": y_rc,
            "yelp_rating": y_rat,
            "google_reviews": g_rc,
            "google_rating": g_rat,
            "google_lat": g_lat,
            "google_lng": g_lng,
            "yelp_lat": y_lat,
            "yelp_lng": y_lng,
            "yelp_categories": y_cats.split(",") if y_cats else [],
            "opentable": ot_st or "unchecked",
            "opentable_url": ot_url,
            "opentable_rating": ot_rat,
            "opentable_reviews": ot_rc,
            "tripadvisor": ta_st or "unchecked",
            "tripadvisor_url": ta_url,
            "tripadvisor_rating": ta_rat,
            "tripadvisor_reviews": ta_rc,
        }
    return flags


def get_stats(conn: sqlite3.Connection) -> dict:
    """Summary counts per service."""
    stats: dict[str, dict[str, int]] = {}
    for svc in ("yelp", "google", "opentable", "tripadvisor"):
        col = f"{svc}_status"
        counts: dict[str, int] = {}
        for row in conn.execute(
            f"SELECT {col}, COUNT(*) FROM crossref GROUP BY {col}"
        ):
            counts[row[0] or "unchecked"] = row[1]
        stats[svc] = counts
    return stats


def get_review_distribution(conn: sqlite3.Connection) -> dict:
    """Return review count distribution for found venues (for threshold exploration)."""
    dist: dict[str, dict] = {}
    for svc, col_rc, col_rat in [
        ("yelp", "yelp_review_count", "yelp_rating"),
        ("google", "google_rating_count", "google_rating"),
    ]:
        status_col = f"{svc}_status"
        rows = conn.execute(
            f"SELECT {col_rc}, {col_rat} FROM crossref "
            f"WHERE {status_col} = 'found' AND {col_rc} IS NOT NULL"
        ).fetchall()
        if not rows:
            dist[svc] = {"count": 0}
            continue
        counts = sorted(r[0] for r in rows)
        ratings = [r[1] for r in rows if r[1] is not None]
        n = len(counts)
        dist[svc] = {
            "count": n,
            "min": counts[0],
            "p10": counts[int(n * 0.10)],
            "p25": counts[int(n * 0.25)],
            "median": counts[n // 2],
            "p75": counts[int(n * 0.75)],
            "p90": counts[int(n * 0.90)],
            "max": counts[-1],
            "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else None,
            "under_10": sum(1 for c in counts if c < 10),
            "under_25": sum(1 for c in counts if c < 25),
            "under_50": sum(1 for c in counts if c < 50),
            "under_100": sum(1 for c in counts if c < 100),
        }
        # Rating breakdown for low-review venues
        low_reviews = [(rc, rat) for rc, rat in rows if rc < 50 and rat is not None]
        if low_reviews:
            high_rated_hidden = [(rc, rat) for rc, rat in low_reviews if rat >= 4.0]
            dist[svc]["hidden_gems_candidates"] = len(high_rated_hidden)
            dist[svc]["hidden_gems_pct"] = round(len(high_rated_hidden) / len(low_reviews) * 100, 1)
    return dist


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

    parser = argparse.ArgumentParser(description="Cross-reference venues vs Yelp, Google, OpenTable & TripAdvisor")
    parser.add_argument("--yelp-limit", type=int, default=None)
    parser.add_argument("--google-limit", type=int, default=None)
    parser.add_argument("--google-borough", default="MANHATTAN")
    parser.add_argument("--opentable-limit", type=int, default=None)
    parser.add_argument("--tripadvisor-limit", type=int, default=None)
    parser.add_argument("--stats", action="store_true", help="Print stats and exit")
    parser.add_argument("--backfill", action="store_true",
                        help="Re-check 'found' venues missing review counts")
    parser.add_argument("--backfill-coords", action="store_true",
                        help="Re-fetch coordinates for 'found' venues missing lat/lng")
    parser.add_argument("--backfill-limit", type=int, default=500,
                        help="Max venues to backfill per run")
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
        dist = get_review_distribution(conn)
        for svc, d in dist.items():
            if d["count"] == 0:
                print(f"\n{svc.upper()} reviews: no data yet")
                continue
            print(f"\n{svc.upper()} review counts ({d['count']} venues with data):")
            print(f"  min={d['min']}, p10={d['p10']}, p25={d['p25']}, "
                  f"median={d['median']}, p75={d['p75']}, p90={d['p90']}, max={d['max']}")
            print(f"  avg rating: {d['avg_rating']}")
            print(f"  <10 reviews: {d['under_10']}, <25: {d['under_25']}, "
                  f"<50: {d['under_50']}, <100: {d['under_100']}")
            if "hidden_gems_candidates" in d:
                print(f"  Hidden gem candidates (<50 reviews, ≥4.0★): "
                      f"{d['hidden_gems_candidates']} ({d['hidden_gems_pct']}% of low-review)")
        # Coordinate stats
        for svc, lat_col in [("Google", "google_lat"), ("Yelp", "yelp_lat")]:
            status_col = f"{svc.lower()}_status"
            total_found = conn.execute(
                f"SELECT COUNT(*) FROM crossref WHERE {status_col} = 'found'"
            ).fetchone()[0]
            has_coords = conn.execute(
                f"SELECT COUNT(*) FROM crossref WHERE {status_col} = 'found' AND {lat_col} IS NOT NULL"
            ).fetchone()[0]
            print(f"\n{svc} coordinates: {has_coords:,} / {total_found:,} found venues have coords")
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

    if args.backfill:
        t0 = time.time()
        yelp_n = backfill_yelp(conn, limit=args.backfill_limit)
        google_n = backfill_google(conn, limit=args.backfill_limit)
        elapsed = time.time() - t0
        log.info("Backfill: yelp=%d, google=%d in %.1fs", yelp_n, google_n, elapsed)
        conn.close()
        return

    if args.backfill_coords:
        t0 = time.time()
        google_n = backfill_coords_google(conn, limit=args.backfill_limit)
        yelp_n = backfill_coords_yelp(conn, limit=args.backfill_limit)
        elapsed = time.time() - t0
        log.info("Coord backfill: google=%d, yelp=%d in %.1fs", google_n, yelp_n, elapsed)
        conn.close()
        return

    t0 = time.time()
    yelp_n = check_yelp(conn, limit=args.yelp_limit)
    google_n = check_google(conn, limit=args.google_limit, borough_filter=args.google_borough)
    ot_n = check_opentable(conn, limit=args.opentable_limit)
    ta_n = check_tripadvisor(conn, limit=args.tripadvisor_limit)
    elapsed = time.time() - t0

    stats = get_stats(conn)
    log.info("Yelp:        %d checked this run | %s", yelp_n, stats.get("yelp", {}))
    log.info("Google:      %d checked this run | %s", google_n, stats.get("google", {}))
    log.info("OpenTable:   %d checked this run | %s", ot_n, stats.get("opentable", {}))
    log.info("TripAdvisor: %d checked this run | %s", ta_n, stats.get("tripadvisor", {}))
    log.info("Cross-reference completed in %.1fs", elapsed)

    conn.close()


if __name__ == "__main__":
    main()
