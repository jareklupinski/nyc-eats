#!/usr/bin/env python3
"""
diet_sources.py — Fetch and cache authoritative dietary-certification data.

Sources:
  • Halal  — HMS USA (hmsusa.org/certified-listing)   HTML data-attributes
  • Kosher — KosherNearMe API (api.koshernear.me)     JSON API

Each fetcher returns a list of normalized dicts with at minimum:
    name, address, city, state, source_id
and optional fields for matching (lat, lng, phone, etc.).

Results are cached in .cache/ as JSON with 24-hour freshness.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("diet_sources")

CACHE_DIR = Path(__file__).parent / ".cache"
CACHE_MAX_AGE = 24 * 60 * 60  # 24 hours

# NYC borough bounding boxes (same as build.py)
_BORO_BOUNDS = {
    "manhattan":     (40.698, 40.882, -74.025, -73.907),
    "brooklyn":      (40.566, 40.740, -74.045, -73.830),
    "queens":        (40.540, 40.812, -73.963, -73.700),
    "bronx":         (40.785, 40.917, -73.935, -73.748),
    "staten island": (40.490, 40.652, -74.260, -74.050),
}

# Generous overall NYC bbox for quick containment check
_NYC_LAT = (40.490, 40.920)
_NYC_LNG = (-74.260, -73.700)


def _in_nyc_bbox(lat: float, lng: float) -> bool:
    """Check if (lat, lng) falls within the generous NYC bounding box."""
    return (_NYC_LAT[0] <= lat <= _NYC_LAT[1] and
            _NYC_LNG[0] <= lng <= _NYC_LNG[1])


def _load_cache(name: str) -> list[dict] | None:
    path = CACHE_DIR / f"diet_{name}.json"
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age > CACHE_MAX_AGE:
        log.info("Diet cache %s stale (%.0fh), refetching", name, age / 3600)
        return None
    data = json.loads(path.read_text())
    log.info("Using cached diet data for %s (%d entries, %.0fh old)",
             name, len(data), age / 3600)
    return data


def _save_cache(name: str, data: list[dict]) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    path = CACHE_DIR / f"diet_{name}.json"
    path.write_text(json.dumps(data, indent=2))
    log.info("Cached %d diet entries for %s", len(data), name)


# ---------------------------------------------------------------------------
# HMS USA — Halal
# ---------------------------------------------------------------------------

_HMS_URL = "https://www.hmsusa.org/certified-listing"

# NYC county names → borough canonical names
_HMS_COUNTY_TO_BORO = {
    "new york county":  "manhattan",
    "kings county":     "brooklyn",
    "queens county":    "queens",
    "bronx county":     "bronx",
    "richmond county":  "staten island",
}


def fetch_hms_halal(use_cache: bool = True) -> list[dict]:
    """Fetch HMS USA certified halal listings for NYC.

    Scrapes the HTML page data-attributes (no API available).
    Returns normalized dicts for matching against venue list.
    """
    if use_cache:
        cached = _load_cache("hms_halal")
        if cached is not None:
            return cached

    log.info("Fetching HMS USA certified listing from %s", _HMS_URL)
    r = requests.get(_HMS_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    listings = soup.find_all("div", class_="MagicListing")
    log.info("HMS: parsed %d total listings", len(listings))

    # Extract data-* attributes
    all_entries = []
    for el in listings:
        attrs = {k.replace("data-", ""): v
                 for k, v in el.attrs.items() if k.startswith("data-")}
        all_entries.append(attrs)

    # Filter: NYC 5 boroughs, certified status only
    results = []
    for e in all_entries:
        county = e.get("county-name", "").lower()
        status = e.get("status-name", "")
        boro = _HMS_COUNTY_TO_BORO.get(county)
        if not boro:
            continue
        if status != "Certified":
            continue

        results.append({
            "name": (e.get("name") or "").strip(),
            "address": (e.get("address") or "").strip(),
            "city": (e.get("city-name") or "").strip(),
            "borough": boro,
            "state": "NY",
            "category": e.get("category", ""),
            "status": status,
            "source_id": e.get("id", ""),
            "hms_serial": e.get("serial-num", ""),
        })

    log.info("HMS: %d certified NYC listings (from %d total)",
             len(results), len(all_entries))
    _save_cache("hms_halal", results)
    return results


# ---------------------------------------------------------------------------
# KosherNearMe — Kosher
# ---------------------------------------------------------------------------

_KNM_API = "https://api.koshernear.me/api/search/location"


def fetch_knm_kosher(use_cache: bool = True) -> list[dict]:
    """Fetch KosherNearMe listings near NYC.

    Uses the search API centered on zip 10282 (Lower Manhattan) with a
    generous radius, then filters to NYC bounding box.
    """
    if use_cache:
        cached = _load_cache("knm_kosher")
        if cached is not None:
            return cached

    log.info("Fetching KosherNearMe listings from API")
    r = requests.get(_KNM_API, params={
        "format": "json",
        "place": "10282",
        "rad": "500",
    }, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()

    data = r.json()
    hits = data.get("hits", [])
    log.info("KNM: received %d hits", len(hits))

    # Filter to NYC bbox and normalize
    results = []
    for h in hits:
        coords = h.get("coords") or {}
        lat = coords.get("latitude")
        lng = coords.get("longitude")

        # Must have coords and be in NYC
        if lat is None or lng is None:
            continue
        try:
            lat, lng = float(lat), float(lng)
        except (ValueError, TypeError):
            continue
        if not _in_nyc_bbox(lat, lng):
            continue

        loc = h.get("location") or {}
        results.append({
            "name": (h.get("name") or "").strip(),
            "address": (loc.get("address1") or "").strip(),
            "address2": (loc.get("address2") or "").strip(),
            "city": (loc.get("city") or "").strip(),
            "state": (loc.get("province") or "").strip(),
            "postal_code": (loc.get("postal_code") or "").strip(),
            "lat": lat,
            "lng": lng,
            "supervision": (h.get("supervision") or "").strip(),
            "business_types": h.get("business_types") or [],
            "food_types": h.get("food_types") or [],
            "source_id": str(h.get("lid") or h.get("nid") or ""),
            "review_avg": h.get("review_avg"),
            "total_reviews": h.get("total_reviews"),
            "phone": ((h.get("contact") or {}).get("phone") or "").strip(),
            "url": ((h.get("contact") or {}).get("url") or "").strip(),
        })

    log.info("KNM: %d listings within NYC bbox (from %d total hits)",
             len(results), len(hits))
    _save_cache("knm_kosher", results)
    return results


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_all(use_cache: bool = True) -> dict[str, list[dict]]:
    """Fetch all dietary source data. Returns {"halal": [...], "kosher": [...]}."""
    return {
        "halal": fetch_hms_halal(use_cache=use_cache),
        "kosher": fetch_knm_kosher(use_cache=use_cache),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                        datefmt="%H:%M:%S")
    data = fetch_all(use_cache=False)
    for diet, entries in data.items():
        print(f"\n{diet}: {len(entries)} entries")
        for e in entries[:5]:
            print(f"  {e['name']:40s} | {e['address']}")
