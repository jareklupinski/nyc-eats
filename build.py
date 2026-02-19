#!/usr/bin/env python3
"""
build.py — NYC Eats static site generator.

Discovers all data sources, fetches venues, and renders a static site
into dist/ for local dev; deployed to the nginx serving root.

Usage:
    python build.py                  # build with all sources
    python build.py --sources dohmh  # build with only DOHMH
    python build.py --sources dohmh,sla
    python build.py --cache          # use cached data if available (< 24h old)
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from sources.base import discover_sources

log = logging.getLogger("build")

ROOT = Path(__file__).parent
DIST = ROOT / "dist"
CACHE_DIR = ROOT / ".cache"
TEMPLATES = ROOT / "templates"
STATIC = ROOT / "static"

CACHE_MAX_AGE_SECONDS = 24 * 60 * 60  # 24 hours


def load_cached(source_name: str) -> list[dict] | None:
    """Return cached venue dicts if fresh enough, else None."""
    cache_file = CACHE_DIR / f"{source_name}.json"
    if not cache_file.exists():
        return None
    age = time.time() - cache_file.stat().st_mtime
    if age > CACHE_MAX_AGE_SECONDS:
        log.info("Cache for %s is stale (%.0fh old), refetching", source_name, age / 3600)
        return None
    log.info("Using cached data for %s (%.0fh old)", source_name, age / 3600)
    return json.loads(cache_file.read_text())


def save_cache(source_name: str, venues: list[dict]) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"{source_name}.json"
    cache_file.write_text(json.dumps(venues))
    log.info("Cached %d venues for %s", len(venues), source_name)


def _normalize(s: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    import re
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _normalize_addr(s: str) -> str:
    """Normalize address for matching: lower, strip punctuation, unify suffixes."""
    import re
    s = _normalize(s)

    # Strip "AKA ..." secondary addresses (e.g. "81 HUDSON ST AKA 1 HARRISON ST")
    s = re.sub(r"\baka\b.*", "", s)

    s = re.sub(r"[.,#\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Strip trailing unit/suite/floor/apt designators:
    # "25 N MOORE ST 1A" → "25 N MOORE ST"
    # "100 BROADWAY STE 200" → "100 BROADWAY"
    # But preserve Queens block-lot like "64-18" (handled by hyphen earlier)
    s = re.sub(r"\s+(?:ste|suite|apt|unit|fl|floor|rm|room|#)\s*\S*$", "", s)
    # Trailing bare unit number (letter+digits or digits+letter at end)
    s = re.sub(r"\s+\d*[a-z]\d*$", "", s)
    s = re.sub(r"\s+\d+$", "", s)  # trailing pure number (floor/unit)
    s = re.sub(r"\s+", " ", s).strip()

    # Strip ordinal suffixes from numbers: 3rd -> 3, 86th -> 86, 1st -> 1
    s = re.sub(r"\b(\d+)(?:st|nd|rd|th)\b", r"\1", s)

    # Canonicalize common street suffixes
    _SUFFIXES = {
        "st": "street", "str": "street",
        "ave": "avenue", "av": "avenue",
        "blvd": "boulevard", "bvd": "boulevard",
        "dr": "drive",
        "ln": "lane",
        "pl": "place",
        "rd": "road",
        "ct": "court",
        "cir": "circle",
        "ter": "terrace", "terr": "terrace",
        "pkwy": "parkway", "pky": "parkway",
        "hwy": "highway", "hgwy": "highway",
        "sq": "square",
        "tpke": "turnpike",
        "expy": "expressway", "expwy": "expressway",
        "e": "east", "w": "west", "n": "north", "s": "south",
        # Saint vs abbreviated
        "saint": "st",
    }
    parts = s.split()
    parts = [_SUFFIXES.get(p, p) for p in parts]
    return " ".join(parts)


# Map various borough representations to a canonical form
_BORO_ALIASES = {
    "manhattan": "manhattan", "new york": "manhattan", "ny": "manhattan",
    "brooklyn": "brooklyn", "bklyn": "brooklyn", "kings": "brooklyn",
    "queens": "queens",
    "bronx": "bronx", "the bronx": "bronx",
    "staten island": "staten island", "richmond": "staten island",
}


def _normalize_boro(s: str) -> str:
    return _BORO_ALIASES.get((s or "").strip().lower(), (s or "").strip().lower())


def _make_combined(dohmh_v: dict, sla_v: dict) -> dict:
    """Merge a DOHMH + SLA venue pair into a single 'both' record."""
    combined = dict(dohmh_v)
    combined["source"] = "both"
    combined["sla_name"] = sla_v.get("name", "")
    all_tags = list(dict.fromkeys(dohmh_v.get("tags", []) + sla_v.get("tags", [])))
    combined["tags"] = all_tags
    combined_meta = dict(dohmh_v.get("meta", {}))
    combined_meta.update(sla_v.get("meta", {}))
    if combined_meta:
        combined["meta"] = combined_meta
    # Keep earliest opened date from either source
    d_opened = dohmh_v.get("opened", "")
    s_opened = sla_v.get("opened", "")
    if d_opened and s_opened:
        combined["opened"] = min(d_opened, s_opened)
    elif s_opened:
        combined["opened"] = s_opened
    return combined


import re as _re
import math as _math
_RANGE_RE = _re.compile(r"^(\d+)\s+(\d+)\s+(.+)$")
_SINGLE_RE = _re.compile(r"^(\d+)\s+(.+)$")


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in meters between two (lat, lon) points."""
    R = 6_371_000
    p = _math.pi / 180
    a = (0.5 - _math.cos((lat2 - lat1) * p) / 2
         + _math.cos(lat1 * p) * _math.cos(lat2 * p)
         * (1 - _math.cos((lon2 - lon1) * p)) / 2)
    return 2 * R * _math.asin(_math.sqrt(a))


def _parse_range(raw_addr: str) -> tuple[int, int, str] | None:
    """If raw address has a range like '77 79 HUDSON ST', return (lo, hi, norm_street).

    Returns None for Queens block-lot addresses ('30-12 20TH AVE') which have a
    hyphen between numbers, and for single-number addresses.
    """
    raw = raw_addr.strip()
    # Queens block-lot: digits-digits at start → NOT a range
    if _re.match(r"^\d+-\d+", raw):
        return None
    normed = _normalize_addr(raw)
    m = _RANGE_RE.match(normed)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        street = m.group(3)
        if hi >= lo and (hi - lo) <= 30:  # sensible address range
            return (lo, hi, street)
    return None


def _parse_single_number(normed_addr: str) -> tuple[int, str] | None:
    """Parse '77 hudson street' → (77, 'hudson street')."""
    m = _SINGLE_RE.match(normed_addr)
    if m:
        try:
            return (int(m.group(1)), m.group(2))
        except ValueError:
            pass
    return None


def merge_cross_source(venues: list[dict]) -> tuple[list[dict], dict]:
    """Merge venues found in both DOHMH and SLA into source='both'.

    Pass 1 — exact match on normalized address + borough.
    Pass 2 — range match: SLA addresses like '77 79 HUDSON ST' match DOHMH
             '77 HUDSON STREET' if the DOHMH number falls within the SLA range.
             Queens block-lot addresses ('30-12 20TH AVE') are excluded.
    Pass 3 — geo-proximity fallback: remaining unmatched venues within 15m of
             each other are merged.  Catches AKA addresses, typos, and every
             other text variation in one shot.

    The DOHMH record is kept as the base (it has grade, cuisine, phone)
    and SLA tags/meta are folded in.
    """
    from collections import defaultdict

    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for v in venues:
        addr = _normalize_addr(v.get("address", ""))
        boro = _normalize_boro(v.get("borough", ""))
        if addr:
            by_key[(addr, boro)].append(v)
        else:
            by_key[("_noaddr_" + str(id(v)), "")].append(v)

    # --- Pass 1: exact address match ---
    merged: list[dict] = []
    unmatched_sla: list[dict] = []
    unmatched_dohmh: list[dict] = []
    pass1_count = 0

    for key, group in by_key.items():
        sources_in_group = {v["source"] for v in group}
        if "dohmh" in sources_in_group and "sla" in sources_in_group:
            dohmh_v = next(v for v in group if v["source"] == "dohmh")
            sla_v = next(v for v in group if v["source"] == "sla")
            merged.append(_make_combined(dohmh_v, sla_v))
            pass1_count += 1
            for v in group:
                if v is not dohmh_v and v is not sla_v:
                    merged.append(v)
        else:
            for v in group:
                if v["source"] == "sla":
                    unmatched_sla.append(v)
                elif v["source"] == "dohmh":
                    unmatched_dohmh.append(v)
                else:
                    merged.append(v)

    log.info("  Pass 1 (exact address): %d merges", pass1_count)

    # --- Pass 2: range matching ---
    dohmh_by_street: dict[tuple[str, str], list[tuple[int, dict]]] = defaultdict(list)
    for v in unmatched_dohmh:
        normed = _normalize_addr(v.get("address", ""))
        boro = _normalize_boro(v.get("borough", ""))
        parsed = _parse_single_number(normed)
        if parsed:
            num, street = parsed
            dohmh_by_street[(street, boro)].append((num, v))

    range_merged_sla: set[int] = set()
    range_merged_dohmh: set[int] = set()

    for sla_v in unmatched_sla:
        rng = _parse_range(sla_v.get("address", ""))
        if not rng:
            continue
        lo, hi, street = rng
        boro = _normalize_boro(sla_v.get("borough", ""))
        candidates = dohmh_by_street.get((street, boro), [])
        for num, dohmh_v in candidates:
            if lo <= num <= hi and id(dohmh_v) not in range_merged_dohmh:
                merged.append(_make_combined(dohmh_v, sla_v))
                range_merged_sla.add(id(sla_v))
                range_merged_dohmh.add(id(dohmh_v))
                break

    log.info("  Pass 2 (address range): %d merges", len(range_merged_sla))

    # Collect remaining unmatched for pass 3
    still_sla = [v for v in unmatched_sla if id(v) not in range_merged_sla]
    still_dohmh = [v for v in unmatched_dohmh if id(v) not in range_merged_dohmh]

    # --- Pass 3: geo-proximity fallback ---
    # Within 30m AND names must be somewhat similar (≥0.35) to avoid merging
    # different businesses that happen to be on the same block.
    GEO_RADIUS_M = 30
    GEO_NAME_THRESHOLD = 0.35
    CELL = 0.0003  # ~33m grid cells for fast lookup

    dohmh_geo_grid: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for v in still_dohmh:
        lat, lng = v.get("lat"), v.get("lng")
        if lat and lng:
            cell = (int(lat / CELL), int(lng / CELL))
            dohmh_geo_grid[cell].append(v)

    geo_merged_sla: set[int] = set()
    geo_merged_dohmh: set[int] = set()

    for sla_v in still_sla:
        lat, lng = sla_v.get("lat"), sla_v.get("lng")
        if not (lat and lng):
            continue
        cell = (int(lat / CELL), int(lng / CELL))
        best_dist = float("inf")
        best_match = None
        sla_name = _normalize(sla_v.get("name", ""))
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for dv in dohmh_geo_grid.get((cell[0] + di, cell[1] + dj), []):
                    if id(dv) in geo_merged_dohmh:
                        continue
                    d = _haversine_m(lat, lng, dv["lat"], dv["lng"])
                    if d < best_dist:
                        # Require some name similarity to avoid merging
                        # completely different businesses
                        dohmh_name = _normalize(dv.get("name", ""))
                        sim = SequenceMatcher(None, sla_name, dohmh_name).ratio()
                        if sim >= GEO_NAME_THRESHOLD:
                            best_dist = d
                            best_match = dv
        if best_dist <= GEO_RADIUS_M and best_match is not None:
            merged.append(_make_combined(best_match, sla_v))
            geo_merged_sla.add(id(sla_v))
            geo_merged_dohmh.add(id(best_match))

    log.info("  Pass 3 (geo ≤%dm): %d merges", GEO_RADIUS_M, len(geo_merged_sla))

    # Add all remaining unmatched
    for v in still_sla:
        if id(v) not in geo_merged_sla:
            merged.append(v)
    for v in still_dohmh:
        if id(v) not in geo_merged_dohmh:
            merged.append(v)

    total_merges = pass1_count + len(range_merged_sla) + len(geo_merged_sla)
    stats = {
        "pre_merge": len(venues),
        "pass1": pass1_count,
        "pass2": len(range_merged_sla),
        "pass3": len(geo_merged_sla),
        "total_merges": total_merges,
        "post_merge": len(merged),
    }
    return merged, stats


def build(selected_sources: set[str] | None = None, use_cache: bool = False) -> None:
    sources = discover_sources()

    if selected_sources:
        sources = [s for s in sources if s.name in selected_sources]
        found = {s.name for s in sources}
        missing = selected_sources - found
        if missing:
            log.warning("Unknown sources: %s", ", ".join(missing))

    if not sources:
        log.error("No data sources found!")
        return

    log.info("Active sources: %s", ", ".join(s.name for s in sources))

    # Fetch from all sources
    all_venues: list[dict] = []
    source_meta: list[dict] = []
    pipeline_stats: list[dict] = []

    for source in sources:
        log.info("=== Fetching: %s — %s ===", source.name, source.description)

        cached = load_cached(source.name) if use_cache else None
        if cached is not None:
            venue_dicts = cached
        else:
            try:
                venues = source.fetch()
                venue_dicts = [v.to_dict() for v in venues]
                save_cache(source.name, venue_dicts)
            except Exception:
                log.exception("Failed to fetch from %s", source.name)
                # Try stale cache as fallback
                cached = load_cached(source.name) if not use_cache else None
                if cached:
                    log.warning("Using stale cache for %s as fallback", source.name)
                    venue_dicts = cached
                else:
                    continue

        all_venues.extend(venue_dicts)
        source_meta.append({
            "name": source.name,
            "description": source.description,
            "count": len(venue_dicts),
        })

        # Collect per-source pipeline stats
        if hasattr(source, "bin_dedup_count") and source.bin_dedup_count:
            pipeline_stats.append({
                "label": f"{source.name.upper()} BIN dedup (same name + building)",
                "removed": source.bin_dedup_count,
                "detail": f"{source.raw_camis_count} unique CAMIS → {source.raw_camis_count - source.bin_dedup_count} after dedup",
            })
        if hasattr(source, "dedup_count") and source.dedup_count:
            pipeline_stats.append({
                "label": f"{source.name.upper()} license dedup (same name + address)",
                "removed": source.dedup_count,
                "detail": f"{source.raw_count} raw → {source.raw_count - source.dedup_count} after dedup",
            })

    log.info("Total venues (pre-merge): %d from %d sources", len(all_venues), len(source_meta))

    # Merge venues that appear in both DOHMH and SLA (same normalized name + address)
    all_venues, merge_stats = merge_cross_source(all_venues)
    log.info("Total venues (post-merge): %d", len(all_venues))

    # --- Apply manual overrides (overrides.json) ---
    # A curated list of known data-quality issues in the source APIs.
    # Currently supports action "drop_coords" to remove bad coordinates.
    # This file is optional — if absent the build still runs bbox validation
    # below.  See overrides.json for documentation and notes.
    overrides_file = ROOT / "overrides.json"
    overrides_applied = 0
    if overrides_file.exists():
        overrides_data = json.loads(overrides_file.read_text())
        override_list = overrides_data.get("overrides", [])
        # Build a lookup set keyed on (name, address, borough) for fast matching
        drop_coords_keys: set[tuple[str, str, str]] = set()
        for o in override_list:
            if o.get("action") == "drop_coords":
                drop_coords_keys.add((
                    o["name"].strip().lower(),
                    o["address"].strip().lower(),
                    _normalize_boro(o.get("borough", "")),
                ))
        for v in all_venues:
            key = (
                v.get("name", "").strip().lower(),
                v.get("address", "").strip().lower(),
                _normalize_boro(v.get("borough", "")),
            )
            if key in drop_coords_keys and "lat" in v and "lng" in v:
                del v["lat"]
                del v["lng"]
                overrides_applied += 1
        log.info("Overrides: applied %d from %d entries in overrides.json",
                 overrides_applied, len(override_list))

    # Validate coordinates against borough bounding boxes.
    # Some source records have correct address/borough but wildly wrong lat/lng
    # (e.g., a Manhattan venue placed in Brooklyn).  Drop bad coords so the
    # crossref override or a future geocode can fix them.
    # This catches issues not yet listed in overrides.json.
    _BORO_BOUNDS = {
        "manhattan":      (40.698, 40.882, -74.025, -73.907),
        "brooklyn":       (40.566, 40.740, -74.045, -73.830),
        "queens":         (40.540, 40.812, -73.963, -73.700),
        "bronx":          (40.785, 40.917, -73.935, -73.748),
        "staten island":  (40.490, 40.652, -74.260, -74.050),
    }
    bad_coords = 0
    for v in all_venues:
        lat, lng = v.get("lat"), v.get("lng")
        if not (lat and lng):
            continue
        boro = _normalize_boro(v.get("borough", ""))
        bounds = _BORO_BOUNDS.get(boro)
        if not bounds:
            continue
        lat_lo, lat_hi, lng_lo, lng_hi = bounds
        if not (lat_lo <= lat <= lat_hi and lng_lo <= lng <= lng_hi):
            log.debug("Bad coords for %s at %s (%s): %.5f,%.5f not in %s bbox",
                      v.get("name"), v.get("address"), v.get("borough"), lat, lng, boro)
            del v["lat"]
            del v["lng"]
            bad_coords += 1
    if bad_coords:
        log.info("Dropped %d venues with coords outside their borough bbox", bad_coords)

    # Stamp cross-reference flags (not-on-Yelp / not-on-Google) from cached DB
    from crossref import DB_PATH as XREF_DB, venue_key as xref_key, get_flags, get_stats as xref_stats, init_db as xref_init
    xref_checked = 0
    coords_upgraded = 0
    if XREF_DB.exists():
        xconn = xref_init()
        flags = get_flags(xconn)
        xs = xref_stats(xconn)
        xconn.close()
        for v in all_venues:
            k = xref_key(v["name"], v.get("address", ""), v.get("borough", ""))
            f = flags.get(k)
            if f:
                v["xr_y"] = f["yelp"]      # yelp status
                v["xr_g"] = f["google"]     # google status
                if f.get("yelp_reviews") is not None:
                    v["yr"] = f["yelp_reviews"]     # yelp review count
                if f.get("yelp_rating") is not None:
                    v["yrt"] = f["yelp_rating"]     # yelp rating
                if f.get("google_reviews") is not None:
                    v["gr"] = f["google_reviews"]   # google review count
                if f.get("google_rating") is not None:
                    v["grt"] = f["google_rating"]   # google rating
                if f.get("yelp_categories"):
                    v["yelp_cats"] = f["yelp_categories"]
                # Override coordinates with more precise ones from Google/Yelp
                # but only if they fall within the venue's borough bbox.
                # Try Google first (preferred), fall back to Yelp.
                boro = _normalize_boro(v.get("borough", ""))
                bounds = _BORO_BOUNDS.get(boro)
                upgraded = False
                for src, slat, slng in (("google", f.get("google_lat"), f.get("google_lng")),
                                        ("yelp",   f.get("yelp_lat"),   f.get("yelp_lng"))):
                    if not slat or not slng:
                        continue
                    if not bounds or (bounds[0] <= slat <= bounds[1] and bounds[2] <= slng <= bounds[3]):
                        v["lat"] = slat
                        v["lng"] = slng
                        coords_upgraded += 1
                        upgraded = True
                        break
                    else:
                        log.debug("Crossref %s coords rejected for %s at %s (%s): %.5f,%.5f outside %s bbox",
                                  src, v.get("name"), v.get("address"), v.get("borough"), slat, slng, boro)
                xref_checked += 1
        log.info("Cross-ref: stamped %d venues (%d coords upgraded) | yelp=%s google=%s",
                 xref_checked, coords_upgraded, xs.get("yelp", {}), xs.get("google", {}))
    else:
        log.info("Cross-ref: no DB yet — skipping (run crossref.py first)")

    # --- Dietary tags ---
    # Authoritative sources: HMS USA → halal, KosherNearMe → kosher
    # Supplementary: DOHMH cuisine, Yelp categories, venue name keywords
    # Yelp-only: vegan, vegetarian, gluten-free
    from diet_sources import fetch_hms_halal, fetch_knm_kosher

    # --- Build matching indices for authoritative diet sources ---
    hms_entries = fetch_hms_halal(use_cache=use_cache)
    knm_entries = fetch_knm_kosher(use_cache=use_cache)

    # HMS matching: name+address normalization
    hms_by_addr: dict[tuple[str, str], dict] = {}   # (norm_addr, boro) → entry
    hms_by_name: dict[tuple[str, str], dict] = {}   # (norm_name, boro) → entry
    for h in hms_entries:
        na = _normalize_addr(h.get("address", "").split(",")[0])  # street part only
        boro = h.get("borough", "")
        nn = _normalize(h.get("name", ""))
        if na and boro:
            hms_by_addr[(na, boro)] = h
        if nn and boro:
            hms_by_name[(nn, boro)] = h

    # KNM matching: geo grid for proximity + name index
    _KNM_CELL = 0.0003  # ~33m grid
    knm_geo: dict[tuple[int, int], list[dict]] = {}
    knm_by_name: dict[str, list[dict]] = {}
    for k in knm_entries:
        lat, lng = k.get("lat"), k.get("lng")
        if lat and lng:
            cell = (int(lat / _KNM_CELL), int(lng / _KNM_CELL))
            knm_geo.setdefault(cell, []).append(k)
        nn = _normalize(k.get("name", ""))
        if nn:
            knm_by_name.setdefault(nn, []).append(k)

    def _match_hms(v: dict) -> dict | None:
        """Try to match a venue to an HMS entry by address or name."""
        boro = _normalize_boro(v.get("borough", ""))
        if not boro:
            return None
        # Address match
        na = _normalize_addr(v.get("address", ""))
        hit = hms_by_addr.get((na, boro))
        if hit:
            return hit
        # Name match within same borough
        nn = _normalize(v.get("name", ""))
        hit = hms_by_name.get((nn, boro))
        if hit:
            return hit
        return None

    def _match_knm(v: dict) -> dict | None:
        """Try to match a venue to a KNM entry by geo proximity or name."""
        lat, lng = v.get("lat"), v.get("lng")
        if lat and lng:
            cell = (int(lat / _KNM_CELL), int(lng / _KNM_CELL))
            v_name = _normalize(v.get("name", ""))
            best_dist = float("inf")
            best = None
            for di in (-1, 0, 1):
                for dj in (-1, 0, 1):
                    for k in knm_geo.get((cell[0] + di, cell[1] + dj), []):
                        d = _haversine_m(lat, lng, k["lat"], k["lng"])
                        if d < best_dist and d <= 80:
                            kn = _normalize(k.get("name", ""))
                            sim = SequenceMatcher(None, v_name, kn).ratio()
                            if sim >= 0.45:
                                best_dist = d
                                best = k
            if best:
                return best
        # Fallback: exact name match
        nn = _normalize(v.get("name", ""))
        hits = knm_by_name.get(nn, [])
        if len(hits) == 1:
            return hits[0]
        return None

    # Yelp category aliases → diet tag (vegan/vegetarian/gluten-free only)
    _YELP_DIET = {
        "vegan":       "vegan",
        "vegetarian":  "vegetarian",
        "veganraw":    "vegan",
        "raw_food":    "vegan",
        "gluten_free": "gluten-free",
    }
    # DOHMH cuisine → diet tag (vegan/vegetarian only; halal/kosher from auth sources)
    _DOHMH_DIET = {
        "Vegetarian":    "vegetarian",
        "Vegan":         "vegan",
    }

    diet_counts: dict[str, int] = {}
    diet_source_counts: dict[str, dict[str, int]] = {}  # diet → {source → count}
    hms_matched = 0
    knm_matched = 0

    for v in all_venues:
        diets: dict[str, str] = {}  # diet_tag → source_name

        # --- Authoritative: HMS USA → halal ---
        hms_hit = _match_hms(v)
        if hms_hit:
            diets["halal"] = "HMS USA"
            hms_matched += 1

        # --- Authoritative: KosherNearMe → kosher ---
        knm_hit = _match_knm(v)
        if knm_hit:
            diets["kosher"] = "KosherNearMe"
            knm_matched += 1

        # --- Supplementary: DOHMH cuisine ---
        cuisine = v.get("cuisine", "")
        dt = _DOHMH_DIET.get(cuisine)
        if dt and dt not in diets:
            diets[dt] = "DOHMH"
        # Halal/kosher from DOHMH cuisine (supplementary, lower priority)
        cl = cuisine.lower()
        if "halal" in cl and "halal" not in diets:
            diets["halal"] = "DOHMH"
        if "kosher" in cl and "kosher" not in diets:
            diets["kosher"] = "DOHMH"
        # Also from DOHMH cuisine type "Jewish/Kosher"
        if cuisine == "Jewish/Kosher" and "kosher" not in diets:
            diets["kosher"] = "DOHMH"

        # --- Supplementary: venue name keywords ---
        nl = v.get("name", "").lower()
        if "halal" in nl and "halal" not in diets:
            diets["halal"] = "Name"
        if "kosher" in nl and "kosher" not in diets:
            diets["kosher"] = "Name"
        if "vegan" in nl and "vegan" not in diets:
            diets["vegan"] = "Name"
        if "vegetarian" in nl and "vegetarian" not in diets:
            diets["vegetarian"] = "Name"

        # --- Supplementary: Yelp categories ---
        for yc in v.get("yelp_cats", []):
            dt = _YELP_DIET.get(yc)
            if dt and dt not in diets:
                diets[dt] = "Yelp"
        # Also allow Yelp halal/kosher as supplementary
        for yc in v.get("yelp_cats", []):
            if yc == "halal" and "halal" not in diets:
                diets["halal"] = "Yelp"
            if yc == "kosher" and "kosher" not in diets:
                diets["kosher"] = "Yelp"

        # vegan implies vegetarian
        if "vegan" in diets and "vegetarian" not in diets:
            diets["vegetarian"] = diets["vegan"]

        if diets:
            v["diet"] = sorted(diets.keys())
            v["diet_src"] = diets  # {tag: source_name}
            for d, src in diets.items():
                diet_counts[d] = diet_counts.get(d, 0) + 1
                diet_source_counts.setdefault(d, {})
                diet_source_counts[d][src] = diet_source_counts[d].get(src, 0) + 1

    all_diets = sorted(diet_counts.keys())
    log.info("Dietary tags: %s", {d: diet_counts[d] for d in all_diets})
    log.info("Diet source breakdown: %s", dict(diet_source_counts))
    log.info("Authoritative matches: HMS=%d, KNM=%d", hms_matched, knm_matched)

    # Strip internal-only keys before serialization
    for v in all_venues:
        v.pop("yelp_cats", None)

    # Collect all unique tags across venues for filter UI
    all_tags = sorted({t for v in all_venues for t in v.get("tags", [])})
    all_source_names = sorted({v["source"] for v in all_venues})

    # Render
    DIST.mkdir(exist_ok=True)

    # Write venue data as a separate JS file (keeps HTML small + cacheable)
    import hashlib
    data_js = DIST / "venues.js"
    data_content = (
        f"// Generated {datetime.now(timezone.utc).isoformat()}\n"
        f"const VENUE_DATA = {json.dumps(all_venues, separators=(',', ':'))};\n"
        f"const SOURCE_META = {json.dumps(source_meta, separators=(',', ':'))};\n"
        f"const ALL_TAGS = {json.dumps(all_tags)};\n"
        f"const ALL_SOURCES = {json.dumps(all_source_names)};\n"
        f"const ALL_DIETS = {json.dumps(all_diets)};\n"
        f"const DIET_SOURCE_STATS = {json.dumps(diet_source_counts, separators=(',', ':'))};\n"
    )
    data_js.write_text(data_content)
    # Content hash for cache-busting
    data_hash = hashlib.sha256(data_content.encode()).hexdigest()[:12]
    log.info("Wrote %s (%.1f MB, hash=%s)", data_js, data_js.stat().st_size / 1e6, data_hash)

    # Copy static assets & compute CSS hash for cache-busting
    css_hash = ""
    if STATIC.exists():
        for f in STATIC.iterdir():
            shutil.copy2(f, DIST / f.name)
            if f.name == "style.css":
                css_hash = hashlib.sha256(f.read_bytes()).hexdigest()[:12]

    # Render HTML
    env = Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=True)
    template = env.get_template("index.html")
    html = template.render(
        build_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        venue_count=len(all_venues),
        sources=source_meta,
        all_tags=all_tags,
        all_sources=all_source_names,
        data_hash=data_hash,
        css_hash=css_hash,
        pipeline_stats=pipeline_stats,
        merge_stats=merge_stats,
        all_diets=all_diets,
        diet_source_stats=diet_source_counts,
        hms_matched=hms_matched,
        knm_matched=knm_matched,
    )
    (DIST / "index.html").write_text(html)

    log.info("Build complete → %s/", DIST)


def main():
    parser = argparse.ArgumentParser(description="NYC Eats — build static site")
    parser.add_argument(
        "--sources",
        type=str,
        default=None,
        help="Comma-separated list of source names to include (default: all)",
    )
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Use cached data if available and < 24h old",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    selected = set(args.sources.split(",")) if args.sources else None
    build(selected_sources=selected, use_cache=args.cache)


if __name__ == "__main__":
    main()
