#!/usr/bin/env python3
"""
build.py — NYC Eats static site generator.

Discovers all data sources, fetches venues, and renders a static site
into dist/ ready to be served by nginx.

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
    # Wider radius (30m) is safe because we prefer matches sharing the same
    # zip code and only fall through to geo-only when text matching failed.
    GEO_RADIUS_M = 30
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
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                for dv in dohmh_geo_grid.get((cell[0] + di, cell[1] + dj), []):
                    if id(dv) in geo_merged_dohmh:
                        continue
                    d = _haversine_m(lat, lng, dv["lat"], dv["lng"])
                    if d < best_dist:
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

    # Stamp cross-reference flags (not-on-Yelp / not-on-Google) from cached DB
    from crossref import DB_PATH as XREF_DB, venue_key as xref_key, get_flags, get_stats as xref_stats, init_db as xref_init
    xref_checked = 0
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
                xref_checked += 1
        log.info("Cross-ref: stamped %d venues | yelp=%s google=%s", xref_checked, xs.get("yelp", {}), xs.get("google", {}))
    else:
        log.info("Cross-ref: no DB yet — skipping (run crossref.py first)")

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
    )
    data_js.write_text(data_content)
    # Content hash for cache-busting
    data_hash = hashlib.sha256(data_content.encode()).hexdigest()[:12]
    log.info("Wrote %s (%.1f MB, hash=%s)", data_js, data_js.stat().st_size / 1e6, data_hash)

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
        pipeline_stats=pipeline_stats,
        merge_stats=merge_stats,
    )
    (DIST / "index.html").write_text(html)

    # Copy static assets
    if STATIC.exists():
        for f in STATIC.iterdir():
            shutil.copy2(f, DIST / f.name)

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
