"""
NYC Food Retail Stores — NYS Agriculture & Markets licensed stores,
enriched with USDA SNAP/EBT acceptance data.

Primary source: NYS Dept of Agriculture & Markets — Retail Food Stores
  https://data.ny.gov/Economic-Development/Retail-Food-Stores/9a8c-vfzj
  Socrata API ID: 9a8c-vfzj

Every store selling food in NY state needs this license — bodegas,
supermarkets, specialty delis, cheese shops, butchers, you name it.
~11,500 active stores in NYC's five boroughs, with lat/lng.

Enrichment: USDA SNAP Retailer Locator (historical CSV)
  https://www.fns.usda.gov/snap/retailer-locator/data
  Adds "ebt" tag to stores that also appear in the SNAP retailer list,
  matched by zip + normalized street address.

Establishment type codes (combinable):
  A = Retail food store
  B = Bakery
  C = Food processing (deli counter, prepared food)
  D = Wholesale
  E = Eggs
  H = Salvage
  K = Kosher certified
  M = Custom/exempt
  W = Warehouse
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import requests

from sources.base import DataSource, Venue

log = logging.getLogger(__name__)

# --- Primary: NYS Ag & Markets Retail Food Stores ---
_NYSDAM_DATASET = "9a8c-vfzj"
_NYSDAM_URL = f"https://data.ny.gov/resource/{_NYSDAM_DATASET}.json"

# --- Enrichment: USDA SNAP retailers ---
_SNAP_URL = (
    "https://www.fns.usda.gov/sites/default/files/resource-files/"
    "snap-retailer-locator-data2005-2025.zip"
)
_CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache"
_SNAP_CACHE = _CACHE_DIR / "snap_retailers.zip"

# NYC bounding box for coord sanity-check
_NYC_BBOX = (40.49, 40.92, -74.27, -73.68)

# Five NYC counties
_NYC_COUNTIES = {"NEW YORK", "KINGS", "QUEENS", "BRONX", "RICHMOND"}

# County → borough display name
_COUNTY_BOROUGH: dict[str, str] = {
    "NEW YORK":  "MANHATTAN",
    "KINGS":     "BROOKLYN",
    "QUEENS":    "QUEENS",
    "BRONX":     "BRONX",
    "RICHMOND":  "STATEN ISLAND",
}

# Establishment type code → human label
_ESTAB_CODES: dict[str, str] = {
    "A": "retail",
    "B": "bakery",
    "C": "food_processing",
    "D": "wholesale",
    "E": "eggs",
    "H": "salvage",
    "K": "kosher",
    "M": "custom_exempt",
    "W": "warehouse",
}


def _normalize_street(s: str) -> str:
    """Normalize a street for fuzzy matching: lowercase, strip punctuation,
    collapse whitespace, expand common abbreviations."""
    s = s.lower().strip()
    s = re.sub(r"[.,#'\"-]", "", s)
    s = re.sub(r"\s+", " ", s)
    # Common suffix expansions
    for full, abbr in [
        ("avenue", "ave"), ("street", "st"), ("boulevard", "blvd"),
        ("road", "rd"), ("drive", "dr"), ("place", "pl"),
        ("lane", "ln"), ("court", "ct"), ("parkway", "pkwy"),
        ("terrace", "ter"), ("highway", "hwy"),
    ]:
        s = re.sub(rf"\b{full}\b", abbr, s)
        s = re.sub(rf"\b{abbr}\b", abbr, s)  # normalize existing abbrs too
    return s.strip()


def _match_key(street_num: str, street_name: str, zipcode: str) -> str:
    """Build a normalized key for address matching."""
    num = re.sub(r"\D", "", street_num.strip())[:6]  # digits only
    street = _normalize_street(street_name)
    z = zipcode.strip()[:5]
    return f"{z}|{num}|{street}"


class GrocerySource(DataSource):
    raw_count: int = 0
    bad_coords: int = 0

    @property
    def name(self) -> str:
        return "grocery"

    @property
    def description(self) -> str:
        return "NYS-Licensed Food Retail Stores (NYC) + SNAP/EBT enrichment"

    # ------------------------------------------------------------------
    # SNAP enrichment: load the set of (zip|streetnum|street) keys
    # ------------------------------------------------------------------
    def _load_snap_keys(self) -> set[str]:
        """Return a set of normalized address keys for active NYC SNAP retailers."""
        _CACHE_DIR.mkdir(exist_ok=True)

        # Download zip if not cached (30-day TTL; data is annual)
        if _SNAP_CACHE.exists():
            import time
            age_h = (time.time() - _SNAP_CACHE.stat().st_mtime) / 3600
            if age_h > 30 * 24:
                _SNAP_CACHE.unlink()

        if not _SNAP_CACHE.exists():
            log.info("SNAP enrichment: downloading CSV from USDA …")
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                tmp_path = tmp.name
            try:
                subprocess.run(
                    ["curl", "-sL", "--retry", "3", "-o", tmp_path, _SNAP_URL],
                    check=True, timeout=300,
                )
                Path(tmp_path).rename(_SNAP_CACHE)
                log.info("SNAP enrichment: downloaded %.1f MB",
                         _SNAP_CACHE.stat().st_size / 1e6)
            except Exception as exc:
                log.warning("SNAP enrichment: download failed (%s), skipping EBT tags", exc)
                Path(tmp_path).unlink(missing_ok=True)
                return set()

        # Parse zip → CSV, extract NYC active retailers
        try:
            with zipfile.ZipFile(_SNAP_CACHE) as zf:
                csv_name = zf.namelist()[0]
                raw = zf.read(csv_name).decode("utf-8-sig")
        except Exception as exc:
            log.warning("SNAP enrichment: failed to read zip (%s)", exc)
            return set()

        keys: set[str] = set()
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            if row.get("State", "").strip() != "NY":
                continue
            if row.get("End Date", "").strip():  # inactive
                continue
            county = row.get("County", "").strip().upper()
            if county not in _NYC_COUNTIES:
                continue
            k = _match_key(
                row.get("Street Number", ""),
                row.get("Street Name", ""),
                row.get("Zip Code", ""),
            )
            keys.add(k)

        log.info("SNAP enrichment: %d active NYC retailer keys loaded", len(keys))
        return keys

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------
    def fetch(self) -> list[Venue]:
        # 1) Load SNAP keys for EBT enrichment
        snap_keys = self._load_snap_keys()

        # 2) Fetch from NYS Ag & Markets Socrata API
        log.info("Grocery: fetching from NYS Ag & Markets …")
        where = "county in('NEW YORK','KINGS','QUEENS','BRONX','RICHMOND')"
        params: dict[str, Any] = {
            "$where": where,
            "$limit": 50000,
        }
        token = os.environ.get("NY_OPEN_DATA_TOKEN", "")
        if token:
            params["$$app_token"] = token

        resp = requests.get(_NYSDAM_URL, params=params, timeout=60)
        resp.raise_for_status()
        rows = resp.json()
        log.info("Grocery: %d rows from API", len(rows))

        # 3) Build venues
        venues: list[Venue] = []
        ebt_matches = 0

        for row in rows:
            self.raw_count += 1

            entity = (row.get("entity_name") or "").strip()
            dba = (row.get("dba_name") or "").strip()
            store_name = dba or entity
            if not store_name:
                continue

            # Coordinates — nested in georeference object
            geo = row.get("georeference")
            if not geo or not isinstance(geo, dict):
                self.bad_coords += 1
                continue
            coords = geo.get("coordinates", [])
            if len(coords) < 2:
                self.bad_coords += 1
                continue
            try:
                lng, lat = float(coords[0]), float(coords[1])
            except (ValueError, TypeError):
                self.bad_coords += 1
                continue

            # Bbox sanity check
            lat_lo, lat_hi, lng_lo, lng_hi = _NYC_BBOX
            if not (lat_lo <= lat <= lat_hi and lng_lo <= lng <= lng_hi):
                self.bad_coords += 1
                log.debug("Grocery: bad coords for %s: %.5f,%.5f", store_name, lat, lng)
                continue

            county = (row.get("county") or "").strip().upper()
            borough = _COUNTY_BOROUGH.get(county, county)
            street_num = (row.get("street_number") or "").strip()
            street_name = (row.get("street_name") or "").strip()
            city = (row.get("city") or "").strip()
            zipcode = (row.get("zip_code") or "").strip()

            address = f"{street_num} {street_name}".strip()
            if city:
                address += f", {city}"

            # Parse establishment type codes into tags
            estab_type = (row.get("estab_type") or "").strip().upper()
            tags: list[str] = ["grocery"]
            for code in estab_type:
                tag = _ESTAB_CODES.get(code)
                if tag:
                    tags.append(tag)

            # SNAP/EBT enrichment via address match
            has_ebt = False
            if snap_keys:
                k = _match_key(street_num, street_name, zipcode)
                if k in snap_keys:
                    tags.append("ebt")
                    has_ebt = True
                    ebt_matches += 1

            # Meta
            meta: dict[str, Any] = {}
            if estab_type:
                meta["estab_type"] = estab_type
            license_num = (row.get("license_number") or "").strip()
            if license_num:
                meta["license"] = license_num
            if entity and dba and entity.lower() != dba.lower():
                meta["entity_name"] = entity
            if has_ebt:
                meta["ebt"] = True

            venues.append(Venue(
                name=store_name,
                lat=lat,
                lng=lng,
                source="grocery",
                address=address,
                borough=borough,
                zipcode=zipcode,
                tags=tags,
                meta=meta,
            ))

        log.info(
            "Grocery: %d venues (from %d rows, %d bad coords, %d EBT matches)",
            len(venues), self.raw_count, self.bad_coords, ebt_matches,
        )
        return venues
