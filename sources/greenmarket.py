"""
NYC Farmers Markets (GrowNYC Greenmarkets + community farm stands).

Primary source (NYC Open Data / DOHMH):
  https://data.cityofnewyork.us/dataset/DOHMH-Farmers-Markets/8vwk-6iz2
  ~163 markets across all five boroughs.

Supplemental source (NYS Dept of Agriculture & Markets):
  https://data.ny.gov/Economic-Development/Farmers-Markets-in-New-York-State/qq4h-8p86
  Statewide dataset filtered to NYC counties.  Adds markets not in the
  city dataset and provides operation_hours / operation_season fields.
  Socrata 4×4 IDs are stable; the slug won't change on data refresh.

Deduplication: venues within 200 m of an existing market are merged.
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any

import requests

from sources.base import DataSource, Venue

log = logging.getLogger(__name__)

# Primary: NYC Open Data (DOHMH)
DATASET_ID = "8vwk-6iz2"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"

# Supplemental: NYS Ag & Markets
NYS_DATASET_ID = "qq4h-8p86"
NYS_BASE_URL = f"https://data.ny.gov/resource/{NYS_DATASET_ID}.json"
_NYC_COUNTIES = ("New York", "Kings", "Queens", "Bronx", "Richmond")

# NYC bounding box for coord sanity-check
_NYC_BBOX = (40.49, 40.92, -74.27, -73.68)

# Borough lookup from NYS county names
_COUNTY_TO_BOROUGH: dict[str, str] = {
    "new york": "MANHATTAN",
    "kings": "BROOKLYN",
    "queens": "QUEENS",
    "bronx": "BRONX",
    "richmond": "STATEN ISLAND",
}


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Return distance in metres between two WGS-84 points."""
    R = 6_371_000  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# Markets not yet in the city dataset — added manually with coords from
# their websites.  Keyed by lowercased name for dedup against the API.
_SUPPLEMENTAL: list[dict[str, Any]] = [
    {
        "marketname": "Battery Park City Down to Earth Farmers Market",
        "borough": "Manhattan",
        "streetaddress": "River Terrace between Murray St & North End Ave",
        "latitude": "40.7163",
        "longitude": "-74.0158",
        "zip_code": "10282",
        "daysoperation": "Sunday",
        "hoursoperations": "9AM - 2PM",
        "accepts_ebt": "Yes",
        "open_year_round": "No",
        "season_begin": "2026-05-10",
        "season_end": "2026-11-22",
        "_note": "https://downtoearthmarkets.com/ — founded 2025, seasonal May–Nov",
    },
]


class FarmersMarketSource(DataSource):
    raw_count: int = 0
    bad_coords: int = 0

    @property
    def name(self) -> str:
        return "greenmarket"

    @property
    def description(self) -> str:
        return "NYC Farmers Markets & Greenmarkets"

    # ------------------------------------------------------------------
    # Primary: NYC Open Data (DOHMH)
    # ------------------------------------------------------------------
    def _fetch_nyc(self, token: str) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "$limit": 5000,
            "$where": "latitude IS NOT NULL AND longitude IS NOT NULL",
        }
        if token:
            params["$$app_token"] = token

        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        rows = resp.json()
        log.info("Farmers Markets: %d rows from NYC Open Data", len(rows))

        # Merge hand-curated supplemental markets
        api_names = {(r.get("marketname") or "").strip().lower() for r in rows}
        added = 0
        for extra in _SUPPLEMENTAL:
            if extra["marketname"].strip().lower() not in api_names:
                rows.append(extra)
                added += 1
        if added:
            log.info("Farmers Markets: injected %d supplemental markets", added)
        return rows

    # ------------------------------------------------------------------
    # Supplemental: NYS Ag & Markets
    # ------------------------------------------------------------------
    def _fetch_nys(self) -> list[dict[str, Any]]:
        county_list = ",".join(f"'{c}'" for c in _NYC_COUNTIES)
        params: dict[str, Any] = {
            "$limit": 5000,
            "$where": f"county in({county_list})",
        }
        try:
            resp = requests.get(NYS_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            rows = resp.json()
            log.info("Farmers Markets: %d rows from NYS Ag & Markets", len(rows))
            return rows
        except Exception:
            log.warning("Farmers Markets: NYS fetch failed, continuing with NYC data only", exc_info=True)
            return []

    # ------------------------------------------------------------------
    # Build Venue from a NYC-format row
    # ------------------------------------------------------------------
    @staticmethod
    def _venue_from_nyc_row(row: dict[str, Any]) -> Venue | None:
        market_name = (row.get("marketname") or "").strip()
        if not market_name:
            return None
        try:
            lat = float(row["latitude"])
            lng = float(row["longitude"])
        except (KeyError, ValueError, TypeError):
            return None

        lat_lo, lat_hi, lng_lo, lng_hi = _NYC_BBOX
        if not (lat_lo <= lat <= lat_hi and lng_lo <= lng <= lng_hi):
            return None

        borough = (row.get("borough") or "").strip().upper()
        address = (row.get("streetaddress") or "").strip()
        zipcode = (row.get("zip_code") or "").strip()

        tags: list[str] = ["farmers_market"]
        name_lower = market_name.lower()
        if "greenmarket" in name_lower:
            tags.append("greenmarket")
        elif "farm stand" in name_lower or "farmstand" in name_lower:
            tags.append("farm_stand")
        elif "youth market" in name_lower:
            tags.append("youth_market")

        meta: dict[str, Any] = {}
        days = (row.get("daysoperation") or "").strip()
        hours = (row.get("hoursoperations") or "").strip()
        if days:
            meta["days"] = days
        if hours:
            meta["hours"] = hours
        if days and hours:
            meta["schedule"] = f"{days} {hours}"

        ebt = (row.get("accepts_ebt") or "").strip().lower() == "yes"
        if ebt:
            tags.append("ebt")
            meta["ebt"] = True

        year_round = (row.get("open_year_round") or "").strip().lower() == "yes"
        if year_round:
            tags.append("year_round")
            meta["year_round"] = True

        season_begin = (row.get("season_begin") or "")[:10]
        season_end = (row.get("season_end") or "")[:10]
        if season_begin:
            meta["season_begin"] = season_begin
        if season_end:
            meta["season_end"] = season_end

        return Venue(
            name=market_name,
            lat=lat,
            lng=lng,
            source="greenmarket",
            address=address,
            borough=borough,
            zipcode=zipcode,
            tags=tags,
            meta=meta,
        )

    # ------------------------------------------------------------------
    # Build Venue from a NYS-format row
    # ------------------------------------------------------------------
    @staticmethod
    def _venue_from_nys_row(row: dict[str, Any]) -> Venue | None:
        market_name = (row.get("market_name") or "").strip()
        if not market_name:
            return None
        try:
            lat = float(row["latitude"])
            lng = float(row["longitude"])
        except (KeyError, ValueError, TypeError):
            return None

        lat_lo, lat_hi, lng_lo, lng_hi = _NYC_BBOX
        if not (lat_lo <= lat <= lat_hi and lng_lo <= lng <= lng_hi):
            return None

        county = (row.get("county") or "").strip().lower()
        borough = _COUNTY_TO_BOROUGH.get(county, "")
        address = (row.get("address_line_1") or "").strip()
        city = (row.get("city") or "").strip()
        zipcode = (row.get("zip") or "").strip()
        if city and address:
            address = f"{address}, {city}"

        tags: list[str] = ["farmers_market"]
        name_lower = market_name.lower()
        if "greenmarket" in name_lower:
            tags.append("greenmarket")
        elif "farm stand" in name_lower or "farmstand" in name_lower:
            tags.append("farm_stand")
        elif "farmers' market" in name_lower or "farmers market" in name_lower:
            tags.append("farmers_market_community")

        meta: dict[str, Any] = {"via_nys": True}

        # NYS has combined operation_hours (e.g. "Sat 8am-3pm")
        op_hours = (row.get("operation_hours") or "").strip()
        if op_hours:
            meta["schedule"] = op_hours

        op_season = (row.get("operation_season") or "").strip()
        if op_season:
            meta["season"] = op_season
            if "year-round" in op_season.lower():
                tags.append("year_round")
                meta["year_round"] = True

        snap = (row.get("snap_status") or "").strip().upper() == "Y"
        fmnp = (row.get("fmnp") or "").strip().upper() == "Y"
        if snap:
            tags.append("ebt")
            meta["ebt"] = True
        if fmnp:
            tags.append("fmnp")
            meta["fmnp"] = True

        return Venue(
            name=market_name,
            lat=lat,
            lng=lng,
            source="greenmarket",
            address=address,
            borough=borough,
            zipcode=zipcode,
            tags=tags,
            meta=meta,
        )

    # ------------------------------------------------------------------
    # Main fetch — merge NYC + NYS, deduplicate by proximity
    # ------------------------------------------------------------------
    def fetch(self) -> list[Venue]:
        token = os.environ.get("NYC_OPEN_DATA_TOKEN", "")

        # 1) Primary: NYC Open Data
        nyc_rows = self._fetch_nyc(token)
        venues: list[Venue] = []
        for row in nyc_rows:
            self.raw_count += 1
            v = self._venue_from_nyc_row(row)
            if v:
                venues.append(v)
            else:
                self.bad_coords += 1

        log.info("Farmers Markets: %d venues from NYC data", len(venues))

        # 2) Supplemental: NYS Ag & Markets — deduplicate by proximity
        nys_rows = self._fetch_nys()
        nys_added = 0
        nys_dup = 0
        for row in nys_rows:
            v = self._venue_from_nys_row(row)
            if v is None:
                continue
            # Check if any existing venue is within 200 m
            is_dup = any(
                _haversine_m(v.lat, v.lng, ev.lat, ev.lng) < 200
                for ev in venues
            )
            if is_dup:
                nys_dup += 1
            else:
                venues.append(v)
                nys_added += 1

        if nys_rows:
            log.info(
                "Farmers Markets: NYS supplement +%d new, %d duplicates skipped",
                nys_added, nys_dup,
            )

        log.info(
            "Farmers Markets: %d venues total (from %d rows, %d bad coords)",
            len(venues), self.raw_count + len(nys_rows), self.bad_coords,
        )
        return venues
