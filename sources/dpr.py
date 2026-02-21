"""
NYC Parks Department (DPR) — Directory of Eateries.

Source: https://data.cityofnewyork.us/Recreation/Directory-of-Eateries/8792-ebcp
XML:    https://www.nycgovparks.org/bigapps/DPR_Eateries_001.xml
Data dict: https://www.nycgovparks.org/bigapps/desc/DPR_Eateries_001.txt

Includes food carts, mobile food trucks, snack bars, specialty carts, and
restaurants operating in NYC parks.  No coordinates in the source data —
we geocode via Google Geocoding API and cache results locally.
"""

from __future__ import annotations

import json
from datetime import date
import logging
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import requests

from sources.base import DataSource, Venue

log = logging.getLogger(__name__)

EATERIES_URL = "https://www.nycgovparks.org/bigapps/DPR_Eateries_001.xml"
XML_NS = {"d": "http://www.nycgovparks.org/bigapps/desc/DPR_Eateries_001.txt"}

GEOCODE_CACHE = Path(__file__).resolve().parent.parent / ".cache" / "dpr_geocode.json"
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")

# NYC center for location biasing
_NYC_BIAS = "circle:50000@40.7128,-74.0060"

# Park-ID prefix → borough name
_PREFIX_BORO = {
    "M": "MANHATTAN",
    "B": "BROOKLYN",
    "Q": "QUEENS",
    "X": "BRONX",
    "R": "STATEN ISLAND",
}

# Major park IDs → park name (for geocoding context)
_PARK_NAMES: dict[str, str] = {
    "M010": "Central Park",
    "Q099": "Flushing Meadows Corona Park",
    "B073": "Prospect Park",
    "M071": "Riverside Park",
    "X039": "Pelham Bay Park",
    "R046": "Clove Lakes Park",
    "M005": "Battery Park",
    "R005": "Conference House Park",
    "X118": "Van Cortlandt Park",
    "X010": "Bronx Park",
    "M029": "Fort Tryon Park",
    "M042": "Inwood Hill Park",
    "M098": "Randalls Island Park",
    "X002": "Crotona Park",
    "M052": "Marcus Garvey Park",
    "R106": "Freshkills Park",
}

# food-related type_name values we want to include
_FOOD_TYPES = {
    "Food Cart",
    "Mobile Food Truck",
    "Snack Bar",
    "Specialty Cart",
    "Restaurant",
    "Breakfast Cart",
}


def _geocode(query: str, cache: dict[str, Any]) -> tuple[float, float] | None:
    """Geocode via Google Places Find Place, with a local JSON cache."""
    if query in cache:
        hit = cache[query]
        if hit:
            return hit["lat"], hit["lng"]
        return None

    if not GOOGLE_API_KEY:
        return None

    try:
        resp = requests.get(
            "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
            params={
                "input": query,
                "inputtype": "textquery",
                "fields": "geometry",
                "locationbias": _NYC_BIAS,
                "key": GOOGLE_API_KEY,
            },
            timeout=15,
        )
        resp.raise_for_status()
        candidates = resp.json().get("candidates", [])
        if candidates:
            loc = candidates[0]["geometry"]["location"]
            cache[query] = {"lat": loc["lat"], "lng": loc["lng"]}
            return loc["lat"], loc["lng"]
        else:
            cache[query] = None
            return None
    except Exception as exc:
        log.warning("Geocode failed for %r: %s", query, exc)
        return None
    finally:
        time.sleep(0.05)  # gentle rate limiting


class DPRSource(DataSource):
    raw_count: int = 0
    geocoded_count: int = 0
    cached_count: int = 0
    skipped_no_coords: int = 0
    skipped_expired: int = 0

    @property
    def name(self) -> str:
        return "dpr"

    @property
    def description(self) -> str:
        return "NYC Parks Dept — Directory of Eateries"

    def fetch(self) -> list[Venue]:
        log.info("DPR: fetching eateries XML …")
        resp = requests.get(EATERIES_URL, timeout=30)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        facilities = root.findall(".//d:facility", XML_NS)
        log.info("DPR: %d facilities in feed", len(facilities))

        # Load geocode cache
        cache: dict[str, Any] = {}
        if GEOCODE_CACHE.exists():
            try:
                cache = json.loads(GEOCODE_CACHE.read_text())
            except Exception:
                pass

        venues: list[Venue] = []
        api_calls = 0

        for fac in facilities:
            type_name = (fac.findtext("d:type_name", "", XML_NS) or "").strip()
            if type_name not in _FOOD_TYPES:
                continue

            self.raw_count += 1
            fac_name = (fac.findtext("d:name", "", XML_NS) or "").strip()
            location = (fac.findtext("d:location", "", XML_NS) or "").strip()
            park_id = (fac.findtext("d:park_id", "", XML_NS) or "").strip()
            end_date = (fac.findtext("d:end_date", "", XML_NS) or "").strip()
            start_date = (fac.findtext("d:start_date", "", XML_NS) or "").strip()
            phone = (fac.findtext("d:phone", "", XML_NS) or "").strip()
            website = (fac.findtext("d:website", "", XML_NS) or "").strip()

            borough = _PREFIX_BORO.get(park_id[:1], "") if park_id else ""

            # Skip expired permits
            if end_date:
                try:
                    if date.fromisoformat(end_date) < date.today():
                        self.skipped_expired += 1
                        continue
                except ValueError:
                    pass  # unparseable date — keep the venue

            # Build geocoding query: "location, park name, borough, New York, NY"
            # Drop vendor name — it confuses the Places API for park-internal locations
            park_name = _PARK_NAMES.get(park_id, "")
            query_parts = [location, park_name, borough.title(), "New York, NY"]
            query = ", ".join(p for p in query_parts if p)

            if not query:
                self.skipped_no_coords += 1
                continue

            was_cached = query in cache
            coords = _geocode(query, cache)
            if not was_cached:
                api_calls += 1

            if not coords:
                self.skipped_no_coords += 1
                continue

            lat, lng = coords
            if was_cached:
                self.cached_count += 1
            else:
                self.geocoded_count += 1

            # Build a readable address from the text location
            address = location

            tags: list[str] = []
            tag = type_name.lower().replace(" ", "_")
            tags.append(tag)

            meta: dict[str, Any] = {}
            if park_id:
                meta["park_id"] = park_id
            if website:
                meta["website"] = website
            if end_date:
                meta["permit_end"] = end_date

            venues.append(Venue(
                name=fac_name,
                lat=lat,
                lng=lng,
                source="dpr",
                address=address,
                borough=borough,
                phone=phone,
                opened=start_date,
                tags=tags,
                meta=meta,
            ))

        # Save geocode cache
        GEOCODE_CACHE.parent.mkdir(parents=True, exist_ok=True)
        GEOCODE_CACHE.write_text(json.dumps(cache, indent=2))

        log.info(
            "DPR: %d food venues | geocoded=%d (api calls=%d), cached=%d, skipped=%d, expired=%d",
            len(venues), self.geocoded_count, api_calls,
            self.cached_count, self.skipped_no_coords, self.skipped_expired,
        )
        return venues
