"""
NYC DOHMH Restaurant Inspection Results.

Source: https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j
API docs: https://dev.socrata.com/foundry/data.cityofnewyork.us/43nn-pn8j

No API key required (throttled to ~1000 req/hr without one).
With an app token you get higher limits — set NYC_OPEN_DATA_TOKEN env var.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from sources.base import DataSource, Venue

log = logging.getLogger(__name__)

DATASET_ID = "43nn-pn8j"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
PAGE_SIZE = 50_000  # Socrata max per request


class DOHMHSource(DataSource):
    # Stats populated after fetch() -- available for pipeline reporting
    raw_camis_count: int = 0
    bin_dedup_count: int = 0

    @property
    def name(self) -> str:
        return "dohmh"

    @property
    def description(self) -> str:
        return "NYC DOHMH Restaurant Inspection Results"

    def fetch(self) -> list[Venue]:
        """Fetch all restaurants with valid coordinates."""
        params: dict[str, Any] = {
            "$select": (
                "camis,dba,building,street,boro,zipcode,phone,"
                "cuisine_description,grade,bin,latitude,longitude"
            ),
            # Only rows that have coordinates
            "$where": "latitude IS NOT NULL AND longitude IS NOT NULL",
            "$order": "camis",
            "$limit": PAGE_SIZE,
        }

        token = os.environ.get("NYC_OPEN_DATA_TOKEN")
        headers = {}
        if token:
            headers["X-App-Token"] = token

        # Use dict keyed by camis to deduplicate — the dataset has
        # one row per inspection, but we just want one pin per restaurant.
        seen: dict[str, Venue] = {}
        offset = 0

        while True:
            params["$offset"] = offset
            log.info("DOHMH: fetching offset %d …", offset)

            resp = requests.get(BASE_URL, params=params, headers=headers, timeout=120)
            resp.raise_for_status()
            rows = resp.json()

            if not rows:
                break

            for row in rows:
                camis = row.get("camis", "")
                if not camis or camis in seen:
                    continue

                try:
                    lat = float(row.get("latitude", 0))
                    lng = float(row.get("longitude", 0))
                except (TypeError, ValueError):
                    continue

                # Skip obviously invalid coords
                if lat == 0 or lng == 0:
                    continue

                address_parts = [
                    row.get("building", ""),
                    row.get("street", ""),
                ]
                address = " ".join(p for p in address_parts if p).strip()

                seen[camis] = Venue(
                    name=row.get("dba", "Unknown").title(),
                    lat=lat,
                    lng=lng,
                    source=self.name,
                    address=address,
                    cuisine=row.get("cuisine_description", ""),
                    borough=row.get("boro", ""),
                    phone=row.get("phone", ""),
                    grade=row.get("grade", ""),
                    zipcode=row.get("zipcode", ""),
                    tags=["restaurant"],
                    meta={"bin": row.get("bin", "")},
                )

            offset += len(rows)
            if len(rows) < PAGE_SIZE:
                break

        # Second dedup pass: same normalized name + BIN (building ID) → same
        # restaurant registered under slightly different punctuation.
        # Keep the entry with a grade, or the first one seen.
        import re
        by_name_bin: dict[tuple[str, str], Venue] = {}
        for v in seen.values():
            bldg_id = v.meta.get("bin", "")
            norm_name = re.sub(r"[^a-z0-9 ]", "", v.name.lower()).strip()
            norm_name = re.sub(r"\s+", " ", norm_name)
            key = (norm_name, bldg_id) if bldg_id else (norm_name, str(id(v)))
            if key in by_name_bin:
                existing = by_name_bin[key]
                # Prefer the one with a grade letter
                if v.grade and not existing.grade:
                    by_name_bin[key] = v
                elif v.cuisine and not existing.cuisine:
                    by_name_bin[key] = v
            else:
                by_name_bin[key] = v

        venues = list(by_name_bin.values())
        deduped = len(seen) - len(venues)
        self.raw_camis_count = len(seen)
        self.bin_dedup_count = deduped
        if deduped:
            log.info("DOHMH: deduped %d same-name-same-building entries", deduped)
        log.info("DOHMH: fetched %d unique venues", len(venues))
        return venues
