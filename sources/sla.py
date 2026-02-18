"""
NYS Liquor Authority Current Active Licenses — via LAMP ArcGIS FeatureServer.

Source: https://lamp.sla.ny.gov/
Backend: ArcGIS FeatureServer at services8.arcgis.com (public, no auth needed)
Filtered to NYC (counties: NEW YORK, KINGS, QUEENS, BRONX, RICHMOND).

Provides bars, clubs, restaurants, and liquor/grocery stores with active
liquor licenses — great for cross-referencing with DOHMH to find bars.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from sources.base import DataSource, Venue

log = logging.getLogger(__name__)

# ArcGIS FeatureServer backing the LAMP web app
BASE_URL = (
    "https://services8.arcgis.com/kHNnQD79LvY0XnKy/arcgis/rest/services"
    "/ActiveLicensesV3/FeatureServer/0/query"
)
PAGE_SIZE = 16_000  # maxRecordCount for this layer

# Map county names → borough names for consistency with DOHMH
COUNTY_TO_BORO = {
    "New York": "MANHATTAN",
    "Kings": "BROOKLYN",
    "Queens": "QUEENS",
    "Bronx": "BRONX",
    "Richmond": "STATEN ISLAND",
}

# Fields to request (keeps response smaller)
OUT_FIELDS = (
    "PremiseName,PremiseDBA,Description,PremiseAddress1,PremiseCity,"
    "CountyName,LicenseCla,LicensePermitID,PremiseZIP,Latitude,Longitude"
)


class SLASource(DataSource):
    @property
    def name(self) -> str:
        return "sla"

    @property
    def description(self) -> str:
        return "NYS Liquor Authority Active Licenses (NYC)"

    def fetch(self) -> list[Venue]:
        """Fetch all active liquor licenses in NYC from LAMP ArcGIS."""
        county_list = ",".join(f"'{c}'" for c in COUNTY_TO_BORO)
        where = (
            f"CountyName IN ({county_list}) "
            "AND Latitude IS NOT NULL AND Longitude IS NOT NULL"
        )

        venues: list[Venue] = []
        offset = 0

        while True:
            log.info("SLA: fetching offset %d …", offset)

            params: dict[str, Any] = {
                "f": "json",
                "where": where,
                "outFields": OUT_FIELDS,
                "returnGeometry": "false",   # lat/lng in attributes already
                "resultOffset": offset,
                "resultRecordCount": PAGE_SIZE,
            }

            resp = requests.get(BASE_URL, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if not features:
                break

            for feat in features:
                attrs = feat.get("attributes", {})

                try:
                    lat = float(attrs.get("Latitude", 0))
                    lng = float(attrs.get("Longitude", 0))
                except (TypeError, ValueError):
                    continue

                if lat == 0 or lng == 0:
                    continue

                county = attrs.get("CountyName", "")
                borough = COUNTY_TO_BORO.get(county, county.upper())

                license_desc = attrs.get("Description", "")
                tags = ["liquor_license"]
                desc_lower = license_desc.lower()
                if any(w in desc_lower for w in ("bar", "tavern", "club", "cabaret")):
                    tags.append("bar")
                elif any(w in desc_lower for w in ("restaurant", "eating")):
                    tags.append("restaurant")
                elif "grocery" in desc_lower or "drug" in desc_lower:
                    tags.append("retail")

                display_name = attrs.get("PremiseDBA") or attrs.get("PremiseName") or "Unknown"

                venues.append(
                    Venue(
                        name=display_name.title(),
                        lat=lat,
                        lng=lng,
                        source=self.name,
                        address=attrs.get("PremiseAddress1", ""),
                        borough=borough,
                        zipcode=attrs.get("PremiseZIP", ""),
                        tags=tags,
                        meta={
                            "license_type": license_desc,
                            "license_id": attrs.get("LicensePermitID", ""),
                        },
                    )
                )

            offset += len(features)

            # ArcGIS signals "no more pages" when exceededTransferLimit is absent/false
            if not data.get("exceededTransferLimit", False):
                break

        log.info("SLA: fetched %d venues", len(venues))
        return venues
