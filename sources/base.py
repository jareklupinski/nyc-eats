"""
Base class for NYC Eats data sources.

To add a new data source:
  1. Create a new .py file in sources/
  2. Subclass DataSource
  3. Implement fetch() to return a list of Venue dicts
  4. The build script auto-discovers all DataSource subclasses

Each venue dict should have at minimum:
  - name: str
  - lat: float
  - lng: float
  - source: str  (identifier for this data source)

Optional but encouraged:
  - address: str
  - cuisine: str
  - borough: str
  - phone: str
  - grade: str
  - tags: list[str]  (e.g. ["bar", "restaurant"])
  - meta: dict       (anything source-specific)
"""

from __future__ import annotations

import abc
import importlib
import logging
import pkgutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class Venue:
    """A single venue (restaurant, bar, etc.)."""

    name: str
    lat: float
    lng: float
    source: str
    address: str = ""
    cuisine: str = ""
    borough: str = ""
    phone: str = ""
    grade: str = ""
    zipcode: str = ""
    opened: str = ""  # ISO date string (YYYY-MM-DD) — earliest known date
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = {
            "name": self.name,
            "lat": self.lat,
            "lng": self.lng,
            "source": self.source,
        }
        # Only include non-empty optional fields to keep JSON compact
        if self.address:
            d["address"] = self.address
        if self.cuisine:
            d["cuisine"] = self.cuisine
        if self.borough:
            d["borough"] = self.borough
        if self.phone:
            d["phone"] = self.phone
        if self.grade:
            d["grade"] = self.grade
        if self.zipcode:
            d["zipcode"] = self.zipcode
        if self.opened:
            d["opened"] = self.opened
        if self.tags:
            d["tags"] = self.tags
        if self.meta:
            d["meta"] = self.meta
        return d


class DataSource(abc.ABC):
    """Abstract base class for a data source."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Short identifier for this source, e.g. 'dohmh', 'sla'."""
        ...

    @property
    @abc.abstractmethod
    def description(self) -> str:
        """Human-readable description shown in the UI."""
        ...

    @abc.abstractmethod
    def fetch(self) -> list[Venue]:
        """Fetch and return all venues from this source.

        Should handle its own pagination, retries, etc.
        Return as many venues as possible — no deduplication needed here.
        """
        ...


def discover_sources() -> list[DataSource]:
    """Auto-discover all DataSource subclasses in the sources/ package."""
    # Import all modules in this package
    package_dir = Path(__file__).parent
    for info in pkgutil.iter_modules([str(package_dir)]):
        if info.name == "base":
            continue
        importlib.import_module(f"sources.{info.name}")

    # Instantiate all concrete subclasses
    sources = []
    for cls in DataSource.__subclasses__():
        try:
            sources.append(cls())
            log.info("Discovered source: %s", cls.__name__)
        except Exception:
            log.exception("Failed to instantiate source %s", cls.__name__)
    return sources
