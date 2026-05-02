"""Great Britain STATS19 source constants."""

from __future__ import annotations

from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId

SOURCE_ID: Final[SourceId] = SourceId("dft-open-data")
STATS19_RELEASE: Final[str] = "2024-final"
STATS19_COLLISIONS_DATASET_ID: Final[DatasetId] = DatasetId("stats19-collisions-2024")
STATS19_VEHICLES_DATASET_ID: Final[DatasetId] = DatasetId("stats19-vehicles-2024")
STATS19_CASUALTIES_DATASET_ID: Final[DatasetId] = DatasetId("stats19-casualties-2024")
STATS19_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="GB")

STATS19_SOURCE_SCOPE: Final[str] = (
    "Personal injury road collisions in Great Britain that were reported to police "
    "and recorded through STATS19."
)
STATS19_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "Final 2024 is the latest full validated year for this fixture-backed slice.",
    "Provisional first-half 2025 files are unvalidated and may contain duplicate casualties.",
    "November 2025 DfT revisions corrected junction_detail and noted an unresolved "
    "2024 vehicle_location_restricted_lane issue.",
    "Sensitive fields such as contributory factors are not included in the open data.",
)
