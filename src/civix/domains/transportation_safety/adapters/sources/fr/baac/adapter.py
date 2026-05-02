"""France BAAC / ONISR source constants."""

from __future__ import annotations

from typing import Final

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction, SourceId

SOURCE_ID: Final[SourceId] = SourceId("onisr-open-data")
BAAC_SOURCE_YEAR: Final[str] = "2024"
BAAC_RELEASE: Final[str] = "2024-data-gouv-2025-12-29"
BAAC_DATASET_LAST_UPDATE: Final[str] = "2025-12-29T09:29:20.308000+00:00"
BAAC_JURISDICTION: Final[Jurisdiction] = Jurisdiction(country="FR")

BAAC_CHARACTERISTICS_DATASET_ID: Final[DatasetId] = DatasetId("baac-caracteristiques-2024")
BAAC_LOCATIONS_DATASET_ID: Final[DatasetId] = DatasetId("baac-lieux-2024")
BAAC_VEHICLES_DATASET_ID: Final[DatasetId] = DatasetId("baac-vehicules-2024")
BAAC_USERS_DATASET_ID: Final[DatasetId] = DatasetId("baac-usagers-2024")

BAAC_CHARACTERISTICS_RESOURCE_ID: Final[str] = "83f0fb0e-e0ef-47fe-93dd-9aaee851674a"
BAAC_LOCATIONS_RESOURCE_ID: Final[str] = "228b3cda-fdfb-4677-bd54-ab2107028d2d"
BAAC_VEHICLES_RESOURCE_ID: Final[str] = "fd30513c-6b11-4a56-b6dc-5ac87728794b"
BAAC_USERS_RESOURCE_ID: Final[str] = "f57b1f58-386d-4048-8f78-2ebe435df868"

BAAC_CHARACTERISTICS_RESOURCE_TITLE: Final[str] = "Caract_2024.csv"
BAAC_LOCATIONS_RESOURCE_TITLE: Final[str] = "Lieux_2024.csv"
BAAC_VEHICLES_RESOURCE_TITLE: Final[str] = "Vehicules_2024.csv"
BAAC_USERS_RESOURCE_TITLE: Final[str] = "Usagers_2024.csv"

BAAC_RESOURCE_LAST_MODIFIED: Final[dict[DatasetId, str]] = {
    BAAC_CHARACTERISTICS_DATASET_ID: "2025-10-21T11:59:01.081000+00:00",
    BAAC_LOCATIONS_DATASET_ID: "2025-10-21T11:58:13.699000+00:00",
    BAAC_VEHICLES_DATASET_ID: "2025-12-29T09:29:20.308000+00:00",
    BAAC_USERS_DATASET_ID: "2025-10-21T11:56:56.552000+00:00",
}

BAAC_SOURCE_SCOPE: Final[str] = (
    "Injury road traffic collisions on roads open to public traffic in France, "
    "recorded through the BAAC national file and published by ONISR/data.gouv.fr."
)
BAAC_LICENCE: Final[str] = "Licence Ouverte / Open Licence"
BAAC_RELEASE_CAVEATS: Final[tuple[str, ...]] = (
    "BAAC open data covers injury collisions and does not include property-damage-only "
    "collisions.",
    "Hospitalised-injury qualification changed from 2018 and the indicator is not "
    "labelled by the official statistics authority from 2019.",
    "Some privacy-sensitive investigation, user, vehicle, and behaviour details are "
    "omitted from the public extract.",
)
