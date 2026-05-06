"""Hazard risk SDK dataset products."""

from __future__ import annotations

from civix.domains.hazard_risk.adapters.sources.fr import georisques_pprn as hr_georisques
from civix.domains.hazard_risk.adapters.sources.us import fema_nfhl as hr_fema_nfhl
from civix.domains.hazard_risk.adapters.sources.us import fema_nri as hr_fema_nri
from civix.domains.hazard_risk.models import HazardRiskArea, HazardRiskScore, HazardRiskZone
from civix.sdk.datasets._helpers import product
from civix.sdk.models import CivixRuntime, DatasetProduct


def _fema_nri(runtime: CivixRuntime) -> hr_fema_nri.FemaNriTractsFetchConfig:
    return hr_fema_nri.FemaNriTractsFetchConfig(
        client=runtime.http_client,
        clock=runtime.clock,
    )


def _fema_nfhl(runtime: CivixRuntime) -> hr_fema_nfhl.FemaNfhlFloodHazardZonesFetchConfig:
    return hr_fema_nfhl.FemaNfhlFloodHazardZonesFetchConfig(
        client=runtime.http_client,
        clock=runtime.clock,
    )


def _georisques(runtime: CivixRuntime) -> hr_georisques.GeorisquesPprnFetchConfig:
    return hr_georisques.GeorisquesPprnFetchConfig(
        client=runtime.http_client,
        clock=runtime.clock,
    )


FEMA_NRI_AREAS: DatasetProduct[HazardRiskArea] = product(
    country="us",
    domain="hazard_risk",
    model="area",
    slug="fema_nri_tracts",
    adapter_factory=lambda runtime: hr_fema_nri.FemaNriTractsAdapter(_fema_nri(runtime)),
    mapper_factory=hr_fema_nri.FemaNriAreaMapper,
)
FEMA_NRI_SCORES: DatasetProduct[tuple[HazardRiskScore, ...]] = product(
    country="us",
    domain="hazard_risk",
    model="score",
    slug="fema_nri_tracts",
    adapter_factory=lambda runtime: hr_fema_nri.FemaNriTractsAdapter(_fema_nri(runtime)),
    mapper_factory=hr_fema_nri.FemaNriScoresMapper,
)
FEMA_NFHL_FLOOD_HAZARD_ZONES: DatasetProduct[HazardRiskZone] = product(
    country="us",
    domain="hazard_risk",
    model="zone",
    slug="fema_nfhl_flood_hazard_zones",
    adapter_factory=lambda runtime: hr_fema_nfhl.FemaNfhlFloodHazardZonesAdapter(
        _fema_nfhl(runtime)
    ),
    mapper_factory=hr_fema_nfhl.FemaNfhlZoneMapper,
)
GEORISQUES_PPRN_AREAS: DatasetProduct[HazardRiskArea] = product(
    country="fr",
    domain="hazard_risk",
    model="area",
    slug="georisques_pprn",
    adapter_factory=lambda runtime: hr_georisques.GeorisquesPprnAdapter(_georisques(runtime)),
    mapper_factory=hr_georisques.GeorisquesPprnAreaMapper,
)
GEORISQUES_PPRN_ZONES: DatasetProduct[HazardRiskZone] = product(
    country="fr",
    domain="hazard_risk",
    model="zone",
    slug="georisques_pprn",
    adapter_factory=lambda runtime: hr_georisques.GeorisquesPprnAdapter(_georisques(runtime)),
    mapper_factory=hr_georisques.GeorisquesPprnZoneMapper,
)

__all__ = [
    "FEMA_NFHL_FLOOD_HAZARD_ZONES",
    "FEMA_NRI_AREAS",
    "FEMA_NRI_SCORES",
    "GEORISQUES_PPRN_AREAS",
    "GEORISQUES_PPRN_ZONES",
]
