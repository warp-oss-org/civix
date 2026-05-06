"""Building energy and emissions SDK dataset products."""

from __future__ import annotations

from civix.domains.building_energy_emissions.adapters.sources.ca import (
    ontario_ewrb as bee_ontario_ewrb,
)
from civix.domains.building_energy_emissions.adapters.sources.us import (
    nyc_ll84 as bee_nyc_ll84,
)
from civix.domains.building_energy_emissions.adapters.sources.us import (
    nyc_ll97 as bee_nyc_ll97,
)
from civix.domains.building_energy_emissions.models import (
    BuildingComplianceCase,
    BuildingEnergyReport,
    BuildingEnergySubject,
    BuildingMetricValue,
)
from civix.sdk.datasets._helpers import product
from civix.sdk.models import CivixRuntime, DatasetProduct


def _nyc_ll84(runtime: CivixRuntime) -> bee_nyc_ll84.NycLl84FetchConfig:
    return bee_nyc_ll84.NycLl84FetchConfig(client=runtime.http_client, clock=runtime.clock)


def _nyc_ll97(runtime: CivixRuntime) -> bee_nyc_ll97.NycLl97FetchConfig:
    return bee_nyc_ll97.NycLl97FetchConfig(client=runtime.http_client, clock=runtime.clock)


def _ontario_ewrb(runtime: CivixRuntime) -> bee_ontario_ewrb.OntarioEwrbFetchConfig:
    return bee_ontario_ewrb.OntarioEwrbFetchConfig(
        client=runtime.http_client,
        clock=runtime.clock,
    )


NYC_LL84_SUBJECTS: DatasetProduct[BuildingEnergySubject] = product(
    country="us",
    domain="building_energy_emissions",
    model="subject",
    slug="nyc_ll84",
    adapter_factory=lambda runtime: bee_nyc_ll84.NycLl84Adapter(_nyc_ll84(runtime)),
    mapper_factory=bee_nyc_ll84.NycLl84SubjectMapper,
)
NYC_LL84_REPORTS: DatasetProduct[BuildingEnergyReport] = product(
    country="us",
    domain="building_energy_emissions",
    model="report",
    slug="nyc_ll84",
    adapter_factory=lambda runtime: bee_nyc_ll84.NycLl84Adapter(_nyc_ll84(runtime)),
    mapper_factory=bee_nyc_ll84.NycLl84ReportMapper,
)
NYC_LL84_METRICS: DatasetProduct[tuple[BuildingMetricValue, ...]] = product(
    country="us",
    domain="building_energy_emissions",
    model="metric",
    slug="nyc_ll84",
    adapter_factory=lambda runtime: bee_nyc_ll84.NycLl84Adapter(_nyc_ll84(runtime)),
    mapper_factory=bee_nyc_ll84.NycLl84MetricsMapper,
)
NYC_LL97_SUBJECTS: DatasetProduct[BuildingEnergySubject] = product(
    country="us",
    domain="building_energy_emissions",
    model="subject",
    slug="nyc_ll97",
    adapter_factory=lambda runtime: bee_nyc_ll97.NycLl97Adapter(_nyc_ll97(runtime)),
    mapper_factory=bee_nyc_ll97.NycLl97SubjectMapper,
)
NYC_LL97_CASES: DatasetProduct[BuildingComplianceCase] = product(
    country="us",
    domain="building_energy_emissions",
    model="case",
    slug="nyc_ll97",
    adapter_factory=lambda runtime: bee_nyc_ll97.NycLl97Adapter(_nyc_ll97(runtime)),
    mapper_factory=bee_nyc_ll97.NycLl97CaseMapper,
)
ONTARIO_EWRB_SUBJECTS: DatasetProduct[BuildingEnergySubject] = product(
    country="ca",
    domain="building_energy_emissions",
    model="subject",
    slug="ontario_ewrb",
    adapter_factory=lambda runtime: bee_ontario_ewrb.OntarioEwrbAdapter(_ontario_ewrb(runtime)),
    mapper_factory=bee_ontario_ewrb.OntarioEwrbSubjectMapper,
)
ONTARIO_EWRB_REPORTS: DatasetProduct[BuildingEnergyReport] = product(
    country="ca",
    domain="building_energy_emissions",
    model="report",
    slug="ontario_ewrb",
    adapter_factory=lambda runtime: bee_ontario_ewrb.OntarioEwrbAdapter(_ontario_ewrb(runtime)),
    mapper_factory=bee_ontario_ewrb.OntarioEwrbReportMapper,
)
ONTARIO_EWRB_METRICS: DatasetProduct[tuple[BuildingMetricValue, ...]] = product(
    country="ca",
    domain="building_energy_emissions",
    model="metric",
    slug="ontario_ewrb",
    adapter_factory=lambda runtime: bee_ontario_ewrb.OntarioEwrbAdapter(_ontario_ewrb(runtime)),
    mapper_factory=bee_ontario_ewrb.OntarioEwrbMetricsMapper,
)

__all__ = [
    "NYC_LL84_METRICS",
    "NYC_LL84_REPORTS",
    "NYC_LL84_SUBJECTS",
    "NYC_LL97_CASES",
    "NYC_LL97_SUBJECTS",
    "ONTARIO_EWRB_METRICS",
    "ONTARIO_EWRB_REPORTS",
    "ONTARIO_EWRB_SUBJECTS",
]
