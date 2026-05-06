"""Hazard mitigation SDK dataset products."""

from __future__ import annotations

from civix.domains.hazard_mitigation.adapters.sources.ca import dmaf as hm_dmaf
from civix.domains.hazard_mitigation.adapters.sources.gb import fcerm as hm_fcerm
from civix.domains.hazard_mitigation.adapters.sources.us import fema_hma as hm_fema_hma
from civix.domains.hazard_mitigation.models import (
    HazardMitigationProject,
    MitigationFundingTransaction,
)
from civix.infra.sources.openfema import OpenFemaFetchConfig
from civix.sdk.datasets._helpers import ckan, product
from civix.sdk.models import CivixRuntime, DatasetProduct


def _fcerm(runtime: CivixRuntime) -> hm_fcerm.EnglandFcermFetchConfig:
    return hm_fcerm.EnglandFcermFetchConfig(client=runtime.http_client, clock=runtime.clock)


def _fema_hma_projects(runtime: CivixRuntime) -> OpenFemaFetchConfig:
    return OpenFemaFetchConfig(
        client=runtime.http_client,
        clock=runtime.clock,
        order_by=hm_fema_hma.FEMA_HMA_PROJECTS_ORDER,
    )


def _fema_hma_transactions(runtime: CivixRuntime) -> OpenFemaFetchConfig:
    return OpenFemaFetchConfig(
        client=runtime.http_client,
        clock=runtime.clock,
        order_by=hm_fema_hma.FEMA_HMA_TRANSACTIONS_ORDER,
    )


CANADA_DMAF_PROJECTS: DatasetProduct[HazardMitigationProject] = product(
    country="ca",
    domain="hazard_mitigation",
    model="project",
    slug="dmaf_projects",
    adapter_factory=lambda runtime: hm_dmaf.CanadaDmafProjectsAdapter(ckan(runtime)),
    mapper_factory=hm_dmaf.CanadaDmafProjectMapper,
)
ENGLAND_FCERM_SCHEMES: DatasetProduct[HazardMitigationProject] = product(
    country="gb",
    domain="hazard_mitigation",
    model="project",
    slug="fcerm_schemes",
    adapter_factory=lambda runtime: hm_fcerm.EnglandFcermSchemesAdapter(_fcerm(runtime)),
    mapper_factory=hm_fcerm.EnglandFcermProjectMapper,
)
FEMA_HMA_PROJECTS: DatasetProduct[HazardMitigationProject] = product(
    country="us",
    domain="hazard_mitigation",
    model="project",
    slug="fema_hma_projects",
    adapter_factory=lambda runtime: hm_fema_hma.FemaHmaProjectsAdapter(
        _fema_hma_projects(runtime)
    ),
    mapper_factory=hm_fema_hma.FemaHmaProjectMapper,
)
FEMA_HMA_TRANSACTIONS: DatasetProduct[MitigationFundingTransaction] = product(
    country="us",
    domain="hazard_mitigation",
    model="transaction",
    slug="fema_hma_transactions",
    adapter_factory=lambda runtime: hm_fema_hma.FemaHmaTransactionsAdapter(
        _fema_hma_transactions(runtime)
    ),
    mapper_factory=hm_fema_hma.FemaHmaTransactionMapper,
)

__all__ = [
    "CANADA_DMAF_PROJECTS",
    "ENGLAND_FCERM_SCHEMES",
    "FEMA_HMA_PROJECTS",
    "FEMA_HMA_TRANSACTIONS",
]
