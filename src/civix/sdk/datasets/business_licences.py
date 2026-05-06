"""Business licence SDK dataset products."""

from __future__ import annotations

from civix.core.identity.models.identifiers import DatasetId, Jurisdiction
from civix.domains.business_licences.adapters.sources.ca import calgary as bl_calgary
from civix.domains.business_licences.adapters.sources.ca import edmonton as bl_edmonton
from civix.domains.business_licences.adapters.sources.ca import toronto as bl_toronto
from civix.domains.business_licences.adapters.sources.ca import vancouver as bl_vancouver
from civix.domains.business_licences.adapters.sources.us import nyc as bl_nyc
from civix.domains.business_licences.models import BusinessLicence
from civix.sdk.datasets._helpers import product
from civix.sdk.models import DatasetProduct

CALGARY_BUSINESS_LICENCES: DatasetProduct[BusinessLicence] = product(
    country="ca",
    domain="business_licences",
    model="licence",
    slug="calgary",
    adapter_factory=lambda runtime: bl_calgary.CalgaryBusinessLicencesAdapter(
        dataset_id=DatasetId("vdjc-pybd"),
        jurisdiction=Jurisdiction(country="CA", region="AB", locality="Calgary"),
        client=runtime.http_client,
        clock=runtime.clock,
    ),
    mapper_factory=bl_calgary.CalgaryBusinessLicencesMapper,
)
EDMONTON_BUSINESS_LICENCES: DatasetProduct[BusinessLicence] = product(
    country="ca",
    domain="business_licences",
    model="licence",
    slug="edmonton",
    adapter_factory=lambda runtime: bl_edmonton.EdmontonBusinessLicencesAdapter(
        dataset_id=DatasetId("qhi4-bdpu"),
        jurisdiction=Jurisdiction(country="CA", region="AB", locality="Edmonton"),
        client=runtime.http_client,
        clock=runtime.clock,
    ),
    mapper_factory=bl_edmonton.EdmontonBusinessLicencesMapper,
)
TORONTO_BUSINESS_LICENCES: DatasetProduct[BusinessLicence] = product(
    country="ca",
    domain="business_licences",
    model="licence",
    slug="toronto",
    adapter_factory=lambda runtime: bl_toronto.TorontoBusinessLicencesAdapter(
        dataset_id=DatasetId("municipal-licensing-and-standards-business-licences-and-permits"),
        jurisdiction=Jurisdiction(country="CA", region="ON", locality="Toronto"),
        client=runtime.http_client,
        clock=runtime.clock,
    ),
    mapper_factory=bl_toronto.TorontoBusinessLicencesMapper,
)
VANCOUVER_BUSINESS_LICENCES: DatasetProduct[BusinessLicence] = product(
    country="ca",
    domain="business_licences",
    model="licence",
    slug="vancouver",
    adapter_factory=lambda runtime: bl_vancouver.VancouverBusinessLicencesAdapter(
        dataset_id=DatasetId("business-licences"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        client=runtime.http_client,
        clock=runtime.clock,
    ),
    mapper_factory=bl_vancouver.VancouverBusinessLicencesMapper,
)
VANCOUVER_BUSINESS_LICENCES_2013_TO_2024: DatasetProduct[BusinessLicence] = product(
    country="ca",
    domain="business_licences",
    model="licence",
    slug="vancouver_2013_to_2024",
    adapter_factory=lambda runtime: bl_vancouver.VancouverBusinessLicencesAdapter(
        dataset_id=DatasetId("business-licences-2013-to-2024"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        client=runtime.http_client,
        clock=runtime.clock,
    ),
    mapper_factory=bl_vancouver.VancouverBusinessLicencesMapper,
)
VANCOUVER_BUSINESS_LICENCES_1997_TO_2012: DatasetProduct[BusinessLicence] = product(
    country="ca",
    domain="business_licences",
    model="licence",
    slug="vancouver_1997_to_2012",
    adapter_factory=lambda runtime: bl_vancouver.VancouverBusinessLicencesAdapter(
        dataset_id=DatasetId("business-licences-1997-to-2012"),
        jurisdiction=Jurisdiction(country="CA", region="BC", locality="Vancouver"),
        client=runtime.http_client,
        clock=runtime.clock,
    ),
    mapper_factory=bl_vancouver.VancouverBusinessLicencesMapper,
)
NYC_BUSINESS_LICENCES: DatasetProduct[BusinessLicence] = product(
    country="us",
    domain="business_licences",
    model="licence",
    slug="nyc",
    adapter_factory=lambda runtime: bl_nyc.NycBusinessLicencesAdapter(
        dataset_id=DatasetId("w7w3-xahh"),
        jurisdiction=Jurisdiction(country="US", region="NY", locality="New York"),
        client=runtime.http_client,
        clock=runtime.clock,
    ),
    mapper_factory=bl_nyc.NycBusinessLicencesMapper,
)

__all__ = [
    "CALGARY_BUSINESS_LICENCES",
    "EDMONTON_BUSINESS_LICENCES",
    "NYC_BUSINESS_LICENCES",
    "TORONTO_BUSINESS_LICENCES",
    "VANCOUVER_BUSINESS_LICENCES",
    "VANCOUVER_BUSINESS_LICENCES_1997_TO_2012",
    "VANCOUVER_BUSINESS_LICENCES_2013_TO_2024",
]
