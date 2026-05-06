from __future__ import annotations

from collections.abc import Iterator
from dataclasses import fields, is_dataclass
from typing import Any, assert_type

import pytest

from civix import Civix as ExportedCivix
from civix.core.pipeline import PipelineResult
from civix.domains.building_energy_emissions.models import BuildingEnergySubject
from civix.domains.business_licences.models import BusinessLicence
from civix.domains.hazard_mitigation.models import HazardMitigationProject
from civix.domains.hazard_risk.models import HazardRiskZone
from civix.domains.mobility_observations.models import MobilitySpeedObservation
from civix.domains.transportation_safety.models.collision import TrafficCollision
from civix.sdk import Civix
from civix.sdk.datasets import DATASETS, DatasetsNamespace
from civix.sdk.models import DatasetProduct


def _products(node: object) -> Iterator[DatasetProduct[Any]]:
    if isinstance(node, DatasetProduct):
        yield node

        return

    if not is_dataclass(node):
        return

    for field in fields(node):
        yield from _products(getattr(node, field.name))


def _read_attr(node: object, name: str) -> object:
    return getattr(node, name)


def test_civix_is_exported_from_package_root() -> None:
    assert ExportedCivix is Civix


def test_datasets_package_import_remains_public() -> None:
    assert isinstance(DATASETS, DatasetsNamespace)


def test_representative_dataset_paths_resolve() -> None:
    client = Civix()

    products = [
        client.datasets.us.business_licences.licence.nyc,
        client.datasets.us.transportation_safety.collision.nyc_crashes,
        client.datasets.gb.transportation_safety.collision.stats19_collisions,
        client.datasets.fr.transportation_safety.collision.baac_characteristics,
        client.datasets.us.mobility_observations.speed.nyc_traffic_speeds,
        client.datasets.us.hazard_risk.zone.fema_nfhl_flood_hazard_zones,
        client.datasets.us.hazard_mitigation.project.fema_hma_projects,
        client.datasets.us.building_energy_emissions.subject.nyc_ll84,
    ]

    assert all(isinstance(product, DatasetProduct) for product in products)
    assert products[0].path == "us.business_licences.licence.nyc"
    assert products[2].path == "gb.transportation_safety.collision.stats19_collisions"
    assert products[3].path == "fr.transportation_safety.collision.baac_characteristics"


def test_country_namespaces_do_not_expose_other_country_products() -> None:
    client = Civix()

    with pytest.raises(AttributeError):
        _read_attr(client.datasets.ca.business_licences.licence, "nyc")

    with pytest.raises(AttributeError):
        _read_attr(client.datasets.us.business_licences.licence, "toronto")

    with pytest.raises(AttributeError):
        _read_attr(client.datasets.ca.mobility_observations.site, "dft_count_points")

    with pytest.raises(AttributeError):
        _read_attr(client.datasets.gb.transportation_safety.collision, "baac_characteristics")


def test_exposed_products_match_country_namespace() -> None:
    client = Civix()

    assert all(product.country == "ca" for product in _products(client.datasets.ca))
    assert all(product.country == "fr" for product in _products(client.datasets.fr))
    assert all(product.country == "gb" for product in _products(client.datasets.gb))
    assert all(product.country == "us" for product in _products(client.datasets.us))


def test_representative_fetch_result_type_is_preserved() -> None:
    async def example(client: Civix) -> None:
        business_result = await client.fetch(client.datasets.us.business_licences.licence.nyc)
        transportation_result = await client.fetch(
            client.datasets.us.transportation_safety.collision.nyc_crashes
        )
        mobility_result = await client.fetch(
            client.datasets.us.mobility_observations.speed.nyc_traffic_speeds
        )
        hazard_risk_result = await client.fetch(
            client.datasets.us.hazard_risk.zone.fema_nfhl_flood_hazard_zones
        )
        hazard_mitigation_result = await client.fetch(
            client.datasets.us.hazard_mitigation.project.fema_hma_projects
        )
        building_result = await client.fetch(
            client.datasets.us.building_energy_emissions.subject.nyc_ll84
        )

        assert_type(business_result, PipelineResult[BusinessLicence])
        assert_type(transportation_result, PipelineResult[TrafficCollision])
        assert_type(mobility_result, PipelineResult[MobilitySpeedObservation])
        assert_type(hazard_risk_result, PipelineResult[HazardRiskZone])
        assert_type(hazard_mitigation_result, PipelineResult[HazardMitigationProject])
        assert_type(building_result, PipelineResult[BuildingEnergySubject])

    assert callable(example)
