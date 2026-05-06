"""Transportation safety SDK dataset products."""

from __future__ import annotations

from civix.domains.transportation_safety.adapters.sources.fr import baac as ts_baac
from civix.domains.transportation_safety.adapters.sources.gb import stats19 as ts_stats19
from civix.domains.transportation_safety.adapters.sources.us import (
    chicago_crashes as ts_chicago_crashes,
)
from civix.domains.transportation_safety.adapters.sources.us import (
    chicago_people as ts_chicago_people,
)
from civix.domains.transportation_safety.adapters.sources.us import (
    chicago_vehicles as ts_chicago_vehicles,
)
from civix.domains.transportation_safety.adapters.sources.us import nyc_crashes as ts_nyc_crashes
from civix.domains.transportation_safety.adapters.sources.us import nyc_persons as ts_nyc_persons
from civix.domains.transportation_safety.adapters.sources.us import nyc_vehicles as ts_nyc_vehicles
from civix.domains.transportation_safety.models.collision import TrafficCollision
from civix.domains.transportation_safety.models.person import CollisionPerson
from civix.domains.transportation_safety.models.vehicle import CollisionVehicle
from civix.sdk.datasets._helpers import product, socrata
from civix.sdk.models import CivixRuntime, DatasetProduct


def _stats19(runtime: CivixRuntime) -> ts_stats19.Stats19FetchConfig:
    return ts_stats19.Stats19FetchConfig(client=runtime.http_client, clock=runtime.clock)


def _baac(runtime: CivixRuntime) -> ts_baac.BaacFetchConfig:
    return ts_baac.BaacFetchConfig(client=runtime.http_client, clock=runtime.clock)


CHICAGO_CRASHES: DatasetProduct[TrafficCollision] = product(
    country="us",
    domain="transportation_safety",
    model="collision",
    slug="chicago_crashes",
    adapter_factory=lambda runtime: ts_chicago_crashes.ChicagoCrashesAdapter(socrata(runtime)),
    mapper_factory=ts_chicago_crashes.ChicagoCrashesMapper,
)
CHICAGO_PEOPLE: DatasetProduct[CollisionPerson] = product(
    country="us",
    domain="transportation_safety",
    model="person",
    slug="chicago_people",
    adapter_factory=lambda runtime: ts_chicago_people.ChicagoPeopleAdapter(socrata(runtime)),
    mapper_factory=ts_chicago_people.ChicagoPeopleMapper,
)
CHICAGO_VEHICLES: DatasetProduct[CollisionVehicle] = product(
    country="us",
    domain="transportation_safety",
    model="vehicle",
    slug="chicago_vehicles",
    adapter_factory=lambda runtime: ts_chicago_vehicles.ChicagoVehiclesAdapter(socrata(runtime)),
    mapper_factory=ts_chicago_vehicles.ChicagoVehiclesMapper,
)
NYC_CRASHES: DatasetProduct[TrafficCollision] = product(
    country="us",
    domain="transportation_safety",
    model="collision",
    slug="nyc_crashes",
    adapter_factory=lambda runtime: ts_nyc_crashes.NycCrashesAdapter(socrata(runtime)),
    mapper_factory=ts_nyc_crashes.NycCrashesMapper,
)
NYC_PERSONS: DatasetProduct[CollisionPerson] = product(
    country="us",
    domain="transportation_safety",
    model="person",
    slug="nyc_persons",
    adapter_factory=lambda runtime: ts_nyc_persons.NycPersonsAdapter(socrata(runtime)),
    mapper_factory=ts_nyc_persons.NycPersonsMapper,
)
NYC_VEHICLES: DatasetProduct[CollisionVehicle] = product(
    country="us",
    domain="transportation_safety",
    model="vehicle",
    slug="nyc_vehicles",
    adapter_factory=lambda runtime: ts_nyc_vehicles.NycVehiclesAdapter(socrata(runtime)),
    mapper_factory=ts_nyc_vehicles.NycVehiclesMapper,
)
STATS19_COLLISIONS: DatasetProduct[TrafficCollision] = product(
    country="gb",
    domain="transportation_safety",
    model="collision",
    slug="stats19_collisions",
    adapter_factory=lambda runtime: ts_stats19.Stats19CollisionsAdapter(_stats19(runtime)),
    mapper_factory=ts_stats19.Stats19CollisionMapper,
)
STATS19_VEHICLES: DatasetProduct[CollisionVehicle] = product(
    country="gb",
    domain="transportation_safety",
    model="vehicle",
    slug="stats19_vehicles",
    adapter_factory=lambda runtime: ts_stats19.Stats19VehiclesAdapter(_stats19(runtime)),
    mapper_factory=ts_stats19.Stats19VehicleMapper,
)
STATS19_CASUALTIES: DatasetProduct[CollisionPerson] = product(
    country="gb",
    domain="transportation_safety",
    model="casualty",
    slug="stats19_casualties",
    adapter_factory=lambda runtime: ts_stats19.Stats19CasualtiesAdapter(_stats19(runtime)),
    mapper_factory=ts_stats19.Stats19CasualtyMapper,
)
BAAC_CHARACTERISTICS: DatasetProduct[TrafficCollision] = product(
    country="fr",
    domain="transportation_safety",
    model="collision",
    slug="baac_characteristics",
    adapter_factory=lambda runtime: ts_baac.BaacCharacteristicsAdapter(_baac(runtime)),
    mapper_factory=ts_baac.BaacCollisionMapper,
)
BAAC_VEHICLES: DatasetProduct[CollisionVehicle] = product(
    country="fr",
    domain="transportation_safety",
    model="vehicle",
    slug="baac_vehicles",
    adapter_factory=lambda runtime: ts_baac.BaacVehiclesAdapter(_baac(runtime)),
    mapper_factory=ts_baac.BaacVehicleMapper,
)
BAAC_USERS: DatasetProduct[CollisionPerson] = product(
    country="fr",
    domain="transportation_safety",
    model="user",
    slug="baac_users",
    adapter_factory=lambda runtime: ts_baac.BaacUsersAdapter(_baac(runtime)),
    mapper_factory=ts_baac.BaacUserMapper,
)

__all__ = [
    "BAAC_CHARACTERISTICS",
    "BAAC_USERS",
    "BAAC_VEHICLES",
    "CHICAGO_CRASHES",
    "CHICAGO_PEOPLE",
    "CHICAGO_VEHICLES",
    "NYC_CRASHES",
    "NYC_PERSONS",
    "NYC_VEHICLES",
    "STATS19_CASUALTIES",
    "STATS19_COLLISIONS",
    "STATS19_VEHICLES",
]
