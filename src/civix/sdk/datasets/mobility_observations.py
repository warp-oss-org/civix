"""Mobility observation SDK dataset products."""

from __future__ import annotations

from civix.domains.mobility_observations.adapters.sources.ca import (
    toronto_bicycle_counters as mo_toronto_bicycle,
)
from civix.domains.mobility_observations.adapters.sources.ca import (
    toronto_turning_movement_counts as mo_toronto_tmc,
)
from civix.domains.mobility_observations.adapters.sources.fr import (
    tmja_road_traffic as mo_fr_tmja,
)
from civix.domains.mobility_observations.adapters.sources.gb import (
    road_traffic_counts as mo_gb_road_traffic,
)
from civix.domains.mobility_observations.adapters.sources.us import (
    chicago_traffic_tracker_regions as mo_chicago_regions,
)
from civix.domains.mobility_observations.adapters.sources.us import (
    chicago_traffic_tracker_segments as mo_chicago_segments,
)
from civix.domains.mobility_observations.adapters.sources.us import (
    nyc_bicycle_pedestrian_counts as mo_nyc_bike_ped,
)
from civix.domains.mobility_observations.adapters.sources.us import (
    nyc_traffic_speeds as mo_nyc_speeds,
)
from civix.domains.mobility_observations.adapters.sources.us import (
    nyc_traffic_volume_counts as mo_nyc_volumes,
)
from civix.domains.mobility_observations.models import (
    MobilityCountObservation,
    MobilityObservationSite,
    MobilitySpeedObservation,
)
from civix.sdk.datasets._helpers import ckan, product, socrata
from civix.sdk.models import CivixRuntime, DatasetProduct


def _fr_tmja(runtime: CivixRuntime) -> mo_fr_tmja.FrTmjaRoadTrafficFetchConfig:
    return mo_fr_tmja.FrTmjaRoadTrafficFetchConfig(
        client=runtime.http_client,
        clock=runtime.clock,
    )


def _dft(runtime: CivixRuntime) -> mo_gb_road_traffic.DftFetchConfig:
    return mo_gb_road_traffic.DftFetchConfig(client=runtime.http_client, clock=runtime.clock)


CHICAGO_TRAFFIC_TRACKER_REGION_SITES: DatasetProduct[MobilityObservationSite] = product(
    country="us",
    domain="mobility_observations",
    model="site",
    slug="chicago_traffic_tracker_regions",
    adapter_factory=lambda runtime: mo_chicago_regions.ChicagoTrafficTrackerRegionsAdapter(
        socrata(runtime)
    ),
    mapper_factory=mo_chicago_regions.ChicagoTrafficTrackerRegionSiteMapper,
)
CHICAGO_TRAFFIC_TRACKER_REGION_SPEEDS: DatasetProduct[MobilitySpeedObservation] = product(
    country="us",
    domain="mobility_observations",
    model="speed",
    slug="chicago_traffic_tracker_regions",
    adapter_factory=lambda runtime: mo_chicago_regions.ChicagoTrafficTrackerRegionsAdapter(
        socrata(runtime)
    ),
    mapper_factory=mo_chicago_regions.ChicagoTrafficTrackerRegionSpeedMapper,
)
CHICAGO_TRAFFIC_TRACKER_SEGMENT_SITES: DatasetProduct[MobilityObservationSite] = product(
    country="us",
    domain="mobility_observations",
    model="site",
    slug="chicago_traffic_tracker_segments",
    adapter_factory=lambda runtime: mo_chicago_segments.ChicagoTrafficTrackerSegmentsAdapter(
        socrata(runtime)
    ),
    mapper_factory=mo_chicago_segments.ChicagoTrafficTrackerSegmentSiteMapper,
)
CHICAGO_TRAFFIC_TRACKER_SEGMENT_SPEEDS: DatasetProduct[MobilitySpeedObservation] = product(
    country="us",
    domain="mobility_observations",
    model="speed",
    slug="chicago_traffic_tracker_segments",
    adapter_factory=lambda runtime: mo_chicago_segments.ChicagoTrafficTrackerSegmentsAdapter(
        socrata(runtime)
    ),
    mapper_factory=mo_chicago_segments.ChicagoTrafficTrackerSegmentSpeedMapper,
)
NYC_BICYCLE_PEDESTRIAN_COUNTS: DatasetProduct[MobilityCountObservation] = product(
    country="us",
    domain="mobility_observations",
    model="count",
    slug="nyc_bicycle_pedestrian_counts",
    adapter_factory=lambda runtime: mo_nyc_bike_ped.NycBicyclePedestrianCountsAdapter(
        socrata(runtime)
    ),
    mapper_factory=mo_nyc_bike_ped.NycBicyclePedestrianCountMapper,
)
NYC_BICYCLE_PEDESTRIAN_SENSORS: DatasetProduct[MobilityObservationSite] = product(
    country="us",
    domain="mobility_observations",
    model="site",
    slug="nyc_bicycle_pedestrian_sensors",
    adapter_factory=lambda runtime: mo_nyc_bike_ped.NycBicyclePedestrianSensorsAdapter(
        socrata(runtime)
    ),
    mapper_factory=mo_nyc_bike_ped.NycBicyclePedestrianSensorMapper,
)
NYC_TRAFFIC_SPEEDS: DatasetProduct[MobilitySpeedObservation] = product(
    country="us",
    domain="mobility_observations",
    model="speed",
    slug="nyc_traffic_speeds",
    adapter_factory=lambda runtime: mo_nyc_speeds.NycTrafficSpeedsAdapter(socrata(runtime)),
    mapper_factory=mo_nyc_speeds.NycTrafficSpeedsMapper,
)
NYC_TRAFFIC_VOLUME_COUNTS: DatasetProduct[MobilityCountObservation] = product(
    country="us",
    domain="mobility_observations",
    model="count",
    slug="nyc_traffic_volume_counts",
    adapter_factory=lambda runtime: mo_nyc_volumes.NycTrafficVolumeCountsAdapter(
        socrata(runtime)
    ),
    mapper_factory=mo_nyc_volumes.NycTrafficVolumeCountMapper,
)
NYC_TRAFFIC_VOLUME_SITES: DatasetProduct[MobilityObservationSite] = product(
    country="us",
    domain="mobility_observations",
    model="site",
    slug="nyc_traffic_volume_counts",
    adapter_factory=lambda runtime: mo_nyc_volumes.NycTrafficVolumeCountsAdapter(
        socrata(runtime)
    ),
    mapper_factory=mo_nyc_volumes.NycTrafficVolumeSiteMapper,
)
TORONTO_BICYCLE_COUNTER_LOCATIONS: DatasetProduct[MobilityObservationSite] = product(
    country="ca",
    domain="mobility_observations",
    model="site",
    slug="toronto_bicycle_counter_locations",
    adapter_factory=lambda runtime: mo_toronto_bicycle.TorontoBicycleCounterLocationsAdapter(
        ckan(runtime)
    ),
    mapper_factory=mo_toronto_bicycle.TorontoBicycleCounterSiteMapper,
)
TORONTO_BICYCLE_COUNTER_15MIN: DatasetProduct[MobilityCountObservation] = product(
    country="ca",
    domain="mobility_observations",
    model="count",
    slug="toronto_bicycle_counter_15min",
    adapter_factory=lambda runtime: mo_toronto_bicycle.TorontoBicycleCounter15MinAdapter(
        ckan(runtime)
    ),
    mapper_factory=mo_toronto_bicycle.TorontoBicycleCounter15MinMapper,
)
TORONTO_TMC_SUMMARY: DatasetProduct[MobilityObservationSite] = product(
    country="ca",
    domain="mobility_observations",
    model="site",
    slug="toronto_turning_movement_counts",
    adapter_factory=lambda runtime: mo_toronto_tmc.TorontoTmcSummaryAdapter(ckan(runtime)),
    mapper_factory=mo_toronto_tmc.TorontoTmcSiteMapper,
)
TORONTO_TMC_RAW_COUNTS: DatasetProduct[tuple[MobilityCountObservation, ...]] = product(
    country="ca",
    domain="mobility_observations",
    model="count",
    slug="toronto_turning_movement_counts",
    adapter_factory=lambda runtime: mo_toronto_tmc.TorontoTmcRawCountsAdapter(ckan(runtime)),
    mapper_factory=mo_toronto_tmc.TorontoTmcRawCountMapper,
)
FR_TMJA_ROAD_SEGMENT_SITES: DatasetProduct[MobilityObservationSite] = product(
    country="fr",
    domain="mobility_observations",
    model="site",
    slug="tmja_road_traffic",
    adapter_factory=lambda runtime: mo_fr_tmja.FrTmjaRoadTrafficAdapter(_fr_tmja(runtime)),
    mapper_factory=mo_fr_tmja.FrTmjaRoadSegmentSiteMapper,
)
FR_TMJA_COUNTS: DatasetProduct[MobilityCountObservation] = product(
    country="fr",
    domain="mobility_observations",
    model="count",
    slug="tmja_road_traffic",
    adapter_factory=lambda runtime: mo_fr_tmja.FrTmjaRoadTrafficAdapter(_fr_tmja(runtime)),
    mapper_factory=mo_fr_tmja.FrTmjaCountMapper,
)
GB_DFT_COUNT_POINTS: DatasetProduct[MobilityObservationSite] = product(
    country="gb",
    domain="mobility_observations",
    model="site",
    slug="dft_count_points",
    adapter_factory=lambda runtime: mo_gb_road_traffic.GbDftCountPointsAdapter(_dft(runtime)),
    mapper_factory=mo_gb_road_traffic.GbDftCountPointSiteMapper,
)
GB_DFT_AADF_BY_DIRECTION: DatasetProduct[tuple[MobilityCountObservation, ...]] = product(
    country="gb",
    domain="mobility_observations",
    model="count",
    slug="dft_aadf_by_direction",
    adapter_factory=lambda runtime: mo_gb_road_traffic.GbDftAadfByDirectionAdapter(_dft(runtime)),
    mapper_factory=mo_gb_road_traffic.GbDftAadfCountMapper,
)

__all__ = [
    "CHICAGO_TRAFFIC_TRACKER_REGION_SITES",
    "CHICAGO_TRAFFIC_TRACKER_REGION_SPEEDS",
    "CHICAGO_TRAFFIC_TRACKER_SEGMENT_SITES",
    "CHICAGO_TRAFFIC_TRACKER_SEGMENT_SPEEDS",
    "FR_TMJA_COUNTS",
    "FR_TMJA_ROAD_SEGMENT_SITES",
    "GB_DFT_AADF_BY_DIRECTION",
    "GB_DFT_COUNT_POINTS",
    "NYC_BICYCLE_PEDESTRIAN_COUNTS",
    "NYC_BICYCLE_PEDESTRIAN_SENSORS",
    "NYC_TRAFFIC_SPEEDS",
    "NYC_TRAFFIC_VOLUME_COUNTS",
    "NYC_TRAFFIC_VOLUME_SITES",
    "TORONTO_BICYCLE_COUNTER_15MIN",
    "TORONTO_BICYCLE_COUNTER_LOCATIONS",
    "TORONTO_TMC_RAW_COUNTS",
    "TORONTO_TMC_SUMMARY",
]
