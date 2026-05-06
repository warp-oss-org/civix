"""Public SDK dataset namespace tree."""

from __future__ import annotations

from dataclasses import dataclass

from civix.domains.building_energy_emissions.models import (
    BuildingComplianceCase,
    BuildingEnergyReport,
    BuildingEnergySubject,
    BuildingMetricValue,
)
from civix.domains.business_licences.models import BusinessLicence
from civix.domains.hazard_mitigation.models import (
    HazardMitigationProject,
    MitigationFundingTransaction,
)
from civix.domains.hazard_risk.models import HazardRiskArea, HazardRiskScore, HazardRiskZone
from civix.domains.mobility_observations.models import (
    MobilityCountObservation,
    MobilityObservationSite,
    MobilitySpeedObservation,
)
from civix.domains.transportation_safety.models.collision import TrafficCollision
from civix.domains.transportation_safety.models.person import CollisionPerson
from civix.domains.transportation_safety.models.vehicle import CollisionVehicle
from civix.sdk.datasets import building_energy_emissions as bee
from civix.sdk.datasets import business_licences as bl
from civix.sdk.datasets import hazard_mitigation as hm
from civix.sdk.datasets import hazard_risk as hr
from civix.sdk.datasets import mobility_observations as mo
from civix.sdk.datasets import transportation_safety as ts
from civix.sdk.models import DatasetProduct


@dataclass(frozen=True, slots=True)
class CaBusinessLicencesLicenceNamespace:
    calgary: DatasetProduct[BusinessLicence] = bl.CALGARY_BUSINESS_LICENCES
    edmonton: DatasetProduct[BusinessLicence] = bl.EDMONTON_BUSINESS_LICENCES
    toronto: DatasetProduct[BusinessLicence] = bl.TORONTO_BUSINESS_LICENCES
    vancouver: DatasetProduct[BusinessLicence] = bl.VANCOUVER_BUSINESS_LICENCES
    vancouver_2013_to_2024: DatasetProduct[BusinessLicence] = (
        bl.VANCOUVER_BUSINESS_LICENCES_2013_TO_2024
    )
    vancouver_1997_to_2012: DatasetProduct[BusinessLicence] = (
        bl.VANCOUVER_BUSINESS_LICENCES_1997_TO_2012
    )


@dataclass(frozen=True, slots=True)
class UsBusinessLicencesLicenceNamespace:
    nyc: DatasetProduct[BusinessLicence] = bl.NYC_BUSINESS_LICENCES


@dataclass(frozen=True, slots=True)
class CaBusinessLicencesNamespace:
    licence: CaBusinessLicencesLicenceNamespace = CaBusinessLicencesLicenceNamespace()


@dataclass(frozen=True, slots=True)
class UsBusinessLicencesNamespace:
    licence: UsBusinessLicencesLicenceNamespace = UsBusinessLicencesLicenceNamespace()


@dataclass(frozen=True, slots=True)
class UsTransportationSafetyCollisionNamespace:
    chicago_crashes: DatasetProduct[TrafficCollision] = ts.CHICAGO_CRASHES
    nyc_crashes: DatasetProduct[TrafficCollision] = ts.NYC_CRASHES


@dataclass(frozen=True, slots=True)
class UsTransportationSafetyVehicleNamespace:
    chicago_vehicles: DatasetProduct[CollisionVehicle] = ts.CHICAGO_VEHICLES
    nyc_vehicles: DatasetProduct[CollisionVehicle] = ts.NYC_VEHICLES


@dataclass(frozen=True, slots=True)
class UsTransportationSafetyPersonNamespace:
    chicago_people: DatasetProduct[CollisionPerson] = ts.CHICAGO_PEOPLE
    nyc_persons: DatasetProduct[CollisionPerson] = ts.NYC_PERSONS


@dataclass(frozen=True, slots=True)
class UsTransportationSafetyNamespace:
    collision: UsTransportationSafetyCollisionNamespace = UsTransportationSafetyCollisionNamespace()
    vehicle: UsTransportationSafetyVehicleNamespace = UsTransportationSafetyVehicleNamespace()
    person: UsTransportationSafetyPersonNamespace = UsTransportationSafetyPersonNamespace()


@dataclass(frozen=True, slots=True)
class GbTransportationSafetyCollisionNamespace:
    stats19_collisions: DatasetProduct[TrafficCollision] = ts.STATS19_COLLISIONS


@dataclass(frozen=True, slots=True)
class GbTransportationSafetyVehicleNamespace:
    stats19_vehicles: DatasetProduct[CollisionVehicle] = ts.STATS19_VEHICLES


@dataclass(frozen=True, slots=True)
class GbTransportationSafetyCasualtyNamespace:
    stats19_casualties: DatasetProduct[CollisionPerson] = ts.STATS19_CASUALTIES


@dataclass(frozen=True, slots=True)
class GbTransportationSafetyNamespace:
    collision: GbTransportationSafetyCollisionNamespace = GbTransportationSafetyCollisionNamespace()
    vehicle: GbTransportationSafetyVehicleNamespace = GbTransportationSafetyVehicleNamespace()
    casualty: GbTransportationSafetyCasualtyNamespace = GbTransportationSafetyCasualtyNamespace()


@dataclass(frozen=True, slots=True)
class FrTransportationSafetyCollisionNamespace:
    baac_characteristics: DatasetProduct[TrafficCollision] = ts.BAAC_CHARACTERISTICS


@dataclass(frozen=True, slots=True)
class FrTransportationSafetyVehicleNamespace:
    baac_vehicles: DatasetProduct[CollisionVehicle] = ts.BAAC_VEHICLES


@dataclass(frozen=True, slots=True)
class FrTransportationSafetyUserNamespace:
    baac_users: DatasetProduct[CollisionPerson] = ts.BAAC_USERS


@dataclass(frozen=True, slots=True)
class FrTransportationSafetyNamespace:
    collision: FrTransportationSafetyCollisionNamespace = FrTransportationSafetyCollisionNamespace()
    vehicle: FrTransportationSafetyVehicleNamespace = FrTransportationSafetyVehicleNamespace()
    user: FrTransportationSafetyUserNamespace = FrTransportationSafetyUserNamespace()


@dataclass(frozen=True, slots=True)
class CaMobilityObservationsSiteNamespace:
    toronto_bicycle_counter_locations: DatasetProduct[MobilityObservationSite] = (
        mo.TORONTO_BICYCLE_COUNTER_LOCATIONS
    )
    toronto_turning_movement_counts: DatasetProduct[MobilityObservationSite] = (
        mo.TORONTO_TMC_SUMMARY
    )


@dataclass(frozen=True, slots=True)
class CaMobilityObservationsCountNamespace:
    toronto_bicycle_counter_15min: DatasetProduct[MobilityCountObservation] = (
        mo.TORONTO_BICYCLE_COUNTER_15MIN
    )
    toronto_turning_movement_counts: DatasetProduct[tuple[MobilityCountObservation, ...]] = (
        mo.TORONTO_TMC_RAW_COUNTS
    )


@dataclass(frozen=True, slots=True)
class CaMobilityObservationsNamespace:
    site: CaMobilityObservationsSiteNamespace = CaMobilityObservationsSiteNamespace()
    count: CaMobilityObservationsCountNamespace = CaMobilityObservationsCountNamespace()


@dataclass(frozen=True, slots=True)
class FrMobilityObservationsSiteNamespace:
    tmja_road_traffic: DatasetProduct[MobilityObservationSite] = mo.FR_TMJA_ROAD_SEGMENT_SITES


@dataclass(frozen=True, slots=True)
class FrMobilityObservationsCountNamespace:
    tmja_road_traffic: DatasetProduct[MobilityCountObservation] = mo.FR_TMJA_COUNTS


@dataclass(frozen=True, slots=True)
class FrMobilityObservationsNamespace:
    site: FrMobilityObservationsSiteNamespace = FrMobilityObservationsSiteNamespace()
    count: FrMobilityObservationsCountNamespace = FrMobilityObservationsCountNamespace()


@dataclass(frozen=True, slots=True)
class GbMobilityObservationsSiteNamespace:
    dft_count_points: DatasetProduct[MobilityObservationSite] = mo.GB_DFT_COUNT_POINTS


@dataclass(frozen=True, slots=True)
class GbMobilityObservationsCountNamespace:
    dft_aadf_by_direction: DatasetProduct[tuple[MobilityCountObservation, ...]] = (
        mo.GB_DFT_AADF_BY_DIRECTION
    )


@dataclass(frozen=True, slots=True)
class GbMobilityObservationsNamespace:
    site: GbMobilityObservationsSiteNamespace = GbMobilityObservationsSiteNamespace()
    count: GbMobilityObservationsCountNamespace = GbMobilityObservationsCountNamespace()


@dataclass(frozen=True, slots=True)
class UsMobilityObservationsSiteNamespace:
    chicago_traffic_tracker_regions: DatasetProduct[MobilityObservationSite] = (
        mo.CHICAGO_TRAFFIC_TRACKER_REGION_SITES
    )
    chicago_traffic_tracker_segments: DatasetProduct[MobilityObservationSite] = (
        mo.CHICAGO_TRAFFIC_TRACKER_SEGMENT_SITES
    )
    nyc_bicycle_pedestrian_sensors: DatasetProduct[MobilityObservationSite] = (
        mo.NYC_BICYCLE_PEDESTRIAN_SENSORS
    )
    nyc_traffic_volume_counts: DatasetProduct[MobilityObservationSite] = (
        mo.NYC_TRAFFIC_VOLUME_SITES
    )


@dataclass(frozen=True, slots=True)
class UsMobilityObservationsSpeedNamespace:
    chicago_traffic_tracker_regions: DatasetProduct[MobilitySpeedObservation] = (
        mo.CHICAGO_TRAFFIC_TRACKER_REGION_SPEEDS
    )
    chicago_traffic_tracker_segments: DatasetProduct[MobilitySpeedObservation] = (
        mo.CHICAGO_TRAFFIC_TRACKER_SEGMENT_SPEEDS
    )
    nyc_traffic_speeds: DatasetProduct[MobilitySpeedObservation] = mo.NYC_TRAFFIC_SPEEDS


@dataclass(frozen=True, slots=True)
class UsMobilityObservationsCountNamespace:
    nyc_bicycle_pedestrian_counts: DatasetProduct[MobilityCountObservation] = (
        mo.NYC_BICYCLE_PEDESTRIAN_COUNTS
    )
    nyc_traffic_volume_counts: DatasetProduct[MobilityCountObservation] = (
        mo.NYC_TRAFFIC_VOLUME_COUNTS
    )


@dataclass(frozen=True, slots=True)
class UsMobilityObservationsNamespace:
    site: UsMobilityObservationsSiteNamespace = UsMobilityObservationsSiteNamespace()
    speed: UsMobilityObservationsSpeedNamespace = UsMobilityObservationsSpeedNamespace()
    count: UsMobilityObservationsCountNamespace = UsMobilityObservationsCountNamespace()


@dataclass(frozen=True, slots=True)
class UsHazardRiskAreaNamespace:
    fema_nri_tracts: DatasetProduct[HazardRiskArea] = hr.FEMA_NRI_AREAS


@dataclass(frozen=True, slots=True)
class UsHazardRiskScoreNamespace:
    fema_nri_tracts: DatasetProduct[tuple[HazardRiskScore, ...]] = hr.FEMA_NRI_SCORES


@dataclass(frozen=True, slots=True)
class UsHazardRiskZoneNamespace:
    fema_nfhl_flood_hazard_zones: DatasetProduct[HazardRiskZone] = (
        hr.FEMA_NFHL_FLOOD_HAZARD_ZONES
    )


@dataclass(frozen=True, slots=True)
class UsHazardRiskNamespace:
    area: UsHazardRiskAreaNamespace = UsHazardRiskAreaNamespace()
    score: UsHazardRiskScoreNamespace = UsHazardRiskScoreNamespace()
    zone: UsHazardRiskZoneNamespace = UsHazardRiskZoneNamespace()


@dataclass(frozen=True, slots=True)
class FrHazardRiskAreaNamespace:
    georisques_pprn: DatasetProduct[HazardRiskArea] = hr.GEORISQUES_PPRN_AREAS


@dataclass(frozen=True, slots=True)
class FrHazardRiskZoneNamespace:
    georisques_pprn: DatasetProduct[HazardRiskZone] = hr.GEORISQUES_PPRN_ZONES


@dataclass(frozen=True, slots=True)
class FrHazardRiskNamespace:
    area: FrHazardRiskAreaNamespace = FrHazardRiskAreaNamespace()
    zone: FrHazardRiskZoneNamespace = FrHazardRiskZoneNamespace()


@dataclass(frozen=True, slots=True)
class CaHazardMitigationProjectNamespace:
    dmaf_projects: DatasetProduct[HazardMitigationProject] = hm.CANADA_DMAF_PROJECTS


@dataclass(frozen=True, slots=True)
class CaHazardMitigationNamespace:
    project: CaHazardMitigationProjectNamespace = CaHazardMitigationProjectNamespace()


@dataclass(frozen=True, slots=True)
class GbHazardMitigationProjectNamespace:
    fcerm_schemes: DatasetProduct[HazardMitigationProject] = hm.ENGLAND_FCERM_SCHEMES


@dataclass(frozen=True, slots=True)
class GbHazardMitigationNamespace:
    project: GbHazardMitigationProjectNamespace = GbHazardMitigationProjectNamespace()


@dataclass(frozen=True, slots=True)
class UsHazardMitigationProjectNamespace:
    fema_hma_projects: DatasetProduct[HazardMitigationProject] = hm.FEMA_HMA_PROJECTS


@dataclass(frozen=True, slots=True)
class UsHazardMitigationTransactionNamespace:
    fema_hma_transactions: DatasetProduct[MitigationFundingTransaction] = (
        hm.FEMA_HMA_TRANSACTIONS
    )


@dataclass(frozen=True, slots=True)
class UsHazardMitigationNamespace:
    project: UsHazardMitigationProjectNamespace = UsHazardMitigationProjectNamespace()
    transaction: UsHazardMitigationTransactionNamespace = (
        UsHazardMitigationTransactionNamespace()
    )


@dataclass(frozen=True, slots=True)
class CaBuildingEnergyEmissionsSubjectNamespace:
    ontario_ewrb: DatasetProduct[BuildingEnergySubject] = bee.ONTARIO_EWRB_SUBJECTS


@dataclass(frozen=True, slots=True)
class CaBuildingEnergyEmissionsReportNamespace:
    ontario_ewrb: DatasetProduct[BuildingEnergyReport] = bee.ONTARIO_EWRB_REPORTS


@dataclass(frozen=True, slots=True)
class CaBuildingEnergyEmissionsMetricNamespace:
    ontario_ewrb: DatasetProduct[tuple[BuildingMetricValue, ...]] = bee.ONTARIO_EWRB_METRICS


@dataclass(frozen=True, slots=True)
class CaBuildingEnergyEmissionsNamespace:
    subject: CaBuildingEnergyEmissionsSubjectNamespace = (
        CaBuildingEnergyEmissionsSubjectNamespace()
    )
    report: CaBuildingEnergyEmissionsReportNamespace = CaBuildingEnergyEmissionsReportNamespace()
    metric: CaBuildingEnergyEmissionsMetricNamespace = CaBuildingEnergyEmissionsMetricNamespace()


@dataclass(frozen=True, slots=True)
class UsBuildingEnergyEmissionsSubjectNamespace:
    nyc_ll84: DatasetProduct[BuildingEnergySubject] = bee.NYC_LL84_SUBJECTS
    nyc_ll97: DatasetProduct[BuildingEnergySubject] = bee.NYC_LL97_SUBJECTS


@dataclass(frozen=True, slots=True)
class UsBuildingEnergyEmissionsReportNamespace:
    nyc_ll84: DatasetProduct[BuildingEnergyReport] = bee.NYC_LL84_REPORTS


@dataclass(frozen=True, slots=True)
class UsBuildingEnergyEmissionsMetricNamespace:
    nyc_ll84: DatasetProduct[tuple[BuildingMetricValue, ...]] = bee.NYC_LL84_METRICS


@dataclass(frozen=True, slots=True)
class UsBuildingEnergyEmissionsCaseNamespace:
    nyc_ll97: DatasetProduct[BuildingComplianceCase] = bee.NYC_LL97_CASES


@dataclass(frozen=True, slots=True)
class UsBuildingEnergyEmissionsNamespace:
    subject: UsBuildingEnergyEmissionsSubjectNamespace = (
        UsBuildingEnergyEmissionsSubjectNamespace()
    )
    report: UsBuildingEnergyEmissionsReportNamespace = UsBuildingEnergyEmissionsReportNamespace()
    metric: UsBuildingEnergyEmissionsMetricNamespace = UsBuildingEnergyEmissionsMetricNamespace()
    case: UsBuildingEnergyEmissionsCaseNamespace = UsBuildingEnergyEmissionsCaseNamespace()


@dataclass(frozen=True, slots=True)
class CanadaNamespace:
    business_licences: CaBusinessLicencesNamespace = CaBusinessLicencesNamespace()
    mobility_observations: CaMobilityObservationsNamespace = CaMobilityObservationsNamespace()
    hazard_mitigation: CaHazardMitigationNamespace = CaHazardMitigationNamespace()
    building_energy_emissions: CaBuildingEnergyEmissionsNamespace = (
        CaBuildingEnergyEmissionsNamespace()
    )


@dataclass(frozen=True, slots=True)
class FranceNamespace:
    transportation_safety: FrTransportationSafetyNamespace = FrTransportationSafetyNamespace()
    mobility_observations: FrMobilityObservationsNamespace = FrMobilityObservationsNamespace()
    hazard_risk: FrHazardRiskNamespace = FrHazardRiskNamespace()


@dataclass(frozen=True, slots=True)
class GreatBritainNamespace:
    transportation_safety: GbTransportationSafetyNamespace = GbTransportationSafetyNamespace()
    mobility_observations: GbMobilityObservationsNamespace = GbMobilityObservationsNamespace()
    hazard_mitigation: GbHazardMitigationNamespace = GbHazardMitigationNamespace()


@dataclass(frozen=True, slots=True)
class UnitedStatesNamespace:
    business_licences: UsBusinessLicencesNamespace = UsBusinessLicencesNamespace()
    transportation_safety: UsTransportationSafetyNamespace = UsTransportationSafetyNamespace()
    mobility_observations: UsMobilityObservationsNamespace = UsMobilityObservationsNamespace()
    hazard_risk: UsHazardRiskNamespace = UsHazardRiskNamespace()
    hazard_mitigation: UsHazardMitigationNamespace = UsHazardMitigationNamespace()
    building_energy_emissions: UsBuildingEnergyEmissionsNamespace = (
        UsBuildingEnergyEmissionsNamespace()
    )


@dataclass(frozen=True, slots=True)
class DatasetsNamespace:
    ca: CanadaNamespace = CanadaNamespace()
    fr: FranceNamespace = FranceNamespace()
    gb: GreatBritainNamespace = GreatBritainNamespace()
    us: UnitedStatesNamespace = UnitedStatesNamespace()


DATASETS = DatasetsNamespace()

__all__ = ["DATASETS", "DatasetsNamespace"]
