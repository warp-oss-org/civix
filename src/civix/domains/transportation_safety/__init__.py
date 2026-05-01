"""Transportation safety domain — canonical models live in `models/`.

The domain centers on `TrafficCollision`, with companion records for the
people and vehicles involved (`CollisionPerson`, `CollisionVehicle`) and
the value types they reference. Source slices will be imported on demand
from their own package paths once the first slice lands.
"""

from civix.domains.transportation_safety.models.collision import (
    CollisionSeverity,
    TrafficCollision,
)
from civix.domains.transportation_safety.models.parties import (
    ContributingFactor,
    RoadUserRole,
)
from civix.domains.transportation_safety.models.person import (
    CollisionPerson,
    InjuryOutcome,
)
from civix.domains.transportation_safety.models.road import (
    SpeedLimit,
    SpeedLimitUnit,
)
from civix.domains.transportation_safety.models.time import (
    OccurrenceTime,
    OccurrenceTimePrecision,
    OccurrenceTimezoneStatus,
)
from civix.domains.transportation_safety.models.vehicle import (
    CollisionVehicle,
    VehicleCategory,
)

__all__ = [
    "CollisionPerson",
    "CollisionSeverity",
    "CollisionVehicle",
    "ContributingFactor",
    "InjuryOutcome",
    "OccurrenceTime",
    "OccurrenceTimePrecision",
    "OccurrenceTimezoneStatus",
    "RoadUserRole",
    "SpeedLimit",
    "SpeedLimitUnit",
    "TrafficCollision",
    "VehicleCategory",
]
