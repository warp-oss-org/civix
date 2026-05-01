"""Business licence domain model.

The normalized shape every business-licence source maps into. Source
quirks (Vancouver's column names, Toronto's date formats, etc.) belong
in source adapters; this layer is source-agnostic.

Civic fields are wrapped in `MappedField[T]`. Missing values are
expressed through the wrapper's `quality` (NOT_PROVIDED, REDACTED, etc.)
rather than by making the field optional, so the record's contract is
uniform across mappers.
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from civix.core.provenance.models.provenance import ProvenanceRef
from civix.core.quality.models.fields import MappedField
from civix.core.spatial.models.location import Address, Coordinate
from civix.core.taxonomy.models.category import CategoryRef

_FROZEN_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True)


class LicenceStatus(StrEnum):
    """Normalized business licence status.

    Membership is sized to cross-jurisdiction reality. The terminal
    states are deliberately distinct because issuers distinguish them:

    - `CANCELLED`   — closed voluntarily by the licensee
    - `REVOKED`     — terminated by regulatory action of the issuer
    - `SUSPENDED`   — temporarily paused by regulatory action
    - `SURRENDERED` — voluntarily handed back (not the same as cancelled
                      in jurisdictions like NYC that record both)
    - `EXPIRED`     — lapsed at end of term, no renewal performed
    - `RENEWAL_DUE` — past renewal due-date but not yet treated as expired

    Collapsing these to one `UNKNOWN` would violate civix's
    "missing/redacted/withheld are not equivalent" principle applied to
    status semantics. Mappers produce `UNKNOWN` with
    `MappedField.quality=INFERRED` for source values that don't fit, as
    a clean signal that the enum needs to grow.
    """

    ACTIVE = "active"
    PENDING = "pending"
    INACTIVE = "inactive"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    REVOKED = "revoked"
    SUSPENDED = "suspended"
    SURRENDERED = "surrendered"
    RENEWAL_DUE = "renewal_due"
    UNKNOWN = "unknown"


class BusinessLicence(BaseModel):
    """A normalized business licence record."""

    model_config = _FROZEN_MODEL

    provenance: ProvenanceRef
    business_name: MappedField[str]
    licence_number: MappedField[str]
    status: MappedField[LicenceStatus]
    category: MappedField[CategoryRef]
    issued_at: MappedField[date]
    expires_at: MappedField[date]
    address: MappedField[Address]
    coordinate: MappedField[Coordinate]
    neighbourhood: MappedField[str]
