"""Organization value models for hazard mitigation records."""

from pydantic import BaseModel

from civix.core.taxonomy.models.category import CategoryRef
from civix.domains.hazard_mitigation.models.common import (
    FROZEN_MODEL,
    MitigationOrganizationRole,
    NonEmptyString,
)


class MitigationOrganization(BaseModel):
    """An organization named on a mitigation project or funding record."""

    model_config = FROZEN_MODEL

    role: MitigationOrganizationRole
    name: NonEmptyString
    source_role: CategoryRef | None = None
