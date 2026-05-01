"""Business licences domain — canonical model lives in `models.licence`.

Source slices (`adapters.sources.<country>.<city>`) are imported on demand
from their source package paths to keep this top-level import cheap.
"""

from civix.domains.business_licences.models.licence import BusinessLicence, LicenceStatus

__all__ = ["BusinessLicence", "LicenceStatus"]
