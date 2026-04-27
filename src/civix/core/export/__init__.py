"""Format-agnostic export contracts.

Concrete writers (JSON, Parquet, etc.) live in `civix.infra.exporters`.
This package holds only the manifest model that every exporter produces.
"""

from civix.core.export.manifest import ExportedFile, ExportManifest, MappingSummary

__all__ = [
    "ExportManifest",
    "ExportedFile",
    "MappingSummary",
]
