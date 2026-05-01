"""Validation tests for the export manifest models."""

from __future__ import annotations

import pytest

from civix.core.export.models.manifest import ExportedFile, MappingSummary


class TestExportedFile:
    def test_invalid_sha_rejected(self) -> None:
        with pytest.raises(ValueError, match="String should match pattern"):
            ExportedFile(filename="x", sha256="not-a-hash", byte_count=0)

    def test_negative_byte_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="greater than or equal to"):
            ExportedFile(filename="x", sha256="0" * 64, byte_count=-1)


class TestMappingSummary:
    def test_defaults_are_empty(self) -> None:
        summary = MappingSummary()

        assert summary.quality_counts == {}
        assert summary.unmapped_source_fields_total == 0
        assert summary.conflicts_total == 0
