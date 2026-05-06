"""Fetch a Civix SDK dataset and preview normalized records.

This example hits Vancouver's live public open-data API.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Iterator
from typing import Final

from civix.core.quality.models.fields import FieldQuality, MappedField
from civix.domains.business_licences.models import BusinessLicence
from civix.sdk import Civix

PREVIEW_LIMIT: Final[int] = 5


def _field_qualities(record: BusinessLicence) -> Iterator[tuple[str, FieldQuality]]:
    for field_name in record.__class__.model_fields:
        value = getattr(record, field_name)

        if isinstance(value, MappedField):
            yield field_name, value.quality


def _display(value: object | None) -> str:
    if value is None:
        return "(none)"

    return str(value)


async def main() -> None:
    quality_counts: Counter[FieldQuality] = Counter()
    unmapped_counts: Counter[str] = Counter()
    preview: list[tuple[str | None, BusinessLicence]] = []
    total_records = 0

    async with Civix() as client:
        product = client.datasets.ca.business_licences.licence.vancouver
        result = await client.fetch(product)
        snapshot = result.snapshot

        print("Snapshot")
        print(f"  source: {snapshot.source_id}")
        print(f"  dataset: {snapshot.dataset_id}")
        print(f"  jurisdiction: {snapshot.jurisdiction.model_dump(mode='json')}")
        print(f"  fetched_at: {snapshot.fetched_at.isoformat()}")
        print(f"  source record count: {snapshot.record_count}")
        print()

        async for paired in result.records:
            record = paired.mapped.record
            report = paired.mapped.report

            if len(preview) < PREVIEW_LIMIT:
                preview.append((paired.raw.source_record_id, record))

            for _, quality in _field_qualities(record):
                quality_counts[quality] += 1

            unmapped_counts.update(report.unmapped_source_fields)
            total_records += 1

    print(f"Normalized records consumed: {total_records}")
    print()

    print("Preview")
    for source_record_id, record in preview:
        status = record.status.value.value if record.status.value is not None else None

        print(f"  source_record_id: {_display(source_record_id)}")
        print(f"    business_name: {_display(record.business_name.value)}")
        print(f"    licence_number: {_display(record.licence_number.value)}")
        print(f"    status: {_display(status)} ({record.status.quality.value})")
        print(f"    neighbourhood: {_display(record.neighbourhood.value)}")

    print()
    print("Mapping quality counts")
    for quality, count in quality_counts.most_common():
        print(f"  {quality.value}: {count}")

    print()
    print("Most common unmapped source fields")
    if unmapped_counts:
        for field_name, count in unmapped_counts.most_common(10):
            print(f"  {field_name}: {count}")
    else:
        print("  (none)")


if __name__ == "__main__":
    asyncio.run(main())
