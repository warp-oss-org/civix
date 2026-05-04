"""Generates the trimmed LL97 CBL fixture workbook.

Run via `python -m tests.domains...nyc_ll97.fixtures._build_fixture` or
just `python _build_fixture.py` from this directory. The resulting
`cbl26_trimmed.xlsx` is a checked-in test asset.
"""

from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

SHEET_NAME = "Sustainability_CBL"

HEADERS: tuple[str, ...] = (
    "BBL",
    "BIN",
    "On LL97 CBL",
    "LL97 Compliance Pathway",
    "On LL84 CBL",
    "Required To Report Water Data From DEP",
    "On LL88 CBL",
    "On LL87",
    "DOF BBL Address",
    "DOF BBL Zip Code",
    "DOF BBL Building Count",
    "DOF BBL Gross Square Footage",
)

# Five fixture rows tuned to the LL97 test plan:
#   row 1 - covered, full pathway, single-building lot (full case)
#   row 2 - covered, no pathway code yet (minimum-valid case)
#   row 3 - covered, multi-building lot, BIN A (BIN-centered identity)
#   row 4 - covered, multi-building lot, BIN B (BIN-centered identity)
#   row 5 - not covered (NOT_COVERED lifecycle, pathway absent)
ROWS: tuple[tuple[object, ...], ...] = (
    (
        1009990001,
        1011223,
        "Y",
        "2",
        "Y",
        "N",
        "Y",
        "N",
        "123 First Avenue",
        "10003",
        1,
        50000,
    ),
    (
        1009990002,
        1011224,
        "Y",
        None,
        "Y",
        "N",
        "N",
        "N",
        "456 Second Avenue",
        "10009",
        1,
        42000,
    ),
    (
        2034560020,
        2050010,
        "Y",
        "1",
        "Y",
        "Y",
        "Y",
        "Y",
        "789 Atlantic Avenue",
        "11217",
        2,
        180000,
    ),
    (
        2034560020,
        2050011,
        "Y",
        "1",
        "Y",
        "Y",
        "Y",
        "Y",
        "789 Atlantic Avenue",
        "11217",
        2,
        180000,
    ),
    (
        3001000004,
        3001500,
        "N",
        None,
        "N",
        "N",
        "N",
        "N",
        "12 Garden Place",
        "11201",
        1,
        4500,
    ),
)


def build() -> Path:
    workbook = Workbook()
    default = workbook.active

    if default is not None:
        workbook.remove(default)

    sheet = workbook.create_sheet(SHEET_NAME)
    sheet.append(HEADERS)

    for row in ROWS:
        sheet.append(row)

    output = Path(__file__).parent / "cbl26_trimmed.xlsx"
    workbook.save(output)
    workbook.close()

    return output


if __name__ == "__main__":
    print(build())
