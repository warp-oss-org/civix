"""Generates the trimmed Ontario EWRB fixture workbook.

Run via `python -m tests...ca.ontario_ewrb.fixtures._build_fixture` or
just `python _build_fixture.py` from this directory. The resulting
`ewrb_2024_trimmed.xlsx` is a checked-in test asset.
"""

from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

SHEET_NAME = "Sheet1"

HEADERS: tuple[str, ...] = (
    "EWRB_ID",
    "City",
    "Postal_Code",
    "PrimPropTypCalc",
    "PrimPropTypSelf",
    "Largest_PropTyp",
    "All_Prop_Types",
    "Thrd_Party_Cert",
    "WN_Sit_Elc_Int1",
    "WN_Sit_Elc_Int2",
    "WN_Sit_Gas_Int1",
    "WN_Sit_Gas_Int2",
    "WN_Sit_Gas_Int3",
    "All_Water_Int1",
    "All_Water_Int2",
    "Ind_Water_Int1",
    "Ind_Water_Int2",
    "Site_EUI1",
    "Site_EUI2",
    "Source_EUI1",
    "Source_EUI2",
    "WN_Site_EUI1",
    "WN_Site_EUI2",
    "WN_Source_EUI1",
    "WN_Source_EUI2",
    "GHG_Emiss_Int1",
    "GHG_Emiss_Int2",
    "Ener_Star_Score",
    "Ener_Star_Certs",
    "Data_Qual_Check",
    "Data_Qual_Date",
    "Post_Aug_2023_Src_Factor",
)

# Four fixture rows tuned to the Sprint 4 acceptance criteria:
#   row 1 - typical Toronto row, all intensities present, post-aug-2023 factor
#   row 2 - non-Toronto row (Ottawa), all intensities present, pre-aug-2023 factor
#   row 3 - Toronto row with `Not Available` and data quality checker not run
#   row 4 - Hamilton row with post-aug-2023 factor and `Not Available` source EUI
ROWS: tuple[tuple[object, ...], ...] = (
    (
        "100001",
        "Toronto",
        "M5V",
        "Office",
        "Office",
        "Office",
        "Office",
        "ENERGY STAR Certified 2024",
        0.45,
        11.6,
        0.21,
        0.0058,
        0.00054,
        0.81,
        0.075,
        0.62,
        0.058,
        1.32,
        37.4,
        2.41,
        68.2,
        1.36,
        38.5,
        2.48,
        70.1,
        18.7,
        1.74,
        78,
        "ENERGY STAR Certified 2024",
        "Yes",
        "2024-09-15",
        "Yes",
    ),
    (
        "100002",
        "Ottawa",
        "K1P",
        "Multifamily Housing",
        "Multifamily Housing",
        "Multifamily Housing",
        "Multifamily Housing; Retail Store",
        None,
        0.31,
        7.8,
        0.36,
        0.0102,
        0.00094,
        0.55,
        0.051,
        0.42,
        0.039,
        1.05,
        29.6,
        1.92,
        54.1,
        1.07,
        30.2,
        1.95,
        55.0,
        14.2,
        1.32,
        62,
        None,
        "Yes",
        "2024-08-22",
        "No",
    ),
    (
        "100003",
        "Toronto",
        "M4Y",
        "K-12 School",
        "K-12 School",
        "K-12 School",
        "K-12 School",
        None,
        "Not Available",
        "Not Available",
        0.18,
        0.0050,
        0.00046,
        "Not Available",
        "Not Available",
        "Not Available",
        "Not Available",
        1.18,
        33.4,
        2.05,
        58.0,
        "Not Available",
        "Not Available",
        2.10,
        59.4,
        15.9,
        1.48,
        "Not Available",
        None,
        "No",
        None,
        None,
    ),
    (
        "100004",
        "Hamilton",
        "L8P",
        "Hospital (General Medical & Surgical)",
        "Hospital (General Medical & Surgical)",
        "Hospital (General Medical & Surgical)",
        "Hospital (General Medical & Surgical)",
        None,
        0.62,
        16.0,
        0.40,
        0.0113,
        0.00105,
        0.94,
        0.087,
        0.71,
        0.066,
        1.85,
        52.4,
        "Not Available",
        "Not Available",
        1.91,
        54.0,
        "Not Available",
        "Not Available",
        24.6,
        2.29,
        55,
        None,
        "Yes",
        "2024-10-01",
        "Yes",
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

    output = Path(__file__).parent / "ewrb_2024_trimmed.xlsx"
    workbook.save(output)
    workbook.close()

    return output


if __name__ == "__main__":
    print(build())
