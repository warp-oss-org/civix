"""Source-field schema and taxonomy constants for FEMA HMA fixture rows."""

from __future__ import annotations

from typing import Final

from civix.core.drift import JsonFieldKind, SchemaFieldSpec, SourceSchemaSpec, TaxonomySpec

FEMA_HMA_SCHEMA_VERSION: Final[str] = "2026-05-02"

FEMA_HMA_PROJECTS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="openfema-hma-projects-raw",
    version=FEMA_HMA_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="projectIdentifier", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="programArea", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="programFy", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="region", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)),
        SchemaFieldSpec(name="state", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="stateNumberCode", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="county", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="countyCode", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="disasterNumber", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="projectCounties", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="projectType", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="status", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="recipient", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="recipientTribalIndicator", kinds=(JsonFieldKind.BOOLEAN,), nullable=True
        ),
        SchemaFieldSpec(name="subrecipient", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="subrecipientTribalIndicator",
            kinds=(JsonFieldKind.BOOLEAN,),
            nullable=True,
        ),
        SchemaFieldSpec(name="dataSource", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(name="dateApproved", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="dateClosed", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="dateInitiallyApproved", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="projectAmount", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="initialObligationDate", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="initialObligationAmount", kinds=(JsonFieldKind.NUMBER,), nullable=True
        ),
        SchemaFieldSpec(name="federalShareObligated", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(
            name="subrecipientAdminCostAmt", kinds=(JsonFieldKind.NUMBER,), nullable=True
        ),
        SchemaFieldSpec(name="srmcObligatedAmt", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="recipientAdminCostAmt", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="costSharePercentage", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="benefitCostRatio", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="netValueBenefits", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(name="numberOfFinalProperties", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="numberOfProperties", kinds=(JsonFieldKind.NUMBER,)),
        SchemaFieldSpec(name="id", kinds=(JsonFieldKind.STRING,)),
    ),
)
FEMA_HMA_TRANSACTIONS_SCHEMA: Final[SourceSchemaSpec] = SourceSchemaSpec(
    spec_id="openfema-hma-financial-transactions-raw",
    version=FEMA_HMA_SCHEMA_VERSION,
    fields=(
        SchemaFieldSpec(name="projectIdentifier", kinds=(JsonFieldKind.STRING,)),
        SchemaFieldSpec(
            name="transactionIdentifier", kinds=(JsonFieldKind.NUMBER, JsonFieldKind.STRING)
        ),
        SchemaFieldSpec(name="transactionDate", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="commitmentIdentifier", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="accsLine", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(name="fundCode", kinds=(JsonFieldKind.STRING,), nullable=True),
        SchemaFieldSpec(
            name="federalShareProjectCostAmt", kinds=(JsonFieldKind.NUMBER,), nullable=True
        ),
        SchemaFieldSpec(name="recipientAdminCostAmt", kinds=(JsonFieldKind.NUMBER,), nullable=True),
        SchemaFieldSpec(
            name="subrecipientAdminCostAmt",
            kinds=(JsonFieldKind.NUMBER,),
            nullable=True,
        ),
        SchemaFieldSpec(
            name="subrecipientMgmtCostAmt", kinds=(JsonFieldKind.NUMBER,), nullable=True
        ),
        SchemaFieldSpec(name="id", kinds=(JsonFieldKind.STRING,)),
    ),
)

FEMA_HMA_PROGRAM_AREA_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="openfema-hma-program-area",
    version=FEMA_HMA_SCHEMA_VERSION,
    source_field="programArea",
    normalization="strip_casefold",
    known_values=frozenset({"bric", "fma", "hmgp", "lpdm", "pdm", "rfc", "srl"}),
)
FEMA_HMA_PROJECT_STATUS_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="openfema-hma-project-status",
    version=FEMA_HMA_SCHEMA_VERSION,
    source_field="status",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "approved",
            "awarded",
            "closed",
            "completed",
            "obligated",
            "pending",
            "revision requested",
            "void",
            "withdrawn",
        }
    ),
)
FEMA_HMA_PROJECT_TYPE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="openfema-hma-project-type",
    version=FEMA_HMA_SCHEMA_VERSION,
    source_field="projectType",
    normalization="strip_casefold",
    known_values=frozenset(
        {
            "200.1: acquisition of private real property (structures and land) - riverine",
            "201.1: elevation of private structures - riverine",
        }
    ),
)
FEMA_HMA_DATA_SOURCE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="openfema-hma-data-source",
    version=FEMA_HMA_SCHEMA_VERSION,
    source_field="dataSource",
    normalization="strip_casefold",
    known_values=frozenset({"egrants", "fma", "hmgp", "hmgp-historical"}),
)
FEMA_HMA_FUND_CODE_TAXONOMY: Final[TaxonomySpec] = TaxonomySpec(
    taxonomy_id="openfema-hma-fund-code",
    version=FEMA_HMA_SCHEMA_VERSION,
    source_field="fundCode",
    normalization="strip_casefold",
    known_values=frozenset({"5", "6m", "6n", "in"}),
)

FEMA_HMA_PROJECTS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (
    FEMA_HMA_PROGRAM_AREA_TAXONOMY,
    FEMA_HMA_PROJECT_STATUS_TAXONOMY,
    FEMA_HMA_PROJECT_TYPE_TAXONOMY,
    FEMA_HMA_DATA_SOURCE_TAXONOMY,
)
FEMA_HMA_TRANSACTIONS_TAXONOMIES: Final[tuple[TaxonomySpec, ...]] = (FEMA_HMA_FUND_CODE_TAXONOMY,)
