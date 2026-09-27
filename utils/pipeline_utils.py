"""
Shared pipeline utilities used across all ingestion pipelines.

Provides environment resolution, full table name getters, and Delta table comment application.
"""

from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# =============================================================================
# ENVIRONMENT RESOLUTION
# =============================================================================

VALID_ENVS: list[str] = ["dev", "uat", "prd"]

# Unity Catalog catalogs the pipelines write to, per environment.
# Replace these placeholders with the catalogs available in your Databricks workspace.
_WRITE_CATALOG_BY_ENV: dict[str, str] = {
    "dev": "dev_catalog",
    "uat": "uat_catalog",
    "prd": "prd_catalog",
}


def get_env(dbutils: Any) -> str:
    """
    Reads and validates the 'env' widget value against VALID_ENVS.

    Args:
        dbutils: Databricks dbutils object.

    Returns:
        The validated, lower-cased environment string.

    Raises:
        ValueError: if the widget value is empty or not in VALID_ENVS.
    """
    env: str = dbutils.widgets.get("env").strip().lower()
    if not env or env not in VALID_ENVS:
        raise ValueError(f"Unknown environment '{env}'. Valid values: {VALID_ENVS}")
    return env


# =============================================================================
# TABLE NAME GETTERS
# =============================================================================


def get_write_catalog(env: str) -> str:
    """Returns the write catalog name for the given environment."""
    return _WRITE_CATALOG_BY_ENV[env]


# =============================================================================
# TABLE DDL
# =============================================================================


def build_column_definitions(output_schema: StructType) -> str:
    """
    Builds the column list of a CREATE TABLE statement from a StructType, adding
    NOT NULL for every non-nullable field so Delta enforces the constraint on write.

    Returns:
        Comma-separated column definitions, e.g. "place_id STRING NOT NULL,\\n    distance DOUBLE".
    """
    return ",\n    ".join(
        f"{field.name} {field.dataType.simpleString().upper()}" + (" NOT NULL" if not field.nullable else "")
        for field in output_schema.fields
    )


# =============================================================================
# TABLE COMMENTS
# =============================================================================


def _escape_sql_string(value: str) -> str:
    """
    Escapes a value for use inside a single-quoted Spark SQL string literal.

    Spark SQL escapes with a backslash, so backslashes are doubled first and quotes become \\'.

    Returns:
        The escaped value, without surrounding quotes.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'")


def apply_table_comments(
    spark: SparkSession,
    full_table_name: str,
    table_comment: str,
    output_schema: StructType,
    env: str,
    prd_table_ref: str,
) -> None:
    """
    Applies the table-level comment and column-level comments to an existing Delta table.

    When env is not 'prd', a non-production warning citing prd_table_ref is prepended
    to the table comment. Column comments are read from the 'comment' key in each
    StructField's metadata in output_schema. Fields with no comment metadata are skipped.

    Args:
        spark: Active SparkSession.
        full_table_name: Fully-qualified Delta table name (catalog.schema.table).
        table_comment: Base table-level comment string.
        output_schema: StructType whose field metadata drives column-level comments.
        env: Resolved environment string — 'prd' skips the non-production prefix.
        prd_table_ref: Fully-qualified production table to cite in the non-production warning.
    """
    _non_prd_prefix: str = (
        f"[NON-PRODUCTION] Development and testing only — not updated. Accurate data: {prd_table_ref}\n\n"
    )
    final_table_comment: str = f"{_non_prd_prefix}{table_comment}" if env != "prd" else table_comment
    spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{_escape_sql_string(final_table_comment)}'")

    for field in output_schema.fields:
        column_comment: str = field.metadata.get("comment", "")
        if not column_comment:
            continue
        spark.sql(
            f"ALTER TABLE {full_table_name} ALTER COLUMN {field.name} COMMENT '{_escape_sql_string(column_comment)}'"
        )

    print(f"Table and column comments applied to {full_table_name}")
