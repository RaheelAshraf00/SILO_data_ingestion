from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from utils.pipeline_utils import (
    VALID_ENVS,
    _escape_sql_string,
    apply_table_comments,
    build_column_definitions,
    get_env,
    get_write_catalog,
)

# =============================================================================
# HELPERS
# =============================================================================


def _dbutils_with_env(env: str) -> SimpleNamespace:
    """Minimal dbutils stand-in whose 'env' widget returns env."""
    return SimpleNamespace(widgets=SimpleNamespace(get={"env": env}.__getitem__))


# =============================================================================
# ENVIRONMENT RESOLUTION
# =============================================================================


@pytest.mark.parametrize(("widget_value", "expected_env"), [("dev", "dev"), ("uat", "uat"), (" PRD ", "prd")])
def test_get_env_normalises_valid_values(widget_value: str, expected_env: str) -> None:
    assert get_env(_dbutils_with_env(widget_value)) == expected_env


@pytest.mark.parametrize("widget_value", ["", "test", "production"])
def test_get_env_rejects_unknown_values(widget_value: str) -> None:
    with pytest.raises(ValueError, match="Unknown environment"):
        get_env(_dbutils_with_env(widget_value))


def test_every_env_has_its_own_write_catalog() -> None:
    assert [get_write_catalog(env) for env in VALID_ENVS] == ["dev_catalog", "uat_catalog", "prd_catalog"]


# =============================================================================
# TABLE DDL
# =============================================================================


def test_build_column_definitions_declares_non_nullable_columns() -> None:
    schema = StructType([StructField("place_id", StringType(), False), StructField("distance", DoubleType(), True)])

    assert build_column_definitions(schema) == "place_id STRING NOT NULL,\n    distance DOUBLE"


# =============================================================================
# TABLE COMMENTS
# =============================================================================


def test_escape_sql_string_round_trips_through_spark_sql(spark: SparkSession) -> None:
    comment = "Key in 'latitude,longitude' format (e.g. '-35.3,149.15'), a back\\slash\nand a second line."

    assert spark.sql(f"SELECT '{_escape_sql_string(comment)}' AS comment").first().comment == comment


def test_apply_table_comments_escapes_quotes_and_flags_non_production() -> None:
    executed_sql: list[str] = []
    schema = StructType(
        [
            StructField("silo_latlon_key", StringType(), False, metadata={"comment": "Key in 'lat,lon' format."}),
            StructField("distance", DoubleType(), True),  # no comment metadata, so no ALTER statement
        ]
    )

    apply_table_comments(
        spark=SimpleNamespace(sql=executed_sql.append),
        full_table_name="uat_catalog.source_silo.t",
        table_comment="Daily data.",
        output_schema=schema,
        env="uat",
        prd_table_ref="prd_catalog.source_silo.t",
    )

    assert executed_sql == [
        "COMMENT ON TABLE uat_catalog.source_silo.t IS '[NON-PRODUCTION] Development and testing only — not updated. "
        "Accurate data: prd_catalog.source_silo.t\n\nDaily data.'",
        "ALTER TABLE uat_catalog.source_silo.t ALTER COLUMN silo_latlon_key COMMENT 'Key in \\'lat,lon\\' format.'",
    ]
