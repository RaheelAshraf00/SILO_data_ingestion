# Databricks notebook source
# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_qa_header
# MAGIC %md
# MAGIC # Place → SILO Grid Cell Ingestion — QA Notebook
# MAGIC
# MAGIC Runs data quality checks against the output table written by
# MAGIC `silo_ingestion.py`. Designed to run as a downstream task in the
# MAGIC same Databricks job, chained with `run_if: ALL_SUCCESS`.
# MAGIC
# MAGIC **Checks:**
# MAGIC 1. Coverage          — every `silo_latlon_key` from the mapping table has at least one row
# MAGIC 2. Schema            — output table has all expected columns with correct types
# MAGIC 3. Date completeness — no missing dates per location from each location's backfill_from_date to yesterday
# MAGIC 4. Value ranges      — climate variables are within SILO documented bounds
# MAGIC 5. Null keys         — no nulls on `silo_latlon_key`, `date`, `lat`, `lon`, or key climate vars
# MAGIC 6. Timestamps        — no null `created_at` or `updated_at`

# COMMAND ----------

# When working on this notebook while building utilities in this repo, reload modules on change.
# get_ipython() is injected by the IPython/Databricks kernel.
try:
    _ip = get_ipython()
    if _ip is not None:
        _ip.run_line_magic("load_ext", "autoreload")
        _ip.run_line_magic("autoreload", "2")
except NameError:
    print(
        "get_ipython not found — if you're seeing this message outside Databricks, ignore it. If you're in Databricks and seeing this message, autoreload won't work and you'll need to restart the cluster to pick up changes to imported modules."
    )
    pass

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_qa_sys_import
import sys

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_qa_sys_path
# Adds the repo root (so the utils/ package is importable) when running as a Databricks job.
# [:-3] strips notebook + its folder + data-source folder.
# Adjust [:-3] if the notebook depth relative to the repo root changes.
try:
    _nb_path: str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _repo_root: str = "/".join(_nb_path.split("/")[:-3])
    if not _repo_root.startswith("/Workspace/"):
        _repo_root = "/Workspace" + _repo_root
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
except Exception:
    print("Running outside Databricks — add repo root manually to PYTHONPATH")

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_qa_imports
from datetime import date, timedelta

from pyspark.sql.types import StructField, StructType
from pyspark.testing import assertSchemaEqual

from SILO.silo_notebooks.constants import *
from SILO.silo_notebooks.silo_utils import get_place_silo_mapping_table, get_silo_ingestion_table
from utils.error_utils import ErrorManager
from utils.pipeline_utils import VALID_ENVS, get_env

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_qa_widget_registration
# =============================================================================
# WIDGET REGISTRATION
# Standalone cell — registers the dropdown before any pipeline logic runs.
# =============================================================================

dbutils.widgets.dropdown("env", "dev", VALID_ENVS, "Environment")

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_qa_config_resolution
# =============================================================================
# CONFIG RESOLUTION
# =============================================================================

env: str = get_env(dbutils)
mapping_full_table_name: str = get_place_silo_mapping_table(env)
ingestion_full_table_name: str = get_silo_ingestion_table(env)

error_manager: ErrorManager = ErrorManager()

print(f"QA target  : {ingestion_full_table_name}")
print(f"QA mapping : {mapping_full_table_name}")
print(f"Environment: {env}")

# COMMAND ----------

# DBTITLE 1,Check 1 - Coverage
# =============================================================================
# CHECK 1 — LOCATION COVERAGE
# Every silo_latlon_key present in the mapping table must have at least one
# row in the ingestion table.
# =============================================================================


def check_coverage() -> None:
    """
    Selects mapping keys that have no corresponding rows in the ingestion table.
    Any results indicate a location was not fetched during the last pipeline run.
    """
    mapping_keys_without_ingestion_data_sdf = spark.sql(
        f"""
        SELECT m.silo_latlon_key
        FROM (SELECT DISTINCT silo_latlon_key FROM {mapping_full_table_name}) m
        LEFT JOIN (SELECT DISTINCT silo_latlon_key FROM {ingestion_full_table_name}) t
            ON m.silo_latlon_key = t.silo_latlon_key
        WHERE t.silo_latlon_key IS NULL
        ORDER BY m.silo_latlon_key
        """
    )

    error_manager.handle_error_df(
        mapping_keys_without_ingestion_data_sdf,
        "Check 1 - Location Coverage — mapping keys absent from ingestion table",
        display_fn=display,
    )


check_coverage()

# COMMAND ----------

# DBTITLE 1,Check 2 - Schema
# =============================================================================
# CHECK 2 — SCHEMA VALIDATION
# Output table must have exactly the expected columns with correct types,
# matching INGESTION_OUTPUT_SCHEMA defined in constants.py.
# =============================================================================


def check_schema() -> None:
    """
    Compares the ingestion output table schema against INGESTION_OUTPUT_SCHEMA.
    Metadata is stripped from both sides before comparison to avoid false failures
    from Delta column comment metadata stored on the table.
    """
    actual_table_schema = spark.table(ingestion_full_table_name).schema
    expected_schema_stripped = StructType(
        [StructField(f.name, f.dataType, f.nullable) for f in INGESTION_OUTPUT_SCHEMA.fields]
    )
    actual_schema_stripped = StructType(
        [StructField(f.name, f.dataType, f.nullable) for f in actual_table_schema.fields]
    )
    try:
        assertSchemaEqual(actual_schema_stripped, expected_schema_stripped)
        print(
            f"PASS: Check 2 - Schema Validation — all {len(INGESTION_OUTPUT_SCHEMA.fields)} columns present with correct types"
        )
    except AssertionError as schema_mismatch_error:
        error_manager.append_error(f"Check 2 - Schema — {schema_mismatch_error}")


check_schema()

# COMMAND ----------

# DBTITLE 1,Check 3 - Date Completeness
# =============================================================================
# CHECK 3 — DATE COMPLETENESS
# Every location must have a row for every date from each location's
# backfill_from_date to yesterday.
# =============================================================================


def check_date_completeness() -> None:
    """
    Generates a per-location date spine from each location's earliest
    backfill_from_date to yesterday, then selects locations with any missing
    dates. Reports the missing date count, earliest gap, and latest gap per location.
    """
    yesterday: str = str(date.today() - timedelta(days=1))

    locations_with_date_gaps_sdf = spark.sql(
        f"""
        WITH location_ranges AS (
            SELECT
                silo_latlon_key,
                MIN(backfill_from_date) AS backfill_from_date,
                DATE('{yesterday}') AS end_date
            FROM {mapping_full_table_name}
            GROUP BY silo_latlon_key
        ),
        per_location_spines AS (
            SELECT
                lr.silo_latlon_key,
                explode(sequence(lr.backfill_from_date, lr.end_date, INTERVAL 1 DAY)) AS date
            FROM location_ranges lr
        )
        SELECT
            spine.silo_latlon_key,
            COUNT(*) AS missing_date_count,
            MIN(spine.date) AS earliest_gap,
            MAX(spine.date) AS latest_gap
        FROM per_location_spines spine
        LEFT JOIN {ingestion_full_table_name} t
            ON spine.silo_latlon_key = t.silo_latlon_key AND spine.date = t.date
        WHERE t.date IS NULL
        GROUP BY spine.silo_latlon_key
        ORDER BY missing_date_count DESC
        """
    )

    error_manager.handle_error_df(
        locations_with_date_gaps_sdf,
        "Check 3 - Date Completeness — locations with missing dates",
        display_fn=display,
    )


check_date_completeness()

# COMMAND ----------

# DBTITLE 1,Check 4 - Value Ranges
# =============================================================================
# CHECK 4 — VALUE RANGES
# Climate variables must fall within SILO's documented acceptable bounds.
# Ref: https://www.longpaddock.qld.gov.au/silo/faq/
# =============================================================================


def check_value_ranges() -> None:
    """
    Selects locations where any climate variable has values outside SILO documented bounds.
    Counts the number of out-of-range rows per variable per location.
    Also checks that max_temp is not strictly less than min_temp on the same date.
    """
    climate_variable_range_violations_sdf = spark.sql(
        f"""
        SELECT
            silo_latlon_key,
            SUM(CASE WHEN CAST(daily_rain AS DOUBLE) NOT BETWEEN {DAILY_RAIN_MIN} AND {DAILY_RAIN_MAX} THEN 1 ELSE 0 END) AS daily_rain_out_of_range,
            SUM(CASE WHEN CAST(max_temp   AS DOUBLE) NOT BETWEEN {MAX_TEMP_MIN}   AND {MAX_TEMP_MAX}   THEN 1 ELSE 0 END) AS max_temp_out_of_range,
            SUM(CASE WHEN CAST(min_temp   AS DOUBLE) NOT BETWEEN {MIN_TEMP_MIN}   AND {MIN_TEMP_MAX}   THEN 1 ELSE 0 END) AS min_temp_out_of_range,
            SUM(CASE WHEN CAST(max_temp   AS DOUBLE) <  CAST(min_temp AS DOUBLE)                       THEN 1 ELSE 0 END) AS max_temp_less_than_min_temp,
            SUM(CASE WHEN CAST(evap_pan   AS DOUBLE) NOT BETWEEN {EVAP_PAN_MIN}   AND {EVAP_PAN_MAX}   THEN 1 ELSE 0 END) AS evap_pan_out_of_range,
            SUM(CASE WHEN CAST(radiation  AS DOUBLE) NOT BETWEEN {RADIATION_MIN}  AND {RADIATION_MAX}  THEN 1 ELSE 0 END) AS radiation_out_of_range,
            SUM(CASE WHEN CAST(vp         AS DOUBLE) NOT BETWEEN {VP_MIN}         AND {VP_MAX}         THEN 1 ELSE 0 END) AS vp_out_of_range
        FROM {ingestion_full_table_name}
        GROUP BY silo_latlon_key
        HAVING
            daily_rain_out_of_range > 0
            OR max_temp_out_of_range > 0
            OR min_temp_out_of_range > 0
            OR max_temp_less_than_min_temp > 0
            OR evap_pan_out_of_range > 0
            OR radiation_out_of_range > 0
            OR vp_out_of_range > 0
        ORDER BY silo_latlon_key
        """
    )

    error_manager.handle_error_df(
        climate_variable_range_violations_sdf,
        "Check 4 - Value Ranges — rows with climate values outside SILO documented bounds",
        display_fn=display,
    )


check_value_ranges()

# COMMAND ----------

# DBTITLE 1,Check 5 - Null Keys
# =============================================================================
# CHECK 5 — NULL KEY COLUMNS
# Non-nullable identifier and core climate columns must have no nulls or
# empty strings.
# =============================================================================


def check_null_keys() -> None:
    """
    Selects locations where any required column contains nulls or empty strings.
    Counts violations per column per location.
    """
    null_and_empty_violations_sdf = spark.sql(
        f"""
        SELECT
            silo_latlon_key,
            SUM(CASE WHEN silo_latlon_key IS NULL OR silo_latlon_key = '' THEN 1 ELSE 0 END) AS null_or_empty_silo_latlon_key,
            SUM(CASE WHEN date       IS NULL                              THEN 1 ELSE 0 END) AS null_date,
            SUM(CASE WHEN lat        IS NULL                              THEN 1 ELSE 0 END) AS null_lat,
            SUM(CASE WHEN lon        IS NULL                              THEN 1 ELSE 0 END) AS null_lon,
            SUM(CASE WHEN daily_rain IS NULL OR daily_rain = ''           THEN 1 ELSE 0 END) AS null_or_empty_daily_rain,
            SUM(CASE WHEN max_temp   IS NULL OR max_temp   = ''           THEN 1 ELSE 0 END) AS null_or_empty_max_temp,
            SUM(CASE WHEN min_temp   IS NULL OR min_temp   = ''           THEN 1 ELSE 0 END) AS null_or_empty_min_temp
        FROM {ingestion_full_table_name}
        GROUP BY silo_latlon_key
        HAVING
            null_or_empty_silo_latlon_key > 0
            OR null_date > 0
            OR null_lat > 0
            OR null_lon > 0
            OR null_or_empty_daily_rain > 0
            OR null_or_empty_max_temp > 0
            OR null_or_empty_min_temp > 0
        ORDER BY silo_latlon_key
        """
    )

    error_manager.handle_error_df(
        null_and_empty_violations_sdf,
        "Check 5 - Null Keys Columns — rows with null or empty values in required columns",
        display_fn=display,
    )


check_null_keys()

# COMMAND ----------

# DBTITLE 1,Check 6 - Timestamps
# =============================================================================
# CHECK 6 — TIMESTAMPS POPULATED
# Every row must have both created_at and updated_at set, and created_at must not be after updated_at.
# =============================================================================


def check_timestamps() -> None:
    """
    Selects locations where any row has a null created_at or updated_at,
    or where created_at is after updated_at.
    Both timestamps are required on every row in the ingestion table.
    """
    null_timestamp_violations_sdf = spark.sql(
        f"""
        SELECT
            silo_latlon_key,
            SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) AS null_created_at,
            SUM(CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END) AS null_updated_at,
            SUM(CASE WHEN created_at > updated_at THEN 1 ELSE 0 END) AS created_at_after_updated_at
        FROM {ingestion_full_table_name}
        GROUP BY silo_latlon_key
        HAVING null_created_at > 0 OR null_updated_at > 0 OR created_at_after_updated_at > 0
        ORDER BY silo_latlon_key
        """
    )

    error_manager.handle_error_df(
        null_timestamp_violations_sdf,
        "Check 6 - Timestamps — rows with null created_at or updated_at or created_at after updated_at",
        display_fn=display,
    )


check_timestamps()

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_qa_final_result
# =============================================================================
# FINAL RESULT
# Raises RuntimeError listing all failures, or prints success.
# A raised exception marks this task FAILED in the Databricks job.
# =============================================================================

error_manager.raise_if_error()
