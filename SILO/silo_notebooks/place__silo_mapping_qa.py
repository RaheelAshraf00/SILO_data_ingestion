# Databricks notebook source
# COMMAND ----------

# DBTITLE 1,place_silo_mapping_qa_header
# MAGIC %md
# MAGIC # Place → SILO Grid Cell Mapping — QA Notebook
# MAGIC
# MAGIC Runs the data quality checks against the output table written by
# MAGIC place__silo_mapping.py. Designed to run as a downstream task in the
# MAGIC same Databricks job, chained with run_if: ALL_SUCCESS.
# MAGIC
# MAGIC **Checks:**
# MAGIC
# MAGIC 1. Row count  — output table is non-empty; place_ids in the output not in the places CSV (extra rows) and places CSV place_ids not in the output (missing rows) are surfaced separately
# MAGIC 2. Schema     — output table has the expected eight columns and types
# MAGIC 3. Uniqueness — place_id is unique in the output table
# MAGIC 4. Distance   — all rows have distance <= 5.0 km (one SILO grid cell width)
# MAGIC 5. Places CSV validity — every CSV row has a place_id, place_name, state and numeric coordinates (invalid rows are silently dropped by the pipeline)
# MAGIC 6. SILO bounds — all output rows have snapped coordinates within SILO coverage (lon: 112–154°E, lat: 44–10°S)
# MAGIC 7. Places CSV bounds — all places with valid coordinates have raw coordinates within SILO coverage (lon: 112–154°E, lat: 44–10°S)
# MAGIC 8. Backfill date — backfill_from_date equals BACKFILL_START_DATE on every row

# COMMAND ----------

# When working on notebook while building the utilities in this repo, we need to reload the module each time we make a change.
# Without it, re-running the cells doing imports actually do not update the imported code...
# get_ipython() is injected by the IPython/Databricks kernel;
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

# DBTITLE 1,place_silo_mapping_qa_imports
import sys

from pyspark.sql.types import StructField, StructType
from pyspark.testing import assertSchemaEqual

# COMMAND ----------

# DBTITLE 1,place_silo_mapping_qa_sys_path
# Adds the repo root (so the SILO/ and utils/ packages are importable) when running as a Databricks job.
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

# DBTITLE 1,place_silo_constants
from SILO.silo_notebooks.constants import *
from SILO.silo_notebooks.place__silo_mapping_transforms import convert_places_to_spark
from SILO.silo_notebooks.silo_utils import (
    SILO_PLACES_CSV_PATH,
    get_place_silo_mapping_table,
    read_silo_places_csv,
)
from utils.error_utils import ErrorManager
from utils.pipeline_utils import VALID_ENVS, get_env

# COMMAND ----------

# DBTITLE 1,place_silo_mapping_qa_widget_registration
# =============================================================================
# WIDGET REGISTRATION
# Standalone cell — registers the dropdown before any pipeline logic runs.
# =============================================================================

dbutils.widgets.dropdown("env", "dev", VALID_ENVS, "Environment")

# COMMAND ----------

# DBTITLE 1,place_silo_mapping_qa_config_resolution
# =============================================================================
# CONFIG RESOLUTION
# The places CSV is registered as a temp view so every check can query it in SQL.
# =============================================================================

env: str = get_env(dbutils)
place_silo_mapping_full_table: str = get_place_silo_mapping_table(env)
places_csv_view: str = "_silo_places_csv"

convert_places_to_spark(spark, read_silo_places_csv(SILO_PLACES_CSV_PATH)).createOrReplaceTempView(places_csv_view)

error_manager: ErrorManager = ErrorManager()

print(f"place_silo_mapping_full_table : {place_silo_mapping_full_table}")
print(f"places_csv                    : {SILO_PLACES_CSV_PATH}")
print(f"Environment                   : {env}")

# COMMAND ----------

# DBTITLE 1,Check 1 - Row Count
# =============================================================================
# CHECK 1 — SET MEMBERSHIP
# Every place in the output table must be in the places CSV, and every
# place in the places CSV must appear in the output table.
# Two violation queries expose exactly which place_ids are missing or extra.
# =============================================================================


def check_row_count() -> None:
    """
    Compares the set of place_ids in the output table against the set of distinct
    non-null place_ids in the places CSV.

    Fails with a fast pre-check if the output table is empty, then runs two
    set-difference queries:
    - Extra rows: place_ids in the mapping table not present in the places CSV.
    - Missing rows: place_ids in the places CSV not present in the mapping table.
    """
    output_row_count: int = spark.sql(f"SELECT COUNT(*) AS n FROM {place_silo_mapping_full_table}").collect()[0]["n"]

    if output_row_count == 0:
        error_manager.append_error("Check 1 - Row Count — output table is empty")
        return

    _eligible_places_cte: str = f"""
        WITH eligible AS (
            SELECT DISTINCT place_id
            FROM {places_csv_view}
            WHERE place_id IS NOT NULL
        )
    """

    _extra_sdf = spark.sql(
        f"""
        {_eligible_places_cte}
        SELECT m.place_id
        FROM {place_silo_mapping_full_table} m
        LEFT ANTI JOIN eligible e ON m.place_id = e.place_id
        ORDER BY m.place_id
        """
    )

    _missing_sdf = spark.sql(
        f"""
        {_eligible_places_cte}
        SELECT e.place_id
        FROM eligible e
        LEFT ANTI JOIN {place_silo_mapping_full_table} m ON e.place_id = m.place_id
        ORDER BY e.place_id
        """
    )

    error_manager.handle_error_df(
        _extra_sdf,
        "Check 1a - Row Count — place_ids in output table not found in places CSV (extra rows)",
        display_fn=display,
    )
    error_manager.handle_error_df(
        _missing_sdf,
        "Check 1b - Row Count — place_ids in places CSV not found in output table (missing rows)",
        display_fn=display,
    )


check_row_count()

# COMMAND ----------

# DBTITLE 1,Check 2 - Schema
# =============================================================================
# CHECK 2 — SCHEMA VALIDATION
# Output table must have exactly the eight expected columns with correct types,
# matching MAPPING_OUTPUT_SCHEMA defined in constants.py.
# =============================================================================


def check_schema() -> None:
    """
    Compares the output table schema against MAPPING_OUTPUT_SCHEMA using assertSchemaEqual.
    Metadata is stripped from both sides before comparison to avoid false failures
    from Delta column comment metadata stored on the table.
    """
    actual_schema = spark.table(place_silo_mapping_full_table).schema
    # Strip metadata from both sides — Delta stores column comments as metadata keys,
    # which would cause assertSchemaEqual to fail even when names and types match.
    expected_stripped = StructType([StructField(f.name, f.dataType, f.nullable) for f in MAPPING_OUTPUT_SCHEMA.fields])
    actual_stripped = StructType([StructField(f.name, f.dataType, f.nullable) for f in actual_schema.fields])
    try:
        assertSchemaEqual(actual_stripped, expected_stripped)
        print("PASS: Check 2 - Schema Validation")
    except AssertionError as schema_error:
        error_manager.append_error(f"Check 2 - Schema — {schema_error}")


check_schema()

# COMMAND ----------

# DBTITLE 1,Check 3 - Uniqueness
# =============================================================================
# CHECK 3 — PLACE ID UNIQUENESS
# Every row in the output must have a unique place_id.
# =============================================================================


def check_uniqueness() -> None:
    """
    Selects place_id values that appear more than once in the output table.
    Any results indicate a deduplication failure in the pipeline.
    """
    _duplicate_sdf = spark.sql(
        f"""
        SELECT place_id, COUNT(*) AS duplicate_count
        FROM {place_silo_mapping_full_table}
        GROUP BY place_id
        HAVING duplicate_count > 1
        ORDER BY duplicate_count DESC
        """
    )

    error_manager.handle_error_df(
        _duplicate_sdf,
        "Check 3 - Place Id Uniqueness — duplicate place_id values found in output table",
        display_fn=display,
    )


check_uniqueness()

# COMMAND ----------

# DBTITLE 1,Check 4 - Distance Threshold
# =============================================================================
# CHECK 4 — DISTANCE THRESHOLD
# All output rows must have distance <= 5.0 km (one SILO grid cell width).
# Rows exceeding this indicate a grid-snapping anomaly.
# =============================================================================


def check_distance_threshold() -> None:
    """
    Selects rows from the output table where the Haversine distance exceeds
    DISTANCE_THRESHOLD_KM. Any results indicate a grid-snapping anomaly.
    """
    _distance_violations_sdf = spark.sql(
        f"""
        SELECT place_id, place_latlon, silo_latlon, silo_latlon_key, distance
        FROM {place_silo_mapping_full_table}
        WHERE distance > {DISTANCE_THRESHOLD_KM}
        ORDER BY distance DESC
        """
    )

    error_manager.handle_error_df(
        _distance_violations_sdf,
        f"Check 4 - Distance Threshold — rows with distance > {DISTANCE_THRESHOLD_KM} km",
        display_fn=display,
    )


check_distance_threshold()

# COMMAND ----------

# DBTITLE 1,Check 5 - Places CSV Validity
# =============================================================================
# CHECK 5 — PLACES CSV VALIDITY
# Every row in the places CSV must have a place_id, place_name, state and
# numeric latitude/longitude. Rows failing this check are silently dropped
# by the pipeline (unparseable coordinates are read as NULL).
# =============================================================================


def check_places_csv_validity() -> None:
    """
    Selects places CSV rows with a NULL place_id, place_name, state, latitude or
    longitude. Such rows are silently dropped by the mapping pipeline.
    """
    _invalid_places_sdf = spark.sql(
        f"""
        SELECT place_id, place_name, state, latitude, longitude
        FROM {places_csv_view}
        WHERE place_id IS NULL
           OR place_name IS NULL
           OR state IS NULL
           OR latitude IS NULL
           OR longitude IS NULL
        ORDER BY place_id
        """
    )

    error_manager.handle_error_df(
        _invalid_places_sdf,
        "Check 5 - Places CSV Validity — rows with missing identifiers or unparsable coordinates",
        display_fn=display,
    )


check_places_csv_validity()

# COMMAND ----------

# DBTITLE 1,Check 6 - SILO Coverage Bounds
# =============================================================================
# CHECK 6 — SILO COVERAGE BOUNDS
# All output rows must have snapped coordinates within SILO coverage
# (lon: 112–154°E, lat: 44–10°S). Any matches indicate a pipeline bug.
# =============================================================================


def check_silo_coverage_bounds() -> None:
    """
    Selects rows from the output table where the snapped silo_latlon coordinates
    fall outside SILO coverage bounds. Any results indicate the pipeline let
    out-of-bounds rows through to the output table.
    """
    _out_of_bounds_sdf = spark.sql(
        f"""
        SELECT place_id, silo_latlon, silo_latlon_key
        FROM {place_silo_mapping_full_table}
        WHERE silo_latlon[0] < {SILO_MIN_LAT}
           OR silo_latlon[0] > {SILO_MAX_LAT}
           OR silo_latlon[1] < {SILO_MIN_LON}
           OR silo_latlon[1] > {SILO_MAX_LON}
        ORDER BY place_id
        """
    )

    error_manager.handle_error_df(
        _out_of_bounds_sdf,
        f"Check 6 - SILO Coverage Bounds — output rows with snapped coordinates outside coverage "
        f"(lon: {SILO_MIN_LON}-{SILO_MAX_LON}°E, lat: {abs(SILO_MAX_LAT)}-{abs(SILO_MIN_LAT)}°S)",
        display_fn=display,
    )


check_silo_coverage_bounds()

# COMMAND ----------

# DBTITLE 1,Check 7 - Places CSV in SILO Bounds
# =============================================================================
# CHECK 7 — PLACES CSV IN SILO BOUNDS
# All places in the CSV with valid coordinates must have raw (unsnapped)
# coordinates within SILO coverage bounds.
# Places outside the bounds are silently dropped by the pipeline.
# =============================================================================


def check_places_csv_in_silo_bounds() -> None:
    """
    Selects places CSV rows with valid coordinates whose raw latitude or longitude
    falls outside SILO coverage bounds. Such places are silently dropped by the
    mapping pipeline.
    """
    _out_of_bounds_places_sdf = spark.sql(
        f"""
        SELECT place_id, place_name, latitude, longitude
        FROM {places_csv_view}
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND (
                latitude < {SILO_MIN_LAT}
             OR latitude > {SILO_MAX_LAT}
             OR longitude < {SILO_MIN_LON}
             OR longitude > {SILO_MAX_LON}
          )
        ORDER BY place_id
        """
    )

    error_manager.handle_error_df(
        _out_of_bounds_places_sdf,
        f"Check 7 - Places CSV in SILO Bounds — places with raw coordinates outside coverage "
        f"(lon: {SILO_MIN_LON}-{SILO_MAX_LON}°E, lat: {abs(SILO_MAX_LAT)}-{abs(SILO_MIN_LAT)}°S)",
        display_fn=display,
    )


check_places_csv_in_silo_bounds()

# COMMAND ----------

# DBTITLE 1,Check 8 - Backfill Date
# =============================================================================
# CHECK 8 — BACKFILL DATE
# backfill_from_date must equal BACKFILL_START_DATE on every row.
# =============================================================================


def check_backfill_date() -> None:
    """
    Selects rows where backfill_from_date is NULL or differs from BACKFILL_START_DATE.
    """
    _wrong_backfill_date_sdf = spark.sql(
        f"""
        SELECT place_id, backfill_from_date
        FROM {place_silo_mapping_full_table}
        WHERE backfill_from_date IS NULL
           OR backfill_from_date != DATE('{BACKFILL_START_DATE.isoformat()}')
        ORDER BY place_id
        """
    )

    error_manager.handle_error_df(
        _wrong_backfill_date_sdf,
        f"Check 8 - Backfill Date — rows where backfill_from_date != {BACKFILL_START_DATE.isoformat()}",
        display_fn=display,
    )


check_backfill_date()

# COMMAND ----------

# DBTITLE 1,Final Result
# =============================================================================
# FINAL RESULT
# Raises RuntimeError listing all failures, or prints success.
# When run as a Databricks job task, a raised exception marks the task FAILED.
# =============================================================================

error_manager.raise_if_error()
