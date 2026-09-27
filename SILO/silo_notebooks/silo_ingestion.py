# Databricks notebook source
# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_header
# MAGIC %md
# MAGIC # Place → SILO Grid Cell — Daily Incremental Ingestion
# MAGIC
# MAGIC Reads all SILO grid-cell locations from the mapping table
# MAGIC (`{write_catalog}.source_silo.place__silo_mapping`), checks which dates
# MAGIC are missing per location since each location's backfill_from_date (1990-01-01), and upserts the missing
# MAGIC data into a Delta table using the SILO DataDrill CSV API.
# MAGIC
# MAGIC **Output table:** `{write_catalog}.source_silo.silo_ingestion`
# MAGIC
# MAGIC **SILO email:** the `silo_email` widget / job parameter is sent as the DataDrill `username`
# MAGIC (SILO requires a valid contact email address; it is never stored in the repository).
# MAGIC
# MAGIC **Idempotency:** Each run checks which (location, date) pairs are absent in the
# MAGIC target table and always re-fetches the trailing `ROLLING_REFRESH_DAYS` window.
# MAGIC A failed location is collected and the loop continues; Databricks task retry
# MAGIC handles remaining locations on the next attempt. After `MAX_CONSECUTIVE_LOCATION_FAILURES`
# MAGIC failures in a row the run stops early, since the SILO API is most likely unreachable.
# MAGIC
# MAGIC **API docs:** [SILO DataDrill API reference](https://www.longpaddock.qld.gov.au/silo/api-documentation/reference/)
# MAGIC **Variables:** [SILO climate variables](https://www.longpaddock.qld.gov.au/silo/about/climate-variables/)
# MAGIC **Source codes:** [SILO data codes](https://www.longpaddock.qld.gov.au/silo/about/about-data/)

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

# DBTITLE 1,place_silo_ingestion_imports
import sys
from datetime import date, timedelta
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_sys_path
# Adds the repo root so utils/ is importable — both when running as a
# Databricks job and interactively.
# [:-3] strips notebook + silo_notebooks/ + SILO/, leaving the repo root.
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
from SILO.silo_notebooks.silo_utils import (
    fetch_silo_csv,
    get_place_silo_mapping_table,
    get_silo_email,
    get_silo_ingestion_table,
    parse_silo_csv,
    upsert_records,
)
from utils.pipeline_utils import (
    VALID_ENVS,
    apply_table_comments,
    build_column_definitions,
    get_env,
)

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_data_functions
# =============================================================================
# DATA FUNCTIONS
# =============================================================================


def load_silo_grid_cell_locations(spark: SparkSession, silo_mapping_table: str) -> DataFrame:
    """
    Reads SILO grid-cell locations from the place__silo_mapping table.
    Aggregates to one row per grid cell, taking MIN(backfill_from_date).

    Returns:
        DataFrame[silo_latlon_key: string, lat: double, lon: double, backfill_from_date: date]
    """
    print(f"Loading SILO grid-cell locations from {silo_mapping_table}")
    silo_locations_sdf: DataFrame = (
        spark.table(silo_mapping_table)
        .select(
            "silo_latlon_key",
            F.col("silo_latlon")[0].alias("lat"),
            F.col("silo_latlon")[1].alias("lon"),
            "backfill_from_date",
        )
        .groupBy("silo_latlon_key", "lat", "lon")
        .agg(F.min("backfill_from_date").alias("backfill_from_date"))
    )
    print(f"Loaded distinct SILO grid-cell locations from {silo_mapping_table}")
    return silo_locations_sdf


def compute_per_location_fetch_ranges(
    spark: SparkSession,
    silo_locations_sdf: DataFrame,
    ingestion_full_table_name: str,
    pipeline_run_date: date,
    test_mode: bool,
) -> list[tuple[str, float, float, date]]:
    """
    Computes the fetch start date for every location.

    For each location the fetch start is the earlier of:
    - The first date missing from the target table since the location's backfill_from_date, or
    - pipeline_run_date minus ROLLING_REFRESH_DAYS.

    In test mode, the start date is capped at pipeline_run_date minus 30 days
    and the location list is limited to the first 2 locations by silo_latlon_key.

    Returns:
        List of (silo_latlon_key, lat, lon, fetch_start) tuples, one per location.
    """
    rolling_window_start: date = pipeline_run_date - timedelta(days=ROLLING_REFRESH_DAYS)
    test_mode_date_cap: date | None = pipeline_run_date - timedelta(days=30) if test_mode else None

    if test_mode:
        silo_locations_sdf = silo_locations_sdf.orderBy("silo_latlon_key").limit(2)

    silo_locations_sdf.createOrReplaceTempView("_place_silo_ingestion_locations")

    first_missing_date_sdf: DataFrame = spark.sql(
        f"""
        WITH per_location_spines AS (
            SELECT lk.silo_latlon_key,
                   explode(sequence(lk.backfill_from_date, DATE('{pipeline_run_date}'), INTERVAL 1 DAY)) AS date
            FROM _place_silo_ingestion_locations lk
        ),
        existing_ingestion_rows AS (
            SELECT silo_latlon_key, date
            FROM {ingestion_full_table_name}
            WHERE date <= DATE('{pipeline_run_date}')
        )
        SELECT spine.silo_latlon_key, MIN(spine.date) AS first_missing_date
        FROM per_location_spines spine
        LEFT JOIN existing_ingestion_rows existing
            ON spine.silo_latlon_key = existing.silo_latlon_key AND spine.date = existing.date
        WHERE existing.date IS NULL
        GROUP BY spine.silo_latlon_key
        """
    )

    locations_with_first_missing_sdf: DataFrame = silo_locations_sdf.join(
        first_missing_date_sdf, on="silo_latlon_key", how="left"
    )

    fetch_ranges: list[tuple[str, float, float, date]] = []
    backfill_location_count: int = 0

    for row in locations_with_first_missing_sdf.collect():
        first_missing_date: date | None = row.first_missing_date
        fetch_start_date: date = (
            first_missing_date
            if (first_missing_date is not None and first_missing_date < rolling_window_start)
            else rolling_window_start
        )
        if test_mode_date_cap is not None:
            fetch_start_date = max(fetch_start_date, test_mode_date_cap)
        if fetch_start_date < rolling_window_start:
            backfill_location_count += 1
        fetch_ranges.append((row.silo_latlon_key, float(row.lat), float(row.lon), fetch_start_date))

    print(
        f"Fetch ranges computed — {len(fetch_ranges)} locations total, {backfill_location_count} require historical backfill"
    )
    return fetch_ranges


def ensure_ingestion_table_exists(spark: SparkSession, ingestion_full_table_name: str, env: str) -> None:
    """
    Creates the target Delta ingestion table if it does not exist, then applies
    table-level and column-level comments on first creation only.
    """
    table_name_parts: list[str] = ingestion_full_table_name.split(".")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_name_parts[0]}.{table_name_parts[1]}")

    table_existed_before_create: bool = spark.catalog.tableExists(ingestion_full_table_name)

    column_definitions: str = build_column_definitions(INGESTION_OUTPUT_SCHEMA)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ingestion_full_table_name} (
            {column_definitions}
        )
        USING DELTA
        PARTITIONED BY (silo_latlon_key)
        """
    )

    if not table_existed_before_create:
        apply_table_comments(
            spark=spark,
            full_table_name=ingestion_full_table_name,
            table_comment=PLACE_SILO_INGESTION_TABLE_COMMENT,
            output_schema=INGESTION_OUTPUT_SCHEMA,
            env=env,
            prd_table_ref=get_silo_ingestion_table("prd"),
        )

    print(f"Ingestion table ready: {ingestion_full_table_name}")


# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_widget_registration
# =============================================================================
# WIDGET REGISTRATION
# Standalone cell — registers widgets before any pipeline logic runs.
# =============================================================================

dbutils.widgets.dropdown("env", "dev", VALID_ENVS, "Environment")
dbutils.widgets.text("silo_email", "", "SILO API email (username)")
dbutils.widgets.dropdown("test_mode", "true", ["true", "false"], "Test mode (2 locations, 30-day cap)")

# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_run
# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================


def run(spark: SparkSession, dbutils: Any) -> None:
    """Executes the full daily incremental ingestion pipeline."""
    env: str = get_env(dbutils)
    test_mode: bool = dbutils.widgets.get("test_mode") == "true"
    silo_email: str = get_silo_email(dbutils)

    silo_mapping_table: str = get_place_silo_mapping_table(env)
    ingestion_full_table_name: str = get_silo_ingestion_table(env)

    print(f"Pipeline start: {env=} {test_mode=} {ingestion_full_table_name=}")

    pipeline_run_date: date = date.today()

    try:
        ensure_ingestion_table_exists(spark, ingestion_full_table_name, env)
        silo_locations_sdf: DataFrame = load_silo_grid_cell_locations(spark, silo_mapping_table)
        fetch_ranges: list[tuple[str, float, float, date]] = compute_per_location_fetch_ranges(
            spark, silo_locations_sdf, ingestion_full_table_name, pipeline_run_date, test_mode
        )
    except Exception:
        print("ERROR: Pipeline aborted during setup phase")
        raise

    failed_locations: list[tuple[str, float, float, str]] = []
    total_rows_upserted: int = 0
    consecutive_failure_count: int = 0
    stopped_early: bool = False

    for latlon_key, location_lat, location_lon, fetch_start_date in fetch_ranges:
        try:
            print(
                f"key={latlon_key} lat={location_lat:.2f} lon={location_lon:.2f} — fetching {fetch_start_date} → {pipeline_run_date}"
            )
            csv_response_text: str = fetch_silo_csv(
                location_lat, location_lon, fetch_start_date, pipeline_run_date, silo_email
            )
            parsed_df: pd.DataFrame = parse_silo_csv(csv_response_text, latlon_key)
            rows_upserted: int = upsert_records(spark, parsed_df, ingestion_full_table_name)
            total_rows_upserted += rows_upserted
            consecutive_failure_count = 0
            print(f"key={latlon_key} — upserted {rows_upserted} rows")
        except Exception as location_error:
            print(f"ERROR: key={latlon_key} lat={location_lat:.2f} lon={location_lon:.2f} — FAILED: {location_error}")
            failed_locations.append((latlon_key, location_lat, location_lon, str(location_error)))
            consecutive_failure_count += 1
            if consecutive_failure_count >= MAX_CONSECUTIVE_LOCATION_FAILURES:
                stopped_early = True
                print(f"ERROR: {consecutive_failure_count} locations failed in a row — stopping early")
                break

    print(
        f"Pipeline complete: {total_rows_upserted} rows upserted, {len(failed_locations)}/{len(fetch_ranges)} locations failed"
    )

    if failed_locations:
        failure_summary: str = "\n".join(
            f"  key={latlon_key} lat={failure_lat:.2f} lon={failure_lon:.2f}: {error_message}"
            for latlon_key, failure_lat, failure_lon, error_message in failed_locations
        )
        stop_reason: str = (
            f"Stopped early after {MAX_CONSECUTIVE_LOCATION_FAILURES} consecutive failures — check that the "
            "SILO API is reachable from this compute (outbound internet access). "
            if stopped_early
            else ""
        )
        raise RuntimeError(
            f"{stop_reason}{len(failed_locations)} location(s) failed — Databricks task retry will pick up "
            f"remaining missing dates on the next attempt.\n{failure_summary}"
        )


# COMMAND ----------

# DBTITLE 1,place_silo_ingestion_entry_point
try:
    run(spark, dbutils)
except ValueError as config_error:
    print(f"ERROR: Pipeline aborted due to configuration error: {config_error}")
    raise
except Exception:
    print("ERROR: Pipeline failed with an unhandled exception")
    raise
