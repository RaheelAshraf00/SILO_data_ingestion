# Databricks notebook source
# COMMAND ----------

# DBTITLE 1,place_silo_mapping_header
# MAGIC %md
# MAGIC # Place → SILO Grid Cell Mapping
# MAGIC
# MAGIC Reads the list of public Australian places from the committed CSV
# MAGIC (`SILO/silo_notebooks/data/silo_places.csv`), snaps each location to the nearest
# MAGIC SILO 0.05° grid cell, computes the Haversine distance between the two points,
# MAGIC and writes the result to a Delta table.
# MAGIC
# MAGIC **Source filters:** non-null place_id, latitude and longitude; coordinates within SILO coverage.
# MAGIC
# MAGIC **Backfill:** every place is ingested from `BACKFILL_START_DATE` (1990-01-01).
# MAGIC
# MAGIC **Output table:** `{write_catalog}.source_silo.place__silo_mapping`

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

# DBTITLE 1,place_silo_mapping_imports
import sys
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

# COMMAND ----------

# DBTITLE 1,place_silo_mapping_sys_path
# Adds the repo root to sys.path so the SILO/ and utils/ packages are importable
# when the notebook runs as a Databricks job.
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
from SILO.silo_notebooks.place__silo_mapping_transforms import (
    build_output,
    compute_haversine_distance,
    extract_place_latlon,
    read_place_candidates,
    snap_to_silo_grid,
)
from SILO.silo_notebooks.silo_utils import (
    SILO_PLACES_CSV_PATH,
    get_place_silo_mapping_table,
    read_silo_places_csv,
)
from utils.pipeline_utils import (
    VALID_ENVS,
    apply_table_comments,
    build_column_definitions,
    get_env,
)


def write_output(sdf: DataFrame, spark: SparkSession, silo_mapping_table: str, env: str) -> None:
    """
    Recreates silo_mapping_table from MAPPING_OUTPUT_SCHEMA (so NOT NULL columns are
    declared and enforced by Delta), overwrites it with sdf, then applies the table
    comment and column-level comments from PLACE_SILO_MAPPING_DELTA_COMMENT and
    MAPPING_OUTPUT_SCHEMA metadata.

    sdf columns must be in MAPPING_OUTPUT_SCHEMA order (insertInto is positional).
    """
    table_name_parts: list[str] = silo_mapping_table.split(".")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_name_parts[0]}.{table_name_parts[1]}")
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {silo_mapping_table} (
            {build_column_definitions(MAPPING_OUTPUT_SCHEMA)}
        )
        USING DELTA
        """
    )
    sdf.write.insertInto(silo_mapping_table, overwrite=True)
    row_count: int = spark.table(silo_mapping_table).count()
    print(f"Write complete: {silo_mapping_table} ({row_count} rows)")
    apply_table_comments(
        spark=spark,
        full_table_name=silo_mapping_table,
        table_comment=PLACE_SILO_MAPPING_DELTA_COMMENT,
        output_schema=MAPPING_OUTPUT_SCHEMA,
        env=env,
        prd_table_ref=get_place_silo_mapping_table("prd"),
    )


# COMMAND ----------

# DBTITLE 1,place_silo_mapping_widget_registration
# =============================================================================
# WIDGET REGISTRATION
# Executed as a standalone cell so the widget is visible before pipeline runs.
# =============================================================================

dbutils.widgets.dropdown("env", "dev", VALID_ENVS, "Environment")

# COMMAND ----------

# DBTITLE 1,place_silo_mapping_main_orchestration
# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================


def run(spark: SparkSession, dbutils: Any) -> None:
    """Executes the full pipeline: env resolution → read places CSV → transform → write."""
    env: str = get_env(dbutils)
    silo_mapping_table: str = get_place_silo_mapping_table(env)

    print(f"Pipeline start: env={env} read={SILO_PLACES_CSV_PATH} write={silo_mapping_table}")

    try:
        places_df: pd.DataFrame = read_silo_places_csv(SILO_PLACES_CSV_PATH)
        places_sdf: DataFrame = read_place_candidates(spark, places_df)
        coordinates_sdf: DataFrame = extract_place_latlon(places_sdf)
        silo_grid_sdf: DataFrame = snap_to_silo_grid(coordinates_sdf)
        distance_sdf: DataFrame = compute_haversine_distance(silo_grid_sdf)
        output_sdf: DataFrame = build_output(distance_sdf, spark)
        write_output(output_sdf, spark, silo_mapping_table, env)
    except Exception:
        print("ERROR: Pipeline failed during data transformation or write stage")
        raise


# COMMAND ----------

# DBTITLE 1,place_silo_mapping_run
try:
    run(spark, dbutils)
except ValueError as config_error:
    print(f"ERROR: Pipeline aborted due to configuration error: {config_error}")
    raise
except Exception:
    print("ERROR: Pipeline failed with an unhandled exception")
    raise
