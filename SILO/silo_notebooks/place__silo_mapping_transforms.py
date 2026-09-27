"""
SILO mapping transformation functions.

Provides data transformation logic for the place__silo_mapping pipeline.
This module is free of Databricks-specific globals (dbutils, spark) and delta dependencies
so it can be imported and tested outside of the Databricks runtime.
"""

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

from SILO.silo_notebooks.constants import (
    EARTH_RADIUS_KM,
    MAPPING_OUTPUT_SCHEMA,
    SILO_GRID_RESOLUTION,
    SILO_MAX_LAT,
    SILO_MAX_LON,
    SILO_MIN_LAT,
    SILO_MIN_LON,
)
from utils.constants import BACKFILL_START_DATE

# Spark schema of the places CSV once loaded by silo_utils.read_silo_places_csv.
_PLACES_INPUT_SCHEMA: StructType = StructType(
    [
        StructField("place_id", StringType(), True),
        StructField("place_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)

# =============================================================================
# DATA TRANSFORMATION FUNCTIONS
# =============================================================================


def convert_places_to_spark(spark: SparkSession, places_df: pd.DataFrame) -> DataFrame:
    """
    Converts the pandas places DataFrame (from silo_utils.read_silo_places_csv) to Spark
    without filtering. pandas NaN values (empty or unparseable CSV cells) become NULL.

    Shared by the mapping pipeline and the mapping QA notebook.

    Returns:
        DataFrame[place_id: string, place_name: string, state: string,
                  latitude: double, longitude: double]
    """
    # astype(object) + where() turns pandas NaN into None so Spark receives proper NULLs.
    places_records_df: pd.DataFrame = places_df.astype(object).where(places_df.notna(), None)
    return spark.createDataFrame(places_records_df.to_dict("records"), schema=_PLACES_INPUT_SCHEMA)


def read_place_candidates(spark: SparkSession, places_df: pd.DataFrame) -> DataFrame:
    """
    Converts the places CSV DataFrame to Spark, keeping rows with a non-null place_id,
    place_name, state, latitude and longitude, deduplicated to one row per place_id.

    Returns:
        DataFrame[place_id: string, place_name: string, state: string,
                  latitude: double, longitude: double]
    """
    print(f"Reading place candidates from places CSV ({len(places_df)} rows)")
    places_sdf: DataFrame = convert_places_to_spark(spark, places_df)
    return places_sdf.filter(
        F.col("place_id").isNotNull()
        & F.col("place_name").isNotNull()
        & F.col("state").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
    ).dropDuplicates(["place_id"])


def extract_place_latlon(sdf: DataFrame) -> DataFrame:
    """
    Appends a place_latlon array column ordered as [latitude, longitude].

    Returns:
        Input DataFrame with added column: place_latlon.
    """
    sdf = sdf.withColumn("place_latlon", F.array(F.col("latitude"), F.col("longitude")))
    print("Place coordinates assembled into place_latlon")
    return sdf


def snap_to_silo_grid(sdf: DataFrame) -> DataFrame:
    """
    Snaps latitude and longitude to the nearest 0.05° grid point using two-pass rounding.

    Formula: snapped = round(round(coord / SILO_GRID_RESOLUTION, 0) * SILO_GRID_RESOLUTION, 2)

    Rows outside SILO coverage bounds are dropped.

    Returns:
        Input DataFrame with added columns: silo_latitude, silo_longitude,
        silo_latlon, silo_latlon_key.
    """
    res: Column = F.lit(SILO_GRID_RESOLUTION)

    sdf = sdf.withColumns(
        {
            "silo_latitude": F.round(F.round(F.col("latitude") / res, 0) * res, 2),
            "silo_longitude": F.round(F.round(F.col("longitude") / res, 0) * res, 2),
        }
    )

    sdf = sdf.filter(
        (F.col("silo_latitude") >= SILO_MIN_LAT)
        & (F.col("silo_latitude") <= SILO_MAX_LAT)
        & (F.col("silo_longitude") >= SILO_MIN_LON)
        & (F.col("silo_longitude") <= SILO_MAX_LON)
    )
    print("Snapped to SILO grid and filtered to coverage bounds")

    sdf = sdf.withColumns(
        {
            "silo_latlon": F.array(F.col("silo_latitude"), F.col("silo_longitude")),
            "silo_latlon_key": F.concat(
                F.col("silo_latitude").cast(StringType()),
                F.lit(","),
                F.col("silo_longitude").cast(StringType()),
            ),
        }
    )
    return sdf


def compute_haversine_distance(sdf: DataFrame) -> DataFrame:
    """
    Appends a distance column (double, km, 6 decimal places) via the Haversine formula.

    Returns:
        Input DataFrame with added column: distance (double, km).
    """
    r: Column = F.lit(EARTH_RADIUS_KM)

    # Spark column implementation of the full Haversine formula, reference: https://github.com/mapado/haversine/blob/main/haversine/haversine.py
    # Formula: d = sin(Δlat/2)² + cos(lat1)·cos(lat2)·sin(Δlon/2)²
    # Distance = r · 2 · asin(√d)  where r is the mean Earth radius in km.
    lat1: Column = F.radians(F.col("latitude"))
    lat2: Column = F.radians(F.col("silo_latitude"))
    lon1: Column = F.radians(F.col("longitude"))
    lon2: Column = F.radians(F.col("silo_longitude"))

    dlat: Column = lat2 - lat1
    dlon: Column = lon2 - lon1

    a: Column = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    distance: Column = r * 2 * F.asin(F.sqrt(a))

    return sdf.withColumn("distance", F.round(distance, 6))


def build_output(sdf: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Selects and casts the eight output columns to conform to MAPPING_OUTPUT_SCHEMA.
    backfill_from_date is set to BACKFILL_START_DATE for every place.
    Returns an empty typed DataFrame when the input contains no rows.

    Returns:
        DataFrame conforming to MAPPING_OUTPUT_SCHEMA.
    """
    if sdf.isEmpty():
        print("WARNING: Pipeline produced no output rows — writing empty table with output schema")
        return spark.createDataFrame([], schema=MAPPING_OUTPUT_SCHEMA)

    return sdf.select(
        F.col("place_id").cast(StringType()),
        F.col("place_name").cast(StringType()),
        F.col("state").cast(StringType()),
        F.col("place_latlon"),
        F.col("silo_latlon"),
        F.col("silo_latlon_key").cast(StringType()),
        F.col("distance").cast(DoubleType()),
        F.lit(BACKFILL_START_DATE).cast(DateType()).alias("backfill_from_date"),
    )
