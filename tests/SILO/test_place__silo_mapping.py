import datetime

import pandas as pd
import pytest
from haversine import Unit, haversine
from haversine.haversine import get_avg_earth_radius
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from SILO.silo_notebooks.constants import EARTH_RADIUS_KM, MAPPING_OUTPUT_SCHEMA
from SILO.silo_notebooks.place__silo_mapping_transforms import (
    build_output,
    compute_haversine_distance,
    extract_place_latlon,
    read_place_candidates,
    snap_to_silo_grid,
)
from utils.constants import BACKFILL_START_DATE

# =============================================================================
# SHARED INPUT SCHEMAS
# =============================================================================

_COORDINATES_SCHEMA = StructType(
    [
        StructField("place_id", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
    ]
)

# =============================================================================
# HELPERS
# =============================================================================


def _places_df(rows: list[tuple]) -> pd.DataFrame:
    """Builds a pandas places DataFrame shaped like silo_utils.read_silo_places_csv output."""
    return pd.DataFrame(rows, columns=["place_id", "place_name", "state", "latitude", "longitude"])


# =============================================================================
# TESTS
# =============================================================================


def test_read_place_candidates_filters_invalid_rows_and_deduplicates(spark: SparkSession) -> None:
    places_df = _places_df(
        [
            ("nsw_dubbo", "Dubbo", "NSW", -32.2569, 148.6011),
            ("nsw_dubbo", "Dubbo", "NSW", -32.2569, 148.6011),  # duplicate — collapsed to one row
            ("vic_sale", "Sale", "VIC", -38.106, 147.068),
            ("qld_nan_lat", "No Latitude", "QLD", float("nan"), 150.0),  # unparseable latitude — dropped
            ("wa_no_state", "No State", None, -31.95, 115.86),  # missing state — dropped
            (None, "No Id", "SA", -34.9, 138.6),  # missing place_id — dropped
        ]
    )

    actual = read_place_candidates(spark, places_df).select("place_id")

    expected = spark.createDataFrame([("nsw_dubbo",), ("vic_sale",)], "place_id string")
    assertDataFrameEqual(actual, expected, checkRowOrder=False, ignoreNullable=True)


def test_snap_to_silo_grid_rounds_to_nearest_cell_and_drops_out_of_bounds(spark: SparkSession) -> None:
    coordinates_sdf = spark.createDataFrame(
        [
            ("dubbo", -32.2569, 148.6011),  # → -32.25, 148.60
            ("canberra", -35.2809, 149.13),  # → -35.30, 149.15 (rounds to nearest 0.05°)
            ("auckland", -36.8485, 174.7633),  # New Zealand — outside SILO longitude coverage
            ("equator", 0.0, 130.0),  # outside SILO latitude coverage
        ],
        _COORDINATES_SCHEMA,
    )

    actual = snap_to_silo_grid(coordinates_sdf).select(
        "place_id", "silo_latitude", "silo_longitude", "silo_latlon_key"
    )

    expected = spark.createDataFrame(
        [
            ("dubbo", -32.25, 148.6, "-32.25,148.6"),
            ("canberra", -35.3, 149.15, "-35.3,149.15"),
        ],
        "place_id string, silo_latitude double, silo_longitude double, silo_latlon_key string",
    )
    assertDataFrameEqual(actual, expected, checkRowOrder=False, ignoreNullable=True)


def test_earth_radius_matches_haversine_library() -> None:
    assert EARTH_RADIUS_KM == get_avg_earth_radius(Unit.KILOMETERS)


def test_compute_haversine_distance_matches_haversine_library(spark: SparkSession) -> None:
    place_latlon: tuple[float, float] = (-33.8688, 151.2093)
    coordinates_sdf = spark.createDataFrame([("sydney", *place_latlon)], _COORDINATES_SCHEMA)

    distance_row = compute_haversine_distance(snap_to_silo_grid(coordinates_sdf)).collect()[0]

    expected_km: float = haversine(
        place_latlon, (distance_row.silo_latitude, distance_row.silo_longitude), Unit.KILOMETERS
    )
    assert distance_row.distance == pytest.approx(expected_km, abs=1e-6)


def test_build_output_conforms_to_mapping_schema_with_fixed_backfill_date(spark: SparkSession) -> None:
    places_df = _places_df(
        [
            ("nsw_dubbo", "Dubbo", "NSW", -32.2569, 148.6011),
            ("tas_hobart", "Hobart", "TAS", -42.8821, 147.3272),
        ]
    )
    distance_sdf = compute_haversine_distance(
        snap_to_silo_grid(extract_place_latlon(read_place_candidates(spark, places_df)))
    )

    actual = build_output(distance_sdf, spark)

    assert [field.name for field in actual.schema.fields] == [field.name for field in MAPPING_OUTPUT_SCHEMA.fields]
    assert [field.dataType for field in actual.schema.fields] == [
        field.dataType for field in MAPPING_OUTPUT_SCHEMA.fields
    ]
    assert BACKFILL_START_DATE == datetime.date(1990, 1, 1)
    assert {row.backfill_from_date for row in actual.collect()} == {BACKFILL_START_DATE}


def test_build_output_returns_empty_typed_dataframe_when_no_rows(spark: SparkSession) -> None:
    empty_places_df = _places_df([("wa_offshore", "Offshore", "WA", -31.95, 100.0)])  # outside SILO coverage
    distance_sdf = compute_haversine_distance(
        snap_to_silo_grid(extract_place_latlon(read_place_candidates(spark, empty_places_df)))
    )

    actual = build_output(distance_sdf, spark)

    assert actual.isEmpty()
    assert actual.schema == MAPPING_OUTPUT_SCHEMA
