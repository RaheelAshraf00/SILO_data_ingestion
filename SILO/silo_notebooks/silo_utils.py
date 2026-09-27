"""
SILO-specific pipeline utilities.

Provides the places CSV reader, SILO API email resolution, SILO DataDrill HTTP fetching with
retry logic, CSV response validation, CSV parsing into a pandas DataFrame, Delta table upsert,
and SILO table name getters.
"""

import io
import re
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from SILO.silo_notebooks.constants import PLACES_CSV_REQUIRED_COLUMNS
from utils.pipeline_utils import get_write_catalog

# =============================================================================
# PLACES CSV
# =============================================================================

# Committed list of public Australian places the pipeline ingests SILO data for.
# Resolved relative to this module so it works both locally and in a Databricks workspace.
SILO_PLACES_CSV_PATH: Path = Path(__file__).resolve().parent / "data" / "silo_places.csv"


def read_silo_places_csv(csv_path: Path = SILO_PLACES_CSV_PATH) -> pd.DataFrame:
    """
    Reads the places CSV into a pandas DataFrame with typed columns.

    Text columns are read as strings and stripped of surrounding whitespace.
    latitude/longitude are coerced to float; values that cannot be parsed become NaN
    and are dropped downstream by the mapping transforms (and reported by the mapping QA).

    Args:
        csv_path: Path to the places CSV. Defaults to SILO_PLACES_CSV_PATH.

    Returns:
        DataFrame[place_id: str, place_name: str, state: str, latitude: float, longitude: float]

    Raises:
        ValueError: When any column in PLACES_CSV_REQUIRED_COLUMNS is missing.
    """
    places_df: pd.DataFrame = pd.read_csv(csv_path, dtype=str, skipinitialspace=True)
    missing_columns: list[str] = [column for column in PLACES_CSV_REQUIRED_COLUMNS if column not in places_df.columns]
    if missing_columns:
        raise ValueError(f"Places CSV {csv_path} is missing required column(s): {missing_columns}")

    places_df = places_df[list(PLACES_CSV_REQUIRED_COLUMNS)]
    for text_column in ("place_id", "place_name", "state"):
        places_df[text_column] = places_df[text_column].str.strip()
    for coordinate_column in ("latitude", "longitude"):
        places_df[coordinate_column] = pd.to_numeric(places_df[coordinate_column], errors="coerce")

    print(f"Read {len(places_df)} places from {csv_path}")
    return places_df


# =============================================================================
# SILO API EMAIL
# =============================================================================

# Basic shape check only — SILO requires a real contact email address as the API username.
_EMAIL_PATTERN: re.Pattern[str] = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def get_silo_email(dbutils: Any) -> str:
    """
    Reads and validates the 'silo_email' widget value.

    SILO asks every DataDrill request to carry the caller's email address as the username
    so they can contact users about the service. It is supplied at run time (widget or job
    parameter) and never hardcoded in the repository.

    Args:
        dbutils: Databricks dbutils object.

    Returns:
        The stripped email address.

    Raises:
        ValueError: When the widget value is empty or not shaped like an email address.
    """
    silo_email: str = dbutils.widgets.get("silo_email").strip()
    if not _EMAIL_PATTERN.match(silo_email):
        raise ValueError(
            f"Invalid 'silo_email' value {silo_email!r}. Provide the email address to send as the SILO API username."
        )
    return silo_email


# =============================================================================
# SILO API CONSTANTS
# =============================================================================

SILO_DATADRILL_BASE_URL: str = "https://www.longpaddock.qld.gov.au/cgi-bin/silo/DataDrillDataset.php"

# Not a secret credential: SILO identifies callers by the email username and accepts any alphanumeric
# string as the password.
# Ref: https://www.longpaddock.qld.gov.au/silo/api-documentation/reference/
SILO_API_PASSWORD: str = "apirequest"

# Variables to return, one letter each (sent as the 'comment' parameter):
# R daily_rain, X max_temp, N min_temp, V vp, D vp_deficit, E evap_pan, S evap_syn, C evap_comb,
# L evap_morton_lake, J radiation, H rh_tmax, G rh_tmin, F et_short_crop, T et_tall_crop,
# A et_morton_actual, P et_morton_potential, W et_morton_wet, M mslp.
# Keep in step with the climate columns of INGESTION_OUTPUT_SCHEMA in constants.py.
# Ref: https://www.longpaddock.qld.gov.au/silo/about/climate-variables/
SILO_REQUEST_COMMENT_CODE: str = "RXNVDESCLJHGFTAPWM"

# Backoff delays in seconds between retry attempts (first attempt has no delay).
RETRY_DELAY_SCHEDULE: list[int] = [2, 5, 10]

# Per-request timeout. A full 1990-to-today request for one grid cell takes about 10 seconds.
REQUEST_TIMEOUT_SECONDS: int = 60

# Minimum set of column headers a valid SILO CSV response must contain.
_REQUIRED_CSV_COLUMN_HEADERS: frozenset[str] = frozenset(
    {
        "YYYY-MM-DD",
        "daily_rain",
        "max_temp",
        "min_temp",
        "vp",
        "radiation",
    }
)


# =============================================================================
# API FETCH
# =============================================================================


def fetch_silo_csv(lat: float, lon: float, start: date, end: date, silo_email: str) -> str:
    """
    Fetches CSV data from the SILO DataDrill API for a single grid-cell location.

    Retries on connection errors or 5xx responses using RETRY_DELAY_SCHEDULE
    (maximum of 4 attempts total). Raises immediately on 4xx without retry.

    Validates the response body is a well-formed SILO CSV by checking for
    required column headers in the first non-comment line.

    Args:
        lat: Latitude of the SILO grid cell in decimal degrees.
        lon: Longitude of the SILO grid cell in decimal degrees.
        start: First date to fetch (inclusive).
        end: Last date to fetch (inclusive).
        silo_email: Registered contact email sent as the API username parameter.

    Returns:
        Raw CSV response text from the SILO DataDrill API.

    Raises:
        requests.HTTPError: On 4xx responses or when all retry attempts are exhausted on 5xx.
        ValueError: When the response body does not contain valid SILO CSV headers.
    """
    request_params: dict[str, str] = {
        "lat": f"{lat:.2f}",
        "lon": f"{lon:.2f}",
        "start": start.strftime("%Y%m%d"),
        "finish": end.strftime("%Y%m%d"),
        "format": "csv",
        "comment": SILO_REQUEST_COMMENT_CODE,
        "username": silo_email,
        "password": SILO_API_PASSWORD,
    }

    last_exception: Exception | None = None
    all_attempt_delays: list[int] = [0] + RETRY_DELAY_SCHEDULE

    for attempt_number, backoff_seconds in enumerate(all_attempt_delays, start=1):
        if backoff_seconds:
            print(
                f"lat={lat:.2f} lon={lon:.2f} — retrying in {backoff_seconds}s (attempt {attempt_number}/{len(all_attempt_delays)})"
            )
            time.sleep(backoff_seconds)

        try:
            response: requests.Response = requests.get(
                SILO_DATADRILL_BASE_URL,
                params=request_params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as connection_error:
            print(
                f"WARNING: lat={lat:.2f} lon={lon:.2f} — connection error on attempt {attempt_number}: {connection_error}"
            )
            last_exception = connection_error
            continue

        if response.status_code == 200:
            _validate_csv_response_body(response.text, lat, lon)
            return response.text

        if 400 <= response.status_code < 500:
            raise requests.HTTPError(
                f"HTTP {response.status_code} for lat={lat:.2f} lon={lon:.2f}: {response.text[:300]}",
                response=response,
            )

        print(
            f"WARNING: lat={lat:.2f} lon={lon:.2f} — HTTP {response.status_code} on attempt {attempt_number}: {response.text[:300]}"
        )
        last_exception = requests.HTTPError(
            f"HTTP {response.status_code} for lat={lat:.2f} lon={lon:.2f}",
            response=response,
        )

    raise last_exception  # type: ignore[misc]


def _validate_csv_response_body(response_text: str, lat: float, lon: float) -> None:
    """
    Checks that the response body contains the required SILO CSV column headers.

    Skips comment lines (starting with '#') and blank lines to locate the header row.
    Raises ValueError when the body appears to be an error page or plain-text
    error message rather than valid SILO CSV data.

    Args:
        response_text: Raw text returned by the SILO DataDrill API.
        lat: Latitude of the requested grid cell (used in error messages).
        lon: Longitude of the requested grid cell (used in error messages).

    Raises:
        ValueError: When no line contains the required SILO CSV headers.
    """
    for line in response_text.splitlines():
        stripped_line: str = line.strip()
        if not stripped_line or stripped_line.startswith("#"):
            continue
        header_columns: set[str] = {column.strip() for column in stripped_line.split(",")}
        if not _REQUIRED_CSV_COLUMN_HEADERS.issubset(header_columns):
            raise ValueError(
                f"lat={lat:.2f} lon={lon:.2f} — response does not contain expected SILO CSV "
                f"headers. First non-comment line: {stripped_line[:200]!r}"
            )
        return
    raise ValueError(f"lat={lat:.2f} lon={lon:.2f} — response contains no parseable content.")


# =============================================================================
# CSV PARSING
# =============================================================================


def parse_silo_csv(response_text: str, silo_latlon_key: str) -> pd.DataFrame:
    """
    Parses a raw SILO CSV API response into a DataFrame ready for upsert.

    Every value is read as a string, so it is stored exactly as SILO sends it (the climate and
    source columns are STRING in the ingestion table). Empty cells become None.

    Renames columns to match the ingestion table schema:
      'YYYY-MM-DD' → 'date', 'latitude' → 'lat', 'longitude' → 'lon'.
    Drops the 'metadata' column if present, and the undated rows SILO appends to carry the rest of
    its metadata when a response covers fewer days than it has metadata entries.
    Injects 'silo_latlon_key' onto every row.

    Args:
        response_text: Raw CSV text from the SILO DataDrill API.
        silo_latlon_key: Grid-cell key to inject onto every row (e.g. "-35.3,149.15").

    Returns:
        DataFrame of strings where each row represents one day of weather data.
    """
    parsed_df: pd.DataFrame = pd.read_csv(io.StringIO(response_text), dtype=str, skipinitialspace=True)
    parsed_df = parsed_df.rename(columns={"latitude": "lat", "longitude": "lon", "YYYY-MM-DD": "date"})
    parsed_df = parsed_df.drop(columns=["metadata"], errors="ignore").dropna(subset=["date"])
    parsed_df["silo_latlon_key"] = silo_latlon_key
    # astype(object) + where() turns pandas NaN into None so Spark receives proper NULLs.
    return parsed_df.astype(object).where(parsed_df.notna(), None).reset_index(drop=True)


# =============================================================================
# DELTA UPSERT
# =============================================================================


def upsert_records(spark: SparkSession, parsed_df: pd.DataFrame, target_table: str) -> int:
    """
    Merges a parsed SILO DataFrame into the target Delta table on (silo_latlon_key, date).

    'created_at' is preserved on matched rows (INSERT only) and set on new rows.
    'updated_at' is set on both INSERT and UPDATE.

    Args:
        spark: Active SparkSession.
        parsed_df: DataFrame produced by parse_silo_csv.
        target_table: Fully-qualified Delta table name (catalog.schema.table).

    Returns:
        Number of records processed. Returns 0 when parsed_df is empty.
    """
    if parsed_df.empty:
        return 0

    # lat, lon and every climate and source column.
    data_columns: list[str] = [
        column_name
        for column_name in parsed_df.columns.tolist()
        if column_name not in ("silo_latlon_key", "date", "created_at", "updated_at")
    ]

    # parse_silo_csv returns strings only, so the schema is explicit rather than inferred.
    string_schema: StructType = StructType(
        [StructField(column_name, StringType(), True) for column_name in parsed_df.columns]
    )
    incoming_sdf: DataFrame = spark.createDataFrame(parsed_df, schema=string_schema).withColumns(
        {
            "date": F.to_date(F.col("date"), "yyyy-MM-dd"),
            "lat": F.col("lat").cast("double"),
            "lon": F.col("lon").cast("double"),
            "updated_at": F.current_timestamp(),
        }
    )

    delta_table: DeltaTable = DeltaTable.forName(spark, target_table)
    (
        delta_table.alias("target")
        .merge(
            incoming_sdf.alias("source"),
            "target.silo_latlon_key = source.silo_latlon_key AND target.date = source.date",
        )
        .whenMatchedUpdate(
            set={
                **{column_name: f"source.{column_name}" for column_name in data_columns},
                "updated_at": "source.updated_at",
            }
        )
        .whenNotMatchedInsert(
            values={
                "silo_latlon_key": "source.silo_latlon_key",
                "date": "source.date",
                **{column_name: f"source.{column_name}" for column_name in data_columns},
                "created_at": "source.updated_at",
                "updated_at": "source.updated_at",
            }
        )
        .execute()
    )
    return len(parsed_df)


# =============================================================================
# TABLE NAME GETTERS
# =============================================================================

_SILO_WRITE_SCHEMA: str = "source_silo"
_PLACE_SILO_MAPPING_TABLE: str = "place__silo_mapping"
_SILO_INGESTION_TABLE: str = "silo_ingestion"


def get_place_silo_mapping_table(env: str) -> str:
    """Returns the fully-qualified table name for place__silo_mapping."""
    return f"{get_write_catalog(env)}.{_SILO_WRITE_SCHEMA}.{_PLACE_SILO_MAPPING_TABLE}"


def get_silo_ingestion_table(env: str) -> str:
    """Returns the fully-qualified table name for silo_ingestion."""
    return f"{get_write_catalog(env)}.{_SILO_WRITE_SCHEMA}.{_SILO_INGESTION_TABLE}"
