from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from utils.constants import BACKFILL_START_DATE

# =============================================================================
# PLACES SOURCE SCHEMA — columns required in data/silo_places.csv
# Validated by silo_utils.read_silo_places_csv and place__silo_mapping_qa.py.
# =============================================================================

PLACES_CSV_REQUIRED_COLUMNS: tuple[str, ...] = ("place_id", "place_name", "state", "latitude", "longitude")

# =============================================================================
# MAPPING OUTPUT SCHEMA — eight-column output of place__silo_mapping.py
# Enforced on write and validated by place__silo_mapping_qa.py.
# =============================================================================

MAPPING_OUTPUT_SCHEMA: StructType = StructType(
    [
        StructField(
            "place_id",
            StringType(),
            False,
            metadata={"comment": "Unique identifier for each place, taken from data/silo_places.csv."},
        ),
        StructField("place_name", StringType(), False, metadata={"comment": "Human-readable place name."}),
        StructField(
            "state",
            StringType(),
            False,
            metadata={"comment": "Australian state or territory abbreviation (e.g. NSW, VIC, QLD)."},
        ),
        StructField(
            "place_latlon",
            ArrayType(DoubleType()),
            False,
            metadata={"comment": "Place coordinates as [latitude, longitude] in decimal degrees."},
        ),
        StructField(
            "silo_latlon",
            ArrayType(DoubleType()),
            False,
            metadata={"comment": "Nearest SILO 0.05° grid cell coordinates as [latitude, longitude]."},
        ),
        StructField(
            "silo_latlon_key",
            StringType(),
            False,
            metadata={"comment": "String key for the SILO grid cell in 'latitude,longitude' format."},
        ),
        StructField(
            "distance",
            DoubleType(),
            False,
            metadata={
                "comment": "Haversine distance in kilometers between the place and its snapped SILO grid cell, rounded to 6 decimal places."
            },
        ),
        StructField(
            "backfill_from_date",
            DateType(),
            False,
            metadata={
                "comment": f"Per-location start date for SILO data ingestion. Always BACKFILL_START_DATE ({BACKFILL_START_DATE.isoformat()})."
            },
        ),
    ]
)

# =============================================================================
# INGESTION OUTPUT SCHEMA — 42-column daily weather data output of silo_ingestion.py
# Enforced on table creation and validated by silo_ingestion_qa.py.
# =============================================================================

_SOURCE_CODES_DESC: str = (
    "Source codes for all *_source columns — "
    "0: Official observation as supplied by the Bureau of Meteorology. "
    "15: Deaccumulated rainfall (original observation was recorded over a period exceeding the standard 24 hour observation period). "
    "25: Interpolated from daily observations for that date. "
    "26: Synthetic Class A pan evaporation, calculated from temperatures, radiation and vapour pressure. "
    "35: Interpolated from daily observations using an anomaly interpolation method. "
    "42: Satellite radiation estimate from BoM. "
    "75: Interpolated from the long term averages of daily observations for that day of year. "
    "Ref: https://www.longpaddock.qld.gov.au/silo/about/about-data/"
)

CODE_COLUMNS_COMMENT: str = (
    "A source code. Refer to the table description for details on possible values and their meanings."
)

INGESTION_OUTPUT_SCHEMA: StructType = StructType(
    [
        StructField(
            "silo_latlon_key",
            StringType(),
            False,
            metadata={
                "comment": "Grid-cell key 'latitude,longitude' of the snapped SILO cell (e.g. '-35.3,149.15') — matches place__silo_mapping.silo_latlon_key."
            },
        ),
        StructField("date", DateType(), False, metadata={"comment": "Observation date."}),
        StructField(
            "lat",
            DoubleType(),
            False,
            metadata={"comment": "Latitude of the SILO grid cell in decimal degrees (GDA94)."},
        ),
        StructField(
            "lon",
            DoubleType(),
            False,
            metadata={"comment": "Longitude of the SILO grid cell in decimal degrees (GDA94)."},
        ),
        StructField("daily_rain", StringType(), True, metadata={"comment": "Daily rainfall (mm)."}),
        StructField("daily_rain_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField("max_temp", StringType(), True, metadata={"comment": "Daily maximum temperature (°C)."}),
        StructField("max_temp_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField("min_temp", StringType(), True, metadata={"comment": "Daily minimum temperature (°C)."}),
        StructField("min_temp_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField("vp", StringType(), True, metadata={"comment": "Vapour pressure at 9am (hPa)."}),
        StructField("vp_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField("vp_deficit", StringType(), True, metadata={"comment": "Vapour pressure deficit (hPa)."}),
        StructField("vp_deficit_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "evap_pan",
            StringType(),
            True,
            metadata={"comment": "Class A pan evaporation (mm); shifted to the previous day per SILO convention."},
        ),
        StructField("evap_pan_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "evap_syn",
            StringType(),
            True,
            metadata={"comment": "Synthetic estimate of Class A pan evaporation (mm)."},
        ),
        StructField("evap_syn_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "evap_comb",
            StringType(),
            True,
            metadata={"comment": "Combination evaporation: synthetic pre-1970, Class A pan from 1970 onwards (mm)."},
        ),
        StructField("evap_comb_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "evap_morton_lake",
            StringType(),
            True,
            metadata={"comment": "Morton shallow lake evaporation (mm)."},
        ),
        StructField("evap_morton_lake_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "radiation",
            StringType(),
            True,
            metadata={"comment": "Solar radiation — total incoming shortwave on a horizontal surface (MJ/m²)."},
        ),
        StructField("radiation_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "rh_tmax",
            StringType(),
            True,
            metadata={"comment": "Relative humidity at the time of maximum temperature (%)."},
        ),
        StructField("rh_tmax_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "rh_tmin",
            StringType(),
            True,
            metadata={"comment": "Relative humidity at the time of minimum temperature (%)."},
        ),
        StructField("rh_tmin_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "et_short_crop",
            StringType(),
            True,
            metadata={"comment": "FAO56 Penman-Monteith potential evapotranspiration, short crop (mm)."},
        ),
        StructField("et_short_crop_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "et_tall_crop",
            StringType(),
            True,
            metadata={"comment": "ASCE Penman-Monteith potential evapotranspiration, tall crop (mm)."},
        ),
        StructField("et_tall_crop_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "et_morton_actual",
            StringType(),
            True,
            metadata={"comment": "Morton areal actual evapotranspiration (mm)."},
        ),
        StructField("et_morton_actual_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "et_morton_potential",
            StringType(),
            True,
            metadata={"comment": "Morton point potential evapotranspiration (mm)."},
        ),
        StructField("et_morton_potential_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "et_morton_wet",
            StringType(),
            True,
            metadata={"comment": "Morton wet-environment areal potential evapotranspiration over land (mm)."},
        ),
        StructField("et_morton_wet_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField("mslp", StringType(), True, metadata={"comment": "Mean sea level pressure (hPa)."}),
        StructField("mslp_source", StringType(), True, metadata={"comment": CODE_COLUMNS_COMMENT}),
        StructField(
            "created_at",
            TimestampType(),
            False,
            metadata={"comment": "Timestamp when this row was first inserted."},
        ),
        StructField(
            "updated_at",
            TimestampType(),
            False,
            metadata={
                "comment": "Timestamp when this row was last updated. SILO regularly corrects past data so this may differ from created_at."
            },
        ),
    ]
)

# =============================================================================
# MAPPING — used by place__silo_mapping.py and place__silo_mapping_qa.py
# =============================================================================

# SILO dataset coverage bounds (decimal degrees).
SILO_MIN_LON: float = 112.0
SILO_MAX_LON: float = 154.0
SILO_MIN_LAT: float = -44.0
SILO_MAX_LAT: float = -10.0

# Mean Earth radius (km), the same value the haversine package uses. Defined here rather than imported so the
# notebooks need no extra library on Databricks; tests check it still matches haversine.
EARTH_RADIUS_KM: float = 6371.0088
SILO_GRID_RESOLUTION: float = 0.05
PLACE_SILO_MAPPING_DELTA_COMMENT: str = (
    "Maps each place listed in data/silo_places.csv to its nearest SILO 0.05° grid cell, "
    "including the snapped coordinates, the Haversine distance in kilometers and the per-place "
    f"backfill start date ({BACKFILL_START_DATE.isoformat()})."
)

# =============================================================================
# MAPPING QA — used by place__silo_mapping_qa.py only
# =============================================================================

# Maximum Haversine distance (km) a place may be from its snapped SILO grid cell.
DISTANCE_THRESHOLD_KM: float = 5.0

# =============================================================================
# INGESTION — used by silo_ingestion.py and silo_ingestion_qa.py
# =============================================================================

ROLLING_REFRESH_DAYS: int = 30

# The ingestion run stops early once this many locations fail in a row. Consecutive failures usually mean the SILO
# API is unreachable (an outage, or no outbound internet access), so trying every remaining location wastes compute.
MAX_CONSECUTIVE_LOCATION_FAILURES: int = 3

PLACE_SILO_INGESTION_TABLE_COMMENT: str = (
    "Daily SILO DataDrill weather data per grid-cell location. Each location's date range starts "
    f"from its backfill_from_date in place__silo_mapping ({BACKFILL_START_DATE.isoformat()}). "
    f"Last {ROLLING_REFRESH_DAYS} days are always re-fetched to capture SILO retroactive "
    "corrections. Each row represents one grid cell for one date. "
    "API: https://www.longpaddock.qld.gov.au/silo/api-documentation/reference/ | "
    "Variables: https://www.longpaddock.qld.gov.au/silo/about/climate-variables/ | " + _SOURCE_CODES_DESC
)

# =============================================================================
# INGESTION QA — used by silo_ingestion_qa.py only
# =============================================================================

# Climate variable bounds per SILO documentation.
# Ref: https://www.longpaddock.qld.gov.au/silo/faq/
DAILY_RAIN_MIN: float = 0.0
DAILY_RAIN_MAX: float = 1300.0

MAX_TEMP_MIN: float = -9.0
MAX_TEMP_MAX: float = 54.0

MIN_TEMP_MIN: float = -20.0
MIN_TEMP_MAX: float = 40.0

EVAP_PAN_MIN: float = 0.0
EVAP_PAN_MAX: float = 35.0

RADIATION_MIN: float = 0.0
RADIATION_MAX: float = 35.0

VP_MIN: float = 0.0
VP_MAX: float = 43.2
