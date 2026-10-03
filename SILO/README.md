# SILO Weather Data Ingestion

Databricks notebooks that map a list of Australian places to [SILO](https://www.longpaddock.qld.gov.au/silo/)
gridded weather data cells and ingest their daily climate history from 1 January 1990.

SILO provides daily gridded datasets at 0.05° resolution (~5 km × 5 km) covering Australia
(112°E–154°E, 10°S–44°S).

## Project Structure

```
silo_notebooks/
├── data/
│   └── silo_places.csv               # the 50 places to ingest (edit to change coverage)
├── constants.py                      # shared constants and output schemas for all notebooks in the package
├── silo_utils.py                     # shared utilities: places CSV reader, SILO API client, CSV parsing, Delta upsert
├── place__silo_mapping_transforms.py # pure transformation functions (unit tested)
├── place__silo_mapping.py            # notebook: places CSV → place__silo_mapping
├── place__silo_mapping_qa.py         # notebook: QA for place__silo_mapping
├── silo_ingestion.py                 # notebook: SILO DataDrill API → silo_ingestion
└── silo_ingestion_qa.py              # notebook: QA for silo_ingestion
```

## Places

`data/silo_places.csv` lists 50 public places (capital cities and major regional and agricultural towns in every
state and territory) with town-centre coordinates:

| Column       | Description                                   |
|--------------|-----------------------------------------------|
| `place_id`   | Unique slug, e.g. `nsw_dubbo`                 |
| `place_name` | Human-readable name                           |
| `state`      | State or territory abbreviation               |
| `latitude`   | Decimal degrees (GDA94 / WGS84)               |
| `longitude`  | Decimal degrees (GDA94 / WGS84)               |

To change coverage, edit the CSV. The next mapping run picks up the change, and ingestion backfills any new grid
cell from 1990. Places that share a 0.05° cell are fetched once.

## Pipeline

### 1. `place__silo_mapping.py`

Reads the places CSV, snaps each place to the nearest SILO grid cell
(`round(round(coord / 0.05) * 0.05, 2)`), drops places outside SILO coverage, computes the Haversine distance to
the cell centre, and overwrites `{catalog}.source_silo.place__silo_mapping`. Every place gets
`backfill_from_date = 1990-01-01`.

### 2. `place__silo_mapping_qa.py`

1. Row count: output matches the places in the CSV (extra and missing rows reported separately)
2. Schema: matches `MAPPING_OUTPUT_SCHEMA`
3. Uniqueness: `place_id` is unique
4. Distance: every place is within 5 km of its cell
5. Places CSV validity: no missing identifiers or unparseable coordinates
6. SILO bounds: snapped coordinates are inside SILO coverage
7. Places CSV bounds: raw coordinates are inside SILO coverage
8. Backfill date: always `1990-01-01`

### 3. `silo_ingestion.py`

For each distinct grid cell in the mapping table:

- builds a date spine from `backfill_from_date` to today and finds the first date missing from
  `{catalog}.source_silo.silo_ingestion`
- fetches from `min(first missing date, today − 30 days)` to today from the SILO DataDrill CSV API, so the last
  30 days are always refreshed to pick up SILO's retroactive corrections
- upserts with a Delta `MERGE` on `(silo_latlon_key, date)`

A failing location is recorded and the loop moves on; the task fails at the end with a summary. After 3 failures in
a row (`MAX_CONSECUTIVE_LOCATION_FAILURES`) the run stops early, since the SILO API is most likely unreachable.

Widgets / parameters:

| Name         | Description                                                                  |
|--------------|------------------------------------------------------------------------------|
| `env`        | `dev`, `uat` or `prd`, selects the write catalog                             |
| `silo_email` | Your email address, sent as the SILO API `username` (required by SILO)        |
| `test_mode`  | `true` limits the run to 2 locations and the last 30 days                     |

### 4. `silo_ingestion_qa.py`

1. Coverage: every mapped grid cell has data
2. Schema: matches `INGESTION_OUTPUT_SCHEMA`
3. Date completeness: no missing dates from 1990-01-01 to yesterday
4. Value ranges: climate variables within SILO documented bounds, `max_temp >= min_temp`
5. Null keys: no nulls in key and core climate columns
6. Timestamps: `created_at` and `updated_at` are populated and ordered

Checks 1 and 3 are only meaningful after a full run (`test_mode=false`).

## Databricks Job

The job definition lives at the repo root:

```
dbx/
├── build_job.py                  # Merges the base definition with one environment → dbx/build/<job>.<env>.json
└── job/silo_ingestion_daily/
    ├── config.json               # Base job definition (tasks, retries, timeouts, serverless environment)
    └── deployment-settings.json  # dev, uat and prd overrides (job name, performance mode, schedule, parameters)
```

Task graph (serverless, environment version 5):

```
run_place__silo_mapping ──┬──► run_silo_ingestion ──► run_silo_ingestion_qa
                          └──► run_place__silo_mapping_qa
```

Notebook paths in `config.json` are relative to the repo root; `dbx/build_job.py` turns them into absolute
workspace paths for the environment you build. Only `prd` has a schedule (daily at 09:00 Singapore time, `Asia/Singapore`) and
uses performance-optimised serverless; `dev` and `uat` run on demand in the cheaper standard mode. Pass
`--silo-email` to the build script, or set the `silo_email` job parameter before running. See the
[root README](../README.md#databricks-job) for the commands.

## References

- [SILO Gridded API Reference](https://www.longpaddock.qld.gov.au/silo/api-documentation/reference/)
- [SILO climate variables](https://www.longpaddock.qld.gov.au/silo/about/climate-variables/)
- [SILO FAQ](https://www.longpaddock.qld.gov.au/silo/faq/) (including the value bounds used by the ingestion QA)
- Data licence: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), Queensland Government
