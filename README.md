# SILO Data Ingestion

Databricks pipelines that ingest daily gridded weather data from [SILO](https://www.longpaddock.qld.gov.au/silo/)
(Queensland Government) for 50 public Australian places, from **1 January 1990** to today, into Delta tables,
with data quality checks at every stage.

```
SILO/silo_notebooks/data/silo_places.csv        50 public places (capital cities and agricultural towns)
        │
        │  place__silo_mapping.py      snap each place to its SILO 0.05° grid cell + Haversine distance
        ▼
{catalog}.source_silo.place__silo_mapping   ──►  place__silo_mapping_qa.py   (8 checks)
        │
        │  silo_ingestion.py           SILO DataDrill API per grid cell, 1990 → today, MERGE into Delta
        ▼
{catalog}.source_silo.silo_ingestion        ──►  silo_ingestion_qa.py        (6 checks)
```

## Highlights

- **Idempotent incremental loads.** Each run computes the first missing date per grid cell, fetches only what is
  missing, and always re-fetches the last 30 days to pick up SILO's retroactive corrections. Rows are upserted
  with a Delta `MERGE` on `(silo_latlon_key, date)`, preserving `created_at` and bumping `updated_at`.
- **Resilient API client.** Retries with backoff on connection errors and 5xx responses, fails fast on 4xx, and
  validates that the response body is real SILO CSV before parsing it. Values are stored exactly as SILO sends them.
- **Failure isolation.** A failing location is collected and the loop continues. The task fails at the end with a
  summary, and the Databricks task retry resumes from the missing dates. If several locations fail in a row, the
  run stops early instead of trying every location against an API it cannot reach.
- **Schema as code.** Output schemas are `StructType`s with per-column comments, applied to the Delta tables and
  validated by the QA notebooks. Non-nullable columns are declared `NOT NULL` so Delta enforces them.
- **QA as a job stage.** Each pipeline has a QA notebook that accumulates every failure (row coverage, schema,
  uniqueness, distance threshold, SILO coverage bounds, date completeness, climate value ranges, nulls,
  timestamps) and fails the task with a full report.
- **Testable code.** Transformation logic lives in plain Python modules, free of `dbutils`, and is unit tested
  locally with PySpark, together with the API client's retry behaviour, the QA error handling and the job builder.

## Repository Structure

```
SILO_data_ingestion/
├── dbx/
│   ├── build_job.py                 # Builds a ready-to-create job definition for one environment
│   └── job/silo_ingestion_daily/    # Databricks job definition + per-environment overrides (dev, uat, prd)
├── SILO/                            # SILO pipeline (see SILO/README.md)
├── tests/                           # pytest unit tests (run locally, no Databricks needed)
├── utils/                           # Shared utilities (env resolution, table comments, QA ErrorManager)
└── README.md
```

## Getting Started

**Prerequisites:** Python 3.12, [Poetry](https://python-poetry.org/), Java 17+ (required by PySpark locally).

```bash
poetry install
poetry run pytest
```

PySpark's Python worker processes often fail to start on native Windows, and the tests that build Spark DataFrames
then fail with "Python worker exited unexpectedly (crashed)". On Windows, run the tests in WSL.

### Running on Databricks

These steps also work on the free [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition).

1. **Free Edition only:** verify your identity with LinkedIn. Free Edition limits outbound internet access
   until you do (see [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)),
   and the pipeline has to reach the SILO API. To check, run this in a notebook; it should print `200`:

   ```python
   import requests
   print(requests.get("https://www.longpaddock.qld.gov.au/silo/", timeout=30).status_code)
   ```

2. Create the Unity Catalog catalogs the pipelines write to: `dev_catalog`, `uat_catalog` and `prd_catalog`
   (Catalog Explorer → **Create catalog**). To use other catalogs, edit `_WRITE_CATALOG_BY_ENV` in
   [utils/pipeline_utils.py](utils/pipeline_utils.py). Tables are written to the `source_silo` schema, which is
   created if it does not exist.
3. Add this repository to your workspace as a Git folder. For a private repository, first link your GitHub
   account in Databricks under **Settings → Linked accounts**.
4. Run the notebooks in `SILO/silo_notebooks/` in this order with `env=dev`: mapping → mapping QA → ingestion →
   ingestion QA. For ingestion, set `silo_email` to your email address (SILO requires one as the API username).
   Start with `test_mode=true` (2 locations, last 30 days). In test mode, ingestion QA checks 1 and 3 fail by
   design. Run ingestion again with `test_mode=false` for the full backfill, then re-run ingestion QA.
5. Optionally, create the job (below).

The first full run backfills about 36 years of daily data for each grid cell (one API call per cell, about 10
seconds each). Later runs fetch only the missing dates plus the 30-day refresh window.

### Databricks Job

The job lives in [dbx/job/silo_ingestion_daily/](dbx/job/silo_ingestion_daily/): `config.json` is the base
definition and `deployment-settings.json` holds the dev, uat and prd overrides. Only prd has a schedule (daily at
09:00 Australia/Sydney); dev and uat jobs run on demand.

Build the definition for one environment, then create it with the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/):

```bash
python dbx/build_job.py --env dev \
    --repo-root /Workspace/Users/<you>/SILO_data_ingestion \
    --silo-email <your-email>
databricks jobs create --json @dbx/build/silo_ingestion_daily.dev.json
```

`--repo-root` is the workspace path of your Git folder. The output in `dbx/build/` is git-ignored because it
contains your email address. To get failure alerts, add recipients to the job's notifications in the Jobs UI
rather than committing email addresses. See [SILO/README.md](SILO/README.md#databricks-job) for the task graph.

## Linting & Formatting

```bash
# Auto-fix lint issues and sort imports
poetry run ruff check --fix .

# Format code
poetry run ruff format .
```

Pre-commit hooks (JSON/YAML/TOML checks, ruff, bandit) are configured in `.pre-commit-config.yaml`:

```bash
poetry run pre-commit install          # run the hooks on every commit
poetry run pre-commit run --all-files  # run them once over the whole repo
```

## Contributing

Coding conventions for notebooks, transforms and QA checks are described in [CONTRIBUTING.md](CONTRIBUTING.md).

## Data Attribution

Weather data is from the SILO climate database, provided by the Queensland Government, Department of
Environment, Tourism, Science and Innovation, and licensed under
[Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/). See
[SILO — About the data](https://www.longpaddock.qld.gov.au/silo/about/about-data/). Please use the API
responsibly: SILO asks every request to carry a valid email address, and large or frequent downloads should
follow their guidance.

## License

Code is released under the [MIT License](LICENSE). SILO data remains subject to its own licence (above).
