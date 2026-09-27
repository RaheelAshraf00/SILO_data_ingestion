# Contributing

These conventions keep the notebooks consistent, readable in the Databricks UI, and testable locally.

## Layout

- A data source lives in its own top-level folder with a `<source>_notebooks/` package (see `SILO/silo_notebooks/`).
- Constants and output schemas shared by a package's notebooks go in `constants.py`; shared helpers go in
  `<source>_utils.py`.
- Code shared across data sources goes in `utils/`. Import it rather than copying it.
- Pure transformation logic goes in a `*_transforms.py` module with no `dbutils` or network calls, so it can be
  unit tested under `tests/`.
- Job definitions go in `dbx/job/<job_name>/` as `config.json` (base definition, notebook paths relative to the
  repo root) plus `deployment-settings.json` (dev, uat and prd overrides). `dbx/build_job.py` merges them.

## Naming

- Pipeline notebooks: `{entity}__{source}_{step}.py` when a step maps an entity to the source
  (`place__silo_mapping.py`), otherwise `{source}_{step}.py` (`silo_ingestion.py`). QA notebooks add `_qa`.
- Functions start with an action verb: `read_`, `extract_`, `compute_`, `build_`, `write_`, `fetch_`, `parse_`.
- Names describe the data: `places_sdf`, `silo_grid_sdf`, `distance_sdf`. Avoid `df1`, `temp` or `data`.

## Notebooks

- Start with `# Databricks notebook source` and separate cells with `# COMMAND ----------`.
- The first cell is markdown describing the purpose, inputs, outputs and key transformations. Keep it current.
- Give every cell a `# DBTITLE 1,...` tag. Mark sections with `# ===` banners and ALL CAPS headings.
- Register widgets in a standalone cell, then resolve and validate configuration before any reads or SQL.
- Wrap the pipeline in a `run(spark, dbutils)` function called from a final cell with explicit error handling.

## Python

- Python 3.12 built-in generics (`list[str]`, `dict[str, str]`, `X | None`) and type hints on every signature and
  variable.
- `from pyspark.sql import functions as F`. Use `sdf.withColumns({...})` instead of chained `.withColumn(...)`.
- Use named constants with `F.lit(CONSTANT)` rather than inline magic numbers.
- Log with `print()`. Python `logging` is awkward to configure on Databricks serverless.
- Keep Spark actions (`count`, `collect`, `display`) to meaningful checkpoints. Serverless compute has no caching,
  so every action recomputes the plan.
- Never commit credentials, emails or workspace-specific identifiers. Pass them in as widgets, job parameters or
  secrets.

## QA notebooks

- Use `ErrorManager` from `utils.error_utils`.
- Put each check in its own function, called straight after it is defined, and number the checks in order
  (`Check 1 - Row Count`, `Check 2 - Schema`, ...).
- Collect every failure and call `error_manager.raise_if_error()` as the final cell.

## Before opening a pull request

```bash
poetry run ruff check --fix .
poetry run ruff format .
poetry run pytest
poetry run pre-commit run --all-files
```
