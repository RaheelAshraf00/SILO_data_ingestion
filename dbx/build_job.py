"""
Builds a ready-to-create Databricks job definition for one environment.

Merges dbx/job/<job>/config.json with that environment's block in deployment-settings.json (top-level keys in
the block replace the base ones) and turns the repo-relative notebook paths into absolute workspace paths.

Usage:
    python dbx/build_job.py --env dev --repo-root /Workspace/Users/<you>/SILO_data_ingestion
    databricks jobs create --json @dbx/build/silo_ingestion_daily.dev.json

dbx/build/ is git-ignored because --silo-email writes your email address into the generated file.
"""

import argparse
import copy
import json
from pathlib import Path
from typing import Any

JOB_DIR: Path = Path(__file__).resolve().parent / "job"
BUILD_DIR: Path = Path(__file__).resolve().parent / "build"
DEFAULT_JOB_NAME: str = "silo_ingestion_daily"


def build_job_definition(
    base_config: dict[str, Any],
    deployment_settings: dict[str, Any],
    env: str,
    repo_root: str,
    silo_email: str | None = None,
) -> dict[str, Any]:
    """
    Returns base_config with env's top-level keys from deployment_settings applied and every notebook_path
    prefixed with repo_root. When silo_email is given it becomes the default of the silo_email job parameter.

    Args:
        base_config: Parsed config.json.
        deployment_settings: Parsed deployment-settings.json, one block per environment.
        env: Environment block to apply, e.g. "dev".
        repo_root: Absolute workspace path of the repository.
        silo_email: Optional email address sent to SILO as the API username.

    Raises:
        ValueError: When env has no block in deployment_settings or repo_root is not an absolute path.
    """
    if env not in deployment_settings:
        raise ValueError(f"Unknown environment '{env}'. Valid values: {list(deployment_settings)}")
    if not repo_root.startswith("/"):
        raise ValueError(f"repo_root must be an absolute workspace path, got {repo_root!r}")

    job_definition: dict[str, Any] = copy.deepcopy(base_config) | copy.deepcopy(deployment_settings[env])

    for task in job_definition["tasks"]:
        notebook_task: dict[str, Any] | None = task.get("notebook_task")
        if notebook_task is not None:
            notebook_task["notebook_path"] = f"{repo_root.rstrip('/')}/{notebook_task['notebook_path']}"

    if silo_email:
        for parameter in job_definition["parameters"]:
            if parameter["name"] == "silo_email":
                parameter["default"] = silo_email

    return job_definition


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a Databricks job definition for one environment.")
    parser.add_argument("--env", required=True, help="Block in deployment-settings.json: dev, uat or prd.")
    parser.add_argument(
        "--repo-root",
        required=True,
        help="Workspace path of this repository, e.g. /Workspace/Users/<you>/SILO_data_ingestion.",
    )
    parser.add_argument("--silo-email", help="Email address sent to SILO as the API username.")
    parser.add_argument("--job", default=DEFAULT_JOB_NAME, help="Job folder under dbx/job/.")
    args = parser.parse_args()

    job_dir: Path = JOB_DIR / args.job
    job_definition: dict[str, Any] = build_job_definition(
        base_config=json.loads((job_dir / "config.json").read_text(encoding="utf-8")),
        deployment_settings=json.loads((job_dir / "deployment-settings.json").read_text(encoding="utf-8")),
        env=args.env,
        repo_root=args.repo_root,
        silo_email=args.silo_email,
    )

    BUILD_DIR.mkdir(exist_ok=True)
    output_path: Path = BUILD_DIR / f"{args.job}.{args.env}.json"
    output_path.write_text(json.dumps(job_definition, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {output_path}")
    print(f"Create the job with: databricks jobs create --json @{output_path}")


if __name__ == "__main__":
    main()
