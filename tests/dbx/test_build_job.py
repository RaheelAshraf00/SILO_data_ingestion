import json
from pathlib import Path
from typing import Any

import pytest

from dbx.build_job import JOB_DIR, build_job_definition

# =============================================================================
# HELPERS
# =============================================================================

_REPO_DIR: Path = Path(__file__).resolve().parents[2]
_JOB_FILES_DIR: Path = JOB_DIR / "silo_ingestion_daily"
_REPO_ROOT: str = "/Workspace/Users/someone@example.com/SILO_data_ingestion"


def _read_job_file(file_name: str) -> dict[str, Any]:
    return json.loads((_JOB_FILES_DIR / file_name).read_text(encoding="utf-8"))


def _build(env: str, silo_email: str | None = None) -> dict[str, Any]:
    return build_job_definition(
        _read_job_file("config.json"), _read_job_file("deployment-settings.json"), env, _REPO_ROOT, silo_email
    )


def _parameter_defaults(job_definition: dict[str, Any]) -> dict[str, str]:
    return {parameter["name"]: parameter["default"] for parameter in job_definition["parameters"]}


# =============================================================================
# COMMITTED JOB FILES
# =============================================================================


def test_notebook_paths_point_at_notebooks_in_the_repo() -> None:
    for task in _read_job_file("config.json")["tasks"]:
        assert (_REPO_DIR / f"{task['notebook_task']['notebook_path']}.py").is_file()


def test_committed_job_files_contain_no_email_address() -> None:
    for file_name in ("config.json", "deployment-settings.json"):
        assert "@" not in (_JOB_FILES_DIR / file_name).read_text(encoding="utf-8")


# =============================================================================
# BUILD
# =============================================================================


@pytest.mark.parametrize("env", ["dev", "uat", "prd"])
def test_build_job_definition_targets_the_environment(env: str) -> None:
    job_definition = _build(env)

    assert _parameter_defaults(job_definition) == {"env": env, "silo_email": ""}
    for task in job_definition["tasks"]:
        assert task["notebook_task"]["notebook_path"].startswith(f"{_REPO_ROOT}/SILO/silo_notebooks/")


def test_each_environment_gets_its_own_job_name() -> None:
    assert [_build(env)["name"] for env in ("dev", "uat", "prd")] == [
        "silo_ingestion_daily_dev",
        "silo_ingestion_daily_uat",
        "silo_ingestion_daily",
    ]


def test_only_prd_runs_on_a_schedule() -> None:
    assert [env for env in ("dev", "uat", "prd") if "schedule" in _build(env)] == ["prd"]


def test_silo_email_becomes_the_parameter_default() -> None:
    job_definition = _build("dev", silo_email="someone@example.com")

    assert _parameter_defaults(job_definition)["silo_email"] == "someone@example.com"


def test_build_job_definition_rejects_unknown_env_and_relative_repo_root() -> None:
    base_config, deployment_settings = _read_job_file("config.json"), _read_job_file("deployment-settings.json")

    with pytest.raises(ValueError, match="Unknown environment"):
        build_job_definition(base_config, deployment_settings, "test", _REPO_ROOT)
    with pytest.raises(ValueError, match="absolute workspace path"):
        build_job_definition(base_config, deployment_settings, "dev", "Users/someone/SILO_data_ingestion")
