import pandas as pd
import pytest
from pyspark.sql import SparkSession

from utils.error_utils import ErrorManager


def test_handle_error_df_passes_on_empty_violations(capsys: pytest.CaptureFixture[str]) -> None:
    error_manager = ErrorManager()

    error_manager.handle_error_df(pd.DataFrame(columns=["place_id"]), "Check 1 - Row Count")
    error_manager.raise_if_error()

    assert "PASS: Check 1 - Row Count" in capsys.readouterr().out


def test_handle_error_df_counts_and_displays_spark_violations(spark: SparkSession) -> None:
    error_manager = ErrorManager()
    displayed_dfs: list[pd.DataFrame] = []
    violations_sdf = spark.createDataFrame([("nsw_dubbo",), ("vic_sale",)], "place_id string")

    error_manager.handle_error_df(violations_sdf, "Check 3 - Uniqueness", display_fn=displayed_dfs.append)

    assert [displayed_df["place_id"].tolist() for displayed_df in displayed_dfs] == [["nsw_dubbo", "vic_sale"]]
    with pytest.raises(RuntimeError, match=r"Check 3 - Uniqueness — 2 violation\(s\) found\."):
        error_manager.raise_if_error()


def test_raise_if_error_reports_every_failed_check() -> None:
    error_manager = ErrorManager()
    error_manager.append_error("Check 2 - Schema — column mismatch")
    error_manager.handle_error_df(pd.DataFrame({"place_id": ["p1"]}), "Check 4 - Distance Threshold")

    with pytest.raises(RuntimeError) as raised:
        error_manager.raise_if_error()

    assert str(raised.value) == (
        "QA FAILED — 2 check(s) did not pass:\n"
        "\tCheck 2 - Schema — column mismatch\n"
        "\tCheck 4 - Distance Threshold — 1 violation(s) found."
    )
