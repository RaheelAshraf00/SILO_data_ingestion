"""
QA error accumulation utility. Imported by all ingestion pipeline QA notebooks.

Import pattern in a Databricks notebook cell:
    import sys
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _repo_root = "/".join(_nb_path.split("/")[:-3])
    if not _repo_root.startswith("/Workspace/"):
        _repo_root = "/Workspace" + _repo_root
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from utils.error_utils import ErrorManager
"""

from typing import Callable

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame


class ErrorManager:
    """
    Collects error messages from multiple QA checks into a list.
    Raises RuntimeError on raise_if_error() if the list is non-empty.
    """

    def __init__(self) -> None:
        self._errors: list[str] = []

    def append_error(self, error_message: str) -> None:
        """Appends a plain-text error message to the accumulated failure list."""
        self._errors.append(error_message)

    def handle_error_df(
        self,
        df: PandasDataFrame | SparkDataFrame,
        error_message_prefix: str,
        display_fn: Callable | None = None,
    ) -> None:
        """
        Accepts a Spark or Pandas DataFrame of violation rows (empty = pass, non-empty = fail).
        Converts a Spark DataFrame to Pandas before evaluation. On failure, builds a
        count-suffixed message, appends it to _errors, prints it, and calls display_fn
        on the violation rows when provided. On pass, prints a PASS confirmation.

        Args:
            df: Spark or Pandas DataFrame of violation rows.
            error_message_prefix: Label prepended to the error message.
            display_fn: Optional callable for rendering the violation rows (e.g. Databricks display).
        """
        # hasattr check avoids isinstance mismatch when pyspark is loaded by both
        # this module and the Databricks runtime as separate class objects.
        pandas_df: PandasDataFrame = df.toPandas() if hasattr(df, "toPandas") else df

        if not pandas_df.empty:
            row_count: int = len(pandas_df)
            error_message: str = f"{error_message_prefix} — {row_count} violation(s) found."
            self._errors.append(error_message)
            print(error_message)
            if display_fn is not None:
                display_fn(pandas_df)
        else:
            print(f"PASS: {error_message_prefix}")

    def raise_if_error(self) -> None:
        """Raises RuntimeError with all accumulated messages when _errors is non-empty."""
        if self._errors:
            formatted: str = "\n\t".join(self._errors)
            raise RuntimeError(f"QA FAILED — {len(self._errors)} check(s) did not pass:\n\t{formatted}")
        print("QA PASSED — all checks completed successfully.")
