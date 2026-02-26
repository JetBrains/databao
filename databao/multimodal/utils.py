"""Utility functions for multimodal content handling."""

from typing import Any


def dataframe_to_csv(df: Any, max_rows: int = 1000000, max_columns: int = 100) -> str:
    """Convert a DataFrame to CSV with row and column limits.

    Args:
        df: The DataFrame to convert.
        max_rows: Maximum number of rows to include in the CSV. Defaults to 1000000.
        max_columns: Maximum number of columns to include in the CSV. Defaults to 100.

    Returns:
        CSV string representation of the DataFrame (limited to max_rows and max_columns).
    """
    if df is None:
        return ""
    if len(df) > max_rows:
        df = df.head(max_rows)
    if len(df.columns) > max_columns:
        df = df.iloc[:, :max_columns]
    csv_result = df.to_csv(index=False)
    return csv_result if csv_result is not None else ""
