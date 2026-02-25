"""Utility functions for multimodal display."""

from __future__ import annotations

import pandas as pd


def dataframe_to_html(df: pd.DataFrame) -> str:
    """Convert a DataFrame to HTML, truncating if necessary.

    Args:
        df: A pandas DataFrame to convert to HTML.

    Returns:
        HTML string representation of the DataFrame.
    """
    if len(df) > 20:
        first_10 = df.head(10)
        last_10 = df.tail(10)

        separator_data = {col: "..." for col in df.columns}
        separator_df = pd.DataFrame([separator_data], index=["..."])

        truncated_df = pd.concat([first_10, separator_df, last_10])
        html_result = truncated_df.to_html()
    else:
        html_result = df.to_html()

    return html_result if html_result is not None else ""
