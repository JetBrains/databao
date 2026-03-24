from __future__ import annotations

import os

import pandas as pd


def must_env(name: str) -> str:
    """Return the value of an environment variable or raise if missing."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _safe_to_markdown(df: pd.DataFrame) -> str:
    """Convert a DataFrame to markdown, falling back to to_string() on failure."""
    try:
        return df.to_markdown() or ""
    except Exception:
        return df.to_string()


def df_to_markdown(df: pd.DataFrame | None, max_rows: int = 20) -> str:
    """Convert a DataFrame to a truncated markdown table."""
    if df is None:
        return "(None)"
    if len(df) > max_rows:
        return _safe_to_markdown(df.head(max_rows)) + f"\n... ({len(df) - max_rows} more rows)"
    return _safe_to_markdown(df)
