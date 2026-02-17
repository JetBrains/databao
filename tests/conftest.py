"""Pytest configuration for databao tests."""

import os

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip apikey tests if OPENAI_API_KEY is not set or is empty."""
    # Check if OPENAI_API_KEY is set and not empty
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        skip_apikey = pytest.mark.skip(reason="OPENAI_API_KEY environment variable is not set or is empty")
        for item in items:
            if "apikey" in item.keywords:
                item.add_marker(skip_apikey)

