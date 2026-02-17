"""Pytest configuration for databao tests."""

import os

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip apikey tests if OPENAI_API_KEY is not set or is empty."""
    # Check if OPENAI_API_KEY is set and not empty
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    has_valid_api_key = bool(api_key)
    
    if not has_valid_api_key:
        skip_apikey = pytest.mark.skip(reason="OPENAI_API_KEY environment variable is not set or is empty")
        for item in items:
            if "apikey" in item.keywords:
                item.add_marker(skip_apikey)

