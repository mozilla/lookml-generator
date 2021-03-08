"""PyTest configuration."""

import pytest


def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless a keyword or marker filter is specified."""
    keywordexpr = config.option.keyword
    markexpr = config.option.markexpr
    if keywordexpr or markexpr:
        return

    skip_integration = pytest.mark.skip(reason="integration marker not selected")

    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)
