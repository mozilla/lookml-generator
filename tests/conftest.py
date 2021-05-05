"""PyTest configuration."""

import gzip
import json

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


@pytest.fixture
def app_listings_uri(tmp_path):
    """
    Mock app listings.

    See: https://probeinfo.telemetry.mozilla.org/v2/glean/app-listings
    """
    dest = tmp_path / "app-listings"
    dest.write_bytes(
        gzip.compress(
            json.dumps(
                [
                    {
                        "app_name": "glean-app",
                        "app_channel": "release",
                        "canonical_app_name": "Glean App",
                        "bq_dataset_family": "glean_app",
                        "notification_emails": ["glean-app-owner@allizom.com"],
                        "v1_name": "glean-app-release",
                    },
                    {
                        "app_name": "glean-app",
                        "app_channel": "beta",
                        "canonical_app_name": "Glean App Beta",
                        "bq_dataset_family": "glean_app_beta",
                        "notification_emails": ["glean-app-owner-beta@allizom.com"],
                        "v1_name": "glean-app-beta",
                    },
                ]
            ).encode()
        )
    )
    return dest.absolute().as_uri()


@pytest.fixture
def metrics_listings_file(tmp_path):
    """Mock metrics listings."""
    dest = tmp_path / "metrics-listings"
    dest.write_bytes(
        gzip.compress(
            json.dumps(
                {
                    "test.counter": {
                        "type": "counter",
                    },
                    "glean_validation_metrics.ping_count": {
                        "type": "counter",
                    },
                }
            ).encode()
        )
    )
    return dest.absolute()


@pytest.fixture
def glean_apps():
    """Mock processed version of app listings (see above)."""
    return [
        {
            "name": "glean-app",
            "glean_app": True,
            "pretty_name": "Glean App",
            "owners": [
                "glean-app-owner@allizom.com",
            ],
            "channels": [
                {
                    "channel": "release",
                    "dataset": "glean_app",
                },
                {
                    "channel": "beta",
                    "dataset": "glean_app_beta",
                },
            ],
            "v1_name": "glean-app-release",
        }
    ]
