import gzip
import json
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from textwrap import dedent

import pytest
from click.testing import CliRunner

from generator.namespaces import (
    _get_db_views,
    _get_glean_apps,
    _get_looker_views,
    namespaces,
)
from generator.views import GrowthAccountingView, PingView


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def custom_namespaces(tmp_path):
    dest = tmp_path / "custom-namespaces.yaml"
    dest.write_text(
        dedent(
            """
            custom:
              canonical_app_name: Custom
              views:
                baseline:
                  type: ping_view
                  tables:
                  - channel: release
                    table: mozdata.custom.baseline
            disallowed:
              canonical_app_name: Disallowed
              views:
                baseline:
                  type: ping_view
                  tables:
                  - channel: release
                    table: mozdata.disallowed.baseline
            """
        ).lstrip()
    )
    return dest.absolute()


@pytest.fixture
def namespace_allowlist(tmp_path):
    dest = tmp_path / "namespace_allowlist.yaml"
    dest.write_text(
        dedent(
            """
            ---
            - custom
            - glean-app
            """
        )
    )
    return dest.absolute()


@pytest.fixture
def generated_sql_uri(tmp_path):
    dest = tmp_path / "bigquery_etl.tar.gz"
    with tarfile.open(dest, "w:gz") as tar:

        def add_to_tar(path, content):
            content = dedent(content).lstrip()
            info = tarfile.TarInfo(path)
            info.size = len(content)
            tar.addfile(info, BytesIO(content.encode()))

        for dataset in ("glean_app", "glean_app_beta"):
            content = f"""
                references:
                  view.sql:
                  - moz-fx-data-shared-prod.{dataset}_derived.baseline_clients_daily_v1
                """
            path = (
                f"sql/moz-fx-data-shared-prod/{dataset}/"
                "baseline_clients_daily/metadata.yaml"
            )
            add_to_tar(path, content)

            content = f"""
                references:
                  view.sql:
                  - moz-fx-data-shared-prod.{dataset}_stable.baseline_v1
                """
            path = f"sql/moz-fx-data-shared-prod/{dataset}/baseline/metadata.yaml"
            add_to_tar(path, content)

            content = f"""
                references:
                  view.sql:
                  - moz-fx-data-shared-prod.{dataset}_derived.baseline_clients_last_seen_v1
                """
            path = (
                f"sql/moz-fx-data-shared-prod/{dataset}/"
                "baseline_clients_last_seen/metadata.yaml"
            )
            add_to_tar(path, content)

    return dest.absolute().as_uri()


@pytest.fixture
def app_listings_uri(tmp_path):
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
                    },
                    {
                        "app_name": "glean-app",
                        "app_channel": "beta",
                        "canonical_app_name": "Glean App Beta",
                        "bq_dataset_family": "glean_app_beta",
                    },
                ]
            ).encode()
        )
    )
    return dest.absolute().as_uri()


@pytest.fixture
def glean_apps():
    return [
        {
            "app_name": "glean-app",
            "canonical_app_name": "Glean App",
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
        }
    ]


def test_namespaces_full(
    runner, custom_namespaces, generated_sql_uri, app_listings_uri, namespace_allowlist
):
    with runner.isolated_filesystem():
        result = runner.invoke(
            namespaces,
            [
                "--custom-namespaces",
                custom_namespaces,
                "--generated-sql-uri",
                generated_sql_uri,
                "--app-listings-uri",
                app_listings_uri,
                "--allowlist",
                namespace_allowlist,
            ],
        )
        sys.stdout.write(result.stdout)
        if result.stderr_bytes is not None:
            sys.stderr.write(result.stderr)
        try:
            assert result.exit_code == 0
        except Exception as e:
            # use exception chaining to expose original traceback
            raise e from result.exception
        print(Path("namespaces.yaml").read_text())
        assert (
            dedent(
                """
                custom:
                  canonical_app_name: Custom
                  views:
                    baseline:
                      tables:
                      - channel: release
                        table: mozdata.custom.baseline
                      type: ping_view
                glean-app:
                  canonical_app_name: Glean App
                  explores:
                    baseline:
                      type: ping_explore
                      views:
                        base_view: baseline
                    growth_accounting:
                      type: growth_accounting_explore
                      views:
                        base_view: growth_accounting
                  views:
                    baseline:
                      tables:
                      - channel: release
                        table: mozdata.glean_app.baseline
                      - channel: beta
                        table: mozdata.glean_app_beta.baseline
                      type: ping_view
                    growth_accounting:
                      tables:
                      - table: mozdata.glean_app.baseline_clients_last_seen
                      type: growth_accounting_view
                """
            ).lstrip()
            == Path("namespaces.yaml").read_text()
        )


def test_get_glean_apps(app_listings_uri, glean_apps):
    assert _get_glean_apps(app_listings_uri) == glean_apps


def test_get_looker_views(glean_apps, generated_sql_uri):
    db_views = _get_db_views(generated_sql_uri)
    views = _get_looker_views(glean_apps[0], db_views)
    assert views == [
        PingView(
            "baseline",
            [
                {"channel": "release", "table": "mozdata.glean_app.baseline"},
                {"channel": "beta", "table": "mozdata.glean_app_beta.baseline"},
            ],
        ),
        GrowthAccountingView(
            [
                {
                    "channel": "release",
                    "table": "mozdata.glean_app.baseline_clients_daily",
                }
            ]
        ),
    ]
