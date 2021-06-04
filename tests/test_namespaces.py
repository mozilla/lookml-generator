import sys
import tarfile
from io import BytesIO
from pathlib import Path
from textwrap import dedent
from typing import Dict

import pytest
import yaml
from click.testing import CliRunner

from generator.namespaces import (
    _get_db_views,
    _get_explores,
    _get_glean_apps,
    _get_looker_views,
    namespaces,
)
from generator.views import (
    ClientCountsView,
    FunnelAnalysisView,
    GleanPingView,
    GrowthAccountingView,
    TableView,
)

from .utils import print_and_test


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
              connection: bigquery-oauth
              glean_app: false
              pretty_name: Custom
              owners:
              - custom-owner@allizom.com
              views:
                baseline:
                  type: ping_view
                  tables:
                  - channel: release
                    table: mozdata.custom.baseline
            disallowed:
              pretty_name: Disallowed
              owners:
              - disallowed-owner@allizom.com
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
    dest = tmp_path / "namespaces-allowlist.yaml"
    dest.write_text(
        dedent(
            """
            ---
            custom:
              owners:
                - custom-owner2@allizom.com
            glean-app:
              owners:
                - glean-app-owner2@allizom.com
            """
        )
    )
    return dest.absolute()


def add_to_tar(tar, path, content):
    content = dedent(content).lstrip()
    info = tarfile.TarInfo(path)
    info.size = len(content)
    tar.addfile(info, BytesIO(content.encode()))


def paths_to_tar(dest_path: Path, paths: Dict[str, str]) -> str:
    with tarfile.open(dest_path, "w:gz") as tar:
        for path, content in paths.items():
            add_to_tar(tar, path, content)

    return dest_path.absolute().as_uri()


@pytest.fixture
def generated_sql_uri(tmp_path):
    dest = tmp_path / "bigquery_etl.tar.gz"
    paths = {}
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
        paths[path] = content

        content = f"""
            references:
              view.sql:
              - moz-fx-data-shared-prod.{dataset}_stable.baseline_v1
            """
        path = f"sql/moz-fx-data-shared-prod/{dataset}/baseline/metadata.yaml"
        paths[path] = content

        content = f"""
            references:
              view.sql:
              - moz-fx-data-shared-prod.{dataset}_derived.baseline_clients_last_seen_v1
            """
        path = (
            f"sql/moz-fx-data-shared-prod/{dataset}/"
            "baseline_clients_last_seen/metadata.yaml"
        )
        paths[path] = content

    return paths_to_tar(dest, paths)


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

        expected = {
            "custom": {
                "glean_app": False,
                "connection": "bigquery-oauth",
                "owners": ["custom-owner@allizom.com", "custom-owner2@allizom.com"],
                "pretty_name": "Custom",
                "views": {
                    "baseline": {
                        "tables": [
                            {"channel": "release", "table": "mozdata.custom.baseline"}
                        ],
                        "type": "ping_view",
                    }
                },
            },
            "glean-app": {
                "explores": {
                    "baseline": {
                        "type": "glean_ping_explore",
                        "views": {"base_view": "baseline"},
                    },
                    "client_counts": {
                        "type": "client_counts_explore",
                        "views": {
                            "base_view": "client_counts",
                            "extended_view": "baseline_clients_daily_table",
                        },
                    },
                    "growth_accounting": {
                        "type": "growth_accounting_explore",
                        "views": {"base_view": "growth_accounting"},
                    },
                },
                "glean_app": True,
                "owners": [
                    "glean-app-owner@allizom.com",
                    "glean-app-owner2@allizom.com",
                ],
                "pretty_name": "Glean App",
                "views": {
                    "baseline_clients_daily_table": {
                        "tables": [
                            {
                                "channel": "release",
                                "table": "mozdata.glean_app.baseline_clients_daily",
                            },
                            {
                                "channel": "beta",
                                "table": "mozdata.glean_app_beta.baseline_clients_daily",
                            },
                        ],
                        "type": "table_view",
                    },
                    "baseline_clients_last_seen_table": {
                        "tables": [
                            {
                                "channel": "release",
                                "table": "mozdata.glean_app.baseline_clients_last_seen",
                            },
                            {
                                "channel": "beta",
                                "table": "mozdata.glean_app_beta.baseline_clients_last_seen",  # noqa: E501
                            },
                        ],
                        "type": "table_view",
                    },
                    "baseline_table": {
                        "tables": [
                            {
                                "channel": "release",
                                "table": "mozdata.glean_app.baseline",
                            },
                            {
                                "channel": "beta",
                                "table": "mozdata.glean_app_beta.baseline",
                            },
                        ],
                        "type": "table_view",
                    },
                    "baseline": {
                        "tables": [
                            {
                                "channel": "release",
                                "table": "mozdata.glean_app.baseline",
                            },
                            {
                                "channel": "beta",
                                "table": "mozdata.glean_app_beta.baseline",
                            },
                        ],
                        "type": "glean_ping_view",
                    },
                    "client_counts": {
                        "tables": [
                            {"table": "mozdata.glean_app.baseline_clients_daily"}
                        ],
                        "type": "client_counts_view",
                    },
                    "growth_accounting": {
                        "tables": [
                            {"table": "mozdata.glean_app.baseline_clients_last_seen"}
                        ],
                        "type": "growth_accounting_view",
                    },
                },
            },
        }
        actual = yaml.load(Path("namespaces.yaml").read_text(), Loader=yaml.FullLoader)

        print_and_test(expected, actual)


def test_get_glean_apps(app_listings_uri, glean_apps):
    assert _get_glean_apps(app_listings_uri) == glean_apps


def test_get_looker_views(glean_apps, generated_sql_uri):
    db_views = _get_db_views(generated_sql_uri)
    actual = _get_looker_views(glean_apps[0], db_views)
    namespace = glean_apps[0]["name"]
    expected = [
        ClientCountsView(
            namespace,
            [
                {
                    "table": "mozdata.glean_app.baseline_clients_daily",
                }
            ],
        ),
        GleanPingView(
            namespace,
            "baseline",
            [
                {"channel": "release", "table": "mozdata.glean_app.baseline"},
                {"channel": "beta", "table": "mozdata.glean_app_beta.baseline"},
            ],
        ),
        GrowthAccountingView(
            namespace,
            [
                {
                    "table": "mozdata.glean_app.baseline_clients_last_seen",
                }
            ],
        ),
        TableView(
            namespace,
            "baseline_clients_daily_table",
            [
                {
                    "table": "mozdata.glean_app.baseline_clients_daily",
                    "channel": "release",
                },
                {
                    "table": "mozdata.glean_app_beta.baseline_clients_daily",
                    "channel": "beta",
                },
            ],
        ),
        TableView(
            namespace,
            "baseline_table",
            [
                {"table": "mozdata.glean_app.baseline", "channel": "release"},
                {"table": "mozdata.glean_app_beta.baseline", "channel": "beta"},
            ],
        ),
        TableView(
            namespace,
            "baseline_clients_last_seen_table",
            [
                {
                    "table": "mozdata.glean_app.baseline_clients_last_seen",
                    "channel": "release",
                },
                {
                    "table": "mozdata.glean_app_beta.baseline_clients_last_seen",
                    "channel": "beta",
                },
            ],
        ),
    ]

    print_and_test(expected, actual)


def test_get_funnel_view(glean_apps, tmp_path):
    dest = tmp_path / "funnels.tar.gz"
    paths = {
        "sql/moz-fx-data-shared-prod/glean_app/events_daily/metadata.yaml": """
                references:
                  view.sql:
                    - moz-fx-data-shared-prod.glean_app_derived.events_daily_v1""",
        "sql/moz-fx-data-shared-prod/glean_app/event_types/metadata.yaml": """
                references:
                  view.sql:
                    - moz-fx-data-shared-prod.glean_app_derived.event_types_v1""",
    }

    sql_uri = paths_to_tar(dest, paths)

    db_views = _get_db_views(sql_uri)
    actual = _get_looker_views(glean_apps[0], db_views)
    namespace = glean_apps[0]["name"]
    expected = [
        FunnelAnalysisView(
            namespace,
            [
                {
                    "funnel_analysis": "events_daily_table",
                    "event_types": "`mozdata.glean_app.event_types`",
                    "step_1": "event_types",
                    "step_2": "event_types",
                    "step_3": "event_types",
                    "step_4": "event_types",
                }
            ],
        ),
        TableView(
            namespace,
            "events_daily_table",
            [
                {
                    "table": "mozdata.glean_app.events_daily",
                    "channel": "release",
                },
            ],
        ),
        TableView(
            namespace,
            "event_types_table",
            [
                {
                    "table": "mozdata.glean_app.event_types",
                    "channel": "release",
                },
            ],
        ),
    ]

    print_and_test(expected, actual)


def test_get_funnel_explore(glean_apps, tmp_path):
    dest = tmp_path / "funnels.tar.gz"
    paths = {
        "sql/moz-fx-data-shared-prod/glean_app/events_daily/metadata.yaml": """
                references:
                  view.sql:
                    - moz-fx-data-shared-prod.glean_app_derived.events_daily_v1""",
        "sql/moz-fx-data-shared-prod/glean_app/event_types/metadata.yaml": """
                references:
                  view.sql:
                    - moz-fx-data-shared-prod.glean_app_derived.event_types_v1""",
    }

    sql_uri = paths_to_tar(dest, paths)

    db_views = _get_db_views(sql_uri)
    views = _get_looker_views(glean_apps[0], db_views)
    actual = _get_explores(views)
    expected = {
        "funnel_analysis": {
            "type": "funnel_analysis_explore",
            "views": {
                "base_view": "funnel_analysis",
            },
        }
    }

    print_and_test(expected, actual)
