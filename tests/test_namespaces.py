import shutil
import sys
import tarfile
from io import BytesIO
from pathlib import Path
from textwrap import dedent
from typing import Dict
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner
from git import Repo

from generator.namespaces import (
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
from generator.views.lookml_utils import get_bigquery_view_reference_map

from .utils import print_and_test

TEST_DIR = Path(__file__).parent


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def custom_namespaces(tmp_path):
    dest = tmp_path / "custom-namespaces.yaml"
    dest.write_text(
        dedent(
            """
            operational_monitoring:
                owners:
                - opmon-owner@allizom.com
                pretty_name: Operational Monitoring
                views:
                  projects:
                    tables:
                    - table: mozdata.operational_monitoring.projects
            firefox_accounts:
              pretty_name: Firefox Accounts
              glean_app: false
              owners:
              - custom-owner@allizom.com
              views:
                growth_accounting:
                  type: growth_accounting_view
                  identifier_field: user_id
                  tables:
                  - table: mozdata.firefox_accounts.fxa_users_last_seen
            custom:
              connection: bigquery-oauth
              glean_app: false
              pretty_name: Custom
              owners:
              - custom-owner@allizom.com
              - custom-owner2@allizom.com
              views:
                baseline:
                  type: ping_view
                  tables:
                  - channel: release
                    table: mozdata.custom.baseline
                partitioned_table:
                  type: table_view
                  tables:
                  - table: mozdata.custom.partitioned_table
                    time_partitioning_field: timestamp
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
            disallowed_wildcard:
              pretty_name: Disallowed Wildcard
              owners:
              - disallowed-owner@allizom.com
              views:
                baseline:
                  type: ping_view
                  tables:
                  - channel: release
                    table: mozdata.disallowed.baseline
            private:
              pretty_name: Private
              spoke: looker-spoke-private
              owners:
              - private-owner@allizom.com
              views:
                events:
                  type: ping_view
                  tables:
                  - table: mozdata.private.events
            glean-app:
              owners:
              - glean-app-owner2@allizom.com
            """
        ).lstrip()
    )
    return dest.absolute()


@pytest.fixture
def namespace_disallowlist(tmp_path):
    dest = tmp_path / "namespaces-disallowlist.yaml"
    dest.write_text(
        dedent(
            """
            ---
            - disallowed
            - disallowed_*
            """
        )
    )
    return dest.absolute()


class MockClient:
    """Mock bigquery.Client."""

    def query(self, query):
        class QueryJob:
            def result(self):
                if "os AS option" in query:
                    return [
                        {
                            "option": "Windows",
                            "count": "10",
                        },
                        {
                            "option": "Linux",
                            "count": "1",
                        },
                    ]
                elif "cores_count AS option" in query:
                    return [
                        {
                            "option": "4",
                            "count": "10",
                        },
                        {
                            "option": "1",
                            "count": "1",
                        },
                    ]
                else:
                    return [
                        {
                            "slug": "op-mon",
                            "name": "OpMon",
                            "branches": ["enabled", "disabled"],
                            "xaxis": "submission_date",
                            "summaries": [
                                {"metric": "GC_MS", "statistic": "mean"},
                                {"metric": "GC_MS_CONTENT", "statistic": "percentile"},
                            ],
                            "dimensions": {
                                "cores_count": {"default": "4", "options": ["4", "1"]}
                            },
                        }
                    ]

        return QueryJob()


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
    for dataset, source_dataset in (
        ("glean_app", "glean_app_release"),
        ("glean_app_beta", "glean_app_beta_stable"),
    ):
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
              - moz-fx-data-shared-prod.{source_dataset}.baseline_v1
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
    runner,
    custom_namespaces,
    generated_sql_uri,
    app_listings_uri,
    namespace_disallowlist,
    tmp_path,
):
    with patch("google.cloud.bigquery.Client", MockClient):
        with runner.isolated_filesystem():
            r = Repo.create(tmp_path)
            r.config_writer().set_value("user", "name", "test").release()
            r.config_writer().set_value("user", "email", "test@example.com").release()
            shutil.copytree(TEST_DIR / "data", tmp_path, dirs_exist_ok=True)
            r.git.add(".")
            r.git.commit("-m", "commit", "--date", "Mon 25 Aug 2020 20:00:19 UTC")

            result = runner.invoke(
                namespaces,
                [
                    "--custom-namespaces",
                    custom_namespaces,
                    "--generated-sql-uri",
                    generated_sql_uri,
                    "--app-listings-uri",
                    app_listings_uri,
                    "--disallowlist",
                    namespace_disallowlist,
                    "--metric-hub-repos",
                    tmp_path / "metric-hub",
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
                    "connection": "bigquery-oauth",
                    "glean_app": False,
                    "owners": ["custom-owner@allizom.com", "custom-owner2@allizom.com"],
                    "pretty_name": "Custom",
                    "spoke": "looker-spoke-default",
                    "views": {
                        "baseline": {
                            "tables": [
                                {
                                    "channel": "release",
                                    "table": "mozdata.custom.baseline",
                                }
                            ],
                            "type": "ping_view",
                        },
                        "partitioned_table": {
                            "tables": [
                                {
                                    "table": "mozdata.custom.partitioned_table",
                                    "time_partitioning_field": "timestamp",
                                }
                            ],
                            "type": "table_view",
                        },
                    },
                },
                "fenix": {
                    "explores": {
                        "metric_definitions_baseline": {
                            "type": "metric_definitions_explore",
                            "views": {"base_view": "metric_definitions_baseline"},
                        },
                        "metric_definitions_metrics": {
                            "type": "metric_definitions_explore",
                            "views": {"base_view": "metric_definitions_metrics"},
                        },
                    },
                    "glean_app": False,
                    "pretty_name": "Fenix",
                    "spoke": "looker-spoke-default",
                    "views": {
                        "metric_definitions_baseline": {
                            "type": "metric_definitions_view"
                        },
                        "metric_definitions_metrics": {
                            "type": "metric_definitions_view"
                        },
                    },
                },
                "firefox_accounts": {
                    "glean_app": False,
                    "owners": [
                        "custom-owner@allizom.com",
                    ],
                    "pretty_name": "Firefox Accounts",
                    "spoke": "looker-spoke-default",
                    "views": {
                        "growth_accounting": {
                            "type": "growth_accounting_view",
                            "identifier_field": "user_id",
                            "tables": [
                                {
                                    "table": "mozdata.firefox_accounts.fxa_users_last_seen",
                                }
                            ],
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
                    "spoke": "looker-spoke-default",
                    "views": {
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
                                    "table": "mozdata.glean_app_beta.baseline_clients_last_seen",
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
                        "client_counts": {
                            "tables": [
                                {"table": "mozdata.glean_app.baseline_clients_daily"}
                            ],
                            "type": "client_counts_view",
                        },
                        "growth_accounting": {
                            "tables": [
                                {
                                    "table": "mozdata.glean_app.baseline_clients_last_seen"
                                }
                            ],
                            "type": "growth_accounting_view",
                        },
                    },
                },
                "operational_monitoring": {
                    "dashboards": {
                        "op_mon": {
                            "tables": [
                                {
                                    "branches": ["enabled", "disabled"],
                                    "compact_visualization": False,
                                    "dimensions": {
                                        "cores_count": {
                                            "default": "4",
                                            "options": ["4", "1"],
                                        }
                                    },
                                    "explore": "op_mon",
                                    "group_by_dimension": None,
                                    "summaries": [
                                        {"metric": "GC_MS", "statistic": "mean"},
                                        {
                                            "metric": "GC_MS_CONTENT",
                                            "statistic": "percentile",
                                        },
                                    ],
                                    "table": "moz-fx-data-shared-prod.operational_monitoring.op_mon_statistics",
                                    "xaxis": "submission_date",
                                }
                            ],
                            "title": "Opmon",
                            "type": "operational_monitoring_dashboard",
                        }
                    },
                    "explores": {
                        "op_mon": {
                            "branches": ["enabled", "disabled"],
                            "dimensions": {
                                "cores_count": {"default": "4", "options": ["4", "1"]}
                            },
                            "summaries": [
                                {"metric": "GC_MS", "statistic": "mean"},
                                {"metric": "GC_MS_CONTENT", "statistic": "percentile"},
                            ],
                            "type": "operational_monitoring_explore",
                            "views": {"base_view": "op_mon"},
                            "xaxis": "submission_date",
                        }
                    },
                    "glean_app": False,
                    "owners": ["opmon-owner@allizom.com"],
                    "pretty_name": "Operational Monitoring",
                    "spoke": "looker-spoke-default",
                    "views": {
                        "op_mon": {
                            "tables": [
                                {
                                    "dimensions": {
                                        "cores_count": {
                                            "default": "4",
                                            "options": ["4", "1"],
                                        }
                                    },
                                    "table": "moz-fx-data-shared-prod.operational_monitoring.op_mon_statistics",
                                    "xaxis": "submission_date",
                                }
                            ],
                            "type": "operational_monitoring_view",
                        }
                    },
                },
                "private": {
                    "glean_app": False,
                    "owners": ["private-owner@allizom.com"],
                    "pretty_name": "Private",
                    "spoke": "looker-spoke-private",
                    "views": {
                        "events": {
                            "tables": [{"table": "mozdata.private.events"}],
                            "type": "ping_view",
                        }
                    },
                },
            }

            actual = yaml.load(
                Path("namespaces.yaml").read_text(), Loader=yaml.FullLoader
            )

            print_and_test(expected, actual)


def test_get_glean_apps(app_listings_uri, glean_apps):
    assert _get_glean_apps(app_listings_uri) == glean_apps


def test_get_looker_views(glean_apps, generated_sql_uri):
    db_views = get_bigquery_view_reference_map(generated_sql_uri)
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

    db_views = get_bigquery_view_reference_map(sql_uri)
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

    db_views = get_bigquery_view_reference_map(sql_uri)
    views = _get_looker_views(glean_apps[0], db_views)
    actual = _get_explores(views)
    expected = {
        "funnel_analysis": {
            "type": "funnel_analysis_explore",
            "views": {"base_view": "funnel_analysis"},
        }
    }

    print_and_test(expected, actual)
