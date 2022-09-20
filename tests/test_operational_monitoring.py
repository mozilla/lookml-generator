import re
from textwrap import dedent

import lkml
import pandas as pd
import pytest
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

from generator.dashboards import OperationalMonitoringDashboard
from generator.explores import OperationalMonitoringExplore
from generator.views import OperationalMonitoringView

from .utils import print_and_test


class MockClient:
    """Mock bigquery.Client."""

    def query(self, query):
        class QueryJob:
            def result(self):
                class ResultObject:
                    def to_dataframe(self):
                        if "summaries" in query:
                            return pd.DataFrame(
                                [
                                    {"metric": "GC_MS", "statistic": "mean"},
                                    {
                                        "metric": "GC_MS_CONTENT",
                                        "statistic": "percentile",
                                    },
                                ],
                                columns=["summary"],
                            )

                        pattern = re.compile("SELECT DISTINCT (.*), COUNT")
                        column = pattern.findall(query)[0]
                        data = [["Windows", 10]]
                        if column == "cores_count":
                            data = [["4", 100]]

                        return pd.DataFrame(data, columns=[column, "count"])

                return ResultObject()

        return QueryJob()

    def get_table(self, table_ref):
        """Mock bigquery.Client.get_table."""
        return bigquery.Table(
            table_ref,
            schema=[
                SchemaField("client_id", "STRING"),
                SchemaField("build_id", "STRING"),
                SchemaField("cores_count", "STRING"),
                SchemaField("os", "STRING"),
                SchemaField("branch", "STRING"),
                SchemaField("metric", "STRING"),
                SchemaField("statistic", "STRING"),
                SchemaField("point", "FLOAT"),
                SchemaField("lower", "FLOAT"),
                SchemaField("upper", "FLOAT"),
                SchemaField("parameter", "FLOAT"),
            ],
        )


@pytest.fixture()
def operational_monitoring_view():
    return OperationalMonitoringView(
        "operational_monitoring",
        "fission",
        [
            {
                "table": "moz-fx-data-shared-prod.operational_monitoring.bug_123_test_statistics",
                "xaxis": "build_id",
                "dimensions": {
                    "cores_count": {
                        "default": "4",
                        "options": ["4", "1"],
                    },
                    "os": {
                        "default": "Windows",
                        "options": ["Windows", "Linux"],
                    },
                },
            }
        ],
    )


@pytest.fixture()
def operational_monitoring_explore(tmp_path, operational_monitoring_view):
    (tmp_path / "fission.view.lkml").write_text(
        lkml.dump(operational_monitoring_view.to_lookml(MockClient(), None))
    )
    return OperationalMonitoringExplore(
        "fission",
        {"base_view": "fission"},
        tmp_path,
        {
            "branches": ["enabled", "disabled"],
            "dimensions": {
                "cores_count": {
                    "default": "4",
                    "options": ["4", "1"],
                },
                "os": {
                    "default": "Windows",
                    "options": ["Windows", "Linux"],
                },
            },
            "summaries": [
                {"metric": "GC_MS", "statistic": "mean"},
                {"metric": "GC_MS_CONTENT", "statistic": "percentile"},
            ],
            "xaxis": "build_id",
        },
    )


@pytest.fixture()
def operational_monitoring_dashboard():
    return OperationalMonitoringDashboard(
        "Fission",
        "fission",
        "newspaper",
        "operational_monitoring",
        [
            {
                "table": "moz-fx-data-shared-prod.operational_monitoring.bug_123_test_statistics",
                "explore": "fission",
                "branches": ["enabled", "disabled"],
                "dimensions": {
                    "cores_count": {
                        "default": "4",
                        "options": ["4", "1"],
                    },
                    "os": {
                        "default": "Windows",
                        "options": ["Windows", "Linux"],
                    },
                },
                "xaxis": "build_id",
                "summaries": [
                    {"metric": "GC_MS", "statistic": "mean"},
                    {"metric": "GC_MS_CONTENT", "statistic": "percentile"},
                ],
            },
        ],
    )


def test_view_from_dict(operational_monitoring_view):
    actual = OperationalMonitoringView.from_dict(
        "operational_monitoring",
        "fission",
        {
            "type": "operational_monitoring_view",
            "tables": [
                {
                    "table": "moz-fx-data-shared-prod.operational_monitoring.bug_123_test_statistics",
                    "xaxis": "build_id",
                    "dimensions": {
                        "cores_count": {
                            "default": "4",
                            "options": ["4", "1"],
                        },
                        "os": {
                            "default": "Windows",
                            "options": ["Windows", "Linux"],
                        },
                    },
                }
            ],
        },
    )

    assert actual == operational_monitoring_view


def test_view_lookml(operational_monitoring_view):
    mock_bq_client = MockClient()
    expected = {
        "views": [
            {
                "dimensions": [
                    {
                        "name": "build_id",
                        "sql": "PARSE_DATE('%Y%m%d', "
                        "CAST(${TABLE}.build_id AS STRING))",
                        "type": "date",
                    },
                    {"name": "branch", "sql": "${TABLE}.branch", "type": "string"},
                    {
                        "name": "cores_count",
                        "sql": "${TABLE}.cores_count",
                        "type": "string",
                    },
                    {"name": "lower", "sql": "${TABLE}.lower", "type": "number"},
                    {"name": "metric", "sql": "${TABLE}.metric", "type": "string"},
                    {"name": "os", "sql": "${TABLE}.os", "type": "string"},
                    {
                        "name": "parameter",
                        "sql": "${TABLE}.parameter",
                        "type": "number",
                    },
                    {"name": "point", "sql": "${TABLE}.point", "type": "number"},
                    {
                        "name": "statistic",
                        "sql": "${TABLE}.statistic",
                        "type": "string",
                    },
                    {"name": "upper", "sql": "${TABLE}.upper", "type": "number"},
                ],
                "name": "fission",
                "sql_table_name": "moz-fx-data-shared-prod.operational_monitoring.bug_123_test_statistics",
            }
        ]
    }
    actual = operational_monitoring_view.to_lookml(mock_bq_client, None)
    print(actual)

    print_and_test(expected=expected, actual=actual)


def test_explore_lookml(operational_monitoring_explore):
    mock_bq_client = MockClient()
    expected = [
        {
            "always_filter": {"filters": [{"branch": "enabled, disabled"}]},
            "name": "fission",
            "hidden": "yes",
        }
    ]

    actual = operational_monitoring_explore.to_lookml(mock_bq_client, None)
    print_and_test(expected=expected, actual=actual)


def test_dashboard_lookml(operational_monitoring_dashboard):
    mock_bq_client = MockClient()
    expected = dedent(
        """\
- dashboard: fission
  title: Fission
  layout: newspaper
  preferred_viewer: dashboards-next

  elements:
  - title: Gc Ms
    name: Gc Ms_mean
    note_state: expanded
    note_display: above
    note_text: Mean
    explore: fission
    type: looker_line
    fields: [
      fission.build_id,
      fission.branch,
      fission.point
    ]
    pivots: [
      fission.branch
    ]
    filters:
      fission.metric: GC_MS
      fission.statistic: mean
    row: 0
    col: 0
    width: 12
    height: 8
    field_x: fission.build_id
    field_y: fission.point
    log_scale: false
    ci_lower: fission.lower
    ci_upper: fission.upper
    show_grid: true
    listen:
      Cores Count: fission.cores_count
      Os: fission.os

    enabled: "#3FE1B0"
    disabled: "#0060E0"
    defaults_version: 0
  - title: Gc Ms Content
    name: Gc Ms Content_percentile
    note_state: expanded
    note_display: above
    note_text: Percentile
    explore: fission
    type: "ci-line-chart"
    fields: [
      fission.build_id,
      fission.branch,
      fission.upper,
      fission.lower,
      fission.point
    ]
    pivots: [
      fission.branch
    ]
    filters:
      fission.metric: GC_MS_CONTENT
      fission.statistic: percentile
    row: 0
    col: 12
    width: 12
    height: 8
    field_x: fission.build_id
    field_y: fission.point
    log_scale: false
    ci_lower: fission.lower
    ci_upper: fission.upper
    show_grid: true
    listen:
      Percentile: fission.parameter
      Cores Count: fission.cores_count
      Os: fission.os

    enabled: "#3FE1B0"
    disabled: "#0060E0"
    defaults_version: 0

  filters:
  - name: Percentile
    title: Percentile
    type: field_filter
    default_value: '50'
    allow_multiple_values: false
    required: true
    ui_config:
      type: slider
      display: inline
      options: []
    model: operational_monitoring
    explore: fission
    listens_to_filters: []
    field: fission.parameter

  - title: Cores Count
    name: Cores Count
    type: string_filter
    default_value: '4'
    allow_multiple_values: false
    required: true
    ui_config:
      type: dropdown_menu
      display: inline
      options:
      - '4'
      - '1'



  - title: Os
    name: Os
    type: string_filter
    default_value: 'Windows'
    allow_multiple_values: false
    required: true
    ui_config:
      type: dropdown_menu
      display: inline
      options:
      - 'Windows'
      - 'Linux'


    """
    )
    actual = operational_monitoring_dashboard.to_lookml(mock_bq_client)

    print_and_test(expected=expected, actual=dedent(actual))
