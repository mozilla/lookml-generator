import re
from textwrap import dedent

import lkml
import pandas as pd
import pytest
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

from generator.dashboards import OperationalMonitoringDashboard
from generator.explores import OperationalMonitoringExplore
from generator.views import (
    OperationalMonitoringHistogramView,
    OperationalMonitoringScalarView,
)

from .utils import print_and_test

TABLE_HISTOGRAM = (
    "moz-fx-data-shared-prod."
    "operational_monitoring."
    "bug_1660366_pref_ongoing_fission_nightly_experiment_nightly_83_100_histogram"
)

TABLE_SCALAR = (
    "moz-fx-data-shared-prod."
    "operational_monitoring."
    "bug_1660366_pref_ongoing_fission_nightly_experiment_nightly_83_100_scalar"
)


class MockClient:
    """Mock bigquery.Client."""

    def query(self, query):
        class QueryJob:
            def result(self):
                class ResultObject:
                    def to_dataframe(self):
                        if "probe" in query:
                            return pd.DataFrame(
                                [["GC_MS"], ["GC_MS_CONTENT"]], columns=["probe"]
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

        if table_ref == TABLE_HISTOGRAM:
            return bigquery.Table(
                table_ref,
                schema=[
                    SchemaField("client_id", "STRING"),
                    SchemaField("build_id", "STRING"),
                    SchemaField("cores_count", "STRING"),
                    SchemaField("os", "STRING"),
                    SchemaField("branch", "STRING"),
                    SchemaField("probe", "STRING"),
                    SchemaField(
                        "histogram",
                        "RECORD",
                        fields=[
                            SchemaField("bucket_count", "INTEGER"),
                            SchemaField("sum", "INTEGER"),
                            SchemaField("histogram_type", "INTEGER"),
                            SchemaField("range", "INTEGER", "REPEATED"),
                            SchemaField(
                                "VALUES",
                                "RECORD",
                                fields=[
                                    SchemaField("key", "INTEGER"),
                                    SchemaField("value", "INTEGER"),
                                ],
                            ),
                        ],
                    ),
                ],
            )

        if table_ref == TABLE_SCALAR:
            return bigquery.Table(
                table_ref,
                schema=[
                    SchemaField("client_id", "STRING"),
                    SchemaField("build_id", "STRING"),
                    SchemaField("cores_count", "STRING"),
                    SchemaField("os", "STRING"),
                    SchemaField("branch", "STRING"),
                    SchemaField("probe", "STRING"),
                    SchemaField("agg_type", "STRING"),
                    SchemaField("value", "STRING"),
                ],
            )

        raise ValueError(f"Table not found: {table_ref}")


@pytest.fixture()
def operational_monitoring_histogram_view():
    return OperationalMonitoringHistogramView(
        "operational_monitoring",
        "fission",
        [{"table": TABLE_HISTOGRAM}],
    )


@pytest.fixture()
def operational_monitoring_scalar_view():
    return OperationalMonitoringScalarView(
        "operational_monitoring",
        "fission",
        [{"table": TABLE_SCALAR}],
    )


@pytest.fixture()
def operational_monitoring_explore(tmp_path, operational_monitoring_histogram_view):
    (tmp_path / "fission_histogram.view.lkml").write_text(
        lkml.dump(operational_monitoring_histogram_view.to_lookml(MockClient(), None))
    )
    return OperationalMonitoringExplore(
        "fission_histogram",
        {"base_view": "fission_histogram"},
        tmp_path,
        {"branches": ["enabled", "disabled"]},
    )


@pytest.fixture()
def operational_monitoring_dashboard():
    return OperationalMonitoringDashboard(
        "Fission",
        "fission",
        "newspaper",
        "operational_monitoring",
        [{"table": TABLE_HISTOGRAM, "explore": "fission_histogram"}],
    )


def test_view_from_dict(operational_monitoring_histogram_view):
    actual = OperationalMonitoringHistogramView.from_dict(
        "operational_monitoring",
        "fission",
        {
            "type": "operational_monitoring_histogram_view",
            "tables": [{"table": TABLE_HISTOGRAM}],
        },
    )

    assert actual == operational_monitoring_histogram_view


def test_histogram_view_lookml(operational_monitoring_histogram_view):
    mock_bq_client = MockClient()
    expected = {
        "views": [
            {
                "name": "fission_histogram",
                "sql_table_name": (
                    "moz-fx-data-shared-prod.operational_monitoring.bug_1660366_"
                    "pref_ongoing_fission_nightly_experiment_nightly_83_100_histogram"
                ),
                "parameters": operational_monitoring_histogram_view.parameters,
                "measures": [
                    operational_monitoring_histogram_view._percentile_measure(label)
                    for label in operational_monitoring_histogram_view.percentile_ci_labels
                ],
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
                    {
                        "group_item_label": "Key",
                        "group_label": "Histogram Values",
                        "name": "histogram__VALUES__key",
                        "sql": "${TABLE}.histogram.VALUES.key",
                        "type": "number",
                    },
                    {
                        "group_item_label": "Value",
                        "group_label": "Histogram Values",
                        "name": "histogram__VALUES__value",
                        "sql": "${TABLE}.histogram.VALUES.value",
                        "type": "number",
                    },
                    {"name": "os", "sql": "${TABLE}.os", "type": "string"},
                    {"name": "probe", "sql": "${TABLE}.probe", "type": "string"},
                ],
            }
        ],
    }
    actual = operational_monitoring_histogram_view.to_lookml(mock_bq_client, None)

    print_and_test(expected=expected, actual=actual)


def test_scalar_view_lookml(operational_monitoring_scalar_view):
    mock_bq_client = MockClient()
    expected = {
        "views": [
            {
                "name": "fission_scalar",
                "derived_table": {
                    "sql": dedent(
                        f"""
                        SELECT *
                        FROM `{TABLE_SCALAR}`
                        WHERE agg_type = "SUM"
                        """
                    )
                },
                "parameters": operational_monitoring_scalar_view.parameters,
                "measures": [
                    operational_monitoring_scalar_view._percentile_measure(label)
                    for label in operational_monitoring_scalar_view.percentile_ci_labels
                ],
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
                    {"name": "os", "sql": "${TABLE}.os", "type": "string"},
                    {"name": "probe", "sql": "${TABLE}.probe", "type": "string"},
                ],
            }
        ],
    }
    actual = operational_monitoring_scalar_view.to_lookml(mock_bq_client, None)

    print_and_test(expected=expected, actual=actual)


def test_explore_lookml(operational_monitoring_explore):
    expected = [
        {
            "name": "fission_histogram",
            "always_filter": {
                "filters": [
                    {"os": "Windows"},
                    {"branch": "enabled, disabled"},
                ]
            },
        },
    ]

    actual = operational_monitoring_explore.to_lookml(None)
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
          name: Gc Ms
          explore: fission_histogram
          type: "looker_line"
          fields: [
            fission_histogram.build_id,
            fission_histogram.branch,
            fission_histogram.high,
            fission_histogram.low,
            fission_histogram.percentile
          ]
          pivots: [fission_histogram.branch]
          filters:
            fission_histogram.probe: GC_MS
          row: 0
          col: 0
          width: 12
          height: 8
          listen:
            Percentile: fission_histogram.percentile_conf
            Cores Count: fission_histogram.cores_count
            Os: fission_histogram.os

        - title: Gc Ms Content
          name: Gc Ms Content
          explore: fission_histogram
          type: "looker_line"
          fields: [
            fission_histogram.build_id,
            fission_histogram.branch,
            fission_histogram.high,
            fission_histogram.low,
            fission_histogram.percentile
          ]
          pivots: [fission_histogram.branch]
          filters:
            fission_histogram.probe: GC_MS_CONTENT
          row: 0
          col: 12
          width: 12
          height: 8
          listen:
            Percentile: fission_histogram.percentile_conf
            Cores Count: fission_histogram.cores_count
            Os: fission_histogram.os

        filters:
        - name: Percentile
          title: Percentile
          type: number_filter
          default_value: '50'
          allow_multiple_values: false
          required: true
          ui_config:
            type: dropdown_menu
            display: inline
            options:
            - '10'
            - '20'
            - '30'
            - '40'
            - '50'
            - '60'
            - '70'
            - '80'
            - '90'
            - '95'
            - '99'

        - title: Cores Count
          name: Cores Count
          type: string_filter
          default_value: 4
          allow_multiple_values: false
          required: true
          ui_config:
            type: dropdown_menu
            display: inline
            options:
            - 4

        - title: Os
          name: Os
          type: string_filter
          default_value: Windows
          allow_multiple_values: false
          required: true
          ui_config:
            type: dropdown_menu
            display: inline
            options:
            - Windows

    """
    )
    actual, _ = operational_monitoring_dashboard.to_lookml(mock_bq_client)

    print_and_test(expected=expected, actual=dedent(actual))
