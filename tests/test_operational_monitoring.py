from textwrap import dedent

import lkml
import pytest
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

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
