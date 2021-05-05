from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock, patch

import lkml
import pytest
from click import ClickException
from click.testing import CliRunner
from google.cloud import bigquery
from mozilla_schema_generator.probes import GleanProbe

from generator.lookml import _lookml
from generator.views import GrowthAccountingView

from .utils import print_and_test


@pytest.fixture
def runner():
    return CliRunner()


class MockClient:
    """Mock bigquery.Client."""

    def get_table(self, table_ref):
        """Mock bigquery.Client.get_table."""

        if table_ref == "mozdata.custom.baseline":
            return bigquery.Table(
                table_ref,
                schema=[
                    bigquery.schema.SchemaField("client_id", "STRING"),
                    bigquery.schema.SchemaField("country", "STRING"),
                    bigquery.schema.SchemaField("document_id", "STRING"),
                ],
            )
        if table_ref == "mozdata.glean_app.baseline_clients_last_seen":
            return bigquery.Table(
                table_ref,
                schema=[
                    bigquery.schema.SchemaField("client_id", "STRING"),
                    bigquery.schema.SchemaField("country", "STRING"),
                    bigquery.schema.SchemaField("document_id", "STRING"),
                ],
            )
        if table_ref == "mozdata.glean_app.baseline":
            return bigquery.Table(
                table_ref,
                schema=[
                    bigquery.schema.SchemaField(
                        "client_info",
                        "RECORD",
                        fields=[
                            bigquery.schema.SchemaField("client_id", "STRING"),
                            bigquery.schema.SchemaField(
                                "parsed_first_run_date", "DATE"
                            ),
                        ],
                    ),
                    bigquery.schema.SchemaField(
                        "metadata",
                        "RECORD",
                        fields=[
                            bigquery.schema.SchemaField(
                                "geo",
                                "RECORD",
                                fields=[
                                    bigquery.schema.SchemaField("country", "STRING"),
                                ],
                            ),
                            bigquery.schema.SchemaField(
                                "header",
                                "RECORD",
                                fields=[
                                    bigquery.schema.SchemaField("date", "STRING"),
                                    bigquery.schema.SchemaField(
                                        "parsed_date", "TIMESTAMP"
                                    ),
                                ],
                            ),
                        ],
                    ),
                    bigquery.schema.SchemaField(
                        "metrics",
                        "RECORD",
                        fields=[
                            bigquery.schema.SchemaField(
                                "counter",
                                "RECORD",
                                fields=[
                                    bigquery.schema.SchemaField(
                                        "test_counter", "INTEGER"
                                    )
                                ],
                            )
                        ],
                    ),
                    bigquery.schema.SchemaField("parsed_timestamp", "TIMESTAMP"),
                    bigquery.schema.SchemaField("submission_timestamp", "TIMESTAMP"),
                    bigquery.schema.SchemaField("submission_date", "DATE"),
                    bigquery.schema.SchemaField("test_bignumeric", "BIGNUMERIC"),
                    bigquery.schema.SchemaField("test_bool", "BOOLEAN"),
                    bigquery.schema.SchemaField("test_bytes", "BYTES"),
                    bigquery.schema.SchemaField("test_float64", "FLOAT"),
                    bigquery.schema.SchemaField("test_int64", "INTEGER"),
                    bigquery.schema.SchemaField("test_numeric", "NUMERIC"),
                    bigquery.schema.SchemaField("test_string", "STRING"),
                ],
            )
        if table_ref == "mozdata.glean_app.metrics":
            return bigquery.Table(
                table_ref,
                schema=[
                    bigquery.schema.SchemaField(
                        "client_info",
                        "RECORD",
                        fields=[
                            bigquery.schema.SchemaField("client_id", "STRING"),
                        ],
                    ),
                    bigquery.schema.SchemaField(
                        "metrics",
                        "RECORD",
                        fields=[
                            bigquery.schema.SchemaField(
                                "counter",
                                "RECORD",
                                fields=[
                                    bigquery.schema.SchemaField(
                                        "test_counter", "INTEGER"
                                    ),
                                    bigquery.schema.SchemaField(
                                        "glean_validation_metrics_ping_count", "INTEGER"
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            )
        if table_ref == "mozdata.fail.duplicate_dimension":
            return bigquery.Table(
                table_ref,
                schema=[
                    bigquery.schema.SchemaField("parsed_timestamp", "TIMESTAMP"),
                    bigquery.schema.SchemaField("parsed_date", "DATE"),
                ],
            )
        if table_ref == "mozdata.fail.duplicate_client":
            return bigquery.Table(
                table_ref,
                schema=[
                    bigquery.schema.SchemaField(
                        "client_info",
                        "RECORD",
                        fields=[
                            bigquery.schema.SchemaField("client_id", "STRING"),
                        ],
                    ),
                    bigquery.schema.SchemaField("client_id", "STRING"),
                ],
            )
        raise ValueError(f"Table not found: {table_ref}")


@patch("generator.views.glean_ping_view.GleanPing")
def test_lookml_actual(mock_glean_ping, runner, glean_apps, tmp_path):
    namespaces = tmp_path / "namespaces.yaml"
    namespaces.write_text(
        dedent(
            """
            custom:
              pretty_name: Custom
              glean_app: false
              views:
                baseline:
                  type: ping_view
                  tables:
                  - channel: release
                    table: mozdata.custom.baseline
            glean-app:
              pretty_name: Glean App
              glean_app: true
              views:
                baseline:
                  type: glean_ping_view
                  tables:
                  - channel: release
                    table: mozdata.glean_app.baseline
                  - channel: beta
                    table: mozdata.glean_app_beta.baseline
                metrics:
                  type: glean_ping_view
                  tables:
                  - channel: release
                    table: mozdata.glean_app.metrics
                growth_accounting:
                  type: growth_accounting_view
                  tables:
                  - table: mozdata.glean_app.baseline_clients_last_seen
              explores:
                baseline:
                  type: glean_ping_explore
                  views:
                    base_view: baseline
                growth_accounting:
                  type: growth_accounting_explore
                  views:
                    base_view: growth_accounting
            """
        )
    )
    mock_glean_ping.get_repos.return_value = [{"name": "glean-app-release"}]
    glean_app = Mock()
    mock_glean_ping.return_value = glean_app
    history = [
        {"dates": {"first": "2020-01-01 00:00:00", "last": "2020-01-02 00:00:00"}}
    ]
    glean_app.get_probes.return_value = [
        GleanProbe(
            "test.counter",
            {"type": "counter", "history": history, "name": "test.counter"},
        ),
        GleanProbe(
            "glean.validation.metrics_ping_count",
            {
                "type": "counter",
                "history": history,
                "name": "glean.validation.metrics_ping_count",
            },
        ),
    ]
    with runner.isolated_filesystem():
        with patch("google.cloud.bigquery.Client", MockClient):
            _lookml(open(namespaces), glean_apps, "looker-hub/")
        expected = {
            "views": [
                {
                    "name": "baseline",
                    "sql_table_name": "`mozdata.custom.baseline`",
                    "dimensions": [
                        {
                            "name": "client_id",
                            "hidden": "yes",
                            "sql": "${TABLE}.client_id",
                        },
                        {
                            "name": "country",
                            "map_layer_name": "countries",
                            "sql": "${TABLE}.country",
                            "type": "string",
                        },
                        {
                            "name": "document_id",
                            "hidden": "yes",
                            "sql": "${TABLE}.document_id",
                        },
                    ],
                    "measures": [
                        {
                            "name": "clients",
                            "type": "count_distinct",
                            "sql": "${client_id}",
                        },
                        {
                            "name": "ping_count",
                            "type": "count",
                        },
                    ],
                }
            ]
        }
        print_and_test(
            expected,
            lkml.load(Path("looker-hub/custom/views/baseline.view.lkml").read_text()),
        )
        expected = {
            "views": [
                {
                    "name": "baseline",
                    "parameters": [
                        {
                            "name": "channel",
                            "type": "unquoted",
                            "allowed_values": [
                                {
                                    "label": "Release",
                                    "value": "mozdata.glean_app.baseline",
                                },
                                {
                                    "label": "Beta",
                                    "value": "mozdata.glean_app_beta.baseline",
                                },
                            ],
                        }
                    ],
                    "sql_table_name": "`{% parameter channel %}`",
                    "dimensions": [
                        {
                            "name": "client_info__client_id",
                            "hidden": "yes",
                            "sql": "${TABLE}.client_info.client_id",
                        },
                        {
                            "name": "metadata__geo__country",
                            "map_layer_name": "countries",
                            "group_item_label": "Country",
                            "group_label": "Metadata Geo",
                            "sql": "${TABLE}.metadata.geo.country",
                            "type": "string",
                        },
                        {
                            "name": "metadata__header__date",
                            "group_item_label": "Date",
                            "group_label": "Metadata Header",
                            "sql": "${TABLE}.metadata.header.date",
                            "type": "string",
                        },
                        {
                            "group_item_label": "Counter",
                            "group_label": "Test",
                            "name": "metrics__counter__test_counter",
                            "sql": "${TABLE}.metrics.counter.test_counter",
                            "type": "number",
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary "
                                    "reference for Test "
                                    "Counter",
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/test_counter",  # noqa: E501
                                }
                            ],
                        },
                        {
                            "name": "test_bignumeric",
                            "sql": "${TABLE}.test_bignumeric",
                            "type": "string",
                        },
                        {
                            "name": "test_bool",
                            "sql": "${TABLE}.test_bool",
                            "type": "yesno",
                        },
                        {
                            "name": "test_bytes",
                            "sql": "${TABLE}.test_bytes",
                            "type": "string",
                        },
                        {
                            "name": "test_float64",
                            "sql": "${TABLE}.test_float64",
                            "type": "number",
                        },
                        {
                            "name": "test_int64",
                            "sql": "${TABLE}.test_int64",
                            "type": "number",
                        },
                        {
                            "name": "test_numeric",
                            "sql": "${TABLE}.test_numeric",
                            "type": "number",
                        },
                        {
                            "name": "test_string",
                            "sql": "${TABLE}.test_string",
                            "type": "string",
                        },
                    ],
                    "dimension_groups": [
                        {
                            "name": "client_info__parsed_first_run",
                            "convert_tz": "no",
                            "datatype": "date",
                            "group_item_label": "Parsed First Run Date",
                            "group_label": "Client Info",
                            "sql": "${TABLE}.client_info.parsed_first_run_date",
                            "timeframes": [
                                "raw",
                                "date",
                                "week",
                                "month",
                                "quarter",
                                "year",
                            ],
                            "type": "time",
                        },
                        {
                            "name": "metadata__header__parsed",
                            "group_item_label": "Parsed Date",
                            "group_label": "Metadata Header",
                            "sql": "${TABLE}.metadata.header.parsed_date",
                            "timeframes": [
                                "raw",
                                "time",
                                "date",
                                "week",
                                "month",
                                "quarter",
                                "year",
                            ],
                            "type": "time",
                        },
                        {
                            "name": "parsed",
                            "sql": "${TABLE}.parsed_timestamp",
                            "timeframes": [
                                "raw",
                                "time",
                                "date",
                                "week",
                                "month",
                                "quarter",
                                "year",
                            ],
                            "type": "time",
                        },
                        {
                            "name": "submission",
                            "sql": "${TABLE}.submission_timestamp",
                            "timeframes": [
                                "raw",
                                "time",
                                "date",
                                "week",
                                "month",
                                "quarter",
                                "year",
                            ],
                            "type": "time",
                        },
                    ],
                    "measures": [
                        {
                            "name": "clients",
                            "type": "count_distinct",
                            "sql": "${client_info__client_id}",
                        },
                        {
                            "name": "test_counter",
                            "type": "sum",
                            "sql": "${metrics__counter__test_counter}",
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary "
                                    "reference for Test "
                                    "Counter",
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/test_counter",  # noqa: E501
                                }
                            ],
                        },
                        {
                            "name": "test_counter_client_count",
                            "type": "count_distinct",
                            "sql": (
                                "case when ${metrics__counter__test_counter} > 0 then "
                                "${client_info__client_id}"
                            ),
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary "
                                    "reference for Test "
                                    "Counter",
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/test_counter",  # noqa: E501
                                }
                            ],
                        },
                    ],
                }
            ]
        }

        print_and_test(
            expected,
            lkml.load(
                Path("looker-hub/glean-app/views/baseline.view.lkml").read_text()
            ),
        )
        expected = {
            "views": [
                {
                    "name": "metrics",
                    "sql_table_name": "`mozdata.glean_app.metrics`",
                    "dimensions": [
                        {
                            "name": "client_info__client_id",
                            "hidden": "yes",
                            "sql": "${TABLE}.client_info.client_id",
                        },
                        {
                            "group_item_label": "Metrics Ping Count",
                            "group_label": "Glean Validation",
                            "name": "metrics__counter__glean_validation_metrics_ping_count",
                            "sql": "${TABLE}.metrics.counter.glean_validation_metrics_ping_count",  # noqa: E501
                            "type": "number",
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary reference for Glean Validation Metrics Ping Count",  # noqa: E501
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/glean_validation_metrics_ping_count",  # noqa: E501
                                }
                            ],
                        },
                        {
                            "group_item_label": "Counter",
                            "group_label": "Test",
                            "name": "metrics__counter__test_counter",
                            "sql": "${TABLE}.metrics.counter.test_counter",
                            "type": "number",
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary reference for Test Counter",
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/test_counter",  # noqa: E501
                                }
                            ],
                        },
                    ],
                    "measures": [
                        {
                            "name": "clients",
                            "type": "count_distinct",
                            "sql": "${client_info__client_id}",
                        },
                        {
                            "name": "glean_validation_metrics_ping_count",
                            "type": "sum",
                            "sql": "${metrics__counter__glean_validation_metrics_ping_count}",  # noqa: E501
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary "
                                    "reference for Glean Validation Metrics Ping Count",
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/glean_validation_metrics_ping_count",  # noqa: E501
                                }
                            ],
                        },
                        {
                            "name": "glean_validation_metrics_ping_count_client_count",
                            "type": "count_distinct",
                            "sql": (
                                "case when ${metrics__counter__glean_validation_metrics_ping_count} > 0 then "
                                "${client_info__client_id}"
                            ),
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary "
                                    "reference for Glean Validation Metrics Ping Count",
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/glean_validation_metrics_ping_count",  # noqa: E501
                                }
                            ],
                        },
                        {
                            "name": "test_counter",
                            "type": "sum",
                            "sql": "${metrics__counter__test_counter}",
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary "
                                    "reference for Test "
                                    "Counter",
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/test_counter",  # noqa: E501
                                }
                            ],
                        },
                        {
                            "name": "test_counter_client_count",
                            "type": "count_distinct",
                            "sql": (
                                "case when ${metrics__counter__test_counter} > 0 then "
                                "${client_info__client_id}"
                            ),
                            "links": [
                                {
                                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",  # noqa: E501
                                    "label": "Glean Dictionary "
                                    "reference for Test "
                                    "Counter",
                                    "url": "https://dictionary.telemetry.mozilla.org/apps/glean-app/metrics/test_counter",  # noqa: E501
                                }
                            ],
                        },
                    ],
                }
            ]
        }
        print_and_test(
            expected,
            lkml.load(Path("looker-hub/glean-app/views/metrics.view.lkml").read_text()),
        )
        expected = {
            "views": [
                {
                    "name": "growth_accounting",
                    "sql_table_name": "`mozdata.glean_app.baseline_clients_last_seen`",
                    "dimensions": [
                        {
                            "name": "client_id",
                            "hidden": "yes",
                            "sql": "${TABLE}.client_id",
                        },
                        {
                            "name": "country",
                            "map_layer_name": "countries",
                            "sql": "${TABLE}.country",
                            "type": "string",
                        },
                        {
                            "name": "document_id",
                            "hidden": "yes",
                            "sql": "${TABLE}.document_id",
                        },
                    ]
                    + GrowthAccountingView.default_dimensions,
                    "measures": GrowthAccountingView.default_measures,
                }
            ]
        }

        # lkml changes the format of lookml, so we need to cycle it through to match
        print_and_test(
            lkml.load(lkml.dump(expected)),
            lkml.load(
                Path(
                    "looker-hub/glean-app/views/growth_accounting.view.lkml"
                ).read_text()
            ),
        )

        expected = {
            "includes": ["/looker-hub/glean-app/views/baseline.view.lkml"],
            "explores": [
                {
                    "name": "baseline",
                    "view_name": "baseline",
                    "always_filter": {
                        "filters": [
                            {"submission_date": "28 days"},
                            {"channel": "mozdata.glean^_app.baseline"},
                        ]
                    },
                }
            ],
        }
        print_and_test(
            lkml.load(lkml.dump(expected)),
            lkml.load(
                Path("looker-hub/glean-app/explores/baseline.explore.lkml").read_text()
            ),
        )


def test_duplicate_dimension(runner, glean_apps, tmp_path):
    namespaces = tmp_path / "namespaces.yaml"
    namespaces.write_text(
        dedent(
            """
            custom:
              pretty_name: Custom
              glean_app: false
              views:
                baseline:
                  type: ping_view
                  tables:
                  - channel: release
                    table: mozdata.fail.duplicate_dimension
            """
        )
    )
    with runner.isolated_filesystem():
        with patch("google.cloud.bigquery.Client", MockClient):
            with pytest.raises(ClickException):
                _lookml(open(namespaces), glean_apps, "looker-hub/")


def test_duplicate_client_id(runner, glean_apps, tmp_path):
    namespaces = tmp_path / "namespaces.yaml"
    namespaces.write_text(
        dedent(
            """
            custom:
              pretty_name: Custom
              glean_app: false
              views:
                baseline:
                  type: ping_view
                  tables:
                  - channel: release
                    table: mozdata.fail.duplicate_client
            """
        )
    )
    with runner.isolated_filesystem():
        with patch("google.cloud.bigquery.Client", MockClient):
            with pytest.raises(ClickException):
                _lookml(open(namespaces), glean_apps, "looker-hub/")
