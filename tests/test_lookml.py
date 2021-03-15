import sys
import traceback
from pathlib import Path
from textwrap import dedent
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from google.cloud import bigquery

from generator.lookml import lookml


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def namespaces(tmp_path):
    dest = tmp_path / "namespaces.yaml"
    dest.write_text(
        dedent(
            """
            custom:
              canonical_app_name: Custom
              views:
                baseline:
                - channel: release
                  table: mozdata.custom.baseline
            glean-app:
              canonical_app_name: Glean App
              views:
                baseline:
                - channel: release
                  table: mozdata.glean_app.baseline
                - channel: beta
                  table: mozdata.glean_app_beta.baseline
            """
        )
    )
    return dest.absolute()


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
                    bigquery.schema.SchemaField("parsed_timestamp", "TIMESTAMP"),
                    bigquery.schema.SchemaField("submission_timestamp", "TIMESTAMP"),
                    bigquery.schema.SchemaField("test_bignumeric", "BIGNUMERIC"),
                    bigquery.schema.SchemaField("test_bool", "BOOLEAN"),
                    bigquery.schema.SchemaField("test_bytes", "BYTES"),
                    bigquery.schema.SchemaField("test_float64", "FLOAT"),
                    bigquery.schema.SchemaField("test_int64", "INTEGER"),
                    bigquery.schema.SchemaField("test_numeric", "NUMERIC"),
                    bigquery.schema.SchemaField("test_string", "STRING"),
                ],
            )
        raise ValueError(f"Table not found: {table_ref}")


def test_lookml(runner, namespaces):
    with runner.isolated_filesystem():
        with patch("google.cloud.bigquery.Client", MockClient):
            result = runner.invoke(
                lookml,
                [
                    "--namespaces",
                    namespaces,
                ],
            )
        sys.stdout.write(result.stdout)
        if result.stderr_bytes is not None:
            sys.stderr.write(result.stderr)
        try:
            assert not result.exception
        except Exception:
            traceback.print_tb(result.exc_info[2])
            raise
        assert (
            dedent(
                """
            view: baseline {
              sql_table_name: `mozdata.custom.baseline` ;;

              dimension: client_id {
                hidden: yes
                sql: ${TABLE}.client_id ;;
              }

              dimension: country {
                map_layer_name: countries
                sql: ${TABLE}.country ;;
                type: string
              }

              dimension: document_id {
                hidden: yes
                sql: ${TABLE}.document_id ;;
              }

              measure: clients {
                sql: COUNT(DISTINCT client_id) ;;
                type: number
              }

              measure: ping_count {
                type: count
              }
            }
            """
            ).strip()
            == Path("looker-hub/custom/views/baseline.view.lkml").read_text()
        )
        assert (
            dedent(
                """
            view: baseline {
              sql_table_name: `mozdata.glean_app.baseline` ;;

              dimension: client_info__client_id {
                hidden: yes
                sql: ${TABLE}.client_info.client_id ;;
              }

              dimension: metadata__geo__country {
                group_item_label: "Country"
                group_label: "Metadata Geo"
                map_layer_name: countries
                sql: ${TABLE}.metadata.geo.country ;;
                type: string
              }

              dimension: metadata__header__date {
                group_item_label: "Date"
                group_label: "Metadata Header"
                sql: ${TABLE}.metadata.header.date ;;
                type: string
              }

              dimension: test_bignumeric {
                sql: ${TABLE}.test_bignumeric ;;
                type: string
              }

              dimension: test_bool {
                sql: ${TABLE}.test_bool ;;
                type: yesno
              }

              dimension: test_bytes {
                sql: ${TABLE}.test_bytes ;;
                type: string
              }

              dimension: test_float64 {
                sql: ${TABLE}.test_float64 ;;
                type: number
              }

              dimension: test_int64 {
                sql: ${TABLE}.test_int64 ;;
                type: number
              }

              dimension: test_numeric {
                sql: ${TABLE}.test_numeric ;;
                type: number
              }

              dimension: test_string {
                sql: ${TABLE}.test_string ;;
                type: string
              }

              dimension_group: client_info__parsed_first_run {
                convert_tz: no
                datatype: date
                group_item_label: "Parsed First Run Date"
                group_label: "Client Info"
                sql: ${TABLE}.client_info.parsed_first_run_date ;;
                timeframes: [
                  raw,
                  date,
                  week,
                  month,
                  quarter,
                  year
                ]
                type: time
              }

              dimension_group: metadata__header__parsed_date {
                group_item_label: "Parsed Date"
                group_label: "Metadata Header"
                sql: ${TABLE}.metadata.header.parsed_date ;;
                timeframes: [
                  raw,
                  time,
                  date,
                  week,
                  month,
                  quarter,
                  year
                ]
                type: time
              }

              dimension_group: parsed_timestamp {
                sql: ${TABLE}.parsed_timestamp ;;
                timeframes: [
                  raw,
                  time,
                  date,
                  week,
                  month,
                  quarter,
                  year
                ]
                type: time
              }

              dimension_group: submission {
                sql: ${TABLE}.submission_timestamp ;;
                timeframes: [
                  raw,
                  time,
                  date,
                  week,
                  month,
                  quarter,
                  year
                ]
                type: time
              }

              measure: clients {
                sql: COUNT(DISTINCT client_info.client_id) ;;
                type: number
              }

              measure: ping_count {
                type: count
              }
            }
            """
            ).strip()
            == Path("looker-hub/glean-app/views/baseline.view.lkml").read_text()
        )
