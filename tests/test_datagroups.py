from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from google.cloud import bigquery

from generator.views import EventsView, TableView
from generator.views.datagroups import FILE_HEADER, generate_datagroups


@pytest.fixture
def runner():
    return CliRunner()


@dataclass
class MockTable(bigquery.Table):
    full_table_id: str
    table_id: str
    friendly_name: str
    table_type: str


@patch("google.cloud.bigquery.Table")
@patch("google.cloud.bigquery.Table")
@patch("google.cloud.bigquery.Client")
@patch("generator.views.lookml_utils.get_bigquery_view_reference_map")
def test_generates_datagroups(reference_map_mock, client, table_1, table_2, runner):
    table_1_expected = (
        FILE_HEADER
        + """datagroup: test_table_last_updated {
  label: "Test Table Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `mozdata`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE table_schema = 'analysis'
    AND table_name = 'test_table' ;;
  description: "Updates when mozdata:analysis.test_table is modified."
  max_cache_age: "24 hours"
}"""
    )

    table_2_expected = (
        FILE_HEADER
        + """datagroup: test_table_2_last_updated {
  label: "Test Table 2 Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `mozdata`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE table_schema = 'analysis'
    AND table_name = 'test_table_2' ;;
  description: "Updates when mozdata:analysis.test_table_2 is modified."
  max_cache_age: "24 hours"
}"""
    )

    views = [
        TableView(
            namespace="test_namespace",
            name="table_1",
            tables=[
                {"table": "mozdata.analysis.test_table"},
            ],
        ),
        TableView(
            namespace="test_namespace",
            name="table_2",
            tables=[
                {"table": "mozdata.analysis.test_table_2"},
            ],
        ),
    ]

    with runner.isolated_filesystem():
        table_1.project = "mozdata"
        table_2.project = "mozdata"
        table_1.dataset_id = "analysis"
        table_2.dataset_id = "analysis"
        table_1.table_type = "TABLE"
        table_2.table_type = "TABLE"
        table_1.full_table_id = "mozdata:analysis.test_table"
        table_2.full_table_id = "mozdata:analysis.test_table_2"
        table_1.table_id = "test_table"
        table_2.table_id = "test_table_2"
        table_1.friendly_name = "Test Table"
        table_2.friendly_name = "Test Table 2"
        tables = {
            "mozdata.analysis.test_table": table_1,
            "mozdata.analysis.test_table_2": table_2,
        }
        client.get_table = tables.get

        namespace_dir = Path("looker-hub/test_namespace")
        namespace_dir.mkdir(parents=True)

        reference_map_mock.return_value = {}
        generate_datagroups(
            views,
            target_dir=Path("looker-hub"),
            namespace="test_namespace",
            client=client,
        )

        assert Path(namespace_dir / "datagroups").exists()
        assert Path(
            namespace_dir / f"datagroups/{table_1.table_id}_last_updated.datagroup.lkml"
        ).exists()
        assert (
            Path(
                namespace_dir
                / f"datagroups/{table_1.table_id}_last_updated.datagroup.lkml"
            ).read_text()
            == table_1_expected
        )
        assert Path(
            namespace_dir / f"datagroups/{table_2.table_id}_last_updated.datagroup.lkml"
        ).exists()
        assert (
            Path(
                namespace_dir
                / f"datagroups/{table_2.table_id}_last_updated.datagroup.lkml"
            ).read_text()
            == table_2_expected
        )


@patch("google.cloud.bigquery.Table")
@patch("google.cloud.bigquery.Table")
@patch("google.cloud.bigquery.Table")
@patch("google.cloud.bigquery.Client")
@patch("generator.views.lookml_utils.get_bigquery_view_reference_map")
def test_generates_datagroups_with_tables_and_views(
    reference_map_mock, client, table_1, view_1, view_1_source_table, runner
):
    table_1_expected = (
        FILE_HEADER
        + """datagroup: test_table_last_updated {
  label: "Test Table Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `mozdata`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE table_schema = 'analysis'
    AND table_name = 'test_table' ;;
  description: "Updates when mozdata:analysis.test_table is modified."
  max_cache_age: "24 hours"
}"""
    )

    source_table_expected = (
        FILE_HEADER
        + """datagroup: view_1_source_last_updated {
  label: "View Source Table Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE table_schema = 'analysis'
    AND table_name = 'view_1_source' ;;
  description: "Updates when moz-fx-data-shared-prod:analysis.view_1_source is modified."
  max_cache_age: "24 hours"
}"""
    )

    views = [
        TableView(
            namespace="test_namespace",
            name="table_1",
            tables=[
                {"table": "mozdata.analysis.test_table"},
            ],
        ),
        TableView(
            namespace="test_namespace",
            name="test_view",
            tables=[
                {
                    "table": "mozdata.analysis.view_1"
                },  # View to moz-fx-data-shared-prod.analysis.view_1_source
            ],
        ),
    ]

    with runner.isolated_filesystem():
        table_1.project = "mozdata"
        table_1.dataset_id = "analysis"
        table_1.table_type = "TABLE"
        table_1.full_table_id = "mozdata:analysis.test_table"
        table_1.table_id = "test_table"
        table_1.friendly_name = "Test Table"

        view_1.project = "mozdata"
        view_1.dataset_id = "analysis"
        view_1.table_type = "VIEW"
        view_1.full_table_id = "mozdata:analysis.view_1"
        view_1.table_id = "view_1"
        view_1.friendly_name = "Test View"

        view_1_source_table.project = "moz-fx-data-shared-prod"
        view_1_source_table.dataset_id = "analysis"
        view_1_source_table.table_type = "TABLE"
        view_1_source_table.full_table_id = (
            "moz-fx-data-shared-prod:analysis.view_1_source"
        )
        view_1_source_table.table_id = "view_1_source"
        view_1_source_table.friendly_name = "View Source Table"

        reference_map_mock.return_value = {
            "analysis": {
                "view_1": [["moz-fx-data-shared-prod", "analysis", "view_1_source"]]
            }
        }

        tables = {
            "mozdata.analysis.test_table": table_1,
            "mozdata.analysis.view_1": view_1,
            "moz-fx-data-shared-prod.analysis.view_1_source": view_1_source_table,
        }
        client.get_table = tables.get

        namespace_dir = Path("looker-hub/test_namespace")
        namespace_dir.mkdir(parents=True)
        generate_datagroups(
            views,
            target_dir=Path("looker-hub"),
            namespace="test_namespace",
            client=client,
        )

        assert Path("looker-hub/test_namespace/datagroups").exists()
        assert Path(
            namespace_dir / f"datagroups/{table_1.table_id}_last_updated.datagroup.lkml"
        ).exists()
        assert (
            Path(
                namespace_dir
                / f"datagroups/{table_1.table_id}_last_updated.datagroup.lkml"
            ).read_text()
            == table_1_expected
        )
        assert Path(
            namespace_dir
            / f"datagroups/{view_1_source_table.table_id}_last_updated.datagroup.lkml"
        ).exists()
        assert (
            Path(
                namespace_dir
                / f"datagroups/{view_1_source_table.table_id}_last_updated.datagroup.lkml"
            ).read_text()
            == source_table_expected
        )


@patch("google.cloud.bigquery.Client")
def test_skips_non_table_views(client, runner):
    views = [
        EventsView(
            namespace="test_namespace",
            name="test_event_view",
            tables=[
                {
                    "events_table_view": "events_unnested_table",
                    "base_table": "mozdata.glean_app.events_unnested",
                },
            ],
        ),
    ]

    with runner.isolated_filesystem():
        Path("looker-hub/test_namespace").mkdir(parents=True)
        generate_datagroups(
            views,
            target_dir=Path("looker-hub"),
            namespace="test_namespace",
            client=client,
        )

        assert not Path("looker-hub/test_namespace/datagroups").exists()


@patch("google.cloud.bigquery.Table")
@patch("google.cloud.bigquery.Table")
@patch("google.cloud.bigquery.Table")
@patch("google.cloud.bigquery.Client")
@patch("generator.views.lookml_utils.get_bigquery_view_reference_map")
def test_only_generates_one_datagroup_for_references_to_same_table(
    reference_map_mock, client, view_1, view_2, view_source_table, runner
):
    expected = (
        FILE_HEADER
        + """datagroup: source_table_last_updated {
  label: "Source Table Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE table_schema = 'analysis'
    AND table_name = 'source_table' ;;
  description: "Updates when moz-fx-data-shared-prod:analysis.source_table is modified."
  max_cache_age: "24 hours"
}"""
    )

    views = [
        TableView(
            namespace="test_namespace",
            name="view_1",
            tables=[
                {
                    "table": "mozdata.analysis.view_1"
                },  # View to moz-fx-data-shared-prod.analysis.view_1_source
            ],
        ),
        TableView(
            namespace="test_namespace",
            name="test_view",
            tables=[
                {
                    "table": "mozdata.analysis.view_2"
                },  # View to moz-fx-data-shared-prod.analysis.view_1_source
            ],
        ),
        TableView(
            namespace="test_namespace",
            name="test_table",
            tables=[{"table": "moz-fx-data-shared-prod.analysis.source_table"}],
        ),
    ]

    with runner.isolated_filesystem():
        view_1.project = "mozdata"
        view_1.dataset_id = "analysis"
        view_1.table_type = "VIEW"
        view_1.full_table_id = "mozdata:analysis.view_1"
        view_1.table_id = "view_1"
        view_1.friendly_name = "Test View"

        view_2.project = "mozdata"
        view_2.dataset_id = "analysis"
        view_2.table_type = "VIEW"
        view_2.full_table_id = "mozdata:analysis.test_view_2"
        view_2.table_id = "view_2"
        view_2.friendly_name = "Test View"

        view_source_table.project = "moz-fx-data-shared-prod"
        view_source_table.dataset_id = "analysis"
        view_source_table.table_type = "TABLE"
        view_source_table.full_table_id = (
            "moz-fx-data-shared-prod:analysis.source_table"
        )
        view_source_table.table_id = "source_table"
        view_source_table.friendly_name = "Source Table"

        reference_map_mock.return_value = {
            "analysis": {
                "view_1": [["moz-fx-data-shared-prod", "analysis", "source_table"]],
                "view_2": [["moz-fx-data-shared-prod", "analysis", "source_table"]],
            }
        }

        tables = {
            "mozdata.analysis.view_1": view_1,
            "mozdata.analysis.view_2": view_2,
            "moz-fx-data-shared-prod.analysis.source_table": view_source_table,
        }
        client.get_table = tables.get

        namespace_dir = Path("looker-hub/test_namespace")
        namespace_dir.mkdir(parents=True)

        generate_datagroups(
            views,
            target_dir=Path("looker-hub"),
            namespace="test_namespace",
            client=client,
        )

        assert Path(namespace_dir / "datagroups").exists()
        assert len(list((namespace_dir / "datagroups").iterdir())) == 1
        assert (
            Path(
                namespace_dir
                / "datagroups"
                / f"{view_source_table.table_id}_last_updated.datagroup.lkml"
            ).read_text()
            == expected
        )
