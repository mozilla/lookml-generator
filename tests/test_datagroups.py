from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from google.cloud import bigquery

from generator.views import EventsView, TableView
from generator.views.datagroups import FILE_HEADER, generate_datagroup

from .utils import MockDryRun, MockDryRunContext


@pytest.fixture
def runner():
    return CliRunner()


@dataclass
class MockTable(bigquery.Table):
    full_table_id: str
    table_id: str
    friendly_name: str
    table_type: str


class MockDryRunDatagroups(MockDryRun):
    """Mock dryrun.DryRun."""

    def get_table_metadata(self):
        """Mock dryrun.DryRun.get_table_metadata"""
        full_table_id = f"{self.project}.{self.dataset}.{self.table}"

        if full_table_id == "mozdata.analysis.test_table":
            return {"tableType": "TABLE", "friendlyName": "Test Table"}
        elif full_table_id == "moz-fx-data-shared-prod.analysis.view_1_source":
            return {"tableType": "TABLE", "friendlyName": "View Source Table"}
        elif full_table_id == "mozdata.analysis.view_1":
            return {"tableType": "VIEW", "friendlyName": "Test View"}
        elif full_table_id == "mozdata.analysis.test_table":
            return {"tableType": "TABLE", "friendlyName": "Test Table"}
        elif full_table_id == "mozdata.analysis.test_table_2":
            return {"tableType": "TABLE", "friendlyName": "Test Table 2"}
        elif full_table_id == "mozdata.analysis.view_1":
            return {"tableType": "VIEW", "friendlyName": "Test View"}
        elif full_table_id == "mozdata.analysis.test_view_2":
            return {"tableType": "VIEW", "friendlyName": "Test View"}
        elif full_table_id == "moz-fx-data-shared-prod.analysis.source_table":
            return {"tableType": "TABLE", "friendlyName": "Source Table"}

        return {}


@patch("generator.views.lookml_utils.get_bigquery_view_reference_map")
def test_generates_datagroups(reference_map_mock, runner):
    table_1_expected = (
        FILE_HEADER
        + """datagroup: table_1_last_updated {
  label: "table_1 Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE (table_schema = 'analysis' AND table_name = 'test_table') ;;
  description: "Updates for table_1 when referenced tables are modified."
  max_cache_age: "24 hours"
}"""
    )

    table_2_expected = (
        FILE_HEADER
        + """datagroup: table_2_last_updated {
  label: "table_2 Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE (table_schema = 'analysis' AND table_name = 'test_table_2') ;;
  description: "Updates for table_2 when referenced tables are modified."
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

    mock_dryrun = MockDryRunContext(MockDryRunDatagroups, False)

    with runner.isolated_filesystem():
        namespace_dir = Path("looker-hub/test_namespace")
        namespace_dir.mkdir(parents=True)

        reference_map_mock.return_value = {}
        for view in views:
            generate_datagroup(
                view,
                target_dir=Path("looker-hub"),
                namespace="test_namespace",
                dryrun=mock_dryrun,
            )

        assert Path(namespace_dir / "datagroups").exists()
        assert Path(
            namespace_dir / "datagroups/table_1_last_updated.datagroup.lkml"
        ).exists()
        assert (
            Path(
                namespace_dir / "datagroups/table_1_last_updated.datagroup.lkml"
            ).read_text()
            == table_1_expected
        )
        assert Path(
            namespace_dir / "datagroups/table_2_last_updated.datagroup.lkml"
        ).exists()
        assert (
            Path(
                namespace_dir / "datagroups/table_2_last_updated.datagroup.lkml"
            ).read_text()
            == table_2_expected
        )


@patch(
    "generator.views.datagroups.DATASET_VIEW_MAP",
    {
        "analysis": {
            "view_1": [["moz-fx-data-shared-prod", "analysis", "view_1_source"]]
        }
    },
)
def test_generates_datagroups_with_tables_and_views(runner):
    table_1_expected = (
        FILE_HEADER
        + """datagroup: table_1_last_updated {
  label: "table_1 Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE (table_schema = 'analysis' AND table_name = 'test_table') ;;
  description: "Updates for table_1 when referenced tables are modified."
  max_cache_age: "24 hours"
}"""
    )

    test_view_expected = (
        FILE_HEADER
        + """datagroup: test_view_last_updated {
  label: "test_view Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE (table_schema = 'analysis' AND table_name = 'view_1_source') ;;
  description: "Updates for test_view when referenced tables are modified."
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

    mock_dryrun = MockDryRunContext(MockDryRunDatagroups, False)

    with runner.isolated_filesystem():
        namespace_dir = Path("looker-hub/test_namespace")
        namespace_dir.mkdir(parents=True)
        for view in views:
            generate_datagroup(
                view,
                target_dir=Path("looker-hub"),
                namespace="test_namespace",
                dryrun=mock_dryrun,
            )

        assert Path("looker-hub/test_namespace/datagroups").exists()
        assert len(list((namespace_dir / "datagroups").iterdir())) == 2
        assert Path(
            namespace_dir / "datagroups/test_view_last_updated.datagroup.lkml"
        ).exists()
        assert (
            Path(
                namespace_dir / "datagroups/test_view_last_updated.datagroup.lkml"
            ).read_text()
            == test_view_expected
        )
        assert Path(
            namespace_dir / "datagroups/table_1_last_updated.datagroup.lkml"
        ).exists()
        assert (
            Path(
                namespace_dir / "datagroups/table_1_last_updated.datagroup.lkml"
            ).read_text()
            == table_1_expected
        )


def test_skips_non_table_views(runner):
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

    mock_dryrun = MockDryRunContext(MockDryRunDatagroups, False)

    with runner.isolated_filesystem():
        Path("looker-hub/test_namespace").mkdir(parents=True)
        for view in views:
            generate_datagroup(
                view,
                target_dir=Path("looker-hub"),
                namespace="test_namespace",
                dryrun=mock_dryrun,
            )

        assert not Path("looker-hub/test_namespace/datagroups").exists()


@patch("generator.views.lookml_utils.get_bigquery_view_reference_map")
def test_only_generates_one_datagroup_for_references_to_same_table(
    reference_map_mock, runner
):
    test_table_expected = (
        FILE_HEADER
        + """datagroup: test_table_last_updated {
  label: "test_table Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE (table_schema = 'analysis' AND table_name = 'source_table') ;;
  description: "Updates for test_table when referenced tables are modified."
  max_cache_age: "24 hours"
}"""
    )

    view_1_expected = (
        FILE_HEADER
        + """datagroup: view_1_last_updated {
  label: "view_1 Last Updated"
  sql_trigger: SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE (table_schema = 'analysis' AND table_name = 'view_1') ;;
  description: "Updates for view_1 when referenced tables are modified."
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

    mock_dryrun = MockDryRunContext(MockDryRunDatagroups, False)

    with runner.isolated_filesystem():
        reference_map_mock.return_value = {
            "analysis": {
                "view_1": [["moz-fx-data-shared-prod", "analysis", "source_table"]],
                "view_2": [["moz-fx-data-shared-prod", "analysis", "source_table"]],
            }
        }

        namespace_dir = Path("looker-hub/test_namespace")
        namespace_dir.mkdir(parents=True)

        for view in views:
            generate_datagroup(
                view,
                target_dir=Path("looker-hub"),
                namespace="test_namespace",
                dryrun=mock_dryrun,
            )

        assert Path(namespace_dir / "datagroups").exists()
        assert len(list((namespace_dir / "datagroups").iterdir())) == 2
        assert (
            Path(
                namespace_dir / "datagroups" / "test_table_last_updated.datagroup.lkml"
            ).read_text()
            == test_table_expected
        )
        assert (
            Path(
                namespace_dir / "datagroups" / "view_1_last_updated.datagroup.lkml"
            ).read_text()
            == view_1_expected
        )
