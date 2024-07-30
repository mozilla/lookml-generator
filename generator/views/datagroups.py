"""Generate datagroup lkml files for each namespace."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import lkml

from generator.dryrun import DryRunError, Errors
from generator.namespaces import DEFAULT_GENERATED_SQL_URI
from generator.utils import get_file_from_looker_hub
from generator.views import TableView, View, lookml_utils
from generator.views.lookml_utils import BQViewReferenceMap

DEFAULT_MAX_CACHE_AGE = "24 hours"

SQL_TRIGGER_TEMPLATE = """
    SELECT MAX(storage_last_modified_time)
    FROM `{project_id}`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE table_schema = '{dataset_id}'
    AND table_name = '{table_id}'
"""

# To map views to their underlying tables:
DATASET_VIEW_MAP = lookml_utils.get_bigquery_view_reference_map(
    DEFAULT_GENERATED_SQL_URI
)

FILE_HEADER = """# *Do not manually modify this file*

# This file has been generated via https://github.com/mozilla/lookml-generator

# Using a datagroup in an Explore: https://cloud.google.com/looker/docs/reference/param-explore-persist-with
# Using a datagroup in a derived table: https://cloud.google.com/looker/docs/reference/param-view-datagroup-trigger

"""


@dataclass(frozen=True, eq=True)
class Datagroup:
    """Represents a Datagroup."""

    name: str
    label: str
    sql_trigger: str
    description: str
    max_cache_age: str = DEFAULT_MAX_CACHE_AGE

    def __str__(self) -> str:
        """Return the LookML string representation of a Datagroup."""
        return lkml.dump({"datagroups": [self.__dict__]})  # type: ignore

    def __lt__(self, other) -> bool:
        """Make datagroups sortable."""
        return self.name < other.name


def _get_datagroup_from_bigquery_table(
    project_id, dataset_id, table_id, table_metadata: Dict[str, Any]
) -> Datagroup:
    """Use template and default values to create a Datagroup from a BQ Table."""
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    return Datagroup(
        name=f"{table_id}_last_updated",
        label=f"{table_metadata.get('friendlyName', table_id) or table_id} Last Updated",
        description=f"Updates when {full_table_id} is modified.",
        sql_trigger=SQL_TRIGGER_TEMPLATE.format(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        ),
        # Note: Ideally we'd use the table's ETL schedule as the `maximum_cache_age` but we don't have schedule
        # metadata here.
    )


def _get_datagroup_from_bigquery_view(
    project_id,
    dataset_id,
    table_id,
    table_metadata: Dict[str, Any],
    dataset_view_map: BQViewReferenceMap,
    dryrun,
) -> Optional[Datagroup]:
    # Dataset view map only contains references for shared-prod views.
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    if project_id not in ("moz-fx-data-shared-prod", "mozdata"):
        logging.debug(
            f"Unable to get sources for non shared-prod/mozdata view: {full_table_id} in generated-sql."
        )
        return None

    dataset_view_references = dataset_view_map.get(dataset_id)
    if dataset_view_references is None:
        logging.debug(f"Unable to find dataset {dataset_id} in generated-sql.")
        return None

    view_references = dataset_view_references.get(table_id)
    if not view_references or len(view_references) > 1:
        # For views with multiple sources it's unclear which table to check for updates.
        logging.debug(
            f"Unable to find a single source for {full_table_id} in generated-sql."
        )
        return None

    source_table_id = ".".join(view_references[0])
    try:
        table_metadata = dryrun(
            project=view_references[0][0],
            dataset=view_references[0][1],
            table=view_references[0][2],
        ).get_table_metadata()
        if "TABLE" == table_metadata.get("tableType"):
            return _get_datagroup_from_bigquery_table(
                view_references[0][0],
                view_references[0][1],
                view_references[0][2],
                table_metadata,
            )
        elif "VIEW" == table_metadata.get("tableType"):
            return _get_datagroup_from_bigquery_view(
                view_references[0][0],
                view_references[0][1],
                view_references[0][2],
                table_metadata,
                dataset_view_map,
                dryrun=dryrun,
            )
    except DryRunError as e:
        raise ValueError(
            f"Unable to find {source_table_id} referenced in {full_table_id}"
        ) from e

    return None


def _generate_view_datagroup(
    view: View,
    dataset_view_map: BQViewReferenceMap,
    dryrun,
) -> Optional[Datagroup]:
    """Generate the Datagroup LookML for a Looker View."""
    # Only generate datagroup for views that can be linked to a BigQuery table:
    if view.view_type != TableView.type:
        return None

    # Use the release channel table or the first available table (usually the only one):
    view_table = next(
        (table for table in view.tables if table.get("channel") == "release"),
        view.tables[0],
    )["table"]

    [project, dataset, table] = view_table.split(".")
    table_metadata = dryrun(
        project=project,
        dataset=dataset,
        table=table,
    ).get_table_metadata()

    if "TABLE" == table_metadata.get("tableType"):
        datagroup: Optional[Datagroup] = _get_datagroup_from_bigquery_table(
            project, dataset, table, table_metadata
        )
        return datagroup
    elif "VIEW" == table_metadata.get("tableType"):
        datagroup = _get_datagroup_from_bigquery_view(
            project,
            dataset,
            table,
            table_metadata,
            dataset_view_map,
            dryrun,
        )
        return datagroup

    return None


def generate_datagroup(
    view: View,
    target_dir: Path,
    namespace: str,
    dryrun,
) -> Optional[Path]:
    """Generate and write a datagroups.lkml file to the namespace folder."""
    datagroups_folder_path = target_dir / namespace / "datagroups"

    datagroup = None
    try:
        datagroup = _generate_view_datagroup(view, DATASET_VIEW_MAP, dryrun)
    except DryRunError as e:
        if e.error == Errors.PERMISSION_DENIED and e.use_cloud_function:
            path = datagroups_folder_path / f"{e.table_id}_last_updated.datagroup.lkml"
            print(
                f"Permission error dry running: {path}. Copy existing file from looker-hub."
            )
            try:
                get_file_from_looker_hub(path)
            except Exception as ex:
                print(f"Skip generating datagroup for {path}: {ex}")
        else:
            raise

    if datagroup:
        datagroups_folder_path.mkdir(exist_ok=True)
        datagroup_lkml_path = (
            datagroups_folder_path / f"{datagroup.name}.datagroup.lkml"
        )
        datagroup_lkml_path.write_text(FILE_HEADER + str(datagroup))
        return datagroup_lkml_path

    return None
