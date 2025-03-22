"""Generate datagroup lkml files for each namespace."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import lkml

from generator.dryrun import DryRunError, Errors
from generator.namespaces import DEFAULT_GENERATED_SQL_URI
from generator.utils import get_file_from_looker_hub
from generator.views import View, lookml_utils
from generator.views.lookml_utils import BQViewReferenceMap

DEFAULT_MAX_CACHE_AGE = "24 hours"

SQL_TRIGGER_TEMPLATE = """
    SELECT MAX(storage_last_modified_time)
    FROM `moz-fx-data-shared-prod`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
    WHERE {tables}
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


def _get_datagroup_from_bigquery_tables(
    project_id, tables, view: View
) -> Optional[Datagroup]:
    """Use template and default values to create a Datagroup from a BQ Table."""
    if len(tables) == 0:
        return None

    datagroup_tables = []
    for table in tables:
        dataset_id = table[1]
        table_id = table[2]

        datagroup_tables.append(
            f"(table_schema = '{dataset_id}' AND table_name = '{table_id}')"
        )

    # create a datagroup associated to a view which will be used for caching
    return Datagroup(
        name=f"{view.name}_last_updated",
        label=f"{view.name} Last Updated",
        description=f"Updates for {view.name} when referenced tables are modified.",
        sql_trigger=SQL_TRIGGER_TEMPLATE.format(
            project_id=project_id, tables=" OR ".join(datagroup_tables)
        ),
    )


def _get_datagroup_from_bigquery_view(
    project_id,
    dataset_id,
    table_id,
    dataset_view_map: BQViewReferenceMap,
    view: View,
) -> Optional[Datagroup]:
    # Dataset view map only contains references for shared-prod views.
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    if project_id not in ("moz-fx-data-shared-prod", "mozdata"):
        logging.debug(
            f"Unable to get sources for non shared-prod/mozdata view: {full_table_id} in generated-sql."
        )
        return None

    view_references = _get_referenced_tables(
        project_id, dataset_id, table_id, dataset_view_map
    )

    if not view_references or len(view_references) == 0:
        # Some views might not reference a source table
        logging.debug(f"Unable to find a source for {full_table_id} in generated-sql.")
        return None

    return _get_datagroup_from_bigquery_tables(project_id, view_references, view)


def _get_referenced_tables(
    project_id,
    dataset_id,
    table_id,
    dataset_view_map: BQViewReferenceMap,
    seen: List[List[str]] = [],
) -> List[List[str]]:
    """
    Return a list of all tables referenced by the provided view.

    Recursively, resolve references of referenced views to only get table dependencies.
    """
    if [project_id, dataset_id, table_id] in seen:
        return []

    seen += [[project_id, dataset_id, table_id]]

    dataset_view_references = dataset_view_map.get(dataset_id)

    if dataset_view_references is None:
        return [[project_id, dataset_id, table_id]]

    view_references = dataset_view_references.get(table_id)
    if view_references is None:
        return [[project_id, dataset_id, table_id]]

    return [
        ref
        for view_reference in view_references
        for ref in _get_referenced_tables(
            view_reference[0],
            view_reference[1],
            view_reference[2],
            dataset_view_map,
            seen + view_references,
        )
        if view_reference not in seen
    ]


def _generate_view_datagroup(
    view: View,
    dataset_view_map: BQViewReferenceMap,
    dryrun,
) -> Optional[Datagroup]:
    """Generate the Datagroup LookML for a Looker View."""
    if len(view.tables) == 0:
        return None

    # Use the release channel table or the first available table (usually the only one):
    view_tables = next(
        (table for table in view.tables if table.get("channel") == "release"),
        view.tables[0],
    )

    if "table" not in view_tables:
        return None

    view_table = view_tables["table"]

    [project, dataset, table] = view_table.split(".")
    table_metadata = dryrun.create(
        project=project,
        dataset=dataset,
        table=table,
    ).get_table_metadata()

    if "TABLE" == table_metadata.get("tableType"):
        datagroups = _get_datagroup_from_bigquery_tables(
            project, [[project, dataset, table]], view
        )
        return datagroups
    elif "VIEW" == table_metadata.get("tableType"):
        datagroups = _get_datagroup_from_bigquery_view(
            project, dataset, table, dataset_view_map, view
        )
        return datagroups

    return None


def generate_datagroup(
    view: View,
    target_dir: Path,
    namespace: str,
    dryrun,
) -> Any:
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

    datagroup_paths = []
    if datagroup:
        datagroups_folder_path.mkdir(exist_ok=True)
        datagroup_lkml_path = (
            datagroups_folder_path / f"{datagroup.name}.datagroup.lkml"
        )
        datagroup_lkml_path.write_text(FILE_HEADER + str(datagroup))
        datagroup_paths.append(datagroup_lkml_path)

    return datagroup_paths
