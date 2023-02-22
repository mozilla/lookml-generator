"""Generate datagroup lkml files for each namespace."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import lkml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from generator.namespaces import DEFAULT_GENERATED_SQL_URI
from generator.views import TableView, View, lookml_utils
from generator.views.lookml_utils import BQViewReferenceMap

DEFAULT_MAX_CACHE_AGE = "24 hours"

# Note: INFORMATION_SCHEMA.PARTITIONS has a row with a `last_modified_time` value even for non-partitioned tables.
SQL_TRIGGER_TEMPLATE = """
    SELECT MAX(last_modified_time)
    FROM `{project_id}`.{dataset_id}.INFORMATION_SCHEMA.PARTITIONS
    WHERE table_name = '{table_id}'
"""

FILE_HEADER = """# *Do not manually modify this file*

# This file has been generated via https://github.com/mozilla/lookml-generator

# Using a datagroup in an Explore: https://cloud.google.com/looker/docs/reference/param-explore-persist-with
# Using a datagroup in a derived table: https://cloud.google.com/looker/docs/reference/param-view-datagroup-trigger

"""


@dataclass
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


def _get_datagroup_from_bigquery_table(table: bigquery.Table) -> Datagroup:
    """Use template and default values to create a Datagroup from a BQ Table."""
    return Datagroup(
        name=f"{table.table_id}_last_updated",
        label=f"{table.friendly_name or table.table_id} Last Updated",
        description=f"Updates when {table.full_table_id} is modified.",
        sql_trigger=SQL_TRIGGER_TEMPLATE.format(
            project_id=table.project,
            dataset_id=table.dataset_id,
            table_id=table.table_id,
        ),
        # Note: Ideally we'd use the table's ETL schedule as the `maximum_cache_age` but we don't have schedule
        # metadata here.
    )


def _get_datagroup_from_bigquery_view(
    view: bigquery.Table,
    client: bigquery.Client,
    dataset_view_map: BQViewReferenceMap,
) -> Optional[Datagroup]:
    # Dataset view map only contains references for shared-prod views.
    if view.project not in ("moz-fx-data-shared-prod", "mozdata"):
        logging.debug(
            f"Unable to get sources for non shared-prod/mozdata view: {view.full_table_id} in generated-sql."
        )
        return None

    dataset_view_references = dataset_view_map.get(view.dataset_id)
    if dataset_view_references is None:
        logging.debug(f"Unable to find dataset {view.dataset_id} in generated-sql.")
        return None

    view_references = dataset_view_references.get(view.table_id)
    if not view_references or len(view_references) > 1:
        # For views with multiple sources it's unclear which table to check for updates.
        logging.debug(
            f"Unable to find a single source for {view.full_table_id} in generated-sql."
        )
        return None

    source_table_id = ".".join(view_references[0])
    try:
        table = client.get_table(source_table_id)
        if "TABLE" == table.table_type:
            return _get_datagroup_from_bigquery_table(table)
        elif "VIEW" == table.table_type:
            return _get_datagroup_from_bigquery_view(table, client, dataset_view_map)
    except NotFound as e:
        raise ValueError(
            f"Unable to find {source_table_id} referenced in {view.full_table_id}"
        ) from e

    return None


def _generate_view_datagroup_lkml(
    view: View,
    client: bigquery.Client,
    dataset_view_map: BQViewReferenceMap,
) -> str:
    """Generate the Datagroup LookML for a Looker View."""
    # Only generate datagroup for views that can be linked to a BigQuery table:
    if view.view_type != TableView.type:
        return ""

    # Use the release channel table or the first available table (usually the only one):
    view_table = next(
        (table for table in view.tables if table.get("channel") == "release"),
        view.tables[0],
    )["table"]

    try:
        bq_table = client.get_table(view_table)
    except NotFound as e:
        raise ValueError(
            f"{view_table} not found when generating datagroup but in namespaces yaml."
        ) from e

    if "TABLE" == bq_table.table_type:
        datagroup: Optional[Datagroup] = _get_datagroup_from_bigquery_table(bq_table)
        return str(datagroup)
    elif "VIEW" == bq_table.table_type:
        datagroup = _get_datagroup_from_bigquery_view(
            bq_table, client, dataset_view_map
        )
        if datagroup is not None:
            return str(datagroup)

    return ""


def generate_datagroups(
    views: List[View], target_dir: Path, namespace: str, client: bigquery.Client
) -> None:
    """Generate and write a datagroups.lkml file to the namespace folder."""
    datagroups_lkml_path = target_dir / namespace / "datagroups.lkml"

    # To map views to their underlying tables:
    dataset_view_map = lookml_utils.get_bigquery_view_reference_map(
        DEFAULT_GENERATED_SQL_URI
    )

    datagroup_content = sorted(
        set(
            lookml
            for view in views
            if (lookml := _generate_view_datagroup_lkml(view, client, dataset_view_map))
        )
    )

    if datagroup_content:
        datagroups_lkml_path.write_text(FILE_HEADER + "\n\n".join(datagroup_content))
