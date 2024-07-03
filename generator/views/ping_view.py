"""Class to describe a Ping View."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterator, List, Optional, Union

from . import lookml_utils
from .view import OMIT_VIEWS, View, ViewDict
from dryrun import DryRun


class PingView(View):
    """A view on a ping table."""

    type: str = "ping_view"
    allow_glean: bool = False

    def __init__(self, namespace: str, name: str, tables: List[Dict[str, Any]]):
        """Create instance of a PingView."""
        super().__init__(namespace, name, self.__class__.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[PingView]:
        """Get Looker views for a namespace."""
        if (klass.allow_glean and not is_glean) or (not klass.allow_glean and is_glean):
            return

        view_tables: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)
        for channel in channels:
            dataset = channel["dataset"]

            for view_id, references in db_views[dataset].items():
                if view_id in OMIT_VIEWS:
                    continue

                table_id = f"mozdata.{dataset}.{view_id}"
                table: Dict[str, str] = {"table": table_id}
                if channel.get("channel") is not None:
                    table["channel"] = channel["channel"]

                # Only include those that select from a single ping source table
                # or union together multiple ping source tables of the same name.
                reference_table_names = set(r[-1] for r in references)
                reference_dataset_names = set(r[-2] for r in references)
                if (
                    len(reference_table_names) != 1
                    or channel["source_dataset"] not in reference_dataset_names
                ):
                    continue

                view_tables[view_id][table_id] = table

        for view_id, tables_by_id in view_tables.items():
            yield klass(namespace, view_id, list(tables_by_id.values()))

    @classmethod
    def from_dict(klass, namespace: str, name: str, _dict: ViewDict) -> PingView:
        """Get a view from a name and dict definition."""
        return klass(namespace, name, _dict["tables"])

    def to_lookml(self, v1_name: Optional[str], dryrun) -> Dict[str, Any]:
        """Generate LookML for this view."""
        view_defn: Dict[str, Any] = {"name": self.name}

        # use schema for the table where channel=="release" or the first one
        table = next(
            (table for table in self.tables if table.get("channel") == "release"),
            self.tables[0],
        )["table"]

        dimensions = self.get_dimensions(table, v1_name, dryrun=dryrun)

        # set document id field as a primary key for joins
        view_defn["dimensions"] = [
            d if d["name"] != "document_id" else dict(**d, primary_key="yes")
            for d in dimensions
            if not lookml_utils._is_dimension_group(d)
        ]
        view_defn["dimension_groups"] = [
            d for d in dimensions if lookml_utils._is_dimension_group(d)
        ]

        # add measures
        view_defn["measures"] = self.get_measures(dimensions, table, v1_name)

        [project, dataset, table_id] = table.split(".")
        table_schema = dryrun(
            project=project,
            dataset=dataset,
            table=table_id,
        ).get_table_schema()
        nested_views = lookml_utils._generate_nested_dimension_views(
            table_schema, self.name
        )

        # Round-tripping through a dict to get an ordered deduped list.
        suggestions = list(
            dict.fromkeys(
                _table["channel"] for _table in self.tables if "channel" in _table
            )
        )

        if len(suggestions) > 1:
            view_defn["filters"] = [
                {
                    "name": "channel",
                    "type": "string",
                    "description": "Filter by the app's channel",
                    "sql": "{% condition %} ${TABLE}.normalized_channel {% endcondition %}",
                    "default_value": suggestions[0],
                    "suggestions": suggestions,
                }
            ]

        view_defn["sql_table_name"] = f"`{table}`"

        return {"views": [view_defn] + nested_views}

    def get_dimensions(
        self, table, v1_name: Optional[str], dryrun
    ) -> List[Dict[str, Any]]:
        """Get the set of dimensions for this view."""
        # add dimensions and dimension groups
        return lookml_utils._generate_dimensions(table, dryrun=dryrun)

    def get_measures(
        self, dimensions: List[dict], table: str, v1_name: Optional[str]
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Generate measures from a list of dimensions.

        When no dimension-specific measures are found, return a single "count" measure.

        Raise ClickException if dimensions result in duplicate measures.
        """
        # Iterate through each of the dimensions and accumulate any measures
        # that we want to include in the view. We pull out the client id first
        # since we'll use it to calculate per-measure client counts.
        measures: List[Dict[str, Union[str, List[Dict[str, str]]]]] = []

        client_id_field = self.get_client_id(dimensions, table)
        if client_id_field is not None:
            measures.append(
                {
                    "name": "clients",
                    "type": "count_distinct",
                    "sql": f"${{{client_id_field}}}",
                }
            )

        for dimension in dimensions:
            dimension_name = dimension["name"]
            if dimension_name == "document_id":
                measures += [{"name": "ping_count", "type": "count"}]

        return measures
