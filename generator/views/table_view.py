"""Class to describe a Table View."""

from __future__ import annotations

from collections import defaultdict
from itertools import filterfalse
from typing import Any, Dict, Iterator, List, Optional, Set

from click import ClickException

from generator.dryrun import DryRun

from . import lookml_utils
from .view import OMIT_VIEWS, View, ViewDict


class TableView(View):
    """A view on any table."""

    type: str = "table_view"
    measures: Optional[Dict[str, Dict[str, Any]]]

    def __init__(
        self,
        namespace: str,
        name: str,
        tables: List[Dict[str, str]],
        measures: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        """Create instance of a TableView."""
        super().__init__(namespace, name, TableView.type, tables)
        self.measures = measures

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[TableView]:
        """Get Looker views for a namespace."""
        view_tables: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)
        for channel in channels:
            dataset = channel["dataset"]

            for view_id, references in db_views[dataset].items():
                if view_id in OMIT_VIEWS:
                    continue

                table_id = f"mozdata.{dataset}.{view_id}"
                table: Dict[str, str] = {"table": table_id}
                if "channel" in channel:
                    table["channel"] = channel["channel"]

                view_tables[view_id][table_id] = table

        for view_id, tables_by_id in view_tables.items():
            yield TableView(namespace, f"{view_id}_table", list(tables_by_id.values()))

    @classmethod
    def from_dict(klass, namespace: str, name: str, _dict: ViewDict) -> TableView:
        """Get a view from a name and dict definition."""
        return TableView(namespace, name, _dict["tables"], _dict.get("measures"))

    def to_lookml(self, v1_name: Optional[str], dryrun) -> Dict[str, Any]:
        """Generate LookML for this view."""
        view_defn: Dict[str, Any] = {"name": self.name}

        # use schema for the table where channel=="release" or the first one
        table = next(
            (table for table in self.tables if table.get("channel") == "release"),
            self.tables[0],
        )["table"]

        # add dimensions and dimension groups
        dimensions = lookml_utils._generate_dimensions(table, dryrun=dryrun)
        view_defn["dimensions"] = list(
            filterfalse(lookml_utils._is_dimension_group, dimensions)
        )
        view_defn["dimension_groups"] = list(
            filter(lookml_utils._is_dimension_group, dimensions)
        )

        # add tag "time_partitioning_field"
        time_partitioning_fields: Set[str] = set(
            # filter out falsy values
            filter(
                None, (table.get("time_partitioning_field") for table in self.tables)
            )
        )
        if len(time_partitioning_fields) > 1:
            raise ClickException(f"Multiple time_partitioning_fields for {self.name!r}")
        elif len(time_partitioning_fields) == 1:
            field_name = time_partitioning_fields.pop()
            sql = f"${{TABLE}}.{field_name}"
            for group_defn in view_defn["dimension_groups"]:
                if group_defn["sql"] == sql:
                    if "tags" not in group_defn:
                        group_defn["tags"] = []
                    group_defn["tags"].append("time_partitioning_field")
                    break
            else:
                raise ClickException(
                    f"time_partitioning_field {field_name!r} not found in {self.name!r}"
                )

        [project, dataset, table] = table.split(".")
        table_schema = dryrun(
            project=project,
            dataset=dataset,
            table=table,
        ).get_table_schema()
        nested_views = lookml_utils._generate_nested_dimension_views(
            table_schema, self.name
        )

        if self.measures:
            view_defn["measures"] = [
                {"name": measure_name, **measure_parameters}
                for measure_name, measure_parameters in self.measures.items()
            ]

        # parameterize table name
        if len(self.tables) > 1:
            view_defn["parameters"] = [
                {
                    "name": "channel",
                    "type": "unquoted",
                    "default_value": table,
                    "allowed_values": [
                        {
                            "label": _table["channel"].title(),
                            "value": _table["table"],
                        }
                        for _table in self.tables
                    ],
                }
            ]
            view_defn["sql_table_name"] = "`{% parameter channel %}`"
        else:
            view_defn["sql_table_name"] = f"`{table}`"

        return {"views": [view_defn] + nested_views}
