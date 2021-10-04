"""Class to describe a Table View."""
from __future__ import annotations

from collections import defaultdict
from itertools import filterfalse
from typing import Any, Dict, Iterator, List, Optional

from . import lookml_utils
from .view import OMIT_VIEWS, View, ViewDict


class TableView(View):
    """A view on any table."""

    type: str = "table_view"

    def __init__(self, namespace: str, name: str, tables: List[Dict[str, str]]):
        """Create instance of a TableView."""
        super().__init__(namespace, name, TableView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[TableView]:
        """Get Looker views for a namespace."""
        views = defaultdict(list)
        for channel in channels:
            dataset = channel["dataset"]

            for view_id, references in db_views[dataset].items():
                if view_id in OMIT_VIEWS:
                    continue

                table: Dict[str, str] = {"table": f"mozdata.{dataset}.{view_id}"}

                if "channel" in channel:
                    table["channel"] = channel["channel"]

                views[view_id].append(table)

        for view_id, tables in views.items():
            yield TableView(namespace, f"{view_id}_table", tables)

    @classmethod
    def from_dict(klass, namespace: str, name: str, _dict: ViewDict) -> TableView:
        """Get a view from a name and dict definition."""
        return TableView(namespace, name, _dict["tables"])

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Generate LookML for this view."""
        view_defn: Dict[str, Any] = {"name": self.name}

        # use schema for the table where channel=="release" or the first one
        table = next(
            (table for table in self.tables if table.get("channel") == "release"),
            self.tables[0],
        )["table"]

        # add dimensions and dimension groups
        dimensions = lookml_utils._generate_dimensions(bq_client, table)
        view_defn["dimensions"] = list(
            filterfalse(lookml_utils._is_dimension_group, dimensions)
        )
        view_defn["dimension_groups"] = list(
            filter(lookml_utils._is_dimension_group, dimensions)
        )

        nested_views = lookml_utils._generate_nested_dimension_views(
            bq_client.get_table(table).schema, self.name
        )

        # Table views have no measures

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
