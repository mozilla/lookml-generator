"""Classes to describe Looker views."""
from __future__ import annotations

from collections import defaultdict
from itertools import filterfalse
from typing import Any, Dict, Iterator, List, TypedDict

import click

from . import lookml_utils

OMIT_VIEWS = {"deletion_request"}


class ViewDict(TypedDict):
    """Represent a view definition."""

    type: str
    tables: List[Dict[str, str]]


class View(object):
    """A generic Looker View."""

    name: str
    view_type: str
    tables: List[Dict[str, str]]

    def __init__(self, name: str, view_type: str, tables: List[Dict[str, str]]):
        """Create an instance of a view."""
        self.tables = tables
        self.name = name
        self.view_type = view_type

    @classmethod
    def from_db_views(
        klass, app: str, channels: List[Dict[str, str]], db_views: dict
    ) -> Iterator[View]:
        """Get Looker views from app."""
        raise NotImplementedError("Only implemented in subclass.")

    @classmethod
    def from_dict(klass, name: str, _dict: ViewDict) -> View:
        """Get a view from a name and dict definition."""
        raise NotImplementedError("Only implemented in subclass.")

    def get_type(self) -> str:
        """Get the type of this view."""
        return self.view_type

    def as_dict(self) -> dict:
        """Get this view as a dictionary."""
        return {
            "type": self.view_type,
            "tables": self.tables,
        }

    def __str__(self):
        """Stringify."""
        return f"name: {self.name}, type: {self.type}, table: {self.tables}"

    def __eq__(self, other) -> bool:
        """Check for equality with other View."""

        def comparable_dict(d):
            return {tuple(sorted(t.items())) for t in self.tables}

        if isinstance(other, View):
            return (
                self.name == other.name
                and self.view_type == other.view_type
                and comparable_dict(self.tables) == comparable_dict(other.tables)
            )
        return False

    def to_lookml(self, bq_client) -> List[dict]:
        """
        Generate Lookml for this view.

        View instances can generate more than one Looker view,
        for e.g. nested fields and joins, so this returns
        a list.
        """
        raise NotImplementedError("Only implemented in subclass.")


class PingView(View):
    """A view on a ping table."""

    type: str = "ping_view"

    def __init__(self, name: str, tables: List[Dict[str, str]]):
        """Create instance of a PingView."""
        super().__init__(name, PingView.type, tables)

    @classmethod
    def from_db_views(
        klass, app: str, channels: List[Dict[str, str]], db_views: dict
    ) -> Iterator[PingView]:
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
                if len(references) != 1 or references[0][-2] != f"{dataset}_stable":
                    continue  # This view is only for ping tables

                views[view_id].append(table)

        for view_id, tables in views.items():
            yield PingView(view_id, tables)

    @classmethod
    def from_dict(klass, name: str, _dict: ViewDict) -> PingView:
        """Get a view from a name and dict definition."""
        return PingView(name, _dict["tables"])

    def to_lookml(self, bq_client) -> List[dict]:
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

        # add measures
        view_defn["measures"] = self.get_measures(dimensions, table)

        # parameterize table name
        if len(self.tables) > 1:
            view_defn["parameters"] = [
                {
                    "name": "channel",
                    "type": "unquoted",
                    "allowed_values": [
                        {
                            "label": table["channel"].title(),
                            "value": table["table"],
                        }
                        for table in self.tables
                    ],
                }
            ]
            view_defn["sql_table_name"] = "`{% parameter channel %}`"
        else:
            view_defn["sql_table_name"] = f"`{table}`"

        return [view_defn]

    def get_measures(self, dimensions: List[dict], table: str) -> List[Dict[str, str]]:
        """Generate measures from a list of dimensions.

        When no dimension-specific measures are found, return a single "count" measure.

        Raise ClickException if dimensions result in duplicate measures.
        """
        measures = {}

        for dimension in dimensions:
            dimension_name = dimension["name"]
            if dimension_name in {"client_id", "client_info__client_id"}:
                measure = {
                    "name": "clients",
                    "type": "count_distinct",
                    "sql": f"${{{dimension_name}}}",
                }
            elif dimension_name == "document_id":
                measure = {"name": "ping_count", "type": "count"}
            else:
                continue
            name = measure["name"]
            if name in measures:
                raise click.ClickException(
                    f"duplicate measure {name!r} for table {table!r}"
                )
            measures[name] = measure

        if len(measures) == 0:
            raise click.ClickException(
                f"Missing client_id and doc_id dimensions in {table!r}"
            )

        return list(measures.values())


class GrowthAccountingView(View):
    """A view for growth accounting measures."""

    type: str = "growth_accounting_view"

    def __init__(self, tables: List[Dict[str, str]]):
        """Get an instance of a GrowthAccountingView."""
        super().__init__("growth_accounting", GrowthAccountingView.type, tables)

    @classmethod
    def from_db_views(
        klass, app: str, channels: List[Dict[str, str]], db_views: dict
    ) -> Iterator[GrowthAccountingView]:
        """Get Growth Accounting Views from db views and app variants."""
        dataset = next(
            (channel for channel in channels if channel.get("channel") == "release"),
            channels[0],
        )["dataset"]

        for view_id, references in db_views[dataset].items():
            if view_id == "baseline_clients_last_seen":
                yield GrowthAccountingView([{"table": f"mozdata.{dataset}.{view_id}"}])

    @classmethod
    def from_dict(klass, name: str, _dict: ViewDict) -> GrowthAccountingView:
        """Get a view from a name and dict definition."""
        return GrowthAccountingView(_dict["tables"])

    def to_lookml(self, bq_client) -> List[dict]:
        """Generate LookML for this view."""
        pass


view_types = {
    PingView.type: PingView,
    GrowthAccountingView.type: GrowthAccountingView,
}
