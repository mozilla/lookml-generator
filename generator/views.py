"""Classes to describe Looker views."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterator, List

OMIT_VIEWS = {"deletion_request"}


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

                if channel.get("channel"):
                    table["channel"] = channel["channel"]
                if len(references) != 1 or references[0][-2] != f"{dataset}_stable":
                    continue  # This view is only for ping tables

                views[view_id].append(table)

        for view_id, tables in views.items():
            yield PingView(view_id, tables)


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


view_types = {
    PingView.type: PingView,
    GrowthAccountingView.type: GrowthAccountingView,
}
