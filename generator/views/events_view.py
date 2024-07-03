"""Class to describe an Events view."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterator, List, Optional

from . import lookml_utils
from .view import View, ViewDict


class EventsView(View):
    """A view for querying events data, with one row per-event."""

    type: str = "events_view"

    default_measures: List[Dict[str, str]] = [
        {
            "name": "event_count",
            "type": "count",
            "description": ("The number of times the event(s) occurred."),
        },
    ]

    def __init__(self, namespace: str, name: str, tables: List[Dict[str, str]]):
        """Get an instance of an EventsView."""
        super().__init__(namespace, name, EventsView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[EventsView]:
        """Get Events Views from db views and app variants."""
        # We can guarantee there will always be at least one channel,
        # because this comes from the associated _get_glean_repos in
        # namespaces.py
        dataset = next(
            (channel for channel in channels if channel.get("channel") == "release"),
            channels[0],
        )["dataset"]

        for view_id, references in db_views[dataset].items():
            if view_id == "events_unnested":
                yield EventsView(
                    namespace,
                    "events",
                    [
                        {
                            "events_table_view": "events_unnested_table",
                            "base_table": f"mozdata.{dataset}.{view_id}",
                        }
                    ],
                )

    @classmethod
    def from_dict(klass, namespace: str, name: str, _dict: ViewDict) -> EventsView:
        """Get a view from a name and dict definition."""
        return EventsView(namespace, name, _dict["tables"])

    def to_lookml(self, v1_name: Optional[str]) -> Dict[str, Any]:
        """Generate LookML for this view."""
        view_defn: Dict[str, Any] = {
            "extends": [self.tables[0]["events_table_view"]],
            "name": self.name,
        }

        # add measures
        dimensions = lookml_utils._generate_dimensions(self.tables[0]["base_table"])
        view_defn["measures"] = self.get_measures(dimensions)

        # set document_id as primary key if it exists in the underlying table
        # this will allow one_to_many joins
        event_id_dimension = self.generate_event_id_dimension(dimensions)
        if event_id_dimension is not None:
            view_defn["dimensions"] = [event_id_dimension]

        return {
            "includes": [f"{self.tables[0]['events_table_view']}.view.lkml"],
            "views": [view_defn],
        }

    def get_measures(self, dimensions) -> List[Dict[str, str]]:
        """Generate measures for Events Views."""
        measures = deepcopy(EventsView.default_measures)
        client_id_field = self.get_client_id(dimensions, "events")
        if client_id_field is not None:
            measures.append(
                {
                    "name": "client_count",
                    "type": "count_distinct",
                    "sql": f"${{{client_id_field}}}",
                    "description": (
                        "The number of clients that completed the event(s)."
                    ),
                }
            )

        return measures

    def generate_event_id_dimension(
        self, dimensions: list[dict]
    ) -> Optional[Dict[str, str]]:
        """Generate the event_id dimension to be used as a primary key for a one to many join."""
        event_id = self.select_dimension("event_id", dimensions, "events")
        if event_id:
            return {
                "name": "event_id",
                "primary_key": "yes",
            }
        return None
