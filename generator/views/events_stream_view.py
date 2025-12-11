"""Class to describe an `events_stream` view."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterator, Optional

from . import lookml_utils
from .view import View, ViewDict


class EventsStreamView(View):
    """A view for querying `events_stream` data, with one row per event."""

    type: str = "events_stream_view"

    default_measures: list[dict[str, str]] = [
        {
            "name": "event_count",
            "type": "count",
            "description": "The number of times the event(s) occurred.",
        },
        # GleanPingViews were previously generated for some `events_stream` views, and those had
        # `ping_count` measures, so we generate the same measures here to avoid breaking anything.
        # TODO: Remove this once dashboards have been migrated to use the proper `event_count` measures.
        {
            "name": "ping_count",
            "type": "count",
            "hidden": "yes",
        },
    ]

    def __init__(self, namespace: str, name: str, tables: list[dict[str, str]]):
        """Get an instance of an EventsStreamView."""
        super().__init__(namespace, name, EventsStreamView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: list[dict[str, str]],
        db_views: dict,
    ) -> Iterator[EventsStreamView]:
        """Get EventsStreamViews from db views."""
        for view_id in db_views[namespace]:
            if view_id.endswith("events_stream"):
                yield EventsStreamView(
                    namespace,
                    view_id,
                    [{"table": f"mozdata.{namespace}.{view_id}"}],
                )

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, _dict: ViewDict
    ) -> EventsStreamView:
        """Get EventsStreamView from a name and dict definition."""
        return EventsStreamView(namespace, name, _dict["tables"])

    def to_lookml(self, v1_name: Optional[str], dryrun) -> dict[str, Any]:
        """Generate LookML for this view."""
        dimensions = lookml_utils._generate_dimensions(
            self.tables[0]["table"], dryrun=dryrun
        )
        for dimension in dimensions:
            if dimension["name"] == "event_id":
                dimension["primary_key"] = "yes"

        measures = self.get_measures(dimensions)

        return {
            "views": [
                {
                    "name": self.name,
                    "sql_table_name": f"`{self.tables[0]['table']}`",
                    "dimensions": [
                        d for d in dimensions if not lookml_utils._is_dimension_group(d)
                    ],
                    "dimension_groups": [
                        d for d in dimensions if lookml_utils._is_dimension_group(d)
                    ],
                    "measures": measures,
                },
            ],
        }

    def get_measures(self, dimensions: list[dict[str, Any]]) -> list[dict[str, str]]:
        """Get measures for this view."""
        measures = deepcopy(EventsStreamView.default_measures)
        if client_id_dimension := self.get_client_id(
            dimensions, self.tables[0]["table"]
        ):
            measures.append(
                {
                    "name": "client_count",
                    "type": "count_distinct",
                    "sql": f"${{{client_id_dimension}}}",
                    "description": "The number of clients that completed the event(s).",
                }
            )
            # GleanPingViews were previously generated for some `events_stream` views, and those had
            # `clients` measures, so we generate the same measures here to avoid breaking anything.
            # TODO: Remove this once dashboards have been migrated to use the proper `client_count` measures.
            measures.append(
                {
                    "name": "clients",
                    "type": "count_distinct",
                    "sql": f"${{{client_id_dimension}}}",
                    "hidden": "yes",
                }
            )
        return measures
