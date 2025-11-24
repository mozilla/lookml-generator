"""An explore for `events_stream` views."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Optional

from ..views import EventsStreamView, View
from .explore import Explore


class EventsStreamExplore(Explore):
    """An explore for `events_stream` views."""

    type: str = "events_stream_explore"

    @staticmethod
    def from_views(views: list[View]) -> Iterator[EventsStreamExplore]:
        """Where possible, generate EventsStreamExplores for views."""
        for view in views:
            if isinstance(view, EventsStreamView):
                yield EventsStreamExplore(view.name, {"base_view": view.name})

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> EventsStreamExplore:
        """Get an instance of this explore from a dictionary definition."""
        return EventsStreamExplore(name, defn["views"], views_path)

    def get_required_filters(self, view_name: str) -> list[dict[str, str]]:
        """Get required filters for this view."""
        # Use 7 days instead of 28 days to avoid "Query Exceeds Data Limit" errors for large event datasets.
        return [{"submission_date": "7 days"}]

    def _to_lookml(self, v1_name: Optional[str]) -> list[dict[str, Any]]:
        lookml: dict[str, Any] = {
            "name": self.name,
            "view_name": self.views["base_view"],
            "joins": self.get_unnested_fields_joins_lookml(),
            "always_filter": self.get_required_filters("base_view"),
            "queries": [
                {
                    "name": "recent_event_counts",
                    "description": "Event counts during the past week.",
                    "dimensions": ["event"],
                    "measures": ["event_count"],
                    "filters": [{"submission_date": "7 days"}],
                },
                {
                    "name": "sampled_recent_event_counts",
                    "description": "A 1% sample of event counts during the past week.",
                    "dimensions": ["event"],
                    "measures": ["event_count"],
                    "filters": [
                        {"submission_date": "7 days"},
                        {"sample_id": "[0, 0]"},
                    ],
                },
            ],
        }

        if datagroup := self.get_datagroup():
            lookml["persist_with"] = datagroup

        return [lookml]
