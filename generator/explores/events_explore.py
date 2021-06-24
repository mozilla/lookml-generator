"""An explore for Events Views."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from ..views import EventsView, View
from .explore import Explore


class EventsExplore(Explore):
    """An Events Explore, from any unnested events table."""

    type: str = "events_explore"

    queries: List[dict] = [
        {
            "description": "Event counts from all events over the past two weeks.",
            "dimensions": ["submission_date"],
            "measures": ["event_count"],
            "filters": [
                {"submission_date": "14 days"},
            ],
            "name": "all_event_counts",
        },
    ]

    @staticmethod
    def from_views(views: List[View]) -> Iterator[EventsExplore]:
        """Where possible, generate EventsExplores for Views."""
        for view in views:
            if isinstance(view, EventsView):
                yield EventsExplore(
                    view.name,
                    {
                        "base_view": "events",
                        "extended_view": view.tables[0]["events_table_view"],
                    },
                )

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> EventsExplore:
        """Get an instance of this explore from a dictionary definition."""
        return EventsExplore(name, defn["views"], views_path)

    def _to_lookml(self, v1_name: Optional[str]) -> List[Dict[str, Any]]:
        lookml = {
            "name": "event_counts",
            "view_name": self.views["base_view"],
            "description": "Event counts over time.",
            "queries": deepcopy(EventsExplore.queries),
        }
        required_filters = self.get_required_filters("extended_view")
        if required_filters:
            lookml["always_filter"] = {"filters": required_filters}
        return [lookml]
