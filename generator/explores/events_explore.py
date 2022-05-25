"""An explore for Events Views."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery

from ..views import EventsView, View
from .explore import Explore


class EventsExplore(Explore):
    """An Events Explore, from any unnested events table."""

    type: str = "events_explore"

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

    def _to_lookml(
        self, client: bigquery.Client, v1_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        name = self.name
        if not name.endswith("_counts"):
            name = "event_counts"

        lookml: Dict[str, Any] = {
            "name": name,
            "view_name": self.views["base_view"],
            "description": "Event counts over time.",
            "joins": self.get_unnested_fields_joins_lookml(),
        }
        if required_filters := self.get_required_filters("extended_view"):
            lookml["always_filter"] = {"filters": required_filters}
        if time_partitioning_group := self.get_view_time_partitioning_group(
            self.views["extended_view"]
        ):
            date_dimension = f"{time_partitioning_group}_date"
            lookml["queries"] = [
                {
                    "description": "Event counts from all events over the past two weeks.",
                    "dimensions": [date_dimension],
                    "measures": ["event_count"],
                    "filters": [
                        {date_dimension: "14 days"},
                    ],
                    "name": "all_event_counts",
                },
            ]
        return [lookml]
