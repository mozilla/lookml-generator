"""Ping explore type."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from ..views import PingView, View
from . import Explore


class PingExplore(Explore):
    """A Ping Table explore."""

    type: str = "ping_explore"
    queries: List[dict] = [
        {
            "description": "Ping count over the past 28 days",
            "dimensions": ["submission_date"],
            "measures": ["ping_count"],
            "filters": [{"submission_date": "28 days"}],
            "sorts": [{"submission_date": "desc"}],
            "name": "ping_count",
        },
        {
            "description": "Ping count per version in the past 6 months",
            "dimensions": ["submission_date", "version"],
            "measures": ["ping_count"],
            "filters": [{"submission_date": "6 months"}],
            "sorts": [{"submission_date": "desc"}],
            "name": "ping_count_per_version",
        },
    ]

    def _to_lookml(self, v1_name: Optional[str]) -> List[Dict[str, Any]]:
        """Generate LookML to represent this explore."""
        return [
            {
                "name": self.name,
                "view_name": self.views["base_view"],
                "always_filter": {
                    "filters": self.get_required_filters("base_view"),
                },
                "queries": deepcopy(PingExplore.queries),
            }
        ]

    @staticmethod
    def from_views(views: List[View]) -> Iterator[PingExplore]:
        """Generate all possible PingExplores from the views."""
        for view in views:
            if view.view_type == PingView.type:
                yield PingExplore(view.name, {"base_view": view.name})

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> PingExplore:
        """Get an instance of this explore from a name and dictionary definition."""
        return PingExplore(name, defn["views"], views_path)
