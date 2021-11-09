"""Ping explore type."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery

from ..views import PingView, View
from . import Explore


class PingExplore(Explore):
    """A Ping Table explore."""

    type: str = "ping_explore"

    def _to_lookml(
        self, client: bigquery.Client, v1_name: Optional[str], data: Dict = {}
    ) -> List[Dict[str, Any]]:
        """Generate LookML to represent this explore."""
        views_lookml = self.get_view_lookml(self.views["base_view"])
        views: List[str] = [view["name"] for view in views_lookml["views"]]

        joins = []
        for view in views_lookml["views"][1:]:
            view_name = view["name"]
            base_name, metric = self._get_base_name_and_metric(
                view_name=view_name, views=views
            )
            joins.append(
                {
                    "name": view_name,
                    "relationship": "one_to_many",
                    "sql": (
                        f"LEFT JOIN UNNEST(${{{base_name}.{metric}}}) AS {view_name} "
                    ),
                }
            )

        return [
            {
                "name": self.name,
                "view_name": self.views["base_view"],
                "always_filter": {
                    "filters": self.get_required_filters("base_view"),
                },
                "joins": joins,
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
