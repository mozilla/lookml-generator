"""Ping explore type."""
from __future__ import annotations

from pathlib import Path
from typing import Iterator, List

from ..views import PingView, View
from ..views.lookml_utils import escape_filter_expr
from . import Explore


class PingExplore(Explore):
    """A Ping Table explore."""

    type: str = "ping_explore"

    def to_lookml(self) -> dict:
        """Generate LookML to represent this explore."""
        filters = [{"submission_date": "28 days"}]
        view = self.views["base_view"]

        # Add a default filter on channel, if it's present in the view
        channel_params = [
            param
            for _view_defn in self.get_view_lookml(view)["views"]
            for param in _view_defn.get("parameters", [])
            if _view_defn["name"] == view and param["name"] == "channel"
        ]

        if channel_params:
            allowed_values = channel_params[0]["allowed_values"]
            default_value = next(
                (value for value in allowed_values if value["label"] == "Release"),
                allowed_values[0],
            )["value"]

            filters.append({"channel": escape_filter_expr(default_value)})

        return {
            "name": self.name,
            "view_name": self.views["base_view"],
            "always_filter": {
                "filters": filters,
            },
        }

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
