"""Ping explore type."""
from __future__ import annotations

from typing import Iterator, List

from ..views import PingView, View
from . import Explore


class PingExplore(Explore):
    """A Ping Table explore."""

    type: str = "ping_explore"

    def to_lookml(self) -> dict:
        """Generate LookML to represent this explore."""
        return {
            "name": self.name,
            "view_name": self.views["base_view"],
        }

    @staticmethod
    def from_views(views: List[View]) -> Iterator[PingExplore]:
        """Generate all possible PingExplores from the views."""
        for view in views:
            if view.view_type == PingView.type:
                yield PingExplore(view.name, {"base_view": view.name})

    @staticmethod
    def from_dict(name: str, defn: dict) -> PingExplore:
        """Get an instance of this explore from a name and dictionary definition."""
        return PingExplore(name, defn["views"])
