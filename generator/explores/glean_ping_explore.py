"""Glean Ping explore type."""
from typing import Iterator, List

from ..views import GleanPingView, View
from .ping_explore import PingExplore


class GleanPingExplore(PingExplore):
    """A Glean Ping Table explore."""

    type: str = "glean_ping_explore"

    @staticmethod
    def from_views(views: List[View]) -> Iterator[PingExplore]:
        """Generate all possible GleanPingExplores from the views."""
        for view in views:
            if view.view_type == GleanPingView.type:
                yield GleanPingExplore(view.name, {"base_view": view.name})
