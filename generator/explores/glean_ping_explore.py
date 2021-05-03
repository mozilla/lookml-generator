from typing import Iterator, List

from .ping_explore import PingExplore
from ..views import GleanPingView, View


class GleanPingExplore(PingExplore):
    """A Glean Ping Table explore."""

    type: str = "glean_ping_explore"

    @staticmethod
    def from_views(views: List[View]) -> Iterator[PingExplore]:
        """Generate all possible PingExplores from the views."""
        for view in views:
            if view.view_type == GleanPingView.type:
                yield GleanPingExplore(view.name, {"base_view": view.name})
