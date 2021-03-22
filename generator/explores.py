"""All possible generated explores."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterator, List


@dataclass
class Explore:
    """A generic explore."""

    name: str
    type: str
    views: Dict[str, str]

    def to_dict(self) -> dict:
        """Explore instance represented as a dict."""
        return {self.name: {"type": self.type, "views": self.views}}


@dataclass
class PingExplore(Explore):
    """A Ping Table explore."""

    @staticmethod
    def from_views(views: Dict[str, List[Dict[str, str]]]) -> Iterator[PingExplore]:
        """Generate all possible PingExplores from the views."""
        for view, channel_infos in views.items():
            is_ping_tbl = all((c.get("is_ping_table", False) for c in channel_infos))
            if is_ping_tbl:
                yield PingExplore(view, "ping_explore", {"base_view": view})


explore_types = [PingExplore]
