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

    def to_lookml(self) -> dict:
        """Generate LookML to represent this explore."""
        return {
            "name": self.name,
            "view_name": self.views["base_view"],
        }

    @staticmethod
    def from_views(views: Dict[str, List[Dict[str, str]]]) -> Iterator[PingExplore]:
        """Generate all possible PingExplores from the views."""
        for view, channel_infos in views.items():
            is_ping_tbl = all((c.get("is_ping_table", False) for c in channel_infos))
            if is_ping_tbl:
                yield PingExplore(view, "ping_explore", {"base_view": view})

    @staticmethod
    def from_dict(name: str, defn: dict) -> PingExplore:
        """Get an instance of this explore from a name and dictionary definition."""
        return PingExplore(name, "ping_explore", defn["views"])


class GrowthAccountingExplore(Explore):
    """A Growth Accounting Explore, from Baseline Clients Last Seen."""

    @staticmethod
    def from_views(views: Dict[str, List[Dict[str, str]]]) -> Iterator[PingExplore]:
        """
        If possible, generate a Growth Accounting explore for this namespace.

        Growth accounting explores are only created for growth_accounting views.
        """
        if "growth_accounting" in views.keys():
            yield GrowthAccountingExplore(
                "growth_accounting",
                "growth_accounting_explore",
                {"base_view": "growth_accounting"},
            )


explore_types = {
    "ping_explore": PingExplore,
    "growth_accounting_explore": GrowthAccountingExplore,
}
