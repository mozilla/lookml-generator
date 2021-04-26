"""Growth Accounting explore type."""
from __future__ import annotations

from typing import Iterator, List

from ..views import View
from . import Explore


class GrowthAccountingExplore(Explore):
    """A Growth Accounting Explore, from Baseline Clients Last Seen."""

    type: str = "growth_accounting_explore"

    def to_lookml(self) -> dict:
        """Generate LookML to represent this explore."""
        return {
            "name": self.name,
            "view_name": self.views["base_view"],
        }

    @staticmethod
    def from_views(views: List[View]) -> Iterator[GrowthAccountingExplore]:
        """
        If possible, generate a Growth Accounting explore for this namespace.

        Growth accounting explores are only created for growth_accounting views.
        """
        for view in views:
            if view.name == "growth_accounting":
                yield GrowthAccountingExplore(
                    "growth_accounting",
                    {"base_view": "growth_accounting"},
                )

    @staticmethod
    def from_dict(name: str, defn: dict) -> GrowthAccountingExplore:
        """Get an instance of this explore from a dictionary definition."""
        return GrowthAccountingExplore(name, defn["views"])
