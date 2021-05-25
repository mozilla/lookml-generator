"""Funnel Analysis explore type."""
from __future__ import annotations

from pathlib import Path
from typing import Iterator, List

from ..views import View
from . import Explore


class FunnelAnalysisExplore(Explore):
    """A Funnel Analysis Explore, from Baseline Clients Last Seen."""

    type: str = "funnel_analysis_explore"
    n_funnel_steps: int = 4

    @staticmethod
    def from_views(views: List[View]) -> Iterator[FunnelAnalysisExplore]:
        """
        If possible, generate a Funnel Analysis explore for this namespace.

        Funnel analysis explores are only created for funnel_analysis views.
        """
        for view in views:
            if view.name == "funnel_analysis":
                yield FunnelAnalysisExplore(
                    "funnel_analysis",
                    {"base_view": view.name},
                )

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> FunnelAnalysisExplore:
        """Get an instance of this explore from a dictionary definition."""
        return FunnelAnalysisExplore(name, defn["views"], views_path)
