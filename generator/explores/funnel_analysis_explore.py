"""Funnel Analysis explore type."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List

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

    def _to_lookml(self) -> List[Dict[str, Any]]:
        view_lookml = self.get_view_lookml("funnel_analysis")
        views = view_lookml["views"]
        n_events = len([d for d in views if d["name"].startswith("event_type_")])
        defn: List[Dict[str, Any]] = [
            {
                "name": "funnel_analysis",
                "view_label": " User-Day Funnels",
                "always_filter": {
                    "filters": [
                        {"submission_date": "14 days"},
                    ]
                },
                "joins": [
                    {
                        "name": f"event_type_{n}",
                        "relationship": "many_to_one",
                        "type": "cross",
                    }
                    for n in range(1, n_events + 1)
                ],
                "sql_always_where": "${funnel_analysis.submission_date} >= '2010-01-01'",
            },
            {"name": "event_names", "hidden": "yes"},
        ]

        return defn
