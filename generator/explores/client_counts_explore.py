"""Client Counts explore type."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Iterator, List

from ..views import View
from . import Explore


class ClientCountsExplore(Explore):
    """A Client Counts Explore, from Baseline Clients Last Seen."""

    type: str = "client_counts_explore"
    queries: List[dict] = [
        {
            "description": "Client Counts of weekly cohorts over the past N days.",
            "dimensions": ["days_since_first_seen", "first_seen_week"],
            "measures": ["client_count"],
            "pivots": ["first_seen_week"],
            "filters": [
                {"submission_date": "8 weeks"},
                {"first_seen_date": "8 weeks"},
                {"have_completed_period": "yes"},
            ],
            "sorts": [{"days_since_first_seen": "asc"}],
            "name": "cohort_analysis",
        },
        {
            "description": "Number of clients per build.",
            "dimensions": ["submission_date", "app_build"],
            "measures": ["client_count"],
            "pivots": ["app_build"],
            "sorts": [{"submission_date": "asc"}],
            "name": "build_breakdown",
        },
    ]

    def _to_lookml(self) -> dict:
        """Generate LookML to represent this explore."""
        return {
            "name": self.name,
            "view_name": self.views["base_view"],
            "description": "Client counts across dimensions and cohorts.",
            "always_filter": {
                "filters": self.get_required_filters("extended_view"),
            },
            "queries": deepcopy(ClientCountsExplore.queries),
        }

    @staticmethod
    def from_views(views: List[View]) -> Iterator[ClientCountsExplore]:
        """
        If possible, generate a Client Counts explore for this namespace.

        Client counts explores are only created for client_counts views.
        """
        for view in views:
            if view.name == "client_counts":
                yield ClientCountsExplore(
                    "client_counts",
                    {
                        "base_view": "client_counts",
                        "extended_view": "baseline_clients_daily_table",
                    },
                )

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> ClientCountsExplore:
        """Get an instance of this explore from a dictionary definition."""
        return ClientCountsExplore(name, defn["views"], views_path)
