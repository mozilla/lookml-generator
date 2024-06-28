"""Client Counts explore type."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery

from ..views import View
from . import Explore


class ClientCountsExplore(Explore):
    """A Client Counts Explore, from Baseline Clients Last Seen."""

    type: str = "client_counts_explore"

    def _to_lookml(
        self, v1_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Generate LookML to represent this explore."""
        queries = []
        if time_partitioning_group := self.get_view_time_partitioning_group(
            self.views["extended_view"]
        ):
            date_dimension = f"{time_partitioning_group}_date"
            queries.append(
                {
                    "description": "Client Counts of weekly cohorts over the past N days.",
                    "dimensions": ["days_since_first_seen", "first_seen_week"],
                    "measures": ["client_count"],
                    "pivots": ["first_seen_week"],
                    "filters": [
                        {date_dimension: "8 weeks"},
                        {"first_seen_date": "8 weeks"},
                        {"have_completed_period": "yes"},
                    ],
                    "sorts": [{"days_since_first_seen": "asc"}],
                    "name": "cohort_analysis",
                }
            )
            if self.has_view_dimension(self.views["extended_view"], "app_build"):
                queries.append(
                    {
                        "description": "Number of clients per build.",
                        "dimensions": [date_dimension, "app_build"],
                        "measures": ["client_count"],
                        "pivots": ["app_build"],
                        "sorts": [{date_dimension: "asc"}],
                        "name": "build_breakdown",
                    }
                )
        return [
            {
                "name": self.name,
                "view_name": self.views["base_view"],
                "description": "Client counts across dimensions and cohorts.",
                "always_filter": {
                    "filters": self.get_required_filters("extended_view"),
                },
                "queries": queries,
                "joins": self.get_unnested_fields_joins_lookml(),
            }
        ]

    @staticmethod
    def from_views(views: List[View]) -> Iterator[ClientCountsExplore]:
        """
        If possible, generate a Client Counts explore for this namespace.

        Client counts explores are only created for client_counts views.
        """
        for view in views:
            if view.name == "client_counts":
                yield ClientCountsExplore(
                    view.name,
                    {
                        "base_view": "client_counts",
                        "extended_view": "baseline_clients_daily_table",
                    },
                )

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> ClientCountsExplore:
        """Get an instance of this explore from a dictionary definition."""
        return ClientCountsExplore(name, defn["views"], views_path)
