"""Metric Hub metrics explore type."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery

from ..views import View, lookml_utils
from . import Explore


class MetricDefinitionsExplore(Explore):
    """Metric Hub Metrics Explore."""

    type: str = "metric_definitions_explore"

    def __init__(
        self,
        name: str,
        views: Dict[str, str],
        views_path: Optional[Path] = None,
        defn: Optional[Dict[str, Any]] = None,
    ):
        """Initialize MetricDefinitionsExplore."""
        super().__init__(name, views, views_path)

    @staticmethod
    def from_views(views: List[View]) -> Iterator[Explore]:
        """Generate an Operational Monitoring explore for this namespace."""
        for view in views:
            if view.view_type == "metric_definitions_view":
                yield MetricDefinitionsExplore(
                    "metric_definitions",
                    {"base_view": view.name},
                )

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> MetricDefinitionsExplore:
        """Get an instance of this explore from a dictionary definition."""
        return MetricDefinitionsExplore(name, defn["views"], views_path, defn)

    def _to_lookml(
        self,
        bq_client: bigquery.Client,
        v1_name: Optional[str],
    ) -> List[Dict[str, Any]]:
        intervals = ["date", "week", "month", "quarter", "year", "raw"]
        explore_lookml: Dict[str, Any] = {
            "name": self.name,
            "from": self.views["base_view"],
            "view_label": lookml_utils.slug_to_title(self.name),
            "joins": self._get_joins(),
            "always_filter": {"filters": [{"date": "7 days"}]},
            # The base view is the only view that exposes the date and client_id fields.
            # All other views only expose the metric definitions.
            "fields": [f"{self.name}.date", f"{self.name}.client_id"]
            + [f"{self.name}.submission_{interval}" for interval in intervals]
            + [f"{view}.metrics*" for view in self.views.get("joined_views", [])],
        }

        return [explore_lookml]

    def _get_joins(self):
        """Generate metric definition joins."""
        # Every metric definitions has a submission_date and client_id field that is used to
        # join the different data source views.
        return [
            {
                "name": joined_view,
                "view_label": lookml_utils.slug_to_title(joined_view),
                "relationship": "many_to_many",
                "type": "full_outer",
                "sql_on": f"""
                  SAFE_CAST({self.name}.submission_date AS TIMESTAMP) =
                  SAFE_CAST({joined_view}.submission_date AS TIMESTAMP) AND
                  SAFE_CAST({self.name}.client_id AS STRING) =
                  SAFE_CAST({joined_view}.client_id AS STRING)""",
            }
            for joined_view in self.views.get("joined_views", [])
        ]

    def get_view_time_partitioning_group(self, view: str) -> Optional[str]:
        """Override time partitioning."""
        return None
