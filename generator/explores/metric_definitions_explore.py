"""Metric Hub metrics explore type."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery

from ..views import View
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
                yield MetricDefinitionsExplore("metric_definitions", {})

    @staticmethod
    def from_dict(name: str, defn: dict, views_path: Path) -> MetricDefinitionsExplore:
        """Get an instance of this explore from a dictionary definition."""
        return MetricDefinitionsExplore(name, defn["views"], views_path, defn)

    def _to_lookml(
        self,
        _bq_client: bigquery.Client,
        _v1_name: Optional[str],
    ) -> List[Dict[str, Any]]:
        exposed_fields = ["ALL_FIELDS*"]

        explore_lookml: Dict[str, Any] = {
            "name": self.name,
            "always_filter": {"filters": [{"submission_date": "7 days"}]},
            # The base view is the only view that exposes the date and client_id fields.
            # All other views only expose the metric definitions.
            "fields": exposed_fields,
        }

        return [explore_lookml]

    def get_view_time_partitioning_group(self, view: str) -> Optional[str]:
        """Override time partitioning."""
        return None
