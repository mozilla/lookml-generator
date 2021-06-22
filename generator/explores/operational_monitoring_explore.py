"""Operational Monitoring Explore type."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from ..views import View
from . import Explore


class OperationalMonitoringExplore(Explore):
    """An Operational Monitoring Explore."""

    type: str = "operational_monitoring_explore"

    def __init__(
        self,
        name: str,
        views: Dict[str, str],
        views_path: Path = None,
        defn: Dict[str, str] = None,
    ):
        """Initialize OperationalMonitoringExplore."""
        super().__init__(name, views, views_path)
        if defn is not None:
            self.branches = ", ".join(defn["branches"])

    @staticmethod
    def from_views(views: List[View]) -> Iterator[Explore]:
        """Generate an Operational Monitoring explore for this namespace."""
        for view in views:
            if view.view_type in {
                "operational_monitoring_histogram_view",
                "operational_monitoring_scalar_view",
            }:
                yield OperationalMonitoringExplore(
                    "operational_monitoring",
                    {"base_view": view.name},
                )

    @staticmethod
    def from_dict(
        name: str, defn: dict, views_path: Path
    ) -> OperationalMonitoringExplore:
        """Get an instance of this explore from a dictionary definition."""
        return OperationalMonitoringExplore(name, defn["views"], views_path, defn)

    def _to_lookml(self, v1_name: Optional[str]) -> List[Dict[str, Any]]:
        defn: List[Dict[str, Any]] = [
            {
                "name": self.views["base_view"],
                "always_filter": {
                    "filters": [
                        {"os": "Windows"},
                        {"branch": self.branches},
                    ]
                },
            },
        ]

        return defn
