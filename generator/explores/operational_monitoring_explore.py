"""Operational Monitoring Explore type."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery

from .. import operational_monitoring_utils
from ..views import View, lookml_utils
from . import Explore


class OperationalMonitoringExplore(Explore):
    """An Operational Monitoring Explore."""

    type: str = "operational_monitoring_explore"

    def __init__(
        self,
        name: str,
        views: Dict[str, str],
        views_path: Path = None,
        defn: Dict[str, Any] = None,
    ):
        """Initialize OperationalMonitoringExplore."""
        super().__init__(name, views, views_path)
        if defn is not None:
            self.branches = ", ".join(defn["branches"])
            self.xaxis = defn.get("xaxis")
            self.dimensions = defn.get("dimensions", [])
            self.probes = defn.get("probes", [])

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

    def _to_lookml(
        self,
        bq_client: bigquery.Client,
        v1_name: Optional[str],
        data: Dict = {},
    ) -> List[Dict[str, Any]]:
        base_view_name = self.views["base_view"]

        dimension_data = operational_monitoring_utils.compute_opmon_dimensions(
            bq_client=bq_client, table=base_view_name, allowed_dimensions=self.dimensions
        )

        filters = [
            {f"{base_view_name}.branch": self.branches},
            {f"{base_view_name}.percentile_conf": "50"},
        ]
        for dimension in dimension_data:
            if "default" in dimension:
                filters.append(
                    {f"{base_view_name}.{dimension['name']}": dimension["default"]}
                )

        aggregate_tables = []
        for probe in self.probes:
            filters_copy = deepcopy(filters)
            filters_copy.append({f"{base_view_name}.probe": probe})
            aggregate_tables.append(
                {
                    "name": f"rollup_{probe}",
                    "query": {
                        "dimensions": [self.xaxis, "branch"],
                        "measures": ["low", "high", "percentile"],
                        "filters": filters_copy,
                    },
                    "materialization": {
                        # Reload the table at 9am when ETL should have been completed
                        "sql_trigger_value": "SELECT CAST(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 9 HOUR) AS DATE)"
                    },
                }
            )

        defn: List[Dict[str, Any]] = [
            {
                "name": self.views["base_view"],
                "always_filter": {
                    "filters": [
                        {"branch": self.branches},
                    ]
                },
                "aggregate_table": aggregate_tables,
            },
        ]

        return defn
