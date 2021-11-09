"""Operational Monitoring Explore type."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery

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

    def _to_lookml(
        self,
        bq_client: bigquery.Client,
        v1_name: Optional[str],
        data: Dict = {},
    ) -> List[Dict[str, Any]]:
        base_view_name = self.views["base_view"]

        namespace_data = data.get("compute_opmon_dimensions", {}).get(
            base_view_name, {}
        )
        table_name = (
            "" if len(namespace_data.keys()) == 0 else list(namespace_data.keys())[0]
        )
        dimension_data = namespace_data[table_name]

        filters = [
            {f"{base_view_name}.branch": self.branches},
            {f"{base_view_name}.percentile_conf": "50"},
        ]
        for dimension in dimension_data:
            filters.append(
                {f"{base_view_name}.{dimension['name']}": dimension["default"]}
            )

        aggregate_tables = []
        probes = lookml_utils.get_distinct_vals(bq_client, table_name, "probe")
        for probe in probes:
            filters_copy = deepcopy(filters)
            filters_copy.append({f"{base_view_name}.probe": probe})
            aggregate_tables.append(
                {
                    "name": f"rollup_{probe}",
                    "query": {
                        "dimensions": ["build_id", "branch"],
                        "measures": ["low", "high", "percentile"],
                        "filters": filters_copy,
                    },
                    "materialization": {"sql_trigger_value": "SELECT CURRENT_DATE()"},
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
