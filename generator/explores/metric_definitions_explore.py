"""Metric Hub metrics explore type."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from google.cloud import bigquery

from generator.metrics_utils import MetricsConfigLoader

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
        _bq_client: bigquery.Client,
        _v1_name: Optional[str],
    ) -> List[Dict[str, Any]]:
        exposed_fields = ["ALL_FIELDS*"]
        if (
            "baseline_clients_daily_table" != self.views["base_view"]
            and "clients_daily_table" != self.views["base_view"]
        ):
            exposed_fields.append(f"-{self.views['base_view']}.metrics*")

        explore_lookml: Dict[str, Any] = {
            "name": self.name,
            "from": self.views["base_view"],
            "view_label": "Base Fields",
            "joins": self._get_joins(),
            "always_filter": {"filters": [{"submission_date": "7 days"}]},
            # The base view is the only view that exposes the date and client_id fields.
            # All other views only expose the metric definitions.
            "fields": exposed_fields,
        }

        return [explore_lookml]

    def _get_joins(self):
        """Generate metric definition joins."""
        # Every metric definitions has a submission_date and client_id field that is used to
        # join the different data source views.
        joins = []

        for joined_view in self.views.get("joined_views", []):
            data_source_name = re.sub("^metric_definitions_", "", joined_view)
            data_source_definition = (
                MetricsConfigLoader.configs.get_data_source_definition(
                    data_source_name, self.views_path.parent.name
                )
            )

            # only join on client_id if it is not explicity set to NULL,
            # otherwise we are loosing data
            sql_on = f"""
                  SAFE_CAST({self.name}.submission_date AS TIMESTAMP) =
                  SAFE_CAST({joined_view}.submission_date AS TIMESTAMP)"""

            if data_source_definition.client_id_column != "NULL":
                sql_on += f""" AND SAFE_CAST({self.name}.client_id AS STRING) =
                  SAFE_CAST({joined_view}.client_id AS STRING)"""

            joins.append(
                {
                    "name": joined_view,
                    "view_label": lookml_utils.slug_to_title(joined_view),
                    "relationship": "many_to_many",
                    "type": "full_outer",
                    "fields": ["metrics*"],
                    "sql_on": sql_on,
                }
            )

        return joins

    def get_view_time_partitioning_group(self, view: str) -> Optional[str]:
        """Override time partitioning."""
        return None
