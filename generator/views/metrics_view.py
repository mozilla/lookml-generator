"""Class to describe a view with metrics from metric-hub."""
from __future__ import annotations
import re

from copy import deepcopy
from typing import Any, Dict, Iterator, List, Optional, Union

from . import lookml_utils
from .view import View, ViewDict
from generator.metrics_utils import MetricsConfigLoader


class MetricsView(View):
    """A view for metric-hub metrics that come from the same data source."""

    type: str = "metrics_view"

    def __init__(self, namespace: str, name: str, tables: List[Dict[str, str]]):
        """Get an instance of an MetricsView."""
        super().__init__(namespace, name, MetricsView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[MetricsView]:
        return []

    @classmethod
    def from_dict(klass, namespace: str, name: str, _dict: ViewDict) -> MetricsView:
        """Get a MetricsView from a dict representation."""
        return klass(namespace, name, [])

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Get this view as LookML."""
        namespace_definitions = MetricsConfigLoader.configs.get_platform_definitions(
            self.namespace
        )
        if namespace_definitions is None:
            return {}

        data_source_name = re.sub("^metrics_", "", self.name)
        data_source_definition = MetricsConfigLoader.configs.get_data_source_definition(
            data_source_name, self.namespace
        )

        if data_source_definition is None:
            return {}

        metric_definitions = [
            f"{MetricsConfigLoader.configs.get_env().from_string(metric.select_expression).render()} AS {metric_slug}"
            for metric_slug, metric in namespace_definitions.metrics.definitions.items()
        ]

        view_defn: Dict[str, Any] = {"name": self.name}
        view_defn["derived_table"] = {
            "sql": f"""
              SELECT
                {",".join(metric_definitions)},
                {data_source_definition.client_id_column or "client_id"} AS client_id,
                {data_source_definition.submission_date_column or "submission_date"} AS submission_date
              FROM (
                {MetricsConfigLoader.configs.get_data_source_sql(data_source_name, self.namespace).format(dataset=self.namespace)}
              )
              GROUP BY
                client_id,
                submission_date
            """
        }
        view_defn["dimensions"] = self.get_dimensions()
        view_defn["measures"] = self.get_measures()

        return view_defn

    def get_dimensions(self) -> List[Dict[str, Any]]:
        namespace_definitions = MetricsConfigLoader.configs.get_platform_definitions(
            self.namespace
        )
        metric_definitions = namespace_definitions.metrics.definitions

        return [
            {
                "name": "submission_date",
                "type": "date",
                "sql": "${TABLE}.submission_date",
                "datatype": "date",
                "convert_tz": "no",
                "label": "Submission Date",
            },
            {
                "name": "client_id",
                "type": "string",
                "sql": "${TABLE}.client_id",
                "label": "Client ID",
                "description": "Unique client identifier",
            },
        ] + [
            {
                "name": metric_slug,
                "label": metric.friendly_name,
                "description": metric.description,
                "type": "number",
                "sql": "${TABLE}." + metric_slug,
            }
            for metric_slug, metric in metric_definitions.items()
        ]

    def get_measures(self) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Get measures."""

        return [
            {
                "name": "clients",
                "type": "count_distinct",
                "sql": f"${{client_id}}",
            }
        ]
