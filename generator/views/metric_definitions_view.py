"""Class to describe a view with metrics from metric-hub."""
from __future__ import annotations

import re
from typing import Any, Dict, Iterator, List, Optional

from generator.metrics_utils import MetricsConfigLoader

from . import lookml_utils
from .view import View, ViewDict


class MetricDefinitionsView(View):
    """A view for metric-hub metrics that come from the same data source."""

    type: str = "metric_definitions_view"

    def __init__(self, namespace: str, name: str, tables: List[Dict[str, str]]):
        """Get an instance of an MetricDefinitionsView."""
        super().__init__(namespace, name, MetricDefinitionsView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[MetricDefinitionsView]:
        """Get Metric Definition Views from db views and app variants."""
        return iter(())

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, definition: ViewDict
    ) -> MetricDefinitionsView:
        """Get a MetricDefinitionsView from a dict representation."""
        return klass(namespace, name, [])

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Get this view as LookML."""
        namespace_definitions = MetricsConfigLoader.configs.get_platform_definitions(
            self.namespace
        )
        if namespace_definitions is None:
            return {}

        # get all metric definitions that depend on the data source represented by this view
        data_source_name = re.sub("^metric_definitions_", "", self.name)
        data_source_definition = MetricsConfigLoader.configs.get_data_source_definition(
            data_source_name, self.namespace
        )

        if data_source_definition is None:
            return {}

        metric_definitions = [
            f"{MetricsConfigLoader.configs.get_env().from_string(metric.select_expression).render()} AS {metric_slug}"
            for metric_slug, metric in namespace_definitions.metrics.definitions.items()
            if metric.select_expression and metric.data_source.name == data_source_name
        ]

        if metric_definitions == []:
            return {}

        # Metric definitions are intended to aggregated by client per date.
        # A derived table is needed to do these aggregations, instead of defining them as measures
        # we want to have them available as dimensions (which don't allow aggregations in their definitions)
        # to allow for custom measures to be later defined in Looker that aggregate these per client metrics.
        base_view_name = f"metric_definitions_{self.namespace}"
        view_defn: Dict[str, Any] = {"name": self.name}
        view_defn["derived_table"] = {
            "sql": f"""
              SELECT
                {",".join(metric_definitions)},
                COALESCE({data_source_definition.client_id_column or "client_id"}, 'NULL') AS client_id,
                {data_source_definition.submission_date_column or "submission_date"} AS submission_date
              FROM
                {
                    MetricsConfigLoader.configs.get_data_source_sql(
                        data_source_name,
                        self.namespace
                    ).format(dataset=self.namespace)
                }
              WHERE {data_source_definition.submission_date_column} BETWEEN
                SAFE_CAST({{% date_start {base_view_name}.date %}} AS DATE) AND
                SAFE_CAST({{% date_end {base_view_name}.date %}} AS DATE)
              GROUP BY
                client_id,
                submission_date
            """
        }
        view_defn["dimensions"] = self.get_dimensions()
        view_defn["dimension_groups"] = self.get_dimension_groups()
        view_defn["measures"] = []

        # Custom filter injected into the derived table SQL to filter on the date partition.
        # All metric definition views reference the date filter from the 'base_view'.
        # This allows us to have a single filter in the explore that gets applied to all the views.
        view_defn["filters"] = [
            {
                "name": "date",
                "type": "date",
                "description": "Date Range",
            }
        ]
        view_defn["sets"] = self._get_sets()

        return {"views": [view_defn]}

    def get_dimensions(
        self, _bq_client=None, _table=None, _v1_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get the set of dimensions for this view based on the metric definitions in metric-hub."""
        namespace_definitions = MetricsConfigLoader.configs.get_platform_definitions(
            self.namespace
        )
        metric_definitions = namespace_definitions.metrics.definitions
        data_source_name = re.sub("^metric_definitions_", "", self.name)

        # Each view has a client_id dimension.
        # For every metric definition view, except the base view, this dimension won't be exposed.
        # So the following logic only gets run for the base view.
        # The client_id column can be overridden in metric-hub, or when joining metric definition
        # view some client_ids might not be available in the base view, but in the view that gets joined.
        # This would result in the selected client_id being set to null (since we always select the client_id)
        # from the base view.
        # The following logic selects the first available client_id from any view that is part of the join.
        joined_client_id_columns = "SAFE_CAST(${TABLE}.client_id AS STRING)"
        for data_source in MetricsConfigLoader.data_sources_of_namespace(
            self.namespace
        ):
            # for any view that is part of the current query, use the first available client_id
            joined_client_id_columns += f"""
                {{% if  metric_definitions_{data_source}._in_query %}}
                , SAFE_CAST(metric_definitions_{data_source}.client_id AS STRING)
                {{% endif %}}
            """

        return [
            {
                "name": "client_id",
                "type": "string",
                "sql": f"COALESCE({joined_client_id_columns})",
                "label": "Client ID",
                "description": "Unique client identifier",
            },
        ] + [  # add a dimension for each metric definition
            {
                "name": metric_slug,
                "label": metric.friendly_name
                or lookml_utils.slug_to_title(metric_slug),
                "description": metric.description or "",
                "type": "number",
                "sql": "${TABLE}." + metric_slug,
            }
            for metric_slug, metric in metric_definitions.items()
            if metric.select_expression and metric.data_source.name == data_source_name
        ]

    def get_dimension_groups(self) -> List[Dict[str, Any]]:
        """Get dimension groups for this view."""
        # Similar to client_id (see above). When joining the views the base view submission_date
        # can be NULL. Use the first submission_date available instead.
        joined_submission_date_columns = "CAST(${TABLE}.submission_date AS TIMESTAMP)"
        for data_source in MetricsConfigLoader.data_sources_of_namespace(
            self.namespace
        ):
            # for any view that is part of the current query, use the first available submission_date
            joined_submission_date_columns += f"""
                {{% if  metric_definitions_{data_source}._in_query %}}
                , CAST(metric_definitions_{data_source}.submission_date AS TIMESTAMP)
                {{% endif %}}
            """

        return [
            {
                "name": "submission",
                "type": "time",
                "sql": f"COALESCE({joined_submission_date_columns})",
                "label": "Submission",
                "timeframes": [
                    "raw",
                    "date",
                    "week",
                    "month",
                    "quarter",
                    "year",
                ],
            }
        ]

    def _get_sets(self) -> List[Dict[str, Any]]:
        """Generate metric sets."""
        # group all the metric dimensions into a set
        dimensions = self.get_dimensions()

        return [
            {
                "name": "metrics",
                "fields": [
                    dimension["name"]
                    for dimension in dimensions
                    if dimension["name"] != "client_id"
                ],
            }
        ]
