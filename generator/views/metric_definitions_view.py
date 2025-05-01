"""Class to describe a view with metrics from metric-hub."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterator, List, Optional, Union

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
        return klass(namespace, name, definition.get("tables", []))

    def to_lookml(self, v1_name: Optional[str], dryrun) -> Dict[str, Any]:
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

        # todo: hide deprecated metrics?
        metric_definitions = [
            f"""{
                MetricsConfigLoader.configs.get_env().from_string(metric.select_expression).render()
            } AS {metric_slug},\n"""
            for metric_slug, metric in namespace_definitions.metrics.definitions.items()
            if metric.select_expression
            and metric.data_source.name == data_source_name
            and metric.type != "histogram"
        ]

        if metric_definitions == []:
            return {}

        # Metric definitions are intended to aggregated by client per date.
        # A derived table is needed to do these aggregations, instead of defining them as measures
        # we want to have them available as dimensions (which don't allow aggregations in their definitions)
        # to allow for custom measures to be later defined in Looker that aggregate these per client metrics.
        view_defn: Dict[str, Any] = {"name": self.name}

        ignore_base_fields = [
            "client_id",
            "submission_date",
            "submission",
            "first_run",
        ] + [
            metric_slug
            for metric_slug, metric in namespace_definitions.metrics.definitions.items()
            if metric.select_expression
            and metric.data_source.name == data_source_name
            and metric.type != "histogram"
        ]

        base_view_dimensions = {}
        joined_data_sources = []

        # check if the metric data source has joins
        # joined data sources are generally used for creating the "Base Fields"
        if data_source_definition.joins:
            # determine the dimensions selected by the joined data sources
            for joined_data_source_slug, join in data_source_definition.joins.items():
                joined_data_source = (
                    MetricsConfigLoader.configs.get_data_source_definition(
                        joined_data_source_slug, self.namespace
                    )
                )

                if joined_data_source.columns_as_dimensions:
                    joined_data_sources.append(joined_data_source)

                    date_filter = None
                    if joined_data_source.submission_date_column != "NULL":
                        date_filter = (
                            None
                            if joined_data_source.submission_date_column is None
                            or joined_data_source.submission_date_column == "NULL"
                            else f"{joined_data_source.submission_date_column} = '2023-01-01'"
                        )

                    # create Looker dimensions by doing a dryrun
                    query = MetricsConfigLoader.configs.get_data_source_sql(
                        joined_data_source_slug,
                        self.namespace,
                        where=date_filter,
                    ).format(dataset=self.namespace)

                    base_view_dimensions[joined_data_source_slug] = (
                        lookml_utils._generate_dimensions_from_query(
                            query, dryrun=dryrun
                        )
                    )

        if (
            data_source_definition.client_id_column == "NULL"
            and not base_view_dimensions
        ) or data_source_definition.columns_as_dimensions:
            # if the metrics data source doesn't have any joins then use the dimensions
            # of the data source itself as base fields
            date_filter = None
            if data_source_definition.submission_date_column != "NULL":
                date_filter = (
                    "submission_date = '2023-01-01'"
                    if data_source_definition.submission_date_column is None
                    else f"{data_source_definition.submission_date_column} = '2023-01-01'"
                )

            query = MetricsConfigLoader.configs.get_data_source_sql(
                data_source_definition.name,
                self.namespace,
                where=date_filter,
                ignore_joins=True,
            ).format(dataset=self.namespace)

            base_view_dimensions[data_source_definition.name] = (
                lookml_utils._generate_dimensions_from_query(query, dryrun)
            )

        # to prevent duplicate dimensions, especially when working with time dimensions
        # where names are modified potentially causing naming collisions
        seen_dimensions = set()
        # prepare base field data for query
        base_view_fields = []
        for data_source, dimensions in base_view_dimensions.items():
            for dimension in dimensions:
                if (
                    dimension["name"] not in ignore_base_fields
                    and dimension["name"] not in seen_dimensions
                    and "hidden" not in dimension
                ):
                    sql = (
                        f"{data_source}.{dimension['name'].replace('__', '.')} AS"
                        + f" {data_source}_{dimension['name']},\n"
                    )
                    # date/time/timestamp suffixes are removed when generating lookml dimensions, however we
                    # need the original field name for the derived view SQL
                    if dimension["type"] == "time" and not dimension["sql"].endswith(
                        dimension["name"]
                    ):
                        suffix = dimension["sql"].split(
                            dimension["name"].replace("__", ".")
                        )[-1]
                        sql = (
                            f"{data_source}.{(dimension['name']+suffix).replace('__', '.')} AS"
                            + f" {data_source}_{dimension['name']},\n"
                        )

                    base_view_fields.append(
                        {
                            "name": f"{data_source}_{dimension['name']}",
                            "select_sql": f"{data_source}_{dimension['name']},\n",
                            "sql": sql,
                        }
                    )
                    seen_dimensions.add(dimension["name"])

        client_id_field = (
            "NULL"
            if data_source_definition.client_id_column == "NULL"
            else f'{data_source_definition.client_id_column or "client_id"}'
        )

        # filters for date ranges
        where_sql = " AND ".join(
            [
                f"""
                    {data_source.name}.{data_source.submission_date_column or "submission_date"}
                    BETWEEN
                    COALESCE(
                        SAFE_CAST(
                            {{% date_start submission_date %}} AS DATE
                        ), CURRENT_DATE()) AND
                    COALESCE(
                        SAFE_CAST(
                            {{% date_end submission_date %}} AS DATE
                        ), CURRENT_DATE())
                """
                for data_source in [data_source_definition] + joined_data_sources
                if data_source.submission_date_column != "NULL"
            ]
        )

        # filte on sample_id if such a field exists
        for field in base_view_fields:
            if field["name"].endswith("_sample_id"):
                where_sql += f"""
                    AND
                        {field['name'].split('_sample_id')[0]}.sample_id < {{% parameter sampling %}}
                """
                break

        view_defn["derived_table"] = {
            "sql": f"""
            SELECT
                {"".join(metric_definitions)}
                {"".join([field['select_sql'] for field in base_view_fields])}
                {client_id_field} AS client_id,
                {{% if aggregate_metrics_by._parameter_value == 'day' %}}
                {data_source_definition.submission_date_column or "submission_date"} AS analysis_basis
                {{% elsif aggregate_metrics_by._parameter_value == 'week'  %}}
                (FORMAT_DATE(
                    '%F',
                    DATE_TRUNC({data_source_definition.submission_date_column or "submission_date"},
                    WEEK(MONDAY)))
                ) AS analysis_basis
                {{% elsif aggregate_metrics_by._parameter_value == 'month'  %}}
                (FORMAT_DATE(
                    '%Y-%m',
                    {data_source_definition.submission_date_column or "submission_date"})
                ) AS analysis_basis
                {{% elsif aggregate_metrics_by._parameter_value == 'quarter'  %}}
                (FORMAT_DATE(
                    '%Y-%m',
                    DATE_TRUNC({data_source_definition.submission_date_column or "submission_date"},
                    QUARTER))
                ) AS analysis_basis
                {{% elsif aggregate_metrics_by._parameter_value == 'year'  %}}
                (EXTRACT(
                    YEAR FROM {data_source_definition.submission_date_column or "submission_date"})
                ) AS analysis_basis
                {{% else %}}
                NULL as analysis_basis
                {{% endif %}}
            FROM
                (
                    SELECT
                        {data_source_name}.*,
                        {"".join([field['sql'] for field in base_view_fields])}
                    FROM
                    {
                        MetricsConfigLoader.configs.get_data_source_sql(
                            data_source_name,
                            self.namespace,
                            select_fields=False
                        ).format(dataset=self.namespace)
                    }
                    WHERE {where_sql}
                )
            GROUP BY
                {"".join([field['select_sql'] for field in base_view_fields])}
                client_id,
                analysis_basis
            """
        }

        view_defn["dimensions"] = self.get_dimensions()
        view_defn["dimension_groups"] = self.get_dimension_groups()

        # add the Looker dimensions
        for data_source, dimensions in base_view_dimensions.items():
            for dimension in dimensions:
                if dimension["name"] not in ignore_base_fields:
                    dimension["sql"] = (
                        "${TABLE}." + f"{data_source}_{dimension['name']}"
                    )
                    dimension["group_label"] = "Base Fields"
                    if not lookml_utils._is_dimension_group(dimension):
                        view_defn["dimensions"].append(dimension)
                    else:
                        view_defn["dimension_groups"].append(dimension)
                    # avoid duplicate dimensions
                    ignore_base_fields.append(dimension["name"])

        view_defn["measures"] = self.get_measures(
            view_defn["dimensions"],
        )
        view_defn["sets"] = self._get_sets()
        view_defn["parameters"] = self._get_parameters(view_defn["dimensions"])

        return {"views": [view_defn]}

    def get_dimensions(
        self,
        _table=None,
        _v1_name: Optional[str] = None,
        _dryrun=None,
    ) -> List[Dict[str, Any]]:
        """Get the set of dimensions for this view based on the metric definitions in metric-hub."""
        namespace_definitions = MetricsConfigLoader.configs.get_platform_definitions(
            self.namespace
        )
        metric_definitions = namespace_definitions.metrics.definitions
        data_source_name = re.sub("^metric_definitions_", "", self.name)

        return [
            {
                "name": "client_id",
                "type": "string",
                "sql": "SAFE_CAST(${TABLE}.client_id AS STRING)",
                "label": "Client ID",
                "primary_key": "yes",
                "group_label": "Base Fields",
                "description": "Unique client identifier",
            },
        ] + [  # add a dimension for each metric definition
            {
                "name": metric_slug,
                "group_label": "Metrics",
                "label": metric.friendly_name
                or lookml_utils.slug_to_title(metric_slug),
                "description": metric.description or "",
                "type": "number",
                "sql": "${TABLE}." + metric_slug,
            }
            for metric_slug, metric in metric_definitions.items()
            if metric.select_expression
            and metric.data_source.name == data_source_name
            and metric.type != "histogram"
        ]

    def get_dimension_groups(self) -> List[Dict[str, Any]]:
        """Get dimension groups for this view."""
        return [
            {
                "name": "submission",
                "type": "time",
                "group_label": "Base Fields",
                "sql": "CAST(${TABLE}.analysis_basis AS TIMESTAMP)",
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
        measures = self.get_measures(dimensions)

        return [
            {
                "name": "metrics",
                "fields": [
                    dimension["name"]
                    for dimension in dimensions
                    if dimension["name"] != "client_id"
                ]
                + [measure["name"] for measure in measures],
            }
        ]

    def _get_parameters(self, dimensions: List[dict]):
        hide_sampling = "yes"

        for dim in dimensions:
            if dim["name"] == "sample_id":
                hide_sampling = "no"
                break

        return [
            {
                "name": "aggregate_metrics_by",
                "label": "Aggregate Client Metrics Per",
                "type": "unquoted",
                "default_value": "day",
                "allowed_values": [
                    {"label": "Per Day", "value": "day"},
                    {"label": "Per Week", "value": "week"},
                    {"label": "Per Month", "value": "month"},
                    {"label": "Per Quarter", "value": "quarter"},
                    {"label": "Per Year", "value": "year"},
                    {"label": "Overall", "value": "overall"},
                ],
            },
            {
                "name": "sampling",
                "label": "Sample of source data in %",
                "type": "unquoted",
                "default_value": "100",
                "hidden": hide_sampling,
            },
        ]

    def get_measures(
        self, dimensions: List[dict]
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Get statistics as measures."""
        measures = []
        sampling = "1"

        for dim in dimensions:
            if dim["name"] == "sample_id":
                sampling = "100 / {% parameter sampling %}"
                break

        for dimension in dimensions:
            metric = MetricsConfigLoader.configs.get_metric_definition(
                dimension["name"], self.namespace
            )
            if metric and metric.statistics:
                for statistic_slug, statistic_conf in metric.statistics.items():
                    dimension_label = dimension.get("label") or dimension.get("name")
                    if statistic_slug in [
                        "average",
                        "max",
                        "min",
                        "median",
                    ]:
                        measures.append(
                            {
                                "name": f"{dimension['name']}_{statistic_slug}",
                                "type": statistic_slug,
                                "sql": "${TABLE}." + dimension["name"],
                                "label": f"{dimension_label} {statistic_slug.title()}",
                                "group_label": "Statistics",
                                "description": f"{statistic_slug.title()} of {dimension_label}",
                            }
                        )
                    elif statistic_slug == "sum":
                        measures.append(
                            {
                                "name": f"{dimension['name']}_{statistic_slug}",
                                "type": "sum",
                                "sql": "${TABLE}." + dimension["name"] + "*" + sampling,
                                "label": f"{dimension_label} Sum",
                                "group_label": "Statistics",
                                "description": f"Sum of {dimension_label}",
                            }
                        )
                    elif statistic_slug == "client_count":
                        measures.append(
                            {
                                "name": (
                                    f"{dimension['name']}_{statistic_slug}_sampled"
                                    if sampling
                                    else f"{dimension['name']}_{statistic_slug}"
                                ),
                                "type": "count_distinct",
                                "label": f"{dimension_label} Client Count",
                                "group_label": "Statistics",
                                "sql": "IF(${TABLE}."
                                + f"{dimension['name']} > 0, "
                                + "${TABLE}.client_id, SAFE_CAST(NULL AS STRING))",
                                "description": f"Number of clients with {dimension_label}",
                                "hidden": "yes" if sampling else "no",
                            }
                        )

                        if sampling:
                            measures.append(
                                {
                                    "name": f"{dimension['name']}_{statistic_slug}",
                                    "type": "number",
                                    "label": f"{dimension_label} Client Count",
                                    "group_label": "Statistics",
                                    "sql": "${"
                                    + f"{dimension['name']}_{statistic_slug}_sampled"
                                    + "} *"
                                    + sampling,
                                    "description": f"Number of clients with {dimension_label}",
                                }
                            )
                    elif statistic_slug == "dau_proportion":
                        if "numerator" in statistic_conf:
                            [numerator, numerator_stat] = statistic_conf[
                                "numerator"
                            ].split(".")
                            measures.append(
                                {
                                    "name": "DAU_sampled" if sampling else "DAU",
                                    "type": "count_distinct",
                                    "label": "DAU",
                                    "group_label": "Statistics",
                                    "sql": "${TABLE}.client_id",
                                    "hidden": "yes",
                                }
                            )

                            if sampling:
                                measures.append(
                                    {
                                        "name": "DAU",
                                        "type": "number",
                                        "label": "DAU",
                                        "group_label": "Statistics",
                                        "sql": "${DAU_sampled} *" + sampling,
                                        "hidden": "yes",
                                    }
                                )

                            measures.append(
                                {
                                    "name": f"{dimension['name']}_{statistic_slug}",
                                    "type": "number",
                                    "label": f"{dimension_label} DAU Proportion",
                                    "sql": "SAFE_DIVIDE(${"
                                    + f"{numerator}_{numerator_stat}"
                                    + "}, ${DAU})",
                                    "group_label": "Statistics",
                                    "description": f"Proportion of daily active users with {dimension['name']}",
                                }
                            )
                    elif statistic_slug == "ratio":
                        if (
                            "numerator" in statistic_conf
                            and "denominator" in statistic_conf
                        ):
                            [numerator, numerator_stat] = statistic_conf[
                                "numerator"
                            ].split(".")
                            [denominator, denominator_stat] = statistic_conf[
                                "denominator"
                            ].split(".")

                            measures.append(
                                {
                                    "name": f"{dimension['name']}_{statistic_slug}",
                                    "type": "number",
                                    "label": f"{dimension_label} Ratio",
                                    "sql": "SAFE_DIVIDE(${"
                                    + f"{numerator}_{numerator_stat}"
                                    + "}, ${"
                                    + f"{denominator}_{denominator_stat}"
                                    + "})",
                                    "group_label": "Statistics",
                                    "description": f""""
                                        Ratio between {statistic_conf['numerator']} and
                                        {statistic_conf['denominator']}""",
                                }
                            )
                    elif statistic_slug == "rolling_average":
                        aggregation = statistic_conf.get("aggregation", "sum")
                        if "window_sizes" in statistic_conf:
                            for window_size in statistic_conf["window_sizes"]:
                                measures.append(
                                    {
                                        "name": f"{dimension['name']}_{window_size}_day_{statistic_slug}",
                                        "type": "number",
                                        "label": f"{dimension_label} {window_size} Day Rolling Average",
                                        "sql": f"""
                                            AVG({aggregation}(${{TABLE}}.{dimension["name"]} * {sampling})) OVER (
                                                ROWS {window_size} PRECEDING
                                        )""",
                                        "group_label": "Statistics",
                                        "description": f"{window_size} day rolling average of {dimension_label}",
                                    }
                                )

        return measures
