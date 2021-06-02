"""Class to describe a Glean Ping View."""
import logging
from collections import Counter
from textwrap import dedent
from typing import Any, Dict, Iterable, List, Optional, Union

import click
from mozilla_schema_generator.glean_ping import GleanPing
from mozilla_schema_generator.probes import GleanProbe

from .ping_view import PingView

DISTRIBUTION_TYPES = {
    "timing_distribution",
    "memory_distribution",
    "custom_distribution",
}


ALLOWED_TYPES = DISTRIBUTION_TYPES | {
    "boolean",
    "counter",
    "labeled_counter",
    "datetime",
    "jwe",
    "quantity",
    "string",
    "rate",
    "timespan",
    "uuid",
}


class GleanPingView(PingView):
    """A view on a ping table for an application using the Glean SDK."""

    type: str = "glean_ping_view"
    allow_glean: bool = True

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Generate LookML for this view.

        The Glean views include a labeled metrics, which need to be joined
        against the view in the explore.
        """
        lookml = super().to_lookml(bq_client, v1_name)

        # iterate over all of the glean metrics and generate views for unnested
        # fields as necessary. Append them to the list of existing view
        # definitions.
        table = next(
            (table for table in self.tables if table.get("channel") == "release"),
            self.tables[0],
        )["table"]
        dimensions = self.get_dimensions(bq_client, table, v1_name)

        client_id_field = self._get_client_id(dimensions, table)

        view_definitions = []
        metrics = self._get_glean_metrics(v1_name)
        for metric in metrics:
            if metric.type == "labeled_counter":
                looker_name = self._to_looker_name(metric)
                view_name = f"{self.name}__{looker_name}"
                suggest_name = f"suggest__{view_name}"
                join_view = {
                    "name": view_name,
                    "label": (
                        "_".join(looker_name.split("__")[1:]).replace("_", " ").title()
                    ),
                    "dimensions": [
                        {
                            "name": "client_id",
                            "type": "string",
                            "sql": f"${{{self.name}.{client_id_field}}}",
                            "primary_key": "yes",
                            "hidden": "yes",
                        },
                        {
                            "name": "key",
                            "type": "string",
                            "sql": "${TABLE}.key",
                            "suggest_explore": suggest_name,
                            "suggest_dimension": f"{suggest_name}.key",
                        },
                        {
                            "name": "value",
                            "type": "number",
                            "sql": "${TABLE}.value",
                            "hidden": "yes",
                        },
                    ],
                    "measures": [
                        {
                            "name": "count",
                            "type": "sum",
                            "sql": "${value}",
                        },
                        {
                            "name": "client_count",
                            "type": "count_distinct",
                            "sql": f"case when ${{value}} > 0 then ${{{self.name}.{client_id_field}}} end",
                        },
                    ],
                }
                suggest_view = {
                    "name": suggest_name,
                    "derived_table": {
                        "sql": dedent(
                            f"""
                            select
                                m.key,
                                count(*) as n
                            from {table} as t,
                            unnest(metrics.{metric.type}.{metric.id.replace(".", "_")}) as m
                            where date(submission_timestamp) > date_sub(current_date, interval 3 day)
                            group by key
                            order by n desc
                            """
                        )
                    },
                    "dimensions": [
                        {"name": "key", "type": "string", "sql": "${TABLE}.key"}
                    ],
                }
                view_definitions += [join_view, suggest_view]
        # deduplicate view definitions, because somehow a few entries make it in
        # twice e.g. metrics__metrics__labeled_counter__media_audio_init_failure
        view_definitions = sorted(
            {v["name"]: v for v in view_definitions}.values(), key=lambda x: x["name"]  # type: ignore
        )

        lookml["views"] += view_definitions

        return lookml

    def _get_links(self, dimension: dict) -> List[Dict[str, str]]:
        """Get a link annotation given a metric name."""
        name = self._get_name(dimension)
        title = name.replace("_", " ").title()
        return [
            {
                "label": (f"Glean Dictionary reference for {title}"),
                "url": (
                    f"https://dictionary.telemetry.mozilla.org"
                    f"/apps/{self.namespace}/metrics/{name}"
                ),
                "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",
            }
        ]

    def _get_name(self, dimension: dict) -> str:
        return dimension["name"].split("__")[-1]

    def _get_metric_type(self, dimension: dict) -> str:
        return dimension["name"].split("__")[1]

    def _is_metric(self, dimension) -> bool:
        return dimension["name"].startswith("metrics__")

    def _get_glean_metrics(self, v1_name: Optional[str]) -> List[GleanProbe]:
        if v1_name is None:
            logging.error(
                f"Error: Missing v1 name for ping {self.name} in namespace {self.namespace}"
            )
            return []

        repo = next((r for r in GleanPing.get_repos() if r["name"] == v1_name))
        glean_app = GleanPing(repo)

        ping_probes = []
        probe_ids = set()
        for probe in glean_app.get_probes():
            if self.name not in probe.definition["send_in_pings"]:
                continue
            if probe.id in probe_ids:
                # Some ids are duplicated, ignore them
                continue

            ping_probes.append(probe)
            probe_ids.add(probe.id)

        return ping_probes

    def _to_looker_name(self, metric: GleanProbe, suffix: str = "") -> str:
        """Convert a glean probe into a looker name."""
        *category, name = metric.id.split(".")
        category = "_".join(category)

        sep = "" if not category else "_"
        label = name
        looker_name = f"metrics__{metric.type}__{category}{sep}{label}"
        if suffix:
            looker_name = f"{looker_name}__{suffix}"
        return looker_name

    def _make_dimension(
        self, metric: GleanProbe, suffix: str, sql_map: Dict[str, Dict[str, str]]
    ) -> Optional[Dict[str, Union[str, List[Dict[str, str]]]]]:
        *category, name = metric.id.split(".")
        category = "_".join(category)

        sep = "" if not category else "_"
        label = name
        looker_name = f"metrics__{metric.type}__{category}{sep}{name}"
        if suffix:
            label = f"{name}_{suffix}"
            looker_name = f"{looker_name}__{suffix}"

        if looker_name not in sql_map:
            return None

        group_label = category.replace("_", " ").title()
        group_item_label = label.replace("_", " ").title()

        if not group_label:
            group_label = "Glean"

        lookml = {
            "name": looker_name,
            "sql": sql_map[looker_name]["sql"],
            "type": sql_map[looker_name]["type"],
            "group_label": group_label,
            "group_item_label": group_item_label,
            "links": [
                {
                    "label": (
                        f"Glean Dictionary reference for {group_label} {group_item_label}"
                    ),
                    "url": (
                        f"https://dictionary.telemetry.mozilla.org"
                        f"/apps/{self.namespace}/metrics/{category}{sep}{name}"
                    ),
                    "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",
                },
            ],
        }

        # remove some elements from the definition if we're handling a labeled
        # counter, as an initial join dimension
        if metric.type == "labeled_counter":
            # this field is not used since labeled counters are maps
            del lookml["type"]
            lookml["hidden"] = "yes"

        if metric.description:
            lookml["description"] = metric.description

        return lookml

    def _get_metric_dimensions(
        self, metric: GleanProbe, sql_map: Dict[str, Dict[str, str]]
    ) -> Iterable[Optional[Dict[str, Union[str, List[Dict[str, str]]]]]]:
        if metric.type == "rate":
            for suffix in ("numerator", "denominator"):
                yield self._make_dimension(metric, suffix, sql_map)
        elif metric.type in DISTRIBUTION_TYPES:
            yield self._make_dimension(metric, "sum", sql_map)
        elif metric.type == "timespan":
            yield self._make_dimension(metric, "value", sql_map)
        elif metric.type in ALLOWED_TYPES:
            yield self._make_dimension(metric, "", sql_map)

    def _get_glean_metric_dimensions(
        self, all_fields: List[dict], v1_name: Optional[str]
    ):
        sql_map = {
            f["name"]: {"sql": f["sql"], "type": f.get("type", "string")}
            for f in all_fields
        }
        metrics = self._get_glean_metrics(v1_name)
        return [
            dimension
            for metric in metrics
            for dimension in self._get_metric_dimensions(metric, sql_map)
            if dimension is not None
        ]

    def _add_link(self, dimension):
        annotations = {}
        if self._is_metric(dimension) and not self._get_metric_type(
            dimension
        ).startswith("labeled"):
            annotations["links"] = self._get_links(dimension)

        return dict(dimension, **annotations)

    def get_dimensions(
        self, bq_client, table, v1_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Get the set of dimensions for this view."""
        all_fields = super().get_dimensions(bq_client, table, v1_name)
        fields = self._get_glean_metric_dimensions(all_fields, v1_name) + [
            self._add_link(d)
            for d in all_fields
            if not d["name"].startswith("metrics__")
        ]
        # later entries will override earlier entries, if there are duplicates
        field_dict = {f["name"]: f for f in fields}
        return list(field_dict.values())

    def get_measures(
        self, dimensions: List[dict], table: str, v1_name: Optional[str]
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Generate measures from a list of dimensions.

        When no dimension-specific measures are found, return a single "count" measure.

        Raise ClickException if dimensions result in duplicate measures.
        """
        measures = super().get_measures(dimensions, table, v1_name)
        client_id_field = self._get_client_id(dimensions, table)

        for dimension in dimensions:
            if (
                self._is_metric(dimension)
                and self._get_metric_type(dimension) == "counter"
            ):
                # handle the counters in the metric ping
                name = self._get_name(dimension)
                dimension_name = dimension["name"]
                measures += [
                    {
                        "name": name,
                        "type": "sum",
                        "sql": f"${{{dimension_name}}}",
                        "links": self._get_links(dimension),
                    },
                    {
                        "name": f"{name}_client_count",
                        "type": "count_distinct",
                        "filters": [{dimension_name: ">0"}],
                        "sql": f"${{{client_id_field}}}",
                        "links": self._get_links(dimension),
                    },
                ]

        # check if there are any duplicate values
        names = [measure["name"] for measure in measures]
        duplicates = [k for k, v in Counter(names).items() if v > 1]
        if duplicates:
            raise click.ClickException(
                f"duplicate measures {duplicates!r} for table {table!r}"
            )

        return measures
