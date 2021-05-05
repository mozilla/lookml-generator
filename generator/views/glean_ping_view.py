"""Class to describe a Glean Ping View."""
import logging
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple, Union

import click
from mozilla_schema_generator.glean_ping import GleanPing

from .ping_view import PingView


class GleanPingView(PingView):
    """A view on a ping table for an application using the Glean SDK."""

    type: str = "glean_ping_view"
    allow_glean: bool = True

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

    def _get_metric_names(self, v1_name: Optional[str]) -> Dict[str, Tuple[str, str]]:
        if v1_name is None:
            return {}

        repo = next((r for r in GleanPing.get_repos() if r["name"] == v1_name))
        glean_app = GleanPing(repo)
        metrics = glean_app.get_probes()

        mapping = {}
        for metric in metrics:
            logging.info(f"Parsing Glean metric {metric.id} for app {self.namespace}")
            *category, name = metric.id.split(".")
            category = "_".join(category)
            looker_name = f"metrics__{metric.type}__{category}_{name}"
            mapping[looker_name] = (category, name)

        return mapping

    def _annotate_dimension(self, dimension, metric_names: Dict[str, Tuple[str, str]]):
        annotations = {}
        if self._is_metric(dimension) and not self._get_metric_type(
            dimension
        ).startswith("labeled"):
            annotations["links"] = self._get_links(dimension)

        if metric_names.get(dimension["name"]) is not None:
            category, name = metric_names[dimension["name"]]
            dimension["group_label"] = category.replace("_", " ").title()
            dimension["group_item_label"] = name.replace("_", " ").title()

        return dict(dimension, **annotations)

    def get_dimensions(
        self, bq_client, table, v1_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Get the set of dimensions for this view."""
        metric_names = self._get_metric_names(v1_name)
        return [
            self._annotate_dimension(d, metric_names)
            for d in super().get_dimensions(bq_client, table, v1_name)
        ]

    def get_measures(
        self, dimensions: List[dict], table: str
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Generate measures from a list of dimensions.

        When no dimension-specific measures are found, return a single "count" measure.

        Raise ClickException if dimensions result in duplicate measures.
        """
        measures = super().get_measures(dimensions, table)
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
                        "sql": (
                            f"case when ${{{dimension_name}}} > 0 then "
                            f"${{{client_id_field}}}"
                        ),
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
