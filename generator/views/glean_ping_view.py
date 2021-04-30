"""Class to describe a Glean Ping View."""
from collections import Counter
from typing import Any, Dict, List

import click

from .ping_view import PingView


class GleanPingView(PingView):
    """A view on a ping table for an application using the Glean SDK."""

    type: str = "glean_ping_view"
    allow_glean: bool = True

    def __init__(self, name: str, tables: List[Dict[str, str]], **kwargs):
        """Create instance of a GleanPingView."""
        super().__init__(name, tables, **kwargs)

    def _annotate_dimension(self, dimension):
        annotations = {}
        if dimension["name"].startswith("metrics__") and dimension.get(
            "group_item_label"
        ):
            metric_name = dimension["name"].split("__")[-1]
            annotations["link"] = {
                "label": f"Glean Dictionary reference for {dimension['group_item_label']}",
                "url": "https://dictionary.telemetry.mozilla.org/apps/{}/metrics/{}".format(
                    self.name, metric_name
                ),
                "icon_url": "https://dictionary.telemetry.mozilla.org/favicon.png",
            }
        return dict(dimension, **annotations)

    def get_dimensions(self, bq_client, table) -> List[Dict[str, Any]]:
        """Get the set of dimensions for this view."""
        return [
            self._annotate_dimension(d)
            for d in super().get_dimensions(bq_client, table)
        ]

    def get_measures(self, dimensions: List[dict], table: str) -> List[Dict[str, str]]:
        """Generate measures from a list of dimensions.

        When no dimension-specific measures are found, return a single "count" measure.

        Raise ClickException if dimensions result in duplicate measures.
        """
        measures = super().get_measures(dimensions, table)
        client_id_field = self._get_client_id(dimensions, table)

        for dimension in dimensions:
            dimension_name = dimension["name"]
            if "metrics__counter__" in dimension_name:
                # handle the counters in the metric ping
                name = dimension_name.ltrim("metrics__")
                measures += [
                    {
                        "name": name,
                        "type": "sum",
                        "sql": f"${{{dimension_name}}}",
                    },
                    {
                        "name": f"{name}_client_count",
                        "type": "count_distinct",
                        "sql": (
                            f"case when ${{{dimension_name}}} > 0 then "
                            f"${{{client_id_field}}}"
                        ),
                    },
                ]

        # check if there are any duplicate values, and report the first one that
        # shows up
        names = [measure["name"] for measure in measures]
        duplicates = [k for k, v in Counter(names).items() if v > 1]
        if duplicates:
            name = duplicates[0]
            raise click.ClickException(
                f"duplicate measure {name!r} for table {table!r}"
            )

        return measures
