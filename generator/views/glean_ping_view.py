"""Class to describe a Glean Ping View."""
from typing import Any, Dict, List

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
