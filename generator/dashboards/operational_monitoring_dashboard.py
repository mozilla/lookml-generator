"""Class to describe Operational Monitoring Dashboard."""
from __future__ import annotations

from typing import Any, Dict, List

from ..views import lookml_utils
from .dashboard import Dashboard


class OperationalMonitoringDashboard(Dashboard):
    """An Operational Monitoring dashboard."""

    type: str = "operational_monitoring_dashboard"

    OPMON_DASH_EXCLUDED_FIELDS: List[str] = [
        "branch",
        "probe",
        "histogram__VALUES__key",
        "histogram__VALUES__value",
    ]

    def __init__(
        self,
        title: str,
        name: str,
        layout: str,
        namespace: str,
        defn: List[Dict[str, Any]],
    ):
        """Get an instance of a Operational Monitoring Dashboard."""
        self.dimensions = defn[0].get("dimensions", {})
        self.xaxis = defn[0]["xaxis"]
        self.group_by_dimension = defn[0].get("group_by_dimension", None)

        super().__init__(title, name, layout, namespace, defn)

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, defn: dict
    ) -> OperationalMonitoringDashboard:
        """Get a OperationalMonitoringDashboard from a dict representation."""
        title = defn["title"]
        return klass(title, name, "newspaper", namespace, defn["tables"])

    def _map_series_to_colours(self, branches, explore):
        colours = [
            "#3FE1B0",
            "#0060E0",
            "#9059FF",
            "#B933E1",
            "#FF2A8A",
            "#FF505F",
            "#FF7139",
            "#FFA537",
            "#005E5D",
            "#073072",
            "#7F165B",
            "#A7341F",
        ]
        return {branch: color for branch, color in zip(branches, colours)}

    def to_lookml(self, bq_client):
        """Get this dashboard as LookML."""
        kwargs = {
            "name": self.name,
            "title": self.title,
            "layout": self.layout,
            "elements": [],
            "dimensions": [],
            "group_by_dimension": self.group_by_dimension,
        }

        includes = []
        graph_index = 0
        for table_defn in self.tables:
            if len(kwargs["dimensions"]) == 0:
                kwargs["dimensions"] = [
                    {
                        "name": name,
                        "title": lookml_utils.slug_to_title(name),
                        "default": info["default"],
                        "options": info["options"],
                    }
                    for name, info in self.dimensions.items()
                ]

            explore = table_defn["explore"]
            includes.append(
                f"/looker-hub/{self.namespace}/explores/{explore}.explore.lkml"
            )

            series_colors = self._map_series_to_colours(table_defn["branches"], explore)
            for metric in table_defn.get("probes", []):
                title = lookml_utils.slug_to_title(metric)
                kwargs["elements"].append(
                    {
                        "title": title,
                        "metric": metric,
                        "explore": explore,
                        "series_colors": series_colors,
                        "xaxis": self.xaxis,
                        "row": int(graph_index / 2) * 10,
                        "col": 0 if graph_index % 2 == 0 else 12,
                    }
                )
                graph_index += 1

                if self.group_by_dimension:
                    kwargs["elements"].append(
                        {
                            "title": f"{title} - By {self.group_by_dimension}",
                            "metric": metric,
                            "explore": explore,
                            "series_colors": series_colors,
                            "xaxis": self.xaxis,
                            "row": int(graph_index / 2) * 10,
                            "col": 0 if graph_index % 2 == 0 else 12,
                        }
                    )
                    graph_index += 1

        dash_lookml = lookml_utils.render_template(
            "dashboard.lkml", "dashboards", **kwargs
        )
        return dash_lookml
