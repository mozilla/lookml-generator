"""Class to describe Operational Monitoring Dashboard."""
from __future__ import annotations

from typing import Dict, List

from .. import operational_monitoring_utils
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
        tables: List[Dict[str, str]],
    ):
        """Get an instance of a Operational Monitoring Dashboard."""
        super().__init__(title, name, layout, namespace, tables)

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, defn: dict
    ) -> OperationalMonitoringDashboard:
        """Get a OperationalMonitoringDashboard from a dict representation."""
        title = lookml_utils.slug_to_title(name)
        return klass(title, name, "newspaper", namespace, defn["tables"])

    def _map_series_to_colours(self, branches, explore):
        colours = ["#ff6a06", "#ffb380", "#ffb380", "blue", "#8cd3ff", "#8cd3ff"]
        series_labels = []
        for branch in branches:
            series_labels += [
                f"{branch} - {explore}.percentile",
                f"{branch} - {explore}.high",
                f"{branch} - {explore}.low",
            ]
        return dict((label, colour) for (label, colour) in zip(series_labels, colours))

    def to_lookml(self, bq_client, data):
        """Get this dashboard as LookML."""
        kwargs = {
            "name": self.name,
            "title": self.title,
            "layout": self.layout,
            "elements": [],
            "dimensions": [],
        }

        table_data = {}
        for view_data in data.get("compute_opmon_dimensions", {}).values():
            table_data.update(view_data)

        includes = []
        graph_index = 0
        for table_defn in self.tables:
            table_name = table_defn["table"]
            if len(kwargs["dimensions"]) == 0:
                kwargs["dimensions"] = table_data[table_name]

            xaxis = operational_monitoring_utils.get_xaxis_val(bq_client, table_name)

            metrics = lookml_utils.get_distinct_vals(bq_client, table_name, "probe")
            explore = table_defn["explore"]
            includes.append(
                f"/looker-hub/{self.namespace}/explores/{explore}.explore.lkml"
            )

            series_colors = self._map_series_to_colours(table_defn["branches"], explore)
            for metric in metrics:
                title = lookml_utils.slug_to_title(metric)
                kwargs["elements"].append(
                    {
                        "title": title,
                        "metric": metric,
                        "explore": explore,
                        "series_colors": series_colors,
                        "xaxis": xaxis,
                        "row": int(graph_index / 2) * 10,
                        "col": 0 if graph_index % 2 == 0 else 12,
                    }
                )
                graph_index += 1

        dash_lookml = lookml_utils.render_template(
            "dashboard.lkml", "dashboards", **kwargs
        )
        return dash_lookml
