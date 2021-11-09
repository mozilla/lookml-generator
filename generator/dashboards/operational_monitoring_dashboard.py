"""Class to describe Operational Monitoring Dashboard."""
from __future__ import annotations

from typing import Dict, List

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
        """Get an instance of a FunnelAnalysisView."""
        super().__init__(title, name, layout, namespace, tables)

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, defn: dict
    ) -> OperationalMonitoringDashboard:
        """Get a OperationalMonitoringDashboard from a dict representation."""
        title = lookml_utils.slug_to_title(name)
        return klass(title, name, "newspaper", namespace, defn["tables"])

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
            if len(kwargs["dimensions"]) == 0:
                kwargs["dimensions"] = table_data[table_defn["table"]]

            metrics = lookml_utils.get_distinct_vals(
                bq_client, table_defn["table"], "probe"
            )
            explore = table_defn["explore"]
            includes.append(
                f"/looker-hub/{self.namespace}/explores/{explore}.explore.lkml"
            )

            for metric in metrics:
                title = lookml_utils.slug_to_title(metric)
                kwargs["elements"].append(
                    {
                        "title": title,
                        "metric": metric,
                        "explore": explore,
                        "row": int(graph_index / 2) * 10,
                        "col": 0 if graph_index % 2 == 0 else 12,
                    }
                )
                graph_index += 1

        model_lookml = lookml_utils.render_template(
            "model.lkml", "dashboards", **{"includes": includes}
        )
        dash_lookml = lookml_utils.render_template(
            "dashboard.lkml", "dashboards", **kwargs
        )
        return dash_lookml, model_lookml
