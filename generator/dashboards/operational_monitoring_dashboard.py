"""Class to describe Operational Monitoring Dashboard."""
from __future__ import annotations

from typing import Dict, List

from ..constants import OPMON_EXCLUDED_FIELDS
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
    def _slug_to_title(self, slug):
        return slug.replace("_", " ").title()

    def _compute_dimension_data(self, bq_client, table, kwargs):
        all_dimensions = lookml_utils._generate_dimensions(bq_client, table)
        copy_excluded = OPMON_EXCLUDED_FIELDS.copy()
        copy_excluded.update(self.OPMON_DASH_EXCLUDED_FIELDS)

        relevant_dimensions = [
            dimension
            for dimension in all_dimensions
            if dimension["name"] not in copy_excluded
        ]

        for dimension in relevant_dimensions:
            dimension_name = dimension["name"]
            query_job = bq_client.query(
                f"""
                    SELECT DISTINCT {dimension_name}, COUNT(*)
                    FROM {table}
                    GROUP BY 1
                    ORDER BY 2 DESC
                """
            )

            title = self._slug_to_title(dimension_name)
            dimension_options = (
                query_job.result().to_dataframe()[dimension_name].tolist()
            )

            dimension_kwarg = {
                "title": title,
                "name": dimension_name,
            }

            if len(dimension_options) > 0:
                dimension_kwarg.update(
                    {
                        "default": dimension_options[0],
                        "options": dimension_options[:10],
                    }
                )

            kwargs["dimensions"].append(dimension_kwarg)

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, defn: dict
    ) -> OperationalMonitoringDashboard:
        """Get a OperationalMonitoringDashboard from a dict representation."""
        title = klass._slug_to_title(name)
        return klass(title, name, "newspaper", namespace, defn["tables"])

    def to_lookml(self, bq_client):
        """Get this dashboard as LookML."""
        kwargs = {
            "name": self.name,
            "title": self.title,
            "layout": self.layout,
            "elements": [],
            "dimensions": [],
        }

        includes = []
        for table_defn in self.tables:
            if len(kwargs["dimensions"]) == 0:
                self._compute_dimension_data(bq_client, table_defn["table"], kwargs)

            query_job = bq_client.query(
                f"""
                    SELECT DISTINCT probe
                    FROM {table_defn["table"]}
                """
            )
            metrics = query_job.result().to_dataframe()["probe"].tolist()
            explore = table_defn["explore"]
            includes.append(
                f"/looker-hub/{self.namespace}/explores/{explore}.explore.lkml"
            )

            for i, metric in enumerate(metrics):
                title = self._slug_to_title(metric)
                kwargs["elements"].append(
                    {
                        "title": title,
                        "metric": metric,
                        "explore": explore,
                        "row": int(i / 2),
                        "col": 0 if i % 2 == 0 else 12,
                    }
                )

        model_lookml = lookml_utils.render_template(
            "model.lkml", "dashboards", **{"includes": includes}
        )
        dash_lookml = lookml_utils.render_template(
            "dashboard.lkml", "dashboards", **kwargs
        )
        return dash_lookml, model_lookml
