"""Class to describe an Operational Monitoring View."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .ping_view import PingView
from .view import ViewDict


class OperationalMonitoringView(PingView):
    """A view on a operational monitoring table."""

    type: str = "operational_monitoring_view"
    percentile_ci_labels = ["percentile", "low", "high"]

    def __init__(self, namespace: str, name: str, tables: List[Dict[str, Any]]):
        """Create instance of a OperationalMonitoringView."""
        super().__init__(namespace, name, tables)
        xaxis = "build_id"
        if "xaxis" in tables[0] and len(tables) > 0:
            xaxis = tables[0]["xaxis"]

        xaxis_to_sql_mapping = {
            "build_id": f"PARSE_DATE('%Y%m%d', CAST(${{TABLE}}.{xaxis} AS STRING))",
            "submission_date": f"${{TABLE}}.{xaxis}",
        }
        self.dimensions: List[Dict[str, str]] = [
            {
                "name": xaxis,
                "type": "date",
                "sql": xaxis_to_sql_mapping[xaxis],
            }
        ]
        self.parameters: List[Dict[str, str]] = [
            {
                "name": "percentile_conf",
                "type": "number",
                "label": "Percentile",
                "default_value": "50.0",
            }
        ]

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, _dict: ViewDict
    ) -> OperationalMonitoringView:
        """Get a OperationalMonitoringView from a dict representation."""
        return klass(namespace, name, _dict["tables"])

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Get this view as LookML."""
        raise NotImplementedError("Only implemented in subclasses")
