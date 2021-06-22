"""Class to describe an Operational Monitoring View."""

from typing import Any, Dict, List, Optional

from .ping_view import PingView
from .view import ViewDict


class OperationalMonitoringView(PingView):
    """A view on a operational monitoring table."""

    type: str = "operational_monitoring_view"
    percentile_ci_labels = ["percentile", "low", "high"]

    def __init__(self, namespace: str, name: str, tables: List[Dict[str, str]]):
        """Create instance of a OperationalMonitoringView."""
        super().__init__(namespace, name, tables)
        self.dimensions: List[Dict[str, str]] = [
            {
                "name": "build_id",
                "type": "date",
                "sql": "PARSE_DATE('%Y%m%d', CAST(${TABLE}.build_id AS STRING))",
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
    def from_dict(klass, namespace: str, name: str, _dict: ViewDict) -> PingView:
        """Get a OperationalMonitoringView from a dict representation."""
        return klass(namespace, name, _dict["tables"])

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Get this view as LookML."""
        raise NotImplementedError("Only implemented in subclasses")
