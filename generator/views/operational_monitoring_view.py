"""Class to describe an Operational Monitoring View."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from . import lookml_utils
from .ping_view import PingView
from .view import ViewDict

ALLOWED_DIMENSIONS = {
    "branch",
    "metric",
    "statistic",
    "parameter",
}


class OperationalMonitoringView(PingView):
    """A view on a operational monitoring table."""

    type: str = "operational_monitoring_view"

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
                "datatype": "date",
                "convert_tz": "no",
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
        if len(self.tables) == 0:
            raise Exception((f"Operational Monitoring view {self.name} has no tables"))

        reference_table = self.tables[0]["table"]
        all_dimensions = lookml_utils._generate_dimensions(bq_client, reference_table)

        filtered_dimensions = [
            d
            for d in all_dimensions
            if d["name"] in ALLOWED_DIMENSIONS
            or d["name"] in self.tables[0].get("dimensions", {}).keys()
        ]
        self.dimensions.extend(filtered_dimensions)

        return {
            "views": [
                {
                    "name": self.name,
                    "sql_table_name": reference_table,
                    "dimensions": self.dimensions,
                    "measures": self.get_measures(
                        self.dimensions, reference_table, v1_name
                    ),
                }
            ]
        }

    def get_measures(
        self, dimensions: List[dict], table: str, v1_name: Optional[str]
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Get OpMon measures."""
        return [
            {"name": "point", "type": "sum", "sql": "${TABLE}.point"},
            {"name": "upper", "type": "sum", "sql": "${TABLE}.upper"},
            {"name": "lower", "type": "sum", "sql": "${TABLE}.lower"},
        ]
