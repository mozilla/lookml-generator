"""Class to describe an Operational Monitoring Alert View."""

from typing import Any, Dict, Optional

from . import lookml_utils
from .operational_monitoring_view import OperationalMonitoringView


class OperationalMonitoringAlertingView(OperationalMonitoringView):
    """A view on a alert operational monitoring table."""

    type: str = "operational_monitoring_alerting_view"

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Get this view as LookML."""
        if len(self.tables) == 0:
            raise Exception((f"Operational Monitoring view {self.name} has no tables"))

        reference_table = self.tables[0]["table"]
        dimensions = [
            d
            for d in lookml_utils._generate_dimensions(bq_client, reference_table)
            if d["name"] != "submission"
        ]

        dimensions.append(
            {
                "name": "submission_date",
                "type": "date",
                "sql": "${TABLE}.submission_date",
            }
        )

        return {
            "views": [
                {
                    "name": self.name,
                    "sql_table_name": f"`{reference_table}`",
                    "dimensions": dimensions,
                    "measures": [
                        {"name": "errors", "type": "number", "sql": "COUNT(*)"}
                    ],
                }
            ]
        }
