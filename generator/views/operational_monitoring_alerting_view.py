"""Class to describe an Operational Monitoring Alert View."""

from typing import Any, Dict, Optional

from . import lookml_utils
from .operational_monitoring_view import OperationalMonitoringView


class OperationalMonitoringAlertingView(OperationalMonitoringView):
    """A view on a alert operational monitoring table."""

    type: str = "operational_monitoring_alerting_view"

    def to_lookml(self, v1_name: Optional[str], dryrun) -> Dict[str, Any]:
        """Get this view as LookML."""
        if len(self.tables) == 0:
            raise Exception((f"Operational Monitoring view {self.name} has no tables"))

        reference_table = self.tables[0]["table"]
        dimensions = [
            d
            for d in lookml_utils._generate_dimensions(reference_table, dryrun=dryrun)
            if d["name"] != "submission"
        ]

        dimensions.append(
            {
                "name": "submission_date",
                "type": "date",
                "sql": "${TABLE}.submission_date",
                "datatype": "date",
                "convert_tz": "no",
            }
        )

        dimensions.append(
            {
                "name": "build_id_date",
                "type": "date",
                "hidden": "yes",
                "sql": "PARSE_DATE('%Y%m%d', CAST(${TABLE}.build_id AS STRING))",
                "datatype": "date",
                "convert_tz": "no",
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
