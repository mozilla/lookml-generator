"""Class to describe an Operational Monitoring View."""
from __future__ import annotations

from textwrap import dedent
from typing import Any, Dict, List, Optional

from . import lookml_utils
from .ping_view import PingView
from .view import ViewDict

ALLOWED_DIMENSIONS = {
    "branch",
    "probe",
    "value__VALUES__key",
    "value__VALUES__value",
}


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

    def _percentile_measure(self, percentile_ci_label) -> Dict[str, str]:
        return {
            "name": percentile_ci_label,
            "type": "number",
            "sql": dedent(
                f"""
                `moz-fx-data-shared-prod`.udf_js.jackknife_percentile_ci(
                    {{% parameter percentile_conf %}},
                    STRUCT(
                        mozfun.hist.merge(
                          ARRAY_AGG(
                            ${{TABLE}}.value IGNORE NULLS
                          )
                        ).values AS values
                    )
                ).{percentile_ci_label}
            """
            ),
        }

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
                    "parameters": self.parameters,
                    "measures": [
                        self._percentile_measure(label)
                        for label in self.percentile_ci_labels
                    ],
                }
            ]
        }
