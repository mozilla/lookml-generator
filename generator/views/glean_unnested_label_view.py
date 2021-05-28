"""Views representing labeled unnested views."""
from typing import Any, Dict, List, Optional, Union

from mozilla_schema_generator.probes import GleanProbe

from .glean_ping_view import GleanPingView
from .ping_view import PingView


class GleanUnnestedLabelView(GleanPingView):
    """A view on an unnested field in a Glean ping view."""

    type: str = "glean_unnested_label_view"

    def __init__(
        self,
        namespace: str,
        name: str,
        tables: List[Dict[str, str]],
        metric: GleanProbe,
    ):
        """Create instance of a PingView."""
        # name should actually be a function of name + probe name
        self.base_name = name
        self.metric = metric
        metric_looker_name = self._to_looker_name(metric)
        super(PingView, self).__init__(
            namespace, f"{name}__{metric_looker_name}", self.__class__.type, tables
        )

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Get lookml definition."""
        table = next(
            (table for table in self.tables if table.get("channel") == "release"),
            self.tables[0],
        )["table"]
        dimensions = self.get_dimensions(bq_client, table, v1_name)
        return {
            "views": [
                {
                    "dimensions": dimensions,
                    "measures": self.get_measures(dimensions, table, v1_name),
                }
            ]
        }

    def get_dimensions(
        self, bq_client, table: Optional[str], v1_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Get dimensions."""
        return [
            {"name": "key", "type": "string", "sql": "${TABLE}.key ;;"},
            {
                "name": "value",
                "type": "number",
                "sql": "${TABLE}.value ;;",
                "hidden": "yes",
            },
        ]

    def get_measures(
        self, dimensions: List[dict], table, v1_name: Optional[str]
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Get measures."""
        # TODO: this might be broken
        client_id_field = self._get_client_id(dimensions, table)
        return [
            {"name": "count", "type": "sum", "sql": "${value} ;;"},
            {
                "name": "client_count",
                "type": "count_distinct",
                "sql": f"case when ${{value}} > 0 then ${{{self.base_name}.{client_id_field}}}",
            },
        ]
