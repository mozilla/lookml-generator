"""Class to describe a view with metrics from metric-hub."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterator, List, Optional

from . import lookml_utils
from .view import View, ViewDict


class MetricsView(View):
    """A view for metric-hub metrics that come from the same data source."""

    type: str = "metrics_view"

    def __init__(self, namespace: str, name: str, tables: List[Dict[str, str]]):
        """Get an instance of an MetricsView."""
        super().__init__(namespace, name, MetricsView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
    ) -> Iterator[MetricsView]:
        pass
    
    @classmethod
    def from_dict(
        klass, namespace: str, name: str, _dict: ViewDict
    ) -> MetricsView:
        """Get a MetricsView from a dict representation."""
        return klass(namespace, name, _dict["tables"])
    
    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Get this view as LookML."""


        return {
            "views": [
                {
                    "name": self.name,
                    "sql_table_name": reference_table,
                    "dimensions": self.get_dimensions(),
                    "measures": self.get_measures(
                        self.dimensions, reference_table, v1_name
                    ),
                }
            ]
        }
    
    def get_dimensions(self, bq_client, table, v1_name: str | None) -> List[Dict[str, Any]]:
        return [
            {"name": "submission_date", "type": "date", "sql"}
        ]
    
    def get_measures(
        self, dimensions: List[dict], table: str, v1_name: Optional[str]
    ) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """Get metric definitions as measures."""
        return [
            {"name": "point", "type": "sum", "sql": "${TABLE}.point"},
            {"name": "upper", "type": "sum", "sql": "${TABLE}.upper"},
            {"name": "lower", "type": "sum", "sql": "${TABLE}.lower"},
        ]