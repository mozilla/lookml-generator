"""Class to describe a Funnel Analysis View."""
from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional

from .view import View, ViewDict


class FunnelAnalysisView(View):
    """A view for Client Counting measures."""

    type: str = "funnel_analysis_view"
    num_funnel_steps: int = 4

    def __init__(self, namespace: str, tables: List[Dict[str, str]]):
        """Get an instance of a FunnelAnalysisView."""
        super().__init__(namespace, "funnel_analysis", FunnelAnalysisView.type, tables)

    @classmethod
    def from_db_views(
        klass,
        namespace: str,
        is_glean: bool,
        channels: List[Dict[str, str]],
        db_views: dict,
        num_funnel_steps: int = num_funnel_steps,
    ) -> Iterator[FunnelAnalysisView]:
        """Get Client Count Views from db views and app variants."""
        # We can guarantee there will always be at least one channel,
        # because this comes from the associated _get_glean_repos in
        # namespaces.py
        dataset = next(
            (channel for channel in channels if channel.get("channel") == "release"),
            channels[0],
        )["dataset"]

        necessary_views = {"events_daily", "event_types"}
        actual_views = {}
        for view_id, references in db_views[dataset].items():
            if view_id in necessary_views:
                actual_views[view_id] = f"`mozdata.{dataset}.{view_id}`"

        if len(actual_views) == 2:
            tables = {
                "funnel_analysis": "events_daily_table",
                "event_types": actual_views["event_types"],
            }
            tables.update(
                {
                    f"event_type_{i}": "event_types"
                    for i in range(1, num_funnel_steps + 1)
                }
            )
            yield FunnelAnalysisView(
                namespace,
                [tables],
            )

    @classmethod
    def from_dict(
        klass, namespace: str, name: str, _dict: ViewDict
    ) -> FunnelAnalysisView:
        """Get a FunnalAnalysisView from a dict representation."""
        return FunnelAnalysisView(namespace, _dict["tables"])

    def to_lookml(self, bq_client, v1_name: Optional[str]) -> Dict[str, Any]:
        """Get this view as LookML."""
        return {
            "includes": [f"{self.tables[0]['funnel_analysis']}.view.lkml"],
            "views": self._funnel_analysis_lookml() + self._event_types_lookml(),
        }

    def n_events(self) -> int:
        """Get the number of events allowed in this funnel."""
        return len([k for k in self.tables[0] if k.startswith("event_type_")])

    def _funnel_analysis_lookml(self) -> List[Dict[str, Any]]:
        dimensions = [
            {
                "name": f"completed_event_{n}",
                "type": "yesno",
                "sql": (
                    "REGEXP_CONTAINS(${TABLE}.events, mozfun.event_analysis.create_funnel_regex(["
                    f"${{event_type_{n}.match_string}}],"
                    "True))"
                ),
            }
            for n in range(1, self.n_events() + 1)
        ]
        count_measures: List[Dict[str, Any]] = [
            {
                "name": f"count_user_days_event_{n}",
                "type": "count",
                "filters": [{f"completed_event_{ni}": "yes"} for ni in range(1, n + 1)],
            }
            for n in range(1, self.n_events() + 1)
        ]
        fractional_measures: List[Dict[str, Any]] = [
            {
                "name": f"fraction_user_days_event_{n}",
                "type": "number",
                "sql": f"SAFE_DIVIDE(${{count_user_days_event_{n}}}, ${{count_user_days_event_1}})",
            }
            for n in range(1, self.n_events() + 1)
        ]
        return [
            {
                "name": "funnel_analysis",
                "extends": ["events_daily_table"],
                "dimensions": dimensions,
                "measures": count_measures + fractional_measures,
            }
        ]

    def _event_types_lookml(self) -> List[Dict[str, Any]]:
        events = [
            {
                "name": "event_types",
                "derived_table": {
                    "sql": (
                        "SELECT "
                        "mozfun.event_analysis.aggregate_match_strings( "
                        "ARRAY_AGG( "
                        "mozfun.event_analysis.event_index_to_match_string(index))) AS match_string "
                        "FROM "
                        f"{self.tables[0]['event_types']} "
                        "WHERE "
                        "{% condition message_id %} event_types.category {% endcondition %} "
                        "AND {% condition event_type %} event_types.event {% endcondition %}"
                    )
                },
                "filters": [
                    {
                        "name": "category",
                        "type": "string",
                        "suggest_explore": "event_names",
                        "suggest_dimension": "event_names.category",
                    },
                    {
                        "name": "name",
                        "type": "string",
                        "suggest_explore": "event_names",
                        "suggest_dimension": "event_names.name",
                    },
                ],
                "dimensions": [
                    {
                        "name": "match_string",
                        "hidden": "yes",
                        "sql": "${TABLE}.match_string",
                    }
                ],
            }
        ] + [
            {
                "name": f"event_type_{n}",
                "extends": ["event_types"],
            }
            for n in range(1, self.n_events() + 1)
        ]

        return events
