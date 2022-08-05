"""Class to describe a Funnel Analysis View.

We create a single View file and a single Explore file.

The View file has many Looker views defined within it:
    funnel_analysis: Based on events_daily, has the `events` string and user dimensions (e.g. country)
    event_names: The names of events. Used for suggestions.
    event_N: For each possible funnel step, a single view. This is used to define what that funnel step is.

The Explore's job is to take this generated file an link all those event_N's to the funnel_analysis.
We join them via cross join, because event_N views only have 1 row and 1 column - the match_string
to use for a regex_match on the `events` string in funnel_analysis.

For example, say we filter event_1 on `event`: `WHERE event in ("session-start, "session-end")`
Then we join that with funnel_analysis: `FROM funnel_analysis CROSS JOIN event_1`
That lets us find out whether the user completed those funnel steps:
    `SELECT REGEXP_CONTAINS(funnel_analysis.events, event_1.match_string) AS completed_step_1`

The `funnel_analysis` view has some nice dimensions to hide these details from the end user,
e.g. `completed_funnel_step_N`. We can then count those users across dimensions.
"""
from __future__ import annotations

from textwrap import dedent
from typing import Any, Dict, Iterator, List, Optional

from .view import View, ViewDict

DEFAULT_NUM_FUNNEL_STEPS: int = 4


class FunnelAnalysisView(View):
    """A view for doing Funnel Analysis."""

    type: str = "funnel_analysis_view"

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
        num_funnel_steps: int = DEFAULT_NUM_FUNNEL_STEPS,
    ) -> Iterator[FunnelAnalysisView]:
        """Get Client Count Views from db views and app variants.

        We only create a FunnelAnalysisView if we have the two necessary db tables:
            - events_daily
            - event_types
        """
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
            # Only create an instance if we have the two necessary tables
            tables = {
                "funnel_analysis": "events_daily_table",
                "event_types": actual_views["event_types"],
            }
            tables.update(
                {f"step_{i}": "event_types" for i in range(1, num_funnel_steps + 1)}
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
        return len([k for k in self.tables[0] if k.startswith("step_")])

    def _funnel_analysis_lookml(self) -> List[Dict[str, Any]]:
        dimensions = [
            {
                "name": f"completed_step_{n}",
                "type": "yesno",
                "description": f"Whether the user completed step {n} on the associated day.",
                "sql": dedent(
                    f"""
                    REGEXP_CONTAINS(
                        ${{TABLE}}.events, mozfun.event_analysis.create_funnel_regex(
                            [{", ".join([
                                f'${{step_{ni}.match_string}}' for ni in range(1, n + 1)
                            ])}],
                            True
                        )
                    )
                    """
                ),
            }
            for n in range(1, self.n_events() + 1)
        ]

        count_measures: List[Dict[str, Any]] = [
            {
                "name": f"count_completed_step_{n}",
                "description": (
                    f"The number of times that step {n} was completed. "
                    "Grouping by day makes this a count of users who completed "
                    f"step {n} on each day."
                ),
                "type": "count",
                "filters": [{f"completed_step_{ni}": "yes"} for ni in range(1, n + 1)],
            }
            for n in range(1, self.n_events() + 1)
        ]

        fractional_measures: List[Dict[str, Any]] = [
            {
                "name": f"fraction_completed_step_{n}",
                "description": f"Of the user-days that completed Step 1, the fraction that completed step {n}.",
                "type": "number",
                "value_format": "0.00%",
                "sql": f"SAFE_DIVIDE(${{count_completed_step_{n}}}, ${{count_completed_step_1}})",
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
        events = (
            [
                {
                    "name": "event_types",
                    "derived_table": {
                        "sql": dedent(
                            f"""
                            SELECT
                                mozfun.event_analysis.aggregate_match_strings(
                                    ARRAY_AGG(
                                        CONCAT(
                                            COALESCE(
                                                mozfun.event_analysis.escape_metachars(property_value.value), '')
                                            ),
                                            mozfun.event_analysis.event_index_to_match_string(et.index)
                                        )
                                    )
                                ) AS match_string
                            FROM
                                {self.tables[0]['event_types']} as et
                                LEFT JOIN UNNEST(COALESCE(event_properties, [])) AS properties
                                LEFT JOIN UNNEST(properties.value) AS property_value
                            WHERE
                                {{% condition category %}} category {{% endcondition %}}
                                AND {{% condition event %}} event {{% endcondition %}}
                                AND {{% condition property_name %}} properties.key {{% endcondition %}}
                                AND {{% condition property_value %}} property_value.key {{% endcondition %}}
                            """
                        ),
                    },
                    "filters": [
                        {
                            "name": "category",
                            "description": "The event category, as defined in metrics.yaml.",
                            "type": "string",
                            "suggest_explore": "event_names",
                            "suggest_dimension": "event_names.category",
                        },
                        {
                            "name": "event",
                            "description": "The event name.",
                            "type": "string",
                            "suggest_explore": "event_names",
                            "suggest_dimension": "event_names.event",
                        },
                        {
                            "name": "property_name",
                            "description": "The event property name.",
                            "type": "string",
                            "suggest_explore": "event_names",
                            "suggest_dimension": "event_names.property_name",
                        },
                        {
                            "name": "property_value",
                            "description": "The event property value.",
                            "type": "string",
                            "suggest_explore": "event_names",
                            "suggest_dimension": "event_names.property_value",
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
            ]
            + [
                {
                    "name": f"step_{n}",
                    "extends": ["event_types"],
                }
                for n in range(1, self.n_events() + 1)
            ]
            + [
                {
                    "name": "event_names",
                    "derived_table": {
                        "sql": (
                            "SELECT category, "
                            "  event, "
                            "  property.key AS property_name, "
                            "  property_value.key AS property_value, "
                            "  property_value.index as property_index "
                            f"FROM {self.tables[0]['event_types']} "
                            "LEFT JOIN UNNEST(event_properties) AS property "
                            "LEFT JOIN UNNEST(property.value) AS property_value "
                        )
                    },
                    "dimensions": [
                        {
                            "name": "category",
                            "type": "string",
                            "sql": "${TABLE}.category",
                        },
                        {
                            "name": "event",
                            "type": "string",
                            "sql": "${TABLE}.event",
                        },
                        {
                            "name": "property_name",
                            "type": "string",
                            "sql": "${TABLE}.property_name",
                        },
                        {
                            "name": "property_value",
                            "type": "string",
                            "sql": "${TABLE}.property_value",
                        },
                    ],
                }
            ]
        )

        return events
