from textwrap import dedent
from unittest.mock import Mock

import lkml
import pytest

from generator.explores import FunnelAnalysisExplore
from generator.views import FunnelAnalysisView

from .utils import print_and_test


@pytest.fixture()
def funnel_analysis_view():
    return FunnelAnalysisView(
        "glean_app",
        [
            {
                "funnel_analysis": "events_daily_table",
                "event_types": "`mozdata.glean_app.event_types`",
                "step_1": "event_types",
                "step_2": "event_types",
            }
        ],
    )


@pytest.fixture()
def funnel_analysis_explore(tmp_path, funnel_analysis_view):
    (tmp_path / "funnel_analysis.view.lkml").write_text(
        lkml.dump(funnel_analysis_view.to_lookml(Mock(), None))
    )
    return FunnelAnalysisExplore(
        "funnel_analysis",
        {"base_view": "funnel_analysis"},
        tmp_path,
    )


def test_view_from_db_views(funnel_analysis_view):
    db_views = {
        "glean_app": {
            "events_daily": [
                ["moz-fx-data-shared-prod", "glean_app_derived", "events_daily_v1"]
            ],
            "event_types": [
                ["moz-fx-data-shared-prod", "glean_app_derived", "event_types_v1"]
            ],
        }
    }
    channels = [
        {"channel": "release", "dataset": "glean_app"},
        {"channel": "beta", "dataset": "glean_app_beta"},
    ]

    actual = next(
        FunnelAnalysisView.from_db_views("glean_app", True, channels, db_views, 2)
    )
    assert actual == funnel_analysis_view


def test_view_from_dict(funnel_analysis_view):
    actual = FunnelAnalysisView.from_dict(
        "glean_app",
        "funnel_analysis",
        {
            "type": "funnel_analysis_view",
            "tables": [
                {
                    "funnel_analysis": "events_daily_table",
                    "event_types": "`mozdata.glean_app.event_types`",
                    "step_1": "event_types",
                    "step_2": "event_types",
                }
            ],
        },
    )

    assert actual == funnel_analysis_view


def test_explore_from_views(funnel_analysis_view):
    expected = FunnelAnalysisExplore(
        "funnel_analysis", {"base_view": "funnel_analysis"}
    )
    views = [funnel_analysis_view]
    actual = next(FunnelAnalysisExplore.from_views(views))

    assert actual == expected


def test_view_lookml(funnel_analysis_view):
    expected = {
        "includes": ["events_daily_table.view.lkml"],
        "views": [
            {
                "name": "funnel_analysis",
                "extends": ["events_daily_table"],
                "dimensions": [
                    {
                        "name": "completed_step_1",
                        "description": "Whether the user completed step 1 on the associated day.",
                        "type": "yesno",
                        "sql": dedent(
                            """
                            REGEXP_CONTAINS(
                                ${TABLE}.events, mozfun.event_analysis.create_funnel_regex(
                                    [${step_1.match_string}],
                                    True
                                )
                            )
                            """
                        ),
                    },
                    {
                        "name": "completed_step_2",
                        "description": "Whether the user completed step 2 on the associated day.",
                        "type": "yesno",
                        "sql": dedent(
                            """
                            REGEXP_CONTAINS(
                                ${TABLE}.events, mozfun.event_analysis.create_funnel_regex(
                                    [${step_1.match_string}, ${step_2.match_string}],
                                    True
                                )
                            )
                            """
                        ),
                    },
                ],
                "measures": [
                    {
                        "name": "count_completed_step_1",
                        "description": (
                            "The number of times that step 1 was completed. "
                            "Grouping by day makes this a count of users who completed "
                            "step 1 on each day."
                        ),
                        "type": "count",
                        "filters": [
                            {"completed_step_1": "yes"},
                        ],
                    },
                    {
                        "name": "count_completed_step_2",
                        "description": (
                            "The number of times that step 2 was completed. "
                            "Grouping by day makes this a count of users who completed "
                            "step 2 on each day."
                        ),
                        "type": "count",
                        "filters": [
                            {"completed_step_1": "yes"},
                            {"completed_step_2": "yes"},
                        ],
                    },
                    {
                        "name": "fraction_completed_step_1",
                        "description": "Of the user-days that completed Step 1, the fraction that completed step 1.",
                        "type": "number",
                        "value_format": "0.00%",
                        "sql": "SAFE_DIVIDE(${count_completed_step_1}, ${count_completed_step_1})",
                    },
                    {
                        "name": "fraction_completed_step_2",
                        "description": "Of the user-days that completed Step 1, the fraction that completed step 2.",
                        "type": "number",
                        "value_format": "0.00%",
                        "sql": "SAFE_DIVIDE(${count_completed_step_2}, ${count_completed_step_1})",
                    },
                ],
            },
            {
                "name": "event_types",
                "derived_table": {
                    "sql": dedent(
                        """
                        SELECT
                          mozfun.event_analysis.aggregate_match_strings(
                            ARRAY_AGG(
                              DISTINCT CONCAT(
                                {% if _filters['property_value'] -%}
                                  mozfun.event_analysis.event_property_value_to_match_string(
                                    properties.index,
                                    property_value.value
                                  ),
                                {% elif _filters['property_name'] -%}
                                  mozfun.event_analysis.event_property_to_match_string(properties.index),
                                {% endif -%}
                                mozfun.event_analysis.event_index_to_match_string(et.index)
                              )
                              IGNORE NULLS
                            )
                          ) AS match_string
                        FROM
                          `mozdata.glean_app.event_types` AS et
                        LEFT JOIN
                          UNNEST(COALESCE(event_properties, [])) AS properties
                        LEFT JOIN
                          UNNEST(properties.value) AS property_value
                        WHERE
                          {% condition category %} category {% endcondition %}
                          AND {% condition event %} event {% endcondition %}
                          AND {% condition property_name %} properties.key {% endcondition %}
                          AND {% condition property_value %} property_value.key {% endcondition %}
                        """
                    )
                },
                "filters": [
                    {
                        "name": "category",
                        "type": "string",
                        "description": "The event category, as defined in metrics.yaml.",
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
            },
            {
                "name": "step_1",
                "extends": ["event_types"],
            },
            {
                "name": "step_2",
                "extends": ["event_types"],
            },
            {
                "name": "event_names",
                "derived_table": {
                    "sql": (
                        "SELECT category, "
                        "  event, "
                        "  property.key AS property_name, "
                        "  property_value.key AS property_value, "
                        "  property_value.index as property_index "
                        "FROM `mozdata.glean_app.event_types` "
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
            },
        ],
    }
    actual = funnel_analysis_view.to_lookml(Mock(), None)

    print_and_test(expected=expected, actual=actual)


def test_explore_lookml(funnel_analysis_explore):
    expected = [
        {
            "name": "funnel_analysis",
            "description": "Count funnel completion over time. Funnels are limited to a single day.",
            "view_label": " User-Day Funnels",
            "always_filter": {
                "filters": [
                    {"submission_date": "14 days"},
                ]
            },
            "joins": [
                {
                    "name": "step_1",
                    "relationship": "many_to_one",
                    "type": "cross",
                },
                {
                    "name": "step_2",
                    "relationship": "many_to_one",
                    "type": "cross",
                },
            ],
            "sql_always_where": "${funnel_analysis.submission_date} >= '2010-01-01'",
        },
        {"name": "event_names", "hidden": "yes"},
    ]

    actual = funnel_analysis_explore.to_lookml(None, None)
    print_and_test(expected=expected, actual=actual)
