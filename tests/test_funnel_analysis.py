from unittest.mock import Mock

import pytest

from generator import lkml_update
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
                "event_type_1": "event_types",
                "event_type_2": "event_types",
            }
        ],
    )


@pytest.fixture()
def funnel_analysis_explore(tmp_path, funnel_analysis_view):
    (tmp_path / "funnel_analysis.view.lkml").write_text(
        lkml_update.dump(funnel_analysis_view.to_lookml(Mock(), None))
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
                    "event_type_1": "event_types",
                    "event_type_2": "event_types",
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
                        "name": "completed_event_1",
                        "type": "yesno",
                        "sql": (
                            "REGEXP_CONTAINS(${TABLE}.events, mozfun.event_analysis.create_funnel_regex(["
                            "${event_type_1.match_string}],"
                            "True))"
                        ),
                    },
                    {
                        "name": "completed_event_2",
                        "type": "yesno",
                        "sql": (
                            "REGEXP_CONTAINS(${TABLE}.events, mozfun.event_analysis.create_funnel_regex(["
                            "${event_type_2.match_string}],"
                            "True))"
                        ),
                    },
                ],
                "measures": [
                    {
                        "name": "count_user_days_event_1",
                        "type": "count",
                        "filters": [
                            {"completed_event_1": "yes"},
                        ],
                    },
                    {
                        "name": "count_user_days_event_2",
                        "type": "count",
                        "filters": [
                            {"completed_event_1": "yes"},
                            {"completed_event_2": "yes"},
                        ],
                    },
                    {
                        "name": "fraction_user_days_event_1",
                        "type": "number",
                        "sql": "SAFE_DIVIDE(${count_user_days_event_1}, ${count_user_days_event_1})",
                    },
                    {
                        "name": "fraction_user_days_event_2",
                        "type": "number",
                        "sql": "SAFE_DIVIDE(${count_user_days_event_2}, ${count_user_days_event_1})",
                    },
                ],
            },
            {
                "name": "event_types",
                "derived_table": {
                    "sql": (
                        "SELECT "
                        "mozfun.event_analysis.aggregate_match_strings( "
                        "ARRAY_AGG( "
                        "mozfun.event_analysis.event_index_to_match_string(index))) AS match_string "
                        "FROM "
                        "`mozdata.glean_app.event_types` "
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
            },
            {
                "name": "event_type_1",
                "extends": ["event_types"],
            },
            {
                "name": "event_type_2",
                "extends": ["event_types"],
            },
        ],
    }
    actual = funnel_analysis_view.to_lookml(Mock(), None)

    print_and_test(expected=expected, actual=actual)


def test_explore_lookml(funnel_analysis_explore):
    expected = {
        "name": "funnel_analysis",
        "view_label": " User-Day Funnels",
        "always_filter": {
            "filters": [
                {"submission_date": "14 days"},
            ]
        },
        "joins": [
            {
                "name": "event_type_1",
                "relationship": "many_to_one",
                "type": "cross",
            },
            {
                "name": "event_type_2",
                "relationship": "many_to_one",
                "type": "cross",
            },
        ],
        "sql_always_where": "${funnel_analysis.submission_date} >= '2010-01-01'",
    }

    actual = funnel_analysis_explore.to_lookml()
    print_and_test(expected=expected, actual=actual)
