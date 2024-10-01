import lkml
import pytest

from generator.explores import EventsExplore
from generator.views import EventsView

from .utils import MockDryRun, MockDryRunContext, print_and_test


class MockDryRunEvents(MockDryRun):
    """Mock dryrun.DryRun."""

    def get_table_schema(self):
        """Mock dryrun.DryRun.get_table_schema"""

        return [
            {
                "name": "client_info",
                "type": "RECORD",
                "fields": [{"name": "client_id", "type": "STRING"}],
            },
            {"name": "event_id", "type": "STRING"},
        ]


@pytest.fixture()
def events_view():
    return EventsView(
        "glean_app",
        "events",
        [
            {
                "events_table_view": "events_unnested_table",
                "base_table": "mozdata.glean_app.events_unnested",
            },
        ],
    )


@pytest.fixture(params=["submission", "timestamp"])
def time_partitioning_group(request):
    return request.param


@pytest.fixture()
def events_explore(events_view, tmp_path, time_partitioning_group):
    (tmp_path / "events_unnested_table.view.lkml").write_text(
        lkml.dump(
            {
                "views": [
                    {
                        "name": "events_unnested_table",
                        "dimensions": [
                            {
                                "name": "client_info__client_count",
                                "type": "string",
                            },
                        ],
                        "dimension_groups": [
                            {
                                "name": time_partitioning_group,
                                "tags": (
                                    ["time_partitioning_field"]
                                    if time_partitioning_group != "submission"
                                    else []
                                ),
                                "type": "time",
                                "timeframes": [
                                    "raw",
                                    "time",
                                    "date",
                                ],
                            }
                        ],
                    }
                ]
            }
        )
    )
    (tmp_path / "events.view.lkml").write_text(
        lkml.dump(
            {
                "views": [
                    {
                        "name": "events",
                        "measures": [
                            {
                                "name": "event_count",
                                "type": "count",
                            }
                        ],
                    }
                ]
            }
        )
    )
    return EventsExplore(
        "events",
        {"base_view": "events", "extended_view": "events_unnested_table"},
        tmp_path,
    )


def test_view_from_db_views(events_view):
    db_views = {
        "glean_app": {
            "events": [["mozdata", "glean_app", "events"]],
            "events_unnested": [["mozdata", "glean_app", "events_unnested"]],
        }
    }

    channels = [
        {"channel": "release", "dataset": "glean_app"},
        {"channel": "beta", "dataset": "glean_app_beta"},
    ]

    actual = next(EventsView.from_db_views("glean_app", True, channels, db_views))

    assert actual == events_view


def test_view_from_dict(events_view):
    actual = EventsView.from_dict(
        "glean_app",
        "events",
        {
            "type": "events_view",
            "tables": [
                {
                    "events_table_view": "events_unnested_table",
                    "base_table": "mozdata.glean_app.events_unnested",
                }
            ],
        },
    )

    assert actual == events_view


def test_explore_from_views(events_view, events_explore):
    views = [events_view]
    actual = next(EventsExplore.from_views(views))

    assert actual == events_explore


def test_explore_from_dict(events_explore, tmp_path):
    actual = EventsExplore.from_dict(
        "events",
        {"views": {"base_view": "events", "extended_view": "events_unnested_table"}},
        tmp_path,
    )
    assert actual == events_explore


def test_view_lookml(events_view):
    expected = {
        "includes": ["events_unnested_table.view.lkml"],
        "views": [
            {
                "name": "events",
                "extends": ["events_unnested_table"],
                "measures": [
                    {
                        "name": "event_count",
                        "description": ("The number of times the event(s) occurred."),
                        "type": "count",
                    },
                    {
                        "name": "client_count",
                        "description": (
                            "The number of clients that completed the event(s)."
                        ),
                        "type": "count_distinct",
                        "sql": "${client_info__client_id}",
                    },
                ],
                "dimensions": [
                    {
                        "name": "event_id",
                        "primary_key": "yes",
                    },
                ],
            },
        ],
    }

    mock_dryrun = MockDryRunContext(MockDryRunEvents, False)

    actual = events_view.to_lookml(None, dryrun=mock_dryrun)
    print_and_test(expected=expected, actual=actual)


def test_explore_lookml(time_partitioning_group, events_explore):
    date_dimension = f"{time_partitioning_group}_date"
    expected = [
        {
            "name": "event_counts",
            "view_name": "events",
            "description": "Event counts over time.",
            "always_filter": {
                "filters": [
                    {date_dimension: "28 days"},
                ]
            },
            "sql_always_where": f"${{events.{date_dimension}}} >= '2010-01-01'",
            "queries": [
                {
                    "description": "Event counts from all events over the past two weeks.",
                    "dimensions": [date_dimension],
                    "measures": ["event_count"],
                    "filters": [
                        {date_dimension: "14 days"},
                    ],
                    "name": "all_event_counts",
                },
            ],
            "joins": [],
        },
    ]

    actual = events_explore.to_lookml(None, None)
    print_and_test(expected=expected, actual=actual)
