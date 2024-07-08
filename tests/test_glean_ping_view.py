from unittest.mock import Mock, patch

from mozilla_schema_generator.probes import GleanProbe

from generator.views import GleanPingView


class MockDryRun:
    """Mock dryrun.DryRun."""

    def __init__(
        self, sql=None, project=None, dataset=None, table=None, use_cloud_function=False
    ):
        self.sql = sql
        self.project = project
        self.dataset = dataset
        self.table = table
        self.use_cloud_function = use_cloud_function

    def get_table_schema(self):
        """Mock dryrun.DryRun.get_table_schema"""
        table_id = f"{self.project}.{self.dataset}.{self.table}"

        if table_id == "mozdata.glean_app.dash_name":
            return [
                {
                    "name": "metrics",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "string",
                            "type": "RECORD",
                            "fields": [{"name": "fun_string_metric", "type": "STRING"}],
                        },
                        {
                            "name": "url2",
                            "type": "RECORD",
                            "fields": [{"name": "fun_url_metric", "type": "STRING"}],
                        },
                        {
                            "name": "datetime",
                            "type": "RECORD",
                            "fields": [
                                {"name": "fun_datetime_metric", "type": "TIMESTAMP"}
                            ],
                        },
                        {
                            "name": "labeled_counter",
                            "type": "RECORD",
                            "fields": [
                                {
                                    "name": "fun_counter_metric",
                                    "type": "STRING",
                                    "mode": "REPEATED",
                                    "fields": [
                                        {"name": "key", "type": "STRING"},
                                        {"name": "value", "type": "INT64"},
                                    ],
                                }
                            ],
                        },
                    ],
                }
            ]

        raise ValueError(f"Table not found: {table_id}")


@patch("generator.views.glean_ping_view.GleanPing")
@patch("generator.views.lookml_utils.DryRun", MockDryRun)
@patch("generator.views.ping_view.DryRun", MockDryRun)
@patch("generator.views.glean_ping_view.DryRun", MockDryRun)
def test_kebab_case(mock_glean_ping):
    """
    Tests that we handle metrics from kebab-case pings
    """
    mock_glean_ping.get_repos.return_value = [{"name": "glean-app"}]
    glean_app = Mock()
    glean_app.get_probes.return_value = [
        GleanProbe(
            "fun.string_metric",
            {
                "type": "string",
                "history": [
                    {
                        "send_in_pings": ["dash-name"],
                        "dates": {
                            "first": "2020-01-01 00:00:00",
                            "last": "2020-01-02 00:00:00",
                        },
                    }
                ],
                "name": "string_metric",
            },
        ),
    ]
    mock_glean_ping.return_value = glean_app
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml("glean-app", False)
    assert len(lookml["views"]) == 1
    assert len(lookml["views"][0]["dimensions"]) == 1
    assert (
        lookml["views"][0]["dimensions"][0]["name"]
        == "metrics__string__fun_string_metric"
    )


@patch("generator.views.glean_ping_view.GleanPing")
@patch("generator.views.lookml_utils.DryRun", MockDryRun)
@patch("generator.views.ping_view.DryRun", MockDryRun)
@patch("generator.views.glean_ping_view.DryRun", MockDryRun)
def test_url_metric(mock_glean_ping):
    """
    Tests that we handle URL metrics
    """
    mock_glean_ping.get_repos.return_value = [{"name": "glean-app"}]
    glean_app = Mock()
    glean_app.get_probes.return_value = [
        GleanProbe(
            "fun.url_metric",
            {
                "type": "url",
                "history": [
                    {
                        "send_in_pings": ["dash-name"],
                        "dates": {
                            "first": "2020-01-01 00:00:00",
                            "last": "2020-01-02 00:00:00",
                        },
                    }
                ],
                "name": "url_metric",
            },
        ),
    ]
    mock_glean_ping.return_value = glean_app
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml("glean-app", False)
    assert len(lookml["views"]) == 1
    assert len(lookml["views"][0]["dimensions"]) == 1
    assert (
        lookml["views"][0]["dimensions"][0]["name"] == "metrics__url2__fun_url_metric"
    )


@patch("generator.views.glean_ping_view.GleanPing")
@patch("generator.views.lookml_utils.DryRun", MockDryRun)
@patch("generator.views.ping_view.DryRun", MockDryRun)
@patch("generator.views.glean_ping_view.DryRun", MockDryRun)
def test_datetime_metric(mock_glean_ping):
    """
    Tests that we handle datetime metrics
    """
    mock_glean_ping.get_repos.return_value = [{"name": "glean-app"}]
    glean_app = Mock()
    glean_app.get_probes.return_value = [
        GleanProbe(
            "fun.datetime_metric",
            {
                "type": "datetime",
                "history": [
                    {
                        "send_in_pings": ["dash-name"],
                        "dates": {
                            "first": "2020-01-01 00:00:00",
                            "last": "2020-01-02 00:00:00",
                        },
                    }
                ],
                "name": "datetime_metric",
            },
        ),
    ]
    mock_glean_ping.return_value = glean_app
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml("glean-app", False)
    assert len(lookml["views"]) == 1
    assert len(lookml["views"][0]["dimension_groups"]) == 1
    assert (
        lookml["views"][0]["dimension_groups"][0]["name"]
        == "metrics__datetime__fun_datetime_metric"
    )
    assert "timeframes" in lookml["views"][0]["dimension_groups"][0]
    assert "group_label" not in lookml["views"][0]["dimension_groups"][0]
    assert "group_item_label" not in lookml["views"][0]["dimension_groups"][0]
    assert "links" not in lookml["views"][0]["dimension_groups"][0]


@patch("generator.views.glean_ping_view.GleanPing")
@patch("generator.views.lookml_utils.DryRun", MockDryRun)
@patch("generator.views.ping_view.DryRun", MockDryRun)
@patch("generator.views.glean_ping_view.DryRun", MockDryRun)
def test_undeployed_probe(mock_glean_ping):
    """
    Tests that we handle metrics not yet deployed to bigquery
    """
    mock_glean_ping.get_repos.return_value = [{"name": "glean-app"}]
    glean_app = Mock()
    glean_app.get_probes.return_value = [
        GleanProbe(
            f"fun.{name}",
            {
                "type": "labeled_counter",
                "history": [
                    {
                        "send_in_pings": ["dash-name"],
                        "dates": {
                            "first": "2020-01-01 00:00:00",
                            "last": "2020-01-02 00:00:00",
                        },
                    }
                ],
                "name": name,
            },
        )
        # "counter_metric2" represents a probe not present in the table schema
        for name in ["counter_metric", "counter_metric2"]
    ]
    mock_glean_ping.return_value = glean_app
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml("glean-app", False)
    # In addition to the table view, each labeled counter adds a join view and a suggest
    # view. Expect 3 views, because 1 for the table view, 2 added for fun.counter_metric
    # because it's in the table schema, and 0 added for fun.counter_metric2 because it's
    # not in the table schema.
    assert len(lookml["views"]) == 2
