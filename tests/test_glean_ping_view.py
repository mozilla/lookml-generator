from unittest.mock import Mock, patch

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from mozilla_schema_generator.probes import GleanProbe

from generator.views import GleanPingView


class MockDryRun:
    """Mock dryrun.DryRun."""
    def __init__(
        self,
        sql=None,
        project=None,
        dataset=None,
        table=None,
    ):
        self.sql = sql
        self.project = project
        self.dataset = dataset
        self.table = table

    def get_table_schema(self):
        """Mock dryrun.DryRun.get_table_schema"""
        table_id = f"{self.project}.{self.dataset}.{self.table}"

        if table_id == "mozdata.glean_app.dash_name":
            return bigquery.Table(
                table_id,
                schema=[
                    SchemaField(
                        "metrics",
                        "RECORD",
                        fields=[
                            SchemaField(
                                "string",
                                "RECORD",
                                fields=[SchemaField("fun_string_metric", "STRING")],
                            ),
                            SchemaField(
                                "url2",
                                "RECORD",
                                fields=[SchemaField("fun_url_metric", "STRING")],
                            ),
                            SchemaField(
                                "datetime",
                                "RECORD",
                                fields=[
                                    SchemaField("fun_datetime_metric", "TIMESTAMP")
                                ],
                            ),
                            SchemaField(
                                "labeled_counter",
                                "RECORD",
                                fields=[
                                    SchemaField(
                                        "fun_counter_metric",
                                        "STRING",
                                        mode="REPEATED",
                                        fields=[
                                            SchemaField("key", "STRING"),
                                            SchemaField("value", "INT64"),
                                        ],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            )

        raise ValueError(f"Table not found: {table_id}")


@patch("generator.views.glean_ping_view.GleanPing")
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
    mock_bq_client = MockDryRun()
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml("glean-app")
    assert len(lookml["views"]) == 1
    assert len(lookml["views"][0]["dimensions"]) == 1
    assert (
        lookml["views"][0]["dimensions"][0]["name"]
        == "metrics__string__fun_string_metric"
    )


@patch("generator.views.glean_ping_view.GleanPing")
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
    mock_bq_client = MockDryRun()
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml(mock_bq_client, "glean-app")
    assert len(lookml["views"]) == 1
    assert len(lookml["views"][0]["dimensions"]) == 1
    assert (
        lookml["views"][0]["dimensions"][0]["name"] == "metrics__url2__fun_url_metric"
    )


@patch("generator.views.glean_ping_view.GleanPing")
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
    mock_bq_client = MockClient()
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml(mock_bq_client, "glean-app")
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
    mock_bq_client = MockClient()
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml(mock_bq_client, "glean-app")
    # In addition to the table view, each labeled counter adds a join view and a suggest
    # view. Expect 3 views, because 1 for the table view, 2 added for fun.counter_metric
    # because it's in the table schema, and 0 added for fun.counter_metric2 because it's
    # not in the table schema.
    assert len(lookml["views"]) == 2
