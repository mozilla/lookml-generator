from unittest.mock import Mock, patch

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from mozilla_schema_generator.probes import GleanProbe

from generator.views import GleanPingView


class MockClient:
    """Mock bigquery.Client."""

    def get_table(self, table_ref):
        """Mock bigquery.Client.get_table."""

        if table_ref == "mozdata.glean_app.dash_name":
            return bigquery.Table(
                table_ref,
                schema=[
                    SchemaField(
                        "metrics",
                        "RECORD",
                        fields=[
                            SchemaField(
                                "string",
                                "RECORD",
                                fields=[SchemaField("fun_string_metric", "STRING")],
                            )
                        ],
                    ),
                ],
            )

        raise ValueError(f"Table not found: {table_ref}")


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
    mock_bq_client = MockClient()
    view = GleanPingView(
        "glean_app",
        "dash_name",
        [{"channel": "release", "table": "mozdata.glean_app.dash_name"}],
    )
    lookml = view.to_lookml(mock_bq_client, "glean-app")
    assert "metrics.string.fun_string_metric" in str(lookml)
