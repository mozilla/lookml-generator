import pytest
from google.cloud import bigquery


@pytest.fixture
def client():
    return bigquery.Client()


@pytest.mark.integration
def test_google_connection(client):
    job = client.query("SELECT NULL")
    assert [(None,)] == [tuple(row) for row in job.result()]
