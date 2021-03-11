import gzip
import json
import tarfile
import traceback
from pathlib import Path

import pytest
from click.testing import CliRunner

from generator.namespaces import namespaces


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def custom_namespaces(tmp_path):
    dest = tmp_path / "custom-namespaces.yaml"
    dest.write_text(
        "custom:\n"
        "  canonical_app_name: Custom\n"
        "  views:\n"
        "    baseline:\n"
        "    - channel: release\n"
        "      table: mozdata.custom.baseline\n"
    )
    return dest.absolute()


@pytest.fixture
def generated_sql_uri(tmp_path):
    dest = tmp_path / "bigquery_etl.tar.gz"
    with tarfile.open(dest, "w:gz") as tar:
        tar.addfile(
            tarfile.TarInfo(
                name="sql/moz-fx-data-shared-prod/glean_app/baseline/view.sql"
            ),
            b"CREATE OR REPLACE VIEW moz-fx-data-shared-prod.glean_app.baseline AS "
            b"SELECT * FROM moz-fx-data-shared-prod.glean_app_stable.baseline",
        )
    return dest.absolute().as_uri()


@pytest.fixture
def app_listings_uri(tmp_path):
    dest = tmp_path / "app-listings"
    dest.write_bytes(
        gzip.compress(
            json.dumps(
                [
                    {
                        "app_name": "glean-app",
                        "app_channel": "release",
                        "canonical_app_name": "Glean App",
                        "bq_dataset_family": "glean_app",
                    }
                ]
            ).encode()
        )
    )
    return dest.absolute().as_uri()


def test_namespaces(runner, custom_namespaces, generated_sql_uri, app_listings_uri):
    with runner.isolated_filesystem():
        result = runner.invoke(
            namespaces,
            [
                "--custom-namespaces",
                custom_namespaces,
                "--generated-sql-uri",
                generated_sql_uri,
                "--app-listings-uri",
                app_listings_uri,
            ],
        )
        try:
            assert not result.exception
        except Exception:
            traceback.print_tb(result.exc_info[2])
            raise
        assert (
            "custom:\n"
            "  canonical_app_name: Custom\n"
            "  views:\n"
            "    baseline:\n"
            "    - channel: release\n"
            "      table: mozdata.custom.baseline\n"
            "glean-app:\n"
            "  canonical_app_name: Glean App\n"
            "  views:\n"
            "    baseline:\n"
            "    - channel: release\n"
            "      table: mozdata.glean_app.baseline\n"
        ) == Path("namespaces.yaml").read_text()
