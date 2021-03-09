import gzip
import json
import tarfile
import traceback
from pathlib import Path
from textwrap import dedent

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
        dedent(
            """
            custom:
              canonical_app_name: Custom
              views:
                baseline:
                - channel: release
                  table: mozdata.custom.baseline
            """
        ).lstrip()
    )
    return dest.absolute()


@pytest.fixture
def generated_sql_uri(tmp_path):
    dest = tmp_path / "bigquery_etl.tar.gz"
    with tarfile.open(dest, "w:gz") as tar:
        for dataset in ("glean_app", "glean_app_beta"):
            tar.addfile(
                tarfile.TarInfo(
                    name=f"sql/moz-fx-data-shared-prod/{dataset}/baseline/view.sql"
                ),
                dedent(
                    f"""
                    CREATE OR REPLACE VIEW
                      `moz-fx-data-shared-prod`.{dataset}.baseline
                    AS
                    SELECT
                      *
                    FROM
                      `moz-fx-data-shared-prod`.{dataset}_stable.baseline
                    """
                ).lstrip(),
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
                    },
                    {
                        "app_name": "glean-app",
                        "app_channel": "beta",
                        "canonical_app_name": "Glean App",
                        "bq_dataset_family": "glean_app_beta",
                    },
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
            dedent(
                """
                custom:
                  canonical_app_name: Custom
                  views:
                    baseline:
                    - channel: release
                      table: mozdata.custom.baseline
                glean-app:
                  canonical_app_name: Glean App
                  views:
                    baseline:
                    - channel: release
                      table: mozdata.glean_app.baseline
                    - channel: beta
                      table: mozdata.glean_app_beta.baseline
                """
            ).lstrip()
            == Path("namespaces.yaml").read_text()
        )
