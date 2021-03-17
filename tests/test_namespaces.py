import gzip
import json
import sys
import tarfile
from io import BytesIO
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
            content = dedent(
                f"""
                references:
                  view.sql:
                  - moz-fx-data-shared-prod.{dataset}_derived.baseline_clients_daily_v1
                """
            ).lstrip()
            info = tarfile.TarInfo(
                f"sql/moz-fx-data-shared-prod/{dataset}/"
                "baseline_clients_daily/metadata.yaml"
            )
            info.size = len(content)
            tar.addfile(info, BytesIO(content.encode()))
            content = dedent(
                f"""
                references:
                  view.sql:
                  - moz-fx-data-shared-prod.{dataset}_stable.baseline_v1
                """
            ).lstrip()
            info = tarfile.TarInfo(
                f"sql/moz-fx-data-shared-prod/{dataset}/baseline/metadata.yaml"
            )
            info.size = len(content)
            tar.addfile(info, BytesIO(content.encode()))
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
        sys.stdout.write(result.stdout)
        if result.stderr_bytes is not None:
            sys.stderr.write(result.stderr)
        try:
            assert result.exit_code == 0
        except Exception as e:
            # use exception chaining to expose original traceback
            raise e from result.exception
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
                      is_ping_table: true
                      table: mozdata.glean_app.baseline
                    - channel: beta
                      is_ping_table: true
                      table: mozdata.glean_app_beta.baseline
                    baseline_clients_daily:
                    - channel: release
                      table: mozdata.glean_app.baseline_clients_daily
                """
            ).lstrip()
            == Path("namespaces.yaml").read_text()
        )
