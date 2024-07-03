from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import bigquery
from google.oauth2.id_token import fetch_id_token
from urllib.request import Request, urlopen
import json
from functools import cached_property
from enum import Enum
from typing import Optional

import sys

DRY_RUN_URL = (
    "https://us-central1-moz-fx-data-shared-prod.cloudfunctions.net/bigquery-etl-dryrun"
)


def is_authenticated():
    """Check if the user is authenticated to GCP."""
    try:
        bigquery.Client(project="")
    except DefaultCredentialsError:
        return False
    return True


class Errors(Enum):
    """DryRun errors that require special handling."""

    READ_ONLY = 1
    DATE_FILTER_NEEDED = 2
    DATE_FILTER_NEEDED_AND_SYNTAX = 3


class DryRun:
    """Dry run SQL."""

    def __init__(
        self,
        sql=None,
        use_cloud_function=True,
        client=None,
        project=None,
        dataset=None,
        table=None,
        dry_run_url=DRY_RUN_URL,
    ):
        self.sql = sql
        self.use_cloud_function = use_cloud_function
        self.client = client if use_cloud_function or client else bigquery.Client()
        self.project = project
        self.dataset = dataset
        self.table = table
        self.dry_run_url = dry_run_url

        if not is_authenticated():
            print(
                "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
                "and check that the project is set correctly."
            )
            sys.exit(1)

    @cached_property
    def dry_run_result(self):
        try:
            if self.use_cloud_function:
                auth_req = GoogleAuthRequest()
                creds, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                creds.refresh(auth_req)
                if hasattr(creds, "id_token"):
                    # Get token from default credentials for the current environment created via Cloud SDK run
                    id_token = creds.id_token
                else:
                    # If the environment variable GOOGLE_APPLICATION_CREDENTIALS is set to service account JSON file,
                    # then ID token is acquired using this service account credentials.
                    id_token = fetch_id_token(auth_req, self.dry_run_url)

                json_data = {
                    "query": self.sql,
                    "project": self.project,
                    "dataset": self.dataset,
                    "table": self.table,
                }

                r = urlopen(
                    Request(
                        self.dry_run_url,
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {id_token}",
                        },
                        data=json.dumps(json_data).encode("utf8"),
                        method="POST",
                    )
                )
                return json.load(r)
            else:
                if self.project:
                    self.client.project = self.project

                job_config = bigquery.QueryJobConfig(
                    dry_run=True,
                    use_query_cache=False,
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "submission_date", "DATE", "2019-01-01"
                        )
                    ],
                )
                job = self.client.query(self.sql, job_config=job_config)
                try:
                    dataset_labels = self.client.get_dataset(job.default_dataset).labels
                except Exception as e:
                    # Most users do not have bigquery.datasets.get permission in
                    # moz-fx-data-shared-prod
                    # This should not prevent the dry run from running since the dataset
                    # labels are usually not required
                    if "Permission bigquery.datasets.get denied on dataset" in str(e):
                        dataset_labels = []
                    else:
                        raise e

                return {
                    "valid": True,
                    "referencedTables": [
                        ref.to_api_repr() for ref in job.referenced_tables
                    ],
                    "schema": (
                        job._properties.get("statistics", {})
                        .get("query", {})
                        .get("schema", {})
                    ),
                    "datasetLabels": dataset_labels,
                }
        except Exception as e:
            print(f"ERROR {e}")
            return None

    def get_schema(self):
        """Return the query schema by dry running the SQL file."""
        if not self.is_valid():
            raise Exception(f"Error when dry running SQL")

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "schema" in self.dry_run_result
        ):
            return self.dry_run_result["schema"]

        return {}

    def get_table_schema(self):
        """Return the schema of the provided table."""
        if not self.is_valid():
            raise Exception(f"Error when dry running SQL")

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "tableSchema" in self.dry_run_result
        ):
            return self.dry_run_result["tableSchema"]

        return {}

    def is_valid(self):
        """Dry run the provided SQL file and check if valid."""
        if self.dry_run_result is None:
            return False

        if self.dry_run_result["valid"]:
            print(f"OK")
        elif self.get_error() == Errors.READ_ONLY:
            # We want the dryrun service to only have read permissions, so
            # we expect CREATE VIEW and CREATE TABLE to throw specific
            # exceptions.
            print(f"OK")
        elif self.get_error() == Errors.DATE_FILTER_NEEDED:
            # With strip_dml flag, some queries require a partition filter
            # (submission_date, submission_timestamp, etc.) to run
            # We mark these requests as valid and add a date filter
            # in get_referenced_table()
            print(f"OK but DATE FILTER NEEDED")
        else:
            print(f"ERROR\n", self.dry_run_result["errors"])
            return False

        return True

    def get_error(self) -> Optional[Errors]:
        """Get specific errors for edge case handling."""
        errors = self.errors()
        if len(errors) != 1:
            return None

        error = errors[0]
        if error and error.get("code") in [400, 403]:
            error_message = error.get("message", "")
            if (
                "does not have bigquery.tables.create permission for dataset"
                in error_message
                or "Permission bigquery.tables.create denied" in error_message
                or "Permission bigquery.datasets.update denied" in error_message
            ):
                return Errors.READ_ONLY
            if "without a filter over column(s)" in error_message:
                return Errors.DATE_FILTER_NEEDED
            if (
                "Syntax error: Expected end of input but got keyword WHERE"
                in error_message
            ):
                return Errors.DATE_FILTER_NEEDED_AND_SYNTAX
        return None
