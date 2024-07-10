"""Dry Run method to get BigQuery metadata."""

import json
from enum import Enum
from functools import cached_property
from typing import Optional
from urllib.request import Request, urlopen

import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import bigquery
from google.oauth2.id_token import fetch_id_token

DRY_RUN_URL = (
    "https://us-central1-moz-fx-data-shared-prod.cloudfunctions.net/bigquery-etl-dryrun"
)


def id_token():
    """Get token to authenticate against Cloud Function."""
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
        id_token = fetch_id_token(auth_req, DRY_RUN_URL)
    return id_token


class DryRunError(Exception):
    """Exception raised on dry run errors."""

    def __init__(self, message, error, use_cloud_function):
        """Initialize DryRunError."""
        super().__init__(message)
        self.error = error
        self.use_cloud_function = use_cloud_function


class Errors(Enum):
    """DryRun errors that require special handling."""

    READ_ONLY = 1
    DATE_FILTER_NEEDED = 2
    DATE_FILTER_NEEDED_AND_SYNTAX = 3
    PERMISSION_DENIED = 4


class DryRun:
    """Dry run SQL."""

    def __init__(
        self,
        client=None,
        use_cloud_function=False,
        id_token=None,
        sql=None,
        project="moz-fx-data-shared-prod",
        dataset=None,
        table=None,
        dry_run_url=DRY_RUN_URL,
    ):
        """Initialize dry run instance."""
        self.sql = sql
        self.use_cloud_function = use_cloud_function
        self.client = client
        self.project = project
        self.dataset = dataset
        self.table = table
        self.dry_run_url = dry_run_url
        self.id_token = id_token

    @cached_property
    def dry_run_result(self):
        """Return the dry run result."""
        try:
            if self.use_cloud_function:
                json_data = {
                    "query": self.sql or "SELECT 1",
                    "project": self.project,
                    "dataset": self.dataset or "telemetry",
                }

                if self.table:
                    json_data["table"] = self.table

                r = urlopen(
                    Request(
                        self.dry_run_url,
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {self.id_token}",
                        },
                        data=json.dumps(json_data).encode("utf8"),
                        method="POST",
                    )
                )
                return json.load(r)
            else:
                if self.project:
                    self.client.project = self.project

                query_schema = None
                referenced_tables = []
                table_metadata = None

                if self.sql:
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
                    query_schema = (
                        job._properties.get("statistics", {})
                        .get("query", {})
                        .get("schema", {})
                    )
                    referenced_tables = [
                        ref.to_api_repr() for ref in job.referenced_tables
                    ]

                if (
                    self.project is not None
                    and self.table is not None
                    and self.dataset is not None
                ):
                    table = self.client.get_table(
                        f"{self.project}.{self.dataset}.{self.table}"
                    )
                    table_metadata = {
                        "tableType": table.table_type,
                        "friendlyName": table.friendly_name,
                        "schema": {
                            "fields": [field.to_api_repr() for field in table.schema]
                        },
                    }

                return {
                    "valid": True,
                    "referencedTables": referenced_tables,
                    "schema": query_schema,
                    "tableMetadata": table_metadata,
                }
        except Exception as e:
            print(f"ERROR {e}")
            return None

    def get_schema(self):
        """Return the query schema by dry running the SQL file."""
        if not self.is_valid():
            raise DryRunError(
                "Error when dry running SQL", self.get_error(), self.use_cloud_function
            )

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "schema" in self.dry_run_result
        ):
            return self.dry_run_result["schema"]["fields"]

        return {}

    def get_table_schema(self):
        """Return the schema of the provided table."""
        if not self.is_valid():
            raise DryRunError(
                "Error when dry running SQL", self.get_error(), self.use_cloud_function
            )

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "tableMetadata" in self.dry_run_result
        ):
            return self.dry_run_result["tableMetadata"]["schema"]["fields"]

        return {}

    def get_table_metadata(self):
        """Return table metadata."""
        if not self.is_valid():
            raise DryRunError(
                "Error when dry running SQL", self.get_error(), self.use_cloud_function
            )

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "tableMetadata" in self.dry_run_result
        ):
            return self.dry_run_result["tableMetadata"]

        return {}

    def is_valid(self):
        """Dry run the provided SQL file and check if valid."""
        if self.dry_run_result is None:
            return False

        if self.dry_run_result["valid"]:
            return True
        elif self.get_error() == Errors.READ_ONLY:
            # We want the dryrun service to only have read permissions, so
            # we expect CREATE VIEW and CREATE TABLE to throw specific
            # exceptions.
            return True
        elif self.get_error() == Errors.DATE_FILTER_NEEDED:
            # With strip_dml flag, some queries require a partition filter
            # (submission_date, submission_timestamp, etc.) to run
            # We mark these requests as valid and add a date filter
            # in get_referenced_table()
            return True
        else:
            print("ERROR\n", self.dry_run_result["errors"])
            return False

    def errors(self):
        """Dry run the provided SQL file and return errors."""
        if self.dry_run_result is None:
            return []
        return self.dry_run_result.get("errors", [])

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
            if "Permission bigquery.tables.get denied on table" in error_message:
                return Errors.PERMISSION_DENIED
        return None
