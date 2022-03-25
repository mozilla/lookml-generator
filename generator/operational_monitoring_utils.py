"""Utils for operational monitoring."""
from typing import Any, Dict, List

from google.api_core import exceptions
from google.cloud import bigquery

from .views import lookml_utils


def get_dimension_defaults(
    bq_client: bigquery.Client, table: str, dimensions: List[str]
) -> Dict[str, Any]:
    """
    Find default values for certain dimensions.

    For a given Operational Monitoring dimension, find its default (most common)
    value and its top 10 most common to be used as dropdown options.
    """
    dimension_defaults = {}

    for dimension in dimensions:
        query_job = bq_client.query(
            f"""
                SELECT DISTINCT {dimension} AS option, COUNT(*)
                FROM {table}
                WHERE {dimension} IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
            """
        )

        dimension_options = [dict(row) for row in query_job.result()]

        if len(dimension_options) > 0:
            dimension_defaults[dimension] = {
                "default": dimension_options[0]["option"],
                "options": [d["option"] for d in dimension_options[:10]],
            }

    return dimension_defaults


def get_xaxis_val(bq_client: bigquery.Client, table: str) -> str:
    """
    Return whether the x-axis should be build_id or submission_date.

    This is based on which one is found in the table provided.
    """
    all_dimensions = lookml_utils._generate_dimensions(bq_client, table)
    return (
        "build_id"
        if "build_id" in {dimension["name"] for dimension in all_dimensions}
        else "submission_date"
    )


def get_projects(
    bq_client: bigquery.Client, project_table: str
) -> List[Dict[str, Any]]:
    """Select all operational monitoring projects."""
    try:
        query_job = bq_client.query(
            f"""
                SELECT *
                FROM `{project_table}`
            """
        )

        projects = [dict(row) for row in query_job.result()]
    except exceptions.Forbidden:
        projects = []
    return projects
