"""Utils for operational monitoring."""
from typing import Any, Dict, List

from google.cloud import bigquery

from .views import lookml_utils

# todo: move to methods and delete file


def compute_opmon_dimensions(
    bq_client: bigquery.Client, table: str, allowed_dimensions: List[str] = []
) -> List[Dict[str, Any]]:
    """
    Compute dimensions for Operational Monitoring.

    For a given Operational Monitoring dimension, find its default (most common)
    value and its top 10 most common to be used as dropdown options.
    """
    all_dimensions = lookml_utils._generate_dimensions(bq_client, table)
    dimensions = []

    relevant_dimensions = [
        dimension
        for dimension in all_dimensions
        if dimension["name"] in allowed_dimensions
    ]
    for dimension in relevant_dimensions:
        dimension_name = dimension["name"]
        query_job = bq_client.query(
            f"""
                SELECT DISTINCT {dimension_name}, COUNT(*)
                FROM {table}
                GROUP BY 1
                ORDER BY 2 DESC
            """
        )

        title = lookml_utils.slug_to_title(dimension_name)
        dimension_options = query_job.result().to_dataframe()[dimension_name].tolist()

        dimension_kwarg = {
            "title": title,
            "name": dimension_name,
        }

        if len(dimension_options) > 0:
            dimension_kwarg.update(
                {
                    "default": dimension_options[0],
                    "options": dimension_options[:10],
                }
            )

        dimensions.append(dimension_kwarg)

    return dimensions


def get_xaxis_val(bq_client: bigquery.Client, table: str) -> str:
    """
    Return whether the x-axis should be build_id or submission_date.

    This is based on which one is found in the table provided.
    """
    all_dimensions = lookml_utils._generate_dimensions(bq_client, table)
    return (
        "build_id"
        if "build_id" in {dimension["name"] for dimension in all_dimensions}
        else "day"
    )


def get_projects(
    bq_client: bigquery.Client, project_table: str
) -> List[Dict[str, Any]]:
    query_job = bq_client.query(
        f"""
            SELECT *
            FROM {project_table}
        """
    )

    projects = [dict(row) for row in query_job.result()]
    return projects
