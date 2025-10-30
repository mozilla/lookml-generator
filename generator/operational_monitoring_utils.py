"""Utils for operational monitoring."""

from multiprocessing.pool import ThreadPool
from typing import Any, Dict, List, Optional, Tuple

from google.api_core import exceptions
from google.cloud import bigquery

from .views import lookml_utils


def _default_helper(
    bq_client: bigquery.Client, table: str, dimension: str
) -> Tuple[Optional[str], dict]:
    query_job = bq_client.query(
        f"""
            SELECT DISTINCT {dimension} AS option, COUNT(*)
            FROM {table}
            WHERE {dimension} IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 10
        """
    )

    dimension_options = list(query_job.result())

    if len(dimension_options) > 0:
        return dimension, {
            "default": dimension_options[0]["option"],
            "options": [d["option"] for d in dimension_options],
        }
    return None, {}


def get_dimension_defaults(
    bq_client: bigquery.Client, table: str, dimensions: List[str]
) -> Dict[str, Any]:
    """
    Find default values for certain dimensions.

    For a given Operational Monitoring dimension, find its default (most common)
    value and its top 10 most common to be used as dropdown options.
    """
    with ThreadPool(4) as pool:
        return {
            key: value
            for key, value in pool.starmap(
                _default_helper,
                [[bq_client, table, dimension] for dimension in dimensions],
            )
            if key is not None
        }


def get_xaxis_val(table: str, dryrun) -> str:
    """
    Return whether the x-axis should be build_id or submission_date.

    This is based on which one is found in the table provided.
    """
    all_dimensions = lookml_utils._generate_dimensions(table, dryrun=dryrun)
    return (
        "build_id"
        if "build_id" in {dimension["name"] for dimension in all_dimensions}
        else "submission_date"
    )


def get_active_projects(
    bq_client: bigquery.Client, project_table: str
) -> List[Dict[str, Any]]:
    """Select all operational monitoring projects."""
    try:
        query_job = bq_client.query(
            f"""
                SELECT *
                FROM `{project_table}`
                WHERE
                    end_date > CURRENT_DATE() OR
                    end_date IS NULL
            """
        )

        projects = [dict(row) for row in query_job.result()]
    except exceptions.Forbidden:
        projects = []
    return projects
